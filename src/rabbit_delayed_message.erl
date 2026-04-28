%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_delayed_message).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").
-include("rabbit_delayed_message.hrl").

-behaviour(gen_server).

% Public API exports
-export([start_link/0,
         disable_plugin/0,
         delay_message/3,
         messages_delayed/1,
         await_khepri_and_setup/0]).

%% Gen server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Testing & debugging exports
-export([refresh_config/0]).

-import(rabbit_delayed_message_utils, [swap_delay_header/1]).

-type t_reference() :: reference().
-type delay() :: non_neg_integer().

-record(state, {timer,
                stats_state}).

%%--------------------------------------------------------------------
% Public API exports
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

disable_plugin() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> rabbit_delayed_message_mnesia:disable_plugin() end,
        khepri => fun() -> rabbit_delayed_message_leveled:disable_plugin() end}
     ).

-spec delay_message(rabbit_types:exchange(),
                    mc:state(),
                    delay()) ->
                           nodelay | {ok, t_reference()}.
delay_message(Exchange, Message, Delay) ->
    gen_server:call(?MODULE, {delay_message, Exchange, Message, Delay},
                    infinity).

messages_delayed(Exchange) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> rabbit_delayed_message_mnesia:messages_delayed(Exchange) end,
        khepri => fun() -> rabbit_delayed_message_leveled:messages_delayed(Exchange) end}
     ).

refresh_config() ->
    gen_server:call(?MODULE, refresh_config).

await_khepri_and_setup() ->
    gen_server:cast(?MODULE, await_khepri_and_setup).

%%--------------------------------------------------------------------
% Gen server exports
init([]) ->
    % TODO: I removed the startup complexity + the `go/0` part in this module.
    % Do I need to do something to compensate and ensure no message is waiting to be
    % delayed/delay-expire without `timer` (i.e. `timer = not_set`)?
    setup(),
    _ = recover(),
    State0 = #state{timer = maybe_delay_first()},
    State = rabbit_event:init_stats_timer(State0, #state.stats_state),
    {ok, State}.

handle_call({delay_message, Exchange, Message, Delay},
            _From, State = #state{timer = CurrTimer}) ->
    Reply = {ok, NewTimer} = internal_delay_message(CurrTimer, Exchange, Message, Delay),
    State2 = State#state{timer = NewTimer},
    {reply, Reply, State2};
handle_call(refresh_config, _From, State) ->
    {reply, ok, refresh_config(State)};
handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(await_khepri_and_setup, State) ->
    {noreply, maybe_switch_to_leveled(State)};
handle_cast(_C, State) ->
    {noreply, State}.

handle_info({timeout, _TimerRef, {deliver, Key}}, State) ->
    case get_many(Key) of
        [] ->
            delete_index(Key);
        Deliveries ->
            _ = route(Deliveries, State),
            delete(Key),
            delete_index(Key)
    end,
    {noreply, State#state{timer = maybe_delay_first()}};
handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------
get_many(Key) ->
    rabbit_khepri:handle_fallback(
            #{mnesia => fun() -> rabbit_delayed_message_mnesia:get_many(Key) end,
              khepri => fun() -> rabbit_delayed_message_leveled:get_many(Key) end}
        ).

delete(Key) ->
    rabbit_khepri:handle_fallback(
            #{mnesia => fun() -> rabbit_delayed_message_mnesia:delete(Key) end,
              khepri => fun() -> rabbit_delayed_message_leveled:delete(Key) end}
        ).

delete_index(Key) ->
    rabbit_khepri:handle_fallback(
            #{mnesia => fun() -> rabbit_delayed_message_mnesia:delete_index(Key) end,
              khepri => fun() -> rabbit_delayed_message_leveled:delete_index(Key) end}
        ).

get_first_delay() ->
    rabbit_khepri:handle_fallback(
            #{mnesia => fun() -> rabbit_delayed_message_mnesia:get_first_delay() end,
              khepri => fun() -> rabbit_delayed_message_leveled:get_first_delay() end}
        ).

maybe_delay_first() ->
    case get_first_delay() of
        undefined ->
            %% nothing to do
            not_set;
        {FirstTS, Key} ->
            %% there are messages that will expire and need to be delivered
            Now = erlang:system_time(milli_seconds),
            start_timer(FirstTS - Now, Key)
    end.

route(Deliveries, State) ->
    lists:map(fun ({Ex, Msg}) ->
                      ExName = Ex#exchange.name,
                      Msg1 = swap_delay_header(Msg),
                      Dests = rabbit_exchange:route(Ex, Msg1),
                      Qs = rabbit_db_queue:get_targets(Dests),
                      _ = rabbit_queue_type:deliver(Qs, Msg1, #{}, stateless),
                      bump_routed_stats(ExName, Qs, State)
              end, Deliveries).

-spec internal_delay_message(TReference,
                             Exchange,
                             MCState,
                             Delay) -> Resp when
    TReference :: t_reference(),
    Exchange :: rabbit_types:exchange(),
    MCState :: mc:state(),
    Delay :: delay(),
    Resp :: nodelay | {ok, t_reference()}.
internal_delay_message(CurrTimer, Exchange, Message, Delay) ->
    Now = erlang:system_time(milli_seconds),
    %% keys are timestamps in milliseconds,in the future
    DelayTS = Now + Delay,
    store_delayed(DelayTS, Exchange, Message),
    case CurrTimer of
        not_set ->
            %% No timer in progress, so we start our own.
            {ok, maybe_delay_first()};
        _ ->
            case erlang:read_timer(CurrTimer) of
                false ->
                    %% Timer is already expired.  Handler will be invoked soon.
                    {ok, CurrTimer};
                CurrMS when Delay < CurrMS ->
                    %% Current timer lasts longer that new message delay
                    _ = erlang:cancel_timer(CurrTimer),
                    % TODO: Can we save some time getting the key from before?
                    {DelayTS, _Key} = get_first_delay(),
                    {ok, start_timer(Delay, DelayTS)};
                _ ->
                    %% Timer is set to expire sooner than this
                    %% message's scheduled delivery time.
                    {ok, CurrTimer}
            end
    end.

%% Key will be used upon message receipt to fetch
%% the deliveries from the database
store_delayed(DelayTS, Exchange, Message) ->
    rabbit_khepri:handle_fallback(
            #{mnesia => fun() ->
                            rabbit_delayed_message_mnesia:store_delay(DelayTS,
                                                                      Exchange,
                                                                      Message)
                        end,
              khepri => fun() ->
                            rabbit_delayed_message_leveled:store_delay(DelayTS,
                                                                       Exchange,
                                                                       Message)
                        end}
        ).

start_timer(Delay, Key) ->
    erlang:start_timer(erlang:max(0, Delay), self(), {deliver, Key}).

setup() ->
    rabbit_khepri:handle_fallback(
            #{mnesia => fun() -> rabbit_delayed_message_mnesia:setup() end,
              khepri => fun() -> rabbit_delayed_message_leveled:setup() end}
        ).

recover() ->
    %% topology recovery has already happened, we have to recover state for any durable
    %% consistent hash exchanges since plugin activation was moved later in boot process
    %% starting with RabbitMQ 3.8.4
    case list_exchanges() of
        {error, Reason} ->
            ?LOG_ERROR(
               "Delayed message exchange: "
               "failed to recover durable bindings of one of the exchanges, reason: ~p",
               [Reason]);
        Xs ->
            ?LOG_DEBUG("Delayed message exchange: "
                       "have ~b durable exchanges to recover",
                       [length(Xs)]),
            [recover_exchange_and_bindings(X) || X <- lists:usort(Xs)]
    end.

list_exchanges() ->
    Pattern = #exchange{durable = true, type = 'x-delayed-message', _ = '_'},
    rabbit_db_exchange:match(Pattern).

recover_exchange_and_bindings(#exchange{name = XName} = X) ->
    Bindings = rabbit_binding:list_for_source(XName),
    _ = [rabbit_exchange_type_delayed_message:add_binding(none, X, B)
         || B <- lists:usort(Bindings)],
    ?LOG_DEBUG("Delayed message exchange: "
               "recovered bindings for ~s",
               [rabbit_misc:rs(XName)]).

%% These metrics are normally bumped from a channel process via which
%% the publish actually happened. In the special case of delayed
%% message delivery, the singleton delayed_message gen_server does
%% this.
%%
%% Difference from delivering from a channel:
%%
%% The channel process keeps track of the state and monitors each
%% queue it routed to. When the channel is notified of a queue DOWN,
%% it marks all core metrics for that channel + queue as deleted.
%% Monitoring all queues would be overkill for the delayed message
%% gen_server, so this delete marking does not happen in this
%% case. Still `rabbit_core_metrics_gc' will periodically scan all the
%% core metrics and eventually delete entries for non-existing queues
%% so there won't be any metrics leak. `rabbit_core_metrics_gc' will
%% also delete the entries when this process is not alive ie when the
%% plugin is disabled.
bump_routed_stats(ExName, Qs, State) ->
    % TODO: I think the `#state.stats_state` is updated somewhere in rabbit
    % and my guess is that that's done based on the mnesia table name that is
    % passed in the `rabbit_mnesia_tables_to_khepri_db` module attribute.
    rabbit_global_counters:messages_routed(amqp091, length(Qs)),
    case rabbit_event:stats_level(State, #state.stats_state) of
        fine ->
            [begin
                 QName = amqqueue:get_name(Q),
                 %% Channel PID is just an identifier in the metrics
                 %% DB. However core metrics GC will delete entries
                 %% with a not-alive PID, and by the time the delayed
                 %% message gets delivered the original channel
                 %% process might be long gone, hence we need a live
                 %% PID in the key.
                 FakeChannelId = self(),
                 Key = {FakeChannelId, {QName, ExName}},
                 rabbit_core_metrics:channel_stats(queue_exchange_stats, publish, Key, 1)
             end
             || Q <- Qs],
            ok;
        _ ->
            ok
    end.

refresh_config(State) ->
    rabbit_event:init_stats_timer(State, #state.stats_state).

%% Called after the mnesia-to-leveled migration has written data to disk.
%% is_enabled/1 uses blocking mode: it waits for the feature flag to stabilise
%% before returning, so no polling loop is needed.
maybe_switch_to_leveled(State = #state{timer = CurrTimer}) ->
    case rabbit_feature_flags:is_enabled(khepri_db, blocking) of
        true ->
            case CurrTimer of
                not_set -> ok;
                _       -> erlang:cancel_timer(CurrTimer)
            end,
            setup(),
            State#state{timer = maybe_delay_first()};
        false ->
            rabbit_log:warning("Delayed message exchange: "
                             "khepri_db feature flag is not enabled, "
                             "delayed messages will continue to be stored in Mnesia"),
            State
    end.
