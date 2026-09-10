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

-define(APP, rabbitmq_delayed_message_exchange).

%% Default interval between journal compactions in seconds. Needed
%% because leveled never compacts its journal on its own and delegates
%% invoking compaction to the application.
-define(DEFAULT_COMPACTION_INTERVAL_SECONDS, 600).

-behaviour(gen_server).

% Public API exports
-export([start_link/0,
         disable_plugin/0,
         delay_message/3,
         messages_delayed/1
        ]).

%% Gen server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Operator exports
-export([refresh_config/0,
         compact_journal/0]).

-import(rabbit_delayed_message_utils, [swap_delay_header/1]).

-type t_reference() :: reference().
-type delay() :: non_neg_integer().

-record(state, {timer,
                compaction_timer,
                stats_state}).

%%--------------------------------------------------------------------
% Public API exports
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

disable_plugin() ->
    rabbit_delayed_message_leveled:disable_plugin().

-spec delay_message(rabbit_types:exchange(),
                    mc:state(),
                    delay()) ->
                           nodelay | {ok, t_reference()}.
delay_message(Exchange, Message, Delay) ->
    gen_server:call(?MODULE, {delay_message, Exchange, Message, Delay},
                    infinity).

messages_delayed(Exchange) ->
    rabbit_delayed_message_leveled:messages_delayed(Exchange).

refresh_config() ->
    gen_server:call(?MODULE, refresh_config).

%% Runs a journal compaction immediately, independently of the schedule.
-spec compact_journal() -> ok | busy | not_running | {error, term()}.
compact_journal() ->
    gen_server:call(?MODULE, compact_journal, infinity).

%%--------------------------------------------------------------------
init([]) ->
    %% Trap exits so terminate/2 runs on supervisor shutdown and we get
    %% a chance to tear down the storage backend symmetrically with
    %% setup/0.
    process_flag(trap_exit, true),
    setup(),
    _ = recover(),
    State0 = #state{timer = maybe_delay_first(),
                    compaction_timer = schedule_compaction()},
    State = rabbit_event:init_stats_timer(State0, #state.stats_state),
    {ok, State}.

handle_call({delay_message, Exchange, Message, Delay},
            _From, State = #state{timer = CurrTimer}) ->
    Reply = {ok, NewTimer} = internal_delay_message(CurrTimer, Exchange, Message, Delay),
    State2 = State#state{timer = NewTimer},
    {reply, Reply, State2};
handle_call(compact_journal, _From, State) ->
    {reply, compact_journal_now(), State};
handle_call(refresh_config, _From, State) ->
    {reply, ok, apply_config(State)};
handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({timeout, _TimerRef, {deliver, Key}}, State) ->
    case get_many(Key) of
        [] ->
            delete_empty_key(Key);
        Deliveries ->
            _ = route(Deliveries, State),
            delete(Key)
    end,
    {noreply, State#state{timer = maybe_delay_first()}};
handle_info({timeout, TRef, compact_journal},
            State = #state{compaction_timer = TRef}) ->
    _ = compact_journal_now(),
    {noreply, State#state{compaction_timer = schedule_compaction()}};
handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    %% We trap exits so the supervisor's shutdown signal lands in
    %% terminate/2, but a linked process dying abnormally (the Leveled
    %% bookie is the only one we link to) must still take us down so
    %% the supervisor can restart us and reopen the store.
    {stop, Reason, State};
handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) ->
    %% terminate/2 fires for both plugin disable and broker shutdown.
    %% Only the former should destroy the storage backend; on broker
    %% shutdown delayed messages must survive across restarts. Neither
    %% rabbit:is_running/0 nor rabbit_boot_state:get/0 distinguishes
    %% the two at this point — apps stop in reverse dependency order,
    %% so our plugin stops while rabbit itself is still in `ready' on
    %% broker shutdown. rabbit_plugins:disable/1 however rewrites the
    %% enabled-plugins file before calling stop_apps/1, so at
    %% terminate time enabled_plugins/0 no longer lists us iff we are
    %% being disabled.
    case lists:member(?APP, rabbit_plugins:enabled_plugins()) of
        true  -> ok;              %% broker shutdown — keep data
        false -> disable_plugin() %% plugin disable — tear down
    end.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------
get_many(Key) ->
    rabbit_delayed_message_leveled:get_many(Key).

delete(Key) ->
    rabbit_delayed_message_leveled:delete(Key).

get_first_delay() ->
    rabbit_delayed_message_leveled:get_first_delay().

delete_empty_key(Key) ->
    rabbit_delayed_message_leveled:delete_empty_key(Key).

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
                    %% Current timer lasts longer than new message delay.
                    _ = erlang:cancel_timer(CurrTimer),
                    {ok, maybe_delay_first()};
                _ ->
                    %% Timer is set to expire sooner than this
                    %% message's scheduled delivery time.
                    {ok, CurrTimer}
            end
    end.

%% Key will be used upon message receipt to fetch
%% the deliveries from the database
store_delayed(DelayTS, Exchange, Message) ->
    rabbit_delayed_message_leveled:store_delay(DelayTS, Exchange, Message).

start_timer(Delay, Key) ->
    erlang:start_timer(erlang:max(0, Delay), self(), {deliver, Key}).

compact_journal_now() ->
    Result = rabbit_delayed_message_leveled:compact_journal(),
    case Result of
        ok ->
            ?LOG_DEBUG("Delayed message exchange: journal compaction started");
        busy ->
            ?LOG_DEBUG("Delayed message exchange: previous journal compaction "
                       "is still running, skipping this run");
        not_running ->
            ?LOG_DEBUG("Delayed message exchange: message store is not "
                       "running, skipping journal compaction");
        {error, Reason} ->
            %% Worth reporting, but not worth taking this process down
            %% over: the store keeps working, it just keeps its journal.
            ?LOG_WARNING("Delayed message exchange: journal compaction "
                         "could not be started: ~tp", [Reason])
    end,
    Result.

cancel_compaction_timer(undefined) ->
    ok;
cancel_compaction_timer(TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            %% The timer had already fired, so drop the message it left
            %% behind: the run it asked for is superseded by the one the
            %% freshly scheduled timer will ask for.
            receive
                {timeout, TRef, compact_journal} -> ok
            after 0 -> ok
            end;
        _ ->
            ok
    end.

schedule_compaction() ->
    case compaction_interval() of
        0 ->
            undefined;
        Seconds ->
            erlang:start_timer(timer:seconds(Seconds), self(), compact_journal)
    end.

compaction_interval() ->
    case application:get_env(?APP, journal_compaction_interval_seconds,
                             ?DEFAULT_COMPACTION_INTERVAL_SECONDS) of
        Seconds when is_integer(Seconds), Seconds >= 0 ->
            Seconds;
        Invalid ->
            ?LOG_WARNING("Delayed message exchange: invalid "
                         "journal_compaction_interval_seconds ~tp, using ~b s",
                         [Invalid, ?DEFAULT_COMPACTION_INTERVAL_SECONDS]),
            ?DEFAULT_COMPACTION_INTERVAL_SECONDS
    end.

setup() ->
    rabbit_delayed_message_leveled:setup().

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

%% Re-reads every setting this process caches in its state: the
%% statistics level and the journal compaction interval.
apply_config(State = #state{compaction_timer = TRef}) ->
    ok = cancel_compaction_timer(TRef),
    State1 = State#state{compaction_timer = schedule_compaction()},
    rabbit_event:init_stats_timer(State1, #state.stats_state).
