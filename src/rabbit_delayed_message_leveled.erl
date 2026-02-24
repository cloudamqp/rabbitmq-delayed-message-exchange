%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_leveled).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server).

-export([start_link/0, delay_message/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([messages_delayed/1]).

%% For testing, debugging and manual use
-export([refresh_config/0]).

-import(rabbit_delayed_message_utils, [swap_delay_header/1]).

%% ?STD_TAG from leveled.hrl
-define(STD_TAG, o).

%% All delayed messages share a single bucket. Keys are <<TS:64/big, Random:16/binary>>
%% so they sort globally by delivery timestamp. The exchange name is stored inside
%% the value, allowing a single range scan at timer expiry to collect all due messages
%% regardless of which exchange they belong to.
-define(BUCKET, <<"x-delayed-messages">>).

-record(state, {timer,
                bookie      :: pid(),
                stats_state}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

delay_message(Exchange, Message, Delay) ->
    gen_server:call(?MODULE, {delay_message, Exchange, Message, Delay},
                    infinity).

messages_delayed(Exchange) ->
    gen_server:call(?MODULE, {messages_delayed, Exchange}, infinity).

refresh_config() ->
    gen_server:call(?MODULE, refresh_config).

%%--------------------------------------------------------------------

init([]) ->
    Path = filename:join([rabbit_mnesia:dir(), "rabbit_delayed_message", "leveled"]),
    ok = filelib:ensure_path(Path),
    {ok, Bookie} = leveled_bookie:book_start([{root_path, Path}]),
    State = rabbit_event:init_stats_timer(
        #state{timer = not_set, bookie = Bookie}, #state.stats_state),
    {ok, State#state{timer = maybe_delay_first(Bookie)}}.

handle_call({delay_message, Exchange, Message, Delay},
            _From,
            State) ->
    NewTimer = internal_delay_message(State, Exchange, Message, Delay),
    {reply, {ok, NewTimer}, State#state{timer = NewTimer}};
handle_call({messages_delayed, Exchange},
            _From,
            State = #state{bookie = Bookie}) ->
    Count = count_messages_for_exchange(Bookie, Exchange#exchange.name),
    {reply, Count, State};
handle_call(refresh_config, _From, State) ->
    {reply, ok, refresh_config(State)};
handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({timeout, _TimerRef, {deliver, DeliveryTS}},
            State = #state{bookie = Bookie}) ->
    ExNameAndMsgs = fetch_and_delete(Bookie, DeliveryTS),
    _ = route_all(ExNameAndMsgs, State),
    {noreply, State#state{timer = maybe_delay_first(Bookie)}};
handle_info(_I, State) ->
    {noreply, State}.

terminate(_, #state{bookie = Bookie}) ->
    leveled_bookie:book_close(Bookie).

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

maybe_delay_first(Bookie) ->
    Now = erlang:system_time(milli_seconds),
    case first_timestamp(Bookie) of
        not_found -> not_set;
        {ok, TS}  -> start_timer(TS - Now, TS)
    end.

%% Returns {ok, EarliestTS} for the earliest pending delivery timestamp,
%% or not_found if the store is empty.
%%
%% Keys are stored as <<TS:64/big, Random:16/binary>> and leveled iterates
%% within a bucket in lexicographic (ascending) order, so the first key
%% carries the minimum timestamp.
first_timestamp(Bookie) ->
    FoldFun = fun(_B, K, _Acc) -> throw({first_key, K}) end,
    {async, Runner} = leveled_bookie:book_keylist(
        Bookie, ?STD_TAG, ?BUCKET, {FoldFun, not_found}),
    try Runner() of
        not_found -> not_found
    catch
        throw:{first_key, <<TS:64/big, _/binary>>} -> {ok, TS}
    end.

route_all(ExNameAndMsgs, State) ->
    lists:map(fun({ExName, Msg0}) ->
        case rabbit_db_exchange:get(ExName) of
            {ok, Exchange} ->
                Msg1 = swap_delay_header(Msg0),
                Dests = rabbit_exchange:route(Exchange, Msg1),
                Qs = rabbit_db_queue:get_targets(Dests),
                _ = rabbit_queue_type:deliver(Qs, Msg1, #{}, stateless),
                bump_routed_stats(ExName, Qs, State);
            _ ->
                ok
        end
    end, ExNameAndMsgs).

internal_delay_message(#state{bookie = Bookie, timer = CurrTimer},
                       Exchange, Message, Delay) ->
    Now = erlang:system_time(milli_seconds),
    DelayTS = Now + Delay,
    ExName = Exchange#exchange.name,
    _ = leveled_bookie:book_put(Bookie, ?BUCKET, make_db_key(DelayTS),
                                term_to_binary({ExName, Message}), []),
    case CurrTimer of
        not_set ->
            %% No timer running, scan to find the global earliest.
            maybe_delay_first(Bookie);
        _ ->
            case erlang:read_timer(CurrTimer) of
                false ->
                    %% Timer already fired, handler will be called soon.
                    CurrTimer;
                CurrMS when Delay < CurrMS ->
                    %% New message fires sooner than the running timer.
                    _ = erlang:cancel_timer(CurrTimer),
                    start_timer(Delay, DelayTS);
                _ ->
                    %% Running timer fires sooner, leave it alone.
                    CurrTimer
            end
    end.

start_timer(Delay, DeliveryTS) ->
    erlang:start_timer(erlang:max(0, Delay), self(), {deliver, DeliveryTS}).

make_db_key(DelayTS) ->
    <<DelayTS:64/big, (crypto:strong_rand_bytes(16))/binary>>.

%% Collect and delete all entries whose key begins with <<DeliveryTS:64/big>>,
%% returning the list of {ExName, Message} pairs.
fetch_and_delete(Bookie, DeliveryTS) ->
    StartKey = <<DeliveryTS:64/big, 0:128>>,
    EndKey   = <<DeliveryTS:64/big, 255, 255, 255, 255, 255, 255, 255, 255,
                                    255, 255, 255, 255, 255, 255, 255, 255>>,
    FoldFun = fun(_B, K, V, Acc) -> [{K, V} | Acc] end,
    {async, Runner} = leveled_bookie:book_objectfold(
        Bookie, ?STD_TAG, ?BUCKET, {StartKey, EndKey}, {FoldFun, []}, false),
    Entries = Runner(),
    lists:foreach(fun({Key, _}) ->
        _ = leveled_bookie:book_delete(Bookie, ?BUCKET, Key, [])
    end, Entries),
    [{ExName, Msg} || {_Key, Value} <- Entries,
                      {ExName, Msg} <- [binary_to_term(Value)]].

count_messages_for_exchange(Bookie, ExName) ->
    FoldFun = fun(_B, _K, V, Acc) ->
        case binary_to_term(V) of
            {ExName, _} -> Acc + 1;
            _           -> Acc
        end
    end,
    {async, Runner} = leveled_bookie:book_objectfold(
        Bookie, ?STD_TAG, ?BUCKET, all, {FoldFun, 0}, false),
    Runner().

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

refresh_config(State) ->
    rabbit_event:init_stats_timer(State, #state.stats_state).
