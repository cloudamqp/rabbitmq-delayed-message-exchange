%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_leveled).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("leveled/include/leveled.hrl").
-include("rabbit_delayed_message.hrl").

-define(BOOKIE(Bookie), persistent_term:put({?MODULE, bookie}, Bookie)).
-define(BOOKIE, persistent_term:get({?MODULE, bookie}, undefined)).

%% ordered_set ETS table keyed by <<TS:64/big, Random:16/binary>>.
%% Big-endian byte order means ets:first/1 always returns the minimum-timestamp key.
-define(INDEX_TABLE, rabbit_delayed_message_leveled_key_index).

-export([setup/0,
         disable_plugin/0,
         messages_delayed/1,
         store_delay/3,
         get_first_delay/0,
         get_many/1,
         delete/1,
         delete_empty_key/1,
         list_all_keys/0
        ]).

% --------------------------------------------
% Storage
% --------------------------------------------
%% All delayed messages share a single bucket. Keys are <<TS:64/big, Random:16/binary>>
%% so they sort globally by delivery timestamp. The exchange name is stored inside
%% the value, allowing a single range scan at timer expiry to collect all due messages
%% regardless of which exchange they belong to.
-define(BUCKET, <<"x-delayed-messages">>).

setup() ->
    Path = filename:join([rabbit_plugins:user_provided_plugins_data_dir(),
                          "rabbit_delayed_message",
                          "leveled"]),
    ok = filelib:ensure_path(Path),
    %% Close any bookie left open by the migration converter so there is at most
    %% one bookie at this path at a time. Errors are swallowed; the data is safe
    %% on disk via the leveled journal and will be recovered by book_start/1.
    catch leveled_bookie:book_close(?BOOKIE),
    {ok, Bookie} = leveled_bookie:book_start([{root_path, Path},
                                              {compression_method, none}]),
    ?BOOKIE(Bookie),
    init_index(),
    init_counters().

disable_plugin() ->
    catch ets:delete(?INDEX_TABLE),
    leveled_bookie:book_destroy(?BOOKIE).

messages_delayed(Exchange) ->
    case get_counter(Exchange#exchange.name) of
        not_found -> 0;
        Count -> Count
    end.

store_delay(DelayTS, Exchange, Message) ->
    Key = make_key(DelayTS),
    % Insert `{DelayTS, Key}' as ETS table key (wrapped in additional `{}')
    ets:insert(?INDEX_TABLE, {{DelayTS, Key}}),
    case leveled_bookie:book_put(?BOOKIE, ?BUCKET, Key,
                                     term_to_binary({Exchange, Message}), []) of
        ok    -> ok;
        pause -> int_store_pause()
    end,
    increase_counter(Exchange#exchange.name).

get_first_delay() ->
    case ets:whereis(?INDEX_TABLE) of
        undefined ->
            undefined;
        _ ->
            case ets:first(?INDEX_TABLE) of
                '$end_of_table' ->
                    undefined;
                {DelayTS, _LeveledKey} = IndexKey ->
                    {DelayTS, IndexKey}
            end
    end.

get_many({_TS, LeveledKey} = _IndexKey) ->
    % We only return one entry per get_many call. This simplifies the
    % implementation around index ETS and Leveled Bookie. We just let the
    % gen_server trigger it's loop more times for leveled.
    case leveled_bookie:book_get(?BOOKIE, ?BUCKET, LeveledKey) of
        {ok, Value} ->
            [binary_to_term(Value)];
        _ ->
            []
    end;
get_many(_) ->
    %% Stale Mnesia-format key from a timer set before the migration to Leveled.
    %% The data is in Leveled under a different key; a new timer will be started
    %% by handle_info via maybe_delay_first/0 after this returns.
    [].

delete({_DelayTS, LeveledKey} = IndexKey) ->
    case ets:whereis(?INDEX_TABLE) of
        undefined ->
            ok;
        _ ->
            case leveled_bookie:book_get(?BOOKIE, ?BUCKET, LeveledKey) of
                {ok, Value} ->
                    {Exchange, _} = binary_to_term(Value),
                    decrease_counter(Exchange#exchange.name);
                _ ->
                    ok
            end,
            leveled_bookie:book_delete(?BOOKIE, ?BUCKET, LeveledKey, []),
            ets:delete(?INDEX_TABLE, IndexKey)
    end.

delete_empty_key(IndexKey) ->
    ets:delete(?INDEX_TABLE, IndexKey).

make_key(DelayTS) ->
    <<DelayTS:64/big, (crypto:strong_rand_bytes(16))/binary>>.

% --------------------------------------------
% Counters
% --------------------------------------------
init_counters() ->
    DelayedPerExchange = delayed_per_exchange(),
    maps:foreach(fun(ExName, Count) ->
                      Atomic = atomics:new(1, [{signed, false}]),
                      atomics:put(Atomic, 1, Count),
                      persistent_term:put(counter_key(ExName), Atomic)
              end, DelayedPerExchange).

counter_key(ExName) ->
    {?MODULE, counter, ExName}.

get_counter(ExName) ->
    case persistent_term:get(counter_key(ExName), undefined) of
        undefined -> not_found;
        Atomic ->
            atomics:get(Atomic, 1)
    end.

increase_counter(ExName) ->
    Counter = persistent_term:get(counter_key(ExName), undefined),
    case Counter of
        undefined ->
            Counter1 = atomics:new(1, [{signed, false}]),
            atomics:put(Counter1, 1, 1),
            persistent_term:put(counter_key(ExName), Counter1);
        _ ->
            atomics:add(Counter, 1, 1)
    end.

decrease_counter(ExName) ->
    Counter = persistent_term:get(counter_key(ExName), undefined),
    case Counter of
        undefined ->
            rabbit_log:warning("delayed message counter not found for exchange ~tp while trying to decrease", [ExName]);
        _ ->
            atomics:sub(Counter, 1, 1)
    end.

% --------------------------------------------
% Internal functions
% --------------------------------------------

%% Creates the index table and populates it from any keys already present
%% in leveled (relevant on restart with existing data).
init_index() ->
    ets:new(?INDEX_TABLE, [named_table, ordered_set, public]),
    FoldFun = fun(_B, <<DelayTS:64/big, _:128>> = Key, _Acc) ->
        ets:insert(?INDEX_TABLE, {{DelayTS, Key}}),
        ok
    end,
    {async, Runner} = leveled_bookie:book_keylist(
        ?BOOKIE, ?STD_TAG, ?BUCKET, {FoldFun, ok}),
    Runner().

list_all_keys() ->
    FoldFun = fun(_B, Key, Acc) -> [Key | Acc] end,
    {async, Runner} = leveled_bookie:book_keylist(
        ?BOOKIE, ?STD_TAG, ?BUCKET, {FoldFun, []}),
    Runner().

%% Named for debugging: trace this function to observe backpressure events
%% during normal operation.
int_store_pause() ->
    rabbit_log:warning("Delayed message store pausing due to backpressure from Leveled Bookie"),
    timer:sleep(1000).

delayed_per_exchange() ->
    FoldFun = fun(_B, _K, Term, Acc) ->
                      % TODO: oof, having to transform each term aint good
                      {Exchange, _} = binary_to_term(Term),
                      maps:update_with(Exchange#exchange.name, fun(C) -> C + 1 end, 1, Acc)
              end,
    {async, Runner} = leveled_bookie:book_objectfold(
        ?BOOKIE, ?STD_TAG, ?BUCKET, all, {FoldFun, #{}}, false),
     Runner().
