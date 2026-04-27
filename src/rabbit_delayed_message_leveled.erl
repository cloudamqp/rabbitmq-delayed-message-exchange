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
         delete_index/1
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
    Path = filename:join([rabbit_khepri:dir(), "rabbit_delayed_message", "leveled"]),
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
    leveled_bookie:book_close(?BOOKIE).

messages_delayed(Exchange) ->
    case get_counter(Exchange#exchange.name) of
        not_found -> 0;
        Count -> Count
    end.

store_delay(DelayTS, Exchange, Message) ->
    increase_counter(Exchange#exchange.name),
    Key = make_key(DelayTS),
    leveled_bookie:book_put(?BOOKIE, ?BUCKET, Key,
                                term_to_binary({Exchange, Message}), []),
    % Insert `{DelayTS, Key}' as ETS table key (wrapped in additional `{}')
    ets:insert(?INDEX_TABLE, {{DelayTS, Key}}).

get_first_delay() ->
    case ets:whereis(?INDEX_TABLE) of
        undefined ->
            undefined;
        _ ->
            case ets:first(?INDEX_TABLE) of
                '$end_of_table' ->
                    undefined;
                {DelayTS, _Key} ->
                    % Return DelayTS both as DelayTS and as key
                    % Mnesia uses the _Key, but on leveled implementation we use the
                    % DelayTS to lookup the entries to deliver
                    {DelayTS, DelayTS}
            end
    end.

get_many(DelayTS) ->
    IndexEntries = get_many(DelayTS, []),
    Results = [leveled_bookie:book_get(?BOOKIE, ?BUCKET, Key) || {_DeliveryTS, Key} <- IndexEntries],
    [Entry || {ok, Value} <- Results,
              Entry <- [binary_to_term(Value)]].

get_many(DelayTS, Acc) ->
    case ets:whereis(?INDEX_TABLE) of
        undefined ->
            lists:reverse(Acc);
        _ ->
            case ets:first(?INDEX_TABLE) of
                '$end_of_table' ->
                    lists:reverse(Acc);
                {FirstDelay, _Key} = IndexEntry when FirstDelay =:= DelayTS ->
                    % We need to delete as we go to avoid infinite loop
                    ets:delete(?INDEX_TABLE, IndexEntry),
                    get_many(DelayTS, [IndexEntry | Acc]);
                _ ->
                    lists:reverse(Acc)
            end
    end.

delete(DelayTS) ->
    case ets:whereis(?INDEX_TABLE) of
        undefined ->
            ok;
        _ ->
            case ets:first(?INDEX_TABLE) of
                '$end_of_table' ->
                    ok;
                {FirstDelay, Key} = IndexEntry when FirstDelay =:= DelayTS ->
                    % We need to delete as we go to avoid infinite loop
                    ets:delete(?INDEX_TABLE, IndexEntry),
                    leveled_bookie:book_delete(?BOOKIE, ?BUCKET, Key, []);
                _ ->
                    ok
            end
    end.

delete_index(DeliveryTS) ->
    case ets:whereis(?INDEX_TABLE) of
        undefined -> ok;
        _ -> ets:delete(?INDEX_TABLE, DeliveryTS)
    end.

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

delayed_per_exchange() ->
    FoldFun = fun(_B, _K, Term, Acc) ->
                      % TODO: oof, having to transform each term aint good
                      {Exchange, _} = binary_to_term(Term),
                      maps:update_with(Exchange#exchange.name, fun(C) -> C + 1 end, 1, Acc)
              end,
    {async, Runner} = leveled_bookie:book_objectfold(
        ?BOOKIE, ?STD_TAG, ?BUCKET, all, {FoldFun, #{}}, false),
     Runner().
