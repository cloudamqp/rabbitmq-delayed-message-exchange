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
    Path = filename:join([rabbit_mnesia:dir(), "rabbit_delayed_message", "leveled"]),
    ok = filelib:ensure_path(Path),
    {ok, Bookie} = leveled_bookie:book_start([{root_path, Path}]),
    ?BOOKIE(Bookie),
    init_counters().

disable_plugin() ->
    leveled_bookie:book_close(?BOOKIE).

messages_delayed(Exchange) ->
    get_counter(Exchange#exchange.name).

store_delay(DelayTS, Exchange, Message) ->
    increase_counter(Exchange#exchange.name),
    _ = leveled_bookie:book_put(?BOOKIE, ?BUCKET, make_key(DelayTS),
                                term_to_binary({Exchange, Message}), []).

get_first_delay() ->
    FoldFun = fun(_B, K, _Acc) -> throw({first_key, K}) end,
    {async, Runner} = leveled_bookie:book_keylist(
        ?BOOKIE, ?STD_TAG, ?BUCKET, {FoldFun, not_found}),
    try Runner() of
        not_found ->
            undefined
    catch
        throw:{first_key, <<TS:64/big, _:16/binary>> = FirstKey} ->
            {TS, FirstKey}
    end.

get_many(<<DeliveryTS:64/big, _:16/binary>>) ->
    StartKey = <<DeliveryTS:64/big, 0:128>>,
    EndKey   = <<DeliveryTS:64/big, 255, 255, 255, 255, 255, 255, 255, 255,
                                    255, 255, 255, 255, 255, 255, 255, 255>>,
    FoldFun = fun(_B, K, V, Acc) -> [{K, V} | Acc] end,
    {async, Runner} = leveled_bookie:book_objectfold(
        ?BOOKIE, ?STD_TAG, ?BUCKET, {StartKey, EndKey}, {FoldFun, []}, false),
    Entries = Runner(),
    [Entry || {_Key, Value} <- Entries,
              Entry <- [binary_to_term(Value)]].

delete(<<DeliveryTS:64/big, _:16/binary>>) ->
    StartKey = <<DeliveryTS:64/big, 0:128>>,
    EndKey   = <<DeliveryTS:64/big, 255, 255, 255, 255, 255, 255, 255, 255,
                                    255, 255, 255, 255, 255, 255, 255, 255>>,
    FoldFun = fun(B, K, Term, _Acc) ->
                      Res = leveled_bookie:book_delete(?BOOKIE, B, K, []),
                      {Exchange, _} = binary_to_term(Term),
                      decrease_counter(Exchange#exchange.name),
                      Res
              end,
    {async, Runner} = leveled_bookie:book_objectfold(
        ?BOOKIE, ?STD_TAG, ?BUCKET, {StartKey, EndKey}, {FoldFun, []}, false),
     Runner().

delete_index(_Key) ->
    ok.

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
    rabbit_log:critical("Getting counter for exchange ~p", [ExName]),
    case persistent_term:get(counter_key(ExName), undefined) of
        undefined -> not_found;
        Atomic ->
            Res = atomics:get(Atomic, 1),
            rabbit_log:critical("Result ~p", [Res]),
            Res
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
    atomics:sub(Counter, 1, 1).

% --------------------------------------------
% Internal functions
% --------------------------------------------
delayed_per_exchange() ->
    FoldFun = fun(_B, _K, Term, Acc) ->
                      % TODO: oof, having to transform each term aint good
                      {Exchange, _} = binary_to_term(Term),
                      maps:update_with(Exchange#exchange.name, fun(C) -> C + 1 end, 1, Acc)
              end,
    {async, Runner} = leveled_bookie:book_objectfold(
        ?BOOKIE, ?STD_TAG, ?BUCKET, all, {FoldFun, #{}}, false),
     Runner().
