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
         list_all_keys/0,
         exchange_to_counter_bin/1,
         start_opts/1
        ]).

%% Internal exports for use by rabbit_delayed_message_m2k_converter
-export([start_db/0, internal_put/3]).

% --------------------------------------------
% Storage
% --------------------------------------------
-define(BUCKET, <<"x-delayed-messages">>).

start_opts(Path) ->
    ExtractFun = fun(?DELAYED_MSG_TAG, _Size, delete) ->
                         {{0, 0, undefined}, []};
                    (?DELAYED_MSG_TAG, Size, {Exchange, _Msg}) ->
                         % We can get rid of the `erlang:phash2(ExNameBin)` in
                         % the first element of the tuple since leveled uses it
                         % during compaction to detect value changes. Since
                         % keys are unique and never overwritten the phash is
                         % not needed to detect changes.
                         {{0, Size, exchange_to_counter_bin(Exchange)}, []}
                 end,
    [{root_path, Path},
     {compression_method, none},
     {reload_strategy, [{?DELAYED_MSG_TAG, retain}]},
     {override_functions, [{extract_metadata, ExtractFun}]}].

start_db() ->
    Path = filename:join([rabbit_plugins:user_provided_plugins_data_dir(),
                          "rabbit_delayed_message",
                          "leveled"]),
    ok = filelib:ensure_path(Path),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(Path)),
    ?BOOKIE(Bookie).

setup() ->
    %% Close any bookie left open by the migration converter so there is at most
    %% one bookie at this path at a time. Errors are swallowed; the data is safe
    %% on disk via the leveled journal and will be recovered by book_start/1.
    catch leveled_bookie:book_close(?BOOKIE),
    start_db(),
    init_index_and_counters().

disable_plugin() ->
    catch ets:delete(?INDEX_TABLE),
    leveled_bookie:book_destroy(?BOOKIE).

messages_delayed(Exchange) ->
    case get_counter(exchange_to_counter_bin(Exchange)) of
        not_found -> 0;
        Count -> Count
    end.

store_delay(DelayTS, Exchange, Message) ->
    ExNameBin = exchange_to_counter_bin(Exchange),
    Key = make_key(DelayTS),
    ets:insert(?INDEX_TABLE, {{DelayTS, Key}, ExNameBin}),
    case internal_put(Key, Exchange, Message) of
        ok    -> ok;
        pause -> int_store_pause()
    end,
    increase_counter(ExNameBin).

internal_put(Key, Exchange, Message) ->
    leveled_bookie:book_put(?BOOKIE, ?BUCKET, Key,
                            {Exchange, Message},
                            [],
                            ?DELAYED_MSG_TAG).

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
    case leveled_bookie:book_get(?BOOKIE, ?BUCKET, LeveledKey, ?DELAYED_MSG_TAG) of
        {ok, Msg} ->
            [Msg];
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
            %% Using book_put/6 instead of book_delete/4 since the latter
            %% hardcodes ?STD_TAG
            leveled_bookie:book_put(?BOOKIE, ?BUCKET, LeveledKey, delete, [], ?DELAYED_MSG_TAG),
            case ets:take(?INDEX_TABLE, IndexKey) of
                [{_, ExNameBin}] -> decrease_counter(ExNameBin);
                [] -> ok
            end
    end.

delete_empty_key(IndexKey) ->
    ets:delete(?INDEX_TABLE, IndexKey).

make_key(DelayTS) ->
    <<DelayTS:64/big, (crypto:strong_rand_bytes(16))/binary>>.

exchange_to_counter_bin(#exchange{name = #resource{virtual_host = VHost, name = Name}}) ->
    VHostLen = byte_size(VHost),
    <<VHostLen:16/big, VHost/binary, Name/binary>>.

counter_bin_to_exchange_name(<<VHostLen:16/big, VHost:VHostLen/binary, Name/binary>>) ->
    #resource{virtual_host = VHost, kind = exchange, name = Name}.

% --------------------------------------------
% Counters
% --------------------------------------------

counter_key(ExNameBin) ->
    {?MODULE, counter, ExNameBin}.

get_counter(ExNameBin) ->
    case persistent_term:get(counter_key(ExNameBin), undefined) of
        undefined -> not_found;
        Atomic ->
            atomics:get(Atomic, 1)
    end.

increase_counter(ExNameBin) ->
    Counter = persistent_term:get(counter_key(ExNameBin), undefined),
    case Counter of
        undefined ->
            Counter1 = atomics:new(1, [{signed, true}]),
            atomics:put(Counter1, 1, 1),
            persistent_term:put(counter_key(ExNameBin), Counter1);
        _ ->
            atomics:add(Counter, 1, 1)
    end.

decrease_counter(ExNameBin) ->
    case persistent_term:get(counter_key(ExNameBin), undefined) of
        undefined ->
            rabbit_log:warning("delayed message counter not found for exchange ~tp while trying to decrease", [counter_bin_to_exchange_name(ExNameBin)]);
        Counter ->
            case atomics:get(Counter, 1) of
                0 ->
                    rabbit_log:warning("delayed message counter already zero for exchange ~tp and we tried to decrease", [counter_bin_to_exchange_name(ExNameBin)]);
                _ ->
                    atomics:sub(Counter, 1, 1)
            end
    end.

% --------------------------------------------
% Internal functions
% --------------------------------------------

%% Creates the index table and initialises per-exchange counters in a single
%% Leveled metadata fold. The extract_metadata override stored ExNameBin in
%% the ledger head, so book_headfold can retrieve it without touching the journal.
init_index_and_counters() ->
    ets:new(?INDEX_TABLE, [named_table, ordered_set, public]),
    FoldFun = fun(_B, <<DelayTS:64/big, _:128>> = Key, ProxyBin, Acc) ->
        %% The ledger head is wrapped in a proxy_object by book_headfold.
        %% ExNameBin is in the UserMeta slot of the metadata tuple stored there.
        {proxy_object, {_Hash, _Size, ExNameBin}, _, _} = binary_to_term(ProxyBin),
        ets:insert(?INDEX_TABLE, {{DelayTS, Key}, ExNameBin}),
        maps:update_with(ExNameBin, fun(C) -> C + 1 end, 1, Acc)
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        ?BOOKIE,
        ?DELAYED_MSG_TAG,
        {bucket_list, [?BUCKET]},
        {FoldFun, #{}},
        false,
        false,
        false),
    DelayedPerExchange = Runner(),
    maps:foreach(fun(ExNameBin, Count) ->
                      Atomic = atomics:new(1, [{signed, true}]),
                      atomics:put(Atomic, 1, Count),
                      persistent_term:put(counter_key(ExNameBin), Atomic)
              end, DelayedPerExchange).

list_all_keys() ->
    FoldFun = fun(_B, Key, Acc) -> [Key | Acc] end,
    {async, Runner} = leveled_bookie:book_keylist(
        ?BOOKIE, ?DELAYED_MSG_TAG, ?BUCKET, {FoldFun, []}),
    Runner().

%% Named for debugging: trace this function to observe backpressure events
%% during normal operation.
int_store_pause() ->
    rabbit_log:warning("Delayed message store pausing due to backpressure from Leveled Bookie", []),
    timer:sleep(1000).
