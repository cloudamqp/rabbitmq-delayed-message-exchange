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
%% Entries are {{DelayTS, Key}, ExNameBin, PayloadBytes}: the size is held here
%% so that deleting an entry can decrease the byte counter without a disk read.
-define(INDEX_TABLE, rabbit_delayed_message_leveled_key_index).

-export([setup/0,
         disable_plugin/0,
         store_delay/3,
         get_first_delay/0,
         get_many/1,
         delete/1,
         delete_empty_key/1,
         list_all_keys/0,
         start_opts/1
        ]).

% --------------------------------------------
% Storage
% --------------------------------------------
-define(BUCKET, <<"x-delayed-messages">>).

start_opts(Path) ->
    ExtractFun = fun(?DELAYED_MSG_TAG, _Size, delete) ->
                         {{0, 0, undefined}, []};
                    (?DELAYED_MSG_TAG, Size, {Exchange, Msg}) ->
                         % The first element is the object hash. Leveled
                         % discards it (`preparefor_ledgercache' keeps only the
                         % key hash) and journal compaction decides currency by
                         % sequence number, so it stays zero: only the
                         % tictac/AAE folds read it and this plugin never runs
                         % them.
                         %
                         % The third element is user metadata. Holding the
                         % exchange and the payload size there lets
                         % `init_index_and_counters/0' rebuild the index and both
                         % counters from the ledger heads alone, without reading
                         % the journal.
                         {{0, Size,
                           {rabbit_delayed_message_counters:id(Exchange),
                            payload_size(Msg)}}, []}
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
    ok = rabbit_delayed_message_counters:setup(),
    start_db(),
    init_index_and_counters().

disable_plugin() ->
    catch ets:delete(?INDEX_TABLE),
    catch rabbit_delayed_message_counters:teardown(),
    leveled_bookie:book_destroy(?BOOKIE).

store_delay(DelayTS, Exchange, Message) ->
    ExNameBin = rabbit_delayed_message_counters:id(Exchange),
    Key = make_key(DelayTS),
    Bytes = payload_size(Message),
    ets:insert(?INDEX_TABLE, {{DelayTS, Key}, ExNameBin, Bytes}),
    case internal_put(Key, Exchange, Message) of
        ok    -> ok;
        pause -> int_store_pause()
    end,
    rabbit_delayed_message_counters:add(ExNameBin, Bytes).

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
                [{_, ExNameBin, Bytes}] ->
                    rabbit_delayed_message_counters:sub(ExNameBin, Bytes);
                [] -> ok
            end
    end.

delete_empty_key(IndexKey) ->
    ets:delete(?INDEX_TABLE, IndexKey).

make_key(DelayTS) ->
    <<DelayTS:64/big, (crypto:strong_rand_bytes(16))/binary>>.

%% The stored value is an `mc:state()'. Rebuilding the ledger from the journal
%% re-runs the metadata extraction over historical objects, which may predate the
%% current message format, so a failure yields zero instead of breaking the
%% rebuild or a publish.
payload_size(Message) ->
    try mc:size(Message) of
        {_MetadataSize, PayloadSize} -> PayloadSize
    catch
        _:_ -> 0
    end.

% --------------------------------------------
% Internal functions
% --------------------------------------------

%% Creates the index table and initialises per-exchange counters in a single
%% Leveled metadata fold. The extract_metadata override stored the exchange and
%% the payload size in the ledger head, so book_headfold can retrieve both
%% without touching the journal.
init_index_and_counters() ->
    ets:new(?INDEX_TABLE, [named_table, ordered_set, public]),
    FoldFun = fun(_B, <<DelayTS:64/big, _:128>> = Key, ProxyBin, Acc) ->
        %% The ledger head is wrapped in a proxy_object by book_headfold, and
        %% the UserMeta slot of the metadata tuple holds what was stored there.
        {proxy_object, {_Hash, _Size, UserMeta}, _, _} = binary_to_term(ProxyBin),
        {ExNameBin, Bytes} = decode_user_meta(UserMeta),
        ets:insert(?INDEX_TABLE, {{DelayTS, Key}, ExNameBin, Bytes}),
        maps:update_with(ExNameBin,
                         fun({C, B}) -> {C + 1, B + Bytes} end,
                         {1, Bytes},
                         Acc)
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
    maps:foreach(fun(ExNameBin, {Count, Bytes}) ->
                      rabbit_delayed_message_counters:init(ExNameBin, Count, Bytes)
              end, DelayedPerExchange).

%% Entries written before the byte counter was introduced hold only the exchange
%% in the user metadata slot, so they count as zero bytes until they expire.
decode_user_meta({ExNameBin, Bytes}) ->
    {ExNameBin, Bytes};
decode_user_meta(ExNameBin) when is_binary(ExNameBin) ->
    {ExNameBin, 0}.

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
