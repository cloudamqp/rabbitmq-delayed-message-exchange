%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_delayed_message_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_delayed_message.hrl").

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3,
         finish_copy_to_khepri/1,
         clear_data_in_khepri/1]).

-record(?MODULE, {}).

-define(BUCKET, <<"x-delayed-messages">>).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
init_copy_to_khepri(_StoreId, _MigrationId, Tables) ->
    ?LOG_DEBUG(
       "Mnesia->Leveled init: tables ~0p",
       [Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = filename:join([rabbit_khepri:dir(), "rabbit_delayed_message", "leveled"]),
    ok = filelib:ensure_path(Path),
    {ok, Bookie} = leveled_bookie:book_start([{root_path, Path}]),
    put({?MODULE, bookie}, Bookie),
    {ok, #?MODULE{}}.

-spec copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
copy_to_khepri(Table,
               #delay_entry{delay_key = #delay_key{timestamp = TS,
                                                   exchange  = Exchange},
                            delivery = Delivery,
                            ref      = Ref},
               State) ->
    ?LOG_DEBUG(
       "Mnesia->Leveled data copy: [~0p] ts: ~0p exchange: ~0p",
       [Table, TS, Exchange#exchange.name],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    %% Derive a deterministic Leveled key from the Mnesia record so that
    %% a retried migration does not produce duplicate entries.
    KeySuffix = crypto:hash(md5, term_to_binary({TS, Exchange, Ref})),
    Key = <<TS:64/big, KeySuffix/binary>>,
    Bookie = get({?MODULE, bookie}),
    ok = leveled_bookie:book_put(Bookie, ?BUCKET, Key,
                                 term_to_binary({Exchange, Delivery}), []),
    {ok, State};
copy_to_khepri(_Table, #delay_index{}, State) ->
    %% Index entries are rebuilt from the Leveled key-set on next startup.
    {ok, State};
copy_to_khepri(Table, Record, _State) ->
    ?LOG_DEBUG(
       "Mnesia->Leveled unexpected record: table ~0p record ~0p",
       [Table, Record],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {error, unexpected_record}.

-spec delete_from_khepri(Table, Key, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% Concurrent Mnesia deletions during migration are not propagated to the
%% Leveled store. Pending messages may be delivered at most once more after the
%% migration window closes.
delete_from_khepri(_Table, _Key, State) ->
    {ok, State}.

finish_copy_to_khepri(_State) ->
    %% Close the migration bookie so all data is flushed to disk before the
    %% gen_server opens its own bookie at the same path.
    ?LOG_DEBUG(
       "Mnesia->Leveled finish: closing migration bookie",
       [],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case get({?MODULE, bookie}) of
        undefined -> ok;
        Bookie    -> ok = leveled_bookie:book_close(Bookie)
    end.

clear_data_in_khepri(Table) ->
    ?LOG_DEBUG(
       "Mnesia->Leveled clear_data_in_khepri: table ~0p",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    ok.
