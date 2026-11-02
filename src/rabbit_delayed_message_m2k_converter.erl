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
         init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3]).

%% Called through RPC from the node running the Mnesia->Khepri migration.
-export([remote_init_to_khepri/1]).

-record(?MODULE, {}).

%% Entry point used by RabbitMQ's khepri_db migration. It only runs on the
%% node performing the data copy, and `Tables' only lists that node's
%% delayed-message tables: they are node-local. Every other node copies its
%% own tables into its own Leveled store, with change tracking, through
%% remote_init_to_khepri/1.
-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv} | {error, any()},
      Priv :: #?MODULE{}.
init_copy_to_khepri(StoreId, MigrationId, Tables) ->
    {ok, Priv} = init_copy_to_khepri(StoreId, MigrationId, Tables, no_fan_out),
    case copy_on_other_nodes(StoreId) of
        ok    -> {ok, Priv};
        Error -> Error
    end.

%% Used for the copy of a single node's tables started by
%% remote_init_to_khepri/1. `no_fan_out' prevents that copy from
%% fanning out to the other nodes again.
-spec init_copy_to_khepri(StoreId, MigrationId, Tables, no_fan_out) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
init_copy_to_khepri(_StoreId, _MigrationId, Tables, no_fan_out) ->
    ?LOG_DEBUG(
       "Mnesia->Leveled init: tables ~0p",
       [Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    rabbit_delayed_message_leveled:start_db(),
    %% Signal the gen_server to switch to the leveled backend. The cast is
    %% non-blocking; is_enabled/1 in the handler uses blocking mode and waits
    %% until khepri_db is fully enabled (i.e. after all data has been copied)
    %% before calling setup() and resetting the timer.
    rabbit_delayed_message:await_khepri_and_setup(),
    {ok, #?MODULE{}}.

-spec remote_init_to_khepri(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: ok | {error, any()}.
remote_init_to_khepri(StoreId) ->
    Tables = rabbit_delayed_message_mnesia:table_names(),
    MigrationId = <<"rabbit_delayed_message_",
                    (atom_to_binary(node()))/binary>>,
    ?LOG_INFO(
       "Mnesia->Leveled data copy: copying local tables ~0p",
       [Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    mnesia_to_khepri:copy_tables(StoreId, MigrationId, Tables,
                                 {?MODULE, no_fan_out}).

copy_on_other_nodes(StoreId) ->
    case mnesia:system_info(db_nodes) -- [node()] of
        [] ->
            ok;
        Nodes ->
            ?LOG_INFO(
               "Mnesia->Leveled data copy: copying delayed messages on "
               "nodes ~0p",
               [Nodes],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            Rets = erpc:multicall(Nodes, ?MODULE, remote_init_to_khepri,
                                  [StoreId], infinity),
            case [{Node, Ret}
                  || {Node, Ret} <- lists:zip(Nodes, Rets),
                     Ret =/= {ok, ok}] of
                [] ->
                    ok;
                Failures ->
                    ?LOG_ERROR(
                       "Mnesia->Leveled data copy: failed on nodes: ~p",
                       [Failures],
                       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                    {error, {delayed_message_copy_failed, Failures}}
            end
    end.

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
    case rabbit_delayed_message_leveled:internal_put(Key, Exchange, Delivery) of
        ok    -> ok;
        pause -> int_migration_pause()
    end,
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

%% Named for debugging: trace this function to observe backpressure events
%% during migration.
int_migration_pause() ->
    rabbit_log:warning("Mnesia->Leveled migration pausing due to backpressure from Leveled Bookie",
                       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    timer:sleep(1000).
