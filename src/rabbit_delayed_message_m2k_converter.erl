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
         delete_from_khepri/3]).

-record(?MODULE, {}).

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
    rabbit_delayed_message_leveled:start_db(),
    %% Signal the gen_server to switch to the leveled backend. The cast is
    %% non-blocking; is_enabled/1 in the handler uses blocking mode and waits
    %% until khepri_db is fully enabled (i.e. after all data has been copied)
    %% before calling setup() and resetting the timer.
    rabbit_delayed_message:await_khepri_and_setup(),
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
    ExNameBin = rabbit_delayed_message_leveled:exchange_to_counter_bin(Exchange),
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
