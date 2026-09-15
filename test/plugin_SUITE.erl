%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(plugin_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

all() ->
    [
      {group, leveled},
      {group, leveled_projection_v1}
    ].

groups() ->
    [
      {leveled, [], [{group, non_parallel_tests}, {group, fine_stats}, {group, leveled_only}]},
      {leveled_only, [], [reload_strategy_is_recovr,
                          bookie_opts_are_tunable,
                          fixed_bookie_opts_cannot_be_overridden,
                          tuned_bookie_starts,
                          journal_compaction_is_scheduled,
                          journal_compaction_interval_seconds_is_refreshed,
                          journal_compaction_on_live_store,
                          disable_cleanup]},
      {leveled_projection_v1, [], [topic_projection_v1_to_v2_migration]},
      {non_parallel_tests, [], [
                                wrong_exchange_argument_type,
                                exchange_argument_type_not_self,
                                routing_topic,
                                routing_topic_unbind_stops_routing,
                                routing_topic_empty_binding_key_isolated,
                                routing_direct,
                                routing_fanout,
                                e2e_nodelay,
                                e2e_delay,
                                delay_order,
                                delayed_messages_count,
                                counter_survives_restart,
                                node_restart_before_delay_expires,
                                node_restart_after_delay_expires,
                                string_delay_header
                               ]},
     {fine_stats, [], [
                       e2e_nodelay,
                       e2e_delay
                      ]}
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]).

end_per_suite(Config) ->
    Config.

init_per_group(leveled, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
        Config,
        [
            {metadata_store, khepri},
            {rmq_nodename_suffix, rabbit_delayed_message_utils:append_to_atom(?MODULE, "-leveled")},
            {tcp_ports_base, 21100}
        ]
    ),
    run_broker_and_clients(Config1);
init_per_group(leveled_projection_v1, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
        Config,
        [
            {metadata_store, khepri},
            {rmq_nodename_suffix, rabbit_delayed_message_utils:append_to_atom(?MODULE, "-proj-v1")},
            {tcp_ports_base, 21400}
        ]
    ),
    %% Boot with all stable feature flags except
    %% delayed_message_topic_projection_v2, so the node starts on the v1
    %% projection and the test can exercise the migration to v2.
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit,
                 [{forced_feature_flags_on_init,
                   {rel, [], [delayed_message_topic_projection_v2]}}]}),
    run_broker_and_clients(Config2);
init_per_group(fine_stats, Config) ->
    CollectStatsOrig = get_collect_stats(Config),
    set_collect_stats(Config, fine),
    refresh_config(Config),
    [{collect_statistics, fine}, {collect_statistics_orig, CollectStatsOrig} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(leveled, Config) ->
    teardown_broker_and_clients(Config);
end_per_group(leveled_projection_v1, Config) ->
    teardown_broker_and_clients(Config);
end_per_group(fine_stats, Config) ->
    CollectStatsOrig = rabbit_ct_helpers:get_config(Config, collect_statistics_orig),
    set_collect_stats(Config, CollectStatsOrig),
    refresh_config(Config),
    Config;
end_per_group(_, Config) ->
    Config.

run_broker_and_clients(Config) ->
    %% The suite declares transient non-exclusive queues, which are
    %% deprecated and denied by default since RabbitMQ 4.3.
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit, [{permit_deprecated_features,
                           #{transient_nonexcl_queues => true}}]}),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

teardown_broker_and_clients(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    TestCaseName = rabbit_ct_helpers:config_to_testcase_name(Config, Testcase),
    BaseName = re:replace(TestCaseName, "/", "-", [global,{return,list}]),
    Config1 = rabbit_ct_helpers:set_config(Config, {test_resource_name, BaseName}),
    reset_publish_out_stats(Config),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

wrong_exchange_argument_type(Config) ->
    Chan =  rabbit_ct_client_helpers:open_channel(Config),
    Ex = make_exchange_name(Config, "fail"),
    Type = <<"x-not-valid-type">>,
    process_flag(trap_exit, true),
    ?assertExit(_, amqp_channel:call(Chan, make_exchange(Ex, Type))),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

exchange_argument_type_not_self(Config) ->
    Chan =  rabbit_ct_client_helpers:open_channel(Config),
    Ex = make_exchange_name(Config, "1"),
    Type = <<"x-delayed-message">>,
    process_flag(trap_exit, true),
    ?assertExit(_, amqp_channel:call(Chan, make_exchange(Ex, Type))),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

routing_topic(Config) ->
    BKs = [<<"a.b.c">>, <<"a.*.c">>, <<"a.#">>],
    RKs = [<<"a.b.c">>, <<"a.z.c">>, <<"a.j.k">>, <<"b.b.c">>],
    %% all except <<"b.b.c">> should be routed.
    Count = 3,
    routing_test0(Config, BKs, RKs, <<"topic">>, Count).

%% Journal compaction can only drop superseded objects and tombstones
%% under the `recovr' reload strategy; leveled tests what that strategy
%% then does, this only pins down that the store asks for it.
reload_strategy_is_recovr(Config) ->
    Opts = rabbit_ct_broker_helpers:rpc(
             Config, 0, rabbit_delayed_message_leveled, start_opts,
             ["/tmp/unused"]),
    ?assertEqual([{delayed_msg, recovr}],
                 proplists:get_value(reload_strategy, Opts)),
    ok.

%% The compaction tunables reach the bookie as they are set. Their values
%% are the schema's business, not this module's.
bookie_opts_are_tunable(Config) ->
    Opts = with_bookie_opts(
             Config,
             [{max_run_length, 4},
              {journalcompaction_scoreonein, 4},
              {max_journalobjectcount, 50_000},
              {max_journalsize, 536_870_912},
              {waste_retention_period, 3600},
              {singlefile_compactionpercentage, 40.0},
              {maxrunlength_compactionpercentage, 80.0}]),
    ?assertEqual(4, proplists:get_value(max_run_length, Opts)),
    ?assertEqual(4, proplists:get_value(journalcompaction_scoreonein, Opts)),
    ?assertEqual(50_000, proplists:get_value(max_journalobjectcount, Opts)),
    ?assertEqual(536_870_912, proplists:get_value(max_journalsize, Opts)),
    ?assertEqual(3600, proplists:get_value(waste_retention_period, Opts)),
    ?assertEqual(40.0,
                 proplists:get_value(singlefile_compactionpercentage, Opts)),
    ?assertEqual(80.0,
                 proplists:get_value(maxrunlength_compactionpercentage, Opts)),
    ok.

%% The options the store depends on are not tunable, whatever the
%% application environment says.
fixed_bookie_opts_cannot_be_overridden(Config) ->
    Opts = with_bookie_opts(Config,
                            [{reload_strategy, [{delayed_msg, retain}]},
                             {compression_method, lz4},
                             {root_path, "/tmp/somewhere_else"}]),
    ?assertEqual([{delayed_msg, recovr}],
                 proplists:get_value(reload_strategy, Opts)),
    ?assertEqual(none, proplists:get_value(compression_method, Opts)),
    ?assertEqual("/tmp/unused", proplists:get_value(root_path, Opts)),
    ok.

%% A bookie tuned the way the schema allows starts and compacts, down to
%% the smallest journal roll point leveled accepts.
tuned_bookie_starts(Config) ->
    Tunables = [{max_journalobjectcount, 5},
                {journalcompaction_scoreonein, 2},
                {singlefile_compactionpercentage, 40.0},
                {maxrunlength_compactionpercentage, 80.0}],
    ok = rabbit_ct_broker_helpers:disable_plugin(
           Config, 0, rabbitmq_delayed_message_exchange),
    [ok = rabbit_ct_broker_helpers:rpc(
            Config, 0, application, set_env,
            [rabbitmq_delayed_message_exchange, Key, Value])
     || {Key, Value} <- Tunables],
    try
        ok = rabbit_ct_broker_helpers:enable_plugin(
               Config, 0, rabbitmq_delayed_message_exchange),
        Chan = rabbit_ct_client_helpers:open_channel(Config),
        Ex = make_exchange_name(Config, "1"),
        Q = make_queue_name(Config, "1"),
        setup_fabric(Chan, make_exchange(Ex, <<"topic">>), make_queue(Q),
                     <<"a.*.c">>),
        amqp_channel:call(Chan, #'confirm.select'{}),
        publish_messages(Chan, Ex, <<"a.b.c">>, [60000]),
        amqp_channel:wait_for_confirms_or_die(Chan),
        ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(
                           Config, 0, rabbit_delayed_message,
                           compact_journal, [])),
        amqp_channel:call(Chan, #'exchange.delete'{exchange = Ex}),
        amqp_channel:call(Chan, #'queue.delete'{queue = Q}),
        rabbit_ct_client_helpers:close_channel(Chan)
    after
        [ok = rabbit_ct_broker_helpers:rpc(
                Config, 0, application, unset_env,
                [rabbitmq_delayed_message_exchange, Key])
         || {Key, _Value} <- Tunables],
        %% Leave the following cases a store started with the defaults.
        ok = rabbit_ct_broker_helpers:disable_plugin(
               Config, 0, rabbitmq_delayed_message_exchange),
        ok = rabbit_ct_broker_helpers:enable_plugin(
               Config, 0, rabbitmq_delayed_message_exchange)
    end,
    ok.

%% Leveled never schedules journal compaction itself, so the plugin's
%% gen_server has to keep asking for it.
journal_compaction_is_scheduled(Config) ->
    ?assert(compaction_timer_remaining(Config) =< 600_000),
    ok.

%% A changed interval has to be picked up without a node restart, and
%% zero has to disable the periodic runs and then re-arm them.
journal_compaction_interval_seconds_is_refreshed(Config) ->
    try
        set_compaction_interval_seconds(Config, 60),
        ?assert(compaction_timer_remaining(Config) =< 60_000),
        set_compaction_interval_seconds(Config, 0),
        ?assertEqual(undefined, compaction_timer(Config))
    after
        unset_compaction_interval_seconds(Config)
    end,
    ?assert(compaction_timer_remaining(Config) > 60_000),
    ok.

%% A run has to reach the live store, whatever it then finds to reclaim.
journal_compaction_on_live_store(Config) ->
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(
                       Config, 0, rabbit_delayed_message,
                       compact_journal, [])),
    ok.

%% Regression: disabling the plugin must run the cleanup boot steps.
%% Two pieces of state need to be released:
%%   * the Khepri topic trie projection and its ETS tables
%%     (rabbit_delayed_message_topic_trie_v2 and
%%     rabbit_delayed_message_topic_binding_v2)
%%   * the Leveled delayed-message store (its in-memory key index ETS
%%     table and the on-disk bookie directory)
%% Re-enabling must restore both.
disable_cleanup(Config) ->
    %% CT nodes boot with all stable feature flags enabled, so the
    %% delayed_message_topic_projection_v2 tables are the ones in use.
    TrieTabs = [rabbit_delayed_message_topic_trie_v2,
                rabbit_delayed_message_topic_binding_v2],
    LeveledIndex = rabbit_delayed_message_leveled_key_index,
    EtsWhereis = fun(T) ->
        rabbit_ct_broker_helpers:rpc(Config, 0, ets, whereis, [T])
    end,
    JournalFiles = filename:join(
                     [rabbit_ct_broker_helpers:rpc(
                        Config, 0, rabbit_plugins,
                        user_provided_plugins_data_dir, []),
                      "rabbit_delayed_message", "leveled",
                      "journal", "journal_files"]),
    %% book_destroy/1 empties the journal and ledger dirs but leaves
    %% the empty parent directories on disk, so the test asserts on
    %% file count rather than directory existence.
    JournalFileCount = fun() ->
        case rabbit_ct_broker_helpers:rpc(
               Config, 0, file, list_dir, [JournalFiles]) of
            {ok, Files}     -> length(Files);
            {error, enoent} -> 0
        end
    end,

    Chan = rabbit_ct_client_helpers:open_channel(Config),
    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),
    setup_fabric(Chan, make_exchange(Ex, <<"topic">>), make_queue(Q), <<"a.*.c">>),

    %% Publish one delayed message so the Leveled store has data on
    %% disk and the index ETS table has a key.
    amqp_channel:call(Chan, #'confirm.select'{}),
    publish_messages(Chan, Ex, <<"a.b.c">>, [60000]),
    amqp_channel:wait_for_confirms_or_die(Chan),

    %% Pre-conditions: both the projection tables and the Leveled state
    %% (index ETS and on-disk journal files) exist.
    [?assertNotEqual(undefined, EtsWhereis(T)) || T <- TrieTabs],
    ?assertNotEqual(undefined, EtsWhereis(LeveledIndex)),
    ?assert(JournalFileCount() > 0),

    amqp_channel:call(Chan, #'exchange.delete'{exchange = Ex}),
    amqp_channel:call(Chan, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Chan),

    %% Disable: cleanup steps unregister the projection and tear down
    %% the Leveled store (the gen_server's terminate/2 calls
    %% rabbit_delayed_message_leveled:disable_plugin/0 which destroys
    %% the bookie's on-disk data).
    ok = rabbit_ct_broker_helpers:disable_plugin(
           Config, 0, rabbitmq_delayed_message_exchange),
    [?awaitMatch(undefined, EtsWhereis(T), 5000) || T <- TrieTabs],
    ?awaitMatch(undefined, EtsWhereis(LeveledIndex), 5000),
    ?awaitMatch(0, JournalFileCount(), 5000),

    %% Re-enable: both pieces of state come back. Registration is
    %% idempotent for the projection; the Leveled store is recreated
    %% fresh by setup/0.
    ok = rabbit_ct_broker_helpers:enable_plugin(
           Config, 0, rabbitmq_delayed_message_exchange),
    [?awaitMatch(T when T =/= undefined, EtsWhereis(Tab), 5000)
     || Tab <- TrieTabs],
    ?awaitMatch(T when T =/= undefined, EtsWhereis(LeveledIndex), 10000),
    ok.

%% Regression: removing a topic binding must also remove it from the
%% Khepri topic trie projection, so further publishes do not route.
routing_topic_unbind_stops_routing(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),
    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),
    BK = <<"a.*.c">>,
    setup_fabric(Chan, make_exchange(Ex, <<"topic">>), make_queue(Q), BK),
    amqp_channel:call(Chan, #'confirm.select'{}),

    %% Before unbind: message routes.
    publish_messages(Chan, Ex, <<"a.b.c">>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C1} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(1, C1),

    #'queue.purge_ok'{} =
        amqp_channel:call(Chan, #'queue.purge'{queue = Q}),
    #'queue.unbind_ok'{} =
        amqp_channel:call(Chan, #'queue.unbind'{queue = Q,
                                                exchange = Ex,
                                                routing_key = BK}),

    %% After unbind: no routing.
    publish_messages(Chan, Ex, <<"a.b.c">>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C2} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(0, C2),
    ok.

%% Regression for the equivalent of rabbitmq/rabbitmq-server#16271: with
%% exchange-scoped trie roots, an empty binding key on one exchange must
%% not capture messages published with an empty routing key to a
%% different exchange.
routing_topic_empty_binding_key_isolated(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),
    Ex1 = make_exchange_name(Config, "1"),
    Ex2 = make_exchange_name(Config, "2"),
    Q = make_queue_name(Config, "1"),
    setup_fabric(Chan, make_exchange(Ex1, <<"topic">>), make_queue(Q), <<>>),
    declare_exchange(Chan, make_exchange(Ex2, <<"topic">>)),
    amqp_channel:call(Chan, #'confirm.select'{}),

    %% An empty routing key published to the unbound exchange must not
    %% cross-route to the queue bound to the other exchange.
    publish_messages(Chan, Ex2, <<>>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C1} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(0, C1),

    %% The same publish to the bound exchange routes.
    publish_messages(Chan, Ex1, <<>>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C2} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(1, C2),

    %% Unbinding the empty key stops routing. Zero words means the root
    %% is also the leaf, so the trie GC has no edges to prune.
    #'queue.unbind_ok'{} =
        amqp_channel:call(Chan, #'queue.unbind'{queue = Q,
                                                exchange = Ex1,
                                                routing_key = <<>>}),
    publish_messages(Chan, Ex1, <<>>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C3} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(1, C3),

    amqp_channel:call(Chan, #'exchange.delete'{exchange = Ex1}),
    amqp_channel:call(Chan, #'exchange.delete'{exchange = Ex2}),
    amqp_channel:call(Chan, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

%% The node boots with the delayed_message_topic_projection_v2 feature
%% flag disabled (see init_per_group/2), so topic routing starts on the
%% v1 single-table projection. Enabling the flag at runtime must migrate
%% routing to the v2 tables via the enable/post_enable callbacks without
%% breaking bindings created before the migration.
topic_projection_v1_to_v2_migration(Config) ->
    FFlag = delayed_message_topic_projection_v2,
    TrieV1 = rabbit_delayed_message_topic_trie,
    TrieTabsV2 = [rabbit_delayed_message_topic_trie_v2,
                  rabbit_delayed_message_topic_binding_v2],
    EtsWhereis = fun(T) ->
        rabbit_ct_broker_helpers:rpc(Config, 0, ets, whereis, [T])
    end,

    %% Pre-conditions: only the v1 projection is registered.
    ?assertNot(rabbit_ct_broker_helpers:is_feature_flag_enabled(
                 Config, FFlag)),
    ?assertNotEqual(undefined, EtsWhereis(TrieV1)),
    [?assertEqual(undefined, EtsWhereis(T)) || T <- TrieTabsV2],

    Chan = rabbit_ct_client_helpers:open_channel(Config),
    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),
    BK = <<"a.*.c">>,
    setup_fabric(Chan, make_exchange(Ex, <<"topic">>), make_queue(Q), BK),
    amqp_channel:call(Chan, #'confirm.select'{}),

    %% Routing works on the v1 path.
    publish_messages(Chan, Ex, <<"a.b.c">>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C1} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(1, C1),

    %% Enabling the flag registers the v2 projection (backfilled from
    %% the Khepri store contents) and unregisters the v1 one.
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, FFlag),
    [?awaitMatch(T when T =/= undefined, EtsWhereis(Tab), 5000)
     || Tab <- TrieTabsV2],
    ?awaitMatch(undefined, EtsWhereis(TrieV1), 5000),

    %% The binding created before the migration still routes, now via
    %% the v2 tables.
    publish_messages(Chan, Ex, <<"a.z.c">>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C2} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(2, C2),

    %% Unbinding after the migration stops routing.
    #'queue.unbind_ok'{} =
        amqp_channel:call(Chan, #'queue.unbind'{queue = Q,
                                                exchange = Ex,
                                                routing_key = BK}),
    publish_messages(Chan, Ex, <<"a.b.c">>, [0]),
    amqp_channel:wait_for_confirms_or_die(Chan),
    #'queue.declare_ok'{message_count = C3} =
        amqp_channel:call(Chan, make_queue(Q)),
    ?assertEqual(2, C3),

    amqp_channel:call(Chan, #'exchange.delete'{exchange = Ex}),
    amqp_channel:call(Chan, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

routing_direct(Config) ->
    BKs = [<<"mykey">>],
    RKs = [<<"mykey">>, <<"noroute">>, <<"mykey">>],
    %% all except <<"noroute">> should be routed.
    Count = 2,
    routing_test0(Config, BKs, RKs, <<"direct">>, Count).

routing_fanout(Config) ->
    BKs = [<<"mykey">>, <<>>, <<"otherkey">>],
    RKs = [<<"mykey">>, <<"noroute">>, <<"mykey">>],
    %% all except <<"noroute">> should be routed.
    Count = 3,
    routing_test0(Config, BKs, RKs, <<"fanout">>, Count).

routing_test0(Config, BKs, RKs, ExType, Count) ->
    Chan =  rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),
    ct:pal("Exchange name: ~p ; Queue name: ~p ;", [Ex, Q]),

    [setup_fabric(Chan, make_exchange(Ex, ExType), make_queue(Q), BRK) ||
        BRK <- BKs],

    %% message delay will be 0, we are testing routing here
    Msgs = [0],

    amqp_channel:call(Chan, #'confirm.select'{}),

    [publish_messages(Chan, Ex, K, Msgs) ||
        K <- RKs],

    % ensure that the messages have been delivered to the queues
    % before asking for the message count
    amqp_channel:wait_for_confirms_or_die(Chan),

    #'queue.declare_ok'{message_count = MCount} =
        amqp_channel:call(Chan, make_queue(Q)),

    ?assertEqual(Count, MCount),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = Ex }),
    amqp_channel:call(Chan, #'queue.delete' { queue = Q }),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

e2e_nodelay(Config) ->
    %% message delay will be 0,
    %% we are testing e2e without delays
    e2e_test0(Config, [0]).

e2e_delay(Config) ->
    e2e_test0(Config, [500, 100, 300, 200, 100, 400]).

e2e_test0(Config, Msgs) ->
    Chan =  rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Ex2 = make_exchange_name(Config, "2"),
    Q = make_queue_name(Config, "1"),

    declare_exchange(Chan, make_exchange(Ex, <<"direct">>)),

    setup_fabric(Chan, make_exchange(Ex2, <<"direct">>), make_queue(Q)),

    #'exchange.bind_ok'{} =
        amqp_channel:call(Chan, #'exchange.bind' {
                                   source      = Ex,
                                   destination = Ex2
                                  }),

    [] = get_publish_out_stat(Config),

    publish_messages(Chan, Ex, Msgs),

    {ok, Result} = consume(Chan, Q, Msgs),
    Sorted = lists:sort(Msgs),
    ?assertEqual(Sorted, Result),

    PublishOutCount = length(Msgs),
    case rabbit_ct_helpers:get_config(Config, collect_statistics, none) of
        fine ->
            ?assertMatch([{_, PublishOutCount, _}], get_publish_out_stat(Config));
        _ ->
            ?assertMatch([], get_publish_out_stat(Config))
    end,

    amqp_channel:call(Chan, #'exchange.delete' { exchange = Ex }),
    amqp_channel:call(Chan, #'exchange.delete' { exchange = Ex2 }),
    amqp_channel:call(Chan, #'queue.delete' { queue = Q }),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

delay_order(Config) ->
    Chan =  rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),

    setup_fabric(Chan, make_exchange(Ex, <<"direct">>), make_queue(Q)),

    Msgs = [500, 100, 300, 200, 100, 400],

    publish_messages(Chan, Ex, Msgs),

    {ok, Result} = consume(Chan, Q, Msgs),
    Sorted = lists:sort(Msgs),
    ?assertEqual(Sorted, Result),

    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

delayed_messages_count(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),

    setup_fabric(Chan, make_exchange(Ex, <<"direct">>), make_queue(Q)),

    Msgs = [500, 200, 300, 200, 300, 400],

    publish_messages(Chan, Ex, Msgs),

    % Let messages schedule.
    timer:sleep(50),
    Exchanges = rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_exchange, info_all, [<<"/">>]),

    FilterEx =
        fun(X) ->
                {resource, <<"/">>, exchange, Ex} == proplists:get_value(name, X)
        end,

    [Exchange] = lists:filter(FilterEx, Exchanges),
    {messages_delayed, 6} = proplists:lookup(messages_delayed, Exchange),

    %% Set a policy for the exchange
    PolicyName = make_policy_name(Config, "1"),
    rabbit_ct_broker_helpers:set_policy(
      Config, 0, PolicyName, <<"^", Ex/binary>>, <<"exchanges">>, [{<<"alternate-exchange">>, <<"altex">>}]),

    %% Same message count returned for modified exchange
    Exchanges2 = rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_exchange, info_all, [<<"/">>]),

    [Exchange2] = lists:filter(FilterEx, Exchanges2),
    {messages_delayed, 6} = proplists:lookup(messages_delayed, Exchange2),

    consume(Chan, Q, Msgs),

    Exchanges3 = rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_exchange, info_all, [<<"/">>]),
    [Exchange3] = lists:filter(FilterEx, Exchanges3),
    {messages_delayed, 0} = proplists:lookup(messages_delayed, Exchange3),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

counter_survives_restart(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),

    setup_fabric(Chan, make_durable_exchange(Ex, <<"direct">>),
                 make_durable_queue(Q)),

    MsgCount = 5,
    Msgs = lists:duplicate(MsgCount, 30000),

    %% Publisher confirms ensure every message is persisted before the restart.
    amqp_channel:call(Chan, #'confirm.select'{}),
    publish_messages(Chan, Ex, Msgs),
    amqp_channel:wait_for_confirms_or_die(Chan),

    rabbit_ct_broker_helpers:restart_node(Config, 0),

    Chan2 = rabbit_ct_client_helpers:open_channel(Config),

    FilterEx = fun(X) ->
        {resource, <<"/">>, exchange, Ex} == proplists:get_value(name, X)
    end,
    Exchanges = rabbit_ct_broker_helpers:rpc(Config, 0,
                    rabbit_exchange, info_all, [<<"/">>]),
    [Exchange] = lists:filter(FilterEx, Exchanges),
    {messages_delayed, MsgCount} = proplists:lookup(messages_delayed, Exchange),

    {ok, _} = consume(Chan2, Q, Msgs),
    amqp_channel:call(Chan2, #'exchange.delete'{exchange = Ex}),
    amqp_channel:call(Chan2, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Chan2),
    ok.

node_restart_before_delay_expires(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),

    setup_fabric(Chan, make_durable_exchange(Ex, <<"direct">>),
                 make_durable_queue(Q)),

    %% Here, we suppose the node will be restarted before all messages
    %% are actually queued.
    Msgs = [5000, 10000, 3000, 2000, 15000, 1000, 4000],
    publish_messages(Chan, Ex, Msgs),
    rabbit_ct_broker_helpers:restart_node(Config, 0),

    Chan2 =  rabbit_ct_client_helpers:open_channel(Config),

    {ok, Result} = consume(Chan2, Q, Msgs),
    Sorted = lists:sort(Msgs),
    ?assertEqual(Sorted, Result),

    amqp_channel:call(Chan2, #'exchange.delete' { exchange = Ex }),
    amqp_channel:call(Chan2, #'queue.delete' { queue = Q }),

    rabbit_ct_client_helpers:close_channel(Chan2),

    ok.

node_restart_after_delay_expires(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    Ex = make_exchange_name(Config, "1"),
    Q = make_queue_name(Config, "1"),

    setup_fabric(Chan, make_durable_exchange(Ex, <<"direct">>),
                 make_durable_queue(Q)),

    Msgs = [5000, 1000, 3000, 2000, 1000, 4000],

    publish_messages(Chan, Ex, Msgs),

    timer:sleep(lists:max(Msgs) + 3000),
    rabbit_ct_broker_helpers:restart_node(Config, 0),

    Chan2 =  rabbit_ct_client_helpers:open_channel(Config),

    {ok, Result} = consume(Chan2, Q, Msgs),
    Sorted = lists:sort(Msgs),
    ?assertEqual(Sorted, Result),

    amqp_channel:call(Chan2, #'exchange.delete' { exchange = Ex }),
    amqp_channel:call(Chan2, #'queue.delete' { queue = Q }),

    rabbit_ct_client_helpers:close_channel(Chan2),

    ok.

string_delay_header(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    Ex = <<"e3">>,
    Q = <<"q1">>,

    setup_fabric(Chan, make_exchange(Ex, <<"direct">>), make_queue(Q)),

    Msgs = [500, 100, 300, 200, 100, 400],

    publish_messages(Chan, Ex, <<>>, Msgs, longstr),

    {ok, Result} = consume(Chan, Q, Msgs),
    Sorted = lists:sort(Msgs),
    ?assertEqual(Sorted, Result),

    ok.

setup_fabric(Chan, ExDeclare, QueueDeclare) ->
    setup_fabric(Chan, ExDeclare, QueueDeclare, <<>>).

setup_fabric(Chan,
             ExDeclare = #'exchange.declare'{exchange = Ex},
             QueueDeclare,
             RK) ->
    declare_exchange(Chan, ExDeclare),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, QueueDeclare),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {
                                   queue       = Q,
                                   exchange    = Ex,
                                   routing_key = RK
                                  }).

declare_exchange(Chan, ExDeclare) ->
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan, ExDeclare).

publish_messages(Chan, Ex, Msgs) ->
    publish_messages(Chan, Ex, <<>>, Msgs).

publish_messages(Chan, Ex, RK, Msgs) ->
    publish_messages(Chan, Ex, RK, Msgs, signedint).

publish_messages(Chan, Ex, RK, Msgs, HeaderType) ->
        [amqp_channel:call(Chan,
                           #'basic.publish'{exchange = Ex,
                                            routing_key = RK},
                           make_msg(HeaderType, V)) || V <- Msgs].

consume(Chan, Q, Msgs) ->
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Chan, #'basic.consume'{queue  = Q,
                                                      no_ack = true}, self()),
    collect(length(Msgs), lists:max(Msgs) + 3000).

collect(N, Timeout) ->
    collect(0, N, Timeout, []).

collect(N, N, _Timeout, Acc) ->
    {ok, lists:reverse(Acc)};
collect(Curr, N, Timeout, Acc) ->
    receive {#'basic.deliver'{},
             #amqp_msg{payload = Bin}} ->
            collect(Curr+1, N, Timeout, [binary_to_term(Bin) | Acc])
    after Timeout ->
            {error, {timeout, Acc}}
    end.

make_queue(Q) ->
    #'queue.declare' {
       queue       = Q
      }.

make_durable_queue(Q) ->
    QR = make_queue(Q),
    QR#'queue.declare'{
      durable     = true,
      auto_delete = false
     }.

make_exchange(Ex, Type) ->
    #'exchange.declare'{
       exchange    = Ex,
       type        = <<"x-delayed-message">>,
       arguments   = [{<<"x-delayed-type">>,
                       longstr, Type}]
      }.

make_durable_exchange(Ex, Type) ->
    ER = make_exchange(Ex, Type),
    ER#'exchange.declare'{
      durable     = true,
      auto_delete = false
     }.

make_msg(HeaderType, V) ->
    #amqp_msg{props = #'P_basic'{
                         delivery_mode = 2,
                         headers = make_h(HeaderType, V)},
              payload = term_to_binary(V)}.

make_h(V) ->
    make_h(signedint, V).

make_h(signedint, V) ->
    [{<<"x-delay">>, signedint, V}];
make_h(longstr, V) ->
    [{<<"x-delay">>, longstr, integer_to_binary(V)}].

tests(Module, Timeout) ->
    {foreach, fun() -> ok end,
     [{timeout, Timeout, fun () -> Module:F() end} ||
         {F, _Arity} <- proplists:get_value(exports, Module:module_info()),
         string:right(atom_to_list(F), 5) =:= "_test"]}.

make_exchange_name(Config, Suffix) ->
    B = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("x-" ++ B ++ "-" ++ Suffix).

make_queue_name(Config, Suffix) ->
    B = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("q-" ++ B ++ "-" ++ Suffix).

make_policy_name(Config, Suffix) ->
    B = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("p-" ++ B ++ "-" ++ Suffix).

get_publish_out_stat(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list, [channel_queue_exchange_metrics]).

reset_publish_out_stats(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ets, delete_all_objects, [channel_queue_exchange_metrics]).

get_collect_stats(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, application, get_env, [rabbit, collect_statistics, undefined]).

set_collect_stats(Config, undefined) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, unset_env, [rabbit, collect_statistics]);
set_collect_stats(Config, CollectStats) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env, [rabbit, collect_statistics, CollectStats]).

refresh_config(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_delayed_message, refresh_config, []).

set_compaction_interval_seconds(Config, Seconds) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbitmq_delayed_message_exchange,
            journal_compaction_interval_seconds, Seconds]),
    refresh_config(Config).

unset_compaction_interval_seconds(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, unset_env,
           [rabbitmq_delayed_message_exchange,
            journal_compaction_interval_seconds]),
    refresh_config(Config).

%% Returns start_opts/1 as it looks with the given bookie options set in
%% the plugin's application environment, leaving the environment as it was.
with_bookie_opts(Config, Opts) ->
    [ok = rabbit_ct_broker_helpers:rpc(
            Config, 0, application, set_env,
            [rabbitmq_delayed_message_exchange, Key, Value])
     || {Key, Value} <- Opts],
    try
        rabbit_ct_broker_helpers:rpc(
          Config, 0, rabbit_delayed_message_leveled, start_opts,
          ["/tmp/unused"])
    after
        [ok = rabbit_ct_broker_helpers:rpc(
                Config, 0, application, unset_env,
                [rabbitmq_delayed_message_exchange, Key])
         || {Key, _Value} <- Opts]
    end.

%% The state record is #state{timer, compaction_timer, stats_state}.
compaction_timer(Config) ->
    {state, _Timer, TRef, _StatsState} =
        rabbit_ct_broker_helpers:rpc(
          Config, 0, sys, get_state, [rabbit_delayed_message]),
    TRef.

compaction_timer_remaining(Config) ->
    TRef = compaction_timer(Config),
    ?assert(is_reference(TRef)),
    Remaining = rabbit_ct_broker_helpers:rpc(
                  Config, 0, erlang, read_timer, [TRef]),
    ?assert(is_integer(Remaining)),
    Remaining.
