%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(config_schema_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     run_snippets
    ].

%%--------------------------------------------------------------------
%% Testsuite setup/teardown
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:run_setup_steps(Config),
    ok = make_schema_discoverable(Config1),
    rabbit_ct_config_schema:init_schemas(rabbitmq_delayed_message_exchange,
                                        Config1).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%--------------------------------------------------------------------
%% Testcases
%%--------------------------------------------------------------------

run_snippets(Config) ->
    rabbit_ct_config_schema:run_snippets(Config).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

%% Schema discovery looks up applications by the entries of the plugins
%% directories and reads priv/schema through code:priv_dir/1. Both derive
%% the application name from a directory name, and this repository's
%% directory is dash-named, so neither finds the plugin when the suite runs
%% from a source checkout. Expose the checkout under a directory named
%% after the application and point discovery at it.
make_schema_discoverable(Config) ->
    Root = filename:dirname(
             filename:dirname(code:which(rabbit_delayed_message))),
    PluginsDir = filename:join(?config(priv_dir, Config), "plugins"),
    ok = filelib:ensure_path(PluginsDir),
    AppDir = filename:join(
               PluginsDir, "rabbitmq_delayed_message_exchange-0.0.0"),
    ok = case file:make_symlink(Root, AppDir) of
             ok              -> ok;
             {error, eexist} -> ok;
             Error           -> Error
         end,
    true = code:add_patha(filename:join(AppDir, "ebin")),
    DepsDir = filename:dirname(code:lib_dir(rabbit_common)),
    true = os:putenv("RABBITMQ_PLUGINS_DIR", PluginsDir ++ ":" ++ DepsDir),
    ok.
