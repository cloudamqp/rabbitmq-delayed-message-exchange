%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_app).

-include_lib("kernel/include/logger.hrl").

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    case rabbit_delayed_message_sup:start_link() of
        {ok, Pid} ->
            ok = maybe_register_prometheus_collector(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    _ = catch rabbit_delayed_message_prometheus:deregister_collector(),
    ok.

%% Registers collector only when the prometheus application is available.
maybe_register_prometheus_collector() ->
    try
        ok = rabbit_delayed_message_prometheus:register_collector()
    catch
        Class:Reason ->
            ?LOG_INFO("Delayed message exchange: Prometheus metrics are not "
                      "exported, the prometheus application is unavailable "
                      "(~ts:~tp)", [Class, Reason]),
            ok
    end.
