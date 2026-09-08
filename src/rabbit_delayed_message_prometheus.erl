%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_prometheus).

-behaviour(prometheus_collector).

-export([register_collector/0,
         deregister_collector/0,
         ensure_registered/0]).

-export([deregister_cleanup/1,
         collect_mf/2]).

-define(REGISTRY, 'detailed').

-define(METRIC_NAME_PREFIX, "rabbitmq_detailed_").

-define(METRIC_FAMILY, delayed_exchange_metrics).

register_collector() ->
    ok = prometheus_registry:register_collector(?REGISTRY, ?MODULE).

deregister_collector() ->
    ok = prometheus_registry:deregister_collector(?REGISTRY, ?MODULE).

%% Registration happens when this plugin starts, so enabling rabbitmq_prometheus
%% afterwards leaves the collector unregistered. Calling this (for example with
%% `rabbitmqctl eval') registers it without restarting the plugin.
ensure_registered() ->
    case lists:member(?MODULE, prometheus_registry:collectors(?REGISTRY)) of
        false ->
            register_collector();
        true ->
            ok
    end.

%%====================================================================
%% Collector behaviour callbacks
%%====================================================================

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    case lists:member(?METRIC_FAMILY, requested_families()) of
        true ->
            collect_delayed_exchange_metrics(Callback);
        false ->
            ok
    end.

%% ===================================================================
%% Private functions
%% ===================================================================

%% Delayed messages are stored by the node that accepted the publish, so the
%% counters reported here only cover this node.
collect_delayed_exchange_metrics(Callback) ->
    maps:foreach(
      fun(Name, #{type := Type, help := Help, values := Values}) ->
              Callback(
                prometheus_model_helpers:create_mf(
                  <<?METRIC_NAME_PREFIX,
                    (prometheus_model_helpers:metric_name(Name))/binary>>,
                  Help,
                  Type,
                  Values))
      end,
      rabbit_delayed_message_counters:format(vhosts_filter_from_pdict())).

%% Retrieves URL query param values from the process dictionary
-spec requested_families() -> [atom()].
requested_families() ->
    case get(prometheus_mf_filter) of
        undefined ->
            [];
        MFamilies ->
            MFamilies
    end.

%% Adapted from `prometheus_rabbitmq_raft_metrics_collector:collect_detailed_metrics/2'.
-spec vhosts_filter_from_pdict() -> fun((seshat:labels_map()) -> boolean()).
vhosts_filter_from_pdict() ->
    case get(prometheus_vhost_filter) of
        undefined ->
            fun(_) -> true end;
        VHosts ->
            fun(#{vhost := VHost}) -> lists:member(VHost, VHosts);
               (_) -> false
            end
    end.
