%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_prometheus).

-behaviour(prometheus_collector).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register_collector/0,
         deregister_collector/0,
         ensure_registered/0]).

-export([deregister_cleanup/1,
         collect_mf/2]).

-export([per_exchange_delayed_messages/1]).

-type labels() :: [{atom(), atom() | binary()}].

-define(REGISTRY, 'detailed').

-define(METRIC_NAME_PREFIX, "rabbitmq_detailed_").

-define(METRIC_FAMILY_DELAYED_MESSAGES, delayed_messages_by_exchange).

-define(METRIC_FAMILIES, [
    {?METRIC_FAMILY_DELAYED_MESSAGES, gauge}
]).

-define(EXCHANGE_TYPE, 'x-delayed-message').

register_collector() ->
    {ok, _} = application:ensure_all_started(prometheus),
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
    MFamilies = requested_families(),
    VHostsFilter = vhosts_filter_from_pdict(),
    [do_collect_mf(Family, VHostsFilter, Callback, Type)
     || {Family, Type} <- ?METRIC_FAMILIES,
        lists:member(Family, MFamilies)],
    ok.

do_collect_mf(Family, VHostsFilter, Callback, Type = gauge) ->
    _ = lists:foreach(
          fun({Name, Values}) ->
            Callback(
                prometheus_model_helpers:create_mf(
                    <<?METRIC_NAME_PREFIX, (atom_to_binary(Name, utf8))/binary>>,
                    help_for_metric_name(Name),
                    Type,
                    Values))
          end,
          prometheus_format(Family, VHostsFilter)
         ).


%% ===================================================================
%% Private functions
%% ===================================================================

%% Retrieves URL query param values from the process dictionary
-spec requested_families() -> [atom()].
requested_families() ->
    case get(prometheus_mf_filter) of
        undefined ->
            [];
        MFamilies ->
            MFamilies
    end.

%% Adapted from `prometheus_rabbitmq_core_metrics_collector:vhosts_filter_from_pdict/1'.
-spec vhosts_filter_from_pdict() -> ordsets:ordset(binary()) | all.
vhosts_filter_from_pdict() ->
    case get(prometheus_vhost_filter) of
        undefined ->
            all;
        L ->
            ordsets:from_list(L)
    end.

-spec prometheus_format(MetricFamily, VHostsFilter) -> Resp when
      MetricFamily :: atom(),
      VHostsFilter :: ordsets:ordset(binary()) | all,
      Resp :: [{atom(), [{labels(), integer()}]}].
prometheus_format(?METRIC_FAMILY_DELAYED_MESSAGES, VHostsFilter) ->
    [{delayed_messages, per_exchange_delayed_messages(VHostsFilter)}].

%% Delayed messages are stored by the node that accepted the publish, so the
%% counts reported here only cover this node.
-spec per_exchange_delayed_messages(VHostsFilter) -> Resp when
      VHostsFilter :: ordsets:ordset(binary()) | all,
      Resp :: [{labels(), integer()}].
per_exchange_delayed_messages(VHostsFilter) ->
    lists:foldl(
      fun(#resource{virtual_host = VHost} = XName, Acc) ->
              case is_vhost_enabled(VHost, VHostsFilter) of
                  true ->
                      delayed_messages(XName, VHost, Acc);
                  false ->
                      Acc
              end
      end,
      [],
      rabbit_exchange:list_names()).

delayed_messages(#resource{name = Name} = XName, VHost, Acc) ->
    case rabbit_exchange:lookup(XName) of
        {ok, #exchange{type = ?EXCHANGE_TYPE} = X} ->
            Count = rabbit_delayed_message:messages_delayed(X),
            [{[{vhost, VHost}, {exchange, Name}], Count} | Acc];
        _ ->
            Acc
    end.

is_vhost_enabled(_VHost, all) ->
    true;
is_vhost_enabled(VHost, VHostsFilter) ->
    ordsets:is_element(VHost, VHostsFilter).

help_for_metric_name(delayed_messages) ->
    "Number of messages delayed by an x-delayed-message exchange on this node".
