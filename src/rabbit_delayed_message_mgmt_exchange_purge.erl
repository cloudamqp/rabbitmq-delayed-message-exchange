%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_mgmt_exchange_purge).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2, resource_exists/2, is_authorized/2, allowed_methods/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

dispatcher() ->
    [{"/exchanges/:vhost/:exchange/contents", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case exchange_resource(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

delete_resource(ReqData, Context) ->
    ExchangeName = exchange_resource(ReqData),
    Result =
        erpc:multicall(
          rabbit_nodes:list_running(), rabbit_delayed_message, purge_exchange, [ExchangeName]),
    case lists:any(fun({ok, _}) -> true; (_) -> false end, Result) of
        true ->
            {true, ReqData, Context};
        false ->
            Msg = "Failed to purge exchange",
            %% FIXME: error code
            rabbit_mgmt_util:bad_request(list_to_binary(Msg), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

exchange_resource(ReqData) ->
    case rabbit_mgmt_wm_exchange:exchange(ReqData) of
        not_found -> not_found;
        ExProps ->
            case proplists:get_value(type, ExProps) of
                'x-delayed-message' ->
                    rabbit_misc:r(proplists:get_value(vhost, ExProps),
                                  exchange,
                                  proplists:get_value(name, ExProps));
                _ ->
                    not_found
            end
    end.
