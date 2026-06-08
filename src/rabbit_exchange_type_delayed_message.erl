%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_delayed_message).

-rabbit_boot_step(
   {?MODULE,
    [{description, "exchange type x-delayed-message: registry"},
     {mfa,         {rabbit_registry, register,
                    [exchange, <<"x-delayed-message">>, ?MODULE]}},
     {cleanup, {rabbit_registry, unregister,
                [exchange, <<"x-delayed-message">>]}},
     {requires,    rabbit_registry},
     {enables,     recovery}]}).

-rabbit_boot_step(
   {rabbit_delayed_message_topic_trie_projection,
    [{description, "exchange type x-delayed-message: Khepri topic trie"},
     {mfa,         {?MODULE, register_topic_trie_projection, []}},
     {cleanup,     {?MODULE, unregister_topic_trie_projection, []}},
     {requires,    core_initialized},
     {enables,     recovery}]}).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-import(rabbit_misc, [table_lookup/2]).
-import(rabbit_delayed_message_utils, [get_delay/1]).

-export([description/0, serialise_events/0, route/3]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).
-export([register_topic_trie_projection/0,
         unregister_topic_trie_projection/0]).

-define(EXCHANGE(Ex), (exchange_module(Ex))).
-define(ERL_MAX_T, 4294967295). %% Max timer delay, per Erlang docs.

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"x-delayed-message">>},
     {description, <<"Delayed Message Exchange.">>}].

route(X = #exchange{name = Name},
      Message,
      Opts) ->
    case delay_message(X, Message) of
        nodelay ->
            %% route the message using proxy module
            case ?EXCHANGE(X) of
                rabbit_exchange_type_direct ->
                    RKs = mc:routing_keys(Message),
                    %% Exchange type x-delayed-message routes via "direct exchange routing v1"
                    %% even when feature flag direct_exchange_routing_v2 is enabled because
                    %% table rabbit_index_route only stores bindings whose source exchange
                    %% is of type direct exchange.
                    rabbit_router:match_routing_key(Name, RKs);
                rabbit_exchange_type_topic ->
                    %% Under Khepri, rabbit's topic trie projection filters
                    %% on type=topic and ignores our x-delayed-message
                    %% bindings, so we query our own projection.
                    %%
                    %% Notice that Opts are not passed in case of
                    %% Khepri. The `return_binding_keys' option is
                    %% only used for the MQTT 5 feature to return
                    %% Subscription Identifiers to the publishers. It
                    %% could only be returned to MQTT publishers when
                    %% not delaying messages. It is not applicable for
                    %% delayed messages (where upon publishing the
                    %% caller gets an empty list of targets, and when
                    %% the message expires and actual routing happens
                    %% the publisher is not available any more to
                    %% receive the collected binding keys). It does
                    %% not make sense to use a delayed exchange for
                    %% MQTT for non-delayed messages. Because of this
                    %% very niche, non-realistic use case the feature
                    %% is not supported by the delayed exchange.
                    case rabbit_khepri:is_enabled() of
                        true  -> topic_route(Name, Message);
                        false -> rabbit_exchange_type_topic:route(X, Message, Opts)
                    end;
                Mod ->
                    Mod:route(X, Message, Opts)
            end;
        _ ->
            []
    end.

topic_route(XName, Message) ->
    lists:flatmap(
      fun(RK) -> rabbit_delayed_message_topic_trie:match(XName, RK) end,
      mc:routing_keys(Message)).

validate(#exchange{arguments = Args} = X) ->
    case table_lookup(Args, <<"x-delayed-type">>) of
        {_ArgType, <<"x-delayed-message">>} ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "Invalid argument, "
                                       "'x-delayed-message' can't be used "
                                       "for 'x-delayed-type'",
                                       []);
        {_ArgType, Type} when is_binary(Type) ->
            rabbit_exchange:check_type(Type),
            ?EXCHANGE(X):validate(X);
        _ ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "Invalid argument, "
                                       "'x-delayed-type' must be "
                                       "an existing exchange type",
                                       [])
    end.

validate_binding(X, B) ->
    ?EXCHANGE(X):validate_binding(X, B).
create(Serial, X) ->
    ?EXCHANGE(X):create(Serial, X).
delete(Serial, X) ->
    ?EXCHANGE(X):delete(Serial, X).
policy_changed(X1, X2) ->
    ?EXCHANGE(X1):policy_changed(X1, X2).
add_binding(Serial, X, B) ->
    ?EXCHANGE(X):add_binding(Serial, X, B).
remove_bindings(Serial, X, Bs) ->
    ?EXCHANGE(X):remove_bindings(Serial, X, Bs).
assert_args_equivalence(X, Args) ->
    ?EXCHANGE(X):assert_args_equivalence(X, Args).
serialise_events() -> false.

info(Exchange) ->
    info(Exchange, [messages_delayed]).

info(Exchange, Items) ->
    case lists:member(messages_delayed, Items) of
        false -> [];
        true  ->
            [{messages_delayed,
              rabbit_delayed_message:messages_delayed(Exchange)}]
    end.


%%----------------------------------------------------------------------------

delay_message(Exchange, Message) ->
    case get_delay(Message) of
        {ok, Delay} when Delay > 0, Delay =< ?ERL_MAX_T ->
            rabbit_delayed_message:delay_message(Exchange, Message, Delay);
        _ ->
            nodelay
    end.

%% assumes the type is set in the args and that validate/1 did its job
exchange_module(Ex) ->
    T = rabbit_registry:binary_to_type(exchange_type(Ex)),
    {ok, M} = rabbit_registry:lookup_module(exchange, T),
    M.

exchange_type(#exchange{arguments = Args}) ->
    case table_lookup(Args, <<"x-delayed-type">>) of
        {_ArgType, Type} -> Type;
        _ -> error
    end.

%% Called as a boot step. Registers the Khepri topic trie projection
%% used to route topic-mode delayed messages. Registered unconditionally:
%% Khepri is set up by the broker regardless of whether the khepri_db
%% feature flag is enabled, so the projection attaches to the local
%% store and simply sees no events until khepri_db is turned on. This
%% handles the case where khepri_db is enabled at runtime, after the
%% plugin's boot step has already run.
-spec register_topic_trie_projection() -> ok.
register_topic_trie_projection() ->
    rabbit_delayed_message_topic_trie:register_projection().

-spec unregister_topic_trie_projection() -> ok.
unregister_topic_trie_projection() ->
    rabbit_delayed_message_topic_trie:unregister_projection().
