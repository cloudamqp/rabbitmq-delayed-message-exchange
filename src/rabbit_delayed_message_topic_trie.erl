%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Khepri topic trie projection scoped to x-delayed-message exchanges.
%%
%% Rabbit's built-in topic trie projection (rabbit_khepri_topic_trie_v3)
%% filters on #exchange{type = topic}, so delayed-message exchanges with
%% x-delayed-type=topic are invisible to it and topic routing breaks.
%%
%% This module registers a parallel projection keyed on exchanges of type
%% x-delayed-message. The trie is updated atomically with the user's
%% binding writes (both happen in the same Khepri Ra apply), eliminating
%% the two-phase commit window the shadow-exchange approach has.
%%
%% The projection fun, follow_down_update/* and trie match walk are
%% copied from rabbit_khepri:register_rabbit_topic_graph_projection/0 and
%% rabbit_db_topic_exchange:trie_match_in_khepri/5, parametrized on the
%% ETS table name. Keep in sync with upstream until those are exported.

-module(rabbit_delayed_message_topic_trie).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([register_projection/0,
         unregister_projection/0,
         match/2]).

-define(TABLE, rabbit_delayed_message_topic_trie).

%%----------------------------------------------------------------------------
%% Projection registration
%%----------------------------------------------------------------------------

-spec register_projection() -> ok.
register_projection() ->
    Projection = khepri_projection:new(
                   ?TABLE, projection_fun(), projection_options()),
    PathPattern = rabbit_db_binding:khepri_route_path(
                    ?KHEPRI_WILDCARD_STAR,
                    #if_data_matches{
                       pattern = #exchange{type = 'x-delayed-message',
                                           _ = '_'}},
                    ?KHEPRI_WILDCARD_STAR,
                    ?KHEPRI_WILDCARD_STAR,
                    ?KHEPRI_WILDCARD_STAR),
    case khepri:register_projection(
           rabbit_khepri:get_store_id(), PathPattern, Projection) of
        ok                                                  -> ok;
        {error, exists}                                     -> ok;
        {error, {khepri, projection_already_exists, _Info}} -> ok
    end.

-spec unregister_projection() -> ok.
unregister_projection() ->
    _ = khepri:unregister_projections(rabbit_khepri:get_store_id(), [?TABLE]),
    ok.

projection_options() ->
    ShouldProcessFun =
        fun (rabbit_db_topic_exchange, split_topic_key_binary, 1, _From) ->
                false;
            (erlang, make_ref, 0, _From) ->
                false;
            (ets, _F, _A, _From) ->
                false;
            (M, F, A, From) ->
                khepri_tx_adv:should_process_function(M, F, A, From)
        end,
    #{keypos => #topic_trie_edge_v2.trie_edge,
      standalone_fun_options =>
          #{should_process_function => ShouldProcessFun},
      read_concurrency => true}.

projection_fun() ->
    fun(Table, Path, OldProps, NewProps) ->
        {VHost, ExchangeName, _Kind, _DstName, RoutingKey} =
            rabbit_db_binding:khepri_route_path_to_args(Path),
        Exchange = rabbit_misc:r(VHost, exchange, ExchangeName),
        Words = rabbit_db_topic_exchange:split_topic_key_binary(RoutingKey),
        case {OldProps, NewProps} of
            {#{data := OldBindings}, #{data := NewBindings}} ->
                ToInsert = sets:subtract(NewBindings, OldBindings),
                ToDelete = sets:subtract(OldBindings, NewBindings),
                follow_down_update(
                  Table, Exchange, Words,
                  fun(Existing) ->
                      sets:union(sets:subtract(Existing, ToDelete), ToInsert)
                  end);
            {_, #{data := NewBindings}} ->
                follow_down_update(
                  Table, Exchange, Words,
                  fun(Existing) -> sets:union(Existing, NewBindings) end);
            {#{data := OldBindings}, _} ->
                %% Skip updates for bindings we never trie-indexed (e.g.
                %% leftover bindings from before the projection was
                %% registered, or cascaded deletes that arrive after the
                %% binding was already removed).
                case has_trie_entry(Table, Exchange, Words) of
                    true ->
                        follow_down_update(
                          Table, Exchange, Words,
                          fun(Existing) ->
                              sets:subtract(Existing, OldBindings)
                          end);
                    false ->
                        ok
                end;
            {_, _} ->
                ok
        end
    end.

has_trie_entry(Table, Exchange, Words) ->
    has_trie_entry(Table, Exchange, root, Words).

has_trie_entry(Table, Exchange, NodeId, []) ->
    Key = #trie_edge{exchange_name = Exchange,
                     node_id = NodeId,
                     word = bindings},
    ets:lookup(Table, Key) =/= [];
has_trie_entry(Table, Exchange, NodeId, [W | Rest]) ->
    Key = #trie_edge{exchange_name = Exchange,
                     node_id = NodeId,
                     word = W},
    case ets:lookup(Table, Key) of
        [#topic_trie_edge_v2{node_id = Next}] ->
            has_trie_entry(Table, Exchange, Next, Rest);
        [] ->
            false
    end.

%%----------------------------------------------------------------------------
%% follow_down_update - copied from rabbit_khepri.erl (private).
%%----------------------------------------------------------------------------

follow_down_update(Table, Exchange, Words, UpdateFn) ->
    follow_down_update(Table, Exchange, root, Words, UpdateFn),
    ok.

follow_down_update(Table, Exchange, FromNodeId, [To | Rest], UpdateFn) ->
    TrieEdge = #trie_edge{exchange_name = Exchange,
                          node_id = FromNodeId,
                          word = To},
    {ToNodeId, IsNew} =
        case ets:lookup(Table, TrieEdge) of
            [#topic_trie_edge_v2{node_id = ExistingId}] ->
                {ExistingId, false};
            [] ->
                NewNodeId = make_ref(),
                NewEdge = #topic_trie_edge_v2{trie_edge = TrieEdge,
                                              node_id = NewNodeId,
                                              child_count = 0},
                ets:insert(Table, NewEdge),
                {NewNodeId, true}
        end,
    case follow_down_update(Table, Exchange, ToNodeId, Rest, UpdateFn) of
        added ->
            _ = ets:update_counter(
                  Table, TrieEdge, {#topic_trie_edge_v2.child_count, 1}),
            case IsNew of true -> added; false -> kept end;
        kept ->
            false = IsNew,
            kept;
        deleted ->
            false = IsNew,
            NewCount = ets:update_counter(
                         Table, TrieEdge,
                         {#topic_trie_edge_v2.child_count, -1}),
            if
                NewCount > 0 -> kept;
                NewCount =:= 0 ->
                    ets:delete(Table, TrieEdge),
                    deleted
            end
    end;
follow_down_update(Table, Exchange, LeafNodeId, [], UpdateFn) ->
    TrieEdge = #trie_edge{exchange_name = Exchange,
                          node_id = LeafNodeId,
                          word = bindings},
    {Bindings, IsNew} =
        case ets:lookup(Table, TrieEdge) of
            [#topic_trie_edge_v2{node_id = {bindings, Existing}}] ->
                {Existing, false};
            [] ->
                {sets:new([{version, 2}]), true}
        end,
    NewBindings = UpdateFn(Bindings),
    case sets:is_empty(NewBindings) of
        true ->
            ets:delete(Table, TrieEdge),
            deleted;
        false ->
            Edge = #topic_trie_edge_v2{
                     trie_edge = TrieEdge,
                     node_id = {bindings, NewBindings}},
            ets:insert(Table, Edge),
            case IsNew of true -> added; false -> kept end
    end.

%%----------------------------------------------------------------------------
%% match - adapted from rabbit_db_topic_exchange:trie_match_in_khepri.
%%----------------------------------------------------------------------------

-spec match(rabbit_exchange:name(), rabbit_types:routing_key()) ->
    [rabbit_types:binding_destination()].
match(XName, RoutingKey) ->
    Words = rabbit_db_topic_exchange:split_topic_key_binary(RoutingKey),
    trie_match(XName, root, Words, []).

trie_match(X, Node, [], Acc0) ->
    Dests = trie_bindings(X, Node),
    Acc = Dests ++ Acc0,
    trie_match_part(X, Node, <<"#">>, fun trie_match_skip_any/4, [], Acc);
trie_match(X, Node, [W | RestW] = Words, Acc) ->
    lists:foldl(
      fun ({WArg, MatchFun, RestWArg}, A) ->
              trie_match_part(X, Node, WArg, MatchFun, RestWArg, A)
      end, Acc,
      [{W, fun trie_match/4, RestW},
       {<<"*">>, fun trie_match/4, RestW},
       {<<"#">>, fun trie_match_skip_any/4, Words}]).

trie_match_part(X, Node, Search, MatchFun, RestW, Acc) ->
    case trie_child(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, Acc);
        error -> Acc
    end.

trie_match_skip_any(X, Node, [], Acc) ->
    trie_match(X, Node, [], Acc);
trie_match_skip_any(X, Node, [_ | RestW] = Words, Acc) ->
    trie_match_skip_any(X, Node, RestW,
                        trie_match(X, Node, Words, Acc)).

trie_child(X, Node, Word) ->
    case ets:lookup(?TABLE,
                    #trie_edge{exchange_name = X,
                               node_id = Node,
                               word = Word}) of
        [#topic_trie_edge_v2{node_id = NextNode}] -> {ok, NextNode};
        [] -> error
    end.

trie_bindings(X, Node) ->
    case ets:lookup(?TABLE,
                    #trie_edge{exchange_name = X,
                               node_id = Node,
                               word = bindings}) of
        [#topic_trie_edge_v2{node_id = {bindings, Bindings}}] ->
            [Dest || #binding{destination = Dest} <- sets:to_list(Bindings)];
        [] -> []
    end.
