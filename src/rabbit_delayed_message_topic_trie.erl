%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Khepri topic trie projections scoped to x-delayed-message exchanges.
%%
%% Rabbit's built-in topic trie projections filter on
%% #exchange{type = topic}, so delayed-message exchanges with
%% x-delayed-type=topic are invisible to them and topic routing breaks.
%% This module registers parallel projections keyed on exchanges of type
%% x-delayed-message. The trie is updated atomically with the user's
%% binding writes (both happen in the same Khepri Ra apply), eliminating
%% the two-phase commit window the shadow-exchange approach has.
%%
%% Two projection formats exist, switched by the
%% `delayed_message_topic_projection_v2' feature flag:
%%
%% * v1: a single ETS table storing sets:set(#binding{}) in trie leaf
%%   nodes. A copy of rabbit's v3 projection
%%   (rabbit_khepri:register_rabbit_topic_graph_projection/0) and match
%%   walk (rabbit_db_topic_exchange:trie_match_v3/3). Kept so that
%%   routing keeps working during rolling upgrades from older plugin
%%   versions; delete once the feature flag becomes required.
%%
%% * v2: one projection with two ETS tables, a trie edges table (set)
%%   plus a leaf bindings table (ordered_set). A copy of rabbit's v5
%%   projection (rabbit_khepri:register_rabbit_topic_trie_projection/1)
%%   and match walk (rabbit_db_topic_exchange:trie_match/7) introduced
%%   in RabbitMQ 4.3.x. Binding updates are O(log N) instead of O(N) in
%%   the number of bindings at a leaf, and trie roots are
%%   exchange-scoped, which keeps empty binding keys isolated per
%%   exchange.
%%
%% The copied code is parametrized on the ETS table names and stripped
%% of the `return_binding_keys' plumbing (deliberately unsupported by
%% this plugin, see rabbit_exchange_type_delayed_message:route/3). Keep
%% in sync with upstream until those functions are exported.

-module(rabbit_delayed_message_topic_trie).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([register_projection/0,
         unregister_projection/0,
         match/2]).
%% Feature flag callbacks.
-export([projection_v2_enable/1,
         projection_v2_post_enable/1]).

%% The v1 table name must not change: nodes running an older plugin
%% version route from this table during rolling upgrades.
-define(TABLE_V1, rabbit_delayed_message_topic_trie).
-define(TRIE_TABLE_V2, rabbit_delayed_message_topic_trie_v2).
-define(BINDING_TABLE_V2, rabbit_delayed_message_topic_binding_v2).

-rabbit_feature_flag(
   {delayed_message_topic_projection_v2,
    #{desc          => "Two-table Khepri topic trie projection for "
                       "x-delayed-message exchanges",
      stability     => stable,
      %% The v5 projection format this module copies. Implies all nodes
      %% run RabbitMQ >= 4.3.0, whose Khepri supports multi-table
      %% projections. Being a plugin flag, it can also only be enabled
      %% once all nodes run a plugin version that knows it.
      depends_on    => [topic_binding_projection_v5],
      callbacks     => #{enable      => {?MODULE, projection_v2_enable},
                         post_enable => {?MODULE, projection_v2_post_enable}}
     }}).

%%----------------------------------------------------------------------------
%% Projection registration
%%----------------------------------------------------------------------------

-spec register_projection() -> ok.
register_projection() ->
    case projection_v2_enabled() of
        true ->
            ok = register_projection_v2(),
            %% The v1 projection may still be registered in the store if
            %% the cluster ran an older plugin version before the feature
            %% flag was enabled.
            unregister_projection_v1();
        false ->
            ok = register_projection_v1()
    end.

-spec unregister_projection() -> ok.
unregister_projection() ->
    _ = khepri:unregister_projections(
          rabbit_khepri:get_store_id(), [?TABLE_V1, ?TRIE_TABLE_V2]),
    ok.

%% Feature flag `enable' callback. It runs while all nodes still route
%% via the v1 path, so it must only register the v2 projection; the v1
%% projection is unregistered in the `post_enable' callback once the
%% flag is enabled cluster-wide.
projection_v2_enable(#{feature_name := _}) ->
    register_projection_v2().

projection_v2_post_enable(#{feature_name := _}) ->
    unregister_projection_v1(),
    ok.

projection_v2_enabled() ->
    rabbit_feature_flags:is_enabled(delayed_message_topic_projection_v2,
                                    non_blocking) =:= true.

unregister_projection_v1() ->
    _ = khepri:unregister_projections(
          rabbit_khepri:get_store_id(), [?TABLE_V1]),
    ok.

path_pattern() ->
    rabbit_db_binding:khepri_route_path(
      ?KHEPRI_WILDCARD_STAR,
      #if_data_matches{
         pattern = #exchange{type = 'x-delayed-message',
                             _ = '_'}},
      ?KHEPRI_WILDCARD_STAR,
      ?KHEPRI_WILDCARD_STAR,
      ?KHEPRI_WILDCARD_STAR).

%% This projection calls some external functions which are disallowed by
%% Horus because they interact with global or random state. We explicitly
%% allow them here for performance reasons.
should_process_fun() ->
    fun (rabbit_db_topic_exchange, split_topic_key_binary, 1, _From) ->
            false;
        (erlang, make_ref, 0, _From) ->
            false;
        (ets, _F, _A, _From) ->
            false;
        (M, F, A, From) ->
            khepri_tx_adv:should_process_function(M, F, A, From)
    end.

register_projection_result(ok)                                     -> ok;
register_projection_result({error, exists})                        -> ok;
register_projection_result({error,
                            {khepri,
                             projection_already_exists, _Info}})   -> ok;
register_projection_result({error, _} = Error)                     -> Error.

%%----------------------------------------------------------------------------
%% v2 projection - copied from
%% rabbit_khepri:register_rabbit_topic_trie_projection/1 (v5 variant).
%%
%% Trie edges table (set) for fast trie navigation during routing:
%%   Row:   {{XSrc, ParentNodeId, Word}, ChildNodeId, ChildCount}
%%
%% Leaf bindings table (ordered_set) for collecting destinations:
%%   Row:   {{NodeId, BindingKey, Dest}}
%%
%% XSrc       = {VHost, ExchangeName} (binaries)
%% NodeId     = {root, XSrc} | reference()
%% Word       = binary() (a single topic segment, e.g. <<"foo">>, <<"*">>, <<"#">>)
%% ChildCount = non_neg_integer() (number of outgoing edges)
%% Dest       = #resource{}
%%----------------------------------------------------------------------------

register_projection_v2() ->
    Opts = #{tables => #{?TRIE_TABLE_V2 => #{type => set},
                         ?BINDING_TABLE_V2 => #{type => ordered_set}},
             keypos => 1,
             read_concurrency => true,
             standalone_fun_options =>
                 #{should_process_function => should_process_fun()}},
    PFun = fun(Tables, Path, OldProps, NewProps) ->
                   #{?TRIE_TABLE_V2 := TrieTab,
                     ?BINDING_TABLE_V2 := BindingTab} = Tables,
                   {VHost, ExchangeName, Kind, DstName, BindingKey} =
                       rabbit_db_binding:khepri_route_path_to_args(Path),
                   XSrc = {VHost, ExchangeName},
                   Dest = rabbit_misc:r(VHost, Kind, DstName),
                   Words = rabbit_db_topic_exchange:split_topic_key_binary(
                             BindingKey),
                   Root = {root, XSrc},
                   case {OldProps, NewProps} of
                       {_, #{data := _}} ->
                           LeafNodeId = trie_follow_down_create(
                                          TrieTab, XSrc, Root, Words),
                           ets:insert(BindingTab,
                                      {{LeafNodeId, BindingKey, Dest}});
                       {#{data := _}, _} ->
                           case trie_follow_down_get_path(
                                  TrieTab, XSrc, Root, Words) of
                               {ok, LeafNodeId, TriePath} ->
                                   ets:delete(BindingTab,
                                              {LeafNodeId, BindingKey, Dest}),
                                   trie_gc_path(TrieTab, BindingTab, TriePath);
                               error ->
                                   ok
                           end;
                       {_, _} ->
                           ok
                   end
           end,
    Projection = khepri_projection:new(?TRIE_TABLE_V2, PFun, Opts),
    register_projection_result(
      khepri:register_projection(
        rabbit_khepri:get_store_id(), path_pattern(), Projection)).

%% Walk down the trie following the given words, creating edges and
%% intermediate nodes as needed. Returns the leaf node ID.
%%
%% Each trie row is a 3-tuple: {Key, ChildNodeId, ChildCount}.
%% ChildCount tracks the number of outgoing edges from ChildNodeId.
%% It is incremented when a new edge is created, decremented during GC.
trie_follow_down_create(TrieTab, XSrc, Root, Words) ->
    trie_follow_down_create(TrieTab, XSrc, Root, none, Words).

trie_follow_down_create(_TrieTab, _XSrc, NodeId, _ParentKey, []) ->
    NodeId;
trie_follow_down_create(TrieTab, XSrc, ParentId, ParentKey, [Word | Rest]) ->
    Key = {XSrc, ParentId, Word},
    case ets:lookup_element(TrieTab, Key, 2, undefined) of
        undefined ->
            NewId = make_ref(),
            ets:insert(TrieTab, {Key, NewId, 0}),
            _ = case ParentKey of
                    none ->
                        ok;
                    _ ->
                        ets:update_counter(TrieTab, ParentKey, {3, 1})
                end,
            trie_follow_down_create(TrieTab, XSrc, NewId, Key, Rest);
        ChildId ->
            trie_follow_down_create(TrieTab, XSrc, ChildId, Key, Rest)
    end.

%% Walk down the trie following the given words, collecting the path
%% for later GC. Returns {ok, LeafNodeId, Path} or error.
trie_follow_down_get_path(TrieTab, XSrc, Root, Words) ->
    trie_follow_down_get_path(TrieTab, XSrc, Root, none, Words, []).

trie_follow_down_get_path(_TrieTab, _XSrc, NodeId, _ParentKey, [], Path) ->
    {ok, NodeId, Path};
trie_follow_down_get_path(TrieTab, XSrc, ParentId, ParentKey,
                          [Word | Rest], Path) ->
    Key = {XSrc, ParentId, Word},
    case ets:lookup_element(TrieTab, Key, 2, undefined) of
        undefined ->
            error;
        ChildId ->
            trie_follow_down_get_path(TrieTab, XSrc, ChildId, Key, Rest,
                                      [{Key, ParentKey, ChildId} | Path])
    end.

%% Walk the path bottom-up (path is already in leaf-to-root order).
%% At each level, if the child node has no outgoing edges and no bindings,
%% delete the edge, decrement the parent's count, and continue upward.
trie_gc_path(_TrieTab, _BindingTab, []) ->
    ok;
trie_gc_path(TrieTab, BindingTab, [{Key, ParentEdgeKey, ChildId} | Rest]) ->
    case trie_node_is_empty(TrieTab, BindingTab, Key, ChildId) of
        true ->
            ets:delete(TrieTab, Key),
            _ = case ParentEdgeKey of
                    none ->
                        ok;
                    _ ->
                        ets:update_counter(TrieTab, ParentEdgeKey, {3, -1})
                end,
            trie_gc_path(TrieTab, BindingTab, Rest);
        false ->
            ok
    end.

%% A trie node is empty when it has no outgoing edges (ChildCount = 0) and
%% no bindings in the bindings table.
trie_node_is_empty(TrieTab, BindingTab, Key, ChildId) ->
    case ets:lookup_element(TrieTab, Key, 3, 0) of
        0 -> not trie_node_has_bindings(BindingTab, ChildId);
        _ -> false
    end.

trie_node_has_bindings(BindingTab, NodeId) ->
    case ets:next(BindingTab, {NodeId, <<>>, {}}) of
        {NodeId, _, _} -> true;
        _ -> false
    end.

%%----------------------------------------------------------------------------
%% match
%%----------------------------------------------------------------------------

-spec match(rabbit_exchange:name(), rabbit_types:routing_key()) ->
    [rabbit_types:binding_destination()].
match(XName, RoutingKey) ->
    Words = rabbit_db_topic_exchange:split_topic_key_binary(RoutingKey),
    case projection_v2_enabled() of
        true  -> match_v2(XName, Words);
        false -> trie_match_v1(XName, root, Words, [])
    end.

match_v2(#resource{virtual_host = VHost, name = Name}, Words) ->
    XSrc = {VHost, Name},
    Root = {root, XSrc},
    try
        trie_match(XSrc, Root, Words, [])
    catch
        %% The projection tables may not exist yet, e.g. on a freshly
        %% booted node before the boot step registered the projection.
        error:badarg ->
            []
    end.

%%----------------------------------------------------------------------------
%% v2 match walk - copied from rabbit_db_topic_exchange:trie_match/7.
%%
%% Routing walks the trie (branching on literal word, <<"*">>, <<"#">>)
%% then collects destinations from the bindings table at each matching
%% leaf. This is O(depth * 3) for the trie walk, plus O(log N) per
%% leaf for fanout 0-2, or O(log N + F) per leaf for fanout F > 2.
%%----------------------------------------------------------------------------

trie_match(XSrc, Node, [], Acc0) ->
    Acc1 = trie_bindings(Node, Acc0),
    trie_match_try(XSrc, Node, <<"#">>, fun trie_match_skip_any/4,
                   [], Acc1);
trie_match(XSrc, Node, [W | RestW] = Words, Acc0) ->
    Acc1 = trie_match_try(XSrc, Node, W, fun trie_match/4,
                          RestW, Acc0),
    Acc2 = trie_match_try(XSrc, Node, <<"*">>, fun trie_match/4,
                          RestW, Acc1),
    trie_match_try(XSrc, Node, <<"#">>, fun trie_match_skip_any/4,
                   Words, Acc2).

trie_match_try(XSrc, Node, Word, MatchFun, RestW, Acc) ->
    case ets:lookup_element(?TRIE_TABLE_V2, {XSrc, Node, Word}, 2,
                            undefined) of
        undefined ->
            Acc;
        NextNode ->
            MatchFun(XSrc, NextNode, RestW, Acc)
    end.

trie_match_skip_any(XSrc, Node, [], Acc) ->
    trie_match(XSrc, Node, [], Acc);
trie_match_skip_any(XSrc, Node, [_ | RestW] = Words, Acc) ->
    trie_match_skip_any(XSrc, Node, RestW,
                        trie_match(XSrc, Node, Words, Acc)).

%% Collect all destinations bound at the given trie node.
%%
%% Uses ets:next/2 for up to two elements (fast path for the common
%% fanout 0-2 cases), then switches to ets:select/2 when fanout > 2.
%%
%% ets:select/2 occurs the expensive match spec compilation overhead.
%% For larger fanouts, the cost for compiling the match spec amortises.
%% ets:select/2 occurs an O(log N) seek followed by an O(F) range scan,
%% which is cheaper than F individual ets:next/2 calls
%% (each O(log N) due to CATree fresh-stack allocation).
trie_bindings(NodeId, Acc) ->
    StartKey = {NodeId, <<>>, {}},
    case ets:next(?BINDING_TABLE_V2, StartKey) of
        {NodeId, _BKey1, Dest1} = Key1 ->
            case ets:next(?BINDING_TABLE_V2, Key1) of
                {NodeId, _BKey2, Dest2} = Key2 ->
                    case ets:next(?BINDING_TABLE_V2, Key2) of
                        {NodeId, _, _} ->
                            collect_select(NodeId, Acc);
                        _ ->
                            [Dest2, Dest1 | Acc]
                    end;
                _ ->
                    [Dest1 | Acc]
            end;
        _ ->
            Acc
    end.

collect_select(NodeId, Acc) ->
    Dests = ets:select(?BINDING_TABLE_V2,
                       [{{{NodeId, '_', '$1'}}, [], ['$1']}]),
    Dests ++ Acc.

%%----------------------------------------------------------------------------
%% v1 projection - copied from
%% rabbit_khepri:register_rabbit_topic_graph_projection/0.
%% Delete together with the v1 match walk when the
%% `delayed_message_topic_projection_v2' feature flag becomes required.
%%----------------------------------------------------------------------------

register_projection_v1() ->
    Projection = khepri_projection:new(
                   ?TABLE_V1, projection_fun_v1(), projection_options_v1()),
    register_projection_result(
      khepri:register_projection(
        rabbit_khepri:get_store_id(), path_pattern(), Projection)).

projection_options_v1() ->
    #{keypos => #topic_trie_edge_v2.trie_edge,
      standalone_fun_options =>
          #{should_process_function => should_process_fun()},
      read_concurrency => true}.

projection_fun_v1() ->
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

%% follow_down_update - copied from rabbit_khepri.erl (private).

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
%% v1 match walk - adapted from
%% rabbit_db_topic_exchange:trie_match_v3 (formerly trie_match_in_khepri).
%%----------------------------------------------------------------------------

trie_match_v1(X, Node, [], Acc0) ->
    Dests = trie_bindings_v1(X, Node),
    Acc = Dests ++ Acc0,
    trie_match_part_v1(X, Node, <<"#">>, fun trie_match_skip_any_v1/4,
                       [], Acc);
trie_match_v1(X, Node, [W | RestW] = Words, Acc) ->
    lists:foldl(
      fun ({WArg, MatchFun, RestWArg}, A) ->
              trie_match_part_v1(X, Node, WArg, MatchFun, RestWArg, A)
      end, Acc,
      [{W, fun trie_match_v1/4, RestW},
       {<<"*">>, fun trie_match_v1/4, RestW},
       {<<"#">>, fun trie_match_skip_any_v1/4, Words}]).

trie_match_part_v1(X, Node, Search, MatchFun, RestW, Acc) ->
    case trie_child_v1(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, Acc);
        error -> Acc
    end.

trie_match_skip_any_v1(X, Node, [], Acc) ->
    trie_match_v1(X, Node, [], Acc);
trie_match_skip_any_v1(X, Node, [_ | RestW] = Words, Acc) ->
    trie_match_skip_any_v1(X, Node, RestW,
                           trie_match_v1(X, Node, Words, Acc)).

trie_child_v1(X, Node, Word) ->
    case ets:lookup(?TABLE_V1,
                    #trie_edge{exchange_name = X,
                               node_id = Node,
                               word = Word}) of
        [#topic_trie_edge_v2{node_id = NextNode}] -> {ok, NextNode};
        [] -> error
    end.

trie_bindings_v1(X, Node) ->
    case ets:lookup(?TABLE_V1,
                    #trie_edge{exchange_name = X,
                               node_id = Node,
                               word = bindings}) of
        [#topic_trie_edge_v2{node_id = {bindings, Bindings}}] ->
            [Dest || #binding{destination = Dest} <- sets:to_list(Bindings)];
        [] -> []
    end.
