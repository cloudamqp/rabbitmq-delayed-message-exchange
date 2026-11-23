%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%

%% Blocks the routing of expired delayed messages, on every node at once.
%%
%% Meant for rolling upgrades: while nodes restart, quorum queues elect new
%% leaders and classic queue processes stop, and a delayed message routed
%% then finds no route and is dropped. While routing is blocked, every
%% x-delayed-message exchange keeps accepting and storing messages, but none
%% is routed when its delay expires: it stays in the store. Once unblocked,
%% each node's delayed message server goes back to routing as usual, starting
%% with the messages that expired meanwhile, in delay order.
%% Messages published without a delay are not affected: the publishing
%% channel routes them, and the publisher learns about a failed route.
%%
%% The block is stored in Khepri, so it applies to the whole cluster and
%% survives restarts: a node that restarts while blocked comes back blocked.
%% A projection keeps a local ETS copy, so checking it never waits on Khepri,
%% and tells each node's delayed message server when routing is unblocked.
-module(rabbit_delayed_message_block).

-include_lib("khepri/include/khepri.hrl").

-rabbit_boot_step(
   {?MODULE,
    [{description, "exchange type x-delayed-message: Khepri routing block"},
     {mfa,         {?MODULE, register_projection, []}},
     {cleanup,     {?MODULE, unregister_projection, []}},
     {requires,    core_initialized},
     {enables,     recovery}]}).

%% Public API
-export([block/0,
         block/1,
         unblock/0,
         unblock/1,
         is_blocked/0,
         status/0]).

%% Internal RPC
-export([local_state/0]).

%% Boot steps
-export([register_projection/0,
         unregister_projection/0]).

-define(TABLE, rabbit_delayed_message_routing_block).
-define(PATH, [rabbitmq_delayed_message_exchange, routing_blocked]).
-define(KEY, routing_blocked).

%% How long block/0 and unblock/0 wait for every node to apply the change.
-define(DEFAULT_TIMEOUT, 5000).
-define(POLL_INTERVAL, 100).
-define(RPC_TIMEOUT, 5000).

-type result() :: ok | {applying, [node()]} | {error, term()}.
-type status() :: blocked | unblocked | {applying, [node()]} | {error, term()}.

-export_type([result/0, status/0]).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

%% Stops routing expired delayed messages on every node until unblock/0 is
%% called. Blocking when already blocked is a no-op.
%%
%% Each node applies the block on its own, so this waits, up to Timeout
%% milliseconds, for every node running the plugin to have applied it.
%% `ok' means they all have: it is safe to start restarting nodes.
%% `{applying, Nodes}' means the nodes listed have not yet.
%% Operators may use status/0 to check whether the block is still being
%% applied or completed.
-spec block() -> result().
block() ->
    block(?DEFAULT_TIMEOUT).

-spec block(non_neg_integer()) -> result().
block(Timeout) ->
    case rabbit_khepri:put(?PATH, true) of
        ok                 -> wait_until_applied(true, Timeout);
        {error, _} = Error -> Error
    end.

%% Lifts the routing block. Unblocking when not blocked is a no-op. Waits and
%% returns like block/1: `ok' means every node has applied it. It does not
%% route anything itself: each node's delayed message server is notified and
%% then routes the messages that expired while blocked, in the background.
-spec unblock() -> result().
unblock() ->
    unblock(?DEFAULT_TIMEOUT).

-spec unblock(non_neg_integer()) -> result().
unblock(Timeout) ->
    case rabbit_khepri:delete(?PATH) of
        ok                 -> wait_until_applied(false, Timeout);
        {error, _} = Error -> Error
    end.

%% The block across the cluster: `blocked' or `unblocked' once every node
%% running the plugin agrees with what is stored, `{applying, Nodes}' while
%% the nodes listed have not caught up yet. Reads the stored value from the
%% cluster, so it needs a Khepri quorum.
-spec status() -> status().
status() ->
    case rabbit_khepri:exists(?PATH, #{favor => consistency}) of
        Blocked when is_boolean(Blocked) ->
            case lagging_nodes(Blocked) of
                [] when Blocked -> blocked;
                []              -> unblocked;
                Nodes           -> {applying, Nodes}
            end;
        {error, _} = Error ->
            Error
    end.

%% Answered from the local projection, without waiting on Khepri.
-spec is_blocked() -> boolean().
is_blocked() ->
    try
        ets:member(?TABLE, ?KEY)
    catch
        error:badarg ->
            %% The projection is not registered.
            false
    end.

%% Called on each node by lagging_nodes/1: `not_running' when the plugin is
%% not running there, so the node has nothing to route.
-spec local_state() -> boolean() | not_running.
local_state() ->
    case whereis(rabbit_delayed_message) of
        undefined -> not_running;
        _         -> is_blocked()
    end.

%%----------------------------------------------------------------------------
%% Waiting for the nodes
%%----------------------------------------------------------------------------

wait_until_applied(Expected, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_until_applied1(Expected, Deadline).

wait_until_applied1(Expected, Deadline) ->
    case lagging_nodes(Expected) of
        [] ->
            ok;
        Nodes ->
            case Deadline - erlang:monotonic_time(millisecond) of
                Remaining when Remaining > 0 ->
                    timer:sleep(min(?POLL_INTERVAL, Remaining)),
                    wait_until_applied1(Expected, Deadline);
                _ ->
                    {applying, Nodes}
            end
    end.

%% The running nodes where the plugin runs but has not applied the expected
%% state yet. Nodes that are down are left out: they route nothing, and pick
%% up the stored state when they start.
lagging_nodes(Expected) ->
    Nodes = rabbit_nodes:list_running(),
    Results = erpc:multicall(Nodes, ?MODULE, local_state, [], ?RPC_TIMEOUT),
    [Node || {Node, Result} <- lists:zip(Nodes, Results),
             is_lagging(Node, Result, Expected)].

is_lagging(_Node, {ok, not_running}, _Expected) ->
    false;
is_lagging(_Node, {ok, Expected}, Expected) ->
    false;
is_lagging(Node, {error, {exception, undef, _}}, _Expected) ->
    %% This module or function does not exist there: the plugin is not
    %% installed, or it is a version without the block, which ignores it.
    plugin_running(Node);
is_lagging(_Node, _Result, _Expected) ->
    %% Not applied yet, or the node did not answer: assume the worst.
    true.

plugin_running(Node) ->
    try erpc:call(Node, erlang, whereis, [rabbit_delayed_message],
                  ?RPC_TIMEOUT) of
        undefined -> false;
        _         -> true
    catch
        _:_ -> true
    end.

%%----------------------------------------------------------------------------
%% Projection
%%----------------------------------------------------------------------------

-spec register_projection() -> ok | {error, term()}.
register_projection() ->
    Opts = #{keypos => 1,
             read_concurrency => true,
             standalone_fun_options =>
                 #{should_process_function => should_process_fun()}},
    Projection = khepri_projection:new(?TABLE, projection_fun(), Opts),
    case khepri:register_projection(rabbit_khepri:get_store_id(),
                                    ?PATH, Projection) of
        ok                                                  -> ok;
        {error, exists}                                     -> ok;
        {error, {khepri, projection_already_exists, _Info}} -> ok;
        {error, _} = Error                                  -> Error
    end.

-spec unregister_projection() -> ok.
unregister_projection() ->
    _ = khepri:unregister_projections(rabbit_khepri:get_store_id(), [?TABLE]),
    ok.

%% Runs on every cluster member as it applies a change to the block. The
%% unblock notification is a cast to the local server, dropped if the plugin
%% is not running there: a server starting up checks the block itself.
projection_fun() ->
    fun(Table, _Path, OldProps, NewProps) ->
            case {OldProps, NewProps} of
                {_, #{data := _}} ->
                    ets:insert(Table, {?KEY});
                {#{data := _}, _} ->
                    ets:delete(Table, ?KEY),
                    rabbit_delayed_message:routing_unblocked();
                {_, _} ->
                    ok
            end
    end.

%% ets and the unblock notification are side effects Horus refuses to
%% extract by default; they are called as they are.
should_process_fun() ->
    fun(ets, _F, _A, _From) ->
            false;
       (rabbit_delayed_message, routing_unblocked, 0, _From) ->
            false;
       (M, F, A, From) ->
            khepri_tx_adv:should_process_function(M, F, A, From)
    end.
