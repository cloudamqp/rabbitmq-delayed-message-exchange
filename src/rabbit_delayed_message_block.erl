%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%

%% Blocks the routing of expired delayed messages on a node.
%%
%% Meant for rolling upgrades: while nodes restart, quorum queues elect new
%% leaders and classic queue processes stop, and a delayed message routed
%% then finds no route and is dropped. While routing is blocked on a node,
%% every x-delayed-message exchange keeps accepting and storing messages
%% there, but none is routed when its delay expires: it stays in the store.
%% Once unblocked, the node's delayed message server goes back to routing as
%% usual, starting with the messages that expired meanwhile, in delay order.
%% Messages published without a delay are not affected: the publishing
%% channel routes them, and the publisher learns about a failed route.
%%
%% The block is per node: each node holds its own delayed messages and its
%% own block. The flag lives in the state of the node's delayed message
%% server, and is persisted in a DETS file so that a node restarting while
%% blocked comes back blocked. block_all_nodes/0 and unblock_all_nodes/0 act
%% on every running node at once.
%%
%% The DETS file is kept next to the node's data directory, not inside it:
%% on RabbitMQ 3.13, the Mnesia to Khepri migration deletes everything in the
%% data directory but a fixed list of files. Keeping it outside makes the
%% location the same for every RabbitMQ version this plugin supports.
-module(rabbit_delayed_message_block).

%% Public API, for operators
-export([block/0,
         unblock/0,
         is_blocked/0,
         block_all_nodes/0,
         unblock_all_nodes/0,
         status_all_nodes/0]).

%% Internal use by the delayed message server, which owns the DETS table
-export([open/0,
         persist/1,
         close/0,
         file/0]).

-define(SERVER, rabbit_delayed_message).
-define(TABLE, rabbit_delayed_message_routing_block).
-define(KEY, routing_blocked).
-define(CALL_TIMEOUT, 30000).
-define(RPC_TIMEOUT, 30000).

-type node_result() :: ok | {error, term()}.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

%% Stops routing expired delayed messages on this node until unblock/0 is
%% called. The block is written to disk before this returns, so it holds
%% across restarts. Blocking when already blocked is a no-op.
-spec block() -> node_result().
block() ->
    call(block_routing).

%% Lifts the block on this node. Unblocking when not blocked is a no-op. It
%% does not route anything itself: the node's delayed message server then
%% routes the messages that expired while blocked, in the background.
-spec unblock() -> node_result().
unblock() ->
    call(unblock_routing).

%% Whether routing is blocked on this node.
-spec is_blocked() -> boolean() | {error, term()}.
is_blocked() ->
    call(is_routing_blocked).

%% block/0 on every running node. `ok' once every node has persisted the
%% block, otherwise the nodes that failed, with the reason.
-spec block_all_nodes() -> ok | {error, [{node(), term()}]}.
block_all_nodes() ->
    on_all_nodes(block).

%% unblock/0 on every running node, returning like block_all_nodes/0.
-spec unblock_all_nodes() -> ok | {error, [{node(), term()}]}.
unblock_all_nodes() ->
    on_all_nodes(unblock).

%% is_blocked/0 on every running node.
-spec status_all_nodes() -> [{node(), boolean() | {error, term()}}].
status_all_nodes() ->
    Nodes = rabbit_nodes:list_running(),
    Results = erpc:multicall(Nodes, ?MODULE, is_blocked, [], ?RPC_TIMEOUT),
    [{Node, unwrap(Result)} || {Node, Result} <- lists:zip(Nodes, Results)].

%%----------------------------------------------------------------------------
%% DETS persistence, used by the delayed message server
%%----------------------------------------------------------------------------

%% Opens the table, owned by the calling process, and returns the persisted
%% flag.
-spec open() -> {ok, boolean()} | {error, term()}.
open() ->
    File = file(),
    ok = filelib:ensure_dir(File),
    case dets:open_file(?TABLE, [{file, File}, {type, set}, {repair, true}]) of
        {ok, ?TABLE} ->
            case dets:lookup(?TABLE, ?KEY) of
                [{?KEY, Blocked}] when is_boolean(Blocked) -> {ok, Blocked};
                []                                         -> {ok, false}
            end;
        {error, _} = Error ->
            Error
    end.

%% Writes the flag and flushes it to disk, so that it survives a crash or a
%% restart right after.
-spec persist(boolean()) -> ok | {error, term()}.
persist(Blocked) when is_boolean(Blocked) ->
    case dets:insert(?TABLE, {?KEY, Blocked}) of
        ok                 -> dets:sync(?TABLE);
        {error, _} = Error -> Error
    end.

-spec close() -> ok.
close() ->
    _ = dets:close(?TABLE),
    ok.

%% `<data dir>-delayed-message-exchange/routing_block.dets', next to the
%% data directory like RabbitMQ's own `<node>-feature_flags' and
%% `<node>-plugins-expand'.
%%
%% Not using rabbit_plugins:user_provided_plugins_data_dir/0 because this
%% block feature is backported to RabbitMQ older than 4.2.6 (the first
%% version to include the function).
-spec file() -> file:filename().
file() ->
    filename:join(rabbit:data_dir() ++ "-delayed-message-exchange",
                  "routing_block.dets").

%%----------------------------------------------------------------------------

call(Request) ->
    try
        gen_server:call(?SERVER, Request, ?CALL_TIMEOUT)
    catch
        exit:{noproc, _} ->
            {error, not_running};
        exit:{timeout, _} ->
            {error, timeout}
    end.

on_all_nodes(Fun) ->
    Nodes = rabbit_nodes:list_running(),
    Results = erpc:multicall(Nodes, ?MODULE, Fun, [], ?RPC_TIMEOUT),
    case [{Node, Reason}
          || {Node, Result} <- lists:zip(Nodes, Results),
             {error, Reason} <- [unwrap(Result)]] of
        []     -> ok;
        Failed -> {error, Failed}
    end.

unwrap({ok, Value})       -> Value;
unwrap({_Class, Reason})  -> {error, Reason}.
