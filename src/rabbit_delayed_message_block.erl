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
%% is routed when its delay expires: it stays in the store. Unblocking routes
%% every expired message, in delay order, and routing goes back to normal.
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
         unblock/0,
         is_blocked/0]).

%% Boot steps
-export([register_projection/0,
         unregister_projection/0]).

-define(TABLE, rabbit_delayed_message_routing_block).
-define(PATH, [rabbitmq_delayed_message_exchange, routing_blocked]).
-define(KEY, routing_blocked).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

%% Stops routing expired delayed messages on every node until unblock/0 is
%% called. Blocking when already blocked is a no-op.
-spec block() -> ok | {error, term()}.
block() ->
    rabbit_khepri:put(?PATH, true).

%% Routes every message that expired while blocked and resumes normal
%% routing, on every node. Unblocking when not blocked is a no-op.
-spec unblock() -> ok | {error, term()}.
unblock() ->
    rabbit_khepri:delete(?PATH).

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
