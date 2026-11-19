%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%

%% Blocks the routing of an x-delayed-message exchange.
%%
%% While an exchange is blocked, messages can still be published to it and are
%% stored as usual, but none of them is routed to the queues: a message whose
%% delay expires is kept in the store instead. Unblocking the exchange routes
%% every kept message, in delay order, and routing goes back to normal.
%%
%% Blocks are stored in Khepri, so they apply to the whole cluster and survive
%% restarts. Each one is tied to its exchange with a keep_while condition: it
%% goes away when the exchange (or its vhost) is deleted. A projection keeps a
%% local ETS copy for the publish and expiry paths, and tells the local delayed
%% message server when a block is lifted, so every node, including one that
%% was cut off when unblock/1 was called, releases its kept messages as soon
%% as its replica catches up.
-module(rabbit_delayed_message_block).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step(
   {?MODULE,
    [{description, "exchange type x-delayed-message: Khepri routing blocks"},
     {mfa,         {?MODULE, register_projection, []}},
     {cleanup,     {?MODULE, unregister_projection, []}},
     {requires,    core_initialized},
     {enables,     recovery}]}).

%% Public API
-export([block/1,
         block/2,
         unblock/1,
         unblock/2,
         is_blocked/1,
         list/0]).

%% Boot steps
-export([register_projection/0,
         unregister_projection/0]).

-define(TABLE, rabbit_delayed_message_blocked_exchanges).
-define(ROOT, rabbitmq_delayed_message_exchange).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

%% Stops routing messages out of the exchange until unblock/1 is called.
%% Blocking an exchange that is already blocked is a no-op.
-spec block(rabbit_types:vhost(), binary()) ->
    ok | {error, not_found | not_delayed_exchange | term()}.
block(VHost, Name) ->
    block(rabbit_misc:r(VHost, exchange, Name)).

-spec block(rabbit_exchange:name()) ->
    ok | {error, not_found | not_delayed_exchange | term()}.
block(XName) ->
    case rabbit_exchange:lookup(XName) of
        {ok, #exchange{type = 'x-delayed-message'}} ->
            %% The keep_while condition ties the block to the exchange: it
            %% fails the put if the exchange was deleted in the meantime, and
            %% deletes the block along with the exchange later on.
            KeepWhile = #{rabbit_db_exchange:khepri_exchange_path(XName) =>
                              #if_node_exists{exists = true}},
            case rabbit_khepri:put(path(XName), blocked,
                                   #{keep_while => KeepWhile}) of
                ok ->
                    ok;
                {error, {khepri, keep_while_conditions_not_met, _}} ->
                    {error, not_found};
                {error, _} = Error ->
                    Error
            end;
        {ok, #exchange{}} ->
            {error, not_delayed_exchange};
        {error, not_found} ->
            {error, not_found}
    end.

%% Routes every message the exchange kept while blocked and resumes normal
%% routing. Unblocking an exchange that is not blocked is a no-op.
-spec unblock(rabbit_types:vhost(), binary()) -> ok | {error, term()}.
unblock(VHost, Name) ->
    unblock(rabbit_misc:r(VHost, exchange, Name)).

-spec unblock(rabbit_exchange:name()) -> ok | {error, term()}.
unblock(XName) ->
    rabbit_khepri:delete(path(XName)).

%% Answered from the local projection: cheap enough for the publish path.
-spec is_blocked(rabbit_types:exchange() | rabbit_exchange:name()) -> boolean().
is_blocked(#exchange{name = XName}) ->
    is_blocked(XName);
is_blocked(#resource{} = XName) ->
    try
        ets:member(?TABLE, XName)
    catch
        error:badarg ->
            %% The projection is not registered (yet).
            false
    end.

-spec list() -> [rabbit_exchange:name()].
list() ->
    try
        [XName || {XName} <- ets:tab2list(?TABLE)]
    catch
        error:badarg ->
            []
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
    PathPattern = [?ROOT, blocked_exchanges,
                   ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR],
    case khepri:register_projection(rabbit_khepri:get_store_id(),
                                    PathPattern, Projection) of
        ok                                                  -> ok;
        {error, exists}                                     -> ok;
        {error, {khepri, projection_already_exists, _Info}} -> ok;
        {error, _} = Error                                  -> Error
    end.

-spec unregister_projection() -> ok.
unregister_projection() ->
    _ = khepri:unregister_projections(rabbit_khepri:get_store_id(), [?TABLE]),
    ok.

%% Runs on every cluster member as it applies a change to a block. The
%% release is a cast to the local server, dropped if the plugin is not
%% running there: a node starting up releases nothing, since the kept messages
%% of an exchange that is no longer blocked are simply routed as they expire.
projection_fun() ->
    fun(Table, [_Root, _Blocks, VHost, Name], OldProps, NewProps) ->
            XName = #resource{virtual_host = VHost, kind = exchange,
                              name = Name},
            case {OldProps, NewProps} of
                {_, #{data := _}} ->
                    ets:insert(Table, {XName});
                {#{data := _}, _} ->
                    ets:delete(Table, XName),
                    rabbit_delayed_message:exchange_unblocked(XName);
                {_, _} ->
                    ok
            end
    end.

%% ets and the release notification are side effects Horus refuses to
%% extract by default; they are called as they are.
should_process_fun() ->
    fun(ets, _F, _A, _From) ->
            false;
       (rabbit_delayed_message, exchange_unblocked, 1, _From) ->
            false;
       (M, F, A, From) ->
            khepri_tx_adv:should_process_function(M, F, A, From)
    end.

path(#resource{virtual_host = VHost, kind = exchange, name = Name}) ->
    [?ROOT, blocked_exchanges, VHost, Name].
