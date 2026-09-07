%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Per-exchange counters of the messages this node holds delayed. They live in
%% a seshat group, which keeps one ETS table for the whole plugin and carries
%% the metric metadata and the Prometheus labels along with each counter.
-module(rabbit_delayed_message_counters).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-export([setup/0,
         teardown/0,
         id/1,
         init/3,
         add/2,
         sub/2,
         forget/1,
         messages_delayed/1,
         bytes_delayed/1,
         counters/1,
         format/1]).

-define(GROUP, rabbitmq_delayed_message_exchange).

%% Indexes into the per-exchange counters array.
-define(MESSAGES, 1).
-define(BYTES, 2).

%% Every exchange shares the same field specification, so it is held in a
%% single persistent term instead of being copied into each seshat entry.
-define(FIELDS_KEY, {?MODULE, fields}).
-define(FIELDS_SPEC, {persistent_term, ?FIELDS_KEY}).
-define(FIELDS,
        [{delayed_messages, ?MESSAGES, gauge,
          "Number of messages delayed by an x-delayed-message exchange on this node"},
         {delayed_message_bytes, ?BYTES, gauge,
          "Size in bytes of the message bodies delayed by an x-delayed-message "
          "exchange on this node"}]).

%% An exchange name packed into a binary. Every delayed message carries one, in
%% the index table and in the Leveled ledger head, so it is kept more compact
%% than the resource record it stands for.
-type id() :: binary().

-export_type([id/0]).

-spec setup() -> ok.
setup() ->
    ok = persistent_term:put(?FIELDS_KEY, ?FIELDS),
    %% The group outlives a crash of the delayed message server, and the
    %% counters are about to be rebuilt from the store, so start from scratch.
    ok = seshat:delete_group(?GROUP),
    _ = seshat:new_group(?GROUP),
    ok.

%% Called on plugin disable, symmetrically with setup/0.
-spec teardown() -> ok.
teardown() ->
    ok = seshat:delete_group(?GROUP),
    _ = persistent_term:erase(?FIELDS_KEY),
    ok.

-spec id(rabbit_types:exchange() | rabbit_exchange:name()) -> id().
id(#exchange{name = XName}) ->
    id(XName);
id(#resource{virtual_host = VHost, name = Name}) ->
    <<(byte_size(VHost)):16/big, VHost/binary, Name/binary>>.

-spec id_to_exchange_name(id()) -> rabbit_exchange:name().
id_to_exchange_name(<<VHostLen:16/big, VHost:VHostLen/binary, Name/binary>>) ->
    #resource{virtual_host = VHost, kind = exchange, name = Name}.

%% Sets the counters of an exchange to the values recovered from the store.
-spec init(id(), non_neg_integer(), non_neg_integer()) -> ok.
init(Id, Count, Bytes) ->
    CRef = new(Id),
    counters:put(CRef, ?MESSAGES, Count),
    counters:put(CRef, ?BYTES, Bytes).

-spec add(id(), non_neg_integer()) -> ok.
add(Id, Bytes) ->
    CRef = ensure(Id),
    counters:add(CRef, ?MESSAGES, 1),
    counters:add(CRef, ?BYTES, Bytes).

-spec sub(id(), non_neg_integer()) -> ok.
sub(Id, Bytes) ->
    case seshat:fetch(?GROUP, Id) of
        undefined ->
            %% Expected once the exchange has been deleted: forget/1 dropped
            %% its counters while some of its messages were still delayed.
            ok;
        CRef ->
            case counters:get(CRef, ?MESSAGES) of
                0 ->
                    ?LOG_WARNING("Delayed message exchange: message counter for ~ts "
                                 "is already zero, not decreasing it",
                                 [rabbit_misc:rs(id_to_exchange_name(Id))]);
                _ ->
                    counters:sub(CRef, ?MESSAGES, 1),
                    counters:sub(CRef, ?BYTES, Bytes)
            end
    end.

%% Drops the counters of a deleted exchange. Messages it had already delayed
%% are still on disk and will be routed to nowhere when they expire, but no
%% metric can be attributed to an exchange that no longer exists.
-spec forget(id()) -> ok.
forget(Id) ->
    try
        seshat:delete(?GROUP, Id)
    catch
        error:badarg ->
            %% The plugin is not set up on this node.
            ok
    end.

-spec messages_delayed(rabbit_types:exchange() | rabbit_exchange:name()) ->
    non_neg_integer().
messages_delayed(Exchange) ->
    value(id(Exchange), ?MESSAGES).

-spec bytes_delayed(rabbit_types:exchange() | rabbit_exchange:name()) ->
    non_neg_integer().
bytes_delayed(Exchange) ->
    value(id(Exchange), ?BYTES).

%% Every counter of an exchange at once, `undefined' when it has never delayed
%% a message on this node. Meant for tests and for debugging from a shell.
-spec counters(rabbit_types:exchange() | rabbit_exchange:name()) ->
    #{atom() => integer()} | undefined.
counters(Exchange) ->
    try
        seshat:counters(?GROUP, id(Exchange))
    catch
        error:badarg ->
            undefined
    end.

%% Returns every counter in the Prometheus shape seshat defines, keeping only
%% the exchanges whose labels satisfy FilterFun.
-spec format(fun((seshat:labels_map()) -> boolean())) -> map().
format(FilterFun) ->
    try
        seshat:format(?GROUP, #{labels => as_map, filter_fun => FilterFun})
    catch
        error:badarg ->
            #{}
    end.

%% ===================================================================
%% Private functions
%% ===================================================================

%% Only ever called from the rabbit_delayed_message gen_server, so the
%% read-then-create sequence has a single writer.
ensure(Id) ->
    case seshat:fetch(?GROUP, Id) of
        undefined -> new(Id);
        CRef      -> CRef
    end.

new(Id) ->
    #resource{virtual_host = VHost, name = Name} = id_to_exchange_name(Id),
    seshat:new(?GROUP, Id, ?FIELDS_SPEC, #{vhost => VHost, exchange => Name}).

%% Reads tolerate a missing group: exchange info is queried by the management
%% plugin and by rabbitmqctl, which can race with the plugin being disabled.
value(Id, Index) ->
    try seshat:fetch(?GROUP, Id) of
        undefined -> 0;
        CRef      -> counters:get(CRef, Index)
    catch
        error:badarg -> 0
    end.
