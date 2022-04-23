%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% NOTE that this module uses os:timestamp/0 but in the future Erlang
%% will have a new time API.
%% See:
%% https://www.erlang.org/documentation/doc-7.0-rc1/erts-7.0/doc/html/erlang.html#now-0
%% and
%% https://www.erlang.org/documentation/doc-7.0-rc1/erts-7.0/doc/html/time_correction.html

-module(rabbit_delayed_message).
-include_lib("rabbit_common/include/rabbit.hrl").
-rabbit_boot_step({?MODULE,
                   [{description, "exchange delayed message mnesia setup"},
                    {mfa, {?MODULE, setup_mnesia, []}},
                    {cleanup, {?MODULE, disable_plugin, []}},
                    {requires, external_infrastructure},
                    {enables, rabbit_registry}]}).

-behaviour(gen_server).

-export([start_link/0, delay_message/3, setup_mnesia/0, disable_plugin/0, go/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([messages_delayed/1,
         update_config/2]).

-import(rabbit_delayed_message_utils, [swap_delay_header/1]).

-type t_reference() :: reference().
-type delay() :: non_neg_integer().


-spec delay_message(rabbit_types:exchange(),
                    rabbit_types:delivery(),
                    delay()) ->
                           no_change | {ok, t_reference()}.

-spec internal_delay_message(t_reference(),
                             rabbit_types:exchange(),
                             rabbit_types:delivery(),
                             delay(),
                             timeout()) ->
                                    no_change | {ok, t_reference()}.

-define(SERVER, ?MODULE).
-define(TABLE_NAME, append_to_atom(?MODULE, node())).
-define(INDEX_TABLE_NAME, append_to_atom(?TABLE_NAME, "_index")).
-define(LONGTERM_TABLE_NAME, append_to_atom(?TABLE_NAME, "_longterm")).

-define(SHORTTERM, 1).
-define(LONGTERM, 2).

-record(state, {timer :: not_set | t_reference(),
                longterm_threshold :: timeout()}).

-record(delay_key,
        { timestamp, %% timestamp delay
          exchange   %% rabbit_types:exchange()
        }).

-record(delay_entry,
        { delay_key, %% delay_key record
          delivery,  %% the message delivery
          ref        %% ref to make records distinct for 'bag' semantics.
        }).

-record(delay_index,
        { delay_key, %% delay_key record
          const      %% record must have two fields
        }).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

go() ->
    gen_server:cast(?MODULE, go).

delay_message(Exchange, Delivery, Delay) ->
    gen_server:call(?MODULE, {delay_message, Exchange, Delivery, Delay},
                    infinity).

setup_mnesia() ->
    mnesia:create_table(?TABLE_NAME, [{record_name, delay_entry},
                                      {attributes,
                                       record_info(fields, delay_entry)},
                                      {type, bag},
                                      {disc_copies, [node()]}]),
    mnesia:create_table(?INDEX_TABLE_NAME, [{record_name, delay_index},
                                            {attributes,
                                             record_info(fields, delay_index)},
                                            {type, ordered_set},
                                            {disc_copies, [node()]}]),
    mnesia:create_table(?LONGTERM_TABLE_NAME,
                        [{record_name, delay_entry},
                         {attributes,
                          record_info(fields, delay_entry)},
                         {type, bag},
                         {disc_only_copies, [node()]}]),
    rabbit_table:wait([?TABLE_NAME,
                       ?LONGTERM_TABLE_NAME,
                       ?INDEX_TABLE_NAME]).

disable_plugin() ->
    mnesia:delete_table(?INDEX_TABLE_NAME),
    mnesia:delete_table(?TABLE_NAME),
    mnesia:delete_table(?LONGTERM_TABLE_NAME),
    ok.

messages_delayed(#exchange{name = ExchangeName}) ->
    messages_delayed(ExchangeName);
messages_delayed(ExchangeName = #resource{}) ->
    MatchHead = #delay_entry{delay_key = make_key('_', '_', ExchangeName),
                             _ = '_'},
    MatchHeadLegacy = #delay_entry{delay_key = #delay_key{exchange  = #exchange{name = ExchangeName, _ = '_'}},
                                   _  = '_'},
    Delays = mnesia:dirty_select(?TABLE_NAME, [{MatchHead, [], [{const, match}]},
                                               {MatchHeadLegacy, [], [{const, match}]}]),
    length(Delays).

update_config(longterm_threshold, Value)
  when is_integer(Value), Value >= 0;
       Value =:= infinity ->
    gen_server:call(?MODULE, {update_config, longterm_threshold, Value}).

%%--------------------------------------------------------------------

init([]) ->
    recover(),
    LongTermThreshold = application:get_env(
                          rabbitmq_delayed_message_exchange, longterm_threshold, infinity),
    {ok, #state{timer = not_set,
                longterm_threshold = LongTermThreshold}}.

handle_call({delay_message, Exchange, Delivery, Delay},
            _From, State = #state{timer = CurrTimer,
                                  longterm_threshold = LongTermThreshold}) ->
    Reply = internal_delay_message(CurrTimer, Exchange, Delivery, Delay, LongTermThreshold),
    State2 = case Reply of
                 {ok, NewTimer} ->
                     State#state{timer = NewTimer};
                 _ ->
                     State
             end,
    {reply, Reply, State2};
handle_call({update_config, longterm_threshold, Value}, _From, State) ->
    {reply, ok, State#state{longterm_threshold = Value}};
handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(go, State) ->
    {noreply, State#state{timer = maybe_delay_first()}};
handle_cast(_C, State) ->
    {noreply, State}.

handle_info({timeout, _TimerRef, {deliver, Key}}, State) ->
    Table = table_name_from_key(Key),
    case mnesia:dirty_read(Table, Key) of
        [] ->
            ok;
        Deliveries ->
            route(Key, Deliveries),
            mnesia:dirty_delete(Table, Key),
            mnesia:dirty_delete(?INDEX_TABLE_NAME, Key)
    end,
    {noreply, State#state{timer = maybe_delay_first()}};
handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

maybe_delay_first() ->
    case mnesia:dirty_first(?INDEX_TABLE_NAME) of
        %% destructuring to prevent matching '$end_of_table'
        #delay_key{timestamp = FirstTS} = Key2 ->
            %% there are messages that expired and need to be delivered
            Now = erlang:system_time(milli_seconds),
            start_timer(FirstTS - Now, Key2);
        _ ->
            %% nothing to do
            not_set
    end.

route(#delay_key{exchange = Ex}, Deliveries) ->
    case lookup_exchange(Ex) of
        {ok, Exchange} ->
            lists:map(fun (#delay_entry{delivery = D}) ->
                              D2 = swap_delay_header(D),
                              Dests = rabbit_exchange:route(Exchange, D2),
                              Qs = rabbit_amqqueue:lookup(Dests),
                              rabbit_amqqueue:deliver(Qs, D2)
                      end, Deliveries);
        {error, not_found} ->
            []
    end.

internal_delay_message(CurrTimer, Exchange, Delivery, Delay, LongTermThreshold) ->
    Now = erlang:system_time(milli_seconds),
    %% keys are timestamps in milliseconds,in the future
    DelayTS = Now + Delay,
    IsLongTerm = is_long_term(Delay, LongTermThreshold),
    mnesia:dirty_write(?INDEX_TABLE_NAME,
                       make_index(DelayTS, IsLongTerm, Exchange)),
    mnesia:dirty_write(table_name(IsLongTerm),
                       make_delay(DelayTS, IsLongTerm, Exchange, Delivery)),
    case CurrTimer of
        not_set ->
            %% No timer in progress, so we start our own.
            {ok, start_timer(Delay, make_key(DelayTS, IsLongTerm, Exchange))};
        _ ->
            case erlang:read_timer(CurrTimer) of
                false ->
                    %% Timer is already expired.  Handler will be invoked soon.
                    no_change;
                CurrMS when Delay < CurrMS ->
                    %% Current timer lasts longer that new message delay
                    erlang:cancel_timer(CurrTimer),
                    {ok, start_timer(Delay, make_key(DelayTS, IsLongTerm, Exchange))};
                _ ->
                    %% Timer is set to expire sooner than this
                    %% message's scheduled delivery time.
                    no_change
            end
    end.

%% Key will be used upon message receipt to fetch
%% the deliveries from the database
start_timer(Delay, Key) ->
    erlang:start_timer(erlang:max(0, Delay), self(), {deliver, Key}).

make_delay(DelayTS, IsLongTerm, Exchange, Delivery) ->
    #delay_entry{delay_key = make_key(DelayTS, IsLongTerm, Exchange),
                 delivery  = Delivery,
                 ref       = make_ref()}.

make_index(DelayTS, IsLongTerm, Exchange) ->
    #delay_index{delay_key = make_key(DelayTS, IsLongTerm, Exchange),
                 const = true}.

make_key(DelayTS, IsLongTerm, #exchange{name = ExchangeName}) ->
    make_key(DelayTS, IsLongTerm, ExchangeName);
make_key(DelayTS, IsLongTerm, ExchangeName = #resource{}) ->
    #delay_key{timestamp = DelayTS,
               exchange  = {IsLongTerm, ExchangeName}}.

append_to_atom(Atom, Append) when is_atom(Append) ->
    append_to_atom(Atom, atom_to_list(Append));
append_to_atom(Atom, Append) when is_list(Append) ->
    list_to_atom(atom_to_list(Atom) ++ Append).

recover() ->
    %% topology recovery has already happened, we have to recover state for any durable
    %% consistent hash exchanges since plugin activation was moved later in boot process
    %% starting with RabbitMQ 3.8.4
    case list_exchanges() of
        {ok, Xs} ->
            rabbit_log:debug("Delayed message exchange: "
                              "have ~b durable exchanges to recover",
                             [length(Xs)]),
            [recover_exchange_and_bindings(X) || X <- lists:usort(Xs)];
        {aborted, Reason} ->
            rabbit_log:error(
                "Delayed message exchange: "
                 "failed to recover durable bindings of one of the exchanges, reason: ~p",
                [Reason])
    end.

list_exchanges() ->
    case mnesia:transaction(
           fun () ->
                   mnesia:match_object(
                     rabbit_exchange, #exchange{durable = true,
                                                type = 'x-delayed-message',
                                                _ = '_'}, write)
           end) of
        {atomic, Xs} ->
            {ok, Xs};
        {aborted, Reason} ->
            {aborted, Reason}
    end.

recover_exchange_and_bindings(#exchange{name = XName} = X) ->
    mnesia:transaction(
        fun () ->
            Bindings = rabbit_binding:list_for_source(XName),
            [rabbit_exchange_type_delayed_message:add_binding(transaction, X, B)
             || B <- lists:usort(Bindings)],
            rabbit_log:debug("Delayed message exchange: "
                              "recovered bindings for ~s",
                             [rabbit_misc:rs(XName)])
    end).

-spec is_long_term(delay(), timeout()) -> ?SHORTTERM | ?LONGTERM.
is_long_term(_, infinity) ->
    ?SHORTTERM;
is_long_term(Delay, LongTermThreshold) ->
    case Delay > LongTermThreshold of
        true -> ?LONGTERM;
        false -> ?SHORTTERM
    end.

table_name(?SHORTTERM) ->
    ?TABLE_NAME;
table_name(?LONGTERM) ->
    ?LONGTERM_TABLE_NAME.

table_name_from_key(#delay_key{exchange = {IsLongTerm, _}}) ->
    table_name(IsLongTerm);
table_name_from_key(_) ->
    %% legacy key #delay_key{exchange = #exchange{}}, always short term
    ?TABLE_NAME.

lookup_exchange({_, ExName = #resource{}}) ->
    rabbit_exchange:lookup(ExName);
lookup_exchange(Ex = #exchange{}) ->
    %% legacy format
    {ok, Ex}.
