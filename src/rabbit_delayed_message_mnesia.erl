-module(rabbit_delayed_message_mnesia).
-include("rabbit_delayed_message.hrl").

-export([setup_schema/0,
         disable_plugin/0,
         messages_delayed/1,
         store_delay/3
        ]).

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

setup_schema() ->
    _ = mnesia:create_table(?TABLE_NAME, [{record_name, delay_entry},
                                          {attributes,
                                           record_info(fields, delay_entry)},
                                          {type, bag},
                                          {disc_copies, [node()]}]),
    _ = mnesia:create_table(?INDEX_TABLE_NAME, [{record_name, delay_index},
                                                {attributes,
                                                 record_info(fields, delay_index)},
                                                {type, ordered_set},
                                                {disc_copies, [node()]}]),
    rabbit_table:wait([?TABLE_NAME, ?INDEX_TABLE_NAME]).

disable_plugin() ->
    _ = mnesia:delete_table(?INDEX_TABLE_NAME),
    _ = mnesia:delete_table(?TABLE_NAME),
    ok.

messages_delayed(Exchange) ->
    ExchangeName = Exchange#exchange.name,
    MatchHead = #delay_entry{delay_key = make_key('_', #exchange{name = ExchangeName, _ = '_'}),
                             delivery  = '_', ref       = '_'},
    Delays = mnesia:dirty_select(?TABLE_NAME, [{MatchHead, [], [true]}]),
    length(Delays).

store_delay(Exchange, Delivery, Delay) ->
    mnesia:dirty_write(?INDEX_TABLE_NAME,
                       make_index(DelayTS, Exchange)),
    mnesia:dirty_write(?TABLE_NAME,
                       make_delay(DelayTS, Exchange, Message)).

make_delay(DelayTS, Exchange, Delivery) ->
    #delay_entry{delay_key = make_key(DelayTS, Exchange),
                 delivery  = Delivery,
                 ref       = make_ref()}.

make_index(DelayTS, Exchange) ->
    #delay_index{delay_key = make_key(DelayTS, Exchange),
                 const = true}.

make_key(DelayTS, Exchange) ->
    #delay_key{timestamp = DelayTS,
               exchange  = Exchange}.

% DO I REALLY NEED THIS??
% ensure_mnesia_running() ->
%     case rabbit_mnesia:is_running() of
%         false ->
%             ensure_mnesia_disc_schema(),
%             rabbit_mnesia:start_mnesia(_CheckConsistency = false);
%         true ->
%             ok
%     end.
%
% ensure_mnesia_disc_schema() ->
%     case mnesia:system_info(use_dir) of
%         true ->
%             %% There is a disc schema already
%             ok;
%         false ->
%             rabbit_misc:ensure_ok(mnesia:create_schema([node()]),
%                                   {?MODULE, cannot_create_mnesia_schema})
%     end.
