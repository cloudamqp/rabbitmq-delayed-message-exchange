-module(rabbit_delayed_message_mnesia).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_delayed_message.hrl").

-export([setup/0,
         disable_plugin/0,
         messages_delayed/1,
         store_delay/3,
         get_first_delay/0,
         get_many/1,
         delete/1,
         delete_index/1
        ]).

%% For testing, debugging and manual use
-export([table_name/0,
         index_table_name/0]).

%%--------------------------------------------------------------------

setup() ->
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

store_delay(DelayTS, Exchange, Message) ->
    mnesia:dirty_write(?INDEX_TABLE_NAME,
                       make_index(DelayTS, Exchange)),
    mnesia:dirty_write(?TABLE_NAME,
                       make_delay(DelayTS, Exchange, Message)).

get_first_delay() ->
    case mnesia:dirty_first(?INDEX_TABLE_NAME) of
        #delay_key{timestamp = DelayTS} = FirstKey ->
            {DelayTS, FirstKey};
        _ ->
            undefined
    end.

get_many(Key) ->
    DelayEntries = mnesia:dirty_read(?TABLE_NAME, Key),
    [{Ex, case Msg0 of
           #delivery{message = BasicMessage} ->
                 BasicMessage;
           _MC ->
               Msg0
       end} || #delay_entry{delay_key = #delay_key{exchange = Ex},
                           delivery = Msg0} <- DelayEntries].

delete(Key) ->
    mnesia:dirty_delete(?TABLE_NAME, Key).

delete_index(Key) ->
    mnesia:dirty_delete(?INDEX_TABLE_NAME, Key).



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

table_name() ->
    ?TABLE_NAME.

index_table_name() ->
    ?INDEX_TABLE_NAME.

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
