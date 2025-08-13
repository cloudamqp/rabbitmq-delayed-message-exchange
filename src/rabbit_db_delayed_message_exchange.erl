-module(rabbit_db_delayed_message_exchange).

-include("rabbit_delayed_message.hrl").

-export([setup_schema/0,
         disable_plugin/0,
         select_messages_delayed/1,
         get_messages_delayed/1,
         get_first_index/0,
         delete_message_delayed/1,
         delete_index_delayed/1,
         put_message_delayed/1,
         put_index_delayed/1
        ]).


setup_schema() ->
  rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> setup_schema_in_mnesia() end,
        khepri => fun() -> setup_schema_in_khepri() end}
  ).

setup_schema_in_mnesia() ->
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

setup_schema_in_khepri() ->
  %% Create the main path structure for delayed messages
  _ = rabbit_khepri:put(khepri_messages_path(), #{}),
  %% Create the index path for timestamp-based ordering
  _ = rabbit_khepri:put(khepri_index_path(), #{}),
  ok.

disable_plugin() ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() ->
                        _ = mnesia:delete_table(?INDEX_TABLE_NAME),
                        _ = mnesia:delete_table(?TABLE_NAME),
                        ok
                    end,
          khepri => fun() ->
                        _ = rabbit_khepri:delete(khepri_delayed_path()),
                        ok
                    end}
     ).

select_messages_delayed(Pattern) ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> select_messages_delayed_mnesia(Pattern) end,
          khepri => fun() -> select_messages_delayed_khepri(Pattern) end}
    ).

select_messages_delayed_mnesia(Pattern) ->
    mnesia:dirty_select(?TABLE_NAME, [{Pattern, [], [true]}]).

select_messages_delayed_khepri(_Pattern) ->
    Path = khepri_messages_path(),
    case rabbit_khepri:get_many(Path) of
        {ok, Messages} -> maps:to_list(Messages);
        {error, _} -> []
    end.

get_messages_delayed(Key) ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> get_messages_delayed_mnesia(Key) end,
          khepri => fun() -> get_messages_delayed_khepri(Key) end}
    ).

get_messages_delayed_mnesia(Key) ->
    mnesia:dirty_read(?TABLE_NAME, Key).

get_messages_delayed_khepri(Key) ->
    rabbit_khepri:get(khepri_messages_path() ++ [Key]).

delete_message_delayed(Key) ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> delete_message_delayed_mnesia(Key) end,
          khepri => fun() -> delete_message_delayed_khepri(Key) end}
    ).

delete_message_delayed_mnesia(Key) ->
    mnesia:dirty_delete(?TABLE_NAME, Key).

delete_message_delayed_khepri(Key) ->
    rabbit_khepri:delete(khepri_messages_path() ++ [Key]).

delete_index_delayed(Key) ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> delete_index_delayed_mnesia(Key) end,
          khepri => fun() -> delete_index_delayed_khepri(Key) end}
    ).

delete_index_delayed_mnesia(Key) ->
    mnesia:dirty_delete(?INDEX_TABLE_NAME, Key).

delete_index_delayed_khepri(Key) ->
    rabbit_khepri:delete(khepri_index_path() ++ [Key]).

get_first_index() ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> get_first_index_mnesia() end,
          khepri => fun() -> get_first_index_khepri() end}
    ).

get_first_index_mnesia() ->
    case mnesia:dirty_first(?INDEX_TABLE_NAME) of
        %% destructuring to prevent matching '$end_of_table'
        #delay_key{} = Key ->
            Key;
        _ ->
            %% nothing to do
            undefined
    end.

get_first_index_khepri() ->
      case rabbit_khepri:get_many(khepri_index_path()) of
          {ok, []} ->
              undefined;
          {ok, IndexEntries} ->
              %% Find the entry with the earliest timestamp
              SortedEntries = lists:sort(fun(#delay_key{timestamp = TS1},
                                             #delay_key{timestamp = TS2}
                                            ) ->
                                                 TS1 =< TS2
                                         end, maps:values(IndexEntries)),
              case SortedEntries of
                  [FirstKey | _] ->
                      FirstKey;
                  [] ->
                      undefined
              end;
          {error, _} ->
              undefined
      end.

put_index_delayed(Index) ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> put_index_delayed_mnesia(Index) end,
          khepri => fun() -> put_index_delayed_khepri(Index) end}
    ).

put_index_delayed_mnesia(Index) ->
    mnesia:dirty_write(?INDEX_TABLE_NAME, Index).

put_index_delayed_khepri(Index) ->
    rabbit_khepri:put(khepri_index_path(), Index).

put_message_delayed(Message) ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> put_message_delayed_mnesia(Message) end,
          khepri => fun() -> put_message_delayed_khepri(Message) end}
    ).

put_message_delayed_mnesia(Message) ->
    mnesia:dirty_write(?TABLE_NAME, Message).

put_message_delayed_khepri(Message) ->
    rabbit_khepri:put(khepri_messages_path(), Message).

khepri_delayed_path() ->
  [rabbit_exchange_type_delayed].

khepri_messages_path() ->
  khepri_delayed_path() ++ [messages].

khepri_index_path() ->
  khepri_delayed_path() ++ [index].
