-ifndef(RABBIT_DELAYED_MESSAGE_HRL).
-define(RABBIT_DELAYED_MESSAGE_HRL, true).

-define(TABLE_NAME, rabbit_delayed_message_utils:append_to_atom(rabbit_delayed_message, node())).
-define(INDEX_TABLE_NAME, rabbit_delayed_message_utils:append_to_atom(?TABLE_NAME, "_index")).

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


-endif.
