-define(TABLE_NAME,
        list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(node()))
       ).
-define(INDEX_TABLE_NAME,
        list_to_atom(atom_to_list(?TABLE_NAME) ++ "_index")
       ).

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
