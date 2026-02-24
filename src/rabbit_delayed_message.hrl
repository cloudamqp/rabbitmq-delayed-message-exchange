-ifndef(RABBIT_DELAYED_MESSAGE_HRL).
-define(RABBIT_DELAYED_MESSAGE_HRL, true).

% We need to inform rabbit about our mnesia tables so that it can trigger its
% migration when khepri is enabled. This is done through the
% `rabbit_mnesia_tables_to_khepri_db` module attribute.
% The table name in the delayed plugin is different compared to other plugins,
% since the delayed plugin includes the current `node()` in the table name.
% Therefore, we cannot pass the table name to the module attribute (calling
% `list_to_atom` and similar funcitons is not allowed).
% Instead we need to pass a `FAKE_TABLE_NAME` and when rabbit asks our conversion module to migrate to khepri, we can use the correct `TABLE_NAME`.
-define(FAKE_TABLE_NAME, rabbit_delayed_message).

-define(TABLE_NAME, rabbit_delayed_message_utils:append_to_atom(rabbit_delayed_message, node())).
-define(INDEX_TABLE_NAME, rabbit_delayed_message_utils:append_to_atom(?TABLE_NAME, "_index")).


-endif.
