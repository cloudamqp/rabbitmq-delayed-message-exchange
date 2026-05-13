%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_delayed_message_sup).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(supervisor2).

-define(SERVER, ?MODULE).

-export([start_link/0, init/1]).

start_link() ->
    supervisor2:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 3, 10},
          [{rabbit_delayed_message, {rabbit_delayed_message, start_link, []},
            transient, ?WORKER_WAIT, worker, [rabbit_delayed_message]}]}}.
