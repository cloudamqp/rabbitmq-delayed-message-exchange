%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.

%% Benchmark suite comparing the mnesia and leveled storage backends.
%% Run with: make benchmarks
%%
%% No RabbitMQ broker is required. Broker infrastructure dependencies
%% (rabbit_table, rabbit_mnesia) are mocked with meck. The backend
%% functions under measurement are wrapped with meck expectations that
%% record timer:tc timings into an ETS table.

-module(benchmark_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%% Number of messages used in each benchmark scenario.
-define(LOADS, [100, 1000, 5000]).

%% Number of repeated calls in bench_get_first_delay and bench_get_many.
-define(SCAN_REPS, 100).

%% ETS table that accumulates {FunctionName, Microseconds} timing entries.
-define(TIMINGS_TABLE, benchmark_timings).

%% persistent_term key for the cross-testcase result accumulator.
-define(RESULTS_KEY, benchmark_results).

%% Exchange name used for all benchmark messages.
-define(BENCH_EXCHANGE_NAME, <<"bench-exchange">>).

%% -------------------------------------------------------------------
%% CT callbacks
%% -------------------------------------------------------------------

all() ->
    [{group, mnesia}, {group, leveled}].

groups() ->
    Benchmarks = [
        bench_store_delay,
        bench_get_first_delay,
        bench_get_many,
        bench_delete,
        bench_full_cycle
    ],
    [{mnesia,  [sequence], Benchmarks},
     {leveled, [sequence], Benchmarks}].

init_per_suite(Config) ->
    persistent_term:put(?RESULTS_KEY, []),
    Config.

end_per_suite(_Config) ->
    Results = persistent_term:get(?RESULTS_KEY, []),
    persistent_term:erase(?RESULTS_KEY),
    print_summary(Results),
    ok.

%% Mnesia group: start mnesia with a temporary directory and mock
%% rabbit_table:wait/1 so setup/0 does not require a running broker.
init_per_group(mnesia, Config) ->
    TmpDir = make_temp_dir("bench_mnesia"),
    application:set_env(mnesia, dir, TmpDir),
    ok = mnesia:create_schema([node()]),
    ok = mnesia:start(),
    meck:new(rabbit_table, [non_strict, passthrough, no_link]),
    meck:expect(rabbit_table, wait, fun(_Tables) -> ok end),
    install_timing_wrappers(rabbit_delayed_message_mnesia),
    [{backend, rabbit_delayed_message_mnesia}, {tmp_dir, TmpDir} | Config];
%% Leveled group: mock rabbit_mnesia:dir/0 to provide a temporary data
%% path instead of requiring the rabbit application to be running.
init_per_group(leveled, Config) ->
    TmpDir = make_temp_dir("bench_leveled"),
    meck:new(rabbit_mnesia, [non_strict, passthrough, no_link]),
    meck:expect(rabbit_mnesia, dir, fun() -> TmpDir end),
    install_timing_wrappers(rabbit_delayed_message_leveled),
    [{backend, rabbit_delayed_message_leveled}, {tmp_dir, TmpDir} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(mnesia, Config) ->
    meck:unload(rabbit_delayed_message_mnesia),
    meck:unload(rabbit_table),
    mnesia:stop(),
    remove_temp_dir(proplists:get_value(tmp_dir, Config)),
    Config;
end_per_group(leveled, Config) ->
    meck:unload(rabbit_delayed_message_leveled),
    meck:unload(rabbit_mnesia),
    remove_temp_dir(proplists:get_value(tmp_dir, Config)),
    Config;
end_per_group(_, Config) ->
    Config.

%% init_per_testcase and the test case function run in the same process.
%% Creating the ETS timing table here ensures it is owned by that process
%% and remains alive for the duration of the test case.  The leveled bookie
%% PID (stored in the process dictionary by setup/0) is also accessible
%% throughout the test case for the same reason.
init_per_testcase(Testcase, Config) ->
    Backend = backend(Config),
    ets:new(?TIMINGS_TABLE, [named_table, public, duplicate_bag]),
    ok = Backend:setup(),
    [{benchmark, Testcase} | Config].

end_per_testcase(_Testcase, Config) ->
    Backend = backend(Config),
    _ = Backend:disable_plugin(),
    ets:delete(?TIMINGS_TABLE),
    Config.

%% -------------------------------------------------------------------
%% Benchmark: write throughput
%% -------------------------------------------------------------------

bench_store_delay(Config) ->
    Backend = backend(Config),
    Exchange = make_exchange(?BENCH_EXCHANGE_NAME),
    Msg = make_message(),
    lists:foreach(fun(N) ->
        Now = erlang:system_time(millisecond),
        [Backend:store_delay(Now + I, Exchange, Msg) || I <- lists:seq(1, N)],
        report_timings(store_delay, Backend, N, Config),
        clear_store(Backend)
    end, ?LOADS).

%% -------------------------------------------------------------------
%% Benchmark: index scan (get_first_delay) under load
%% -------------------------------------------------------------------

bench_get_first_delay(Config) ->
    Backend = backend(Config),
    Exchange = make_exchange(?BENCH_EXCHANGE_NAME),
    Msg = make_message(),
    lists:foreach(fun(N) ->
        %% Pre-populate the store so there are N entries to scan.
        Now = erlang:system_time(millisecond),
        [Backend:store_delay(Now + I, Exchange, Msg) || I <- lists:seq(1, N)],
        ets:delete_all_objects(?TIMINGS_TABLE),
        [Backend:get_first_delay() || _ <- lists:seq(1, ?SCAN_REPS)],
        %% N is the store size (the load); actual call count is in Stats.
        report_timings(get_first_delay, Backend, N, Config),
        clear_store(Backend)
    end, ?LOADS).

%% -------------------------------------------------------------------
%% Benchmark: bulk read (get_many) with N messages per batch
%% -------------------------------------------------------------------

bench_get_many(Config) ->
    Backend = backend(Config),
    Exchange = make_exchange(?BENCH_EXCHANGE_NAME),
    Msg = make_message(),
    lists:foreach(fun(N) ->
        %% Store N messages all at the same timestamp so that a single
        %% get_many call returns all N of them in one scan.
        Now = erlang:system_time(millisecond),
        [Backend:store_delay(Now, Exchange, Msg) || _ <- lists:seq(1, N)],
        {_TS, Key} = Backend:get_first_delay(),
        ets:delete_all_objects(?TIMINGS_TABLE),
        %% Repeat the retrieval to get stable timing numbers.
        [Backend:get_many(Key) || _ <- lists:seq(1, ?SCAN_REPS)],
        %% N is the batch size (messages returned per call); actual call count is in Stats.
        report_timings(get_many, Backend, N, Config),
        clear_store(Backend)
    end, ?LOADS).

%% -------------------------------------------------------------------
%% Benchmark: delete throughput (drain the store)
%% -------------------------------------------------------------------

bench_delete(Config) ->
    Backend = backend(Config),
    Exchange = make_exchange(?BENCH_EXCHANGE_NAME),
    Msg = make_message(),
    lists:foreach(fun(N) ->
        Now = erlang:system_time(millisecond),
        [Backend:store_delay(Now + I, Exchange, Msg) || I <- lists:seq(1, N)],
        ets:delete_all_objects(?TIMINGS_TABLE),
        %% Drain all N messages.  Each iteration calls get_first_delay
        %% then delete (the production pattern).
        drain_store(Backend),
        report_timings(delete,          Backend, N, Config),
        report_timings(get_first_delay, Backend, N, Config)
    end, ?LOADS).

%% -------------------------------------------------------------------
%% Benchmark: full end-to-end cycle per message
%% -------------------------------------------------------------------

bench_full_cycle(Config) ->
    Backend = backend(Config),
    Exchange = make_exchange(?BENCH_EXCHANGE_NAME),
    Msg = make_message(),
    lists:foreach(fun(N) ->
        Now = erlang:system_time(millisecond),
        lists:foreach(fun(I) ->
            Backend:store_delay(Now + I, Exchange, Msg),
            {_TS, Key} = Backend:get_first_delay(),
            Backend:get_many(Key),
            Backend:delete(Key),
            Backend:delete_index(Key)
        end, lists:seq(1, N)),
        report_timings(store_delay,     Backend, N, Config),
        report_timings(get_first_delay, Backend, N, Config),
        report_timings(get_many,        Backend, N, Config),
        report_timings(delete,          Backend, N, Config)
    end, ?LOADS).

%% -------------------------------------------------------------------
%% meck timing wrapper installation
%%
%% A passthrough mock is created for Backend.  For each benchmarked
%% function an expectation is installed that wraps the real call in
%% timer:tc and records the elapsed microseconds in the ETS table.
%% Functions without explicit expectations fall through to the
%% original implementation unchanged.
%% -------------------------------------------------------------------

install_timing_wrappers(Backend) ->
    meck:new(Backend, [passthrough, no_link]),
    meck:expect(Backend, store_delay,
        fun(TS, Exchange, Msg) ->
            {Time, Res} = timer:tc(fun() -> meck:passthrough([TS, Exchange, Msg]) end),
            ets:insert(?TIMINGS_TABLE, {store_delay, Time}),
            Res
        end),
    meck:expect(Backend, get_first_delay,
        fun() ->
            {Time, Res} = timer:tc(fun() -> meck:passthrough([]) end),
            ets:insert(?TIMINGS_TABLE, {get_first_delay, Time}),
            Res
        end),
    meck:expect(Backend, get_many,
        fun(Key) ->
            {Time, Res} = timer:tc(fun() -> meck:passthrough([Key]) end),
            ets:insert(?TIMINGS_TABLE, {get_many, Time}),
            Res
        end),
    meck:expect(Backend, delete,
        fun(Key) ->
            {Time, Res} = timer:tc(fun() -> meck:passthrough([Key]) end),
            ets:insert(?TIMINGS_TABLE, {delete, Time}),
            Res
        end).

%% -------------------------------------------------------------------
%% Helpers: store lifecycle
%% -------------------------------------------------------------------

%% Clears all entries from the store between load-size runs.
clear_store(rabbit_delayed_message_mnesia) ->
    _ = mnesia:clear_table(rabbit_delayed_message_mnesia:table_name()),
    _ = mnesia:clear_table(rabbit_delayed_message_mnesia:index_table_name()),
    ok;
clear_store(Backend) ->
    drain_store(Backend),
    ets:delete_all_objects(?TIMINGS_TABLE).

%% Repeatedly fetches the earliest entry and deletes it until the
%% store is empty.  This is the production delivery pattern.
drain_store(Backend) ->
    case Backend:get_first_delay() of
        undefined ->
            ok;
        {_TS, Key} ->
            Backend:delete(Key),
            Backend:delete_index(Key),
            drain_store(Backend)
    end.

%% -------------------------------------------------------------------
%% Helpers: reporting
%% -------------------------------------------------------------------

report_timings(FunName, Backend, N, Config) ->
    Timings = [T || {_, T} <- ets:lookup(?TIMINGS_TABLE, FunName)],
    ets:match_delete(?TIMINGS_TABLE, {FunName, '_'}),
    Stats = compute_stats(Timings),
    print_stats(FunName, Backend, N, Stats),
    accumulate_result(Config, FunName, Backend, N, Stats).

accumulate_result(_Config, _FunName, _Backend, _N, #{count := 0}) ->
    ok;
accumulate_result(Config, FunName, Backend, N, Stats) ->
    Benchmark = proplists:get_value(benchmark, Config),
    Result = #{benchmark => Benchmark,
               op        => FunName,
               load      => N,
               backend   => Backend,
               stats     => Stats},
    Acc = persistent_term:get(?RESULTS_KEY, []),
    persistent_term:put(?RESULTS_KEY, [Result | Acc]).

print_stats(_FunName, _Backend, _N, #{count := 0}) ->
    ok;
print_stats(FunName, Backend, N, Stats) ->
    #{count   := Count,
      total   := TotalUs,
      mean    := MeanUs,
      min     := MinUs,
      max     := MaxUs,
      p95     := P95Us,
      p99     := P99Us} = Stats,
    ct:pal(
        "~n"
        "Backend : ~s~n"
        "Op      : ~s~n"
        "Load    : ~B messages~n"
        "---~n"
        "Calls   : ~B~n"
        "Total   : ~s~n"
        "Mean    : ~s~n"
        "Min     : ~s~n"
        "Max     : ~s~n"
        "P95     : ~s~n"
        "P99     : ~s~n",
        [backend_label(Backend),
         FunName,
         N,
         Count,
         fmt_us(TotalUs),
         fmt_us(MeanUs),
         fmt_us(MinUs),
         fmt_us(MaxUs),
         fmt_us(P95Us),
         fmt_us(P99Us)]).

%% Prints a single table with all accumulated results once the suite
%% finishes, sorted so that mnesia and leveled appear on adjacent rows
%% for the same (benchmark, op, load) combination.
print_summary([]) ->
    ok;
print_summary(Results) ->
    Sorted = lists:sort(fun(A, B) -> sort_key(A) =< sort_key(B) end, Results),
    %% Column widths: Scenario(22) Op(16) Load(6) Backend(8) Calls(6) Mean(12) P99(12)
    Widths = [22, 16, 6, 8, 6, 12, 12],
    Header = row(Widths, ["Scenario", "Op", "Load", "Backend", "Calls", "Mean", "P99"]),
    Sep    = lists:duplicate(lists:sum(Widths) + length(Widths) - 1, $-),
    Rows   = [result_row(Widths, R) || R <- Sorted],
    ct:pal("~n====== Benchmark Summary ======~n~n~s~n~s~n~s~n",
           [Header, Sep, string:join(Rows, "\n")]).

result_row(Widths, #{benchmark := Bench,
                     op        := Op,
                     load      := Load,
                     backend   := Backend,
                     stats     := #{mean := Mean, p99 := P99, count := Count}}) ->
    row(Widths, [atom_to_list(Bench),
                 atom_to_list(Op),
                 integer_to_list(Load),
                 backend_label(Backend),
                 integer_to_list(Count),
                 fmt_us_ascii(Mean),
                 fmt_us_ascii(P99)]).

%% Formats a list of cells into a fixed-width row.  All cells are
%% left-aligned (trailing spaces) within their column width.
row(Widths, Cells) ->
    Padded = lists:zipwith(
        fun(W, Cell) ->
            string:pad(lists:flatten(io_lib:format("~s", [Cell])), W)
        end, Widths, Cells),
    string:join(Padded, " ").

sort_key(#{benchmark := B, load := L, op := Op, backend := Backend}) ->
    {bench_order(B), L, op_order(Op), backend_order(Backend)}.

bench_order(bench_store_delay)     -> 1;
bench_order(bench_get_first_delay) -> 2;
bench_order(bench_get_many)        -> 3;
bench_order(bench_delete)          -> 4;
bench_order(bench_full_cycle)      -> 5;
bench_order(_)                     -> 9.

op_order(store_delay)     -> 1;
op_order(get_first_delay) -> 2;
op_order(get_many)        -> 3;
op_order(delete)          -> 4;
op_order(_)               -> 9.

backend_order(rabbit_delayed_message_mnesia)  -> 1;
backend_order(rabbit_delayed_message_leveled) -> 2.

compute_stats([]) ->
    #{count => 0};
compute_stats(Timings) ->
    Sorted = lists:sort(Timings),
    Count  = length(Sorted),
    Total  = lists:sum(Sorted),
    Mean   = Total div Count,
    Min    = hd(Sorted),
    Max    = lists:last(Sorted),
    P95    = lists:nth(max(1, ceil(0.95 * Count)), Sorted),
    P99    = lists:nth(max(1, ceil(0.99 * Count)), Sorted),
    #{count => Count,
      total => Total,
      mean  => Mean,
      min   => Min,
      max   => Max,
      p95   => P95,
      p99   => P99}.

fmt_us(Us) when Us < 1000 ->
    io_lib:format("~B µs", [Us]);
fmt_us(Us) when Us < 1_000_000 ->
    io_lib:format("~.2f ms", [Us / 1000]);
fmt_us(Us) ->
    io_lib:format("~.3f s", [Us / 1_000_000]).

%% Plain ASCII variant used in the summary table where µ would break alignment.
fmt_us_ascii(Us) when Us < 1000 ->
    io_lib:format("~B us", [Us]);
fmt_us_ascii(Us) when Us < 1_000_000 ->
    io_lib:format("~.2f ms", [Us / 1000]);
fmt_us_ascii(Us) ->
    io_lib:format("~.3f s", [Us / 1_000_000]).

backend_label(rabbit_delayed_message_mnesia)  -> "mnesia";
backend_label(rabbit_delayed_message_leveled) -> "leveled".

%% -------------------------------------------------------------------
%% Helpers: data construction
%% -------------------------------------------------------------------

make_exchange(Name) ->
    #exchange{
        name        = #resource{virtual_host = <<"/">>,
                                kind         = exchange,
                                name         = Name},
        type        = 'x-delayed-message',
        durable     = true,
        auto_delete = false,
        internal    = false,
        arguments   = [],
        policy      = undefined
    }.

%% A 256-byte payload representative of a small but non-trivial message.
make_message() ->
    binary:copy(<<"x">>, 256).

backend(Config) ->
    proplists:get_value(backend, Config).

%% -------------------------------------------------------------------
%% Helpers: temporary directories
%% -------------------------------------------------------------------

make_temp_dir(Prefix) ->
    Dir = filename:join(["/tmp", Prefix ++ "_" ++
                         integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_path(Dir),
    Dir.

remove_temp_dir(undefined) ->
    ok;
remove_temp_dir(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.
