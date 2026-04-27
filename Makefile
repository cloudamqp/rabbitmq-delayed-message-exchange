PROJECT = rabbitmq_delayed_message_exchange
PROJECT_DESCRIPTION = RabbitMQ Delayed Message Exchange
PROJECT_MOD = rabbit_delayed_message_app

RABBITMQ_VERSION ?= v4.2.0
current_rmq_ref = $(RABBITMQ_VERSION)

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, ["4.2.0"]}
endef

dep_amqp_client                = git_rmq-subfolder rabbitmq-erlang-client $(RABBITMQ_VERSION)
dep_rabbit_common              = git_rmq-subfolder rabbitmq-common $(RABBITMQ_VERSION)
dep_rabbit                     = git_rmq-subfolder rabbitmq-server $(RABBITMQ_VERSION)
dep_rabbitmq_ct_client_helpers = git_rmq-subfolder rabbitmq-ct-client-helpers $(RABBITMQ_VERSION)
dep_rabbitmq_ct_helpers        = git_rmq-subfolder rabbitmq-ct-helpers $(RABBITMQ_VERSION)
dep_leveled                = git https://github.com/martinsumner/leveled.git develop-3.4
# lz4 and zstd are omitted from DEPS intentionally: OTP 28 ships its own stdlib
# zstd module which clashes with the OpenRiak NIF, and RabbitMQ 4.2.0 ships an
# incompatible lz4. leveled is configured to use no compression, so neither NIF
# is needed at runtime. They are still fetched transitively by leveled for
# compilation but must not be started as OTP applications.
DEPS = rabbit_common rabbit leveled
export IGNORE_DEPS += eqwalizer_support
TEST_DEPS = ct_helper rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client meck eflame
dep_ct_helper = git https://github.com/extend/ct_helper.git master
dep_eflame    = git https://github.com/slfritchie/eflame.git master

# Only the integration suite runs under `make ct` / `make tests`.
# The benchmark suite is excluded here and invoked via `make benchmarks`.
CT_SUITES = plugin

BENCH_LOGS_DIR  ?= $(CURDIR)/logs/benchmarks
FLAMEGRAPH_DIR  ?= $(CURDIR)/flamegraphs
FLAMEGRAPH_PL   ?= flamegraph.pl

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# After deps are fetched, strip lz4 and zstd from leveled's OTP application
# dependency list so they are never started. See comment above DEPS for why.
deps:: patch-leveled-app

patch-leveled-app:
	$(verbose) if [ -f $(DEPS_DIR)/leveled/ebin/leveled.app ]; then \
		erl -noshell -eval 'F = "$(DEPS_DIR)/leveled/ebin/leveled.app", {ok, [{application, Name, Props}]} = file:consult(F), Apps = proplists:get_value(applications, Props, []), Apps2 = Apps -- [lz4, zstd], case Apps2 =:= Apps of true -> ok; false -> Props2 = lists:keystore(applications, 1, Props, {applications, Apps2}), ok = file:write_file(F, io_lib:format("~tp.~n", [{application, Name, Props2}])) end' -s init stop; \
	fi

benchmarks: test-build
	$(verbose) mkdir -p $(BENCH_LOGS_DIR)
	$(gen_verbose) WITH_TIMERS=true $(CT_RUN) \
		-sname ct_$(PROJECT)_bench \
		-suite benchmark_SUITE \
		-logdir $(BENCH_LOGS_DIR) \
		$(CT_OPTS)

# Runs the benchmark suite with eflame profiling enabled.
# Each benchmark operation writes a collapsed-stack file under FLAMEGRAPH_DIR.
# Convert to SVGs afterwards with: make flamegraphs-svg
flamegraphs: test-build
	$(verbose) mkdir -p $(BENCH_LOGS_DIR) $(FLAMEGRAPH_DIR)
	$(gen_verbose) WITH_TIMERS=false FLAMEGRAPH_DIR=$(FLAMEGRAPH_DIR) $(CT_RUN) \
		-sname ct_$(PROJECT)_bench \
		-suite benchmark_SUITE \
		-logdir $(BENCH_LOGS_DIR) \
		$(CT_OPTS)

# Converts all .out files produced by `make flamegraphs` into SVGs.
# eflame writes one raw sample per line as "<pid>;frame1;...;leaf".
# The pipeline strips the PID, counts duplicate stacks, then feeds
# the resulting "stack count" lines to flamegraph.pl.
# Requires flamegraph.pl on PATH, or override: make flamegraphs-svg FLAMEGRAPH_PL=/path/to/flamegraph.pl
flamegraphs-svg:
	$(verbose) for f in $(FLAMEGRAPH_DIR)/*.out; do \
		sed 's/^<[^>]*>;//' "$$f" \
			| sort | uniq -c \
			| awk '{n=$$1; sub(/^[[:space:]]*[0-9]+[[:space:]]/, ""); print $$0 " " n}' \
			| $(FLAMEGRAPH_PL) --colors=hot \
			> "$${f%.out}.svg"; \
	done
	@echo $(FLAMEGRAPH_DIR)
