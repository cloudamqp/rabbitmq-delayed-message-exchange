PROJECT = rabbitmq_delayed_message_exchange
PROJECT_DESCRIPTION = RabbitMQ Delayed Message Exchange
PROJECT_MOD = rabbit_delayed_message_app

RABBITMQ_VERSION ?= v4.3.1
current_rmq_ref = $(RABBITMQ_VERSION)

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, ["4.3.0"]}
endef

dep_amqp_client                = git_rmq-subfolder rabbitmq-erlang-client $(RABBITMQ_VERSION)
dep_rabbit_common              = git_rmq-subfolder rabbitmq-common $(RABBITMQ_VERSION)
dep_rabbit                     = git_rmq-subfolder rabbitmq-server $(RABBITMQ_VERSION)
dep_rabbitmq_ct_client_helpers = git_rmq-subfolder rabbitmq-ct-client-helpers $(RABBITMQ_VERSION)
dep_rabbitmq_ct_helpers        = git_rmq-subfolder rabbitmq-ct-helpers $(RABBITMQ_VERSION)
dep_leveled                = git https://github.com/martinsumner/leveled.git develop-3.4
# lz4 and zstd are omitted from DEPS intentionally: we don't want NIFs in the
# plugin. leveled is configured to use no compression, so neither NIF is needed
# at runtime. They are still fetched transitively by leveled for compilation
# but must not be started as OTP applications.
DEPS = rabbit_common rabbit leveled
export IGNORE_DEPS += eqwalizer_support
TEST_DEPS = ct_helper rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client meck
dep_ct_helper = git https://github.com/extend/ct_helper.git master

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# Strip lz4 and zstd from leveled's OTP application list. Patching the source
# .app.src file means the generated leveled.app will already have them absent
# when leveled is compiled. See comment above DEPS for why the patch is needed
autopatch-leveled::
	$(verbose) if [ -f $(DEPS_DIR)/leveled/src/leveled.app.src ]; then \
		erl -noshell -eval 'F = "$(DEPS_DIR)/leveled/src/leveled.app.src", {ok, [{application, Name, Props}]} = file:consult(F), Apps = proplists:get_value(applications, Props, []), Apps2 = Apps -- [lz4, zstd], case Apps2 =:= Apps of true -> ok; false -> Props2 = lists:keystore(applications, 1, Props, {applications, Apps2}), ok = file:write_file(F, io_lib:format("~tp.~n", [{application, Name, Props2}])) end' -s init stop; \
	fi
