# RabbitMQ Delayed Message Plugin

This is a fork of https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
which is no longer maintained by Team RabbitMQ. The goal of this fork is to
support newer RabbitMQ versions that are using the Khepri metadata store.

This plugin adds delayed-messaging (or scheduled-messaging) to RabbitMQ.

Its current design has **significant limitations** (documented below)
consider [the alternatives on the original repo](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange#alternatives-available).

If you accept the limitations, please read on.

## The Basics

With this plugin [enabled](https://www.rabbitmq.com/docs/plugins), a user can declare an exchange with the type `x-delayed-message` and
then publish messages with the custom header `x-delay` expressing in
milliseconds a delay time for the message. The message will be
delivered to the respective queues after `x-delay` milliseconds.

## Intended Use Cases

This plugin was designed for delaying message publishing for a number of seconds, minutes, or hours.
A day or two at most.

It is **not a longer term scheduling solution**. If you need to delay publishing by days, weeks, months, or years,
consider using a data store suitable for long-term storage, and an external scheduling tool
of some kind.


## Supported RabbitMQ Versions

This version of the plugin requires **RabbitMQ 4.3.3 or later**.

Each build of this plugin pins to a specific RabbitMQ patch version (see `RABBITMQ_VERSION` in the `Makefile`).

This version of the plugin requires the `khepri_db` feature flag to be enabled
(the default in RabbitMQ 4.2+). Delayed messages are stored in a
[Leveled](https://github.com/martinsumner/leveled) LSM-tree database local to each node.

## Supported Erlang/OTP Versions

The latest version of this plugin [requires Erlang 27.0 or later versions](https://www.rabbitmq.com/docs/which-erlang).
RabbitMQ 4.3.3 dropped support for Erlang 26, so Erlang 26 nodes cannot run this
version of the plugin.


## Feature Flags

This plugin ships a [stable feature flag](https://www.rabbitmq.com/docs/feature-flags),
`delayed_message_topic_projection_v2`. It selects the internal Khepri projection
the plugin uses to route messages whose `x-delayed-type` is `topic`, once their
delay elapses:

 * when disabled, the plugin uses a single-table projection (v1)
 * when enabled, the plugin uses a two-table projection (v2) that mirrors the
   core broker's v5 topic trie projection

The v5 topic trie projection was introduced in [RabbitMQ
4.3.1](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.3.1) as part
of a routing bugfix. The flag depends on `topic_binding_projection_v5`, so it
can only be enabled once every node runs RabbitMQ 4.3.1 or later with the
`topic_binding_projection_v5` flag enabled.

Enabling the flag does not change the plugin's routing results, only its
internal representation.

On a new cluster the flag is enabled by default once the plugin is enabled, so
no action is needed. When upgrading from a plugin version that predates the flag,
enable it once every node has been upgraded:

``` bash
rabbitmqctl enable_feature_flag delayed_message_topic_projection_v2
```

The migration is safe to run on a live cluster: the plugin keeps routing through
the v1 projection while the v2 projection is registered, and only removes the v1
projection after the flag is enabled cluster-wide.


## Installation

### Download a Binary Build

Binary builds are distributed [via GitHub releases](https://github.com/cloudamqp/rabbitmq-delayed-message-exchange/releases).

As with all 3rd party plugins, the `.ez` files the release provides must be copied into the [node's plugins directory](https://rabbitmq.com/plugins.html#plugin-directories)
with sufficient permissions for the effective user of the RabbitMQ process to load it from disk.

To find out what the plugins directory is, use `rabbitmq-plugins directories`

``` bash
rabbitmq-plugins directories -s
```

### Enabling the Plugin

To enable the plugin run the following command:

``` bash
rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

## Usage ##

To use the delayed-messaging feature, declare an exchange with the
type `x-delayed-message`:


```java
// ... elided code ...
Map<String, Object> args = new HashMap<String, Object>();
args.put("x-delayed-type", "direct");
channel.exchangeDeclare("my-exchange", "x-delayed-message", true, false, args);
// ... more code ...
```

Note that we pass an extra header called `x-delayed-type`, more on it
under the _Routing_ section.

Once we have the exchange declared we can publish messages providing a
header telling the plugin for how long to delay our messages:

```java
// ... elided code ...
byte[] messageBodyBytes = "delayed payload".getBytes("UTF-8");
Map<String, Object> headers = new HashMap<String, Object>();
headers.put("x-delay", 5000);
AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder().headers(headers);
channel.basicPublish("my-exchange", "", props.build(), messageBodyBytes);

byte[] messageBodyBytes2 = "more delayed payload".getBytes("UTF-8");
Map<String, Object> headers2 = new HashMap<String, Object>();
headers2.put("x-delay", 1000);
AMQP.BasicProperties.Builder props2 = new AMQP.BasicProperties.Builder().headers(headers2);
channel.basicPublish("my-exchange", "", props2.build(), messageBodyBytes2);
// ... more code ...
```

In the above example we publish two messages, specifying the delay
time with the `x-delay` header. For this example, the plugin will
deliver to our queues first the message with the body `"more delayed
payload"` and then the one with the body `"delayed payload"`.

If the `x-delay` header is not present, then the plugin will proceed
to route the message without delay.

## Routing ##

This plugin allows for flexible routing via the `x-delayed-type`
arguments that can be passed during `exchange.declare`. In the example
above we used `"direct"` as exchange type. That means the plugin
will have the same routing behavior shown by the direct exchange.

If you want a different routing behavior, then you could provide a
different exchange type, like `"topic"` for example. You can also
specify exchange types provided by plugins. Note that this argument is
**required** and **must** refer to an **existing exchange type**.

## Performance Impact ##

Due to the `"x-delayed-type"` argument, one could use this exchange in
place of other exchanges, since the `"x-delayed-message"` exchange
will just act as proxy. Note that there might be some performance
implications if you do this.

For each message that crosses an `"x-delayed-message"` exchange, the
plugin will try to determine if the message has to be expired by
making sure the delay is within range, i.e.: `Delay > 0, Delay =<
?ERL_MAX_T` (In Erlang a timer can be set up to (2^32)-1 milliseconds
in the future).

If the previous condition holds, then the message is persisted. The message body
goes into a Leveled LSM-tree database on disk, and an entry keyed by the scheduled
delivery time stamp is inserted into an in-memory `ordered_set` ETS index that drives
next-timer selection. Some other logic will then kick in to determine if this
particular message delay needs to replace the current scheduled timer and so on.

This means that while one _could_ use this exchange in place of a
_direct_ or _fanout_ exchange (or any other exchange for that matter),
_it will be slower_ than using the actual exchange. If you don't need
to delay messages, then use the actual exchange.


## Storage Characteristics

The Leveled-based storage has two notable characteristics:

 * **Smaller memory footprint**: delayed messages are not kept in memory in their entirety.
   Only an index is held in memory while the message bodies live in the Leveled LSM-tree on disk,
   resulting in a considerably smaller memory footprint.
 * **Stable behavior under scheduling collisions**: write and startup times remain stable even when
   a large number of messages are scheduled to the exact same expiry timestamp.

## Configuration

The plugin reads its configuration from `rabbitmq.conf`:

```ini
delayed_message_exchange.journal_compaction_interval_seconds = 600
```

 * `journal_compaction_interval_seconds`: how often, in seconds, the plugin asks Leveled to
   compact its journal. Defaults to `600` (10 minutes); `0` disables the periodic runs

Delivering a message appends a tombstone to the Leveled journal instead of freeing the space the
message occupied, and that space only comes back when the journal is compacted. Leveled leaves the
scheduling of journal compaction to the application embedding it, so the plugin has to ask for it,
and this setting controls how often. Note that a run can only reclaim journal entries that the
on-disk ledger already covers, so a store that has seen little traffic since the node started may
hold on to its journal even though the messages in it have all been delivered.

`rabbit_delayed_message:compact_journal()` runs a compaction immediately, independently of the
schedule. It returns `ok` once the run has been handed to Leveled, `busy` when the previous run has
not finished yet (the periodic runs skip for the same reason), `not_running` when the store is not
up, and `{error, Reason}` if the request could not be made.

The same setting can be given in the `rabbitmq_delayed_message_exchange` application environment
in [`advanced.config`](https://www.rabbitmq.com/docs/configure#advanced-config-file):

```erlang
[
  {rabbitmq_delayed_message_exchange, [
    {journal_compaction_interval_seconds, 600}
  ]}
].
```

Both files are only read at boot. To change the interval on a running node, set the application
environment variable and ask the plugin to re-read its configuration:

```erlang
application:set_env(rabbitmq_delayed_message_exchange,
                    journal_compaction_interval_seconds, 60),
rabbit_delayed_message:refresh_config().
```

### Tuning the Leveled Store

The settings below are passed straight to the Leveled bookie the plugin keeps its messages in,
under the name Leveled gives them, so Leveled's
[startup options](https://github.com/martinsumner/leveled/blob/develop-3.4/docs/STARTUP_OPTIONS.md)
documentation applies to them as well. Leaving one unset leaves Leveled's own default in place.
They are only read when the store starts, so a change needs a node restart (or a plugin disable and
re-enable), unlike the compaction interval:

```ini
delayed_message_exchange.leveled.max_run_length = 8
delayed_message_exchange.leveled.singlefile_compactionpercentage = 30.0
delayed_message_exchange.leveled.maxrunlength_compactionpercentage = 70.0
delayed_message_exchange.leveled.journalcompaction_scoreonein = 1
delayed_message_exchange.leveled.max_journalobjectcount = 200000
delayed_message_exchange.leveled.max_journalsize = 1GB
delayed_message_exchange.leveled.waste_retention_period_seconds = 3600
```

 * `max_run_length`: largest number of consecutive journal files one compaction run may rewrite.
   This is what caps how long a run can take. Defaults to `8`
 * `singlefile_compactionpercentage`: percentage of a journal file that may be kept for that file
   to still be worth compacting on its own. Defaults to `30.0`, meaning a file has to be at least
   70% reclaimable
 * `maxrunlength_compactionpercentage`: the same percentage for a run of `max_run_length` files,
   interpolated for shorter runs. Defaults to `70.0`, and has to be at or above
   `singlefile_compactionpercentage`
 * `journalcompaction_scoreonein`: probability (1 in this many) that a journal file is rescored on
   a run instead of reusing its cached score. Defaults to `1`, which rescores every file on every
   run; higher values trade accuracy for less scan I/O
 * `max_journalobjectcount`: number of entries after which a journal file is rolled. Only a rolled
   file can be compacted, so this is also the unit of reclaim. Defaults to `200000`
 * `max_journalsize`: size at which a journal file is rolled, whichever comes first with
   `max_journalobjectcount`. Defaults to `1GB`, and Leveled caps it at 4GB
 * `waste_retention_period_seconds`: how long journal files that compaction has replaced are kept
   in a waste directory instead of being deleted. Unset by default, which deletes them; setting it
   keeps the disk space for that long, so it is only useful for debugging

In `advanced.config` these are `rabbitmq_delayed_message_exchange` application environment keys of
the same name, except for `waste_retention_period_seconds`, whose environment key is Leveled's own
`waste_retention_period`:

```erlang
[
  {rabbitmq_delayed_message_exchange, [
    {max_run_length, 4},
    {singlefile_compactionpercentage, 40.0}
  ]}
].
```

Everything else the plugin passes to the bookie is fixed, including the `recovr` reload strategy
and the metadata extractor, both of which this store depends on.

Values that Leveled would refuse to start with are ignored with a warning in the log rather than
taken, so a typo in `advanced.config` (which, unlike `rabbitmq.conf`, is not validated against the
schema) cannot keep the node from booting. That includes a
`maxrunlength_compactionpercentage`/`singlefile_compactionpercentage` pair in the wrong order, in
which case both fall back to the Leveled defaults.

Leveled's [design notes](https://github.com/martinsumner/leveled/blob/develop-3.4/docs/DESIGN.md)
and [startup options](https://github.com/martinsumner/leveled/blob/develop-3.4/docs/STARTUP_OPTIONS.md)
describe how its journal, ledger and compaction work; the *Max Journal Size*, *Journal Compaction*
and *Waste Retention Period* sections of the latter cover the settings above.


## Limitations

Delayed messages are stored in a Leveled LSM-tree database with a single copy on the current node.
They will survive a node restart. While timer(s) that triggered scheduled delivery are not persisted,
they will be re-initialised during plugin activation on node start.
Obviously, only having one copy of a scheduled message in a cluster means
that losing that node or disabling the plugin on it will lose the
messages residing on that node.

The plugin only performs one attempt at publishing each message but since publishing
is local, in practice the only issue that may prevent delivery is the lack of queues
(or bindings) to route to.

Closely related to the above, the mandatory flag is not supported by this exchange:
we cannot be sure that at the future publishing point in time

 * there is at least one queue we can route to
 * the original connection is still around to send a `basic.return` to

The Leveled-based storage keeps message bodies on disk (see _Storage Characteristics_ above),
but its in-memory ETS index still holds one entry per scheduled message until delivery,
so memory overhead grows linearly with the number of pending messages. Workloads with very large backlogs
(hundreds of thousands or millions of pending messages) should size node memory
accordingly.

## Disabling the Plugin ##

You can disable this plugin by calling `rabbitmq-plugins disable
rabbitmq_delayed_message_exchange` but note that **ALL DELAYED MESSAGES THAT
HAVEN'T BEEN DELIVERED WILL BE LOST**.

## Building the Plugin

```shell
PROJECT_VERSION=4.3.3 PRODUCT_VERSION=4.3.3 VERSION=4.3.3 \
    gmake dist PROJECT_VERSION=4.3.3 PRODUCT_VERSION=4.3.3 \
    VERSION=4.3.3 DIST_AS_EZS=true
```

The EZ file is created in the `plugins` directory.

## Creating a Release

1. Update `RABBITMQ_VERSION` in `Makefile` to the target RabbitMQ release (e.g. `v4.3.3`)
1. Update `broker_version_requirements` in the `PROJECT_APP_EXTRA_KEYS` block of `Makefile` to the oldest patch release the artifacts can load on, which need not be `RABBITMQ_VERSION` but must not predate the first release requiring the Erlang version used to build them
1. Update the RabbitMQ and Erlang versions in `.github/workflows/test.yml` and `.github/workflows/package.yml`
1. Push a tag (i.e. `v4.3.3`) with the matching version
1. The Package workflow (`.github/workflows/package.yml`) builds the `.ez` artifacts on push
1. Attach the produced `rabbitmq_delayed_message_exchange*.ez` and `leveled*.ez` files to a GitHub release

## LICENSE

See the LICENSE file.
