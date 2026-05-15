# RabbitMQ Delayed Message Plugin

This is a fork of https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
which is no longer maintained by Team RabbitMQ. The goal of this fork is to
support newer RabbitMQ versions.

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

This version of the plugin requires **RabbitMQ 4.2.6 or later** (the minimum version that
supports migration from dynamically named Mnesia tables [rabbitmq/rabbitmq-server#16139](https://github.com/rabbitmq/rabbitmq-server/pull/16139)).

Every [release](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) of this plugin targets one RabbitMQ release series.

When the `khepri_db` feature flag is enabled (the default in RabbitMQ 4.2+), delayed messages are stored
in a [Leveled](https://github.com/martinsumner/leveled) LSM-tree database local to each node.
On clusters still using Mnesia as the schema store, the plugin falls back to Mnesia for storage.
If the `khepri_db` feature flag is enabled while the plugin is already running, existing
Mnesia-stored delayed messages are migrated to Leveled automatically.

## Supported Erlang/OTP Versions

The latest version of this plugin [requires Erlang 26.2 or later versions](https://www.rabbitmq.com/docs/which-erlang).


## Installation

### Download a Binary Build

Binary builds are distributed [via GitHub releases](https://github.com/cloudamqp/rabbitmq-delayed-message-exchange/releases).

As with all 3rd party plugins, the `.ez` files the release provides must be copied into a [node's plugins directory](https://rabbitmq.com/plugins.html#plugin-directories)
with sufficient permissions for the effective user of the RabbitMQ process to load it from disk.

To find out what the plugins directory is, use `rabbitmq-plugins directories`

``` bash
rabbitmq-plugins directories -s
```

### Enabling the Plugin

Then run the following command:

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
making sure the delay is within range, ie: `Delay > 0, Delay =<
?ERL_MAX_T` (In Erlang a timer can be set up to (2^32)-1 milliseconds
in the future).

If the previous condition holds, then the message will be persisted to
a Leveled LSM-tree database (or to Mnesia on clusters that have not enabled the
`khepri_db` feature flag) and some other logic will kick in to determine if this
particular message delay needs to replace the current scheduled timer
and so on.

This means that while one _could_ use this exchange in place of a
_direct_ or _fanout_ exchange (or any other exchange for that matter),
_it will be slower_ than using the actual exchange. If you don't need
to delay messages, then use the actual exchange.


## Performance Improvements

The Leveled-based implementation introduces two notable improvements over the previous Mnesia-based storage:

 * **Smaller memory footprint**: delayed messages are no longer kept in memory in their entirety.
   Only an index is held in memory while the message bodies live in the Leveled LSM-tree on disk,
   resulting in a considerably smaller memory footprint
 * **Stable behavior under scheduling collisions**: with Mnesia-based storage, write and startup times
   degraded exponentially when a large number of messages were scheduled to the exact same expiry timestamp.
   This is no longer the case with the Leveled-based implementation


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

Current design of this plugin doesn't really fit scenarios
with a high number of delayed messages (e.g. 100s of thousands or millions).
See [#72](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/issues/72) for details.

## Disabling the Plugin ##

You can disable this plugin by calling `rabbitmq-plugins disable
rabbitmq_delayed_message_exchange` but note that **ALL DELAYED MESSAGES THAT
HAVEN'T BEEN DELIVERED WILL BE LOST**.

## Building the Plugin

```shell
PROJECT_VERSION=4.2.6-rc.1 PRODUCT_VERSION=4.2.6-rc.1 VERSION=4.2.6-rc.1 \
    gmake dist PROJECT_VERSION=4.2.6-rc.1 PRODUCT_VERSION=4.2.6-rc.1 \
    VERSION=4.2.6-rc.1 DIST_AS_EZS=true
```

The EZ file is created in the `plugins` directory.

## Creating a Release

1. Update `RABBITMQ_VERSION` in `Makefile` to the target RabbitMQ release (e.g. `v4.2.6`)
1. Update `broker_version_requirements` in the `PROJECT_APP_EXTRA_KEYS` block of `Makefile` to match
1. Push a tag (i.e. `v4.2.6`) with the matching version
1. The Package workflow (`.github/workflows/package.yml`) builds the `.ez` artifacts on push
1. Attach the produced `rabbitmq_delayed_message_exchange*.ez` and `leveled*.ez` files to a GitHub release

## LICENSE

See the LICENSE file.
