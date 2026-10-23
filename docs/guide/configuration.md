# Configuration

Saranga reads the configuration of an Artemis instance: the same `etc/` directory, the same `broker.xml`, `bootstrap.xml`, `login.config` and profiles, and the same broker properties. An instance made by `artemis create` starts on Saranga as it is, and the `etc/` files `saranga create` writes are the ones `artemis create` writes. This page explains how `saranga run` finds the files, how `${...}` placeholders and broker properties are resolved, what a configuration reload changes, and which parts of `broker.xml` Saranga ignores or refuses. Where Saranga does what Artemis does, the page says so and links Artemis's documentation; the full list of differences is in [COMPAT.md](../../COMPAT.md#configuration-differences).

## The instance layout

`saranga create` writes the instance `artemis create` writes, from the same templates (see [The CLI](cli.md)). The files Saranga reads are these:

| File | What Saranga does with it |
|---|---|
| `etc/bootstrap.xml` | Names `broker.xml` (`<server configuration>`), the security manager (`<jaas-security>` or `<security-manager>`) and the web server (`<web>`). |
| `etc/broker.xml` | The broker's configuration. |
| `etc/broker.properties` | Broker properties applied over `broker.xml`, when the file exists. |
| `etc/login.config` | The JAAS domains. See [Security](security.md). |
| `etc/artemis.profile` (`artemis.profile.cmd` on Windows) | The `-D` options of `JAVA_ARGS` and `JAVA_ARGS_APPEND` become system properties for `saranga run`. Other Java options are ignored. |
| `etc/management.xml` | Its `<authorisation>` guards the web console and Jolokia. Saranga has no JMX, so the rest of the file has no effect. See [Management](management.md). |
| `etc/jolokia-access.xml` | The Jolokia layer's CORS rules. |
| `etc/log4j2.properties` | Logging for `saranga run`. `etc/log4j2-utility.properties` configures the other commands. See [Operations](operations.md). |
| `etc/artemis-users.properties`, `etc/artemis-roles.properties` | Users and roles for the properties login module. See [Security](security.md). |

Relative data directories in `broker.xml` (`data/journal` and the like) are taken from the instance directory, the directory above `etc/`.

Logging works as Artemis's does: Saranga writes Artemis's format, codes and logger names to the console and to `log/artemis.log`. It supports the log4j 2 properties format with the common appenders and layouts; see [COMPAT.md](../../COMPAT.md#logging-differences) for what it does not support.

## How `saranga run` finds the configuration

`saranga run` looks for its configuration in this order:

1. `--config <broker.xml>`: that file. Its directory is taken as `etc/`, so `bootstrap.xml` (for the security manager), `login.config` and `broker.properties` next to it apply.
2. A configuration URI, as `artemis run` takes one: `xml:<path to bootstrap.xml>`, relative to the current directory. The bootstrap file names `broker.xml`.
3. `--etc <dir>`: an instance's `etc/` directory, read as `artemis run` reads it. Its `bootstrap.xml` names `broker.xml`; without a `bootstrap.xml`, `broker.xml` in that directory is used.
4. The instance named by `ARTEMIS_INSTANCE_ETC` or `ARTEMIS_INSTANCE`, which an instance's `bin/artemis` sets.

With none of these, the broker keeps everything in memory, has no security and listens on `0.0.0.0:61616`.

`--broker <broker.xml>` replaces the `broker.xml` that `bootstrap.xml` names. `--bind <host:port>` makes the broker listen there instead of on its configured acceptors, with the settings of the first acceptor.

The usual ways to start an instance:

```bash
# From the instance, as with Artemis
/var/lib/broker1/bin/artemis run

# The same instance, from anywhere
saranga run --etc /var/lib/broker1/etc

# A single broker.xml, for tests
saranga run --config ./broker.xml
```

On Windows, use `bin\artemis.cmd run`.

`artemis create` writes the instance's absolute path into `bootstrap.xml`. If the instance has moved and that path no longer exists, Saranga logs a warning and uses `broker.xml` in the instance's `etc/`.

A URI without a scheme, with a scheme other than `xml`, or naming a file that is not there is refused with Artemis's `Invalid configuration URI` messages, and `saranga run` exits with status 1.

## `${...}` placeholders and system properties

`broker.xml`, `login.config` and broker properties may hold `${name}` and `${name:default}` placeholders. They are replaced in the file's text before it is parsed, as Artemis replaces them. A name is looked up in this order:

1. System properties given to `saranga run` with `-D`. Those from the profile's `JAVA_ARGS` and `JAVA_ARGS_APPEND` come first, so the command line's win.
2. The properties `artemis run` defines: `artemis.instance`, `artemis.home`, `artemis.instance.etc`, `artemis.URI.instance`, `data.dir` and `java.security.auth.login.config`.
3. Environment variables.
4. The default after `:`, else the empty string.

```bash
saranga run --etc /var/lib/broker1/etc -D broker.port=61617 -Dbroker.host=0.0.0.0
```

`-D` takes `name=value`, and may be repeated. Artemis's `artemis run` does not take `-D` options; there, set them in `JAVA_ARGS` in `etc/artemis.profile`. That works with Saranga too, because an instance's `bin/artemis` passes the profile's `JAVA_ARGS` to `saranga run`. The arguments are split on spaces, so a value cannot hold a space.

`bootstrap.xml` and `management.xml` are read as Artemis's `XmlUtil.decode` reads them, which works differently. Only attribute values are replaced, not element text. A name is looked up in the system properties, including those `artemis run` defines, then in the environment. There is no `:default`, and a placeholder with no value is left as it is. Artemis replaces each value with `Matcher.replaceFirst`, which replaces the first placeholder of the value. So `${unset}-${set}` becomes `<value>-${set}`, and Saranga does the same.

Some system properties have a meaning of their own:

- `brokerconfig.<key>` is a broker property (see below). `<system-property-prefix>` in `broker.xml` changes the prefix.
- `broker.properties` names the broker properties sources, over `--properties`.
- `webconfig.` properties change `bootstrap.xml`'s `<web>`, and `securityconfig.` properties choose the security manager, as in Artemis. See [Management](management.md) and [Security](security.md).
- `artemis.discovery.enabled` (or the `ARTEMIS_DISCOVERY_ENABLED` environment variable) turns on UDP discovery. See [Clustering](clustering.md).
- `log4j2.configurationFile` names another logging configuration.

## Broker properties

Broker properties set configuration values over `broker.xml`, as in Artemis. Keys are paths over the configuration, in the camel-case form of the `broker.xml` element:

```properties
globalMaxSize=1G
addressSettings."orders.#".maxSizeBytes=10M
acceptorConfigurations.tcp.params.port=61617
```

A key segment that holds dots is wrapped in quotes (or in what `key.surround` says). A collection entry whose value is `-` (or what `remove.value` says) is removed.

The sources, in the order Saranga applies them:

1. The `broker.properties` system property, else `--properties`, else `etc/broker.properties` when it exists. `--properties` takes a comma-separated list of files, and of directories ending in `/`. A directory's files that match `.*\.(json|yaml|properties)` (or the regular expression of a `?filter=` query) are applied in name order.
2. System properties starting with `brokerconfig.`.

```bash
saranga run --etc /var/lib/broker1/etc \
  --properties /etc/broker1/common.properties,/etc/broker1/conf.d/ \
  -Dbrokerconfig.globalMaxSize=512M
```

Files ending in `.json` are read as Artemis reads them: nested members become dotted keys. YAML files are reported as a load error and skipped.

Saranga applies the top-level settings it supports and these collections: `addressSettings`, `securityRoles`, `addressConfigurations`, `acceptorConfigurations`, `divertConfigurations`, `bridgeConfigurations`, `clusterConfigurations` and `jaasConfigs`. Any other key is not applied. The broker still starts, logs a warning such as

    broker.properties: brokerPlugins.p1.className was not applied: brokerPlugins is not supported by Saranga

and records the key in the broker's status under `configuration.properties.<source>`, as Artemis does. See [COMPAT.md](../../COMPAT.md#configuration-differences).

A top-level setting takes the name Artemis gives it as a bean property, such as `journalFileSize`, `IDCacheSize`, `JMXDomain` or `journalBufferTimeout_NIO`; the camel-case form of its `broker.xml` element, such as `idCacheSize`, is also taken. `exportConfigAsProperties` (management, and the console's Configuration page) exports the configuration the broker runs on, the broker properties it applied included, as Artemis exports it.

Artemis's documentation on broker properties is in its [configuration index](https://activemq.apache.org/components/artemis/documentation/latest/configuration-index.html).

## Configuration reload

While the broker runs, Saranga checks `broker.xml` and the broker properties sources every `configuration-file-refresh-period` milliseconds (5000 by default), as Artemis's reload manager does: a file is changed when its modification time moves forward, or when it appears, and a directory of properties files when its own modification time does. Each changed file reloads the configuration once, so `broker.xml` and `broker.properties` changed together reload twice, as in Artemis. `etc/broker.properties` is watched when it exists as the broker starts; one created later is not read until a restart, as in Artemis.

A reload reads the configuration again and logs `AMQ221056: Reloading configuration: ...` for each part, then `AMQ221087: Configuration reload completed`. If the new configuration does not parse, it keeps the running one: a `broker.xml` that is not well formed logs `AMQ224069: Change detected in broker configuration file, but reload failed` with the parser's exception, as Artemis logs it, and other failures log `configuration not reloaded: <reason>`.

A reload does what Artemis's does:

- applies the `jaasConfigs` of the broker properties (see [Security](security.md));
- swaps the security settings and the address settings;
- reconciles the diverts with the configured ones;
- deploys new addresses and queues and updates the queues that exist;
- undeploys addresses and queues that left the configuration, where `config-delete-addresses` or `config-delete-queues` is `FORCE`;
- deploys new bridges, deploys again bridges that changed, and destroys bridges no longer configured;
- updates the acceptors, by name: an acceptor whose parameters changed is stopped and started again with them, one no longer configured is stopped, and a new one is started. Parameters are compared as Artemis compares them, so writing the same ones in another order changes nothing. Stopping an acceptor closes the connections it accepted. An acceptor with `autoStart=false` is made but not started, at startup as on a reload, and management can start it.
- updates the AMQP broker connections, by name. A connection whose configuration changed is shut down and made again. It is started if it was started or has `auto-start`, so one stopped through management stays stopped without `auto-start`. A new connection is made, and started with `auto-start`. A connection no longer configured is shut down and leaves management. All the old connections are shut down before any new one is made. A connection with a `<mirror>` is neither changed nor removed. As in Artemis, the reload logs `Skipping update of broker connection <name> which contains a mirror configuration which are not reloadable.` (or `Skipping remove ...`), and changing such a connection takes a restart.

An acceptor that cannot be started (its port is in use, say) logs `AMQ224104: Error starting the Acceptor <name> <parameters>`, and the reload then fails with `AMQ224069` and the bind exception, before `AMQ221087`, as in Artemis. What it changed before that stays changed.

Saranga has no connection routers. `login.config` is not reloaded, as in Artemis. Other settings take a restart. See [Artemis's configuration reload](https://activemq.apache.org/components/artemis/documentation/latest/config-reload.html) and [COMPAT.md](../../COMPAT.md#configuration-differences).

When a reload changes a queue's filter, the record Saranga writes holds the new filter, where Artemis writes the old one. Either broker reads the other's record. See [COMPAT.md](../../COMPAT.md#persistence-differences).

## Address settings

`<address-settings>` work as in Artemis, with the same match syntax, the same hierarchy and the same defaults, except where noted below. See [Artemis's address settings](https://activemq.apache.org/components/artemis/documentation/latest/address-settings.html) and [wildcard syntax](https://activemq.apache.org/components/artemis/documentation/latest/wildcard-syntax.html).

- A second `<address-setting>` with the same match is reported, and the first is kept, as Artemis keeps it.
- An element Saranga does not know inside an `<address-setting>` is reported and ignored.
- Paging needs persistence. Without `persistence-enabled`, only `BLOCK`, `FAIL` and `DROP` limit an address. See [Storage](storage.md).

The queue behaviours that address settings turn on (dead letters, expiry, redelivery, last-value, ring, non-destructive and exclusive queues, message groups and the like) follow Artemis. The small differences in timing are listed in [COMPAT.md](../../COMPAT.md#queue-semantics-differences).

## Addresses and queues

`<addresses>`, with their `<anycast>` and `<multicast>` queues and every queue attribute, are deployed as Artemis deploys them, at startup and on a reload. Auto-creation and auto-deletion follow the address settings. See [Artemis's address model](https://activemq.apache.org/components/artemis/documentation/latest/address-model.html).

The deprecated top-level `<queues>` element is ignored, with a warning: declare queues under `<addresses>`.

`<wild-card-routing-enabled>` in `<core>` is ignored with a warning, as Artemis ignores it. Use `<wildcard-addresses><routing-enabled>`.

## Diverts

`<diverts>` work as in Artemis: exclusive and non-exclusive diverts, filters and routing types. See [Artemis's diverts](https://activemq.apache.org/components/artemis/documentation/latest/diverts.html). Where Saranga differs:

- A transformer is Rust code, not a Java class. An application that embeds Saranga registers a transformer under the class name that `broker.xml` gives. A divert whose transformer is not registered is not deployed, and a warning is logged.
- Duplicate detection checks the message a client sends. Copies that a divert makes are not checked against the forwarding address's cache.

See [COMPAT.md](../../COMPAT.md#queue-semantics-differences).

## Well-formedness

Artemis reads its files with the JDK's Xerces parser: `broker.xml` through a DOM parser that resolves `xi:include`, `bootstrap.xml` and `management.xml` through the streaming reader JAXB uses. Saranga ports both, so a file that is not well formed (an unclosed tag, an attribute written twice, an undeclared entity, a bad byte in the declared encoding, a missing included file) is refused with the same message, at the same line and column. The DOM parser's lines on standard error come first, as Java's default error handler prints them:

```
[Fatal Error] :286:6: The element type "core" must be terminated by the matching end-tag "</core>".
org.xml.sax.SAXParseException; lineNumber: 286; columnNumber: 6; The element type "core" must be terminated by the matching end-tag "</core>".
```

`saranga run` prints this and exits with status 1. The commands that read `broker.xml` end as Artemis's do: `data print` and `perf-journal` with `Exception in thread "main"`, `data exp` and `compact` with `Error:` and the message, and the CLI's default URL with `Can not get the broker url instance:`. As in Artemis, `broker.xml` is read as UTF-8 whatever its XML declaration says, while `bootstrap.xml` and `management.xml` are read in the encoding their declaration or byte-order mark gives. All three are held to the JDK's limits on entity expansion, attributes, nesting and name length. The few cases that differ are listed in [COMPAT.md](../../COMPAT.md#configuration-differences).

## Schema validation

As Artemis does, the broker validates `broker.xml` against Artemis's schemas before it reads it, once `${...}` and `xi:include` are resolved: `<core>` against `artemis-configuration.xsd` and the legacy `<jms>` against `artemis-jms.xsd`. The first error stops the broker before it starts, logged as Artemis logs it:

```
ERROR [org.apache.activemq.artemis.core.client] AMQ214019: Invalid configuration
org.xml.sax.SAXParseException; cvc-datatype-valid.1.2.1: '100K' is not a valid value for 'integer'.
```

The messages are Xerces's, so an element out of place, a missing required attribute, a value that is not of its type (such as byte notation where the schema wants a plain number), a duplicate `name` where the schema makes it an ID, or a `discovery-group-ref` to a group that does not exist is reported in the same words. The CLI's commands that read `broker.xml` validate it the same way.

A reload validates the whole file against `artemis-server.xsd` and, as Artemis does, only logs an error and reloads what it can read.

As Artemis's XInclude processing does, an included element gets an `xml:base` attribute, and the schema allows it only on some elements: an included `<addresses>` or `<address-settings>` is valid, a lone `<acceptor>` is not.

`bootstrap.xml` and `management.xml` are validated against Artemis's `activemq.xsd`, as JAXB validates them in Artemis. The attributes are checked in the order they are written, after their `${...}` are replaced. An error gives the line and column where Java's XML reader stops. The first error is printed as Artemis prints it:

```
javax.xml.bind.UnmarshalException
 - with linked exception:
[org.xml.sax.SAXParseException; lineNumber: 21; columnNumber: 48; cvc-complex-type.3.2.2: Attribute 'bogus' is not allowed to appear in element 'jaas-security'.]
Caused by: org.xml.sax.SAXParseException; lineNumber: 21; columnNumber: 48; cvc-complex-type.3.2.2: Attribute 'bogus' is not allowed to appear in element 'jaas-security'.
```

Java's stack frames are left out. Every command checks `management.xml` before it runs, prints such an error, and goes on, as Artemis's CLI does. `saranga run` exits with status 1 when `bootstrap.xml` is not valid, when `management.xml` is not valid, or when `management.xml` is missing; with `--config`, a `management.xml` next to the file is checked only if it is there. `stop`, `kill`, `data print`, `data recover`, `perf-journal` and `activation` end as Artemis's do on the exception, with `Exception in thread "main"` and status 1. `data exp`, `compact`, `encode` and `decode` print `Error:null` and then the exception, with status 0. A file whose root element is another of the schema's ends with Artemis's `ClassCastException` (`Cannot cast org.apache.activemq.artemis.dto.ManagementContextDTO to org.apache.activemq.artemis.dto.BrokerDTO`). The commands that only need `broker.xml`, such as `mask` and the CLI's default URL, do not read `bootstrap.xml`, as in Artemis.

## What Saranga ignores, and what stops it

Saranga reads the whole `broker.xml` schema. Elements are matched by their local name, so a prefixed element that Java would miss is accepted.

These are read and ignored, each with a warning in the log:

- an element the schema does not define at that position, in a file a reload reads (when the broker starts, it is an error);
- `<connection-routers>`;
- `<lock-coordinators>`;
- `<metrics>` and `<metrics-plugin>` (the broker, address and queue metrics are still served where `bootstrap.xml` declares the metrics app; see [Management](management.md));
- `<resource-limit-settings>`, which are not enforced;
- `<remoting-interceptors>`, `<remoting-incoming-interceptors>`, `<remoting-outgoing-interceptors>` and `<connector-services>`, which name Java classes;
- an acceptor's `proxyProtocolEnabled`, `httpEnabled` and `router`;
- `scale-down` in an HA policy;
- `<security-setting-plugin>` (see [Security](security.md));
- in `bootstrap.xml`, `<component>`s and every `<web>` after the first: Artemis starts each as a Java component, and Saranga runs one web server and no Java code;
- deprecated settings, such as `master` and `slave`, with Artemis's `AMQ221038` message.

The critical analyzer settings are accepted. See [Operations](operations.md).

These stop the broker from starting, with a reason. Starting anyway would make it something it is not:

- a colocated backup (`<colocated>` in the HA policy);
- the JDBC store (`<database-store>`);
- a JGroups broadcast or discovery group (`jgroups-file`): discovery is over UDP only;
- broadcast or discovery groups while neither the `artemis.discovery.enabled` system property nor the `ARTEMIS_DISCOVERY_ENABLED` environment variable is `true` (`AMQ229262`, `AMQ229263`), as in Artemis 2.57;
- a `<security-manager>` class other than `ActiveMQBasicSecurityManager`;
- a `<broker-plugin>` class Saranga has no port of;
- an acceptor with TLS settings Saranga cannot honour (see [Protocols](protocols.md#tls)).

For HA, clustering, federation and AMQP broker connections, see [High availability](ha.md) and [Clustering](clustering.md). The complete list of configuration differences is in [COMPAT.md](../../COMPAT.md#configuration-differences).
