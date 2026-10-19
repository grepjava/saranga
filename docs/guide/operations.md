# Operations

This page covers running a Saranga broker day to day: its log and audit log, metrics, the critical analyzer, the network check, broker plugins, disk limits, stopping and killing it, running it as a service, upgrading an instance, and the resources it uses. Saranga reads the same files and settings as Artemis for all of these, so runbooks written for Artemis work. The sections below say what to do and where Saranga differs.

## Logging

`saranga run` reads the instance's `etc/log4j2.properties`, or the file `-Dlog4j2.configurationFile` names (on `saranga run` or in the profile's `JAVA_ARGS`). It writes Artemis's format, AMQ codes and logger names. With the file `create` writes, the log goes to the console and to `log/artemis.log`, rolled daily.

```bash
tail -f /var/lib/mybroker/log/artemis.log
grep AMQ221007 /var/lib/mybroker/log/artemis.log   # "Server is now active"
```

To change a level, edit the file while the broker runs. It is reread every `monitorInterval` seconds (5 in the file `create` writes), as log4j rereads it.

```properties
logger.artemis_server.level=DEBUG
```

What Saranga logs:

- The startup and shutdown messages runbooks look for, with their codes (`AMQ221000`, `AMQ221001`, `AMQ221002`, `AMQ221007`, `AMQ221020`, `AMQ241001` and others). Their text names Saranga and its version where Artemis names itself.
- Other messages with Artemis's code and text where Saranga has the same event. Saranga's own messages have no code.
- No JVM or Netty messages.

Each message is logged under the logger of the Artemis message bundle its code belongs to. For example, `AMQ22xxxx` goes under `org.apache.activemq.artemis.core.server` and `AMQ14xxxx` under `org.apache.activemq.artemis.journal`. A level set for a single Artemis class therefore has no effect. Set it on the package.

Of log4j 2, Saranga supports the properties format with these parts:

- `Console`, `File` and `RollingFile` appenders;
- `PatternLayout` and `ThresholdFilter`;
- levels, appender references and additivity;
- `${sys:}`, `${env:}` and `property.` lookups.

XML, JSON and YAML configurations, other appenders and layouts, async loggers and colours are not supported. A line Saranga does not understand is logged as a warning.

`RUST_LOG`, when set, chooses what is logged in place of the configured levels. Loggers set to `OFF` stay off. The CLI's other commands log as Artemis's do, WARN to the console, configured by `etc/log4j2-utility.properties` when there is one.

See [COMPAT.md, Logging differences](../../COMPAT.md#logging-differences) and Artemis's [logging](https://activemq.apache.org/components/artemis/documentation/latest/logging.html) chapter.

## The audit log

The three audit loggers, `org.apache.activemq.audit.base`, `.resource` and `.message`, are off unless `log4j2.properties` turns them on, as in Artemis. To turn them on, set their levels to `INFO`. With the file `create` writes they go to `log/audit.log`, and the change takes effect while the broker runs:

```properties
logger.audit_base.level = INFO
logger.audit_resource.level = INFO
logger.audit_message.level = INFO
```

Saranga logs Artemis's codes, texts and callers (`user(roles)@address`). Each management method logs what Artemis's control logs. This holds for every way management is called: management messages, the REST API and Jolokia.

Where Saranga differs:

- Objects in messages are shown in their Java classes' `toString` layout with Saranga's values. Identity hash codes are not Java's, and dates are in UTC.
- MBean registration and the JMX guard are not logged. Neither are acknowledgements made without a consumer, the end of XA branches, or the broker's own sends.
- Security failures (601264) have no stack trace.

See [COMPAT.md, Audit log differences](../../COMPAT.md#audit-log-differences).

## Metrics

Prometheus metrics are served by the web server where `bootstrap.xml` declares the metrics app, with Artemis's names for the broker, addresses and queues. `<metrics>` and `<metrics-plugin>` in `broker.xml` are read but not acted on, each with a warning, and JVM, process and Netty metrics are not exported. See [management.md](management.md#prometheus-metrics).

## The critical analyzer

The analyzer reads `critical-analyzer`, `critical-analyzer-timeout`, `critical-analyzer-check-period` and `critical-analyzer-policy` as Artemis does. It watches the journal's writer, each queue and each session. A component past the timeout is reported once, with AMQ224107, AMQ224081, AMQ224080 or AMQ224079, and AMQ222199. Broker plugins are told. Then the policy applies:

| Policy | Saranga does |
|---|---|
| `LOG` | logs only |
| `SHUTDOWN` | stops `saranga run` cleanly |
| `HALT` | ends the process with status 70 |

AMQ222199 is Artemis's thread dump. In Saranga it lists the components in a critical path and how long each has been there, as a Rust process cannot dump its threads' stacks. See [COMPAT.md, Critical analyzer differences](../../COMPAT.md#critical-analyzer-differences) and Artemis's [critical analysis](https://activemq.apache.org/components/artemis/documentation/latest/critical-analysis.html) chapter.

A failed write, sync, open, rename or removal under the journals, a page file or a large message file is a critical I/O error, as in Artemis. It is logged with AMQ144011 (AMQ144010 when there is no file) and AMQ222010, and the broker stops. Artemis leaves its process running with nothing in it. `saranga run` exits with status 1, so a service manager sees the failure and can restart it. See [COMPAT.md, Critical I/O error differences](../../COMPAT.md#critical-io-error-differences).

## The network check

`saranga run` reads `network-check-list`, `network-check-URL-list`, `network-check-period`, `network-check-timeout`, `network-check-ping-command` and `network-check-ping6-command` as Artemis does. While no address or URL answers, the broker is stopped (AMQ201001). Once one answers again, a new broker is started on the same data and ports (AMQ201000). The process and its web server stay up throughout.

A URL is read as Java's `URLConnection` reads it. An `http` or `https` URL answers when the server's status is below 400, after redirects to the same protocol (at most 20). An `https` server's certificate is checked against the `javax.net.ssl.trustStore` given in `JAVA_ARGS` (or with `-D`), else against the platform's certificate authorities, and its host name is checked. The `http.proxyHost`, `https.proxyHost`, their ports and `http.nonProxyHosts` system properties route the request through a proxy, as in Java. A `file` URL answers when the file can be opened. A URL that does not answer logs `AMQ202006` with Java's exception, such as `java.io.FileNotFoundException: http://host/health` for a 404.

`saranga create --ping` writes the settings, as `artemis create --ping` does:

```bash
saranga create --ping 10.0.0.1,10.0.0.2 /var/lib/mybroker
```

Where Saranga differs:

- Without a custom ping command, Saranga first tries a TCP connection to the address's echo port (7), and a refusal counts as an answer. If there is no answer, it runs the ping command. It never sends ICMP itself.
- `network-check-NIC` is reported and not used.
- Without a truststore, an `https` server is checked against the platform's certificate authorities, where Java uses its own `cacerts`.
- SOCKS proxies (`socksProxyHost`) are not used.
- An unhealthy network at startup leaves the process running, and the broker starts once the network answers. Artemis's process exits.

See [COMPAT.md, Network check differences](../../COMPAT.md#network-check-differences) and Artemis's [network isolation](https://activemq.apache.org/components/artemis/documentation/latest/network-isolation.html) chapter.

## Broker plugins

`<broker-plugins>` in `broker.xml` selects Saranga's ports of the plugins Artemis ships. Name them by their full Java class names, with their properties, as for Artemis:

| Class (in `org.apache.activemq.artemis.core.server.plugin.impl`) | Does |
|---|---|
| `LoggingActiveMQServerPlugin` | logs connections, sessions, consumers, deliveries and the like |
| `NotificationActiveMQServerPlugin` | sends extra management notifications |
| `BrokerMessageAuthorizationPlugin` | delivers a message only to consumers whose user has the role its `requiredRole` property names (`ROLE_PROPERTY`) |
| `ConnectionPeriodicExpiryPlugin` | closes the connections of the acceptors `acceptorMatchRegex` matches after `periodSeconds` |

```xml
<broker-plugins>
   <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin">
      <property key="LOG_CONNECTION_EVENTS" value="true"/>
   </broker-plugin>
</broker-plugins>
```

Saranga cannot run Java code. A `<broker-plugin>` naming any other class stops the broker from starting, because leaving out what that plugin enforces could open what it closes. A built-in plugin with a configuration error stops it too.

An application that embeds Saranga can write its own plugin in Rust. It implements `saranga_server::plugin::BrokerPlugin`, the equivalent of `ActiveMQServerPlugin`, and is registered in `BrokerConfig::plugins` or with `Broker::register_plugin`. Hooks run on the broker's tasks, so a plugin must not block. Some of Artemis's hooks are missing, and some arguments are reported without their transaction or consumer. See [COMPAT.md, Broker plugin differences](../../COMPAT.md#broker-plugin-differences) and Artemis's [broker plugins](https://activemq.apache.org/components/artemis/documentation/latest/broker-plugins.html) chapter.

## Disk limits

`max-disk-usage`, `min-disk-free`, `disk-scan-period` and the `disk-full-policy` address setting work as in Artemis: past the limit, producers are blocked, refused or their messages dropped. Sends from connections without flow control (STOMP, MQTT and management's `sendMessage`) fail with AMQ229119, as in Artemis. See [storage.md](storage.md).

## Stopping and killing

A broker stops cleanly on any of these:

- Ctrl-C in the terminal running `saranga run`;
- SIGTERM on Linux and other Unix systems, which `bin/artemis-service stop` and service managers send;
- `saranga stop` or `bin/artemis stop`, which write `STOP_ME` next to `broker.xml`. The broker checks for it every half second.

```bash
bin/artemis stop          # Windows: bin\artemis.cmd stop
saranga stop --broker /var/lib/mybroker/etc/broker.xml
```

`graceful-shutdown-enabled` and `graceful-shutdown-timeout` are honoured, as in Artemis's [graceful shutdown](https://activemq.apache.org/components/artemis/documentation/latest/graceful-shutdown.html).

`saranga kill` (or `bin/artemis kill`) writes `KILL_ME`. A broker honours it only when it was started with `saranga run --allow-kill`, and then halts at once, as in Artemis.

## Running as a service

On Linux and other Unix systems, an instance's `bin/artemis-service` is Artemis's script, unchanged. `bin/artemis-service start` and `stop` work. Under a service manager such as systemd, run `bin/artemis run` in the foreground. Stopping with SIGTERM is a clean stop.

On Windows, `saranga create` writes no service wrapper (`artemis-service.exe`). Run `bin\artemis.cmd run` under a service manager of your choice. `saranga upgrade` leaves an Artemis instance's wrapper as it is.

The exit status tells a service manager what happened. It is 70 after the critical analyzer's `HALT` and 1 after a critical I/O error.

## Upgrading an instance

`saranga upgrade` rewrites an instance's scripts and profiles as `artemis upgrade` does. It makes the same `bootstrap.xml` and logging changes and keeps `broker.xml` and the data. Stop the broker first, and back up the instance.

```bash
bin/artemis stop
saranga upgrade /var/lib/mybroker
```

What it does:

- It backs up each file it changes in `old-config-bkp.<n>`.
- It keeps the lines Artemis keeps: `ARTEMIS_INSTANCE_ETC`, the data and dump paths, `HAWTIO_ROLES`, `-Xmx` and the other kept JVM arguments.
- The instance's `bin/artemis` then runs this `saranga`, and `bin/saranga` (`saranga.cmd` on Windows) is written beside it.

The same command moves an Artemis instance to Saranga, and `artemis upgrade` from an Artemis home moves it back. See [migration.md](migration.md), Artemis's [upgrading](https://activemq.apache.org/components/artemis/documentation/latest/upgrading.html) chapter and [COMPAT.md, CLI differences](../../COMPAT.md#cli-differences).

## Resource use

Saranga is a native process with no JVM. Of the profile's `JAVA_ARGS`, only the `-D` options are read, so `-Xmx` and the other Java options have no effect. Memory for messages is bounded by `global-max-size`, which defaults to 1 GiB. Artemis defaults to half the JVM's maximum heap. See [storage.md](storage.md) for paging.

On the benchmark machine Saranga reached its acceptor in 145 ms with 36 MiB resident, against 2,717 ms and 209 MiB for Artemis. It used less CPU per message in 12 of 13 scenarios. Peak memory depends on the backlog a run builds up. [docs/benchmarks.md](../benchmarks.md) gives the method, the machine and every result. `saranga perf` and `saranga perf-journal` measure your own setup, as Artemis's [perf tools](https://activemq.apache.org/components/artemis/documentation/latest/perf-tools.html) do.
