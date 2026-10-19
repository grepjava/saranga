# Migrating from Artemis

Saranga is inspired by Apache ActiveMQ Artemis 2.57.0 and works interchangeably with it. It reads the same instance directory: the same `broker.xml` and `etc` files, the same journal, bindings, paging and large-message files, and the same `bin/artemis` scripts. Clients connect to it unchanged. Moving an instance from Artemis to Saranga is therefore mostly a matter of stopping one broker and starting the other on the same files, and moving back is the same in reverse. This page covers what to check first, three ways to make the move (in place, side by side, and without downtime through a mixed HA pair or cluster), what happens to each file in `etc`, and the differences you are likely to notice. Every deliberate difference is listed in [COMPAT.md](../../COMPAT.md).

## Before you start

**The Artemis version.** Saranga's compatibility target is Artemis 2.57.0. Its interop tests run against 2.57.0, and its cross-store checks use data directories that 2.57.0 wrote. Artemis 2.45.0 to 2.57.0 are tagged but were not published as distributions; [COMPAT.md](../../COMPAT.md#why-we-build-the-reference-from-source) explains how the project builds 2.57.0 from its tag. If you run an older Artemis, rehearse the move on a copy of the instance and its data first.

**Back up.** Stop the broker and copy the whole instance directory, `etc` and `data` included, before you change anything. Data directories outside the instance (an absolute `journal-directory`, `paging-directory`, `bindings-directory` or `large-messages-directory` in `broker.xml`) need copying too.

**Check what Saranga does not have yet.** Read the list under [Configuration differences](../../COMPAT.md#configuration-differences). Some features stop the broker from starting, with a reason, because starting without them would pretend to be something Saranga is not. Others are read and then ignored with a warning in the log.

These stop the broker from starting:

- a colocated backup (`<colocated>` in `<ha-policy>`);
- the JDBC store (`<database-store>`);
- a JGroups discovery group or broadcast group (`jgroups-file`);
- a `<broker-plugin>` naming a class other than the four plugins Saranga ports (see [Plugins and extensions](#plugins-and-extensions));
- a `<security-manager>` class other than `ActiveMQBasicSecurityManager` (the JAAS manager, `<jaas-security>`, is supported);
- a `password-codec` other than `DefaultSensitiveStringCodec`.

These are read but not acted on, each with a warning:

- `scale-down`, `<connection-routers>`, `<metrics>` and `<resource-limit-settings>` in `broker.xml`;
- `xi:include`: the included file is not read, so move its contents into `broker.xml`;
- `<security-setting-plugin>`, so security settings that such a plugin would load are missing;
- a `login.config` entry naming a login module Saranga does not have: logins that reach that module fail (see [security.md](security.md));
- an acceptor protocol Saranga does not have: an acceptor that lists only such protocols is not opened;
- a divert whose transformer is not registered: the divert is not deployed.

Lock coordinators (`<lock-coordinators>`, a technical preview in Artemis) are not there yet: they are ignored with a warning.

After the first start on Saranga, read the log for `WARN` lines. Each one names what Saranga did not act on.

## Moving an instance in place

This is the shortest path. The instance directory stays where it is, and only its scripts and profiles are rewritten so that `bin/artemis` runs Saranga.

1. Stop Artemis.

   ```bash
   /var/lib/artemis-instance/bin/artemis stop
   ```

2. Run `saranga upgrade` on the instance.

   ```bash
   saranga upgrade /var/lib/artemis-instance
   ```

   On Windows, run `saranga.exe upgrade C:\artemis-instance`.

3. Check the result, then start the broker as before.

   ```bash
   /var/lib/artemis-instance/bin/artemis version
   /var/lib/artemis-instance/bin/artemis run
   ```

   `version` prints `Saranga <version> (Apache Artemis 2.57.0)`. On Unix, `bin/artemis-service start` works as well. On Windows, use `bin\artemis.cmd run`.

`saranga upgrade` takes the same steps as `artemis upgrade`, with the same options (see `saranga help upgrade` and [cli.md](cli.md)):

- It backs up every file it changes into `old-config-bkp.<n>` in the instance, using the first free number.
- It rewrites `bin/artemis` (`bin\artemis.cmd` on Windows) so that it runs the `saranga` you ran the upgrade with, and writes `bin/saranga` (`bin\saranga.cmd`) beside it. On Unix it rewrites `bin/artemis-service` too.
- It rewrites `etc/artemis.profile` and `etc/artemis-utility.profile` (the `.cmd` profiles on Windows) from the templates. It keeps `ARTEMIS_INSTANCE_ETC`, the data and dump paths, `HAWTIO_ROLES`, and from `JAVA_ARGS` the `-Xmx`, `-Djava.security.auth.login.config`, `-Dhawtio.roles` and `-Dartemis.discovery.enabled` options. Other options you added to `JAVA_ARGS` are replaced by the template's, as `artemis upgrade` replaces them. Compare the new profile with the backup and copy back any `-D` options you need.
- It applies Artemis's `bootstrap.xml` changes for older layouts and replaces an old `logging.properties` with `log4j2.properties`. It adds `log4j2-utility.properties` if it is missing.
- It leaves `broker.xml`, `login.config`, the users and roles files, `management.xml` and the data alone.

Two things need a manual step:

- **The web console.** After the upgrade, `ARTEMIS_HOME` in the instance points at Saranga, which ships no `console.war`. Saranga looks for the console in the instance's `web` directory first, then in the home's. To keep the console, copy Artemis's `web/console.war` directory into the instance. Without it, Jolokia is still served but the console's pages are not.

  ```bash
  mkdir -p /var/lib/artemis-instance/web
  cp -r /opt/apache-artemis-2.57.0/web/console.war /var/lib/artemis-instance/web/
  ```

- **The Windows service.** On Windows, the upgrade leaves `bin\artemis-service.exe`, its `.config` and `artemis-service.xml` as they are. That wrapper starts Java directly, so it still starts Artemis. Saranga has no Windows service wrapper of its own; run `bin\artemis.cmd run` under the service manager you use. See [operations.md](operations.md).

## Moving side by side

Use this path to keep the Artemis instance untouched, or to move the broker to another machine.

1. Create a Saranga instance. `saranga create` writes the instance `artemis create` writes, from the same templates, and takes the same options (see `saranga help create`).

   ```bash
   saranga create /var/lib/saranga-instance --user admin --password secret --require-login
   ```

2. Stop Artemis.

3. Copy the configuration from the Artemis instance's `etc` into the new instance's `etc`: `broker.xml`, and any of `login.config`, `artemis-users.properties`, `artemis-roles.properties`, `bootstrap.xml`, `management.xml`, `jolokia-access.xml` and `log4j2.properties` that you changed. Copy across any `-D` options from `JAVA_ARGS` in `artemis.profile` that your configuration relies on.

4. Copy the data directories: by default the instance's `data` directory, which holds `bindings`, `journal`, `paging` and `large-messages`. `server.lock` in the journal directory holds the broker's node ID; copy it with the rest so that the broker keeps its identity in a cluster or HA pair.

   ```bash
   cp -a /var/lib/artemis-instance/data /var/lib/saranga-instance/
   ```

   If `broker.xml` names directories outside the instance, either copy them to the same paths or leave `broker.xml` pointing at them.

5. Copy the console as described above, if you want it, and start the new instance.

   ```bash
   /var/lib/saranga-instance/bin/artemis run
   ```

A copied `broker.xml` keeps whatever `journal-type` it had. Saranga reads `ASYNCIO` journal files and writes its own the way Artemis's `NIO` journal does, whatever `journal-type` says; Artemis reads such files with any journal type (see [Persistence differences](../../COMPAT.md#persistence-differences)). Without `--aio`, `--nio` or `--mapped`, `saranga create` chooses `NIO`.

## Moving without downtime

Saranga and Artemis brokers can be each other's HA partner and can share a cluster. You can therefore move one broker at a time while clients go on.

### A mixed HA pair

A Saranga backup works with an Artemis primary, and an Artemis backup with a Saranga primary, in all three ways Artemis pairs them: shared store, replication with the quorum vote, and replication with the ZooKeeper lock manager. The broker locks are taken as Java takes them, so a Java broker and a Saranga broker exclude each other on one journal directory. [tests/interop/ha/no-loss.sh](https://github.com/grepjava/saranga/blob/main/tests/interop/ha/no-loss.sh) checks it in all four Artemis/Saranga pairings: the Artemis JMS client produces and consumes in transactions while the primary is killed, the backup takes over, and the restarted primary takes its node back. No committed message is lost or received twice. CI runs it with a Java primary and a Saranga backup, then the other way round.

With an Artemis HA pair already running, a move looks like this:

1. Stop the Artemis backup, `saranga upgrade` its instance (and copy the console), and start it again. It is now a Saranga backup of the Artemis primary. A replicating backup copies the primary's data again before it can take over.
2. Stop the Artemis primary. Whether a clean stop hands over to the backup depends on the HA policy (`failover-on-shutdown`), as in Artemis; the no-loss test kills the primary instead. Clients fail over to the Saranga backup.
3. `saranga upgrade` the primary's instance and start it. With `allow-failback` on the backup (and `check-for-active-server` on a replicating primary) the primary takes its node back.

Clients keep going through the move only if they fail over: for the Artemis JMS client, a connection with `ha=true` and `reconnectAttempts`. Rehearse the sequence with your own configuration first. The no-loss test kills and restarts each broker as the same kind; the steps above restart the primary as the other kind, which relies on the same mixed pairings. See [ha.md](ha.md) for the HA settings.

### A mixed cluster

Java and Saranga nodes work in one cluster: load balancing, redistribution, message grouping (either kind can be the `LOCAL` grouping node), topic subscriptions and the topology clients see. [tests/interop/cluster/mixed.sh](https://github.com/grepjava/saranga/blob/main/tests/interop/cluster/mixed.sh) runs a three-node cluster and stops and restarts a node while the others go on; the cluster bridges to it again when it comes back. Its output, recorded with Artemis on every node, matches with all three nodes Saranga and in four mixes of the two. Core and AMQP federation, core bridges, AMQP broker connections and mirroring also work between Java and Saranga in both directions ([scripts/cluster-interop.sh](https://github.com/grepjava/saranga/blob/main/scripts/cluster-interop.sh)).

To move a cluster, take one node at a time: stop it, `saranga upgrade` it, start it, and wait until the other nodes have bridged to it again before moving the next one. Messages held on a stopped node wait for it unless that node has an HA backup. See [clustering.md](clustering.md).

## The files in etc

| File | On Saranga |
|---|---|
| `broker.xml` | Read as Artemis reads it, `${...}` properties included. Elements are matched by local name. Broker properties files (`.properties` and `.json`) are read, with a subset of collections applied and every other key reported as a warning. See [Configuration differences](../../COMPAT.md#configuration-differences) and [configuration.md](configuration.md). |
| `artemis.profile`, `artemis.profile.cmd` | `bin/artemis` reads them as Artemis's does and passes `JAVA_ARGS` and `JAVA_ARGS_APPEND` on. Saranga takes only the `-D` options from them, as system properties. Everything else is for a JVM and is ignored: `-Xmx`, `-Xms`, garbage collector and `--add-opens` options have no effect. |
| `artemis-utility.profile` | Used by the CLI's commands other than `run`, as in Artemis. |
| `bootstrap.xml` | Read as Artemis reads it: the `broker.xml` URI, `<jaas-security>` or `<security-manager>`, and `<web>` with its bindings and apps. Saranga serves the console app, Jolokia under it, the metrics app, and its own REST API at `/api` on every binding; it runs no other web apps. See [Web server differences](../../COMPAT.md#web-server-differences) and [management.md](management.md). |
| `login.config` | `PropertiesLoginModule`, `GuestLoginModule`, `TextFileCertificateLoginModule`, `ExternalCertificateLoginModule` and `LDAPLoginModule` are supported. `Krb5LoginModule`, `KubernetesLoginModule`, `PrincipalConversionLoginModule` and custom Java modules are not. See [Security differences](../../COMPAT.md#security-differences) and [security.md](security.md). |
| `artemis-users.properties`, `artemis-roles.properties` | Read as Artemis reads them, masked passwords (`ENC(...)`) included. |
| `management.xml` | Its `<authorisation>` decides who may call what through Jolokia and the console, as Artemis's guard applies it. Saranga has no JMX, so no JMX connector is opened. |
| `jolokia-access.xml` | Its CORS rules are applied. |
| `log4j2.properties`, `log4j2-utility.properties` | Read and written in Artemis's format, codes and logger names, so `log/artemis.log` looks as it did. Only the properties format is supported, with `Console`, `File` and `RollingFile` appenders and `PatternLayout`. A level set for a single Artemis class has no effect. See [Logging differences](../../COMPAT.md#logging-differences) and [operations.md](operations.md). |

## Clients

Clients need no change. They connect to the same acceptors on the same ports and speak the same protocols. The client matrix of Phase 7 in [PHASES.md](https://github.com/grepjava/saranga/blob/main/PHASES.md#phase-7-protocols) passes against Artemis 2.57.0 and against Saranga:

- the Artemis JMS client (Core);
- Qpid JMS (AMQP 1.0);
- the ActiveMQ 5 client (OpenWire);
- Eclipse Paho for MQTT 3.1.1 and 5;
- STOMP 1.0 to 1.2;
- the Python (Qpid Proton), .NET (AMQPNetLite) and Rust (fe2o3-amqp) AMQP clients;
- STOMP, MQTT and AMQP over WebSockets.

A 1.x Artemis JMS client, which names queues `jms.queue.<name>` and topics `jms.topic.<name>`, works against an acceptor with those prefixes, as on Artemis (Phase 6 in [PHASES.md](https://github.com/grepjava/saranga/blob/main/PHASES.md#phase-6-config-and-security)).

Messages sent with one protocol are received with another as on Artemis. The protocol features Saranga does not have yet are listed per protocol in COMPAT.md; the ones most likely to matter are the AMQP SASL mechanisms SCRAM, GSSAPI, OAUTHBEARER and XOAUTH2 ([AMQP differences](../../COMPAT.md#amqp-differences)), Core over HTTP and WebSocket compression ([WebSocket differences](../../COMPAT.md#websocket-differences)), and address-full policies for OpenWire producers ([OpenWire differences](../../COMPAT.md#openwire-differences)). See [protocols.md](protocols.md).

A client that checks the broker's name sees Saranga's: the STOMP `server` header is `Saranga/<version>`, and the AMQP Open's `product` is `saranga`. Management's `getVersion` reports 2.57.0, so tools that check the version keep working.

## Going back to Artemis

The data directory Saranga writes is readable by Artemis. The Phase 3 cross-store checks in [PHASES.md](https://github.com/grepjava/saranga/blob/main/PHASES.md#phase-3-persistence-on-disk-compatible-first-usable-release) move data directories from Saranga to Artemis and back, after a clean stop and after a kill, and Artemis's own journal code loads journals Saranga wrote. Address settings, diverts, paused queues and addresses, and the basic security manager's users are journaled as Artemis journals them, so they move too.

To move an instance back in place, stop Saranga and run Artemis's own `upgrade` on it:

```bash
/var/lib/artemis-instance/bin/artemis stop
/opt/apache-artemis-2.57.0/bin/artemis upgrade /var/lib/artemis-instance
/var/lib/artemis-instance/bin/artemis run
```

`artemis upgrade` rewrites `bin/artemis` and the profiles to run Java again. `bin/saranga` stays behind and does no harm. To move side by side instead, point a new Artemis instance's `broker.xml` at the data directories, or copy them into it, as in [Moving side by side](#moving-side-by-side).

A few records differ in ways Artemis handles:

- Saranga deletes the page counter records when it loads them and rebuilds the counters from the page files. Artemis rebuilds its counters the same way when the records are missing ([Paging differences](../../COMPAT.md#paging-differences)).
- When a queue's filter changes, Saranga journals the new filter where Artemis 2.57.0 journals the old one ([Persistence differences](../../COMPAT.md#persistence-differences)).

## What differs

Most operators will notice few differences. These are the ones most likely to show:

- **Memory.** There is no JVM heap. `global-max-size` defaults to 1 GiB, where Artemis defaults to half the maximum heap; set it explicitly if you relied on the default. ([Paging differences](../../COMPAT.md#paging-differences))
- **Paging needs persistence.** With `persistence-enabled` false, Saranga writes no page files and keeps every message in memory. ([Paging differences](../../COMPAT.md#paging-differences))
- **Queue browsers** see the messages a queue holds in memory, not the whole paged backlog. ([Paging differences](../../COMPAT.md#paging-differences))
- **Journal tuning.** `journal-buffer-timeout` has no effect, and appends wait while the journal compacts. ([Persistence differences](../../COMPAT.md#persistence-differences))
- **Management.** `browse` through management messages answers `no operation`; `listMessages`, the REST API and Jolokia return the messages as JSON. Dates in management views are in UTC. ([Management differences](../../COMPAT.md#management-differences))
- **Web.** A REST API at `/api` on every web binding, which Artemis does not have. No `console.war` ships with Saranga. ([Web server differences](../../COMPAT.md#web-server-differences))
- **Logging.** Only the properties format of log4j 2 is read, and per-class logger levels have no effect. ([Logging differences](../../COMPAT.md#logging-differences))
- **The CLI.** Its messaging commands connect over Core only, and `--serializer` cannot load a Java class. There is no Windows service wrapper. ([CLI differences](../../COMPAT.md#cli-differences))
- **Failures.** A critical I/O error ends the `saranga run` process with status 1, where Artemis stops the broker and leaves the process up. ([Critical I/O error differences](../../COMPAT.md#critical-io-error-differences))
- **Network checks.** A broker stopped by a network check comes back in the same process once the network answers. ([Network check differences](../../COMPAT.md#network-check-differences))
- **Diagnostics.** The critical analyzer's "thread dump" lists the components in a critical path, not thread stacks. ([Critical analyzer differences](../../COMPAT.md#critical-analyzer-differences))

## Plugins and extensions

Saranga has no JVM, so it cannot load Java classes. Anything you added to Artemis as Java code needs another plan:

- **Broker plugins.** `<broker-plugins>` selects Saranga's ports of the plugins Artemis ships, by their Java class names and with their properties: `LoggingActiveMQServerPlugin`, `NotificationActiveMQServerPlugin`, `BrokerMessageAuthorizationPlugin` and `ConnectionPeriodicExpiryPlugin`. A `<broker-plugin>` naming any other class stops the broker from starting, because leaving out what it enforces could open what it closes. Plugins of your own are written in Rust and registered by an application that embeds the broker. ([Broker plugin differences](../../COMPAT.md#broker-plugin-differences))
- **Transformers.** Divert and core federation transformers are Rust code, registered by an embedding application under the class name `broker.xml` gives. A divert whose transformer is not registered is not deployed, with a warning. Transformers of AMQP federation and AMQP bridge policies are not applied. ([Queue semantics differences](../../COMPAT.md#queue-semantics-differences), [Configuration differences](../../COMPAT.md#configuration-differences))
- **Login modules and security managers.** Only the modules and managers listed in [The files in etc](#the-files-in-etc) are there; a custom module gets a warning and its logins fail, and a custom security manager stops the broker. ([Security differences](../../COMPAT.md#security-differences))
- **Interceptors** are Java classes and are not loaded, and connection routers are not supported. ([STOMP](../../COMPAT.md#stomp-differences), [MQTT](../../COMPAT.md#mqtt-differences), [AMQP](../../COMPAT.md#amqp-differences) and [OpenWire differences](../../COMPAT.md#openwire-differences))
- **Password codecs.** Only `DefaultSensitiveStringCodec` is available.
- **Web apps.** Other `.war` apps in `bootstrap.xml` are not served.

If one of these is essential to you, keep that broker on Artemis. A mixed cluster or HA pair lets the rest move.
