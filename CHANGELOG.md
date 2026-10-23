# Changelog

Each release of Saranga, newest first. Saranga is inspired by Apache ActiveMQ Artemis 2.57.0 and works interchangeably with it. [COMPAT.md](COMPAT.md) lists where the two differ.

## 1.1.0 (2026-10-23)

Saranga's own console.

### The Saranga console

- **A web console built into the binary** at `/saranga/` on every `<web>` binding: no `console.war`, no JVM and no Node.js at run time. It takes the users, console roles and throttling of Artemis's console, and every call goes through the same guard (`management.xml` or the RBAC guard) and audit as Jolokia. `-Dsaranga.console.enabled=false` turns it off.
- **What Artemis's console does**: the overview, the connections, sessions, consumers, producers, addresses and queues, browsing and sending messages and moving, copying, retrying, resending or deleting them, the explorer of every management resource, the diagram and the preferences.
- **Live data**: the pages follow the broker over one server-sent-events stream, the charts open on the last hour of the broker's own history, and the events page shows the management notifications as they come.
- **The cluster**: every node of the topology, Artemis or Saranga, is reached by management messages over Core as the logged-in user, so each node checks that user itself. The listings span every node; pages for the nodes, high availability (with `forceFailover` and `freezeReplication`) and bridges and broker connections.
- **Administration**: address settings with where each value comes from, diverts, acceptors, the configuration exported and reloaded, users and security settings with a permission checker, prepared and heuristic XA transactions, a dead-letter workbench, actions on every message a filter selects, and a queue's messages exported and imported whole.
- **Accessible and at a phone's width**: every page is checked against WCAG 2.1 AA for an admin and a read-only user, on one broker and across a cluster.
- **A typed API** at `/saranga/api/v1`, described as OpenAPI 3.1 at `/saranga/api/v1/openapi.json`.

### The broker

- **`forceFailover` and `freezeReplication`** are implemented as Artemis implements them.
- **`exportConfigAsProperties`** is implemented: the configuration the broker runs on, with the broker properties applied at start and the configured queues as its deployment left them, written as a running Artemis 2.57.0 writes it.
- **`closeSessionWithID`** closes a session of every protocol, as Artemis does, not only Core.
- **Broker properties** set a top-level setting by Artemis's bean name (`IDCacheSize`, `JMXDomain`, `journalBufferTimeout_NIO`), and leave an address's routing types as given, where a queue's routing type was added to them.
- **An address's routing types** are merged at deployment as Artemis merges them, so an address configured without any fails as Artemis's does (AMQ229209).
- **`criticalAnalyzerCheckPeriod`** takes its default when `broker.xml` is read, as Artemis's parser sets it.
- **`java.io.tmpdir`**: `artemis run` sets it to the instance's `tmp` as `bin/artemis` does; on Windows it stays Java's own, as `artemis.cmd` leaves it.

## 1.0.0 (2026-10-21)

The first release.

### The broker

- **Core protocol**: every Core packet, protocol versions 121 to 136, for the Artemis JMS and Core clients. HORNETQ is served for HornetQ 2.4 clients.
- **AMQP 1.0, MQTT 3.1.1 and 5, STOMP 1.0 to 1.2 and OpenWire**, each with Artemis's conversions. WebSocket transport for every protocol, and protocol detection on a single port.
- **Addressing and queues**: anycast and multicast addresses, wildcards, fully qualified queue names, auto-create and auto-delete. Diverts, and the address settings with Artemis's match hierarchy. Dead-letter and expiry addresses, and redelivery delays.
- **Queue types and features**: message groups, exclusive, last-value, non-destructive, ring and temporary queues, scheduled delivery, duplicate detection and priority.
- **Transactions**: local transactions, and XA.

### Storage

- **On-disk compatible with Artemis**: the message and bindings journals, paging and large messages are in Artemis's formats. A data directory moves between Artemis and Saranga in both directions.
- **Paging and large messages**: the PAGE, BLOCK, FAIL and DROP address-full policies, and large messages streamed in chunks and compressed.
- **Storage faults**: past `max-disk-usage`, producers are held back, refused or their messages dropped as `disk-full-policy` says, with Artemis's messages, and a critical I/O error stops the broker.

### Configuration and security

- **Configuration**: Artemis's `broker.xml` schema, parsed and validated as Artemis does it, with the JDK's messages for XML that is not well formed and Xerces's for schema errors, as are `bootstrap.xml` and `management.xml`, `broker.properties`, environment overrides and hot reload. Instances are made by `saranga create` with Artemis's templates.
- **TLS** with PEM, PKCS12 and JKS keystores, SNI and mutual TLS.
- **Authentication**: JAAS login modules (properties file, LDAP, certificates and guest), and the basic security manager.
- **Authorization**: security settings, and password masking.

### Management and operations

- **Management**: management messages to `activemq.management`, Artemis's MBeans through a Jolokia layer, and a REST API.
- **Web console**: Artemis's own web console (`console.war`) is served when it is present.
- **Monitoring**: Prometheus metrics with Artemis's names. The audit log, the critical analyzer and broker plugins as a Rust trait.
- **CLI**: the full `artemis` command line as `saranga`: `run`, `create`, `upgrade`, `producer`, `consumer`, `queue`, `address`, `user`, `check`, `data`, `perf`, `properties` and the shell. Command lines are parsed as Artemis's picocli parses them, so options, values and mistakes give the same results and messages. The messaging commands connect over Core or, with `--protocol AMQP`, over AMQP as Artemis's CLI does through Qpid JMS.

### High availability

- **Shared store**: primary and backup on one journal, taking the same file locks as Java.
- **Replication**: wire-compatible with a Java peer. A Java broker and a Saranga broker can be each other's backup.
- **Failover**: failback, and client failover.
- **Pluggable quorum** through ZooKeeper.

### Clustering and federation

- **Clusters**: discovery by UDP broadcast and static lists, cluster connections with Artemis's load balancing, redistribution and grouping. Clusters can mix Java and Saranga nodes.
- **Bridges and federation**: Core bridges, and Core federation of addresses and queues.
- **AMQP broker connections**: federation, bridges and mirroring, updated by a configuration reload as Artemis updates them.

### Verified

- **Interoperability**: interop suites run the protocols, storage, HA, clustering, management and the CLI against Artemis 2.57.0, with Java and Saranga brokers in each role.
- **Chaos and HA checks**: chaos soaks under network faults, and linearizability checks of HA pairs.
- **Fuzzing and benchmarks**: the protocol codecs (Core, AMQP, MQTT, STOMP, OpenWire, WebSocket), the journal, page and storage records, `broker.xml` and filters are fuzzed, and benchmarks against Artemis are in [docs/benchmarks.md](docs/benchmarks.md).

### Distribution

- **Binaries** for Linux (x86_64, aarch64), Windows (x86_64) and macOS (x86_64, aarch64), laid out as an Artemis home: `bin/saranga`, with `web/` for Artemis's console.
- **A container image** laid out as Artemis's, with the same variables, volume and ports.
