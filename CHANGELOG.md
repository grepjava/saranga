# Changelog

Each release of Saranga, newest first. Saranga is inspired by Apache ActiveMQ Artemis 2.57.0 and works interchangeably with it. [COMPAT.md](COMPAT.md) lists where the two differ.

## 1.0.0 (2026-10-19)

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
- **Storage faults**: a full disk refuses sends with Artemis's error, and a critical I/O error stops the broker as Artemis does.

### Configuration and security

- **Configuration**: Artemis's `broker.xml` schema, parsed and validated as Artemis does it, with the JDK's messages for XML that is not well formed and Xerces's for schema errors, as are `bootstrap.xml` and `management.xml`, `broker.properties`, environment overrides and hot reload. Instances are made by `saranga create` with Artemis's templates.
- **TLS** with PEM, PKCS12 and JKS keystores, SNI and mutual TLS.
- **Authentication**: JAAS login modules (properties file, LDAP, certificates and guest), and the basic security manager.
- **Authorization**: security settings, and password masking.

### Management and operations

- **Management**: management messages to `activemq.management`, Artemis's MBeans through a Jolokia layer, and a REST API.
- **Web console**: Artemis's own web console (`console.war`) is served when it is present.
- **Monitoring**: Prometheus metrics with Artemis's names. The audit log, the critical analyzer and broker plugins as a Rust trait.
- **CLI**: the full `artemis` command line as `saranga`: `run`, `create`, `upgrade`, `producer`, `consumer`, `queue`, `address`, `user`, `check`, `data`, `perf` and the shell. Command lines are parsed as Artemis's picocli parses them, so options, values and mistakes give the same results and messages. The messaging commands connect over Core or, with `--protocol AMQP`, over AMQP as Artemis's CLI does through Qpid JMS.

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

- **Interoperability**: every feature is tested against Artemis 2.57.0, with Java and Saranga brokers in each role.
- **Chaos and HA checks**: chaos soaks under network faults, and linearizability checks of HA pairs.
- **Fuzzing and benchmarks**: every codec is fuzzed, and benchmarks against Artemis are in [docs/benchmarks.md](docs/benchmarks.md).

### Distribution

- **Binaries** for Linux (x86_64, aarch64), Windows (x86_64) and macOS (x86_64, aarch64), laid out as an Artemis home: `bin/saranga`, with `web/` for Artemis's console.
- **A container image** laid out as Artemis's, with the same variables, volume and ports.
