# Saranga: Apache Artemis rewritten in pure Rust (design and phases)

## Context
`D:\code\saranga` is empty. The goal is Saranga, a pure-Rust message broker that clones Apache ActiveMQ Artemis. Scope (confirmed with the user):
- **Wire-compatible and on-disk compatible.** Existing Artemis/JMS clients connect unchanged. Saranga reads and writes Artemis journal, bindings, paging and large-message files, so a Java broker's data directory can be moved over in place, and back again.
- **Every protocol Artemis supports:** Core, AMQP 1.0, MQTT 3.1.1/5, STOMP, OpenWire, and all of them over WebSockets.
- **Full HA and clustering parity:** shared-store, replication, pluggable quorum, cluster connections, redistribution, bridges, federation, and AMQP broker connections (including mirroring).
- **Management:** REST API, Prometheus metrics and a CLI. No JMX or Jolokia.

Artemis is very large (about 1M+ lines of Java including tests), so this is a multi-year effort. The phases are ordered so that every phase ends with something runnable and testable against real Artemis clients and real Artemis data files. The Java code is the specification; we port behaviour, not structure.

**Target version:** Artemis **2.57.0**, recorded in `COMPAT.md`. All golden files and interop tests use that version.

**Licence:** Artemis is Apache-2.0, and a port is a derivative work. Saranga is therefore Apache-2.0, with a NOTICE file crediting the Apache ActiveMQ project. Do not use the Artemis or ActiveMQ names or branding for the product.

---

## Architecture

### Runtime model
- **tokio** multi-threaded runtime. Each connection runs as a task that owns its codec and session state.
- **Each queue is an actor** (one task plus an mpsc mailbox). It owns its message-reference list, its consumers and its delivery loop. This replaces Artemis's per-queue `OrderedExecutor` and keeps the delivery path lock-free.
- **The journal has a single writer task with group commit.** This mirrors Artemis's `TimedBuffer`: appends are batched, then one `fdatasync` covers the batch, and every waiting completion future is resolved afterwards. I/O backends:
  - io_uring on Linux (the equivalent of ASYNCIO/libaio)
  - buffered std file I/O (the equivalent of NIO) on every platform, including Windows
  - mmap (the equivalent of MAPPED)
- **Messages are `Arc<Message>` with cached encoded bytes**, so a message is not re-encoded for every consumer or for persistence. Artemis does the same.
- **Global memory accounting** (`global-max-size`, per-address `max-size-bytes`) drives paging, blocking and failing, exactly as Artemis does.

### Cargo workspace (`crates/`)
| Crate | Responsibility | Main Artemis counterpart |
|---|---|---|
| `saranga-buffer` | `ActiveMQBuffer` semantics, `SimpleString` (UTF-16), `TypedProperties`, nullable encodings | `artemis-commons` |
| `saranga-message` | `CoreMessage` encoding/decoding, message properties, AMQP and OpenWire message wrappers, conversion between them | `CoreMessage`, `AMQPMessage`, converters |
| `saranga-core-protocol` | Core packet codec (every packet type, every protocol version), channels, ping, versioning | `core.protocol.core.impl.wireformat` |
| `saranga-filter` | JMS selector parser and evaluator, including Artemis extensions | `artemis-selector` |
| `saranga-journal` | Append-only journal: file format, records, transactions, reclaim, compaction, I/O backends | `artemis-journal` |
| `saranga-storage` | `StorageManager`: journal record IDs and payloads, bindings journal, large messages, ID generator | `JournalStorageManager`, `JournalRecordIds` |
| `saranga-paging` | Page stores, page files, cursors, page transactions, page counters | `paging.*` |
| `saranga-server` | PostOffice, addresses, queues, routing, bindings, diverts, address settings, transactions (local and XA), scheduled delivery, duplicate-ID cache, redelivery, DLA/expiry, LVQ/ring/non-destructive/exclusive/groups | `core.server.*`, `postoffice.*` |
| `saranga-config` | `broker.xml` parser that accepts the full schema, plus env/system-property overrides and hot reload | `FileConfiguration`, `artemis-configuration.xsd` |
| `saranga-security` | Authentication (properties files, LDAP, certificates, Kerberos later), role-based authorization with match hierarchy, security caching | `security.*`, JAAS login modules |
| `saranga-transport` | Acceptors and connectors: TCP, TLS (rustls), WebSocket, multi-protocol detection on one port, Netty URI/parameter compatibility | `remoting.impl.netty` |
| `saranga-protocol-amqp` | AMQP 1.0, built on a ported or own codec | `artemis-amqp-protocol` |
| `saranga-protocol-mqtt` | MQTT 3.1.1/5, including session state and retained messages | `artemis-mqtt-protocol` |
| `saranga-protocol-stomp` | STOMP 1.0/1.1/1.2 | `artemis-stomp-protocol` |
| `saranga-protocol-openwire` | OpenWire marshallers (v1–v12) and the ActiveMQ 5 client semantics | `artemis-openwire-protocol` |
| `saranga-ha` | Shared-store (file locks), replication (Core replication packets), pluggable quorum (ZooKeeper) | `server.impl.*Activation`, `replication.*`, `artemis-quorum-*` |
| `saranga-cluster` | Discovery (UDP broadcast, static, JGroups-compatible where feasible), cluster connections, load balancing, redistribution, core bridges, federation, AMQP broker connections and mirroring | `cluster.*`, `federation.*`, `connect.*` |
| `saranga-management` | REST API (axum), Prometheus exporter, management messages sent to `activemq.management` | `management.*`, `artemis-prometheus-metrics-plugin` |
| `saranga-client` | Rust Core-protocol client. Used for testing and also shippable on its own | `artemis-core-client` |
| `saranga-cli` | Binary named `saranga`: `create`, `run`, `stop`, `queue`, `address`, `producer`/`consumer`, `perf`, `data print/exp/imp/compact`, `mask` | `artemis-cli` |

### Compatibility strategy (the core of the project)
1. **Reference checkout.** Clone `apache/activemq-artemis` at the pinned tag into `reference/` (git-ignored). Every module we port links back to the Java classes it mirrors.
2. **Golden corpus.** Scripts run the real Java broker (Docker) to produce:
   - pcap/byte captures of each protocol conversation
   - journal, bindings, paging and large-message directories for many scenarios (transactions, compaction, paging, XA prepared, scheduled, duplicate IDs)

   Saranga decodes the corpus and re-encodes it byte for byte. It also writes data dirs that the Java `artemis data print` tool and a Java broker can recover.
3. **Interop test matrix.** Run in CI and in Docker against Saranga:
   - Artemis JMS (Core) client
   - Qpid JMS (AMQP)
   - ActiveMQ 5 client (OpenWire)
   - Paho (MQTT 3 and 5)
   - a STOMP client
   - Python, .NET and Rust AMQP clients

   Where it is practical, port the relevant parts of the Artemis integration test suite (`tests/integration-tests`) as black-box tests.
4. **Mixed-cluster tests.** A Java Artemis node and a Saranga node act as primary/backup and as cluster peers. This verifies replication and cluster wire compatibility and gives users a zero-downtime migration path.

---

## Phases
Each phase has an exit criterion that is checked with real clients and data.

**Phase 0: Foundation.**
- Set up the workspace, CI (Linux and Windows), lint/fmt/deny, Apache-2.0 + NOTICE, and `COMPAT.md`.
- Clone the reference source and pin the Artemis version.
- Build the Docker interop harness and golden-corpus generator scripts.
- Write ADRs: runtime model, I/O backends, error model.

*Exit:* CI is green, and the harness can start a Java broker and collect golden data.

**Phase 1: Encoding primitives.**
- Implement `saranga-buffer`, the `CoreMessage` codec in `saranga-message`, and the full `saranga-core-protocol` packet set across protocol versions.
- Add property-based round-trip tests.

*Exit:* Every captured Core frame and message in the corpus decodes and re-encodes byte for byte.

**Phase 2: Single-node in-memory broker (Core).**
- Transport: a TCP acceptor.
- Connection, session, producer and consumer handling.
- Credit-based flow control (consumer window, producer credits).
- Addresses, queues, ANYCAST/MULTICAST routing, wildcards, FQQN, auto-create/delete, and non-durable delivery.
- Local transactions, acks and the minimal `broker.xml` subset.
- `saranga run`.

*Exit:* The Artemis JMS client passes send/receive, topic/queue, transacted and client-ack tests. The Rust client works.

**Phase 3: Persistence (on-disk compatible). This is the first usable milestone.**
- Journal file format: headers, all record types, transactional records, reclaim, compaction and its rename protocol.
- Bindings journal and the full set of `StorageManager` record IDs.
- Recovery on restart, XA (prepare/commit/rollback/recover) and the group-commit writer.
- Durable messages.
- `saranga data print`.

*Exit:*
- A Saranga-written data dir is recovered by the Java broker, and a Java-written one by Saranga, across the whole corpus.
- Kill -9 crash tests lose no committed messages.

**Phase 4: Queue semantics parity.**
- Address-settings match hierarchy, DLA/expiry, redelivery delay/backoff, max-delivery-attempts.
- Scheduled delivery, duplicate detection, priority, message groups (including rebalance), exclusive, last-value, non-destructive, ring and temporary queues.
- Consumer filters (full selector grammar), diverts (exclusive/non-exclusive, transformers as Rust plugins) and consumer-windows tuning.

*Exit:* The ported Artemis integration tests for each feature pass.

**Phase 5: Paging and large messages.**
- Page store, page cursors, page transactions, page counters, and the PAGE/BLOCK/FAIL/DROP policies.
- Large messages: streaming chunks, compression, and the `.msg` file format.
- Conversion of large messages between protocols.

*Exit:* Multi-GB backlogs page correctly. Paging and large-message directories interoperate with Java in both directions.

**Phase 6: Config and security.**
- Full `broker.xml` schema (unknown elements warn, they do not fail), `broker.properties`, env overrides and hot reload.
- TLS (rustls, PEM/PKCS12/JKS keystores), SNI and mTLS.
- Login modules: properties-file, LDAP and certificate. Authorization via security-settings with the match hierarchy, plus password masking.

*Exit:* Unmodified Artemis `etc/` directories from typical deployments start Saranga.

**Phase 7: Protocols.**
- AMQP 1.0, including transactions, links, durable subscriptions and message conversion.
- MQTT 3.1.1/5, including sessions, retained messages, will, shared subscriptions and QoS 0/1/2.
- STOMP 1.0–1.2.
- OpenWire, including advisories, virtual topics and the prefetch model.
- WebSocket transport for all of them, and protocol detection on a single port.

*Exit:* The client matrix passes for every protocol. Messages convert correctly across protocols, for example MQTT in and JMS out.

**Phase 8: Management and operations.**
- REST API covering the operations of the `ActiveMQServerControl`, `AddressControl` and `QueueControl` equivalents.
- Prometheus metrics with the same metric names Artemis exposes.
- Management via messages to `activemq.management`.
- Full CLI, plugin hooks (the `ActiveMQServerPlugin` equivalent as a Rust trait) and critical analyzer.
- Logging and audit log via `tracing`.

*Exit:* Ops runbooks written for Artemis work with the Saranga equivalents.

**Phase 9: High availability.**
- Shared-store primary/backup using file locks on the journal dir and the NFS lock semantics.
- Replication: initial sync and live journal replication over the Core replication packets, wire-compatible with a Java peer.
- Failback, client failover and reconnection, and the pluggable quorum (ZooKeeper).

*Exit:*
- Failover tests pass with zero loss of committed messages.
- A Java primary works with a Saranga backup, and the reverse.

**Phase 10: Clustering and federation.**
- Discovery: UDP broadcast and static. JGroups only for UDP compatibility; full JGroups is not planned.
- Cluster connections, topology broadcast to clients and server-side load balancing (OFF/STRICT/ON_DEMAND/OFF_WITH_REDISTRIBUTION).
- Redistribution, grouping handlers, core bridges, federation (address and queue, upstream and downstream), and AMQP broker connections including mirroring.

*Exit:* Mixed Java+Saranga clusters work. Federation and mirroring are verified in both directions.

**Phase 11: Hardening and 1.0.**
- Soak and chaos testing: network partitions, disk-full, fsync failure injection.
- Jepsen-style linearizability checks on HA.
- Fuzzing every codec with cargo-fuzz.
- Benchmarks against Artemis (throughput, p99 latency, memory, startup time).
- Docs and migration guide, container images and release.

*Exit:* Saranga 1.0.

---

## Key risks and mitigations
- **Undocumented binary formats** (journal compaction, page counters, replication sync). Mitigation: rely on the golden corpus and the Java source as the authority, and test both read and write directions for every format.
- **Core protocol version skew across client releases.** Mitigation: a version matrix in the interop harness covering the pinned version and older client releases.
- **Selector and conversion semantics edge cases.** Mitigation: port the Artemis unit tests for the selector and converters directly.
- **Scope.** Mitigation: Phase 3 is the first point where users can adopt Saranga. Each later phase can ship as a minor release, with features gated by Cargo features.
- **No Linux AIO on Windows.** Mitigation: the buffered backend is the default there, as Artemis's NIO is.

## Verification (applies to every phase)
- `cargo test --workspace` covers unit tests, property tests and golden-corpus round-trips.
- `scripts/interop.sh <phase>` brings up Saranga plus the client containers and runs the matrix.
- `scripts/cross-store.sh` checks data dirs both ways: Java to Saranga and Saranga to Java.
- From Phase 9 on, a mixed-cluster test runs in CI.

## Immediate next step after approval
Phase 0:
1. `git init` and create the workspace skeleton with empty crates.
2. Shallow-clone Artemis at the pinned tag into `reference/`.
3. Write `docs/design.md` (this document), `docs/phases.md`, `COMPAT.md` and the first ADRs.
4. Build the Docker harness that starts the Java broker and captures the first golden data.
