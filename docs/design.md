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
The twelve phases (0 to 11), with their scope, exit checks and current status, are in [PHASES.md](../PHASES.md).

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
3. Write `docs/design.md` (this document), `PHASES.md`, `COMPAT.md` and the first ADRs.
4. Build the Docker harness that starts the Java broker and captures the first golden data.
