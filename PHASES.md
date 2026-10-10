# Saranga phases

Saranga is built in twelve phases. Each one ends with something runnable, and its exit check runs against real Artemis clients or real Artemis data, never only against Saranga's own tests. The design behind the phases is in [docs/design.md](docs/design.md). The pinned Artemis version (2.57.0) and the deliberate wire quirks are in [COMPAT.md](COMPAT.md).

| Phase | Name | Status |
|---|---|---|
| 0 | Foundation | Done |
| 1 | Encoding primitives | Done |
| 2 | Single-node in-memory broker (Core) | In progress |
| 3 | Persistence, on-disk compatible (first usable release) | Not started |
| 4 | Queue semantics parity | Not started |
| 5 | Paging and large messages | Not started |
| 6 | Config and security | Not started |
| 7 | Protocols (AMQP, MQTT, STOMP, OpenWire, WebSockets) | Not started |
| 8 | Management and operations | Not started |
| 9 | High availability | Not started |
| 10 | Clustering and federation | Not started |
| 11 | Hardening and 1.0 | Not started |

Phase 3 is the first release anyone could adopt. Every phase after it can ship as a minor release.

---

## Phase 0: Foundation (done)

**Scope**
- Cargo workspace with one crate per subsystem, CI on Linux and Windows (fmt, clippy with warnings as errors, tests, cargo-deny), Apache-2.0 licence and NOTICE.
- Artemis 2.57.0 pinned and built from source into the Docker image `saranga/artemis-ref:2.57.0` by [scripts/build-artemis.sh](scripts/build-artemis.sh).
- Golden-corpus harness:
  - [scripts/golden.sh](scripts/golden.sh) runs a Java client scenario against the real broker through a recording proxy ([tools/saranga-capture](tools/saranga-capture)) and keeps the broker's data directory.
  - [scripts/packet-fixtures.sh](scripts/packet-fixtures.sh) has the real Java classes encode single packets.
- ADRs for the runtime model, I/O backends and error model in [docs/adr/](docs/adr/).

**Exit check (met):** CI is green, and the harness starts a Java broker and collects golden data.

## Phase 1: Encoding primitives (done)

**Scope**
- `saranga-buffer`: Artemis buffer semantics, `SimpleString`, Java string encodings, `TypedProperties`, both nullable encodings.
- `saranga-message`: the `CoreMessage` codec.
- `saranga-core-protocol`: frames and every Core packet type, about 120 of them, across protocol versions 121 to 136. This covers connection, session, XA, large-message, cluster/HA/federation and replication traffic.

**Exit check (met)**
- Every frame in the recorded conversations (`core-basic`, `core-xa`, `core-large`) decodes to a typed packet and re-encodes byte for byte. `tests/golden_wire.rs` fails if any packet falls back to `Unknown`.
- 208 fixtures encoded by the Java classes re-encode byte for byte (`tests/golden_packets.rs`).
- Every packet type constant in Artemis has a decoder.

## Phase 2: Single-node in-memory broker, Core protocol (in progress)

**Scope**
- TCP acceptor (`saranga-transport`) that accepts the `ARTEMIS` handshake and Core frames.
- Connections: pings and connection TTL, the cluster-topology subscription (answered with a single-node topology), and disconnect.
- Sessions: create/close, start/stop, metadata, and the queue and binding queries the JMS client issues before every producer and consumer.
- Addresses and queues: ANYCAST and MULTICAST routing, wildcards, fully qualified queue names (FQQN), auto-create and auto-delete. Everything in Phase 2 is kept in memory, durable or not.
- Producers and consumers with credit-based flow control: producer credits and the consumer window.
- Acknowledgements (individual and cumulative), local transactions (commit and rollback, with redelivery and delivery counts) and browse-only consumers.
- Enough of `broker.xml` for acceptors, address-settings (with Artemis's match rules) and addresses.
- The JMS selector engine (`saranga-filter`). It was planned for Phase 4, but durable subscriptions with selectors are part of the Phase 2 exit check, so it moved forward.
- `saranga run`.
- A minimal Rust Core client (`saranga-client`).

**Exit check**
- The Artemis JMS client passes send/receive, queue, topic, durable-subscription, transacted and client-ack tests against `saranga run`. `scripts/interop.sh saranga` runs [tests/interop/jms/Phase2.java](tests/interop/jms/Phase2.java) (24 tests) and the `core-basic` golden scenario; `scripts/interop.sh artemis` runs the same suite against the reference broker, so a wrong test shows up there first.
- The Rust client sends and receives through Saranga.

**Progress**
- Done: the broker core, `saranga run --config`, selectors. All 24 JMS interop tests and `core-basic` pass against Saranga, as they do against Artemis 2.57.0.
- Remaining: the Rust client and its CI tests against Saranga.

**Deferred to later phases on purpose:** XA (Phase 3), large messages (Phase 5), expiry and dead-letter addresses, redelivery delay and max delivery attempts (Phase 4), acceptor address prefixes and the 1.x client prefixes (Phase 6), security (Phase 6).

## Phase 3: Persistence, on-disk compatible (first usable release)

**Scope**
- Journal file format: file headers, every record type, transactional records, reclaim, and compaction with its rename protocol.
- The bindings journal and the full set of `StorageManager` record IDs.
- Recovery on restart, XA (prepare, commit, rollback, recover) and the group-commit journal writer.
- Durable messages.
- `saranga data print`.

**Exit check**
- The Java broker recovers a data directory Saranga wrote, and Saranga recovers one the Java broker wrote, for every scenario in the corpus.
- Crash tests that kill the broker with `kill -9` lose no committed messages.

## Phase 4: Queue semantics parity

**Scope**
- The address-settings match hierarchy, dead-letter and expiry addresses, redelivery delay and backoff, and max delivery attempts.
- Scheduled delivery, duplicate detection and priority.
- Queue types: message groups (including rebalancing), exclusive, last-value, non-destructive, ring and temporary queues.
- Consumer filters: the selector engine is already done (Phase 2); what remains is the delivery cost of filtered consumers (per-consumer cursors).
- Diverts, exclusive and non-exclusive, with transformers written as Rust plugins.
- Consumer-window tuning.

**Exit check:** the ported Artemis integration tests for each feature pass.

## Phase 5: Paging and large messages

**Scope**
- Page stores, page cursors, page transactions and page counters.
- The PAGE, BLOCK, FAIL and DROP address-full policies.
- Large messages: streamed chunks, compression and the `.msg` file format.
- Large-message conversion between protocols.

**Exit check**
- Backlogs of several gigabytes page correctly.
- Paging and large-message directories interoperate with Java in both directions.

## Phase 6: Config and security

**Scope**
- The full `broker.xml` schema, where unknown elements produce a warning, not a failure.
- `broker.properties`, environment overrides and hot reload.
- TLS with rustls: PEM, PKCS12 and JKS keystores, SNI and mutual TLS.
- Login modules: properties file, LDAP and certificate.
- Authorization through security settings with the match hierarchy.
- Password masking.

**Exit check:** unmodified Artemis `etc/` directories from typical deployments start Saranga.

## Phase 7: Protocols

**Scope**
- AMQP 1.0: transactions, links, durable subscriptions and message conversion.
- MQTT 3.1.1 and 5: sessions, retained messages, wills, shared subscriptions, and QoS 0, 1 and 2.
- STOMP 1.0 to 1.2.
- OpenWire: advisories, virtual topics and the prefetch model.
- WebSocket transport for every protocol, and protocol detection on a single port.

**Exit check**
- The client test matrix passes for every protocol: Qpid JMS, the ActiveMQ 5 client, Paho, a STOMP client, and Python, .NET and Rust AMQP clients.
- Messages convert correctly between protocols, for example MQTT in and JMS out.

## Phase 8: Management and operations

**Scope**
- A REST API covering the operations of the `ActiveMQServerControl`, `AddressControl` and `QueueControl` equivalents.
- Prometheus metrics with the same metric names Artemis exposes.
- Management through messages sent to `activemq.management`.
- The full CLI.
- Plugin hooks: a Rust trait equivalent to `ActiveMQServerPlugin`.
- The critical analyzer.
- Logging and the audit log through `tracing`.

**Exit check:** operational runbooks written for Artemis work with the Saranga equivalents.

## Phase 9: High availability

**Scope**
- Shared-store primary/backup using file locks on the journal directory, including NFS lock semantics.
- Replication: initial sync and live journal replication over the Core replication packets, wire-compatible with a Java peer.
- Failback.
- Client failover and reconnection.
- Pluggable quorum through ZooKeeper.

**Exit check**
- Failover tests lose no committed messages.
- A Java primary works with a Saranga backup, and a Saranga primary works with a Java backup.

## Phase 10: Clustering and federation

**Scope**
- Discovery by UDP broadcast and static lists. JGroups is supported only as far as UDP compatibility needs; full JGroups is not planned.
- Cluster connections and topology broadcast to clients.
- Server-side load balancing: OFF, STRICT, ON_DEMAND and OFF_WITH_REDISTRIBUTION.
- Redistribution and grouping handlers.
- Core bridges.
- Federation of addresses and queues, upstream and downstream.
- AMQP broker connections, including mirroring.

**Exit check**
- Mixed clusters of Java and Saranga nodes work.
- Federation and mirroring work in both directions.

## Phase 11: Hardening and 1.0

**Scope**
- Soak and chaos testing: network partitions, full disks and injected fsync failures.
- Jepsen-style linearizability checks on HA.
- Fuzzing every codec with cargo-fuzz.
- Benchmarks against Artemis: throughput, p99 latency, memory and startup time.
- Documentation, a migration guide, container images and the release.

**Exit check:** Saranga 1.0 ships.

---

## How every phase is verified
- `cargo test --workspace` runs the unit tests, property tests and golden-corpus round-trips.
- `scripts/golden.sh <scenario>` records new real-broker traffic and data. `scripts/packet-fixtures.sh <group>` regenerates the Java-encoded packet fixtures.
- From Phase 2 on, an interop script runs the same Java client scenarios against Saranga.
- From Phase 3 on, a cross-store check moves data directories from Java to Saranga and from Saranga to Java.
- From Phase 9 on, CI runs a mixed Java and Saranga cluster test.
