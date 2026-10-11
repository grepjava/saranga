# Saranga phases

Saranga is built in twelve phases. Each one ends with something runnable, and its exit check runs against real Artemis clients or real Artemis data, never only against Saranga's own tests. The design behind the phases is in [docs/design.md](docs/design.md). The pinned Artemis version (2.57.0) and the deliberate wire quirks are in [COMPAT.md](COMPAT.md).

| Phase | Name | Status |
|---|---|---|
| 0 | Foundation | Done |
| 1 | Encoding primitives | Done |
| 2 | Single-node in-memory broker (Core) | Done |
| 3 | Persistence, on-disk compatible (first usable release) | Done |
| 4 | Queue semantics parity | Done |
| 5 | Paging and large messages | Done |
| 6 | Config and security | Done |
| 7 | Protocols (AMQP, MQTT, STOMP, OpenWire, WebSockets) | In progress |
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

## Phase 2: Single-node in-memory broker, Core protocol (done)

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

**Exit check (met)**
- All 24 JMS interop tests and the `core-basic` golden scenario pass against `saranga run`, as they do against Artemis 2.57.0.
- The Rust client (`saranga-client`) passes the same 9 broker tests against Saranga and against Artemis. CI runs them against `saranga run` on Linux and Windows.

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

**Exit check (met)** — `scripts/cross-store.sh` runs all of it:
- Saranga recovers every golden data directory Artemis 2.57.0 wrote (7 scenarios, including a killed broker and a journal Artemis compacted) and a client sees what it saw on Artemis (`recovered.txt`). Three scenarios differ only through features of later phases: large messages (Phase 5), scheduled delivery and message groups (Phase 4).
- [tests/interop/jms/Persist.java](tests/interop/jms/Persist.java) passes in every direction through `scripts/persistence.sh`: Saranga to Saranga, Artemis to Saranga and Saranga to Artemis, each after a clean stop and after a kill. It covers persistent and non-persistent messages, acknowledgements, durable subscriptions with selectors, local transactions, persisted delivery counts, prepared XA branches, queue attributes and 4,000 messages over several journal files.
- `scripts/journal-interop.sh`: Artemis's `JournalImpl` loads a journal Saranga wrote (with compaction and every record type) to the same records, and the reverse.
- Every record of every golden journal re-encodes byte for byte, at the journal level (`saranga-journal`) and at the record-body level (`saranga-storage`). `saranga data print` prints what `artemis data print` prints for all 7 scenarios.
- XA in Core sessions: [tests/interop/jms/Phase3.java](tests/interop/jms/Phase3.java) (9 tests) and the `core-xa` scenario pass against Saranga and Artemis.

**Deferred to later phases on purpose:** scheduled-delivery and duplicate-ID records are kept in the journal but not acted on (Phase 4); large messages and paging records are read but not recovered (Phase 5); heuristic completions and `xaForget` (Phase 8); transaction timeouts (Phase 4). Compaction blocks appends while it runs (see COMPAT.md).

## Phase 4: Queue semantics parity

**Scope**
- The address-settings match hierarchy, dead-letter and expiry addresses, redelivery delay and backoff, and max delivery attempts.
- Scheduled delivery, duplicate detection and priority.
- Queue types: message groups (including rebalancing), exclusive, last-value, non-destructive, ring and temporary queues.
- Consumer filters: the selector engine is already done (Phase 2); what remains is the delivery cost of filtered consumers (per-consumer cursors).
- Diverts, exclusive and non-exclusive, with transformers written as Rust plugins.
- Consumer-window tuning.

**Exit check:** the ported Artemis integration tests for each feature pass.

**Exit check (met)**
- [tests/interop/jms/Phase4.java](tests/interop/jms/Phase4.java) has 48 tests ported from the Artemis integration tests. They cover:
  - the address-settings hierarchy;
  - dead-letter and expiry addresses, including auto-created resources and send-to-DLA-on-no-route;
  - redelivery delay and backoff;
  - expiry, both from the reaper and reported by the client;
  - scheduled delivery and duplicate detection;
  - priority, message groups (buckets, rebalance, first key), and exclusive, last-value, non-destructive, ring and temporary queues;
  - consumers-before-dispatch and delay-before-dispatch;
  - selectors, diverts, consumer windows and XA transaction timeouts.

  Run with [tests/interop/jms/phase4.broker.xml](tests/interop/jms/phase4.broker.xml), they pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/jms/phase4.broker.xml SUITE=Phase4 scripts/interop.sh saranga`.
- The Phase 2 and Phase 3 suites still pass.
- `scripts/cross-store.sh` still passes. Saranga now recovers the `persist-durable` and `persist-crash` golden scenarios (scheduled messages and message groups) exactly as Artemis does. Only `core-large` waits for Phase 5.
- Divert transformers are Rust plugins: a `Transformer` registered in `BrokerConfig::transformers` under the class name that `broker.xml` gives.

**Deferred on purpose:**
- Per-consumer cursors for filtered consumers, a delivery-cost optimisation, to Phase 11.
- Diverts and queues created through management, and the slow-consumer policy, to Phase 8.
- Redistribution delay, to Phase 10.

The behaviour differences are listed in COMPAT.md.

## Phase 5: Paging and large messages

**Scope**
- Page stores, page cursors, page transactions and page counters.
- The PAGE, BLOCK, FAIL and DROP address-full policies.
- Large messages: streamed chunks, compression and the `.msg` file format.
- Large-message conversion between protocols.

**Exit check**
- Backlogs of several gigabytes page correctly.
- Paging and large-message directories interoperate with Java in both directions.

**Exit check (met)**
- `scripts/paging-soak.sh` fills a paging queue with 3 GiB (314,572 messages of 10 KiB), stops the broker, starts one on the same data directory and drains the queue. Every message arrives once, in order and intact. It passes in every direction:

  | Sent on | Drained on | Pages | Saranga peak memory |
  |---|---|---|---|
  | Saranga | Saranga | 10 MiB | about 340 MiB |
  | Saranga | Saranga | 20 KiB (one message per page file, 314,572 files) | 64 MiB |
  | Artemis | Saranga | 10 MiB | 337 MiB |
  | Saranga | Artemis | 10 MiB | 358 MiB |

  Saranga's memory does not grow with the backlog: a 1 GiB run peaks at the same level as a 3 GiB one. With 10 MiB pages, the peak is the 50 MiB the address may hold in memory, plus the page being written and the decoded pages cached for the cursors.
- [tests/interop/jms/Phase5.java](tests/interop/jms/Phase5.java) has 40 tests.
  - Large messages (22): bytes and text bodies, compressed bodies, streamed bodies, non-persistent, transacted, redelivered and pre-acknowledged messages, dead-letter and expiry moves, browsing, topic fan-out, selectors, small consumer windows, conversion of oversized regular messages, divert copies, scheduled delivery, duplicate detection, the Core streaming API (`setBodyInputStream`, `setOutputStream`), and consumers closed mid-transfer.
  - Paging (18): paging and depaging, message counts, non-persistent messages, topics and durable subscriptions, transactions, consumer rollback, selectors, dead-letter and expiry moves into paging addresses, paged large messages, browsing, `max-size-messages`, the BLOCK, FAIL and DROP policies, and `page-limit-messages` with `page-full-policy`.

  Run with [tests/interop/jms/phase5.broker.xml](tests/interop/jms/phase5.broker.xml), they pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/jms/phase5.broker.xml SUITE=Phase5 scripts/interop.sh saranga`.
- [tests/interop/jms/Persist.java](tests/interop/jms/Persist.java) gains three tests:
  - large messages;
  - a prepared XA branch with a large message;
  - paging: a queue consumed part-way across pages, durable subscriptions consumed unevenly, committed and rolled-back transactions, a prepared XA branch and a paged large message.

  All three pass through `scripts/persistence.sh` in every direction (Saranga to Saranga, Artemis to Saranga, Saranga to Artemis, and Artemis to Artemis), after a clean stop and after a kill.
- `scripts/cross-store.sh` passes with nothing left for later phases. Saranga recovers `core-large` and the new `persist-paging` golden scenario exactly as Artemis does.
- Every golden page file re-encodes byte for byte. `saranga data print` prints the paging section that `artemis data print` prints.
- The Phase 2, 3 and 4 suites still pass.

**Deferred on purpose:**
- Large-message conversion between protocols, to Phase 7, which brings the other protocols.
- Core frame size limits before and after authentication, to Phase 6, which brings security.
- Large messages in the Rust client; the broker side is complete.
- Paging without persistence, browsers that read through page files, and reading page files incrementally rather than whole, to Phase 11.

The behaviour differences are listed in COMPAT.md.

## Phase 6: Config and security

**Scope**
- The full `broker.xml` schema, where unknown elements produce a warning, not a failure.
- `broker.properties`, environment overrides and hot reload.
- TLS with rustls: PEM, PKCS12 and JKS keystores, SNI and mutual TLS.
- Login modules: properties file, LDAP and certificate.
- Authorization through security settings with the match hierarchy.
- Password masking.

**Exit check:** unmodified Artemis `etc/` directories from typical deployments start Saranga.

**Exit check (met)**: `scripts/etc-check.sh` makes 13 instances with the reference image's own `artemis create` and runs each with `saranga run --etc` exactly as created.
- 10 variants start, take a login and a send and receive round trip, and refuse a wrong password. The variants are: the default, anonymous, NIO with `--blocking` and pre-declared queues and addresses, mapped journal with paging and retention, no persistence, a custom role and user, web options, a replicated primary, a static cluster member and a shared-store primary.
- 3 variants refuse to start with a reason: a shared-store backup, a replicated backup and the JDBC store.
- Every warning they print names something Saranga does not do yet: protocols other than Core, clustering, HA and retention.

What else Phase 6 delivered, each suite passing against both Artemis and Saranga:
- **Security:** properties, guest, certificate and LDAP login modules, password hashes and masks, security settings with role mapping, and checks on every Core operation, including login at `CONNECT`, the pre-authentication topology and frame size limits. Covered by `Phase6.java` (23 tests) and `Ldap.java` (6 tests, against a real OpenLDAP).
- **TLS:** JKS, PKCS12 and PEM stores, mutual TLS, CRLs, SNI, protocol and cipher choices, and reloading the stores. Covered by `Tls.java` (10 tests).
- **`broker.properties`:** `-D` system properties and `brokerconfig.` overrides, including `jaasConfigs`. Covered by `Props.java` (5 tests).
- **Configuration reload:** settings swap, diverts, address and queue deployment, `updateQueue` and the `config-delete-*` policies. Covered by `Reload.java` (7 tests).
- **`saranga run --etc`** reads `bootstrap.xml`, `login.config` and `broker.properties` the way `artemis run` does.
- **`ActiveMQBasicSecurityManager`** with bootstrap users, whose user and role records move between the brokers in both directions: `scripts/basic-security.sh`.
- **`saranga mask`**: the same results as `artemis mask` for the default and keyed two-way codecs, and one-way hashes (`--hash`) for users files.
- **Acceptor address prefixes** (`anycastPrefix`, `multicastPrefix`) on every Core operation: `Prefix.java` (6 tests).
- **1.x clients:** the old `jms.queue.`/`jms.topic.` prefixes are put back on query results and delivered messages. Checked with a real Artemis 1.5.6 JMS client by `scripts/legacy-interop.sh` (`Legacy.java`, 4 tests).

The differences from Artemis are listed in COMPAT.md.

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

**Stages**, each committed on its own:
1. **Groundwork.** Protocol detection on one port from the first 8 bytes, as `ProtocolHandler` does, honouring each acceptor's `protocols`. A `ProtocolManager` trait that protocol crates implement and `saranga run` registers. A protocol-neutral server session (`ServerSessionImpl`) split out of the Core session, which every protocol drives.
2. **STOMP** 1.0, 1.1 and 1.2: frames, heart-beating, subscriptions and ack modes, transactions, receipts and durable subscriptions.
3. **MQTT** 3.1, 3.1.1 and 5: sessions and their persisted state, retained messages, wills, shared subscriptions, QoS 0, 1 and 2, and the MQTT 5 properties.
4. **AMQP 1.0**: the type codec, SASL, connections, sessions and links, flow control, transactions, durable subscriptions, AMQP messages kept in their own format in the journal and in pages (as `AMQPMessagePersisterV3` writes them), and conversion to and from Core.
5. **OpenWire**: the marshallers for every version, the prefetch model, advisories and virtual topics.
6. **WebSockets** for every protocol.
7. **Exit check**: the client matrix and conversion between protocols.

**Progress**
- Stage 1 (groundwork) is done. Acceptors pick each connection's protocol from its first 8 bytes. `ServerSession` holds what Core and every other protocol share. `tests/protocols.rs` in `saranga-server` covers the detection.
- Stage 2 (STOMP) is done. [tests/interop/stomp/Stomp.java](tests/interop/stomp/Stomp.java) has 76 tests ported from Artemis's `StompTest`, `StompV11Test` and `StompV12Test`. They pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/stomp/broker.xml SUITE_FILE=tests/interop/stomp/Stomp.java scripts/interop.sh saranga`. They cover:
  - version negotiation, `
` line ends, and header escaping and repetition;
  - receipts, and the ERROR frames with their texts;
  - the `auto`, `client` and `client-individual` ack modes, NACK, and consumer windows;
  - transactions;
  - topics, durable and no-local subscriptions, and selectors;
  - auto-creation, routing types and the `/queue/` and `/topic/` prefixes;
  - scheduled and expired messages, heart-beats and the TTL;
  - large messages in both directions;
  - messages crossing between STOMP and JMS.
- Stage 3 (MQTT) is done. The codec follows Netty's `MqttDecoder` and `MqttEncoder` 4.1.137, which Artemis uses: the same validations, the same handling of a bad packet, and the same byte layout, including the order of MQTT 5 properties. [tests/interop/mqtt/Mqtt.java](tests/interop/mqtt/Mqtt.java) has 40 tests using Paho 1.2.5 (3.1.1 and 5), raw sockets for byte-level checks, and JMS. They pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/mqtt/broker.xml SUITE_FILE=tests/interop/mqtt/Mqtt.java SUITE_CP='<artemis lib>/*;<paho mqttv3 jar>;<paho mqttv5 jar>' scripts/interop.sh saranga`. They cover:
  - QoS 0, 1 and 2 in both directions, with QoS 2 duplicates and redelivery after a reconnect using the same packet ID;
  - wildcards, and topics containing `.`;
  - retained messages;
  - persistent and clean sessions, and session-present;
  - wills on failure and on keep-alive timeout, and no will after DISCONNECT;
  - link stealing;
  - MQTT 3.1 and its client ID rules;
  - malformed packets;
  - in MQTT 5: properties, subscription identifiers, no-local, retain handling, retain-as-published, shared subscriptions, session expiry, topic aliases, the CONNACK properties, authentication methods, reason codes and Receive Maximum;
  - messages crossing between MQTT and JMS.

  `tests/mqtt.rs` in `saranga-protocol-mqtt` checks that a session and its in-flight packet IDs survive a restart. Session state is kept in `$sys.mqtt.sessions`, packet-ID correlations as journal records of type 54, and QoS 2 packet IDs in the `mqtt.qos.*` duplicate-ID caches, all as Artemis keeps them.
- Stage 4 (AMQP 1.0) is done. Its parts:
  - **The codec** picks the same encodings as Proton-J 0.35.0, which Artemis uses, including its fast-path list forms. 158 values that Proton-J encodes (`tests/golden-gen/amqp/CodecFixtures.java`) and every frame of a Qpid JMS session with Artemis decode and re-encode to the same bytes.
  - **Messages** stay in their AMQP encoding in queues, the journal and pages, as `AMQPStandardMessage` and `AMQPLargeMessage` keep them. The broker works on a view of each message and carries its changes back, re-encoding only where Artemis does. `tests/golden-gen/amqp/MessageFixtures.java` checks these against Artemis's own classes, byte for byte: records, moves to a dead-letter address, diverts, added properties, redelivery, and conversion from Core. Conversion to Core is checked by its content, since Artemis keeps Core properties in a hash map.
  - **On disk.** `tests/golden/amqp-durable` is a journal Artemis wrote through Qpid JMS, with a large message and a message moved to the dead-letter address. `saranga data print` prints it line for line as `artemis data print` does, and Saranga recovers it to what Artemis shows. `Persist.java`'s AMQP cases pass from Artemis to Saranga, from Saranga to Artemis and from Saranga to Saranga, after a clean stop and after a kill.
  - **The protocol head** follows `AMQPConnectionContext`, the link contexts, `AMQPSessionCallback` and `ProtonTransactionHandler`:
    - SASL PLAIN, ANONYMOUS and EXTERNAL;
    - container-ID uniqueness, idle timeouts and heartbeats;
    - producer credit, consumer credit and drain;
    - anonymous relay, dynamic sources and targets, and FQQN sources;
    - durable, shared and volatile subscriptions, named as Artemis names them;
    - selectors and no-local, browsing, and every outcome a client settles with;
    - transactions, including the unsettled acknowledgements that a rolled-back transaction keeps with their consumer.
  - **Interop.** [tests/interop/amqp/Amqp.java](tests/interop/amqp/Amqp.java) has 34 Qpid JMS tests. They pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/amqp/broker.xml SUITE_FILE=tests/interop/amqp/Amqp.java scripts/interop.sh saranga`. They cover message types, properties and headers, queues and topics, durable and shared subscriptions, selectors, no-local, transactions, acknowledgement modes, dead letters, expiry, temporary destinations, browsing, anonymous producers, FQQN, priority, delivery delay, groups, receiveNoWait, a large message, messages crossing between AMQP and Core JMS, and the SASL and Open exchanges on a raw socket.
- Stage 5 (OpenWire) is done. Its parts:
  - **The codec** handles versions 1 to 12, tight and loose, with the marshal cache. Its command structs are generated from the ActiveMQ 5.19.10 marshallers that Artemis ships (`tests/golden-gen/openwire`). About 5,000 frames the Java marshallers wrote decode and re-encode to the same bytes.
  - **Conversion** to and from Core ports `OpenWireMessageConverter`. `tests/golden-gen/openwire/MessageFixtures.java` checks it against Artemis's converter both ways: the Core messages match exactly, property order included, and dispatches match byte for byte except compressed bodies, which match once inflated.
  - **The protocol head** follows `OpenWireConnection`, `AMQSession`, `AMQConsumer` and `OpenWireProtocolManager`:
    - wire-format negotiation, `BrokerInfo`, keep-alive pings and the inactivity TTL;
    - the pre-login gate, client ID uniqueness, and the connection's own session `-1`;
    - sync and async sends, producer windows answered with `ProducerAck`, composite destinations and failover duplicate IDs;
    - the prefetch window with its delivered-ack extension, prefetch 0 with `MessagePull`, forced-delivery markers and null dispatches;
    - every ack type, matched by message ID range, with poison acks to the dead-letter address carrying the failure cause;
    - local and XA transactions; a rollback leaves acknowledged messages with their consumers, as `returnReferences` does;
    - durable and non-durable topic subscriptions, no-local, selectors, browsers, wildcards and virtual topic consumers mapped to FQQNs;
    - temporary destinations, removed with their connection;
    - the connection and destination advisories, and the replay of temporary destinations to advisory subscribers.
  - **Interop.** [tests/interop/openwire/OpenWire.java](tests/interop/openwire/OpenWire.java) has 60 tests using the ActiveMQ 5.19.10 client, ported mostly from Artemis's `SimpleOpenWireTest` and its neighbours. They pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/openwire/broker.xml SUITE_FILE=tests/interop/openwire/OpenWire.java scripts/interop.sh saranga`. They cover message types, compression and encodings, every acknowledgement mode, redelivery and the dead-letter queue, transactions and XA (two-phase commit, recovery, rollback), producer windows, async sends, queues and topics, durable subscriptions, no-local, selectors, browsing, prefetch 0 and 1, groups, composite destinations, wildcards, virtual topics, scheduled and expired messages, temporary destinations, missing destinations, auto-deletion, advisories, keep-alive, duplicate client IDs and messages crossing between OpenWire and Core JMS.
- Stage 6 (WebSockets) is done. An acceptor answers an HTTP request whose first bytes it reads with the WebSocket handshake, as `ProtocolHandler` and Netty's `WebSocketServerHandshaker13` do, offering the subprotocols of its protocols (`v10.stomp`, `v11.stomp`, `v12.stomp`, `mqtt`, `mqttv3.1`, `amqp`). It then detects the protocol again inside the WebSocket. Text, binary and continuation frames carry the stream, pings are answered and a close is echoed. What the broker writes goes out in binary (or, with `webSocketEncoderType=text`, text) frames of at most `webSocketMaxFramePayloadLength` bytes, and a larger frame from the client closes the connection with 1009. [tests/interop/websocket/WebSocket.java](tests/interop/websocket/WebSocket.java) has 19 tests. They pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/websocket/broker.xml SUITE_FILE=tests/interop/websocket/WebSocket.java SUITE_CP='<artemis lib>/*;<paho mqttv3 jar>' scripts/interop.sh saranga`. They cover the handshake and subprotocol choice, refused requests, fragmentation both ways, control frames, STOMP through the JDK's WebSocket client and a raw socket, MQTT through Paho (`ws://`), AMQP through Qpid JMS (`amqpws://`), the OpenWire handshake, and messages crossing to Core JMS.

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
