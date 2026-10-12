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
| 7 | Protocols (AMQP, MQTT, STOMP, OpenWire, WebSockets) | Done |
| 8 | Management and operations | In progress |
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
- Stage 7 (the exit check) is done, and with it Phase 7. Every client in the matrix passes against Artemis 2.57.0 and against Saranga:
  - **Java clients**, in the suites of the stages above: the Artemis JMS client (Core), Qpid JMS (AMQP), the ActiveMQ 5 client (OpenWire), Paho 3.1.1 and 5 (MQTT), and STOMP over raw sockets. The same suites cover WebSockets.
  - **Python, .NET and Rust AMQP clients**: Qpid Proton's Python binding (`python-qpid-proton` 0.40.0), AMQPNetLite 2.5.4 and fe2o3-amqp 0.18.2. Each runs the same checks from [tests/interop/clients](tests/interop/clients): header fields and application properties, a modified or released delivery coming back, rejection, a 600 KB body, and a map body. `scripts/client-matrix.sh saranga` fetches and builds them outside the repository and runs them.
  - **Conversion between protocols.** [tests/interop/conversion/Conversion.java](tests/interop/conversion/Conversion.java) sends from each of Core JMS, Qpid JMS, the ActiveMQ 5 client, STOMP and MQTT 5 to each of them: 25 pairs. It checks the body, a string property, and whether a JMS receiver gets text or bytes. MQTT user properties reach only MQTT receivers, as in Artemis. Run it with `CONFIG=tests/interop/conversion/broker.xml SUITE_FILE=tests/interop/conversion/Conversion.java SUITE_CP='<artemis lib>/*;<paho mqttv5 jar>' scripts/interop.sh saranga`.

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

**Stages**, each committed on its own:
1. **Management messages.** Messages sent to the management address (`activemq.management`), from any protocol, are handled as `ManagementServiceImpl.handleMessage` handles them: the resource and the attribute or operation from the `_AMQ_ResourceName`, `_AMQ_Attribute` and `_AMQ_OperationName` properties, the parameters as a JSON array in the body, matched to an operation by name and count and converted as `invokeOperation` converts them, and the result or the error text stored in the reply with `_AMQ_OperationSucceeded`. The `manage` permission is checked, and the reply goes to the reply-to address with the request's correlation ID. The queue statistics Artemis keeps (messages added, acknowledged, expired and killed; delivering, scheduled and durable counts and sizes; the first message's age). The `broker`, `address.<name>` and `queue.<name>` resources: their attributes, and the operations on addresses, queues, diverts, address settings and the messages in a queue.
2. **Connections, sessions, consumers and producers, transactions and users.** Listing and closing them, with the JSON Artemis returns and the filtered, paged `list*` operations; prepared and heuristically completed transactions, with `xaForget`; the users of the basic security manager; security settings and their caches; management notifications to `activemq.notifications`; the view and edit permissions of `management-message-rbac`; message counters; and the slow-consumer policy.
3. **The CLI.** The Artemis CLI's `queue`, `address`, `user`, `producer`, `consumer`, `browser`, `transfer`, `check` and `stop` work against Saranga, and `saranga` has the same commands, plus `create` for a broker instance.
4. **The REST API and Prometheus metrics**, served on the `<web>` binding of `bootstrap.xml`. The REST API reads attributes and invokes operations of the same resources; the metrics have the names the Artemis Prometheus plugin exports. `browse` is served here, as JSON message maps; over management messages it stays unsupported (see COMPAT.md). An optional Jolokia compatibility layer serves the same resources to the Artemis console and other Jolokia tools.
5. **Plugins, the critical analyzer, logging and the audit log.**
6. **Exit check:** runbooks written for Artemis, run against Saranga.

**Progress**
- Stage 1 (management messages) is done. A message sent to the management address from Core, JMS or AMQP is a request, answered on its reply-to address with the request's correlation ID, as `ManagementServiceImpl.handleMessage` answers it. Its parts:
  - **Dispatch.** Resources `broker`, `address.<name>`, `queue.<name>` and `divert.<name>`. Attributes are read as the control's getter would be, getters work as operations too, and an operation is matched by name, parameter count and the types `invokeOperation` accepts. Errors carry Artemis's texts (`AMQ229067`, `AMQ229069`, the operations' own).
  - **JSON** is written as Johnzon writes it for Artemis: members in order, Java's number formats, the same escaping.
  - **Queue statistics** as `QueueImpl` keeps them: messages added, acknowledged, expired, killed and replaced, with the reason each message left; delivering, scheduled and durable counts and sizes; persistent sizes; the first message's age.
  - **Queue operations:** counting (with filters and grouping), listing (queued, scheduled, delivering, consumers, groups), removing, moving (with duplicate rejection and message counts), copying, expiring, sending to the dead-letter address, retrying from it, changing priority, delivering scheduled messages now, pausing (persisted or not), enabling and disabling, resetting counters and groups, and sending a message.
  - **Address operations and attributes:** routed and unrouted counts, queue and binding names, paging state, pausing (persisted or not), blocking and unblocking producers, purging, the duplicate-ID cache, and sending a message.
  - **Broker:** creating, updating and deleting addresses and queues in every form of the operations (text, JSON and positional), `getAddressInfo`, `listBindingsForAddress`, `listAddresses`, the paged, filtered and sorted `listQueues` and `listAddresses` views the CLI reads, diverts (created, updated and destroyed), address settings as JSON (added, read and removed), `reloadConfigurationFile`, and the broker's attributes, disk usage included.
  - **Records.** Paused queues and addresses, address settings and diverts are journaled as Artemis journals them and recovered at startup (`tests/management.rs` checks a restart).
  - **Interop.** [tests/interop/management/Management.java](tests/interop/management/Management.java) has 34 tests using `ClientRequestor`, `JMSManagementHelper` with a `QueueRequestor`, and Qpid JMS. They pass against Artemis 2.57.0 and against Saranga: `CONFIG=tests/interop/management/broker.xml SUITE_FILE=tests/interop/management/Management.java scripts/interop.sh saranga`.
- Stage 2 (connections, sessions, consumers and producers, transactions, users, security settings, notifications, `management-message-rbac`, message counters and the slow-consumer policy) is done:
  - **Registry.** Every protocol's connections are listed with their ID, protocol, implementation, addresses and creation time, and their sessions with their users, consumers and producers (`RemotingService.getConnections`, `ActiveMQServer.getSessions`).
  - **Counts:** `ConnectionCount`, `TotalConnectionCount`, `SessionCount`, `TotalSessionCount`.
  - **Consumer metrics** as `ServerConsumerMetrics` keeps them: messages in transit and their size, delivered and acknowledged counts and sizes, acknowledgements awaiting commit, and the last delivery and acknowledgement times.
  - **Producers** as each protocol makes them: a Core client from 2.28 names its producers and a 1.x or older 2.x client gets one per address (`ServerLegacyProducersImpl`); an AMQP producer per receiving link, an OpenWire producer per `ProducerInfo`, and one per STOMP or MQTT session, made at its first send. Each counts its messages, their size and the last message's ID.
  - **Listing:** `listConnectionIDs`, `listRemoteAddresses`, `listSessions`, `listConnectionsAsJSON`, `listSessionsAsJSON`, `listAllSessionsAsJSON` (with session metadata), `listConsumersAsJSON`, `listAllConsumersAsJSON`, `listProducersInfoAsJSON`, and the paged, filtered and sorted `listConnections`, `listSessions`, `listConsumers` and `listProducers` views, with the fields their predicates filter on and their errors for an unknown field.
  - **Closing:** `closeConnectionWithID`, `closeConnectionsForAddress`, `closeConnectionsForUser` and `closeConsumerConnectionsForAddress` fail the connection as if its client had gone, whatever its protocol. `closeConsumerWithID` and `closeSessionWithID` tell the client its consumers are gone as each protocol does (`DisconnectConsumerMessage` for Core, a detach with `amqp:resource-deleted` for AMQP, an `ERROR` frame for STOMP, the consumer removed for MQTT, and a failed connection for OpenWire); a destroyed queue's consumers are told the same way.
  - **Transactions:** `listPreparedTransactions`, `listPreparedTransactionDetailsAsJSON` and `AsHTML` (the messages sent and acknowledged in each branch), and `commitPreparedTransaction` and `rollbackPreparedTransaction`, which complete a branch and journal a `HEURISTIC_COMPLETION` record, as Artemis does. The completed branches are listed by `listHeuristicCommittedTransactions` and `listHeuristicRolledBackTransactions`, returned by XA recovery, refused to the transaction manager's commit or rollback with `XA_HEURCOM` or `XA_HEURRB`, and dropped by `xaForget`, which deletes the record. `tests/management.rs` checks the record across restarts.
  - **Users:** `addUser`, `resetUser`, `removeUser` and `listUser`, on the users of the basic security manager in the bindings journal, or on the users and roles files of the JAAS `PropertiesLoginModule`, edited as Artemis edits them (comments, order and separators kept, a changed role moved to the end, a new empty file given the license header, passwords hashed as `ENC(...)` unless plain text is asked for). As in Artemis, the security manager exists with security disabled, so users can be managed before security is turned on.
  - **Security settings:** `addSecuritySettings` in all its forms, `removeSecuritySettings`, `getRoles` and `getRolesAsJSON`. The settings are journaled as `SECURITY_SETTING_RECORD` and restored at startup the way Artemis restores them, quirks included (view and edit roles are lost, and manage, create-address and delete-address are granted to a role whose name appears anywhere in those lists). `tests/management.rs` checks a restart. The caches: `clearAuthenticationCache`, `clearAuthorizationCache`, their sizes, and the authentication and authorization counts.
  - **Notifications** go to the management notification address as Artemis sends them: durable messages with `_AMQ_NotifType`, `_AMQ_NotifTimestamp` and the properties of each type, for `BINDING_ADDED`, `BINDING_REMOVED` and `BINDING_UPDATED` (queues and diverts), `CONSUMER_CREATED` and `CONSUMER_CLOSED`, `SESSION_CREATED` and `SESSION_CLOSED` (unless `suppress-session-notifications`), `SECURITY_AUTHENTICATION_VIOLATION`, `SECURITY_PERMISSION_VIOLATION` and `ACCEPTOR_STARTED`. They start once the broker is active and are routed in order, to the notification address, which the broker adds (MULTICAST, journaled) as it activates, as Artemis does.
  - **`management-message-rbac`.** With it on, a management message also needs the view permission (for a method `view-permission-method-match-pattern` matches) or the edit permission on `<management-rbac-prefix>.<resource>.<method>`, checked once the resource is found, as `ManagementServiceImpl.securityCheck` checks it. An attribute is checked as its getter: `is<Attribute>` for a boolean, `get<Attribute>` otherwise.
  - **Message counters** as `MessageCounter` keeps them: with `message-counter-enabled` (or `enableMessageCounters`), each queue is sampled every `message-counter-sample-period` for its count, depth and their deltas, the last add, acknowledgement and update times, and an hourly history kept for `message-counter-max-day-history` days. `listMessageCounter`, `listMessageCounterHistory`, their HTML forms and `resetMessageCounter` on a queue; enabling, disabling, resetting all counters and histories, and setting the sample period and day count on the broker.
  - **The slow-consumer policy.** A queue whose address settings give a `slow-consumer-threshold` is checked every `slow-consumer-check-period` seconds as `SlowConsumerReaperRunnable` checks it: once the queue's message rate or depth reaches the threshold times its consumers, each consumer acknowledging fewer messages a second than the threshold (in its `slow-consumer-threshold-measurement-unit`) is logged and either reported with a `CONSUMER_SLOW` notification (`NOTIFY`) or has its connection failed (`KILL`, with `DisconnectConsumerWithKillMessage` first for a Core client of version 128 or later). Settings changed at runtime reschedule the check.
  - **Interop.** Management.java has 21 more tests for these, 55 in all, passing against Artemis 2.57.0 and Saranga. `tests/management.rs` checks the security notifications. [tests/interop/rbac/Rbac.java](tests/interop/rbac/Rbac.java) checks `management-message-rbac` with three users: `CONFIG=tests/interop/rbac/broker.xml ETC=tests/interop/rbac SUITE_FILE=tests/interop/rbac/Rbac.java scripts/interop.sh saranga`.
- Stage 3 (the CLI) is done:
  - **The Artemis CLI against Saranga.** `queue create|delete|update|stat|purge`, `address create|delete|update|show`, `user add|list|reset|rm`, `producer`, `consumer`, `browser`, `transfer` and `check node|queue` give the output they give against Artemis. What they needed: `listNetworkTopology` (`[]` until clustering), the notification address added at activation, and `listUser` in Java's `HashMap`/`HashSet` order. `artemis stop` stops a Saranga broker through `STOP_ME`.
  - **`saranga` has the same commands**, with the same options and output, over Core with `saranga-client`: management requests as `ManagementHelper` sends them, and what the Core JMS client does for the CLI's JMS calls (destination checks and auto-creation, temporary and durable subscriptions, shared subscriptions for `transfer`, browsers, and the producer's headers, including text, bytes and object messages). `saranga stop` writes `STOP_ME` for the instance's broker.
  - **`saranga create`** makes the instance `artemis create` makes, from the same templates and with the same options (clustering, HA and JDBC aside), including the journal auto-tuning. Its `bin/artemis` and `bin/saranga` run `saranga` for the instance, so `bin/artemis run`, `bin/artemis queue stat` and `bin/artemis-service start|stop` work as on an Artemis instance; `saranga run` takes the instance from `ARTEMIS_INSTANCE` and stops cleanly on SIGTERM.
  - **Interop.** [tests/interop/cli/cli.sh](tests/interop/cli/cli.sh) runs 67 commands and compares their normalized output with [expected.txt](tests/interop/cli/expected.txt), made with the Artemis CLI against Artemis 2.57.0; it passes with either CLI against either broker (`CLI=artemis|saranga SUITE_CMD=tests/interop/cli/cli.sh scripts/interop.sh saranga|artemis`). [tests/interop/cli/create.sh](tests/interop/cli/create.sh) compares `saranga create` with `artemis create` for four sets of options, runs a Saranga instance through its `bin/artemis` and stops it with `artemis stop`, and runs an Artemis instance on `saranga run` and stops it with `saranga stop`.
- Stage 4 (the REST API and Prometheus metrics) is done. The web server reads `bootstrap.xml`'s `<web>`: its bindings, with TLS from their key and trust stores, and its apps. On each binding:
  - **The REST API** at `/api`, Saranga's documented management interface. It is management messages over HTTP: `GET /api/broker/attributes/{name}` and `GET /api/{addresses|queues|diverts}/{name}/attributes/{attribute}` read attributes; `POST .../operations/{operation}` invokes an operation with a JSON array of parameters; `GET /api/queues/{name}/messages` browses a queue, filtered and paged, as `QueueControl.browse` shows messages. With broker security on, a user given by HTTP Basic needs the `manage` permission on the management address and the `management-message-rbac` permissions. Errors are 401, 403, 404 and 400 with `{"error": ...}`.
  - **Prometheus metrics** where the metrics plugin's app is declared, with the names, labels and help texts the Artemis plugin exports for the broker, addresses and queues, as Micrometer writes them.
  - **The Jolokia compatibility layer** where the console's app is declared, at `/console/jolokia`: Jolokia 2.4.3's `read`, `write`, `exec`, `search`, `list` and `version`, over GET and POST, with bulk requests, patterns, paths and Jolokia's errors. The MBeans and their metadata come from Artemis's `*Control` interfaces (`scripts/gen-mbean-catalog.py`). Logins need a console role (`HAWTIO_ROLES`), and `management.xml`'s `<authorisation>` is applied as `ArtemisMBeanServerGuard` applies it.
  - **Interop.** [tests/interop/web/web.sh](tests/interop/web/web.sh) runs a `saranga create` instance and checks the REST API, browsing, the metrics and the Jolokia layer, comparing the Jolokia layer's answers with Artemis 2.57.0's console: version, search and list, the errors found before an MBean is called, logins, and MBean reads, writes, invocations (their errors' Java exception classes included), pattern reads and bulk requests.
- Stage 5 (plugins, the critical analyzer, logging and the audit log) is done:
  - **Logging** as Artemis logs, configured by the instance's `etc/log4j2.properties`: its pattern, appenders (console and the rolling `log/artemis.log`, gzip included), logger levels and `monitorInterval` rereading. Each message is logged under the logger Artemis logs it under, chosen from its AMQ code, and the startup and shutdown messages runbooks look for are there (AMQ221000, AMQ221001, AMQ221002 with the uptime, AMQ221007, AMQ221035 and others).
  - **The audit log.** Artemis's three audit loggers (`base`, `resource`, `message`), off by default as in Artemis. Each management method logs what Artemis's control logs, from a catalog generated from the Java source (`scripts/gen-audit-catalog.py`), whether the call comes as a management message, over the REST API or through Jolokia; logins and their failures, connections, sessions, consumers, queues, addresses, and message sends, deliveries, acknowledgements and transactions are logged with Artemis's codes, texts and callers. [tests/interop/audit/audit.sh](tests/interop/audit/audit.sh) compares a CLI session's audit log with Artemis 2.57.0's.
  - **The critical analyzer** watches the journal's writer, each queue and each session, and acts on `critical-analyzer-policy`: LOG, SHUTDOWN (`saranga run` stops) or HALT (exit 70). `tests/critical.rs` checks a stuck queue.
  - **Plugins.** `BrokerPlugin` is `ActiveMQServerPlugin` as a Rust trait: connections, sessions and their metadata, consumers, addresses, queues and their bindings, sends, routing, `canAccept`, deliveries, acknowledgements, expiry and critical failures, with `before_*` hooks that can refuse. `<broker-plugins>` selects the ports of Artemis's own plugins (logging, notifications, message authorization, connection expiry); another class stops the broker from starting. `tests/plugins.rs` checks the hooks of a message's life, refusals, `can_accept`, the configuration and the notification plugin; [tests/interop/plugin/plugin.sh](tests/interop/plugin/plugin.sh) compares the logging plugin's lines for a CLI session with Artemis 2.57.0's.
  - **The reference image** is built with JDK 25 (`scripts/build-artemis.sh`). Built with JDK 21, Artemis's jars lack their Java 24 classes, and on JDK 25 its console and audit log fail (the withdrawn [issue 8](docs/upstream/artemis-issues.md)); built with JDK 25, the web suite compares MBean calls with Artemis's.
- Stage 6 (the exit check) is in progress. The rest of the CLI comes first, so that runbooks find each command they call:
  - **The data tools.** `data print`, `exp`, `imp`, `compact`, `encode`, `decode` and `recover`, with Artemis's options and output, and its CLI lock. `tests/interop/data` compares them with Artemis's, including how they report failures (`errors.sh`), and runs `imp` round trips both ways.
  - **Instance commands.** `version`, `pwd`, `kill` (honoured only by `run --allow-kill`), `upgrade` and `completion`. `saranga upgrade` moves an Artemis instance to Saranga, and `artemis upgrade` moves it back, keeping its configuration and data.
  - **The shell.** `saranga shell` (or `saranga` alone) runs the commands typed at a prompt, with `connect` and `disconnect` keeping a connection for the commands that follow, and the instance's history file. The CLI suite runs a piped shell session and compares it with Artemis's.
  - **`perf client`, `producer` and `consumer`, and `perf-journal`.** They print the same reports, `--json` file and HdrHistogram `--hdr` log; [tests/interop/perf/perf.sh](tests/interop/perf/perf.sh) compares them with Artemis 2.57.0's.
  - **Journal retention**, which the manual's data-retention procedure needs. `<journal-retention-directory>` keeps a copy of each message-journal file, removes copies by age and storage limit, and records paged messages and large-message bodies as journal events. The broker's `replay` operations route the retained messages again. Artemis's `data print` reads Saranga's copies; `tests/retention.rs` covers `replay`.
  - **The Core client** sends and receives large messages (compressed or not), as Artemis's does. The broker routes a message carrying `_AMQ_ROUTE_TO` to the queues it names, which `data imp` relies on.
  - **Full disks.** `max-disk-usage`, `min-disk-free`, `disk-scan-period` and the `disk-full-policy` address setting: past the limit, producers are blocked (the client warns every 10 s, as Artemis's does), refused or their messages dropped. `tests/disk.rs` covers the policies, and the client's `tests/disk_full.rs` that sends to the management address still go through.
  - **The runbook suite.** [tests/interop/runbook/runbook.sh](tests/interop/runbook/runbook.sh) follows the manual's procedures with the instance's own `bin/artemis` and the manual's `curl` calls: running and stopping a broker, the CLI's queue, producer, consumer, browser and user commands, `check node` and `check queue` (passing, and failing with status 1), the shell (connected with `connect` and with its options, its lines piped to it), masking, Jolokia reads, `listConnections` with a consumer connected, an acceptor's `reload` with the upgrade notes' `curl` command (which Jolokia refuses without an `Origin`, on both brokers), the audit log turned on in `log4j2.properties` while running, data retention and `replay`, configuration reload, `perf client` (and its target rate test with an HDR log), `data print`, `recover`, `exp`, `compact`, `encode`, `decode` (a round trip that keeps the records) and `imp`, recovery on restart, a graceful shutdown (`graceful-shutdown-enabled`, with and without `graceful-shutdown-timeout`) while a consumer is connected, and the network check of an instance made with `create --ping`, which stops the broker while the network is down and starts it again once it is back, and the upgrade chapter's tool (`artemis upgrade` from the home, `saranga upgrade` for Saranga) run on a stopped instance, which keeps `broker.xml` and the messages and backs up the scripts and profiles it changes. Its output is recorded against Artemis 2.57.0 and compared for a Saranga instance and for an Artemis instance moved to Saranga with `saranga upgrade`, on Windows and on Linux.
  - **Interop.** [tests/interop/cli/create.sh](tests/interop/cli/create.sh) also upgrades an Artemis instance both ways and runs it. The CLI, perf and cross-store suites pass with either CLI against either broker.

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
