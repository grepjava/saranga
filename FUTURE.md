# Saranga after 1.0

Work planned for after 1.0, once Phase 11 of [PHASES.md](PHASES.md) is done. Nothing here is started, and nothing here changes what Saranga promises about Artemis compatibility.

---

## IBM MQ client support

**Goal**

Applications written for IBM MQ connect to Saranga as they would to a queue manager. That covers the MQI over a client connection, as used by the C client, MQ classes for Java and JMS, .NET and XMS, and pymqi, Go and Node, which wrap the C client. They run unchanged apart from where they connect. Their messages flow to and from Core, AMQP, STOMP, MQTT and OpenWire.

As in every phase, one test suite runs against a real IBM MQ and against Saranga, and the results must match. The reference is the MQ Advanced for Developers container, pinned to one LTS release.

"IBM MQ" is named only to describe compatibility. Saranga is not affiliated with IBM, and the documentation must never imply it is.

**Out of scope**
- **Bindings-mode applications**, which link `libmqm` and talk to the queue manager over shared memory. They must switch to client mode (`MQSERVER` or a CCDT), and the migration guide says so.
- **Channel exits.** They are user C code loaded into the queue manager.
- z/OS, shared queues, MFT and the mqweb console.
- **Queue-manager-to-queue-manager channels and MQ clusters.** These are an optional late stage, not part of the first release.

**Decisions**
- **Storage.** MQ messages are kept as Core messages: the MQMD goes in reserved properties, along with its raw bytes so it round-trips exactly. A separate MQ message persister would leave the data directory unreadable by Artemis, which breaks Phase 3's on-disk compatibility.
- **Configuration.** Saranga accepts MQSC scripts, as the MQ container reads `/etc/mqm/*.mqsc`, and translates them into addresses, queues and address settings.
- **Object model.**
  - A local queue is an ANYCAST queue.
  - An alias queue is a divert or an alias address.
  - Model and dynamic queues are temporary or durable queues.
  - A topic object or topic string is a MULTICAST address, with `/`, `#` and `+` mapped through the wildcard configuration.
  - `DEADQ`, `BOQNAME` and `BOTHRESH` map to dead-letter addresses and max delivery attempts.

**Stages**, each committed on its own:
0. **Gate.**
   - The FAP (MQ's wire protocol) has no public specification. The only sources are traffic captured between a real client and a real queue manager, and Wireshark's MQ dissector. The dissector is GPL: it may be read to understand the protocol, but none of its code goes into Saranga.
   - IBM's license terms on reverse engineering and on publishing benchmarks need legal review before work starts.
   - Then: pin the MQ version, run the container in WSL, and write a capture script for golden FAP traces, as `scripts/golden.sh` does for Artemis.
1. **Codec**, in a new crate `saranga-protocol-ibmmq`.
   - The TSH segment framing.
   - The structures: MQMD v1 and v2, MQOD, MQPMO, MQGMO, MQCNO, MQCD, MQCSP, MQSD, MQSTS, and the async-consume structures.
   - Both integer encodings and the FAP levels.
   - Golden round-trip tests and a target in `saranga-fuzz`.
2. **Connections.**
   - `is_protocol` recognises the `TSH` header, and an acceptor listens on 1414.
   - Initial-data negotiation: the FAP level, the maximum message length, the heartbeat interval, and SHARECNV conversations on one socket.
   - SVRCONN channel definitions, TLS with CipherSpecs, and CONNAUTH user and password through Saranga's security.
   - A subset of CHLAUTH.
   - MQCONN, MQCONNX and MQDISC, with MQ's exact reason codes.
3. **Objects.**
   - MQOPEN and MQCLOSE, including dynamic queues opened from model queues.
   - MQINQ and MQSET.
   - Open options checked against Saranga's permissions: input as consume, output as send, browse as browse, inquire and set as manage.
4. **Point-to-point.**
   - MQPUT, MQPUT1 and MQGET: waits, matching on MsgId, CorrelId and GroupId, and browse cursors with locking.
   - Syncpoint with MQCMIT and MQBACK.
   - The backout count, and requeueing to the backout queue.
   - Expiry, priority, persistence and the context options.
   - MsgId, PutDate and PutTime filled in as MQ fills them.
   - Report messages (COA, COD, exception and expiry), and the dead-letter queue with MQDLH.
   - Message properties.
   - Async consume (MQCB and MQCTL) and read-ahead.
5. **Publish/subscribe.**
   - MQSUB: managed and unmanaged, durable and non-durable subscriptions, with selection strings.
   - Retained publications and MQSUBRQ.
   - Topics shared with MQTT, AMQP and Core subscribers.
6. **Conversion.**
   - MQ to and from Core, and so to and from every other protocol.
   - JMS over MQ: the RFH2 folders (`jms`, `mcd` and `usr`) and PROPCTL, so MQ JMS and Artemis JMS see the same JMS message.
   - `MQGMO_CONVERT`: MQSTR data and the built-in headers converted between CCSIDs, including EBCDIC 037 and 500 and UTF-8 (1208).
7. **Transactions and groups.**
   - XA through the client, against Saranga's journal.
   - Message groups and segmentation (`LOGICAL_ORDER`, `ALL_MSGS_AVAILABLE`).
8. **Administration.**
   - PCF on `SYSTEM.ADMIN.COMMAND.QUEUE`, with MQSC carried in `MQCMD_ESCAPE`, so that `runmqsc -c` and MQ Explorer work.
   - The mqweb REST admin and messaging APIs.
   - MQ connections and queues shown through Jolokia and the console.
9. **HA.**
   - Client reconnect (`MQCNO_RECONNECT`) across Saranga's HA pairs, with a queue manager identity (QMID) that stays the same.
   - Optionally, sender and receiver channels to a real IBM MQ, to bridge the two during a migration.
10. **Exit check.**
    - IBM's samples (`amqsput`, `amqsget`, `amqsbcg` and `amqssub`), plus MQ JMS, pymqi and Go suites, give the same results against both brokers.
    - Messages cross between MQ and the other protocols.
    - Chaos and fuzz runs pass.
    - A benchmark is published only if the license allows it.

**Risks**
- **No specification.** All behaviour comes from captures, and an IBM fix pack can change it. So the version stays pinned and the capture corpus can be re-run.
- **Semantics wider than Artemis's.** Browse locking, segmentation, report messages and triggering may need new hooks in `saranga-server`. Triggering may be deferred.
- **Size.** About Phases 7 and 8 together. The smallest useful release is Stages 0 to 4 plus 6.

---

## A Saranga console

A console of Saranga's own, in Svelte, comes after 1.0. Until then Saranga serves Artemis's console (Phase 11, Stage 6).
