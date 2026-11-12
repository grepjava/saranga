# Changelog

Each release of Saranga, newest first. Saranga is inspired by Apache ActiveMQ Artemis 2.57.0 and works interchangeably with it. [COMPAT.md](COMPAT.md) lists where the two differ.

## Unreleased

### Storage

- **The relational store**: with `database-storage`, a broker keeps its messages, paging and large bodies in PostgreSQL, in the tables of Artemis pull request 6838, so that either broker opens what the other wrote. `saranga create --database-storage` makes such an instance. See docs/guide/storage.md.
- **TLS to the database**, with the JDBC driver's `sslmode` and its certificate parameters.
- **Shared-store HA** on the store: the backup opens the tables when it takes over. Replication is refused with a reason, as there are no journals to copy.
- **`data print` and `data exp`** read the tables, and `data exp` writes what Artemis's exporter writes.

### Fixes

- **A selector with a very long operator chain is rejected, not a crash.** A selector such as `x + x + … + x` with thousands of terms parsed (the parser's nesting limit bounds only parentheses and unary operators) and then overflowed the stack when evaluated, aborting the broker. The parser now bounds the depth of the expression tree it builds; `OR` and `AND` lists, which stay flat, are unaffected.
- **Committing or rolling back a prepared transaction through management waits for a `sync` mirror.** `commitPreparedTransaction` and `rollbackPreparedTransaction` reported success before the mirror peer had acknowledged what the outcome sent and acknowledged; they now wait for it, as a client's commit does and as Artemis does.
- **The console's browse, export and import drop a failed link to another node.** When the connection to a peer node failed during one of these requests, the console kept the broken link cached (and reported a 400), so later requests reused it indefinitely; it is now evicted and remade on the next request, as for operations and attributes, and the failure is reported as the node being unreachable.
- **Changing a queued message's priority reaches the paged messages too.** `changeMessagesPriority` now changes the priority of matching messages still in pages — those held in the page cache — not only the ones in memory, the way Artemis's queue iteration reaches paged messages. As in Artemis, the new priority is not written to the page file, so a paged message is read back at its original priority after a restart or once its page leaves the cache.
- **A copy of a large message whose body cannot be written fails instead of succeeding silently.** When moving, copying, diverting, redistributing or mirroring a large message to a new ID, if its body cannot be copied to the new ID's file the operation is now refused — the original stays where it is — rather than handing back a message that shares the original's body under an ID with no body of its own, which could never be recovered after a restart.
- **A reject-duplicates move journals the duplicate ID.** Moving a message with `rejectDuplicates` now records the destination's duplicate ID in the same transaction as the move, so with `persist-id-cache` it still blocks a resend of that ID after a restart, the way an ordinary send's duplicate ID does, rather than being kept only in memory.
- **A paged large message's body outlives the first queue to acknowledge it (relational store).** Under database paging, a large message read into two queues from pages no longer loses its body file when the first delivery is acknowledged: the file now goes with the message's row, deleted once no reference is left on it, instead of being deleted by whichever reader dropped its copy first.
- **A deeply nested selector is rejected, not a crash.** A consumer filter (or any selector) that nests parentheses or unary operators beyond a fixed depth is refused with an error, rather than driving the recursive-descent parser past the stack and aborting the broker. A real selector nests a handful of levels, far below the limit.
- **Concurrent large-message sends on one session stay whole.** Two producers that share a session and each send a large message no longer interleave their headers and chunks across the credit waits between them — which could stall the sends or let one message's chunks land in another's — because the whole large send is now serialized per session.
- **Exporting a paging queue returns all of its messages.** The export reads the messages still in pages as well as those in memory, so a paging queue's export is complete instead of silently dropping the paged messages.
- **A large AMQP message keeps only its headers in memory.** Its body is streamed to its file as the message arrives — the whole delivery is never buffered in memory, so a single multi-gigabyte message no longer takes gigabytes of heap — and stays in the file, read back whole only when the message is delivered, rather than being held in memory for its whole life. A restart no longer reads every large body back in either, and a large AMQP message no longer double-counts its body in the memory it charges its address. This covers client sends and messages taken in over a mirror, a bridge or a federation link.
- **Changing a queued message's priority no longer leaks its address memory or journal records, or loses a multicast message.** `changeMessagesPriority` rebuilds the immutable message with its new priority; the rebuilt message shares the reference's paging-memory charge and journal accounting, so deleting the message frees its memory and deletes its journal record, and — for a message fanned out to several queues — changing its priority in one queue no longer deletes the journal record another queue still holds (which lost that queue's copy on restart).
- **A poison-acknowledged message no longer leaks its memory or journal record.** When a client's poison acknowledgement adds the failure's cause to a message before it is dead-lettered, the rebuilt message carrying the cause now keeps the reference's accounting, so dead-lettering or dropping it frees the address memory and deletes its journal record instead of leaving them charged.
- **An address containing an apostrophe no longer breaks its dead-letter or expiry resource queue.** The address is escaped before it goes into the resource queue's selector, so an address such as `orders'west` produces a valid filter and its auto-created dead-letter and expiry resource queues are built, rather than failing to parse and dropping the dead-lettered or expired message.
- **A move to a paging address pages the copy in the acknowledgement's transaction.** Moving, redistributing, expiring or dead-lettering a message to an address that is paging now writes the copy to the destination's pages inside the same transaction that acknowledges the source, so a crash can no longer leave the copy paged while the source stays unacknowledged (a duplicate) or drop the source before the copy is durable (a loss).
- **A configured legacy `<queues>` queue survives a reload.** Under `config-delete-queues=FORCE`, a queue declared in the deprecated top-level `<queues>` is kept when the configuration is reloaded, instead of being deleted and recreated with a new identity and no messages; the retained set now counts the legacy queues as configured, not only those under `<addresses>`.
- **Concurrent user-management edits no longer overwrite each other.** File-backed `addUser`, `resetUser` and `removeUser` serialize their read-modify-write of the users and roles files, so simultaneous changes all persist rather than all but one being lost to a last-writer-wins race.
- **Cluster routing reaches an unfiltered consumer on a mixed queue.** A remote queue that has both a filtered and an unfiltered consumer is now eligible for a message the filter rejects — the unfiltered consumer would take it — where before such a message could be stranded in on-demand routing and redistribution.
- **Active-node detection checks every cluster connector.** When a replication primary restarts with `check-for-active-server`, it now keeps asking the configured connectors after a peer that does not know the node, instead of stopping at the first reachable one — so it cannot miss a promoted backup reachable through a later connector and activate alongside it.
- **A malformed AMQP disposition closes its connection cleanly.** A delivery-disposition range whose first number is greater than its last is rejected as a protocol error instead of panicking the connection task, which left the session's queue consumer registered after the socket was gone. (The AMQP CLI client guards the same range.)
- **Quorum votes keep the peer's TLS settings.** A backup voting on whether its failed primary is down now dials each peer with its advertised TLS configuration rather than a plaintext connector rebuilt from host and port, so a cluster whose quorum needs votes from TLS-only peers can still fail over.
- **A retained `$` topic is not replayed to a wildcard subscription.** A subscription whose first level is a wildcard (`#`, `+/...`) does not see `$` topics ([MQTT-4.7.2-1]); the leading-`$` exclusion now applies to retained-message replay too, which inserts directly into the queue and bypassed the routing filter, so retained and live delivery agree.
- **Journal history recovery deletes only the recovered file's own copies.** A backup file left by a broker that stopped mid-copy is matched to its history copies by the whole file ID, so recovering file 1 no longer deletes the retained history of files 11, 21, and every other ID ending in 1.
- **XA management refuses an ambiguous prepared transaction.** A base64 Xid id (branch qualifier, global ID and format ID with no lengths) can be shared by distinct Xids; when more than one prepared branch shares the id given to commit or roll back, none is completed, rather than silently completing the wrong one.
- **A routing-cache entry refreshed as it expires keeps its persisted record.** The in-memory and journal views of a key are updated under one lock order, so a `put` that renews a key no longer races its expiry and leaves the entry live in memory with its journal record deleted (lost on restart).
- **A rejected STOMP acknowledgement can be retried.** The subscription is validated before the pending acknowledgement is consumed, so an ACK that names the wrong subscription (which leaves a STOMP 1.1+ connection open) no longer discards the entry, and a corrected retry acknowledges the message instead of failing as missing.
- **A cached login holds only for the client certificate it was made with**, not for another certificate with the same subject.
- **`OIDCLoginModule` reads RFC 8705's `cnf.x5t#S256`**: a token bound to a certificate that way logs in only with that certificate.
- **One retained message per MQTT topic.** Clients publishing retained messages to one topic at the same time could leave it several.
- **STOMP frames with `content-length`** wait for their NUL. Before, how a frame was read depended on how its bytes arrived.
- **The relational store** writes a queue or address changed twice at once. PostgreSQL refused the batch, and the store failed after its retries.
- **A message sent again after a refusal is delivered.** A send outside a transaction that an address refused (full under `FAIL`, say) kept its duplicate ID, so sending it again was dropped as a duplicate.
- **Duplicate detection holds in three more cases.** One transaction sending the same duplicate ID twice is now rolled back, where both messages were delivered. A refused send no longer pushes the oldest ID out of a full cache, so that ID is still a duplicate. And an ID held twice in the cache stays a duplicate until its newer copy leaves it, where it was forgotten when the older one did.
- **A move or copy whose journal write fails** reports the error and leaves the message where it was. Before, the copy was seen and the original acknowledged, and management reported success.
- **JSON and OpenWire nested too deeply** are refused, where they overflowed the stack and ended the broker: an OIDC token, or an OpenWire frame, before its connection logs in.
- **A duplicate ID is a duplicate from the moment its transaction commits**, not only once the commit's journal record is written. A pipelined retry in that window was delivered a second time.
- **An OpenWire body allocates what it holds, not what it declares.** A stream byte array longer than the rest of the frame is refused rather than padded out to its declared length, and a compressed body is inflated only up to 100 MiB and no further than the length it declares, so a small frame can no longer make the broker allocate gigabytes.
- **A cached permission holds only for the credentials it was granted to**, not for every login under the same user name.
- **An XA commit of the wrong kind** leaves the branch to be committed or rolled back properly. Before, it was lost until a restart.
- **`handshake-timeout` covers a WebSocket upgrade**: the HTTP request and the protocol header inside the WebSocket.
- **MQTT wills and expired sessions.** An MQTT 5 will without a delay is published as soon as its client fails, not when its session expires. A session resumed without a will no longer publishes the will of the connection before. A session restored at start that expires before its client returns stays gone after the next restart.
- **A STOMP frame has a size limit**: 64 KiB before the connection logs in, 100 MiB after. A larger one gets an ERROR and the connection closes.
- **`jdbc:postgresql:` URLs are read as the JDBC driver reads them**: parameter values and the database name are URL-decoded (`%26` is `&`, `+` a space), and an IPv6 host in brackets works with a port or without one.
- **MQTT No Local** takes a client ID with a quote, and a No Local subscriber gets messages that came without a client ID (from STOMP or the Core API).
- **MQTT shared subscriptions on a configured queue** keep their backlog when a member leaves; the queue is emptied only when no member or other consumer is left on it.
- **MQTT 5 Message Expiry and Will Delay Intervals** are read as unsigned counts of seconds: a message expiry longer than about 24.8 days no longer expires the message at once, and a will delay over 2^31 seconds no longer sends the will at once.
- **A disabled queue no longer takes an anycast address's routing turns.** A message to an anycast address goes to a queue that can take it, instead of being lost to a disabled queue's turn in the round robin. The same holds for a queue that purges when it has no consumers.
- **An MQTT 5 Maximum Packet Size** is read as an unsigned four-byte value, so a client that advertises a large one (over 2^31) still receives its messages.
- **An MQTT 5 property block of 128 bytes or more keeps the payload whole.** The property length is the count of the property bytes alone, so the last property is no longer read as the start of the payload.
- **A reused session-local consumer ID is refused** instead of leaving the first consumer registered on its queue with nothing to reach it.
- **An MQTT 5 DISCONNECT with reason 0x04** (Disconnect with Will Message) publishes the will, while a normal DISCONNECT still holds it back.
- **A large MQTT 5 Session Expiry Interval** (one with the high bit set) is read as an unsigned count of seconds rather than a negative that never expires; `0xFFFFFFFF` still means never.
- **A publication discarded for a client's MQTT 5 Maximum Packet Size takes back its topic alias**, so the next message to that topic carries the full topic name rather than an alias whose defining packet the client never received.
- **An MQTT 5 acknowledgement wakes a message waiting on any of the connection's subscriptions.** A QoS 1 or 2 message held back by Receive Maximum on one subscription is delivered as soon as an acknowledgement on another frees a slot.
- **An MQTT client taken over by a new connection with the same client ID has its will published**, as any other abnormal close does.
- **An MQTT 5 wildcard subscription with Retain As Published keeps the RETAIN flag** on the messages forwarded to it live, instead of the publication being dropped.
- **Receive Maximum bounds QoS 1 and 2 messages whatever their persistence.** A non-durable message — from a non-persistent Core or STOMP producer — delivered to an MQTT subscriber at QoS 1 now counts against the client's Receive Maximum instead of slipping past it.
- **An MQTT 5 delivery larger than the client's Maximum Packet Size is discarded.** The size check counts every property length prefix, and a packet ID only when the QoS carries one, so a packet whose true size exceeds the limit is no longer sent for the client to reject.
- **A queue's `max-consumers` holds under a race.** Two sessions adding their last consumer at the same moment can no longer both be admitted: the limit is checked and the slot reserved as one atomic step.
- **A large MQTT 5 Message Expiry Interval is forwarded whole.** The remaining seconds keep their unsigned value, rather than being cut to about 24.8 days when the interval is over 2^31.
- **An MQTT 5 will keeps its publication properties.** A published will carries its Message Expiry Interval (started when the will is published), Payload Format Indicator, Content Type, Response Topic and Correlation Data, not only its user properties — so an expiring will no longer stays queued or retained indefinitely.
- **`xaStart` no longer risks a deadlock with the transaction reaper.** Replacing an open, unprepared branch releases the transaction before it touches the resource-manager table, the order the reaper and recovery already take the two locks.
- **MQTT Receive Maximum is reserved as one atomic step.** A publication fanning out to several of a connection's subscription queues at once can no longer have two of them both pass the check and send past the limit.
- **A QoS 2 message holds its Receive Maximum slot until PUBCOMP.** The slot is freed when the exchange completes, not at PUBREC, so a slow receiver cannot accumulate more incomplete QoS 2 exchanges than its Receive Maximum (an error PUBREC still frees it early).
- **A resumed QoS 2 exchange grants no extra Receive Maximum credit.** Completing an exchange that outlived its connection — a PUBCOMP for a PUBREL retransmitted on a resumed session — releases a slot only when the current connection reserved one, so the quota can no longer be driven below zero into letting an extra PUBLISH past Receive Maximum.
- **An acknowledgement after an unsubscribe still frees its slot.** A PUBACK or PUBREC whose subscription's consumer has gone (unsubscribed or closed) now completes packet-level settlement — freeing the Receive Maximum slot and waking the connection's other subscriptions — instead of leaving the slot held until the client reconnects.
- **A null Core text body is delivered as an empty MQTT payload.** A TEXT message with no body (a JMS text message without text, or a management import without a `text` member) is sent on as an empty payload rather than failing the publish and stranding its reserved Receive Maximum slot.
- **Overlapping MQTT subscriptions keep their packet IDs across a reconnect.** One message delivered to several of a client's overlapping subscriptions now records a separate correlation per delivery, so a redelivery reuses each exchange's own packet ID instead of keeping only the last and generating fresh IDs for the rest, which broke client duplicate handling.
- **A Session Expiry Interval lowered (or raised) on DISCONNECT is persisted.** An MQTT 5 DISCONNECT that changes the session expiry now writes the revised interval, so after a restart the session expires on the interval the client last asked for, not the one from CONNECT.
- **A persistent MQTT session survives unsubscribing from its last topic.** Removing the final subscription keeps the durable session record (with an empty subscription set) instead of deleting it, so a resumed or restarted session is still found; only a clean session or an actual expiry ends it.
- **An MQTT message whose topic cannot be represented is discarded, not left to stall the connection.** When a Core address reaches a subscriber as a topic that has an MQTT wildcard character, the message is discarded (its Receive Maximum slot freed and its reference settled) the way an oversized one is, rather than failing the delivery and stranding the slot so later messages cannot flow.
- **An MQTT topic or property too long for its two-byte wire length is rejected, not truncated.** A converted topic or string property over 65535 bytes is discarded rather than encoded with a wrapped length that would corrupt the packet.
- **A wildcard address keeps a literal delimiter in its word.** An escaped delimiter (`a\.b`, as an MQTT topic level containing a `.` becomes) is one word, not split in two, so a subscription like `+/c` still matches `a.b/c`.
- **Wildcard matching backtracks.** An any-words wildcard whose first candidate word leaves the rest of the pattern unmatched tries the next, and a wildcard following an any-words wildcard is matched as a wildcard rather than literal text, so patterns such as `a.#.b.c` and `a.#.*.c` match the addresses they should instead of silently missing the subscriber.
- **An expiring MQTT session is not removed out from under a reconnection.** A client that reconnects while the session scan is expiring its old session keeps its session and queues: the scan re-checks and removes atomically, rather than deleting a session the client has just re-attached to.
- **MQTT topic names convert to distinct core addresses.** A literal backslash in a topic is kept as a literal (not read as an escape), so distinct topics such as `a\/b` and `a\\/b` map to distinct addresses that round-trip, instead of colliding so a publication reaches a subscriber of a different topic.
- **A literal backslash in an MQTT topic is matched as a literal.** A topic level containing a `\` is split and matched like any other (an MQTT topic has no escape), so a wildcard subscription still matches it and the delivered message keeps its subscription identifier.
- **An MQTT subscription whose core queue name collides with another client's is refused, not mis-delivered.** The queue name is `<client>.<address>`, and either half can contain the delimiter, so distinct client-and-topic pairs can name one queue (client `a.b` subscribing to `c` and client `a` to `b/c` both name `a.b.c`). When the name already belongs to a different address the subscription is refused (a SUBACK failure) instead of reusing the queue and serving one client the other's messages.

## 1.2.0 (2026-11-02)

Faster under load, connection routers, and the rest of Artemis's login modules and SASL mechanisms.

### Performance

- **Connections run on event loops**, as Netty runs Artemis's: each connection an acceptor takes runs on one of `remotingThreads` (or `nioRemotingThreads`) single-threaded loops, with what it creates. The default is one a processor, where Artemis has three (see COMPAT.md). A delivery no longer moves between threads at each step, and p50 latency is now lower than Artemis's in every benchmark scenario, OpenWire, STOMP and MQTT without persistence included.
- **Cheaper hot paths**: sends and acknowledgements with no journal write, Core responses written straight to the socket, routing to a lone queue, paging checks, the journal's maps, and AMQP transfers sent without copying. A non-persistent message no longer overtakes a persistent one sent before it on the same session, as in Artemis.
- **Benchmarks** rerun, with charts in the README and docs/benchmarks.md, and a second run with clients written in Rust (`CLIENT=rust scripts/bench.sh`), which showed that the JVM clients, not the broker, set Saranga's Core and AMQP figures.

### Connection routers

- **An acceptor's `router`** routes each connection, once authenticated, by client ID, user, role, source address or TLS SNI host, with Artemis's five policies, pools of static connectors, discovery groups or cluster connections, a cache that can be persisted, and the redirects each protocol's clients follow (Core, AMQP, MQTT 5 and OpenWire). `ConnectionRouterControl` answers over management messages and Jolokia. `saranga-client` follows a Core redirect as the Java client does.

### Security

- **Login modules**: `OIDCLoginModule`, `KubernetesLoginModule`, `PrincipalConversionLoginModule` and the JDK's `Krb5LoginModule` with Artemis's own `Krb5LoginModule`, as Artemis 2.57.0 has them. The system's Kerberos library (MIT Kerberos, or SSPI on Windows) is loaded when first used.
- **`LDAPLoginModule`** binds with GSSAPI (`authentication=GSSAPI`), over `ldap://` and `ldaps://`.
- **AMQP SASL**: `GSSAPI`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` and `XOAUTH2`.

### Protocols

- **AMQP**: `maxTransfersPerDelivery`, and the broker's Open gives `failover-server-list` when this node has a backup.
- **OpenWire**: the slow-consumer advisory, `updateClusterClients` and `rebalanceClusterClients`, and a `ConsumerControl` with prefetch 0 for a queue whose `queuePrefetch` is 0 (now taken from broker properties).
- **WebSockets**: `webSocketCompressionSupported` (permessage-deflate and deflate-frame).
- **Interceptors**: `<remoting-incoming-interceptors>` and `<remoting-outgoing-interceptors>` name interceptors an embedding application registers.

### Packaging

- **A FreeBSD image** for podman with ocijail: `docker/Dockerfile.freebsd`.

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
