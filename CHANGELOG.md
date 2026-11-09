# Changelog

Each release of Saranga, newest first. Saranga is inspired by Apache ActiveMQ Artemis 2.57.0 and works interchangeably with it. [COMPAT.md](COMPAT.md) lists where the two differ.

## Unreleased

### Storage

- **The relational store**: with `database-storage`, a broker keeps its messages, paging and large bodies in PostgreSQL, in the tables of Artemis pull request 6838, so that either broker opens what the other wrote. `saranga create --database-storage` makes such an instance. See docs/guide/storage.md.
- **TLS to the database**, with the JDBC driver's `sslmode` and its certificate parameters.
- **Shared-store HA** on the store: the backup opens the tables when it takes over. Replication is refused with a reason, as there are no journals to copy.
- **`data print` and `data exp`** read the tables, and `data exp` writes what Artemis's exporter writes.

### Fixes

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
