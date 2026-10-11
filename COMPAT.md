# Compatibility target

| Item | Value |
|---|---|
| Reference implementation | Apache ActiveMQ Artemis |
| Pinned version | **2.57.0** (tag `2.57.0`, checked out in `reference/activemq-artemis`, git-ignored) |

Saranga aims for wire compatibility (Core, AMQP 1.0, MQTT 3.1.1/5, STOMP, OpenWire, WebSockets)
and on-disk compatibility (journal, bindings, paging, large messages) with the pinned version.
See [docs/design.md](docs/design.md) for the strategy.

## Why we build the reference from source
Artemis 2.45.0 to 2.57.0 are tagged in git, but no distribution, server jars or Docker images
were published for them. Maven Central has POMs only, and the newest published release is 2.44.0.
`scripts/build-artemis.sh` therefore builds 2.57.0 from the tag and packages it with Artemis's own
`artemis-docker` Dockerfile as `saranga/artemis-ref:2.57.0`. Clone the reference with
`core.autocrlf=false` on Windows, because CRLF line endings break the container entrypoint.

## Artemis wire quirks Saranga reproduces on purpose
Saranga matches what Artemis actually puts on the wire, including the oddities below. Do not "fix" them, or interop breaks.

- **Two boolean encodings.** `ActiveMQBuffer.writeBoolean` writes true as `0xFF`. Packets and journal records use it. Netty's `ByteBuf.writeBoolean` writes `0x01`, and `TypedProperties` and `CoreMessage` headers use that one. Readers treat any non-zero byte as true. In Saranga these are `Writer::bool` and `Writer::byte_buf_bool`.
- **Two nullable encodings.** `writeNullableSimpleString` and similar write a `0x00`/`0x01` marker (`Option<T>`). `BufferHelper.writeNullableX` and boolean presence flags write `0x00`/`0xFF` (`Flagged<T>`).
- **Property order.** `TypedProperties` iterates a Java `HashMap`. Saranga keeps decode order, so re-encoding is exact.
- **Optional trailing fields.** Many `_V2` and later packets read their newest fields only "if bytes remain". Saranga models these as `Option<T>` written only when present. Some packets (`CreateQueueMessage_V2`, `SessionQueueQueryResponseMessage_V3`, `SessionBindingQueryResponseMessage_V4/V5`, `CreateSharedQueueMessage_V2`) use a `sections` count. Set it to the struct's `SECTIONS` constant when building a packet to send.
- **`FederationConnectionConfiguration` encode/decode mismatch.** Encode writes `retryInterval` into the slot that decode reads as `maxRetryInterval`. This is an Artemis bug. Saranga keeps the wire value as-is.
- **`ReplicationSyncFileMessage`.** The primary sends the whole 32 KiB read buffer even when fewer bytes were read. Only `dataSize` bytes are meaningful, and the rest is kept as `trailing`.
- **1.x message layout.** Clients older than version 129 put an extra end-of-message offset into sends and receives. Artemis ignores it on decode. Saranga keeps it so the message re-encodes exactly.

## Configuration differences
- **`${...}`** in `broker.xml`, `bootstrap.xml`, `login.config` and broker properties resolves from `saranga run -D name=value`, then from the properties `artemis run` defines (`artemis.instance`, `artemis.home`, `artemis.instance.etc`, `artemis.URI.instance`, `data.dir`, `java.security.auth.login.config`), then from environment variables. Saranga does not read `JAVA_ARGS` in `artemis.profile`, so pass its `-D` options to `saranga run`.
- **Broker properties** are read from `.properties` files only. JSON and YAML files are reported and skipped. The collections Saranga applies are `addressSettings`, `securityRoles`, `addressConfigurations`, `acceptorConfigurations`, `divertConfigurations` and `jaasConfigs`, plus the top-level settings it supports. Every other key is reported as a warning. Artemis reports such keys in its status, which Saranga does not have until management arrives in Phase 8.
- **Configuration reload** swaps the security settings and address settings, reconciles diverts, and deploys and undeploys addresses and queues, as Artemis does. Saranga does not reload acceptors, bridges, connection routers or `jaasConfigs`. It also does not reload the `login.config` file itself, which Artemis does not reload either.
- **Namespaces.** Elements are matched by local name, so prefixed elements that Java would miss are accepted.
- **`xi:include`** is not expanded yet; it produces a warning.
- **Features Saranga does not have yet.** An HA backup (`<ha-policy>` with a backup, slave or colocated role) and the JDBC store (`<database-store>`) stop the broker from starting with a reason. Starting anyway would pretend to be something it is not. The following elements are read but not acted on, each with a warning: a primary `<ha-policy>`, `<cluster-connections>`, broadcast and discovery groups, `<bridges>`, `<federations>`, `<broker-connections>`, `<connection-routers>`, `<grouping-handler>`, `<broker-plugins>`, metrics, `<resource-limit-settings>` and `<journal-retention-directory>`. The critical analyzer settings are accepted silently.
- **Acceptors** pick each connection's protocol from its first 8 bytes, as Artemis does, among the protocols the acceptor lists that Saranga has. Protocols Saranga does not have yet produce a warning, and an acceptor that lists only such protocols is not opened. A host binds to the address Java's `InetAddress.getByName` gives, which is IPv4 first, so `localhost` means 127.0.0.1 as it does for Artemis.

## Persistence differences
- Saranga writes journal files the way Artemis's `NIO` journal does: records follow each other without block alignment, whatever `journal-type` says. Artemis reads such files with any journal type, and Saranga reads `ASYNCIO` files (aligned flushes padded with zeros).
- Appends wait while the journal compacts; Artemis keeps appending and replays the appends afterwards. The compacted files are the same.
- Group commit comes from a flusher thread that syncs whatever accumulated while the previous sync ran, rather than `TimedBuffer`'s timer, so `journal-buffer-timeout` has no effect.
- `server.lock` holds the node ID and state as in Artemis; the primary lock is a lock on `serverlock.1`. The node ID of a new instance is a random UUID rather than a time-based one.
- When an update changes a queue's filter (configuration reload, or management from Phase 8), the queue binding record Saranga writes holds the new filter. Artemis 2.57.0 writes the old one, because `QueueImpl.setFilter` does not update the queue's `QueueConfiguration`, which is what gets journalled (drafted upstream in [docs/upstream/artemis-issues.md](docs/upstream/artemis-issues.md), issue 2). The record has the same layout, and either broker reads the other's. A queue whose filter was changed by a Java broker and not by broker.xml therefore comes back on Saranga with the old filter, as it would on Artemis. Queue queries, as on Artemis, keep reporting the old filter until restart.
- The XA error messages print `XidImpl (<n> ...` with a hash of the Xid where Java prints an identity hash.

## Paging differences
- **Paging needs persistence.** Without `persistence-enabled`, Artemis still writes page files (its null storage manager keeps no journal but the page store works); Saranga keeps every message in memory, and only BLOCK, FAIL and DROP limit an address.
- **`global-max-size`** defaults to 1 GiB. Artemis defaults to half the JVM's maximum heap, which Saranga does not have.
- **Messages sent in a transaction** count against the address size when the transaction commits. Artemis counts them as they are routed, before the commit. Under transactional producers a Saranga address therefore fills a little later.
- **Queue browsers** see the messages a queue holds in memory, including those read in from its pages up to `max-read-page-messages` and `max-read-page-bytes` (1,000 messages when neither is set), but not the rest of a longer backlog. Artemis browsers read through the page files as well (`QueueBrowserIterator`).
- **Page counters** are rebuilt from the page files at every start, and the counter records Artemis writes are deleted when Saranga loads them. Artemis rebuilds its counters the same way when the records are missing, so the data directory still moves back.

## Queue semantics differences
- **delay-before-dispatch** ends on a timer. Artemis has no timer: it starts dispatching at the next delivery attempt after the delay, such as a new message, a consumer's credits or a forced receive.
- **Last-value queues** drop a replaced value as soon as its replacement arrives. Artemis prunes on its next delivery pass. The queue holds the same messages afterwards.
- **Message groups.** While a group's consumer has no credit, only that group's messages wait. Artemis ends the whole delivery pass at that point.
- **Duplicate detection** checks the message a client sends. Copies a divert makes are not checked against the forwarding address's cache.
- **A prepared XA branch's duplicate IDs** keep their own journal record IDs after recovery. Artemis files them under the transaction's ID.
- **Large-message files** are deleted once nothing holds the message any more, which is when Artemis's reference count releases it. When the broker stops, the files of messages still in memory are kept, as Artemis keeps them.
- **Divert transformers** are Rust plugins, not Java classes. The embedding application registers a `Transformer` under the class name that `broker.xml` gives. A divert whose transformer is not registered is not deployed, and a warning is logged.

## Security differences
- **Login modules.** `PropertiesLoginModule`, `GuestLoginModule`, `TextFileCertificateLoginModule`, `ExternalCertificateLoginModule` and `LDAPLoginModule` are supported. `Krb5LoginModule`, `KubernetesLoginModule`, `PrincipalConversionLoginModule` and custom Java modules are not. A `login.config` entry that names one gets a warning at startup, and logins that reach that module fail.
- **Security managers.** The JAAS security manager (`<jaas-security>`) and `ActiveMQBasicSecurityManager` are supported. The basic manager keeps its users in the bindings journal, so its users move with the data directory in both directions. Any other `<security-manager>` class stops the broker from starting. As in Artemis, the basic manager has no users without persistence. Users can be added only through the bootstrap credentials until management arrives in Phase 8.
- **A role named `*`** in a security setting grants nothing in Artemis 2.57, despite looking like a wildcard. Saranga matches this.
- **Security setting plugins** (`<security-setting-plugin>`) are not supported and produce a warning.
