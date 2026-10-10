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
- **`${...}` in `broker.xml`** resolves from environment variables. Java resolves from system properties (`-Dname=value`) first; Saranga has no system properties, so set an environment variable of the same name.
- **Namespaces.** Elements are matched by local name, so prefixed elements that Java would miss are accepted.
- **`xi:include`** is not expanded yet; it produces a warning.

## Persistence differences
- Saranga writes journal files the way Artemis's `NIO` journal does: records follow each other without block alignment, whatever `journal-type` says. Artemis reads such files with any journal type, and Saranga reads `ASYNCIO` files (aligned flushes padded with zeros).
- Appends wait while the journal compacts; Artemis keeps appending and replays the appends afterwards. The compacted files are the same.
- Group commit comes from a flusher thread that syncs whatever accumulated while the previous sync ran, rather than `TimedBuffer`'s timer, so `journal-buffer-timeout` has no effect.
- `server.lock` holds the node ID and state as in Artemis; the primary lock is a lock on `serverlock.1`. The node ID of a new instance is a random UUID rather than a time-based one.
- The XA error messages print `XidImpl (<n> ...` with a hash of the Xid where Java prints an identity hash.

## Queue semantics differences
- **delay-before-dispatch** ends on a timer. Artemis has no timer: it starts dispatching at the next delivery attempt after the delay, such as a new message, a consumer's credits or a forced receive.
- **Last-value queues** drop a replaced value as soon as its replacement arrives. Artemis prunes on its next delivery pass. The queue holds the same messages afterwards.
- **Message groups.** While a group's consumer has no credit, only that group's messages wait. Artemis ends the whole delivery pass at that point.
- **Duplicate detection** checks the message a client sends. Copies a divert makes are not checked against the forwarding address's cache.
- **A prepared XA branch's duplicate IDs** keep their own journal record IDs after recovery. Artemis files them under the transaction's ID.
- **Large-message files** are deleted once nothing holds the message any more, which is when Artemis's reference count releases it. When the broker stops, the files of messages still in memory are kept, as Artemis keeps them.
- **Core frame size.** With security enabled, Artemis rejects Core frames over 4 KiB before a connection authenticates and over 128 KiB afterwards (`coreInitialMaxFrameSize`, `coreMaxFrameSize`). Saranga has no limit until security arrives in Phase 6.
- **Divert transformers** are Rust plugins, not Java classes. The embedding application registers a `Transformer` under the class name that `broker.xml` gives. A divert whose transformer is not registered is not deployed, and a warning is logged.
