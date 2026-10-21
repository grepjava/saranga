# Storage

Saranga keeps its data in the same files as Apache ActiveMQ Artemis 2.57.0: the message journal, the bindings journal, page files, large-message files and `server.lock`, laid out the same way and configured by the same `broker.xml` elements. A Java broker reads a data directory Saranga wrote, and Saranga reads one a Java broker wrote, so you can move an instance between the two by stopping one and starting the other on the same data. This page covers where the data lives, how Saranga writes it, paging, large messages, full disks, journal retention, I/O failures and the `saranga data` tools. For each setting that behaves as in Artemis it gives one line and a link; the differences are summarised here and described in [COMPAT.md](../../COMPAT.md).

## The data directory

An instance made by `saranga create` (or `artemis create`) keeps its data under `data/`:

| Directory | `broker.xml` element | Holds |
|---|---|---|
| `data/journal` | `journal-directory` | the message journal, `activemq-data-<n>.amq`, and `server.lock` with `serverlock.1` and `serverlock.2` |
| `data/bindings` | `bindings-directory` | the bindings journal, `activemq-bindings-<n>.bindings`: addresses, queues, the ID counter and the other records Artemis keeps there |
| `data/paging` | `paging-directory` | one directory per paging address, named by a UUID, with `address.txt` and the page files `<nnnnnnnnn>.page` |
| `data/large-messages` | `large-messages-directory` | one `<id>.msg` file per durable large message (`.tmp` for a non-durable one) |

Relative directories are resolved against the instance directory, as Artemis resolves them. `persistence-enabled` set to `false` keeps every message in memory; see [Paging](#paging) for what that means for full addresses.

Saranga does not have the JDBC store. A `broker.xml` with `<database-store>` does not start, with a reason (see [COMPAT.md, Configuration differences](../../COMPAT.md#configuration-differences)).

The Artemis documentation on [persistence](https://activemq.apache.org/components/artemis/documentation/latest/persistence.html) describes the journal and its settings; it applies to Saranga except where this page says otherwise.

## The journal

The journal is Artemis's append-only journal, record for record. Saranga honours the settings that shape the files and their syncing: `journal-file-size`, `journal-min-files`, `journal-pool-files`, `journal-buffer-size`, `journal-sync-transactional`, `journal-sync-non-transactional`, `journal-datasync`, `journal-compact-min-files`, `journal-compact-percentage` and `journal-max-attic-files`, with Artemis's defaults (10 MiB files, at least 2 of them, compaction considered beyond 10 files when less than 30% of the data is live).

Where Saranga's journal differs (see [COMPAT.md, Persistence differences](../../COMPAT.md#persistence-differences)):

- **Journal type.** Saranga writes every journal the way Artemis's `NIO` journal does, with records following each other without block alignment, whatever `journal-type` says. `ASYNCIO` and `MAPPED` are accepted, and Saranga reads `ASYNCIO` files written by Artemis. Artemis reads Saranga's files with any journal type. `saranga create` chooses `NIO` unless you pass `--aio` or `--mapped`.
- **Group commit.** A flusher thread writes whatever appends are queued, syncs each file it touched once, and answers the appends. Appends that arrive while it writes or syncs go into the next round. There is no timer, so `journal-buffer-timeout` has no effect, and the value `artemis create` tunes for it does not matter.
- **Compaction.** Compaction produces the same files as Artemis's. While it runs, appends wait; Artemis keeps appending and replays the appends afterwards.

To compact a stopped broker's journals by hand, use `saranga data compact` (see [The data tools](#the-data-tools)).

## server.lock

`server.lock` sits in the journal directory, or in `node-manager-lock-directory` when that is set. It holds the node ID and the node's state, as Artemis's does. A broker takes a lock on `serverlock.1` while it is active, and a shared-store backup takes `serverlock.2` while it waits. The locks are the ones Java takes (`fcntl` record locks on Unix, `LockFileEx` on Windows), so a Java broker and a Saranga broker exclude each other on one data directory, and either can be the other's shared-store backup. A primary that finds the lock held waits for `journal-lock-acquisition-timeout`, as in Artemis.

Never delete `server.lock` to move an instance between brokers: it holds the node ID, which clients and cluster members use to recognise the node. See [ha.md](ha.md) for shared-store and replicated pairs.

## Paging

Paging works as in Artemis. Each address has a size limit (`max-size-bytes`, `max-size-messages`, and the broker-wide `global-max-size` and `global-max-messages`), and `address-full-policy` says what happens at the limit. When `global-max-size` is not set, it is `global-max-size-percent-of-jvm-max-memory` (50% by default) of the heap Artemis's JVM would have: the `-Xmx` (or `-XX:MaxHeapSize`) of the profile's `JAVA_ARGS`, else the JVM's default, a quarter of the memory. An instance `create` makes has `-Xmx2G`, so 1 GiB.

- `PAGE` (the default) writes new messages to the address's page files, and queues read them back as they consume.
- `BLOCK` holds producers back until there is room: Core producers get no more credit, AMQP links no more link credit, and OpenWire sends wait or the connection stops being read, as on Artemis. With `max-size-bytes-reject-threshold`, AMQP sends past it are rejected. An address blocked through management (`block`) refuses Core producers' credit as `FAIL` does.
- `FAIL` fails the send.
- `DROP` drops the message.

`page-size-bytes` (10 MiB by default), `max-read-page-messages`, `max-read-page-bytes`, `prefetch-page-messages`, `prefetch-page-bytes`, `page-limit-bytes`, `page-limit-messages` and `page-full-policy` are honoured. The page files are Artemis's, so a backlog paged by one broker can be drained by the other. See the Artemis documentation on [paging](https://activemq.apache.org/components/artemis/documentation/latest/paging.html).

Where Saranga differs (see [COMPAT.md, Paging differences](../../COMPAT.md#paging-differences)):

- **Paging needs persistence.** With `persistence-enabled` set to `false`, Saranga writes no page files and keeps every message in memory; only `BLOCK`, `FAIL` and `DROP` limit an address. Artemis still pages in that case.
- **Queue browsers** see the messages in memory, including those read in from the pages up to `max-read-page-messages` and `max-read-page-bytes`, but not the rest of a longer backlog.
- **Page counters** are rebuilt from the page files at every start. The counter records Artemis writes are deleted when Saranga loads them; Artemis rebuilds its counters when they are missing, so the data directory still moves back.

## Large messages

A Core message larger than the client's `minLargeMessageSize` is streamed to its own file in the large-messages directory, and its record in the journal or page file refers to it, as in Artemis. AMQP and STOMP messages above the acceptor's `amqpMinLargeMessageSize` or `stompMinLargeMessageSize` are stored as large messages too. `large-message-sync` is honoured. A file is deleted once nothing holds its message any more; when the broker stops, the files of messages still in memory are kept, and files a crash left incomplete are removed at the next start. Without persistence, large bodies stay in memory. See the Artemis documentation on [large messages](https://activemq.apache.org/components/artemis/documentation/latest/large-messages.html).

Where Saranga differs:

- An AMQP large message is held whole in memory while the broker holds it. It is still written to its file with Artemis's record, so either broker reads the other's ([COMPAT.md, AMQP differences](../../COMPAT.md#amqp-differences)).
- An MQTT retained message over the journal's record size stays a regular message in the retained-message queue ([COMPAT.md, MQTT differences](../../COMPAT.md#mqtt-differences)).

## Full disks

Every `disk-scan-period` (5000 ms by default) Saranga reads the free space of the disks holding the journal, bindings, paging and large-messages directories from the operating system (`statvfs` on Unix, `GetDiskFreeSpaceExW` on Windows). A disk is full when its usage is beyond `max-disk-usage` (90% by default), or, when `min-disk-free` is set, when its free space is below that; `min-disk-free` wins when both are set, with Artemis's `AMQ224130` notice. Setting both to `-1` turns the monitor off.

While a disk is full, each address does what its `disk-full-policy` address setting says: `BLOCK` (the default) holds producers back, `FAIL` fails sends, and `DROP` drops messages. Saranga logs Artemis's messages, so existing alerts keep working:

```text
AMQ222210: Free storage space is at ... which is beyond the configured <max-disk-usage>. System will start blocking producers.
AMQ222212: Disk Full! Blocking message production on address '...'. Clients will report blocked.
AMQ222211: Free storage space is at ... which is below the configured <max-disk-usage>. System will unblock producers.
```

With `min-disk-free` the messages are `AMQ224128` and `AMQ224129`. The management address is never blocked.

Core, AMQP and OpenWire connections have flow control, so the policy can hold them back. STOMP and MQTT connections, and management's `sendMessage`, have none. While the disk is full, a send from one of them fails with `AMQ229119`, as in Artemis:

- **STOMP:** the client gets an `ERROR` frame carrying `AMQ229119`, then the `SEND`'s own `ERROR` (`AMQ339011`) with its receipt, and the connection closes. The broker logs `AMQ222067`, `AMQ222061` and `AMQ222107`.
- **MQTT:** the connection closes without a `PUBACK` (and, for MQTT 5, without a `DISCONNECT`).
- **Management:** `sendMessage` on an address or queue fails with an `IllegalStateException` carrying the `AMQ229119` text, whatever the address's `disk-full-policy`.

The disk-monitoring section of Artemis's [paging](https://activemq.apache.org/components/artemis/documentation/latest/paging.html) page describes the settings.

## Journal retention and replay

Journal retention works as in Artemis. Set a retention directory, with a period, a storage limit or both:

```xml
<journal-retention-directory period="7" unit="DAYS" storage-limit="10G">data/history</journal-retention-directory>
```

Each message-journal file the journal moves on from is copied there as `activemq-data-<yyyyMMddHHmmss>-<id>.amq`, named in local time. Copies older than the period, or beyond the storage limit, are removed. Paged messages and large-message bodies are also written to the journal as events, so the copies hold them, and `saranga data compact` keeps the compacted files in the history.

The broker's `replay` management operations route retained messages again, to the same address or another, optionally filtered and limited to a time range (`yyyyMMddHHmmss`). Without a retention directory they fail with `AMQ229239`. `artemis data print --journal <retention directory>` reads Saranga's copies, and `saranga data print` reads Artemis's. See Artemis's [data retention](https://activemq.apache.org/components/artemis/documentation/latest/data-retention.html) page, and [management.md](management.md) for calling operations.

## Critical I/O errors

A failed write, sync, open, rename or removal under the journals, a page file or a large-message file is a critical I/O error, as Artemis's `IOCriticalErrorListener` makes it. Saranga logs `AMQ144011` (`AMQ144010` when there is no file) and `AMQ222010`. The journal accepts no more writes, the client waiting on the operation gets an `IO_ERROR` exception, and the broker stops: its lock is paused, or let go with `failover-on-shutdown`, so a backup can take over. Every send or commit that was answered before the error is on disk and comes back when the broker restarts.

Two differences (see [COMPAT.md, Critical I/O error differences](../../COMPAT.md#critical-io-error-differences)):

- **The process ends.** `saranga run` exits with status 1, so a service manager such as systemd sees the failure and can restart the broker once the disk is fixed. Artemis's `artemis run` stays up with nothing running.
- **What is covered.** Page directories and their `address.txt`, the large-message directory and a replicating backup's synchronisation files are made outside the critical path: an error there fails the operation that needed them but does not stop the broker.

See [operations.md](operations.md) for running the broker under a service manager.

## The data tools

`saranga data` has Artemis's data tools, with the same options and output:

| Command | Does |
|---|---|
| `data print` | prints the bindings and message journals and the page files (`--safe` hides message data, `--reclaimed` reads reclaimed files too) |
| `data exp` | exports every message, with its bindings, to Artemis's XML format (`--queue` limits it to some queues) |
| `data imp` | sends an exported XML file to a running broker over Core |
| `data compact` | compacts the message and bindings journals of a stopped broker |
| `data recover` | writes a new journal (`--target`) with every message it can find, rolled-back and acknowledged ones included |
| `data encode` / `data decode` | turn journal files into Artemis's text encoding and back |

`saranga help data <command>` lists every option.

Run them from the instance, which tells them where `etc/bootstrap.xml` is, so they read the directories from `broker.xml`:

```bash
cd /var/lib/mybroker
bin/saranga data print --safe > print.txt
bin/saranga data exp --output export.xml
```

On Windows, use `bin\saranga.cmd`. Since the instance's `bin/artemis` runs Saranga too, `bin/artemis data ...` does the same.

Outside an instance, name the configuration (`xml:/path/to/etc/bootstrap.xml`) or the directories:

```bash
saranga data print --journal data/journal --bindings data/bindings \
  --paging data/paging --large-messages data/large-messages
```

The tools take the instance's CLI lock (`lock/cli.lock`), so they refuse to run against an instance whose broker is running. `print`, `exp` and `recover` take `--f` to skip the lock and read a running broker's data; what they read may then be inconsistent.

To move messages between brokers, or into a new data directory, export them and import them into a running broker:

```bash
bin/saranga data exp --output export.xml
saranga data imp --input export.xml --url tcp://newhost:61616 --user admin --password admin
```

`data imp` sends over Core only. On the data directories of the golden corpus, `data exp` writes Artemis's XML byte for byte, and `data encode` its text. Differences from Artemis's tools, such as `--jdbc` being refused and failures being reported as `Error:<message>` with status 0, are listed in [COMPAT.md, CLI differences](../../COMPAT.md#cli-differences). Artemis's [data tools](https://activemq.apache.org/components/artemis/documentation/latest/data-tools.html) page describes the formats. See [cli.md](cli.md) for the rest of the command line.

## Moving data between Artemis and Saranga

Because the files are the same, moving an instance is a matter of stopping one broker and starting the other on the same directories:

1. Stop the broker cleanly.
2. Start the other broker with the same `broker.xml` and data directories. `saranga upgrade` (or `artemis upgrade` to go back) rewrites an instance's scripts to run the other broker.

Saranga recovers the data directories Artemis 2.57.0 writes, including those of a killed broker and of a journal Artemis compacted, and Artemis recovers Saranga's. Paged backlogs move in both directions. A few records are written differently but read the same way by both: see the queue filter note and the page counter note in [COMPAT.md](../../COMPAT.md#persistence-differences). [migration.md](migration.md) has the full procedure.
