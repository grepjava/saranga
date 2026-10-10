# Golden corpus

Ground truth recorded from the real Artemis broker (version in `COMPAT.md`).
Saranga's codecs must decode every file here and re-encode it byte for byte.

Each `<scenario>/` holds:
- `wire/conn-N/c2s.bin`, `s2c.bin`: raw TCP bytes for connection N, client to server and server to client.
- `data.tar.gz`: the broker's `data/` dir after shutdown (journal, bindings, paging,
  large-messages). It is compressed because journal files are pre-allocated.
- `MANIFEST`: Artemis version and commit, generation time, how the broker was shut down,
  which `broker.xml` it ran with, and whether the wire was captured.
- `data-print.txt`: the output of Java's `artemis data print` over `data/`. It is the
  reference for `saranga data print`.
- `QUEUES`: the queues to dump after recovery, one per line. A subscription queue is
  written as an FQQN, `address::queue`. This file is written by hand.
- `recovered.txt`: what a Core client sees after the real broker recovers `data/`.

Never edit the generated files by hand. To regenerate:

    scripts/build-artemis.sh          # once: builds saranga/artemis-ref:<version>
    scripts/golden.sh <scenario>      # runs tests/golden-gen/scenarios/<Scenario>.java

`golden.sh` also writes `data-print.txt`, and `recovered.txt` when `QUEUES` exists.
It keeps `QUEUES`. Options, as environment variables:
- `GOLDEN_KILL=1`: the scenario prints `READY` and blocks. The broker is then killed with
  `docker kill` (SIGKILL) while the client is still connected. Used by `persist-crash`.
- `GOLDEN_WIRE=0`: no capture proxy and no `wire/`. Used by `persist-many-files`, whose
  traffic would be about 70 MB.

If `tests/golden/etc/<scenario>.broker.xml` exists, the broker runs with it instead of
`tests/golden/etc/broker.xml` (the image's generated config). It is copied into the
instance's `etc-override/` before the instance is created. `golden-verify.sh` uses the
same file.

Requires Docker and a JDK 17+ on the host.

## Reference outputs

    scripts/golden-print.sh <scenario>             # writes data-print.txt
    scripts/golden-verify.sh <scenario> [artemis|saranga] > out.txt

`golden-print.sh` runs `artemis data print` in the reference image on a copy of the data
dir, mounted at `/golden/data`, with explicit `--journal`, `--bindings`, `--paging` and
`--large-messages`. No broker instance is needed. Notes on its output:
- Paths are always `/golden/data/...`, and dates print in UTC.
- `Message(...)@<n>` and `XidImpl (<n> ...` end with Java identity hash codes. They are
  stable between runs today but carry no meaning. A Saranga comparison should mask them.
- The tool prints a warning about running outside a broker instance on stderr. It is not
  part of the file.

`golden-verify.sh` starts a broker on a fresh copy of `data.tar.gz` and runs
`tests/golden-gen/tools/DumpQueues.java` with the queues in `QUEUES`. The golden files are
never touched. `artemis` mode copies the data into the reference image. `saranga` mode
starts `target/debug/saranga run`, but Saranga cannot open a data dir yet (TODO in the
script), so it only works once Phase 3 adds that. `recovered.txt` is `artemis` mode's
output.

`DumpQueues` uses only the Core client API. It prints, one line each:
- the in-doubt XA transactions from `recover()`, sorted, left unresolved;
- per queue, the `QueueQuery` result, then every message drained with
  `receiveImmediate()` and acked: ID, user ID, durable, priority, delivery count,
  timestamp, expiration, sorted typed properties, body type and body (hex up to 256
  bytes, else SHA-256), and the text of a text message.

Scheduled messages are not delivered, so they only show in the query's `messageCount`.
The same goes for messages acked inside a prepared XA transaction.

## Scenarios

- `core-basic`, `core-xa`, `core-large`: Phase 1 wire scenarios. Their data dirs were
  not regenerated; only `QUEUES`, `data-print.txt` and `recovered.txt` were added.
- `persist-durable`: clean shutdown with every kind of durable state. Queues with
  non-default attributes (exclusive, last-value, filter, non-destructive, ring, groups,
  user, auto-delete), an address with both routing types, durable subscriptions (one
  with a selector), unconsumed messages of every priority and body type, a duplicate ID,
  individual acks, rollback redeliveries, a message scheduled for 2100, a committed
  transaction, LVQ and ring removals. The non-durable queue `persist.topic.volatile` and
  the non-persistent messages must not survive.
- `persist-xa`: XA branches in every state. Two are left prepared (sends and acks), one
  is committed after prepare, one rolled back after prepare, one committed in one phase
  and one rolled back without prepare.
- `persist-crash`: the broker is killed while a local transaction with two sends and two
  acks is open. The journal ends with that transaction and no commit or rollback record;
  recovery drops it.
- `persist-many-files`: about 35 MB through 1 MiB journal files with eager compaction
  (`tests/golden/etc/persist-many-files.broker.xml`). Files roll over, are reclaimed and
  compacted. One message in each batch of 100 stays (35 in all), plus 3 on a tail queue.
