# High availability

Saranga pairs a primary broker with a backup the way Artemis does: on a shared store, by replication with a quorum vote, or by replication with a lock manager. The `<ha-policy>` in `broker.xml` is Artemis's, the lock files, replication packets and ZooKeeper znodes are Artemis's, and clients fail over to a Saranga backup as they do to an Artemis one. So a Java broker and a Saranga broker can form a pair, either one as the other's backup. This page shows how to set up each kind of pair with Saranga and where it differs from Artemis. For the concepts and every setting, see Artemis's [high availability chapter](https://activemq.apache.org/components/artemis/documentation/latest/ha.html).

## Choosing a policy

| Policy | How the backup gets the data | What decides who is active | Use it when |
|---|---|---|---|
| Shared store | Both brokers use one journal directory | File locks on that directory | You have shared storage with working locks |
| Replication with a quorum vote | The primary sends every write to the backup | A vote among the cluster's other primaries | Each broker has its own disk, and the cluster has at least three primaries |
| Replication with a lock manager | The primary sends every write to the backup | A lock and an activation sequence in ZooKeeper (or a shared folder) | Each broker has its own disk, and you want protection from split brain in a single pair |

Every policy needs a cluster connection: the backup uses it to announce itself to its primary, or to find the primary it replicates. Clients learn the pair from the topology the cluster connection sends them. See [Clustering](clustering.md) for cluster connections and discovery.

## Shared store

A shared-store primary and backup point at the same journal, bindings, paging and large-message directories. The primary holds the primary lock; the backup waits for the backup lock, then for the primary to fail, and becomes active on the same data.

Make the pair with `saranga create`, which writes the settings `artemis create` writes:

```bash
saranga create primary --shared-store --static-cluster tcp://localhost:61716 \
  --host localhost --data /shared/data \
  --cluster-user cluster --cluster-password secret --user admin --password admin --require-login
saranga create backup --shared-store --backup --static-cluster tcp://localhost:61616 \
  --host localhost --port-offset 100 --data /shared/data \
  --cluster-user cluster --cluster-password secret --user admin --password admin --require-login
```

The policy it writes is Artemis's:

```xml
<ha-policy>
   <shared-store>
      <backup>
         <failover-on-shutdown>false</failover-on-shutdown>
      </backup>
   </shared-store>
</ha-policy>
```

`failover-on-shutdown`, `allow-failback` and `restart-backup` work as in Artemis. So do `journal-lock-acquisition-timeout`, `journal-lock-monitor-timeout`, `journal-lock-monitor-max-retries` and `node-manager-lock-directory`.

What to know:

- `server.lock` holds the node ID and state as Artemis writes it, and the primary and backup locks are on `serverlock.1` and `serverlock.2`. Saranga takes them as Java does (`fcntl` record locks on Unix, `LockFileEx` on Windows), so a Java broker and a Saranga broker exclude each other on one directory.
- A backup's acceptors and web server start only once it is active.
- A primary whose lock another broker holds waits for it for `journal-lock-acquisition-timeout`.
- Without a cluster connection, a backup logs `AMQ222308` and cannot announce itself, as in Artemis.

See [Persistence differences](../../COMPAT.md#persistence-differences) for the details.

### Failback with a shared store

With `allow-failback` on the backup, a restarted primary announces itself to the active backup through the cluster connection. The backup gives way and, with `restart-backup`, waits as a backup again. This is Artemis's behaviour. Artemis describes it under [failing back to the primary](https://activemq.apache.org/components/artemis/documentation/latest/ha.html).

## Replication with a quorum vote

A replicating primary and backup each keep their own data. When the backup starts, it moves its old data aside into `oldreplica.N` (up to `max-saved-replicated-journals-size` copies). It then finds its primary through its cluster connection, authenticates as the cluster user and registers. The primary copies its journal files, page files and large messages to it, and from then on sends every record, page write and large-message write as it makes them. A client's operation is answered once the backup has what it wrote.

```bash
saranga create primary --replicated --static-cluster tcp://localhost:61716 \
  --host localhost --cluster-user cluster --cluster-password secret \
  --user admin --password admin --require-login
saranga create backup --replicated --backup --static-cluster tcp://localhost:61616 \
  --host localhost --port-offset 100 --cluster-user cluster --cluster-password secret \
  --user admin --password admin --require-login
```

The primary's policy, as `create --replicated` writes it:

```xml
<ha-policy>
   <replication>
      <primary>
         <vote-on-replication-failure>true</vote-on-replication-failure>
      </primary>
   </replication>
</ha-policy>
```

When the primary's connection goes, a synchronized backup asks the cluster's other primaries whether they still see its primary. If the vote says the primary is gone, the backup becomes active on its copy with the primary's node ID and logs `AMQ221007`. A backup not yet synchronized starts again. A primary that loses its backup goes on alone, after a vote if `vote-on-replication-failure` is set, and stops if it loses the vote. `quorum-size`, `vote-retries`, `vote-retry-wait`, `quorum-vote-wait`, `initial-replication-sync-timeout` and `retry-replication-wait` work as in Artemis.

Two cautions:

- **A lone pair has no one to ask.** In a cluster with no other primaries the backup needs no votes, so any break in its replication connection makes it active beside a live primary. This is split brain, and it happens in Artemis too. Use a lock manager (below) for a single pair, or run at least three primaries. Artemis's [network isolation chapter](https://activemq.apache.org/components/artemis/documentation/latest/network-isolation.html) describes the problem, and Saranga's network check is described in [Operations](operations.md).
- **`group-name` does not choose a primary.** A Saranga backup does not keep backup group names, so with several primaries in the cluster `group-name` does not decide which one it replicates.

### Failback with replication

Set `check-for-active-server` on the primary and `allow-failback` on the backup:

```xml
<!-- primary -->
<ha-policy>
   <replication>
      <primary>
         <check-for-active-server>true</check-for-active-server>
      </primary>
   </replication>
</ha-policy>

<!-- backup -->
<ha-policy>
   <replication>
      <backup>
         <allow-failback>true</allow-failback>
      </backup>
   </replication>
</ha-policy>
```

A primary with `check-for-active-server` first asks the brokers of its cluster whether one is active with its node ID. It asks with a session as the cluster user, as Artemis does. If one is, the primary moves its data aside and replicates that broker as its backup, asking for failback. A broker that allows it (a backup with `allow-failback`, or a primary, which always does) waits for the new backup to synchronize and announce itself. It then tells the new backup to take over, stops, and replicates it in turn, logging `AMQ221039`. A broker that does not allow it leaves the restarted primary as its backup, which logs `AMQ224056`.

`check-for-live-server` is accepted with a deprecation warning, as are the old `master` and `slave` element names.

## Replication with a lock manager

Add a `<manager>` to the replication `primary` and `backup`. The pair then decides who is active through a distributed lock and an activation sequence, not a vote. Saranga has both of Artemis's lock managers, chosen by their class names.

### ZooKeeper

Curator's ZooKeeper manager is the default class:

```xml
<ha-policy>
   <replication>
      <primary>
         <manager>
            <class-name>org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManager</class-name>
            <properties>
               <property key="connect-string" value="zk1:2181,zk2:2181,zk3:2181"/>
            </properties>
         </manager>
         <coordination-id>orders</coordination-id>
      </primary>
   </replication>
</ha-policy>
```

The backup takes the same `<manager>` and, if it should give way to the primary when it returns, `<allow-failback>true</allow-failback>`.

- The properties are Curator's: `connect-string`, `namespace`, `session-ms` (18000 by default), `session-percent` (33), `connection-ms` (8000), `retries` (1) and `retries-ms` (1000). Any other key is refused, as Artemis refuses it.
- The deprecated `org.apache.activemq.artemis.quorum.zookeeper.CuratorDistributedPrimitiveManager` class name works too.
- Saranga's ZooKeeper client is its own, but it makes the znodes Curator makes. A node's lock is a one-lease semaphore at `/<node ID>/locks`, and its activation sequence a counter at `/<node ID>/activation-sequence`. A Java broker and a Saranga broker therefore coordinate on one ensemble.
- `coordination-id` is applied as Artemis applies it (at most 16 characters, padded with `+`, dashes made dots) and becomes the node ID written to `server.lock`.

### File-based

`FileBasedLockManager` coordinates through a folder both brokers can lock:

```xml
<manager>
   <class-name>org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager</class-name>
   <properties>
      <property key="locks-folder" value="/shared/locks"/>
   </properties>
</manager>
```

The folder must already exist and be a directory. Each lock is a file in it, locked as Java locks it, so Java and Saranga brokers exclude each other here too.

### How activation works

Each broker keeps its local activation sequence in `server.activation.sequence`, beside `server.lock`. A broker becomes active only when it holds the node's lock and its local sequence is not behind the coordinated one. It claims and commits the next sequence before it serves clients.

- A primary with stale data replicates the broker active with its node ID and asks for failback.
- A backup activates once its primary is gone, its copy is complete and the lock is free. After a crash, that is when the primary's ZooKeeper session expires, so `session-ms` sets how long a failover takes.
- An active broker that loses its backup moves the sequence on, so the backup's stale copy cannot activate.
- An active broker that loses the lock or the ensemble stops, logging "Quorum UNAVAILABLE".
- A broker that hands back (`allow-failback`) waits for the other to activate before it replicates it, and logs `AMQ221039`.

Artemis describes the same behaviour under [pluggable lock manager](https://activemq.apache.org/components/artemis/documentation/latest/ha.html).

### Repairing activation sequences

`saranga activation list` and `saranga activation set` read and force the local and coordinated sequences, as Artemis's [activation tools](https://activemq.apache.org/components/artemis/documentation/latest/activation-tools.html) do. Run them from the instance's `bin` directory while the broker is stopped. They take the instance's lock, as the data tools do.

```bash
# both sequences of this broker's node
bin/artemis activation list
# only the local one, kept beside server.lock
bin/artemis activation list --local
# after a ZooKeeper disaster, on the broker with the highest local sequence
bin/artemis activation set --remote --to 1
```

`--remote` connects to the manager in `broker.xml`. `--node-id` names another node with `--remote`, and `--verbose` prints more. The commands work only on an instance whose `ha-policy` is replication `primary` or `backup` with a `<manager>`.

## Client failover

Core clients fail over as they do with Artemis. Give the connection `ha=true` and a reconnection setting, and list more than one server if you like:

```text
(tcp://broker1:61616,tcp://broker2:61616)?ha=true&reconnectAttempts=-1&retryInterval=1000
```

The client learns from the topology which backup is paired with its server, connects to it when the server fails, and recreates its sessions there. A transaction with work in it can then only roll back (`TRANSACTION_ROLLED_BACK`, or `TRANSACTION_OUTCOME_UNKNOWN` for a commit in flight), as with Artemis. Use duplicate IDs to make a resent message safe. Artemis's [client failover chapter](https://activemq.apache.org/components/artemis/documentation/latest/client-failover.html) applies unchanged to the Artemis JMS client against Saranga.

- **The CLI.** `saranga producer`, `consumer`, `browser` and the other messaging commands take the server list and the locator parameters `ha`, `reconnectAttempts`, `failoverAttempts`, `initialConnectAttempts`, `retryInterval`, `retryIntervalMultiplier`, `maxRetryInterval`, `callTimeout`, `callFailoverTimeout`, `clientFailureCheckPeriod` and `connectionTTL`. Other parameters are ignored. On Windows, separate parameters with `;` rather than `&`, because `artemis.cmd` passes its arguments through `cmd`. See [CLI differences](../../COMPAT.md#cli-differences).
- **saranga-client.** The Rust client fails over the same way, but it keeps no resend cache: after a failover a session is recreated, not reattached, and sends still waiting for their response fail (`sendFailed`). See [Configuration differences](../../COMPAT.md#configuration-differences).
- **`failover-on-shutdown`.** With a shared store, a primary stopped cleanly lets its backup take over only when `failover-on-shutdown` is set. Otherwise connected clients get a disconnect when it stops.

## Mixed Java and Saranga pairs

Each policy works with Artemis 2.57.0 on one side and Saranga on the other, either way round. The suites under `tests/interop/ha` run every policy in all four pairings, and the no-loss suite kills the primary during a transactional load and checks that every committed message is received once. This lets you move a pair node by node: replace the backup first, let it synchronize (or, with a shared store, let it take the backup lock), fail over to it, then replace the primary. See [Migrating from Artemis](migration.md).

## What is not supported

- **Colocated backups.** `<colocated>` under `shared-store` or `replication` stops the broker from starting, with a reason.
- **Scale-down.** `<scale-down>` is read and logged as a warning. A backup becomes active on the primary's data, and a primary does not scale its messages down to another broker when it stops.
- **Lock coordinators.** `<lock-coordinators>`, a technical preview in Artemis 2.57, is not implemented: it is ignored with a warning. An acceptor or broker connection that names a lock coordinator is not held back by it, and `listLockCoordinatorsAsJSON` answers with an empty list.
- **The JDBC store,** and so a JDBC shared store. `<database-store>` stops the broker from starting.

The full list of differences is in [Persistence differences](../../COMPAT.md#persistence-differences) and [Configuration differences](../../COMPAT.md#configuration-differences).
