# Clustering

Saranga links brokers together the ways Artemis does: cluster connections that share load and redistribute messages, core bridges, core federation, and AMQP broker connections with their federation, bridges and mirroring. The configuration is Artemis's `broker.xml`, and what goes over the wire is what Artemis sends. So Java and Saranga nodes can sit in one cluster, and either can federate, bridge or mirror to the other. This page shows how to set each up with Saranga and where it differs. For the concepts and every setting, see Artemis's [clusters chapter](https://activemq.apache.org/components/artemis/documentation/latest/clusters.html).

## Cluster connections

A cluster connection makes the node part of a cluster. Each active node bridges to every other primary in the topology through a store-and-forward queue (`$.artemis.internal.sf.<cluster>.<node ID>`), learns their queues and consumers from their notifications, and routes messages among local and remote queues. The topology also goes to clients, which use it to balance connections and to fail over (see [High availability](ha.md)).

`saranga create` writes the cluster settings `artemis create` writes. For a static cluster of two nodes on one host:

```bash
saranga create node1 --clustered --static-cluster tcp://localhost:61716 \
  --host localhost --cluster-user cluster --cluster-password secret \
  --user admin --password admin --require-login
saranga create node2 --clustered --static-cluster tcp://localhost:61616 \
  --host localhost --port-offset 100 --cluster-user cluster --cluster-password secret \
  --user admin --password admin --require-login
```

`--message-load-balancing` and `--max-hops` set those two elements. The cluster connection it writes:

```xml
<cluster-connections>
   <cluster-connection name="my-cluster">
      <connector-ref>artemis</connector-ref>
      <message-load-balancing>ON_DEMAND</message-load-balancing>
      <max-hops>1</max-hops>
      <static-connectors>
         <connector-ref>node0</connector-ref>
      </static-connectors>
   </cluster-connection>
</cluster-connections>
```

Every `<cluster-connection>` element is read as Artemis reads it, including the reconnection settings, `confirmation-window-size`, `producer-window-size`, `notification-interval`, `notification-attempts`, `topology-scanner-attempts` and `allow-direct-connections-only`. The nodes connect to each other as the cluster user (`cluster-user` and `cluster-password`), as in Artemis.

### Several cluster connections

A node can have several cluster connections, as in Artemis, each with its own `address`, load balancing and bridges. Every one is deployed. One that names a connector that does not exist (`AMQ222129`), reuses a name (`AMQ222130`) or names a missing discovery group (`AMQ222131`) is left out with Artemis's warning. The first one deployed is the default, which HA and quorum votes use. An acceptor sends its clients the default one's topology and hands it the node announcements other nodes send, unless the acceptor names another with its `clusterConnection` parameter:

```xml
<acceptor name="news">tcp://0.0.0.0:61626?clusterConnection=news-cluster</acceptor>
```

A cluster connection can also be written as a URI, as Artemis takes it, with the settings as its parameters. `static://` lists the nodes to connect to; `multicast://<discovery group>` finds them through a discovery group:

```xml
<cluster-connections>
   <cluster-connection name="orders-cluster">
      <address>orders</address>
      <connector-ref>artemis</connector-ref>
      <static-connectors>
         <connector-ref>node0</connector-ref>
      </static-connectors>
   </cluster-connection>
   <cluster-connection-uri name="news-cluster"
      address="static://(tcp://node2:61616)?connectorName=artemis&amp;address=news&amp;messageLoadBalancingType=STRICT"/>
</cluster-connections>
```

Broker properties set them too (`clusterConfigurations.news-cluster.maxHops=2`), with the same property names. As in Artemis, a value that is not a number or a boolean becomes 0 or false, and a load balancing type that is no constant is refused.

### Load balancing and redistribution

`message-load-balancing` takes `OFF`, `STRICT`, `ON_DEMAND` and `OFF_WITH_REDISTRIBUTION`, with Artemis's meanings. A cluster connection's URI and broker properties also take `LOCAL_ONLY`, which Artemis keeps for its own use: remote queues get nothing. A message picks one queue among the local and remote queues that share a routing name, round robin, as Artemis picks it.

`redistribution-delay` in the address settings moves messages from a queue with no consumers to a node with consumers. `STRICT` and `OFF` do not redistribute. Internal queues, the management address and grouped messages are left alone, as in Artemis. See Artemis's [message redistribution](https://activemq.apache.org/components/artemis/documentation/latest/clusters.html).

Clients: the Artemis client balances its connections across the topology as usual. saranga-client's locator sends each new connection to the next node round robin once the topology has arrived.

### Message grouping

A `<grouping-handler>` keeps each message group on one node across the cluster. `LOCAL` and `REMOTE` handlers work as in Artemis, with `timeout`, `group-timeout` and `reaper-period`. The `LOCAL` node journals its groupings and, after a restart, sends each group where it went before.

```xml
<grouping-handler name="my-grouping-handler">
   <type>LOCAL</type>
   <address>jms</address>
   <timeout>5000</timeout>
</grouping-handler>
```

One difference: when the queue a handler named is gone, Saranga retries only that routing name, so the message's other routing names do not get it twice. See Artemis's [message grouping chapter](https://activemq.apache.org/components/artemis/documentation/latest/message-grouping.html), and [Queue semantics differences](../../COMPAT.md#queue-semantics-differences) for how a group waits for its consumer.

### MQTT clients across the cluster

As in Artemis, an MQTT client that connects to one node with a client ID already connected to another node disconnects the first.

## Discovery

Nodes and clients can find each other by UDP multicast instead of a static list. Broadcast groups send the node's connectors every `broadcast-period`; discovery groups listen. The datagrams are Artemis's, so Java and Saranga brokers and clients hear each other.

```xml
<broadcast-groups>
   <broadcast-group name="bg-group1">
      <group-address>231.7.7.7</group-address>
      <group-port>9876</group-port>
      <broadcast-period>5000</broadcast-period>
      <connector-ref>artemis</connector-ref>
   </broadcast-group>
</broadcast-groups>

<discovery-groups>
   <discovery-group name="dg-group1">
      <group-address>231.7.7.7</group-address>
      <group-port>9876</group-port>
      <refresh-timeout>10000</refresh-timeout>
   </discovery-group>
</discovery-groups>
```

A cluster connection then uses `<discovery-group-ref discovery-group-name="dg-group1"/>` in place of `<static-connectors>`.

**Discovery is off by default,** as in Artemis 2.57. Turn it on with the `artemis.discovery.enabled` system property or the `ARTEMIS_DISCOVERY_ENABLED` environment variable:

```bash
saranga run -D artemis.discovery.enabled=true
```

In an instance, put `-Dartemis.discovery.enabled=true` in the profile's `JAVA_ARGS`, which `bin/artemis` passes on. Without the switch, a broker with broadcast or discovery groups does not start (`AMQ229262`, `AMQ229263`), and a client refuses a `udp://` URL (`AMQ219070`). `saranga create --clustered --discovery-enabled` writes the groups above and puts the switch first in both profiles, as Artemis's hidden option of that name does. `--clustered` without `--static-cluster` or `--discovery-enabled` is refused.

What to know:

- **UDP only.** A JGroups configuration (`jgroups-file`) stops the broker from starting, with a reason. Use `group-address` and `group-port`.
- A broadcast group starts once the broker is active, so a waiting backup does not broadcast.
- Every `connector-ref` of a broadcast group is read as its first one, as Artemis 2.57 reads them.
- The CLI and saranga-client take `udp://group:port` URLs with their parameters.
- A broadcast group has its `BroadcastGroupControl` (`component=broadcast-groups`) and notifications.

See [Configuration differences](../../COMPAT.md#configuration-differences).

## Core bridges

A core bridge consumes a local queue and sends to an address on another broker, found through `static-connectors` or a `discovery-group-ref`. Saranga's bridges work as Artemis's do: the same sessions and windows, `_AMQ_BRIDGE_DUP` with `use-duplicate-detection`, each message acknowledged once its send is, and reconnection to the same node `reconnect-attempts-same-node` times before trying the others. AMQP messages travel embedded in Core messages and are taken out again on the other side.

```xml
<bridges>
   <bridge name="orders-to-dc2">
      <queue-name>orders</queue-name>
      <forwarding-address>orders</forwarding-address>
      <use-duplicate-detection>true</use-duplicate-detection>
      <static-connectors>
         <connector-ref>dc2</connector-ref>
      </static-connectors>
   </bridge>
</bridges>
```

`BridgeControl`, `createBridge` and `destroyBridge` work. Bridges created through management are journaled and deployed again on restart, and a configuration reload redeploys a bridge whose configuration changed. A bridge that fails to authenticate stops the broker's bridges, broadcast groups and cluster connection (`AMQ224058`), as in Artemis. See Artemis's [core bridges chapter](https://activemq.apache.org/components/artemis/documentation/latest/core-bridges.html).

Differences:

- A large message is read whole into memory before the bridge sends it.
- `pause` and `resume` are not there (no management control exposes them).
- A bridge's transformer is a Rust transformer, registered under the class name `broker.xml` gives, not a Java class. See [Queue semantics differences](../../COMPAT.md#queue-semantics-differences).

## Core federation

`<federations>` federates addresses and queues from upstream brokers, and a downstream makes another broker federate from this one. Address policies, queue policies, upstreams, downstreams and `downstream-authorization` work as in Artemis. See Artemis's [federation](https://activemq.apache.org/components/artemis/documentation/latest/federation.html), [address federation](https://activemq.apache.org/components/artemis/documentation/latest/federation-address.html) and [queue federation](https://activemq.apache.org/components/artemis/documentation/latest/federation-queue.html) chapters.

`<federations>` is read as Artemis 2.57 reads it, oddities included. Where Saranga behaves differently or has less:

- An address policy's `max-hops` is not read (Artemis 2.57 takes the hops from another attribute), so from `broker.xml` a message that has crossed one federation is not federated again. `share-connection` is not read.
- Include and exclude patterns are anchored only at the end, as in Artemis, so `news.#` also matches `breaking.news.x`.
- Federation connections do not use TLS, and `federationConfigurations` broker properties are not supported.
- Policy transformers are Rust transformers registered by class name.
- With `enable-divert-bindings`, Saranga gives up a divert's demand when the last queue it forwards to is removed; Artemis keeps the remote consumer.
- The connection underneath does not reconnect by itself. When it fails, each federated consumer connects again (at once, then after 1, 2, 4… up to 30 seconds), as Artemis's consumers do once their locator gives up.
- A federated consumer that cannot find its queue on the other broker closes only its own session.
- There are no federation plugin hooks.

The full list is under [Configuration differences](../../COMPAT.md#configuration-differences).

## AMQP broker connections

An `<amqp-connection>` makes the broker connect out to another AMQP endpoint, which need not be a broker of either kind. `sender`, `receiver` and `peer` elements make links for the queues they match, now and when such queues are made later:

```xml
<broker-connections>
   <amqp-connection uri="tcp://dc2:5672" name="dc2" retry-interval="1000" reconnect-attempts="-1"
                    user="bridge" password="secret">
      <sender queue-name="outbox"/>
      <receiver queue-name="inbox"/>
   </amqp-connection>
</broker-connections>
```

The connection retries with Artemis's counts and logs (`AMQ111001`, `AMQ111002`, `AMQ111003`) and connects again when the peer closes one of its links. `BrokerConnectionControl` and the server's `listBrokerConnections`, `startBrokerConnection` and `stopBrokerConnection` work. See Artemis's [broker connections chapter](https://activemq.apache.org/components/artemis/documentation/latest/amqp-broker-connections.html).

Differences:

- SASL: PLAIN (with a user and password), XOAUTH2 and ANONYMOUS are chosen in Artemis's order. SCRAM and EXTERNAL are not, so a peer that offers only SCRAM refuses the connection.
- TLS (`sslEnabled` in the URI) is not supported.
- `AMQPConnections` broker properties are not supported, and a configuration reload does not update broker connections.
- The `lock-coordinator` attribute is not acted on (see [High availability](ha.md#what-is-not-supported)).

### AMQP federation

A `<federation>` element in a broker connection federates addresses and queues over it: the control link, remote policies sent to the peer, the events links, and receivers attached where there is demand, with hops and idle timeouts. The other end needs no federation configuration of its own: a peer's control link makes it the federation's target, once the connection's user may send to `$ACTIVEMQ_ARTEMIS_FEDERATION`. See Artemis's [address federation](https://activemq.apache.org/components/artemis/documentation/latest/amqp-address-federation.html) and [queue federation](https://activemq.apache.org/components/artemis/documentation/latest/amqp-queue-federation.html) chapters.

Differences, in short:

- Address federation always uses one receiver per address. `enable-divert-bindings`, `allow-wildcard-groupings` and `ignoreAddressBindingFilters=false` are sent to the peer but not acted on here.
- Policy transformers are sent to the peer but not applied.
- A queue receiver with `amqpCredits=0` is given 100 credits at a time.
- On a target, the federation does not check that the connection's user may send to the addresses and queues it federates into.
- The federation's management controls are not registered, and the control and events links have no attach timeout.

### AMQP bridges

A `<bridge>` in a broker connection moves messages to and from a plain AMQP endpoint. From policies (`bridge-from-address`, `bridge-from-queue`) attach receivers for the demand here; to policies (`bridge-to-address`, `bridge-to-queue`) attach senders from each matching address and queue. Links the peer closes are attached again with the recovery delays and attempts you set.

Differences, in short:

- `enable-divert-bindings` is not acted on, and policy transformers are not applied.
- Core messages are always sent converted to AMQP; core tunnelling is not offered.
- There are no management controls and no metrics.
- A to-address policy's durable subscription takes `auto-delete-durable-subscription` and its settings, which Artemis never applies.
- A sender attached again starts its recovery count afresh, where Artemis's accumulates.

### Mirroring

A `<mirror>` sends everything the broker routes, each acknowledgement, and each address and queue added or removed to the peer, through the store-and-forward queue `$ACTIVEMQ_ARTEMIS_MIRROR_<connection>`. The peer applies them, keeping each message's broker ID and message ID and dropping duplicates. Configure a mirror on each side for a dual mirror; neither side sends back what the other sent.

```xml
<broker-connections>
   <amqp-connection uri="tcp://dc2:5672" name="DC2">
      <mirror message-acknowledgements="true" queue-creation="true" queue-removal="true" sync="false"/>
   </amqp-connection>
</broker-connections>
```

With `sync="true"`, a client's send, acknowledgement or commit is answered once the peer has it. `address-filter` limits what is mirrored. A Saranga broker mirrors to and from an Artemis broker. See Artemis's [mirroring section](https://activemq.apache.org/components/artemis/documentation/latest/amqp-broker-connections.html).

Differences, in short:

- Core messages are always sent converted to AMQP. Artemis's mirror asks for core tunnelling by default; Saranga's does not.
- A message routed to chosen queues of its address (from a cluster node) is mirrored without the queue list, so the peer routes it to all the address's queues.
- Acknowledgements join the store-and-forward queue after their transaction commits.
- The target does not withhold credit while many acknowledgements are pending, and `ackManagerFlushTimeout` and `mirror-page-transaction` are not acted on.
- There are no mirror metrics, and no management beyond the store-and-forward queue's own control.

The full lists of AMQP federation, bridge and mirror differences are under [Configuration differences](../../COMPAT.md#configuration-differences).

## Mixed Java and Saranga clusters

Java and Saranga nodes can be in one cluster. The interop suites run a three-node cluster with every mix of Artemis 2.57.0 and Saranga nodes: the Artemis JMS client sees every node in the topology, messages are shared out and redistributed, groups keep their node, transacted producers and consumers move every message once, and a node that stops and returns is bridged to again. Each two-node suite (discovery, core bridges, cluster connections, grouping, core federation, AMQP broker connections, AMQP federation, AMQP bridges and mirroring) passes in all four Artemis/Saranga pairings.

So you can migrate a cluster node by node. Stop one Artemis node, start Saranga on the same instance and data, and wait for the other nodes to bridge to it again; then move on to the next. Saranga reads the node's journal, including its store-and-forward queues, and uses the node ID in its `server.lock`, so the cluster sees the same node come back. See [Migrating from Artemis](migration.md) for moving an instance.

## Managing a cluster

- `listNetworkTopology` on the server control lists the members of every cluster connection.
- `BroadcastGroupControl`, `BridgeControl`, `BrokerConnectionControl` and `ClusterConnectionControl` are registered as in Artemis. Each cluster connection's control shows its settings, the topology, the nodes it is bridged to and its bridges' metrics, and stops and starts it. The server's `ClusterConnectionNames` lists them all.

See [Management](management.md) and [Management differences](../../COMPAT.md#management-differences).
