//! Cluster connections between in-process Saranga brokers: each node
//! bridges to the other through a store-and-forward queue, learns the
//! other's queues and consumers as remote bindings, and routes messages
//! among local and remote queues by the message load balancing.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

struct Node {
    broker: Arc<Broker>,
    port: u16,
    acceptor: JoinHandle<std::io::Result<()>>,
}

fn cluster_config(me: u16, other: u16, balancing: &str, addresses: &str, extra: &str) -> String {
    format!(
        r#"<connectors>
             <connector name="me">tcp://127.0.0.1:{me}</connector>
             <connector name="other">tcp://127.0.0.1:{other}</connector>
           </connectors>
           <cluster-user>cluster</cluster-user>
           <cluster-password>secret</cluster-password>
           <cluster-connections>
             <cluster-connection name="my-cluster">
               <connector-ref>me</connector-ref>
               <retry-interval>100</retry-interval>
               <message-load-balancing>{balancing}</message-load-balancing>
               <max-hops>1</max-hops>
               <static-connectors><connector-ref>other</connector-ref></static-connectors>
             </cluster-connection>
           </cluster-connections>
           <addresses>{addresses}</addresses>
           {extra}"#
    )
}

async fn start(dir: &std::path::Path, listener: TcpListener, core: &str) -> Node {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
             {core}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    Node {
        broker,
        port,
        acceptor,
    }
}

async fn stop(n: Node) {
    n.acceptor.abort();
    let _ = n.acceptor.await;
    for c in n.broker.registry().connections() {
        c.disconnect();
    }
    n.broker.stop().unwrap();
}

/// Two nodes, each with the other as its static connector.
async fn pair(
    dirs: &(tempfile::TempDir, tempfile::TempDir),
    balancing: &str,
    addresses: &str,
) -> (Node, Node) {
    pair_with(dirs, balancing, addresses, ("", "")).await
}

/// Two nodes with more configuration each.
async fn pair_with(
    dirs: &(tempfile::TempDir, tempfile::TempDir),
    balancing: &str,
    addresses: &str,
    extra: (&str, &str),
) -> (Node, Node) {
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let (pa, pb) = (
        la.local_addr().unwrap().port(),
        lb.local_addr().unwrap().port(),
    );
    let a = start(
        dirs.0.path(),
        la,
        &cluster_config(pa, pb, balancing, addresses, extra.0),
    )
    .await;
    let b = start(
        dirs.1.path(),
        lb,
        &cluster_config(pb, pa, balancing, addresses, extra.1),
    )
    .await;
    (a, b)
}

fn dirs() -> (tempfile::TempDir, tempfile::TempDir) {
    (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn session(port: u16) -> (Connection, Session) {
    let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions {
            ack_batch_size: 0,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    session.start().await.unwrap();
    (conn, session)
}

async fn send(session: &Session, address: &str, n: u32, routing_type: Option<u8>) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in 0..n {
        let mut m = message::text_message(&format!("m{i}"));
        if let Some(t) = routing_type {
            m.put_property("_AMQ_ROUTING_TYPE", PropertyValue::Byte(t as i8));
        }
        producer.send(m).await.unwrap();
    }
    producer.close();
}

async fn drain(consumer: &Consumer, n: usize) -> Vec<CoreMessage> {
    let mut out = Vec::new();
    while out.len() < n {
        let d = consumer
            .receive(WAIT)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("only {} of {n} messages", out.len()));
        consumer.acknowledge(&d).await.unwrap();
        out.push(d.message);
    }
    out
}

fn remote(node: &Node, address: &str) -> Vec<Arc<saranga_server::remote_binding::RemoteBinding>> {
    node.broker.post_office().remote_bindings(address)
}

const ORDERS: &str =
    r#"<address name="orders"><anycast><queue name="orders"/></anycast></address>"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_demand_sends_messages_to_the_node_with_consumers() {
    let d = dirs();
    let (a, b) = pair(&d, "ON_DEMAND", ORDERS).await;
    wait_until("both nodes bind the other's queue", || {
        remote(&a, "orders").len() == 1 && remote(&b, "orders").len() == 1
    })
    .await;
    let a_id = a.broker.node_id().to_owned();
    let b_id = b.broker.node_id().to_owned();
    let binding = &remote(&a, "orders")[0];
    assert_eq!(&**binding.unique_name(), format!("orders{b_id}"));
    assert_eq!(&**binding.routing_name(), "orders");
    assert_eq!(
        a.broker
            .cluster()
            .unwrap()
            .flow_records()
            .iter()
            .map(|r| r.target_node_id().to_owned())
            .collect::<Vec<_>>(),
        vec![b_id.clone()]
    );
    assert!(
        a.broker
            .post_office()
            .queue(&format!("$.artemis.internal.sf.my-cluster.{b_id}"))
            .is_some()
    );
    assert!(
        b.broker
            .post_office()
            .queue(&format!("$.artemis.internal.sf.my-cluster.{a_id}"))
            .is_some()
    );

    let (_cb, sb) = session(b.port).await;
    let consumer = sb.create_consumer("orders", None, false).await.unwrap();
    wait_until("A hears of B's consumer", || {
        remote(&a, "orders")[0].consumer_count() == 1
    })
    .await;

    let (_ca, sa) = session(a.port).await;
    send(&sa, "orders", 10, None).await;
    let got = drain(&consumer, 10).await;
    let texts: Vec<String> = got.iter().map(|m| m.text().unwrap().unwrap()).collect();
    assert_eq!(texts, (0..10).map(|i| format!("m{i}")).collect::<Vec<_>>());
    // None carries the routing context on.
    assert!(got.iter().all(|m| {
        !m.properties
            .entries()
            .iter()
            .any(|(k, _)| k.to_string_lossy().starts_with("_AMQ_ROUTE_TO"))
    }));
    assert_eq!(
        a.broker
            .post_office()
            .queue("orders")
            .unwrap()
            .message_count(),
        0
    );
    consumer.close().await;
    wait_until("A hears B's consumer went", || {
        remote(&a, "orders")[0].consumer_count() == 0
    })
    .await;
    stop(a).await;
    stop(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_balances_round_robin_without_consumers() {
    let d = dirs();
    let (a, b) = pair(&d, "STRICT", ORDERS).await;
    wait_until("A binds B's queue", || remote(&a, "orders").len() == 1).await;
    let (_ca, sa) = session(a.port).await;
    send(&sa, "orders", 10, None).await;
    let qa = a.broker.post_office().queue("orders").unwrap();
    let qb = b.broker.post_office().queue("orders").unwrap();
    wait_until("half reach B", || qb.message_count() == 5).await;
    assert_eq!(qa.message_count(), 5);
    stop(a).await;
    stop(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn off_keeps_anycast_messages_local_but_not_multicast_ones() {
    let d = dirs();
    let addresses = format!(
        r#"{ORDERS}<address name="news"><multicast><queue name="sub"/></multicast></address>"#
    );
    let (a, b) = pair(&d, "OFF", &addresses).await;
    wait_until("A binds B's queues", || {
        remote(&a, "orders").len() == 1 && remote(&a, "news").len() == 1
    })
    .await;
    let (_cb, sb) = session(b.port).await;
    let consumer = sb.create_consumer("orders", None, false).await.unwrap();
    wait_until("A hears of B's consumer", || {
        remote(&a, "orders")[0].consumer_count() == 1
    })
    .await;
    let (_ca, sa) = session(a.port).await;
    send(&sa, "orders", 4, Some(1)).await;
    let qa = a.broker.post_office().queue("orders").unwrap();
    wait_until("the anycast messages stay on A", || qa.message_count() == 4).await;
    assert!(consumer.receive_immediate().await.unwrap().is_none());
    // The queue "sub" on both nodes shares one routing name: a multicast
    // message goes to one of them.
    send(&sa, "news", 4, Some(0)).await;
    let (sub_a, sub_b) = (
        a.broker.post_office().queue("sub").unwrap(),
        b.broker.post_office().queue("sub").unwrap(),
    );
    wait_until("the multicast messages are shared out", || {
        sub_a.message_count() + sub_b.message_count() == 4
    })
    .await;
    consumer.close().await;
    stop(a).await;
    stop(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_stops_takes_its_bindings_with_it() {
    let d = dirs();
    let (a, b) = pair(&d, "ON_DEMAND", ORDERS).await;
    wait_until("A binds B's queue", || remote(&a, "orders").len() == 1).await;
    let b_id = b.broker.node_id().to_owned();
    stop(b).await;
    wait_until("A drops B's binding", || remote(&a, "orders").is_empty()).await;
    wait_until("B leaves A's topology", || {
        a.broker.cluster().unwrap().member(&b_id).is_none()
    })
    .await;
    stop(a).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_locator_balances_its_connections_across_the_topology() {
    let d = dirs();
    let (a, b) = pair(&d, "ON_DEMAND", ORDERS).await;
    wait_until("the nodes bridge to each other", || {
        remote(&a, "orders").len() == 1 && remote(&b, "orders").len() == 1
    })
    .await;
    let locator = Locator::new(
        vec![Connector::new("127.0.0.1", a.port)],
        ConnectionOptions {
            ha: true,
            ..ConnectionOptions::default()
        },
    );
    // The first connection goes to the initial connector and brings the
    // topology.
    let first = locator.connect().await.unwrap();
    assert_eq!(first.connector().unwrap().port, a.port);
    assert_eq!(locator.topology().len(), 2);
    let mut ports = Vec::new();
    let mut conns = Vec::new();
    for _ in 0..4 {
        let c = locator.connect().await.unwrap();
        ports.push(c.connector().unwrap().port);
        conns.push(c);
    }
    // Round robin over the two nodes, from wherever it started.
    assert_ne!(ports[0], ports[1]);
    assert_eq!(ports[0], ports[2]);
    assert_eq!(ports[1], ports[3]);
    for c in conns {
        c.close().await;
    }
    first.close().await;
    stop(a).await;
    stop(b).await;
}

const REDISTRIBUTE: &str = r##"<address-settings>
     <address-setting match="#"><redistribution-delay>0</redistribution-delay></address-setting>
   </address-settings>"##;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_without_consumers_go_to_a_node_that_has_them() {
    let d = dirs();
    let (a, b) = pair_with(&d, "ON_DEMAND", ORDERS, (REDISTRIBUTE, REDISTRIBUTE)).await;
    wait_until("both nodes bind the other's queue", || {
        remote(&a, "orders").len() == 1 && remote(&b, "orders").len() == 1
    })
    .await;
    // Nobody consumes yet: the messages stay where they were sent.
    let (_ca, sa) = session(a.port).await;
    send(&sa, "orders", 10, None).await;
    let qa = a.broker.post_office().queue("orders").unwrap();
    wait_until("A holds the messages", || qa.message_count() == 10).await;
    // A consumer on B: A's queue redistributes them all.
    let (_cb, sb) = session(b.port).await;
    let consumer = sb.create_consumer("orders", None, false).await.unwrap();
    let got = drain(&consumer, 10).await;
    let texts: Vec<String> = got.iter().map(|m| m.text().unwrap().unwrap()).collect();
    assert_eq!(texts, (0..10).map(|i| format!("m{i}")).collect::<Vec<_>>());
    assert!(got.iter().all(|m| {
        !m.properties
            .entries()
            .iter()
            .any(|(k, _)| k.to_string_lossy().starts_with("_AMQ_ROUTE_TO"))
    }));
    wait_until("A's queue is empty", || qa.message_count() == 0).await;
    consumer.close().await;
    stop(a).await;
    stop(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_whose_last_consumer_closes_redistributes_after_the_delay() {
    let d = dirs();
    let delayed = r##"<address-settings>
         <address-setting match="#"><redistribution-delay>500</redistribution-delay></address-setting>
       </address-settings>"##;
    let (a, b) = pair_with(&d, "OFF_WITH_REDISTRIBUTION", ORDERS, (delayed, delayed)).await;
    wait_until("both nodes bind the other's queue", || {
        remote(&a, "orders").len() == 1 && remote(&b, "orders").len() == 1
    })
    .await;
    let (_cb, sb) = session(b.port).await;
    let on_b = sb.create_consumer("orders", None, false).await.unwrap();
    let (_ca, sa) = session(a.port).await;
    let on_a = sa.create_consumer("orders", None, false).await.unwrap();
    wait_until("A hears of B's consumer", || {
        remote(&a, "orders")[0].consumer_count() == 1
    })
    .await;
    // OFF_WITH_REDISTRIBUTION keeps what is sent to A on A, while A has a
    // consumer.
    on_a.close().await;
    send(&sa, "orders", 5, None).await;
    let qa = a.broker.post_office().queue("orders").unwrap();
    wait_until("A holds the messages", || qa.message_count() == 5).await;
    let started = tokio::time::Instant::now();
    let got = drain(&on_b, 5).await;
    assert_eq!(got.len(), 5);
    assert!(started.elapsed() >= Duration::from_millis(300));
    on_b.close().await;
    stop(a).await;
    stop(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_never_redistributes() {
    let d = dirs();
    let (a, b) = pair_with(&d, "STRICT", ORDERS, (REDISTRIBUTE, REDISTRIBUTE)).await;
    wait_until("A binds B's queue", || remote(&a, "orders").len() == 1).await;
    let qa = a.broker.post_office().queue("orders").unwrap();
    let (_ca, sa) = session(a.port).await;
    send(&sa, "orders", 4, None).await;
    wait_until("half stay on A", || qa.message_count() == 2).await;
    let (_cb, sb) = session(b.port).await;
    let consumer = sb.create_consumer("orders", None, false).await.unwrap();
    drain(&consumer, 2).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(consumer.receive_immediate().await.unwrap().is_none());
    assert_eq!(qa.message_count(), 2);
    consumer.close().await;
    stop(a).await;
    stop(b).await;
}

fn grouping_handler(kind: &str) -> String {
    format!(
        r#"<grouping-handler name="my-grouping-handler">
             <type>{kind}</type>
             <address>orders</address>
             <timeout>5000</timeout>
           </grouping-handler>"#
    )
}

/// Sends `n` messages to `address`, each of group `g<i % groups>`.
async fn send_grouped(session: &Session, address: &str, n: u32, groups: u32) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in 0..n {
        let mut m = message::text_message(&format!("m{i}"));
        m.put_property(
            "_AMQ_GROUP_ID",
            PropertyValue::String(format!("g{}", i % groups).as_str().into()),
        );
        producer.send(m).await.unwrap();
    }
    producer.close();
}

/// The group of each message the consumers get, by consumer, until `n`
/// came.
async fn groups_received(consumers: &[&Consumer], n: usize) -> Vec<Vec<String>> {
    let mut out = vec![Vec::new(); consumers.len()];
    let deadline = tokio::time::Instant::now() + WAIT;
    while out.iter().map(Vec::len).sum::<usize>() < n {
        assert!(tokio::time::Instant::now() < deadline, "only {out:?}");
        for (i, c) in consumers.iter().enumerate() {
            if let Some(d) = c.receive(Duration::from_millis(50)).await.unwrap() {
                c.acknowledge(&d).await.unwrap();
                let g = match d.message.properties.get(&"_AMQ_GROUP_ID".into()) {
                    Some(PropertyValue::String(s)) => s.to_string_lossy(),
                    other => panic!("no group: {other:?}"),
                };
                out[i].push(g);
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_goes_to_one_node_whichever_node_it_is_sent_to() {
    let d = dirs();
    let (local, remote_handler) = (grouping_handler("LOCAL"), grouping_handler("REMOTE"));
    let (a, b) = pair_with(&d, "ON_DEMAND", ORDERS, (&local, &remote_handler)).await;
    wait_until("both nodes bind the other's queue", || {
        remote(&a, "orders").len() == 1 && remote(&b, "orders").len() == 1
    })
    .await;
    let (_ca, sa) = session(a.port).await;
    let (_cb, sb) = session(b.port).await;
    let on_a = sa.create_consumer("orders", None, false).await.unwrap();
    let on_b = sb.create_consumer("orders", None, false).await.unwrap();
    wait_until("each node hears of the other's consumer", || {
        remote(&a, "orders")[0].consumer_count() == 1
            && remote(&b, "orders")[0].consumer_count() == 1
    })
    .await;
    // Sent to B, which asks A where each group goes; then to A.
    send_grouped(&sb, "orders", 20, 4).await;
    send_grouped(&sa, "orders", 20, 4).await;
    let got = groups_received(&[&on_a, &on_b], 40).await;
    let mut seen = std::collections::HashMap::new();
    for (consumer, groups) in got.iter().enumerate() {
        for g in groups {
            assert_eq!(
                *seen.entry(g.clone()).or_insert(consumer),
                consumer,
                "group {g} went to both consumers: {got:?}"
            );
        }
    }
    assert_eq!(seen.len(), 4);
    assert!(
        got.iter().all(|g| !g.is_empty()),
        "one consumer got nothing: {got:?}"
    );
    let handler = a.broker.post_office().grouping().unwrap().clone();
    assert!(handler.is_local());
    for g in 0..4 {
        assert!(
            handler
                .get_proposal(&format!("g{g}.orders"), false)
                .is_some()
        );
    }
    on_a.close().await;
    on_b.close().await;
    stop(a).await;
    stop(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_are_journaled_and_reaped_when_unused() {
    let dir = tempfile::tempdir().unwrap();
    let core = format!(
        r#"<grouping-handler name="h">
             <type>LOCAL</type>
             <address>orders</address>
             <group-timeout>600</group-timeout>
             <reaper-period>200</reaper-period>
           </grouping-handler>
           <addresses>{ORDERS}</addresses>"#
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let n = start(dir.path(), listener, &core).await;
    let (conn, s) = session(n.port).await;
    send_grouped(&s, "orders", 2, 2).await;
    conn.close().await;
    let handler = n.broker.post_office().grouping().unwrap().clone();
    let cluster = format!("orders{}", n.broker.node_id());
    assert_eq!(
        handler
            .get_proposal("g0.orders", false)
            .unwrap()
            .cluster_name,
        cluster
    );
    stop(n).await;

    // Recovered from the journal, then reaped once unused.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let n = start(dir.path(), listener, &core).await;
    let handler = n.broker.post_office().grouping().unwrap().clone();
    assert!(handler.get_proposal("g1.orders", false).is_some());
    wait_until("the groups are reaped", || {
        handler.get_proposal("g0.orders", false).is_none()
            && handler.get_proposal("g1.orders", false).is_none()
    })
    .await;
    stop(n).await;

    // And their records are gone.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let n = start(dir.path(), listener, &core).await;
    let handler = n.broker.post_office().grouping().unwrap().clone();
    assert!(handler.get_proposal("g0.orders", false).is_none());
    stop(n).await;
}
