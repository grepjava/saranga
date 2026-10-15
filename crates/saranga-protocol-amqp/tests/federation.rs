//! AMQP federation between in-process Saranga brokers: one broker
//! connects to the other with a `<federation>`; its local policies take the
//! peer's messages where there is demand here, its remote policies the
//! other way.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol};
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
    let mut config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    config
        .protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig {
            broker_connections: file.broker_connections.clone(),
            ..AmqpConfig::default()
        })));
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

/// Sends `n` messages, each with a connection ID as the JMS client gives
/// them (an address receiver's no-local filter compares it).
async fn send(session: &Session, address: &str, n: u32) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in 0..n {
        let mut m = message::text_message(&format!("m{i}"));
        m.put_string_property("__AMQ_CID", "test-client");
        producer.send(m).await.unwrap();
    }
    producer.close();
}

async fn drain(consumer: &Consumer, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    while out.len() < n {
        let d = consumer
            .receive(WAIT)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("only {} of {n} messages", out.len()));
        consumer.acknowledge(&d).await.unwrap();
        out.push(d.message.text().unwrap().unwrap());
    }
    out
}

const ADDRESSES: &str = r#"<addresses>
    <address name="orders"><anycast><queue name="orders"/></anycast></address>
    <address name="jobs"><anycast><queue name="jobs"/></anycast></address>
    <address name="news"><multicast/></address>
    <address name="alerts"><multicast/></address>
  </addresses>"#;

const FEDERATION: &str = r##"<federation>
    <local-address-policy name="news-policy" max-hops="1"><include address-match="news"/></local-address-policy>
    <local-queue-policy name="orders-policy">
      <include address-match="#" queue-match="orders"/>
      <include address-match="#" queue-match="late"/>
    </local-queue-policy>
    <remote-address-policy name="alerts-policy" max-hops="1"><include address-match="alerts"/></remote-address-policy>
    <remote-queue-policy name="jobs-policy"><include address-match="#" queue-match="jobs"/></remote-queue-policy>
  </federation>"##;

/// The peer, then the broker federating with it.
async fn pair(dirs: &(tempfile::TempDir, tempfile::TempDir)) -> (Node, Node) {
    let lp = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let lc = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pp = lp.local_addr().unwrap().port();
    let peer = start(dirs.0.path(), lp, ADDRESSES).await;
    let connecting = start(
        dirs.1.path(),
        lc,
        &format!(
            r#"{ADDRESSES}
               <broker-connections>
                 <amqp-connection uri="tcp://127.0.0.1:{pp}" name="to-peer" retry-interval="100">
                   {FEDERATION}
                 </amqp-connection>
               </broker-connections>"#
        ),
    )
    .await;
    (peer, connecting)
}

fn dirs() -> (tempfile::TempDir, tempfile::TempDir) {
    (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
}

fn connected(n: &Node) -> bool {
    n.broker
        .broker_connection_registry()
        .get("to-peer")
        .is_some_and(|c| c.is_connected())
}

fn consumers(n: &Node, queue: &str) -> i32 {
    n.broker
        .post_office()
        .queue(queue)
        .map_or(0, |q| q.consumer_count())
}

/// A queue's messages go to the other broker's consumer.
async fn queue_federates(from: &Node, to: &Node, queue: &str) {
    let (_cf, sf) = session(from.port).await;
    send(&sf, queue, 5).await;
    let (_ct, st) = session(to.port).await;
    let consumer = st.create_consumer(queue, None, false).await.unwrap();
    assert_eq!(drain(&consumer, 5).await, ["m0", "m1", "m2", "m3", "m4"]);
    wait_until("the first broker's queue is empty", || {
        from.broker
            .post_office()
            .queue(queue)
            .is_some_and(|q| q.message_count() == 0)
    })
    .await;
    consumer.close().await;
}

/// A subscription on one broker gets what is published to the address
/// on the other.
async fn address_federates(from: &Node, to: &Node, address: &str) {
    let (_ct, st) = session(to.port).await;
    let sub = format!("{address}-sub");
    st.create_queue(
        QueueConfiguration::new(sub.as_str())
            .address(address)
            .routing_type(RoutingType::Multicast),
    )
    .await
    .unwrap();
    // The receiver attaches to the other broker's address.
    let queue = format!(
        "federation.to-peer.policy.{address}-policy.address.{address}.node.{}",
        to.broker.node_id()
    );
    wait_until("the federation binding", || consumers(from, &queue) == 1).await;
    let (_cf, sf) = session(from.port).await;
    send(&sf, address, 5).await;
    let consumer = st.create_consumer(sub.as_str(), None, false).await.unwrap();
    assert_eq!(drain(&consumer, 5).await, ["m0", "m1", "m2", "m3", "m4"]);
    consumer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_queue_policy() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    queue_federates(&peer, &me, "orders").await;
    stop(me).await;
    stop(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_queue_policy() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    queue_federates(&me, &peer, "jobs").await;
    stop(me).await;
    stop(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_address_policy() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    address_federates(&peer, &me, "news").await;
    stop(me).await;
    stop(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_address_policy() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    address_federates(&me, &peer, "alerts").await;
    stop(me).await;
    stop(peer).await;
}

/// A queue the peer lacks: its receiver is refused, and made again once
/// the peer says the queue was added.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_queue_is_federated_once_added() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    let (_cm, sm) = session(me.port).await;
    sm.create_queue(
        QueueConfiguration::new("late")
            .address("late")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    let consumer = sm.create_consumer("late", None, false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_cp, sp) = session(peer.port).await;
    sp.create_queue(
        QueueConfiguration::new("late")
            .address("late")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    wait_until("the receiver on the added queue", || {
        consumers(&peer, "late") == 1
    })
    .await;
    send(&sp, "late", 3).await;
    assert_eq!(drain(&consumer, 3).await, ["m0", "m1", "m2"]);
    stop(me).await;
    stop(peer).await;
}
