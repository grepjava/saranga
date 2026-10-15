//! AMQP bridges between in-process Saranga brokers: one broker connects to
//! the other with a `<bridge>`; its from policies take the peer's messages
//! into local addresses and queues, its to policies send local ones to the
//! peer.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message;
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

async fn send(session: &Session, address: &str, n: u32) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in 0..n {
        producer
            .send(message::text_message(&format!("m{i}")))
            .await
            .unwrap();
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
    <address name="outbox"><anycast><queue name="outbox"/></anycast></address>
    <address name="inbox"><anycast><queue name="inbox"/></anycast></address>
    <address name="news"><multicast/></address>
    <address name="alerts"><multicast/></address>
  </addresses>
  <address-settings>
    <address-setting match="late">
      <auto-create-queues>false</auto-create-queues>
      <auto-create-addresses>false</auto-create-addresses>
    </address-setting>
  </address-settings>"#;

const BRIDGE: &str = r##"<bridge name="b">
    <bridge-to-queue name="tq" remote-address="inbox">
      <include address-match="#" queue-match="outbox"/>
    </bridge-to-queue>
    <bridge-to-address name="ta"><include address-match="news"/></bridge-to-address>
    <bridge-from-queue name="fq">
      <include address-match="#" queue-match="orders"/>
      <include address-match="#" queue-match="late"/>
    </bridge-from-queue>
    <bridge-from-address name="fa"><include address-match="alerts"/></bridge-from-address>
    <property key="linkRecoveryInitialDelay" value="100"/>
    <property key="linkRecoveryDelay" value="200"/>
  </bridge>"##;

/// The peer, then the broker bridging to it.
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
                   {BRIDGE}
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

/// The queues bound to an address with a consumer.
fn consumed_on(n: &Node, address: &str) -> usize {
    n.broker
        .post_office()
        .queues()
        .iter()
        .filter(|q| q.config().address.to_string() == address && q.consumer_count() > 0)
        .count()
}

async fn subscribe(port: u16, address: &str, sub: &str) -> (Connection, Session) {
    let (c, s) = session(port).await;
    s.create_queue(
        QueueConfiguration::new(sub)
            .address(address)
            .routing_type(RoutingType::Multicast),
    )
    .await
    .unwrap();
    (c, s)
}

/// A local queue's messages go to the peer's `inbox`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn to_queue() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    wait_until("the sender consumes outbox", || {
        consumers(&me, "outbox") == 1
    })
    .await;
    let (_cm, sm) = session(me.port).await;
    send(&sm, "outbox", 5).await;
    let (_cp, sp) = session(peer.port).await;
    let consumer = sp.create_consumer("inbox", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 5).await, ["m0", "m1", "m2", "m3", "m4"]);
    stop(me).await;
    stop(peer).await;
}

/// A local address's messages go to the peer's address, through a
/// temporary subscription here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn to_address() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    let (_cp, sp) = subscribe(peer.port, "news", "news-sub").await;
    wait_until("connected", || connected(&me)).await;
    wait_until("the sender's subscription", || {
        consumed_on(&me, "news") == 1
    })
    .await;
    let (_cm, sm) = session(me.port).await;
    send(&sm, "news", 3).await;
    let consumer = sp.create_consumer("news-sub", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 3).await, ["m0", "m1", "m2"]);
    // The subscription goes with the connection.
    stop(peer).await;
    wait_until("the subscription removed", || {
        me.broker
            .post_office()
            .queues()
            .iter()
            .all(|q| !q.name().to_string().starts_with("amqp-bridge-b-policy-ta"))
    })
    .await;
    stop(me).await;
}

/// A consumer here makes demand on the peer's queue of the same name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn from_queue() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    let (_cp, sp) = session(peer.port).await;
    send(&sp, "orders", 4).await;
    // No demand yet: nothing moves.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(consumers(&peer, "orders"), 0);
    let (_cm, sm) = session(me.port).await;
    let consumer = sm.create_consumer("orders", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 4).await, ["m0", "m1", "m2", "m3"]);
    // The demand goes with the consumer: the receiver is stopped.
    consumer.close().await;
    send(&sp, "orders", 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        peer.broker
            .post_office()
            .queue("orders")
            .unwrap()
            .message_count(),
        2
    );
    stop(me).await;
    stop(peer).await;
}

/// A subscription here makes demand on the peer's address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn from_address() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    let (_cm, sm) = subscribe(me.port, "alerts", "alerts-sub").await;
    wait_until("the receiver's subscription", || {
        consumed_on(&peer, "alerts") == 1
    })
    .await;
    let (_cp, sp) = session(peer.port).await;
    send(&sp, "alerts", 3).await;
    let consumer = sm.create_consumer("alerts-sub", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 3).await, ["m0", "m1", "m2"]);
    stop(me).await;
    stop(peer).await;
}

/// A receiver the peer refuses (no such queue there) is attached again
/// after the recovery delay, and takes the queue once it is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_link_is_recovered() {
    let d = dirs();
    let (peer, me) = pair(&d).await;
    wait_until("connected", || connected(&me)).await;
    let (_cm, sm) = session(me.port).await;
    sm.create_address("late", &[RoutingType::Anycast], false)
        .await
        .unwrap();
    sm.create_queue(
        QueueConfiguration::new("late")
            .address("late")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    let consumer = sm.create_consumer("late", None, false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Refused, not the connection's failure.
    assert!(connected(&me));
    let (_cp, sp) = session(peer.port).await;
    sp.create_address("late", &[RoutingType::Anycast], false)
        .await
        .unwrap();
    sp.create_queue(
        QueueConfiguration::new("late")
            .address("late")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    wait_until("the receiver recovered", || consumers(&peer, "late") == 1).await;
    send(&sp, "late", 3).await;
    assert_eq!(drain(&consumer, 3).await, ["m0", "m1", "m2"]);
    stop(me).await;
    stop(peer).await;
}
