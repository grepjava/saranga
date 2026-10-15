//! AMQP broker connections between in-process Saranga brokers: one
//! broker connects to the other, sending its queues' messages there and
//! taking the other's into its own.

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
    <address name="out"><anycast><queue name="out"/></anycast></address>
    <address name="in"><anycast><queue name="in"/></anycast></address>
    <address name="later"><anycast/></address>
  </addresses>"#;

/// The peer, then the broker connecting to it with `elements`.
async fn pair(dirs: &(tempfile::TempDir, tempfile::TempDir), elements: &str) -> (Node, Node) {
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
                   {elements}
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

fn connection(n: &Node) -> Arc<dyn saranga_server::broker_connection::BrokerConnection> {
    n.broker
        .broker_connection_registry()
        .get("to-peer")
        .expect("registered")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_sends_a_queues_messages_to_the_peer() {
    let d = dirs();
    let (peer, me) = pair(&d, r#"<sender address-match="out"/>"#).await;
    wait_until("connected", || connection(&me).is_connected()).await;
    let (_cm, sm) = session(me.port).await;
    send(&sm, "out", 5).await;
    let (_cp, sp) = session(peer.port).await;
    let consumer = sp.create_consumer("out", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 5).await, ["m0", "m1", "m2", "m3", "m4"]);
    // Each was acknowledged here once the peer took it.
    wait_until("the queue here is empty", || {
        me.broker
            .post_office()
            .queue("out")
            .is_some_and(|q| q.message_count() == 0)
    })
    .await;
    stop(me).await;
    stop(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receiver_takes_the_peers_messages() {
    let d = dirs();
    let (peer, me) = pair(&d, r#"<receiver queue-name="in"/>"#).await;
    // The peer's queue has the broker connection's consumer.
    wait_until("the peer's queue has a consumer", || {
        peer.broker
            .post_office()
            .queue("in")
            .is_some_and(|q| q.consumer_count() == 1)
    })
    .await;
    let (_cp, sp) = session(peer.port).await;
    send(&sp, "in", 3).await;
    let (_cm, sm) = session(me.port).await;
    let consumer = sm.create_consumer("in", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 3).await, ["m0", "m1", "m2"]);
    stop(me).await;
    stop(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_made_later_gets_its_link_and_stop_and_start_reconnect() {
    let d = dirs();
    let (peer, me) = pair(&d, r#"<receiver address-match="later"/>"#).await;
    wait_until("connected", || connection(&me).is_connected()).await;
    let (_cp, sp) = session(peer.port).await;
    sp.create_queue(
        QueueConfiguration::new("later")
            .address("later")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    let (_cm, sm) = session(me.port).await;
    sm.create_queue(
        QueueConfiguration::new("later")
            .address("later")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    wait_until("the peer's queue has a consumer", || {
        peer.broker
            .post_office()
            .queue("later")
            .is_some_and(|q| q.consumer_count() == 1)
    })
    .await;
    send(&sp, "later", 2).await;
    let consumer = sm.create_consumer("later", None, false).await.unwrap();
    assert_eq!(drain(&consumer, 2).await, ["m0", "m1"]);
    consumer.close().await;

    // Stopped through management, the connection goes; started, it comes
    // back.
    let c = connection(&me);
    c.stop();
    assert!(!c.is_started());
    wait_until("the peer's queue lost its consumer", || {
        peer.broker
            .post_office()
            .queue("later")
            .is_some_and(|q| q.consumer_count() == 0)
    })
    .await;
    c.start();
    wait_until("the peer's queue has a consumer again", || {
        peer.broker
            .post_office()
            .queue("later")
            .is_some_and(|q| q.consumer_count() == 1)
    })
    .await;
    stop(me).await;
    stop(peer).await;
}
