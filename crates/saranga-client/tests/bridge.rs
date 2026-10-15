//! Core bridges between in-process Saranga brokers: a bridge forwards what
//! reaches its queue to another broker, acknowledging each message once
//! the other broker has it, and connects again when that broker comes back.
//! And the confirmation window the bridge's session relies on.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

struct Running {
    broker: Arc<Broker>,
    acceptor: JoinHandle<std::io::Result<()>>,
}

async fn start(dir: &std::path::Path, port: u16, core: &str) -> (Running, u16) {
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
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    (Running { broker, acceptor }, port)
}

async fn stop(r: Running) {
    r.acceptor.abort();
    let _ = r.acceptor.await;
    for c in r.broker.registry().connections() {
        c.disconnect();
    }
    for _ in 0..200 {
        if r.broker.registry().all_closed() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    r.broker.stop().unwrap();
}

const QUEUE: &str = r#"<addresses>
     <address name="dst"><anycast><queue name="dst"/></anycast></address>
     <address name="src"><anycast><queue name="src"/></anycast></address>
   </addresses>"#;

fn bridged(target_port: u16, extra: &str) -> String {
    format!(
        r#"<connectors><connector name="target">tcp://127.0.0.1:{target_port}</connector></connectors>
           {QUEUE}
           <bridges>
             <bridge name="br">
               <queue-name>src</queue-name>
               <forwarding-address>dst</forwarding-address>
               <retry-interval>100</retry-interval>
               {extra}
               <static-connectors><connector-ref>target</connector-ref></static-connectors>
             </bridge>
           </bridges>"#
    )
}

async fn connect(port: u16) -> (Connection, Session) {
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
    (conn, session)
}

async fn send(session: &Session, address: &str, from: u32, to: u32) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in from..to {
        let mut m = message::text_message(&format!("m{i}"));
        m.durable = true;
        producer.send(m).await.unwrap();
    }
    producer.close();
}

async fn receive(session: &Session, queue: &str, n: usize) -> Vec<CoreMessage> {
    let consumer = session.create_consumer(queue, None, false).await.unwrap();
    session.start().await.unwrap();
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
    assert!(consumer.receive_immediate().await.unwrap().is_none());
    consumer.close().await;
    out
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..(WAIT.as_millis() / 20) {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bridge_forwards_and_reconnects() {
    let target_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let (target, target_port) = start(target_dir.path(), 0, QUEUE).await;
    let (source, source_port) = start(source_dir.path(), 0, &bridged(target_port, "")).await;
    let bridge = source.broker.bridges().get("br").unwrap();
    wait_until("the bridge to connect", || bridge.is_connected()).await;

    let (src_conn, src) = connect(source_port).await;
    send(&src, "src", 0, 10).await;
    let (dst_conn, dst) = connect(target_port).await;
    let got = receive(&dst, "dst", 10).await;
    let texts: Vec<String> = got.iter().map(|m| m.text().unwrap().unwrap()).collect();
    assert_eq!(texts, (0..10).map(|i| format!("m{i}")).collect::<Vec<_>>());
    // The target takes the bridge's duplicate ID off each message and keeps
    // it in the address's bridge cache.
    for m in &got {
        assert_eq!(m.address.as_ref().unwrap().to_string_lossy(), "dst");
        assert!(m.property("_AMQ_BRIDGE_DUP").is_none());
    }
    let po = target.broker.post_office();
    assert!(po.duplicate_cache_exists("BRIDGE.dst"));
    assert_eq!(
        saranga_server::duplicate::lock(&po.duplicate_cache("BRIDGE.dst")).len(),
        10
    );
    let src_queue = source.broker.post_office().queue("src").unwrap();
    wait_until("the source queue to empty", || {
        src_queue.message_count() == 0
    })
    .await;
    wait_until("ten acknowledgements", || {
        bridge.metrics().messages_acknowledged() == 10
    })
    .await;
    assert_eq!(bridge.metrics().messages_pending_acknowledgement(), 10);
    dst_conn.close().await;

    // The target goes: messages wait on the source queue.
    stop(target).await;
    wait_until("the bridge to notice", || !bridge.is_connected()).await;
    send(&src, "src", 10, 15).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(src_queue.message_count(), 5);

    // It comes back on its port: the bridge connects again and forwards.
    let (target, _) = start(target_dir.path(), target_port, QUEUE).await;
    wait_until("the bridge to reconnect", || bridge.is_connected()).await;
    let (dst_conn, dst) = connect(target_port).await;
    let got = receive(&dst, "dst", 5).await;
    let texts: Vec<String> = got.iter().map(|m| m.text().unwrap().unwrap()).collect();
    assert_eq!(texts, (10..15).map(|i| format!("m{i}")).collect::<Vec<_>>());
    wait_until("the source queue to empty", || {
        src_queue.message_count() == 0
    })
    .await;

    dst_conn.close().await;
    src_conn.close().await;
    bridge.stop().await.unwrap();
    assert!(!bridge.is_started());
    stop(source).await;
    stop(target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bridge_keeps_the_address_and_filters() {
    let target_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let (target, target_port) = start(target_dir.path(), 0, QUEUE).await;
    // No forwarding address: each message keeps its own (here "src", which
    // the target has too). Only odd messages pass the filter; no duplicate
    // ID without duplicate detection.
    let core = format!(
        r#"<connectors><connector name="target">tcp://127.0.0.1:{target_port}</connector></connectors>
           {QUEUE}
           <bridges>
             <bridge name="br">
               <queue-name>src</queue-name>
               <filter string="odd = true"/>
               <use-duplicate-detection>false</use-duplicate-detection>
               <static-connectors><connector-ref>target</connector-ref></static-connectors>
             </bridge>
           </bridges>"#
    );
    let (source, source_port) = start(source_dir.path(), 0, &core).await;
    let bridge = source.broker.bridges().get("br").unwrap();
    wait_until("the bridge to connect", || bridge.is_connected()).await;
    let (src_conn, src) = connect(source_port).await;
    let producer = src.create_producer(Some("src")).await.unwrap();
    for i in 0..6 {
        let mut m = message::text_message(&format!("m{i}"));
        m.put_property("odd", PropertyValue::Bool(i % 2 == 1));
        producer.send(m).await.unwrap();
    }
    let (dst_conn, dst) = connect(target_port).await;
    let got = receive(&dst, "src", 3).await;
    let texts: Vec<String> = got.iter().map(|m| m.text().unwrap().unwrap()).collect();
    assert_eq!(texts, ["m1", "m3", "m5"]);
    assert!(got.iter().all(|m| m.property("_AMQ_BRIDGE_DUP").is_none()));
    // The even ones stay behind.
    let src_queue = source.broker.post_office().queue("src").unwrap();
    wait_until("three left", || src_queue.message_count() == 3).await;
    dst_conn.close().await;
    src_conn.close().await;
    stop(source).await;
    stop(target).await;
}

/// A session with a confirmation window hears of every send once the
/// broker has it, a send refused by the broker included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_confirmation_window_acknowledges_sends() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), 0, QUEUE).await;
    let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
        .await
        .unwrap();
    struct Handler {
        acked: Mutex<Vec<String>>,
        failed: AtomicUsize,
    }
    impl SendAcknowledgementHandler for Handler {
        fn send_acknowledged(&self, message: &CoreMessage) {
            self.acked
                .lock()
                .unwrap()
                .push(message.text().ok().flatten().unwrap_or_default());
        }
        fn send_failed(&self, _: &CoreMessage, _: &Error) {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
    }
    let handler = Arc::new(Handler {
        acked: Mutex::new(Vec::new()),
        failed: AtomicUsize::new(0),
    });
    // Without a window, no handler.
    let plain = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    assert!(
        plain
            .set_send_acknowledgement_handler(handler.clone())
            .is_err()
    );
    let session = conn
        .create_session(SessionOptions {
            confirmation_window_size: 1024,
            block_on_durable_send: false,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    session
        .set_send_acknowledgement_handler(handler.clone())
        .unwrap();
    let producer = session.create_producer(Some("dst")).await.unwrap();
    for i in 0..20 {
        let mut m = message::text_message(&format!("m{i}"));
        m.durable = true;
        producer.send(m).await.unwrap();
    }
    // A large message: acknowledged once, after its last chunk.
    let mut big = message::bytes_message(&vec![7u8; 300 * 1024]);
    big.durable = true;
    producer.send(big).await.unwrap();
    wait_until("21 acknowledgements", || {
        handler.acked.lock().unwrap().len() == 21
    })
    .await;
    let acked = handler.acked.lock().unwrap().clone();
    assert_eq!(
        acked[..20],
        (0..20).map(|i| format!("m{i}")).collect::<Vec<_>>()[..]
    );
    assert_eq!(handler.failed.load(Ordering::SeqCst), 0);
    assert_eq!(
        broker
            .broker
            .post_office()
            .queue("dst")
            .unwrap()
            .message_count(),
        21
    );
    conn.close().await;
    stop(broker).await;
}

async fn invoke(broker: &Arc<Broker>, resource: &str, op: &str, params: &[Json]) -> Json {
    broker
        .management()
        .invoke(broker, resource, op, params, Default::default())
        .await
        .unwrap_or_else(|e| panic!("{op}: {e}"))
}

async fn attribute(broker: &Arc<Broker>, resource: &str, name: &str) -> Json {
    broker
        .management()
        .attribute(broker, resource, name, Default::default())
        .await
        .unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn names(broker: &Broker) -> Vec<String> {
    let mut n = broker.bridges().names();
    n.sort();
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_deploys_and_destroys_journaled_bridges() {
    let target_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let (target, target_port) = start(target_dir.path(), 0, QUEUE).await;
    let core = format!(
        r#"<connectors><connector name="target">tcp://127.0.0.1:{target_port}</connector></connectors>{QUEUE}"#
    );
    let (source, source_port) = start(source_dir.path(), 0, &core).await;
    let b = &source.broker;
    invoke(
        b,
        "broker",
        "createBridge",
        &[Json::from(
            r#"{"name":"mb","queue-name":"src","forwarding-address":"dst","static-connectors":["target"],"retry-interval":100}"#,
        )],
    )
    .await;
    assert_eq!(
        attribute(b, "broker", "BridgeNames").await,
        Json::Array(vec![Json::from("mb")])
    );
    let bridge = b.bridges().get("mb").unwrap();
    assert!(!bridge.config().configuration_managed);
    wait_until("the bridge to connect", || bridge.is_connected()).await;
    let r = "bridge.mb";
    assert_eq!(attribute(b, r, "QueueName").await, Json::from("src"));
    assert_eq!(
        attribute(b, r, "ForwardingAddress").await,
        Json::from("dst")
    );
    assert_eq!(
        attribute(b, r, "StaticConnectors").await,
        Json::Array(vec![Json::from("target")])
    );
    assert_eq!(attribute(b, r, "RetryInterval").await, Json::from(100i64));
    assert_eq!(
        attribute(b, r, "UseDuplicateDetection").await,
        Json::from(true)
    );
    assert_eq!(attribute(b, r, "HA").await, Json::from(false));
    assert_eq!(attribute(b, r, "Connected").await, Json::from(true));
    assert_eq!(attribute(b, r, "Started").await, Json::from(true));
    assert_eq!(attribute(b, r, "TransformerClassName").await, Json::Null);
    assert_eq!(
        attribute(b, r, "TransformerPropertiesAsJSON").await,
        Json::from("{}")
    );
    let (src_conn, src) = connect(source_port).await;
    send(&src, "src", 0, 3).await;
    let (dst_conn, dst) = connect(target_port).await;
    assert_eq!(receive(&dst, "dst", 3).await.len(), 3);
    wait_until("three acknowledgements", || {
        bridge.metrics().messages_acknowledged() == 3
    })
    .await;
    let metrics = attribute(b, r, "Metrics").await;
    assert_eq!(metrics.get("messagesAcknowledged"), Some(&Json::from(3i64)));
    assert_eq!(
        metrics.get("messagesPendingAcknowledgement"),
        Some(&Json::from(3i64))
    );

    // stop and start through BridgeControl.
    invoke(b, r, "stop", &[]).await;
    assert_eq!(attribute(b, r, "Started").await, Json::from(false));
    assert_eq!(attribute(b, r, "Connected").await, Json::from(false));
    send(&src, "src", 3, 4).await;
    invoke(b, r, "start", &[]).await;
    assert_eq!(receive(&dst, "dst", 1).await.len(), 1);
    src_conn.close().await;
    dst_conn.close().await;

    // The bridge was journaled: it is deployed again after a restart.
    drop(bridge);
    stop(source).await;
    let (source, _) = start(source_dir.path(), source_port, &core).await;
    let b = &source.broker;
    assert_eq!(names(b), ["mb"]);
    assert!(
        !b.bridges()
            .get("mb")
            .unwrap()
            .config()
            .configuration_managed
    );

    // destroyBridge forgets it, record and all.
    invoke(b, "broker", "destroyBridge", &[Json::from("mb")]).await;
    assert!(names(b).is_empty());
    stop(source).await;
    let (source, _) = start(source_dir.path(), source_port, &core).await;
    let b = &source.broker;
    assert!(names(b).is_empty());

    // The parameter form, with a concurrency of one; a JSON one with two
    // makes two bridges under one name.
    invoke(
        b,
        "broker",
        "createBridge",
        &[
            Json::from("pb"),
            Json::from("src"),
            Json::from("dst"),
            Json::Null,
            Json::Null,
            Json::from(100i64),
            Json::from(1.0),
            Json::from(-1i64),
            Json::from(-1i64),
            Json::from(true),
            Json::from(1024 * 1024i64),
            Json::from(1024 * 1024i64),
            Json::from(30_000i64),
            Json::from("target"),
            Json::from(false),
            Json::from(false),
            Json::Null,
            Json::Null,
        ],
    )
    .await;
    invoke(
        b,
        "broker",
        "createBridge",
        &[Json::from(
            r#"{"name":"cb","queue-name":"src","forwarding-address":"dst","static-connectors":["target"],"concurrency":2}"#,
        )],
    )
    .await;
    assert_eq!(names(b), ["cb-0", "cb-1", "pb"]);
    assert_eq!(b.bridges().get("cb-1").unwrap().config().parent_name, "cb");
    invoke(b, "broker", "destroyBridge", &[Json::from("cb")]).await;
    assert_eq!(names(b), ["pb"]);
    stop(source).await;
    stop(target).await;
}
