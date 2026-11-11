//! The console's export of a large AMQP message gives its body whole and
//! correct. On a broker that took a large message in over AMQP (here a
//! mirror), the message is native and its whole native encoding lives in its
//! file. Export must reconstitute the native message and convert it to Core,
//! so the exported Core body is the message's body, not the native file
//! encoding (descriptors, headers and all).

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
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    session.start().await.unwrap();
    (conn, session)
}

fn count(n: &Node, queue: &str) -> Option<i64> {
    n.broker
        .post_office()
        .queue(queue)
        .map(|q| q.message_count())
}

fn connected(n: &Node, name: &str) -> bool {
    n.broker
        .broker_connection_registry()
        .get(name)
        .is_some_and(|c| c.is_connected())
}

fn mirror_to(port: u16, name: &str) -> String {
    format!(
        r#"<broker-connections>
             <amqp-connection uri="tcp://127.0.0.1:{port}" name="{name}" retry-interval="100">
               <mirror sync="false"/>
             </amqp-connection>
           </broker-connections>"#
    )
}

/// Standard base64 with padding, as `management::export` writes a body.
fn base64(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(A[(n >> 18 & 63) as usize] as char);
        out.push(A[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            A[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// A large message mirrored to a peer is native there, its whole native
/// encoding in its file. Exporting it from the peer gives its body whole: the
/// exported Core body is the message body, not the native file encoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exports_a_native_large_message_body_whole() {
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let b = start(db.path(), lb, "").await;
    let a = start(da.path(), la, &mirror_to(pb, "to-b")).await;
    wait_until("connected", || connected(&a, "to-b")).await;
    let (_ca, sa) = session(a.port).await;
    sa.create_queue(
        QueueConfiguration::new("big")
            .address("big")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    wait_until("the queue mirrored", || count(&b, "big").is_some()).await;

    // Well past the 100 KiB large-message threshold.
    let body: Vec<u8> = (0..300_000).map(|i| (i * 31 % 251) as u8).collect();
    let producer = sa.create_producer(Some("big")).await.unwrap();
    producer.send(message::bytes_message(&body)).await.unwrap();
    // On b the message is native (taken in over the AMQP mirror) and large.
    wait_until("the message mirrored", || count(&b, "big") == Some(1)).await;

    let exported = saranga_server::management::export_messages(&b.broker, "big", None, 10)
        .await
        .unwrap();
    assert_eq!(exported.len(), 1, "one message exported");
    let got = exported[0]
        .get("body")
        .and_then(saranga_buffer::json::Json::as_str)
        .expect("a bytes body");
    assert_eq!(
        got,
        base64(&body),
        "the exported body is the message body, not the native file encoding"
    );

    stop(a).await;
    stop(b).await;
}
