//! Reconnection through in-process Saranga brokers: a broker stopped and
//! started again on its journal directory and port, with clients that
//! reconnect (`reconnectAttempts`) and clients that do not.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

struct Running {
    broker: Arc<Broker>,
    acceptor: JoinHandle<std::io::Result<()>>,
}

async fn start(dir: &std::path::Path, port: u16) -> (Running, u16) {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    (Running { broker, acceptor }, port)
}

/// The broker's stop: the acceptor closes, so that no client reconnects
/// to the broker going away, then clients are told the node is going
/// (`DisconnectMessage`) and the journals stop.
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

fn reconnecting() -> ConnectionOptions {
    ConnectionOptions {
        reconnect_attempts: -1,
        retry_interval: Duration::from_millis(50),
        max_retry_interval: Duration::from_millis(50),
        ..ConnectionOptions::default()
    }
}

fn transacted() -> SessionOptions {
    SessionOptions {
        auto_commit_sends: false,
        auto_commit_acks: false,
        ..SessionOptions::default()
    }
}

fn listen(conn: &Connection) -> Arc<Mutex<Vec<FailoverEvent>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    conn.set_failover_listener(move |e| seen.lock().unwrap().push(e));
    events
}

async fn wait_for(events: &Mutex<Vec<FailoverEvent>>, event: FailoverEvent) {
    for _ in 0..(WAIT.as_millis() / 10) {
        if events.lock().unwrap().contains(&event) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no {event:?}, only {:?}", events.lock().unwrap());
}

async fn send(session: &Session, producer: &Producer, round: u32, count: u32) {
    for i in 0..count {
        producer
            .send(message::text_message(&format!("round {round} message {i}")))
            .await
            .unwrap();
    }
    session.commit().await.unwrap();
}

async fn receive(consumer: &Consumer, round: u32, count: u32) {
    for i in 0..count {
        let d = consumer
            .receive(WAIT)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("round {round}: message {i} not received"));
        assert_eq!(
            d.message.text().unwrap().as_deref(),
            Some(format!("round {round} message {i}").as_str())
        );
        consumer.acknowledge(&d).await.unwrap();
    }
}

async fn durable_queue(session: &Session, name: &str) {
    session
        .create_queue(
            QueueConfiguration::new(name)
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnects_to_the_restarted_server() {
    let dir = tempfile::tempdir().unwrap();
    let (running, port) = start(dir.path(), 0).await;
    let conn = Connection::connect_to(vec![Connector::new("127.0.0.1", port)], reconnecting())
        .await
        .unwrap();
    let events = listen(&conn);
    let session = conn.create_session(transacted()).await.unwrap();
    durable_queue(&session, "failover").await;
    let producer = session.create_producer(Some("failover")).await.unwrap();
    let consumer = session
        .create_consumer("failover", None, false)
        .await
        .unwrap();
    session.start().await.unwrap();
    let name = session.name();

    send(&session, &producer, 1, 10).await;
    stop(running).await;
    let (running, _) = start(dir.path(), port).await;
    wait_for(&events, FailoverEvent::FailoverCompleted).await;
    assert_eq!(
        *events.lock().unwrap(),
        [
            FailoverEvent::FailureDetected,
            FailoverEvent::FailoverCompleted
        ]
    );
    // Recreated under a new name, with its consumer.
    assert_ne!(session.name(), name);
    receive(&consumer, 1, 10).await;
    session.commit().await.unwrap();

    send(&session, &producer, 2, 10).await;
    receive(&consumer, 2, 10).await;
    session.commit().await.unwrap();
    assert!(
        consumer
            .receive(Duration::from_millis(100))
            .await
            .unwrap()
            .is_none()
    );
    conn.close().await;
    stop(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_with_work_rolls_back_after_failover() {
    let dir = tempfile::tempdir().unwrap();
    let (running, port) = start(dir.path(), 0).await;
    let conn = Connection::connect_to(vec![Connector::new("127.0.0.1", port)], reconnecting())
        .await
        .unwrap();
    let events = listen(&conn);
    let session = conn.create_session(transacted()).await.unwrap();
    durable_queue(&session, "rollback").await;
    let producer = session.create_producer(Some("rollback")).await.unwrap();

    producer
        .send(message::text_message("lost with the transaction"))
        .await
        .unwrap();
    stop(running).await;
    let (running, _) = start(dir.path(), port).await;
    wait_for(&events, FailoverEvent::FailoverCompleted).await;
    let err = session.commit().await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::TransactionRolledBack), "{err}");

    // The session goes on: the next transaction commits.
    send(&session, &producer, 1, 1).await;
    let consumer = session
        .create_consumer("rollback", None, false)
        .await
        .unwrap();
    session.start().await.unwrap();
    receive(&consumer, 1, 1).await;
    session.commit().await.unwrap();
    assert!(
        consumer
            .receive(Duration::from_millis(100))
            .await
            .unwrap()
            .is_none()
    );
    conn.close().await;
    stop(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_reconnect_attempts_the_sessions_fail() {
    let dir = tempfile::tempdir().unwrap();
    let (running, port) = start(dir.path(), 0).await;
    let conn = Connection::connect_to(
        vec![Connector::new("127.0.0.1", port)],
        ConnectionOptions::default(),
    )
    .await
    .unwrap();
    let events = listen(&conn);
    let session = conn.create_session(transacted()).await.unwrap();
    durable_queue(&session, "fails").await;
    stop(running).await;
    wait_for(&events, FailoverEvent::FailoverFailed).await;
    assert_eq!(
        *events.lock().unwrap(),
        [
            FailoverEvent::FailureDetected,
            FailoverEvent::FailoverFailed
        ]
    );
    let err = session.commit().await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::Disconnected), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_initial_connectors_are_tried_in_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (running, port) = start(dir.path(), 0).await;
    // A port nothing listens on: bound, then let go.
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let options = ConnectionOptions {
        retry_interval: Duration::from_millis(10),
        ..ConnectionOptions::default()
    };
    let conn = Connection::connect_to(
        vec![
            Connector::new("127.0.0.1", closed),
            Connector::new("127.0.0.1", port),
        ],
        options.clone(),
    )
    .await
    .unwrap();
    assert_eq!(conn.connector(), Some(Connector::new("127.0.0.1", port)));
    conn.close().await;

    let err = Connection::connect_to(vec![Connector::new("127.0.0.1", closed)], options)
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), Some(ErrorType::NotConnected));
    assert!(err.to_string().starts_with("AMQ219007"), "{err}");
    stop(running).await;
}
