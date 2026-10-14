//! Replication between in-process Saranga brokers: a primary with data
//! from before and after its backup registered, a primary stopped, and the
//! backup becoming active on its copy with the same messages.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::replication::backup::{BackupConfig, replicate};
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(10);

fn xml(name: &str, me: u16, other: u16, policy: &str) -> saranga_config::BrokerConfig {
    saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <name>{name}</name>
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
             <journal-file-size>256K</journal-file-size>
             <connectors>
               <connector name="me">tcp://127.0.0.1:{me}</connector>
               <connector name="other">tcp://127.0.0.1:{other}</connector>
             </connectors>
             <cluster-connections><cluster-connection name="c">
               <connector-ref>me</connector-ref>
               <retry-interval>100</retry-interval>
               <static-connectors><connector-ref>other</connector-ref></static-connectors>
             </cluster-connection></cluster-connections>
             <ha-policy><replication>{policy}</replication></ha-policy>
           </core></configuration>"#
    ))
    .unwrap()
}

fn config(file: &saranga_config::BrokerConfig, dir: &std::path::Path) -> BrokerConfig {
    BrokerConfig::from_file_config(file, Arc::new(SelectorFilters), dir).unwrap()
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn serve(broker: &Arc<Broker>, port: u16) -> tokio::task::JoinHandle<std::io::Result<()>> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()))
}

async fn send(session: &Session, producer: &Producer, from: u32, count: u32) {
    for i in from..from + count {
        producer
            .send(message::text_message(&format!("message {i}")))
            .await
            .unwrap();
    }
    session.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backup_takes_over_with_the_primarys_messages() {
    let pdir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let (pp, pb) = (free_port().await, free_port().await);

    let primary = Broker::start(config(&xml("primary", pp, pb, "<primary/>"), pdir.path()))
        .await
        .unwrap();
    let acceptor = serve(&primary, pp).await;
    let conn = Connection::connect(("127.0.0.1", pp), ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions {
            auto_commit_sends: false,
            auto_commit_acks: false,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    session
        .create_queue(
            QueueConfiguration::new("replicated")
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await
        .unwrap();
    let producer = session.create_producer(Some("replicated")).await.unwrap();
    // Before the backup: copied in the synchronization, across a few
    // journal files, with a large message.
    send(&session, &producer, 0, 300).await;
    producer
        .send(message::bytes_message(&vec![7u8; 300 * 1024]))
        .await
        .unwrap();
    session.commit().await.unwrap();

    let backup_file = xml("backup", pb, pp, "<backup/>");
    let bc = config(&backup_file, bdir.path());
    let storage = bc.storage.clone().unwrap();
    let backup = tokio::spawn(replicate(
        BackupConfig {
            name: "backup".into(),
            policy: saranga_config::Replication::default(),
            cluster: bc.cluster.clone().unwrap(),
            messages: storage.messages.clone(),
            bindings: storage.bindings.clone(),
            paging: storage.paging.clone(),
            large_messages: storage.large_messages.clone(),
        },
        "test",
    ));
    for _ in 0..(WAIT.as_millis() / 20) {
        if primary.is_replica_sync() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(primary.is_replica_sync(), "the backup never synchronized");

    // After: replicated as it is written. Some messages are consumed.
    send(&session, &producer, 300, 50).await;
    producer
        .send(message::bytes_message(&vec![9u8; 200 * 1024]))
        .await
        .unwrap();
    session.commit().await.unwrap();
    let consumer = session
        .create_consumer("replicated", None, false)
        .await
        .unwrap();
    session.start().await.unwrap();
    for i in 0..100 {
        let d = consumer.receive(WAIT).await.unwrap().unwrap();
        assert_eq!(
            d.message.text().unwrap().as_deref(),
            Some(format!("message {i}").as_str())
        );
        consumer.acknowledge(&d).await.unwrap();
    }
    session.commit().await.unwrap();
    // A transaction left open is not the backup's to keep.
    producer
        .send(message::text_message("never committed"))
        .await
        .unwrap();

    // The primary stops.
    acceptor.abort();
    let _ = acceptor.await;
    for c in primary.registry().connections() {
        c.disconnect();
    }
    for _ in 0..200 {
        if primary.registry().all_closed() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    primary.stop().unwrap();
    let node_id = tokio::time::timeout(WAIT, backup)
        .await
        .expect("the backup did not take over")
        .unwrap()
        .unwrap();
    assert_eq!(node_id, primary.node_id());
    drop(conn);

    // The backup, active on its copy, as the primary's node.
    let lock_dir = storage.messages.directory.clone();
    saranga_server::node::write_node_id(&lock_dir, &node_id).unwrap();
    let mut bc = config(&backup_file, bdir.path());
    bc.ha.policy = saranga_config::HaPolicy::ReplicationPrimary(Default::default());
    let active = Broker::start(bc).await.unwrap();
    assert_eq!(active.node_id(), node_id);
    let acceptor = serve(&active, pb).await;
    let conn = Connection::connect(("127.0.0.1", pb), ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    let consumer = session
        .create_consumer("replicated", None, false)
        .await
        .unwrap();
    session.start().await.unwrap();
    for i in 100..300 {
        let d = consumer.receive(WAIT).await.unwrap().unwrap();
        assert_eq!(
            d.message.text().unwrap().as_deref(),
            Some(format!("message {i}").as_str())
        );
    }
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    assert_eq!(d.message.body, vec![7u8; 300 * 1024]);
    for i in 300..350 {
        let d = consumer.receive(WAIT).await.unwrap().unwrap();
        assert_eq!(
            d.message.text().unwrap().as_deref(),
            Some(format!("message {i}").as_str())
        );
    }
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    assert_eq!(d.message.body, vec![9u8; 200 * 1024]);
    assert!(
        consumer
            .receive(Duration::from_millis(200))
            .await
            .unwrap()
            .is_none()
    );
    conn.close().await;
    acceptor.abort();
    active.stop().unwrap();
}
