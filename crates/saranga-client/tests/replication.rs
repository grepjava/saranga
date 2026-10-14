//! Replication between in-process Saranga brokers: a primary with data
//! from before and after its backup registered, a primary stopped, and the
//! backup becoming active on its copy with the same messages; then the
//! primary restarted, copying the active backup's data and taking its node
//! back.

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

fn backup_config(bc: &BrokerConfig, fail_back: bool) -> BackupConfig {
    let storage = bc.storage.clone().unwrap();
    BackupConfig {
        name: "backup".into(),
        policy: saranga_config::Replication::default(),
        cluster: bc.cluster.clone().unwrap(),
        messages: storage.messages.clone(),
        bindings: storage.bindings.clone(),
        paging: storage.paging.clone(),
        large_messages: storage.large_messages.clone(),
        fail_back,
        pluggable: None,
    }
}

async fn wait_for_sync(primary: &Broker) {
    for _ in 0..(WAIT.as_millis() / 20) {
        if primary.is_replica_sync() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(primary.is_replica_sync(), "the backup never synchronized");
}

/// As the CLI stops a broker: its clients disconnected, the backup's
/// connection left for the stop to end replication.
async fn stop(broker: &Broker, acceptor: tokio::task::JoinHandle<std::io::Result<()>>) {
    acceptor.abort();
    let _ = acceptor.await;
    for c in broker.registry().connections() {
        if !broker.is_backup_connection(&c) {
            c.disconnect();
        }
    }
    for _ in 0..200 {
        let registry = broker.registry();
        let backups = registry
            .connections()
            .iter()
            .filter(|c| broker.is_backup_connection(c))
            .count();
        if registry.registered() <= backups {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    broker.stop().unwrap();
}

async fn receive_all(port: u16, from: u32, to: u32) {
    let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
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
    for i in from..to {
        let d = consumer.receive(WAIT).await.unwrap().unwrap();
        assert_eq!(
            d.message.text().unwrap().as_deref(),
            Some(format!("message {i}").as_str())
        );
    }
    assert!(
        consumer
            .receive(Duration::from_millis(200))
            .await
            .unwrap()
            .is_none()
    );
    conn.close().await;
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
    let backup = tokio::spawn(replicate(backup_config(&bc, false), "test"));
    wait_for_sync(&primary).await;

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
    stop(&primary, acceptor).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_restarted_primary_takes_its_node_back() {
    let pdir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let (pp, pb) = (free_port().await, free_port().await);
    let primary_file = xml(
        "primary",
        pp,
        pb,
        "<primary><check-for-active-server>true</check-for-active-server></primary>",
    );
    let backup_file = xml(
        "backup",
        pb,
        pp,
        "<backup><allow-failback>true</allow-failback></backup>",
    );
    let send_some = |port: u16, from: u32, count: u32| async move {
        let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
            .await
            .unwrap();
        let session = conn
            .create_session(SessionOptions {
                auto_commit_sends: false,
                ..SessionOptions::default()
            })
            .await
            .unwrap();
        if from == 0 {
            session
                .create_queue(
                    QueueConfiguration::new("replicated")
                        .routing_type(RoutingType::Anycast)
                        .durable(true),
                )
                .await
                .unwrap();
        }
        let producer = session.create_producer(Some("replicated")).await.unwrap();
        send(&session, &producer, from, count).await;
        conn.close().await;
    };

    // A primary with messages, and its backup, which takes over.
    let pc = config(&primary_file, pdir.path());
    let primary = Broker::start(config(&primary_file, pdir.path()))
        .await
        .unwrap();
    let acceptor = serve(&primary, pp).await;
    send_some(pp, 0, 20).await;
    let bc = config(&backup_file, bdir.path());
    let backup = tokio::spawn(replicate(backup_config(&bc, false), "test"));
    wait_for_sync(&primary).await;
    stop(&primary, acceptor).await;
    let node_id = tokio::time::timeout(WAIT, backup)
        .await
        .expect("the backup did not take over")
        .unwrap()
        .unwrap();

    // The backup, active, gets more messages.
    let lock_dir = bc.storage.as_ref().unwrap().messages.directory.clone();
    saranga_server::node::write_node_id(&lock_dir, &node_id).unwrap();
    let mut active_config = config(&backup_file, bdir.path());
    let saranga_config::HaPolicy::ReplicationBackup(policy) = active_config.ha.policy.clone()
    else {
        panic!("{:?}", active_config.ha.policy)
    };
    active_config.ha.policy = saranga_config::HaPolicy::ReplicationPrimary(policy);
    let active = Broker::start(active_config).await.unwrap();
    let active_acceptor = serve(&active, pb).await;
    send_some(pb, 20, 10).await;

    // The primary starts again: its node is active on the backup, so it
    // copies the backup's data and asks for its node back.
    let cluster = pc.cluster.clone().unwrap();
    use saranga_server::replication::backup::is_node_id_used;
    assert!(is_node_id_used(&cluster, &node_id).await);
    assert!(!is_node_id_used(&cluster, "no-such-node").await);
    let failing_back = tokio::spawn(replicate(backup_config(&pc, true), "test"));
    tokio::time::timeout(Duration::from_secs(30), active.failback_requested())
        .await
        .expect("the active backup never handed back");
    stop(&active, active_acceptor).await;
    let back = tokio::time::timeout(WAIT, failing_back)
        .await
        .expect("the primary did not take over")
        .unwrap()
        .unwrap();
    assert_eq!(back, node_id);

    // The primary, active again with everything.
    let primary = Broker::start(config(&primary_file, pdir.path()))
        .await
        .unwrap();
    assert_eq!(primary.node_id(), node_id);
    let acceptor = serve(&primary, pp).await;
    receive_all(pp, 0, 30).await;
    acceptor.abort();
    primary.stop().unwrap();
}

/// Replication coordinated through Artemis's file-based lock manager:
/// the backup takes over once it has the node's lock with the latest
/// activation sequence, and the restarted primary, its data stale, copies
/// the active backup's and takes its node back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_a_lock_manager_the_latest_copy_activates() {
    use saranga_server::replication::pluggable::{Activation, activate};
    let locks = tempfile::tempdir().unwrap();
    let pdir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let (pp, pb) = (free_port().await, free_port().await);
    let manager = format!(
        r#"<manager><class-name>org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager</class-name>
             <properties><property key="locks-folder" value="{}"/></properties></manager>"#,
        locks.path().display()
    );
    let primary_file = xml("primary", pp, pb, &format!("<primary>{manager}</primary>"));
    let backup_file = xml(
        "backup",
        pb,
        pp,
        &format!("<backup>{manager}<allow-failback>true</allow-failback></backup>"),
    );
    let activation = |file: &saranga_config::BrokerConfig, dir: &std::path::Path| {
        let c = config(file, dir);
        let policy = file.ha_policy.replication().unwrap().clone();
        let m = policy.manager.clone().unwrap();
        Activation {
            name: c.management.name.clone(),
            manager: saranga_ha::lockmanager::LockManager::new(&m.class_name, &m.properties)
                .unwrap(),
            lock_dir: c.storage.as_ref().unwrap().messages.directory.clone(),
            primary: !file.ha_policy.is_backup(),
            as_backup: false,
            coordination_id: None,
            backup: backup_config(&c, false),
            version: "test".into(),
        }
    };
    let start = |file: &saranga_config::BrokerConfig,
                 dir: &std::path::Path,
                 a: saranga_server::replication::pluggable::Activated| {
        let mut c = config(file, dir);
        let policy = file.ha_policy.replication().unwrap().clone();
        c.ha.policy = saranga_config::HaPolicy::ReplicationPrimary(policy);
        c.ha.coordination = Some(a.coordination);
        Broker::start(c)
    };
    let send_some = |port: u16, from: u32, count: u32| async move {
        let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
            .await
            .unwrap();
        let session = conn
            .create_session(SessionOptions {
                auto_commit_sends: false,
                ..SessionOptions::default()
            })
            .await
            .unwrap();
        if from == 0 {
            session
                .create_queue(
                    QueueConfiguration::new("replicated")
                        .routing_type(RoutingType::Anycast)
                        .durable(true),
                )
                .await
                .unwrap();
        }
        let producer = session.create_producer(Some("replicated")).await.unwrap();
        send(&session, &producer, from, count).await;
        conn.close().await;
    };
    let sequence = |dir: &std::path::Path| {
        saranga_ha::activation::read_sequence(&dir.join("data/journal")).unwrap()
    };

    // The primary activates on its fresh data: sequence 1.
    let a = activate(activation(&primary_file, pdir.path()))
        .await
        .unwrap();
    assert!(!a.from_backup);
    let primary = start(&primary_file, pdir.path(), a).await.unwrap();
    let node_id = primary.node_id().to_owned();
    let acceptor = serve(&primary, pp).await;
    send_some(pp, 0, 20).await;
    assert_eq!(sequence(pdir.path()), 1);

    // The backup copies it, with its node ID and sequence.
    let backup = tokio::spawn(activate(activation(&backup_file, bdir.path())));
    wait_for_sync(&primary).await;
    send_some(pp, 20, 5).await;
    assert_eq!(sequence(bdir.path()), 1);

    // The primary stops: the backup takes the lock and moves to 2.
    stop(&primary, acceptor).await;
    primary.coordination().unwrap().manager.stop().await;
    let b = tokio::time::timeout(WAIT, backup)
        .await
        .expect("the backup did not take over")
        .unwrap()
        .unwrap();
    assert!(b.from_backup);
    assert_eq!(b.coordination.node_id(), node_id);
    assert_eq!(sequence(bdir.path()), 2);
    let active = start(&backup_file, bdir.path(), b).await.unwrap();
    assert_eq!(active.node_id(), node_id);
    let active_acceptor = serve(&active, pb).await;
    send_some(pb, 25, 5).await;

    // The primary starts again: at 1, its data is stale, so it copies the
    // active backup's and asks for the node back.
    let failing_back = tokio::spawn(activate(activation(&primary_file, pdir.path())));
    tokio::time::timeout(Duration::from_secs(30), active.failback_requested())
        .await
        .expect("the active backup never handed back");
    stop(&active, active_acceptor).await;
    let c = active.coordination().unwrap().clone();
    c.manager.stop().await;
    c.await_failback().await;
    let back = tokio::time::timeout(WAIT, failing_back)
        .await
        .expect("the primary did not take over")
        .unwrap()
        .unwrap();
    assert_eq!(sequence(pdir.path()), 3);
    let primary = start(&primary_file, pdir.path(), back).await.unwrap();
    assert_eq!(primary.node_id(), node_id);
    let acceptor = serve(&primary, pp).await;
    receive_all(pp, 0, 30).await;
    acceptor.abort();
    primary.stop().unwrap();
    primary.coordination().unwrap().manager.stop().await;
}
