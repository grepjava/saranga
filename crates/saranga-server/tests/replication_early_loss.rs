//! A backup that registers and closes its socket at once: its loss is
//! handled before the sync starts the stores' replication, and the sync,
//! failing then, stops what it started, so journal files are reclaimed (and
//! pages cleaned up) as without a backup.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_config as cfg;
use saranga_core_protocol::packets::cluster::{BackupRegistrationMessage, ClusterConnectMessage};
use saranga_core_protocol::{Context, Direction, HANDSHAKE, Packet};
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn start(dir: &Path, address: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <security-enabled>false</security-enabled>
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             <journal-min-files>2</journal-min-files>
             <journal-pool-files>2</journal-pool-files>
             <ha-policy><replication><primary/></replication></ha-policy>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    broker
        .add_acceptor("artemis", address, saranga_server::Acceptor::default())
        .await
        .unwrap();
    broker
}

async fn op(b: &Arc<Broker>, res: &str, o: &str, p: Vec<Json>) -> Json {
    b.management()
        .invoke(b, res, o, &p, Default::default())
        .await
        .unwrap()
}

fn amq_files(dir: &Path) -> usize {
    fn walk(d: &Path, n: &mut usize) {
        for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                if p.file_name().is_some_and(|f| f == "journal") {
                    *n += std::fs::read_dir(&p)
                        .unwrap()
                        .flatten()
                        .filter(|e| e.path().extension().is_some_and(|x| x == "amq"))
                        .count();
                } else {
                    walk(&p, n);
                }
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

/// Fills and empties the messages journal several times over.
async fn churn(b: &Arc<Broker>) {
    let body = "x".repeat(4000);
    for _ in 0..8 {
        for _ in 0..40 {
            op(
                b,
                "queue.q",
                "sendMessage",
                vec![
                    Json::object(),
                    Json::from(3i64),
                    Json::from(body.as_str()),
                    Json::from(true),
                    Json::Null,
                    Json::Null,
                ],
            )
            .await;
        }
        op(b, "queue.q", "removeAllMessages", vec![]).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}

async fn run(register: bool, late: bool) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let address = format!("127.0.0.1:{}", free_port());
    let b = start(dir.path(), &address).await;
    op(
        &b,
        "broker",
        "createQueue",
        vec![Json::from(
            r#"{"name":"q","address":"q","routing-type":"ANYCAST","durable":true}"#,
        )],
    )
    .await;
    if register {
        let ctx = Context::new(Direction::ToServer, 0);
        let mut bytes = HANDSHAKE.to_vec();
        bytes.extend(
            Packet::from(ClusterConnectMessage {
                cluster_user: "".into(),
                cluster_password: "".into(),
            })
            .encode_frame(3, &ctx),
        );
        bytes.extend(
            Packet::from(BackupRegistrationMessage {
                cluster_user: "".into(),
                cluster_password: "".into(),
                backup_wants_fail_back: false,
                connector: Default::default(),
            })
            .encode_frame(3, &ctx),
        );
        let mut s = TcpStream::connect(&address).await.unwrap();
        // Registration and EOF in one go: the connection's task handles the
        // registration, then the EOF, before the blocking sync thread runs.
        s.write_all(&bytes).await.unwrap();
        if late {
            // Control: the sync is under way (stores started) before the close.
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        s.shutdown().await.unwrap();
        drop(s);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    churn(&b).await;
    let n = amq_files(dir.path());
    b.stop().unwrap();
    n
}

#[tokio::test(flavor = "current_thread")]
async fn a_backup_lost_before_its_sync_leaves_reclaim_running() {
    let control = run(false, false).await;
    for i in 0..3 {
        let n = run(true, false).await;
        assert!(
            n <= control + 2,
            "run {i}: {n} journal files kept, {control} without a backup"
        );
    }
}
