//! Against a ZooKeeper server named by `SARANGA_ZOOKEEPER` (a connect
//! string); skipped without it. tests/interop/ha/pluggable.sh starts one.

use std::collections::BTreeMap;
use std::time::Duration;

use saranga_ha::activation::{LocalNode, ensure_sequential_access, try_activate};
use saranga_ha::lockmanager::{CURATOR_CLASS, LockManager};
use saranga_ha::zookeeper::{self, CreateMode, ZooKeeper};

fn server() -> Option<String> {
    std::env::var("SARANGA_ZOOKEEPER")
        .ok()
        .filter(|s| !s.is_empty())
}

fn manager(connect: &str, namespace: &str) -> std::sync::Arc<LockManager> {
    let p: BTreeMap<String, String> = [
        ("connect-string".to_owned(), connect.to_owned()),
        ("namespace".to_owned(), namespace.to_owned()),
        ("session-ms".to_owned(), "4000".to_owned()),
    ]
    .into();
    LockManager::new(CURATOR_CLASS, &p).unwrap()
}

#[tokio::test]
async fn operations_and_ephemeral_nodes() {
    let Some(connect) = server() else { return };
    let cfg = zookeeper::Config {
        connect_string: connect.clone(),
        session_timeout: Duration::from_secs(4),
        connection_timeout: Duration::from_secs(4),
        session_percent: 33,
    };
    let zk = ZooKeeper::start(cfg.clone()).unwrap();
    assert!(zk.wait_connected(Some(Duration::from_secs(10))).await);
    let base = format!("/saranga-test-{}", uuid::Uuid::new_v4());
    zk.create(&base, b"", CreateMode::Persistent).await.unwrap();
    let e = zk
        .create(
            &format!("{base}/e-"),
            b"data",
            CreateMode::EphemeralSequential,
        )
        .await
        .unwrap();
    assert!(e.starts_with(&format!("{base}/e-")) && e.len() == base.len() + 3 + 10);
    assert_eq!(zk.get_data(&e).await.unwrap().0, b"data");
    zk.set_data(&e, b"more", -1).await.unwrap();
    assert_eq!(zk.get_children(&base).await.unwrap().len(), 1);
    let missing = zk.get_data(&format!("{base}/nope")).await.unwrap_err();
    assert_eq!(missing.code(), zookeeper::proto::code::NO_NODE);
    // Closing the session removes its ephemeral node.
    zk.close().await;
    let other = ZooKeeper::start(cfg).unwrap();
    assert!(other.wait_connected(Some(Duration::from_secs(10))).await);
    assert!(other.get_children(&base).await.unwrap().is_empty());
    other.delete(&base, -1).await.unwrap();
    other.close().await;
}

#[tokio::test]
async fn one_holder_of_a_lock() {
    let Some(connect) = server() else { return };
    let ns = format!("saranga-test-{}", uuid::Uuid::new_v4());
    let a = manager(&connect, &ns);
    let b = manager(&connect, &ns);
    assert!(a.start(Some(Duration::from_secs(10))).await);
    assert!(b.start(Some(Duration::from_secs(10))).await);
    let la = a.distributed_lock("node").unwrap();
    let lb = b.distributed_lock("node").unwrap();
    assert!(la.try_lock().await.unwrap());
    assert!(la.is_held_by_caller().await.unwrap());
    assert!(!lb.try_lock().await.unwrap());
    assert!(!lb.is_held_by_caller().await.unwrap());
    la.unlock().await.unwrap();
    assert!(lb.try_lock().await.unwrap());
    // The holder's session ends: its lease goes with it.
    b.stop().await;
    let la = a.distributed_lock("node").unwrap();
    assert!(la.try_lock().await.unwrap());

    let mut n = LocalNode {
        dir: tempfile::tempdir().unwrap().keep(),
        node_id: "node".into(),
        sequence: 0,
    };
    la.unlock().await.unwrap();
    let l = try_activate(&mut n, &a).await.unwrap().expect("in sync");
    ensure_sequential_access("test", &mut n, &a).await.unwrap();
    assert_eq!(a.mutable_long("node").unwrap().get().await.unwrap(), 1);
    l.close().await;
    a.stop().await;
}

/// With `SARANGA_ZK_PEER=hold|free|<n>`: what a Java broker's
/// `CuratorDistributedLockManager` left in namespace `SARANGA_ZK_NS`.
#[tokio::test]
async fn as_curator_sees_it() {
    let (Some(connect), Ok(ns), Ok(peer)) = (
        server(),
        std::env::var("SARANGA_ZK_NS"),
        std::env::var("SARANGA_ZK_PEER"),
    ) else {
        return;
    };
    let m = manager(&connect, &ns);
    assert!(m.start(Some(Duration::from_secs(10))).await);
    match peer.as_str() {
        "hold" => assert!(
            !m.distributed_lock("node")
                .unwrap()
                .try_lock()
                .await
                .unwrap()
        ),
        "free" => {
            let l = m.distributed_lock("node").unwrap();
            assert!(l.try_lock().await.unwrap());
            tokio::time::sleep(Duration::from_secs(5)).await;
            assert!(l.is_held_by_caller().await.unwrap());
        }
        n => {
            let lng = m.mutable_long("node").unwrap();
            assert_eq!(lng.get().await.unwrap(), n.parse::<i64>().unwrap());
            lng.set(-42).await.unwrap();
        }
    }
    m.stop().await;
}
