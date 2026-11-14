//! An XA commit refused for its kind (one phase for a prepared branch, two
//! for one not prepared) leaves the branch as it was: still there to commit
//! the right way, to roll back, and to recover.

use std::path::Path;
use std::sync::Arc;

use saranga_config as cfg;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path) -> Arc<Broker> {
    start_with(dir, "").await
}

/// [`start`] with `extra` in `<core>`.
async fn start_with(dir: &Path, extra: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
             {extra}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn xa_session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(saranga_security::ConnectionInfo {
            remote_address: "test".into(),
            certificates: None,
            security_domain: None,
            connection_id: None,
            sasl_principals: None,
            sni_host: None,
        }),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "xa".into(),
            username: None,
            password: None,
            xa: true,
            auto_commit_sends: false,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn xid(name: &str) -> Xid {
    Xid {
        format_id: 123,
        branch_qualifier: name.as_bytes().to_vec(),
        global_transaction_id: name.as_bytes().to_vec(),
    }
}

#[tokio::test]
async fn a_one_phase_commit_of_a_prepared_branch_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let x = xid("prepared");
    let mut s = xa_session(&broker);
    s.xa_start(x.clone()).unwrap();
    s.xa_end(x.clone()).unwrap();
    s.xa_prepare(x.clone()).unwrap();
    s.take_completion().wait().await.unwrap();
    assert!(s.xa_commit(x.clone(), true).is_err());
    assert!(
        broker.resource_manager().get(&x).is_some(),
        "the branch was lost"
    );
    s.xa_commit(x.clone(), false).unwrap();
    s.take_completion().wait().await.unwrap();
    assert!(broker.resource_manager().get(&x).is_none(), "committed");
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_two_phase_commit_of_a_branch_not_prepared_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let x = xid("active");
    let mut s = xa_session(&broker);
    s.xa_start(x.clone()).unwrap();
    s.xa_end(x.clone()).unwrap();
    assert!(s.xa_commit(x.clone(), false).is_err());
    assert!(
        broker.resource_manager().get(&x).is_some(),
        "the branch was lost"
    );
    s.xa_commit(x.clone(), true).unwrap();
    s.take_completion().wait().await.unwrap();
    assert!(broker.resource_manager().get(&x).is_none(), "committed");
    broker.stop().unwrap();
}

/// `xa_start` replacing an unprepared branch must not hold the transaction lock
/// while taking the resource-manager map lock: the reaper takes them in the
/// opposite order, so the old ordering could deadlock. This hammers both paths
/// at once and must finish well inside the timeout.
#[tokio::test]
async fn xa_start_does_not_deadlock_with_the_reaper() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;

    // The reaper: take_timed_out locks the map, then each transaction. A huge
    // `now` makes every unprepared branch look timed out, so it reaches them.
    // Both run on plain threads so a regression (a real deadlock) times out and
    // fails here rather than hanging the runtime's shutdown.
    let stop = Arc::new(AtomicBool::new(false));
    let reaper = {
        let broker = broker.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = broker.resource_manager().take_timed_out(i64::MAX / 2);
            }
        })
    };

    // The worker: each xa_start replaces the previous, unprepared branch, which
    // takes the transaction lock and then the map lock (remove_own).
    let (tx, rx) = mpsc::channel();
    {
        let broker = broker.clone();
        std::thread::spawn(move || {
            let mut s = xa_session(&broker);
            for i in 0..20_000 {
                let _ = s.xa_start(xid(&format!("branch-{i}")));
            }
            let _ = tx.send(());
        });
    }

    let finished = rx.recv_timeout(Duration::from_secs(25)).is_ok();
    stop.store(true, Ordering::Relaxed);
    assert!(finished, "xa_start deadlocked with the reaper");
    // Only safe once the worker finished: otherwise the reaper is blocked on a
    // transaction the worker holds and this would hang too.
    reaper.join().unwrap();
    broker.stop().unwrap();
}

/// [`start`] with the transaction reaper an hour apart. [`race`] holds a
/// branch's lock while it starts the Xid on another session, which takes the
/// resource manager's: the reaper takes them the other way round, so a pass
/// meanwhile would deadlock the test (the broker never holds them so).
async fn start_without_reaper(dir: &Path) -> Arc<Broker> {
    start_with(
        dir,
        "<transaction-timeout-scan-period>3600000</transaction-timeout-scan-period>",
    )
    .await
}

/// Runs `complete` on another thread while the branch's lock is held, starts
/// the same Xid on another session meanwhile, and returns whether that start
/// was accepted, what `complete` answered, and whether the manager still
/// holds the original branch. A branch is checked while it is registered,
/// so the start is refused; it was accepted once the branch had been taken
/// out to be checked, and the refused completion could not put it back.
fn race(
    broker: &Arc<Broker>,
    x: &Xid,
    complete: impl FnOnce(&mut ServerSession) -> bool + Send + 'static,
) -> (bool, bool, bool) {
    let original = broker.resource_manager().get(x).unwrap();
    let guard = saranga_server::transaction::lock(&original);
    let other = broker.clone();
    let done = std::thread::spawn(move || complete(&mut xa_session(&other)));
    // With the branch taken out to be checked, it goes at once.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while broker.resource_manager().get(x).is_some() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let started = xa_session(broker).xa_start(x.clone()).is_ok();
    drop(guard);
    let completed = done.join().unwrap();
    let kept = broker
        .resource_manager()
        .get(x)
        .is_some_and(|t| Arc::ptr_eq(&t, &original));
    (started, completed, kept)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_commit_keeps_its_branch_from_another_start() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_without_reaper(dir.path()).await;
    let x = xid("raced-commit");
    let mut s = xa_session(&broker);
    s.xa_start(x.clone()).unwrap();
    s.xa_end(x.clone()).unwrap();
    s.xa_prepare(x.clone()).unwrap();
    s.take_completion().wait().await.unwrap();
    // One phase, for a prepared branch: refused.
    let (started, committed, kept) = race(&broker, &x, {
        let x = x.clone();
        move |s| s.xa_commit(x, true).is_ok()
    });
    assert!(!committed, "a one-phase commit of a prepared branch");
    assert!(!started, "another session started the branch's Xid");
    assert!(kept, "the branch was lost");
    broker.stop().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_rollback_keeps_its_branch_from_another_start() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_without_reaper(dir.path()).await;
    let x = xid("raced-rollback");
    let mut s = xa_session(&broker);
    s.xa_start(x.clone()).unwrap();
    s.xa_suspend().unwrap();
    // A suspended branch cannot be rolled back.
    let (started, rolled_back, kept) = race(&broker, &x, {
        let x = x.clone();
        move |s| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(s.xa_rollback(x))
                .is_ok()
        }
    });
    assert!(!rolled_back, "a suspended branch rolled back");
    assert!(!started, "another session started the branch's Xid");
    assert!(kept, "the branch was lost");
    broker.stop().unwrap();
}
