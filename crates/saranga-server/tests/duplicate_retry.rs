//! A send refused does not keep its duplicate ID: sending it again once the
//! address has room delivers it, as in Artemis, where the ID joins the cache
//! only when the send commits. Nor does it push an older ID out of the
//! cache, and one transaction cannot send one ID twice.

use std::path::Path;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, persistence: bool) -> Arc<Broker> {
    start_with(dir, persistence, "").await
}

async fn start_with(dir: &Path, persistence: bool, extra: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>{persistence}</persistence-enabled>
             {extra}
             <address-settings>
               <address-setting match="full">
                 <max-size-bytes>10000</max-size-bytes>
                 <address-full-policy>FAIL</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

async fn op(broker: &Arc<Broker>, resource: &str, operation: &str, params: Vec<Json>) -> Json {
    broker
        .management()
        .invoke(broker, resource, operation, &params, Default::default())
        .await
        .unwrap()
}

/// A session whose connection has flow control, as a Core client's has.
fn session(broker: &Arc<Broker>) -> ServerSession {
    session_with(broker, true)
}

/// A session sending in transactions unless `auto_commit_sends`.
fn session_with(broker: &Arc<Broker>, auto_commit_sends: bool) -> ServerSession {
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
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

/// Sends 4 KB, with a duplicate ID if given; the error's message if refused.
async fn send(session: &mut ServerSession, duplicate: Option<&str>) -> Result<(), String> {
    let mut message = CoreMessage {
        address: Some(SimpleString::from("full")),
        durable: true,
        body: vec![7u8; 4000].into(),
        ..Default::default()
    };
    if let Some(id) = duplicate {
        message.properties.insert(
            SimpleString::from("_AMQ_DUPL_ID"),
            PropertyValue::String(SimpleString::from(id)),
        );
    }
    let sent = session.send(message, None).await;
    session
        .take_completion()
        .wait()
        .await
        .map_err(|e| e.to_string())?;
    sent.map_err(|e| e.message)
}

async fn retry_after_full(persistence: bool) {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), persistence).await;
    op(
        &broker,
        "broker",
        "createQueue",
        vec![
            Json::from("full"),
            Json::from("ANYCAST"),
            Json::from("full"),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await;
    let mut s = session(&broker);
    // Fill the address until it refuses.
    let mut filled = 0;
    while send(&mut s, None).await.is_ok() {
        filled += 1;
        assert!(filled < 10, "the address never filled");
    }
    let e = send(&mut s, Some("d1")).await.unwrap_err();
    assert!(e.contains("AMQ229102"), "{e}");
    op(&broker, "queue.full", "removeAllMessages", vec![]).await;
    send(&mut s, Some("d1")).await.unwrap();
    assert_eq!(
        op(&broker, "queue.full", "countMessages", vec![]).await,
        Json::Long(1),
        "the retry was delivered"
    );
    // Once delivered, the ID is a duplicate.
    send(&mut s, Some("d1")).await.unwrap();
    assert_eq!(
        op(&broker, "queue.full", "countMessages", vec![]).await,
        Json::Long(1),
        "a second copy was dropped"
    );
    s.close(false).await;
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_send_refused_as_full_can_be_sent_again() {
    retry_after_full(true).await;
}

#[tokio::test]
async fn a_send_refused_as_full_can_be_sent_again_without_persistence() {
    retry_after_full(false).await;
}

async fn create_full_queue(broker: &Arc<Broker>) {
    op(
        broker,
        "broker",
        "createQueue",
        vec![
            Json::from("full"),
            Json::from("ANYCAST"),
            Json::from("full"),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await;
}

async fn count(broker: &Arc<Broker>) -> Json {
    op(broker, "queue.full", "countMessages", vec![]).await
}

/// With room for one ID, a send refused with another ID leaves the first in
/// the cache: sent again, it is still a duplicate.
#[tokio::test]
async fn a_send_refused_keeps_the_ids_before_it() {
    for persistence in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start_with(dir.path(), persistence, "<id-cache-size>1</id-cache-size>").await;
        create_full_queue(&broker).await;
        let mut s = session(&broker);
        send(&mut s, Some("d1")).await.unwrap();
        let mut filled = 1;
        while send(&mut s, None).await.is_ok() {
            filled += 1;
            assert!(filled < 10, "the address never filled");
        }
        let e = send(&mut s, Some("d2")).await.unwrap_err();
        assert!(e.contains("AMQ229102"), "{e}");
        op(&broker, "queue.full", "removeAllMessages", vec![]).await;
        send(&mut s, Some("d1")).await.unwrap();
        assert_eq!(
            count(&broker).await,
            Json::Long(0),
            "d1 was delivered again"
        );
        s.close(false).await;
        broker.stop().unwrap();
    }
}

/// The second send of an ID in one transaction is a duplicate: the
/// transaction is rolled back, as when the ID was in the cache before.
#[tokio::test]
async fn one_transaction_cannot_send_an_id_twice() {
    for persistence in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start(dir.path(), persistence).await;
        create_full_queue(&broker).await;
        let mut s = session_with(&broker, false);
        send(&mut s, Some("t1")).await.unwrap();
        send(&mut s, Some("t1")).await.unwrap();
        let e = s.commit().expect_err("the commit took both");
        s.take_completion().wait().await.unwrap();
        assert!(e.message.contains("Duplicate"), "{}", e.message);
        assert_eq!(count(&broker).await, Json::Long(0));
        // The ID was not taken: a new transaction sends it.
        send(&mut s, Some("t1")).await.unwrap();
        s.commit().unwrap();
        s.take_completion().wait().await.unwrap();
        assert_eq!(count(&broker).await, Json::Long(1));
        s.close(false).await;
        broker.stop().unwrap();
    }
}

/// Two transactions sending one ID before either commits: the second to
/// commit is a duplicate and rolls back, so the message is delivered once.
/// One that rolls back leaves the ID to the other.
#[tokio::test]
async fn overlapping_transactions_deliver_an_id_once() {
    for persistence in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start(dir.path(), persistence).await;
        create_full_queue(&broker).await;
        let mut first = session_with(&broker, false);
        let mut second = session_with(&broker, false);
        send(&mut first, Some("o1")).await.unwrap();
        send(&mut second, Some("o1")).await.unwrap();
        first.commit().unwrap();
        first.take_completion().wait().await.unwrap();
        let e = second.commit().expect_err("both transactions committed o1");
        second.take_completion().wait().await.unwrap();
        assert!(e.message.contains("Duplicate"), "{}", e.message);
        assert_eq!(
            count(&broker).await,
            Json::Long(1),
            "persistence {persistence}"
        );
        // A transaction that rolls back takes no ID from another.
        send(&mut first, Some("o2")).await.unwrap();
        send(&mut second, Some("o2")).await.unwrap();
        first.rollback(false, false).await;
        first.take_completion().wait().await.unwrap();
        second.commit().unwrap();
        second.take_completion().wait().await.unwrap();
        assert_eq!(
            count(&broker).await,
            Json::Long(2),
            "persistence {persistence}"
        );
        first.close(false).await;
        second.close(false).await;
        broker.stop().unwrap();
    }
}

/// A send with a duplicate ID in a transaction another thread is
/// committing waits for the commit, without holding the cache the commit
/// needs: the two take the transaction, then the cache.
#[tokio::test]
async fn a_send_and_a_commit_of_one_transaction_do_not_deadlock() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), false).await;
    create_full_queue(&broker).await;
    let mut s = session(&broker);
    let tx = s.new_transaction();
    let message = |id: &str| {
        let mut m = CoreMessage {
            address: Some(SimpleString::from("full")),
            ..Default::default()
        };
        m.properties.insert(
            SimpleString::from("_AMQ_DUPL_ID"),
            PropertyValue::String(SimpleString::from(id)),
        );
        m
    };
    s.send_in(message("first"), None, Some(tx.clone()))
        .await
        .unwrap();
    let cache = broker.post_office().duplicate_cache("full");
    let (locked, is_locked) = mpsc::channel();
    let (go, gate) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let committing = tx.clone();
    std::thread::spawn(move || {
        let mut t = saranga_server::transaction::lock(&committing);
        locked.send(()).unwrap();
        gate.recv().unwrap();
        let _ = t.commit(true);
        let _ = done.send(());
    });
    is_locked.recv_timeout(Duration::from_secs(5)).unwrap();
    let sender_broker = broker.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut s = session(&sender_broker);
            let _ = s.send_in(message("second"), None, Some(tx)).await;
        });
    });
    // Give the send time to reach its locks: with the order reversed it
    // takes the cache, then waits for the transaction.
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline && cache.try_lock().is_ok() {
        std::thread::sleep(Duration::from_millis(1));
    }
    go.send(()).unwrap();
    assert!(
        finished.recv_timeout(Duration::from_secs(5)).is_ok(),
        "the commit waits for the cache the send holds while it waits for the transaction"
    );
}

/// A paged send with a duplicate ID whose journal write fails leaves no
/// page record behind: after a restart nothing of it is recovered, and
/// sending it again delivers it once.
#[tokio::test]
async fn a_failed_paged_send_with_a_duplicate_id_leaves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <address-settings><address-setting match="full">
               <max-size-bytes>1</max-size-bytes>
               <page-size-bytes>10240</page-size-bytes>
               <address-full-policy>PAGE</address-full-policy>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let start_paging = || {
        let config =
            BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
        Broker::start(config)
    };
    let broker = start_paging().await.unwrap();
    create_full_queue(&broker).await;
    let mut s = session(&broker);
    send(&mut s, None).await.unwrap();
    assert!(broker.post_office().paging_store("full").is_paging());
    let fault =
        saranga_fault::Fault::new(saranga_fault::Op::Write, dir.path().join("data/journal")).arm();
    assert!(send(&mut s, Some("p1")).await.is_err());
    drop(fault);
    s.close(false).await;
    let _ = broker.stop();
    drop(s);
    drop(broker);
    let broker = start_paging().await.unwrap();
    assert_eq!(
        count(&broker).await,
        Json::Long(1),
        "the failed send was recovered"
    );
    let mut retry = session(&broker);
    send(&mut retry, Some("p1")).await.unwrap();
    assert_eq!(count(&broker).await, Json::Long(2));
    retry.close(false).await;
    broker.stop().unwrap();
}

/// A prepared branch recovered after a restart still holds its duplicate
/// IDs: another send of one is a duplicate, and committing the branch
/// delivers the message once.
#[tokio::test]
async fn a_recovered_prepared_branch_holds_its_duplicate_ids() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    create_full_queue(&broker).await;
    let xid = saranga_server::transaction::Xid {
        format_id: 7,
        branch_qualifier: b"b".to_vec(),
        global_transaction_id: b"g".to_vec(),
    };
    let mut s = session_with(&broker, false);
    s.xa_start(xid.clone()).unwrap();
    send(&mut s, Some("x1")).await.unwrap();
    s.xa_end(xid.clone()).unwrap();
    s.xa_prepare(xid.clone()).unwrap();
    s.take_completion().wait().await.unwrap();
    s.close(false).await;
    broker.stop().unwrap();
    drop(s);
    drop(broker);
    let broker = start(dir.path(), true).await;
    let mut retry = session(&broker);
    send(&mut retry, Some("x1")).await.unwrap();
    assert_eq!(
        count(&broker).await,
        Json::Long(0),
        "the retry was delivered"
    );
    retry.xa_commit(xid, false).unwrap();
    retry.take_completion().wait().await.unwrap();
    assert_eq!(count(&broker).await, Json::Long(1));
    retry.close(false).await;
    broker.stop().unwrap();
}

/// A send of an ID a commit took, before that commit's record is written,
/// is already a duplicate.
#[tokio::test]
async fn an_id_is_a_duplicate_from_its_commit_on() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    create_full_queue(&broker).await;
    let mut tx = session_with(&broker, false);
    let mut retry = session(&broker);
    let mut message = CoreMessage {
        address: Some(SimpleString::from("full")),
        durable: true,
        body: vec![7u8; 100].into(),
        ..Default::default()
    };
    message.properties.insert(
        SimpleString::from("_AMQ_DUPL_ID"),
        PropertyValue::String(SimpleString::from("c1")),
    );
    tx.send(message, None).await.unwrap();
    tx.commit().unwrap();
    // Not waited for: the commit's record may not be written yet.
    let pending = tx.take_completion();
    send(&mut retry, Some("c1")).await.unwrap();
    pending.wait().await.unwrap();
    assert_eq!(
        count(&broker).await,
        Json::Long(1),
        "the retry was delivered too"
    );
    // Once applied, the ID is in the cache.
    send(&mut retry, Some("c1")).await.unwrap();
    assert_eq!(count(&broker).await, Json::Long(1));
    tx.close(false).await;
    retry.close(false).await;
    broker.stop().unwrap();
}

/// A management move to an address full under `FAIL` is refused as a send
/// there is, and the message stays where it was: Artemis's `page` throws out
/// of the move's routing and its transaction rolls back.
#[tokio::test]
async fn a_move_to_a_full_fail_address_is_refused_and_keeps_the_message() {
    for persistence in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start(dir.path(), persistence).await;
        create_full_queue(&broker).await;
        op(
            &broker,
            "broker",
            "createQueue",
            vec![
                Json::from("src"),
                Json::from("ANYCAST"),
                Json::from("src"),
                Json::Null,
                Json::Bool(true),
                Json::Long(-1),
                Json::Bool(false),
                Json::Bool(true),
            ],
        )
        .await;
        let mut s = session(&broker);
        let mut filled = 0;
        while send(&mut s, None).await.is_ok() {
            filled += 1;
            assert!(filled < 10, "the address never filled");
        }
        let held = count(&broker).await;
        let message = CoreMessage {
            address: Some(SimpleString::from("src")),
            durable: true,
            body: vec![7u8; 4000].into(),
            ..Default::default()
        };
        s.send(message, None).await.unwrap();
        s.take_completion().wait().await.unwrap();

        let moved = broker
            .management()
            .invoke(
                &broker,
                "queue.src",
                "moveMessages",
                &[Json::Null, Json::from("full")],
                Default::default(),
            )
            .await;
        assert!(
            moved.is_err(),
            "persistence {persistence}: the move must be refused: {moved:?}"
        );
        assert_eq!(
            op(&broker, "queue.src", "countMessages", vec![]).await,
            Json::Long(1),
            "persistence {persistence}: the message stays on its queue"
        );
        assert_eq!(count(&broker).await, held, "persistence {persistence}");
        s.close(false).await;
        broker.stop().unwrap();
    }
}

/// With `id-cache-size` 0 duplicate detection is off, as Artemis's
/// `NoOpDuplicateIDCache` has it: an ID is never a duplicate, and nothing is
/// journaled for it. A record journaled for it would never be deleted, and
/// a later start with a cache would load the IDs back.
#[tokio::test]
async fn a_cache_of_size_zero_journals_no_ids() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_with(dir.path(), true, "<id-cache-size>0</id-cache-size>").await;
    create_full_queue(&broker).await;
    let mut s = session(&broker);
    for id in ["d1", "d2", "d3", "d1"] {
        send(&mut s, Some(id)).await.unwrap();
        assert_eq!(count(&broker).await, Json::Long(1), "{id} delivered");
        op(&broker, "queue.full", "removeAllMessages", vec![]).await;
    }
    s.close(false).await;
    broker.stop().unwrap();
    drop(s);
    drop(broker);

    // A cache now: had the IDs been journaled, d1 would load as a duplicate.
    let broker = start_with(dir.path(), true, "<id-cache-size>10</id-cache-size>").await;
    let mut s = session(&broker);
    send(&mut s, Some("d1")).await.unwrap();
    assert_eq!(count(&broker).await, Json::Long(1), "no ID was journaled");
    s.close(false).await;
    broker.stop().unwrap();
}

/// With duplicate detection off (`id-cache-size` 0), one transaction can
/// send an ID twice: both are delivered and the commit succeeds.
#[tokio::test]
async fn a_transaction_sends_an_id_twice_with_detection_off() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_with(dir.path(), false, "<id-cache-size>0</id-cache-size>").await;
    create_full_queue(&broker).await;
    let mut s = session_with(&broker, false);
    send(&mut s, Some("d1")).await.unwrap();
    send(&mut s, Some("d1")).await.unwrap();
    s.commit().expect("the commit succeeds");
    s.take_completion().wait().await.unwrap();
    assert_eq!(count(&broker).await, Json::Long(2), "both delivered");
    s.close(false).await;
    broker.stop().unwrap();
}
