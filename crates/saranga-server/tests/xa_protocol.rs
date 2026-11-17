//! XA calls as the XA specification answers them, where Artemis does not:
//! no joining a prepared branch or joining over another transaction, a
//! timeout of 0 is the default, and a branch the timeout rolled back ends
//! with `XA_RBTIMEOUT`.

use std::sync::Arc;
use std::time::Duration;

use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::{TxError, Xid, xa_code};
use saranga_server::{Broker, BrokerConfig};

async fn broker(timeout_ms: i64) -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.transaction_timeout = timeout_ms;
    config.transaction_timeout_scan_period = 50;
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "x".into(),
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

fn xid(g: &[u8]) -> Xid {
    Xid {
        format_id: 1,
        branch_qualifier: b"b".to_vec(),
        global_transaction_id: g.to_vec(),
    }
}

fn code<T: std::fmt::Debug>(r: Result<T, TxError>) -> i32 {
    match r {
        Err(TxError::Xa(e)) => e.code,
        other => panic!("not an XA error: {other:?}"),
    }
}

#[tokio::test]
async fn a_prepared_branch_cannot_be_joined() {
    let b = broker(300_000).await;
    let mut s = session(&b);
    s.xa_start(xid(b"g")).unwrap();
    s.xa_end(xid(b"g")).unwrap();
    s.xa_prepare(xid(b"g")).unwrap();
    let mut other = session(&b);
    assert_eq!(code(other.xa_join(xid(b"g"))), xa_code::XAER_PROTO);
    b.stop().unwrap();
}

#[tokio::test]
async fn a_session_in_a_transaction_cannot_join_another() {
    let b = broker(300_000).await;
    let mut s = session(&b);
    s.xa_start(xid(b"g1")).unwrap();
    s.xa_end(xid(b"g1")).unwrap();
    let mut other = session(&b);
    other.xa_start(xid(b"g2")).unwrap();
    assert_eq!(code(other.xa_join(xid(b"g1"))), xa_code::XAER_PROTO);
    // Its own transaction is still its.
    other.xa_end(xid(b"g2")).unwrap();
    // Free of it, it joins.
    other.xa_join(xid(b"g1")).unwrap();
    b.stop().unwrap();
}

#[tokio::test]
async fn a_timeout_of_zero_is_the_default() {
    let b = broker(300_000).await;
    let mut s = session(&b);
    assert_eq!(s.xa_timeout(), 300);
    s.xa_set_timeout(7);
    assert_eq!(s.xa_timeout(), 7);
    s.xa_set_timeout(0);
    assert_eq!(s.xa_timeout(), 300);
    s.xa_start(xid(b"g")).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        b.resource_manager().get(&xid(b"g")).is_some(),
        "timed out at once"
    );
    s.xa_end(xid(b"g")).unwrap();
    b.stop().unwrap();
}

#[tokio::test]
async fn a_branch_the_timeout_rolled_back_ends_with_rbtimeout() {
    let b = broker(1_000).await;
    let mut s = session(&b);
    s.xa_set_timeout(1);
    s.xa_start(xid(b"g")).unwrap();
    for _ in 0..100 {
        if b.resource_manager().get(&xid(b"g")).is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        b.resource_manager().get(&xid(b"g")).is_none(),
        "not timed out"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(code(s.xa_end(xid(b"g"))), xa_code::XA_RBTIMEOUT);
    b.stop().unwrap();
}
