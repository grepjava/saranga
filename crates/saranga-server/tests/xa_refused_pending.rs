//! A session's ended XA branch whose commit it refused (a two-phase commit
//! of a branch not prepared: see COMPAT.md) stays the session's, so the
//! session's failure rolls it back as it would have before the commit.

use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig, RoutingType};

#[tokio::test]
async fn a_refused_commit_leaves_the_branch_to_the_session() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.routing_type = Some(RoutingType::Anycast);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    let mut s = ServerSession::new(
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
    );
    let xid = Xid {
        format_id: 1,
        branch_qualifier: b"b".to_vec(),
        global_transaction_id: b"g".to_vec(),
    };
    s.xa_start(xid.clone()).unwrap();
    s.send(
        CoreMessage {
            address: Some("q".into()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.xa_end(xid.clone()).unwrap();
    assert!(s.xa_commit(xid.clone(), false).is_err(), "not prepared");
    let rm = broker.resource_manager();
    assert!(rm.get(&xid).is_some(), "still registered");
    s.close(true).await;
    assert!(rm.get(&xid).is_none(), "rolled back with the session");
    assert_eq!(q.message_count(), 0);
    broker.stop().unwrap();
}
