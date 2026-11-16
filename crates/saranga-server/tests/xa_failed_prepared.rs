//! `SessionXAAfterFailedMessage` for a branch already prepared: the branch
//! is the transaction manager's to end (`isEffective` includes PREPARED),
//! so it stays in doubt and commits.

use std::sync::Arc;

use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

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
            name: "s".into(),
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

#[tokio::test]
async fn a_failed_client_leaves_a_prepared_branch_in_doubt() {
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    let queue = broker.post_office().create_queue(spec, false).unwrap();
    let xid = Xid {
        format_id: 1,
        branch_qualifier: b"b1".to_vec(),
        global_transaction_id: b"g1".to_vec(),
    };

    let mut s = session(&broker);
    s.xa_start(xid.clone()).unwrap();
    s.send(
        CoreMessage {
            address: Some("q".into()),
            durable: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.xa_end(xid.clone()).unwrap();
    s.xa_prepare(xid.clone()).unwrap();
    s.take_completion().wait().await.unwrap();
    assert_eq!(broker.resource_manager().in_doubt().len(), 1);

    // A client failing over tells the broker of the branch it had.
    session(&broker).xa_failed(xid.clone());
    assert_eq!(
        broker.resource_manager().in_doubt().len(),
        1,
        "the prepared branch is still in doubt"
    );

    let mut tm = session(&broker);
    tm.xa_commit(xid, false).unwrap();
    tm.take_completion().wait().await.unwrap();
    assert_eq!(queue.message_count(), 1, "the prepared branch commits");
    broker.stop().unwrap();
}
