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
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
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
