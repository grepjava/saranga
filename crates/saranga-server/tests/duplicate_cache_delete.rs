//! An address's duplicate-ID cache goes with it (`deleteDuplicateCache`),
//! and with its last queue (`removeBinding` with `deleteData`): a queue
//! made there again takes a message whose ID the old one had seen.

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

fn session(b: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        b.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

async fn send(s: &mut ServerSession) {
    let mut m = CoreMessage {
        address: Some("q".into()),
        durable: true,
        ..Default::default()
    };
    m.properties.insert(
        SimpleString::from("_AMQ_DUPL_ID"),
        PropertyValue::String(SimpleString::from("k1")),
    );
    s.send(m, None).await.unwrap();
    s.take_completion().wait().await.unwrap();
}

fn create(b: &Arc<Broker>) {
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
}

async fn count(b: &Arc<Broker>, n: i64) {
    let q = b.post_office().queue("q").unwrap();
    for _ in 0..300 {
        if q.message_count() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(q.message_count(), n);
}

#[tokio::test]
async fn a_queue_made_again_takes_an_id_the_old_one_saw() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let mut s = session(&b);
    for remove_address in [false, true] {
        create(&b);
        send(&mut s).await;
        count(&b, 1).await;
        b.post_office().delete_queue("q", false, false).unwrap();
        if remove_address {
            assert!(b.post_office().remove_address("q"));
        }
        create(&b);
        send(&mut s).await;
        count(&b, 1).await;
        b.post_office().delete_queue("q", false, false).unwrap();
    }
    b.stop().unwrap();
}
