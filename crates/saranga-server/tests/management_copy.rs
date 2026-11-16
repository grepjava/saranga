//! Management's copy (`copyMessage`) routes the copy as a sent message is
//! routed: one scheduled for later stays scheduled, the destination's
//! expiry delay applies, and the original's original-headers are kept
//! (`makeCopy` with `copyOriginalHeaders` false), so a copied dead letter
//! can still be retried. A move's scheduled copy keeps its time across a
//! restart.

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{Selection, SelectionAction};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn start(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings><address-setting match="dst">
               <expiry-delay>600000</expiry-delay>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap(),
    )
    .await
    .unwrap();
    for name in ["src", "dst"] {
        if b.post_office().queue(name).is_none() {
            let mut spec = QueueSpec::new(name, name);
            spec.auto_create_address = Some(true);
            spec.routing_type = Some(RoutingType::Anycast);
            spec.durable = true;
            b.post_office().create_queue(spec, false).unwrap();
        }
    }
    b
}

async fn send(b: &Arc<Broker>, scheduled: bool) {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut s = ServerSession::new(
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
    );
    let mut m = CoreMessage {
        address: Some("src".into()),
        durable: true,
        ..Default::default()
    };
    m.properties.insert(
        SimpleString::from("_AMQ_ORIG_ADDRESS"),
        PropertyValue::String(SimpleString::from("first")),
    );
    if scheduled {
        let later = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3_600_000;
        m.properties.insert(
            SimpleString::from("_AMQ_SCHED_DELIVERY"),
            PropertyValue::Long(later),
        );
    }
    s.send(m, None).await.unwrap();
    s.take_completion().wait().await.unwrap();
}

async fn scheduled(b: &Arc<Broker>, queue: &str, n: i64) {
    let q = b.post_office().queue(queue).unwrap();
    for _ in 0..300 {
        if q.stats().await.scheduled_count == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(q.stats().await.scheduled_count, n, "{queue}");
}

#[tokio::test]
async fn a_copy_is_routed_as_a_sent_message() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path()).await;
    send(&b, true).await;
    scheduled(&b, "src", 1).await;
    let src = b.post_office().queue("src").unwrap();
    let dst = b.post_office().queue("dst").unwrap();
    let n = src
        .act(
            Selection::filter(None),
            SelectionAction::Copy {
                address: "dst".into(),
                target: dst.clone(),
            },
        )
        .await;
    assert_eq!(n, 1);
    scheduled(&b, "dst", 1).await;
    scheduled(&b, "src", 1).await;
    let copy = dst.browse().await.pop().expect("the copy");
    let core = copy.message.core();
    assert!(core.expiration > 0, "the destination's expiry delay");
    assert_eq!(
        core.properties
            .get(&SimpleString::from("_AMQ_ORIG_ADDRESS")),
        Some(&PropertyValue::String(SimpleString::from("first"))),
        "the original's original address"
    );
    b.stop().unwrap();
}

#[tokio::test]
async fn a_moved_scheduled_message_stays_scheduled_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path()).await;
    send(&b, true).await;
    scheduled(&b, "src", 1).await;
    let src = b.post_office().queue("src").unwrap();
    let n = src
        .act(
            Selection::filter(None),
            SelectionAction::Move {
                address: "dst".into(),
                target: None,
                reject_duplicates: false,
            },
        )
        .await;
    assert_eq!(n, 1);
    scheduled(&b, "dst", 1).await;
    b.stop().unwrap();
    drop(b);
    let b = start(dir.path()).await;
    scheduled(&b, "dst", 1).await;
    b.stop().unwrap();
}
