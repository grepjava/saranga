//! With `populate-validated-user` and no validated user (security off), a
//! user the producer put on its message is dropped: the broker populates the
//! user, so `reject-empty-validated-user` refuses the send.

use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn start(reject_empty: bool) -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.populate_validated_user = true;
    config.reject_empty_validated_user = reject_empty;
    let b = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    b.post_office().create_queue(spec, false).unwrap();
    b
}

fn session(b: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
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

fn claimed() -> CoreMessage {
    let mut m = CoreMessage {
        address: Some("q".into()),
        ..Default::default()
    };
    m.properties.insert(
        SimpleString::from("_AMQ_VALIDATED_USER"),
        PropertyValue::String(SimpleString::from("admin")),
    );
    m
}

#[tokio::test]
async fn a_claimed_user_is_rejected_as_empty() {
    let b = start(true).await;
    let mut s = session(&b);
    assert!(s.send(claimed(), None).await.is_err());
    assert_eq!(b.post_office().queue("q").unwrap().message_count(), 0);
    b.stop().unwrap();
}

#[tokio::test]
async fn a_claimed_user_is_dropped() {
    let b = start(false).await;
    let mut s = session(&b);
    s.send(claimed(), None).await.unwrap();
    let q = b.post_office().queue("q").unwrap();
    let held = q.browse().await;
    assert_eq!(held.len(), 1);
    assert!(
        held[0]
            .message
            .core()
            .properties
            .get(&SimpleString::from("_AMQ_VALIDATED_USER"))
            .is_none()
    );
    b.stop().unwrap();
}
