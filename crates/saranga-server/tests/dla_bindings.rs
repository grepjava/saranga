//! The dead-letter and expiry addresses are checked for bindings as a
//! message sent there would find them (`lookupBindingsForAddress`): an
//! FQQN's address, and the wildcard addresses matching it. A dead-letter
//! address given as `address::queue`, or reached only through a wildcard
//! queue, takes the message instead of it being dropped.

use std::sync::Arc;
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{Selection, SelectionAction};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn start(dir: &std::path::Path, dla: &str, expiry: &str) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <address-settings><address-setting match="work">
               <dead-letter-address>{dla}</dead-letter-address>
               <expiry-address>{expiry}</expiry-address>
             </address-setting></address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    Broker::start(BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap())
        .await
        .unwrap()
}

fn queue(b: &Arc<Broker>, name: &str, address: &str, routing: RoutingType) {
    let mut spec = QueueSpec::new(name, address);
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(routing);
    b.post_office().create_queue(spec, false).unwrap();
}

async fn send_work(b: &Arc<Broker>) {
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
    s.send(
        CoreMessage {
            address: Some("work".into()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.take_completion().wait().await.unwrap();
}

async fn count(b: &Arc<Broker>, queue: &str, n: i64) {
    let q = b.post_office().queue(queue).unwrap();
    for _ in 0..300 {
        if q.message_count() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(q.message_count(), n, "{queue}");
}

#[tokio::test]
async fn a_dead_letter_fqqn_takes_the_message() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), "DLQ::dlq_q", "EXP::exp_q").await;
    queue(&b, "work", "work", RoutingType::Anycast);
    queue(&b, "dlq_q", "DLQ", RoutingType::Anycast);
    queue(&b, "exp_q", "EXP", RoutingType::Anycast);
    let work = b.post_office().queue("work").unwrap();
    for (action, target) in [
        (SelectionAction::SendToDeadLetter, "dlq_q"),
        (SelectionAction::Expire, "exp_q"),
    ] {
        send_work(&b).await;
        count(&b, "work", 1).await;
        assert_eq!(work.act(Selection::filter(None), action).await, 1);
        count(&b, target, 1).await;
    }
    b.stop().unwrap();
}

#[tokio::test]
async fn a_dead_letter_address_bound_by_a_wildcard_takes_the_message() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), "dlq.work", "exp.work").await;
    queue(&b, "work", "work", RoutingType::Anycast);
    queue(&b, "all_dlq", "dlq.#", RoutingType::Multicast);
    queue(&b, "all_exp", "exp.#", RoutingType::Multicast);
    let work = b.post_office().queue("work").unwrap();
    for (action, target) in [
        (SelectionAction::SendToDeadLetter, "all_dlq"),
        (SelectionAction::Expire, "all_exp"),
    ] {
        send_work(&b).await;
        count(&b, "work", 1).await;
        assert_eq!(work.act(Selection::filter(None), action).await, 1);
        count(&b, target, 1).await;
    }
    b.stop().unwrap();
}

/// An address bound only by another broker's queue is bound: it is not
/// removed, and it takes dead letters (`getDirectBindings` and the
/// routing bindings both count `RemoteQueueBinding`s).
#[tokio::test]
async fn an_address_bound_by_another_broker_is_bound() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), "remote.dlq", "EXP").await;
    queue(&b, "work", "work", RoutingType::Anycast);
    queue(&b, "sf", "sf", RoutingType::Anycast);
    let po = b.post_office();
    po.add_address_info(saranga_server::postoffice::AddressInfo {
        id: 0,
        name: "remote.dlq".into(),
        routing_types: saranga_server::postoffice::RoutingTypes::of(RoutingType::Multicast),
        auto_created: true,
        temporary: false,
        internal: false,
    })
    .unwrap();
    assert!(!po.is_address_bound("remote.dlq"));
    po.add_remote_binding(Arc::new(
        saranga_server::remote_binding::RemoteBinding::new(
            po.ids().next(),
            "remote.dlq",
            "remote.dlq.q.node2",
            "q",
            7,
            None,
            po.queue("sf").unwrap(),
            "bridge",
            1,
            saranga_config::MessageLoadBalancing::OnDemand,
        ),
    ));
    assert!(po.is_address_bound("remote.dlq"));
    assert!(po.has_bindings("remote.dlq"));
    assert!(
        !po.remove_address("remote.dlq"),
        "bound by the other broker"
    );
    assert!(po.address_info("remote.dlq").is_some());
    b.stop().unwrap();
}
