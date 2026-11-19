//! `_AMQ_ROUTE_TO` on an address a wildcard address matches: the wildcard
//! address's queues and exclusive diverts are the address's too
//! (WildcardAddressManager puts them in its Bindings), so a cluster
//! bridge's or `data imp`'s message reaches a wildcard queue, and an
//! exclusive wildcard divert takes it first.

use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{DivertDeployment, DivertRoutingType, QueueSpec, SelectorFilters};
use saranga_server::queue::Queue;
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn broker() -> Arc<Broker> {
    Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap()
}

fn spec(name: &str, address: &str, rt: RoutingType) -> QueueSpec {
    let mut s = QueueSpec::new(name, address);
    s.auto_create_address = Some(true);
    s.routing_type = Some(rt);
    s
}

fn route_to_message(address: &str, id: i64) -> MessageRef {
    let mut core = CoreMessage {
        message_id: 1,
        address: Some(SimpleString::from(address)),
        ..Default::default()
    };
    core.properties.insert(
        "_AMQ_ROUTE_TO".into(),
        PropertyValue::Bytes(id.to_be_bytes().to_vec()),
    );
    MessageRef::new(ServerMessage::new(core))
}

fn names(routed: &[(MessageRef, Vec<Queue>)]) -> Vec<String> {
    routed
        .iter()
        .flat_map(|(_, qs)| qs.iter().map(|q| q.name().to_string()))
        .collect()
}

#[tokio::test]
async fn route_to_reaches_a_wildcard_queue() {
    let b = broker().await;
    let po = b.post_office();
    let w = po
        .create_queue(spec("w", "news.#", RoutingType::Multicast), false)
        .unwrap();
    po.create_queue(spec("a", "news.a", RoutingType::Multicast), false)
        .unwrap();
    let m = route_to_message("news.a", w.id());
    let routed = po.route(&m, "news.a", None, Default::default()).unwrap();
    assert_eq!(names(&routed), ["w"]);
    b.stop().unwrap();
}

#[tokio::test]
async fn an_exclusive_wildcard_divert_takes_a_route_to_message() {
    let b = broker().await;
    let po = b.post_office();
    let a = po
        .create_queue(spec("a", "news.a", RoutingType::Multicast), false)
        .unwrap();
    po.create_queue(spec("dst", "dst", RoutingType::Multicast), false)
        .unwrap();
    po.create_queue(spec("wq", "news.#", RoutingType::Multicast), false)
        .unwrap();
    po.deploy_divert(
        &DivertDeployment {
            name: "d".to_owned(),
            routing_name: None,
            address: "news.#".to_owned(),
            forwarding_address: "dst".to_owned(),
            exclusive: true,
            filter: None,
            routing_type: DivertRoutingType::Strip,
            transformer: None,
        },
        &Default::default(),
    )
    .unwrap();
    let m = route_to_message("news.a", a.id());
    let routed = po.route(&m, "news.a", None, Default::default()).unwrap();
    assert_eq!(names(&routed), ["dst"]);
    b.stop().unwrap();
}

/// Another broker's queue on a wildcard address is a binding of the
/// addresses it matches: a message sent to one of them goes to it (through
/// its store-and-forward queue), until the binding goes.
#[tokio::test]
async fn a_remote_wildcard_queue_takes_matching_messages() {
    let b = broker().await;
    let po = b.post_office();
    let sf = po
        .create_queue(spec("sf", "sf", RoutingType::Anycast), false)
        .unwrap();
    assert!(!po.has_bindings("news.a"));
    po.add_remote_binding(Arc::new(
        saranga_server::remote_binding::RemoteBinding::new(
            po.ids().next(),
            "news.#",
            "w.node2",
            "w",
            7,
            None,
            sf,
            "bridge",
            1,
            saranga_config::MessageLoadBalancing::OnDemand,
        ),
    ));
    assert!(po.has_bindings("news.a"));
    let plain = MessageRef::new(ServerMessage::new(CoreMessage {
        message_id: 2,
        address: Some(SimpleString::from("news.a")),
        ..Default::default()
    }));
    let routed = po
        .route(&plain, "news.a", None, Default::default())
        .unwrap();
    assert_eq!(names(&routed), ["sf"]);
    po.remove_remote_binding("w.node2").unwrap();
    assert!(!po.has_bindings("news.a"));
    b.stop().unwrap();
}
