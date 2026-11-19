//! The routing round robin's positions (`BindingIndex`, kept in each
//! address's `Bindings` in Artemis) go with their address and with the last
//! binding of their routing name, so routing to many short-lived addresses
//! does not grow them without bound.

use std::sync::Arc;

use saranga_buffer::SimpleString;
use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{AddressInfo, QueueSpec, RoutingTypes, SelectorFilters};
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

fn message() -> MessageRef {
    MessageRef::new(ServerMessage::new(CoreMessage {
        message_id: 1,
        address: Some(SimpleString::from("x")),
        ..Default::default()
    }))
}

#[tokio::test]
async fn positions_go_with_their_addresses() {
    let b = broker().await;
    let po = b.post_office();
    // Two subscriptions (a routing name each) and two anycast queues (one
    // routing name, taking turns) on wildcard addresses.
    for (name, address, rt) in [
        ("w1", "news.#", RoutingType::Multicast),
        ("w2", "news.#", RoutingType::Multicast),
        ("o1", "orders.#", RoutingType::Anycast),
        ("o2", "orders.#", RoutingType::Anycast),
    ] {
        po.create_queue(spec(name, address, rt), false).unwrap();
    }
    let m = message();
    for i in 0..1000 {
        for (address, rt) in [
            (format!("news.{i}"), RoutingType::Multicast),
            (format!("orders.{i}"), RoutingType::Anycast),
        ] {
            // Auto-created by the send, deleted once idle.
            po.add_address_info(AddressInfo {
                id: 0,
                name: address.as_str().into(),
                routing_types: RoutingTypes::of(rt),
                auto_created: true,
                temporary: false,
                internal: false,
            })
            .unwrap();
            po.route(&m, &address, Some(rt), Default::default())
                .unwrap();
            assert!(po.remove_address(&address));
        }
    }
    assert_eq!(po.routing_positions(), 0);
    b.stop().unwrap();
}

#[tokio::test]
async fn positions_go_with_the_last_binding_of_their_routing_name() {
    let b = broker().await;
    let po = b.post_office();
    for name in ["a1", "a2"] {
        po.create_queue(spec(name, "a", RoutingType::Anycast), false)
            .unwrap();
    }
    po.route(
        &message(),
        "a",
        Some(RoutingType::Anycast),
        Default::default(),
    )
    .unwrap();
    assert_eq!(po.routing_positions(), 1, "a1 had the turn, a2 has it next");
    po.delete_queue("a1", false, false).unwrap();
    assert_eq!(po.routing_positions(), 1, "a2 is bound still");
    po.delete_queue("a2", false, false).unwrap();
    assert_eq!(po.routing_positions(), 0);
    b.stop().unwrap();
}
