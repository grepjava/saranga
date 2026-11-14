//! Routing through diverts, with a transformer plugin.

use std::collections::HashMap;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::{HDR_ORIGINAL_ADDRESS, MessageRef, ServerMessage};
use saranga_server::postoffice::{
    DivertDeployment, DivertRoutingType, QueueSpec, RoutingTypes, SelectorFilters, Transformer,
};
use saranga_server::{AddressDeployment, Broker, BrokerConfig, RoutingType};

/// Tags each copy with a property named in its configuration.
struct Tag(String);

impl Transformer for Tag {
    fn transform(&self, mut message: CoreMessage) -> CoreMessage {
        message
            .properties
            .insert(self.0.as_str().into(), PropertyValue::Bool(true));
        message
    }
}

fn anycast(name: &str) -> AddressDeployment {
    let mut q = QueueSpec::new(name, name);
    q.routing_type = Some(RoutingType::Anycast);
    AddressDeployment {
        name: name.to_owned(),
        routing_types: RoutingTypes::of(RoutingType::Anycast),
        queues: vec![q],
    }
}

fn divert(exclusive: bool) -> DivertDeployment {
    DivertDeployment {
        name: "d".to_owned(),
        routing_name: None,
        address: "src".to_owned(),
        forwarding_address: "dst".to_owned(),
        exclusive,
        filter: Some("color = 'red'".to_owned()),
        routing_type: DivertRoutingType::Strip,
        transformer: Some((
            "tag".to_owned(),
            HashMap::from([("name".to_owned(), "tagged".to_owned())]),
        )),
    }
}

async fn broker(exclusive: bool) -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.addresses = vec![anycast("src"), anycast("dst")];
    config.diverts = vec![divert(exclusive)];
    config.transformers.insert(
        "tag".to_owned(),
        Arc::new(|p: &HashMap<String, String>| {
            Arc::new(Tag(p["name"].clone())) as Arc<dyn Transformer>
        }),
    );
    Broker::start(config).await.unwrap()
}

fn message(id: i64, color: &str) -> MessageRef {
    let mut core = CoreMessage {
        message_id: id,
        address: Some(SimpleString::from("src")),
        ..Default::default()
    };
    core.properties.insert(
        "color".into(),
        PropertyValue::String(SimpleString::from(color)),
    );
    MessageRef::new(ServerMessage::new(core))
}

fn queues(routed: &[(MessageRef, Vec<saranga_server::queue::Queue>)]) -> Vec<String> {
    routed
        .iter()
        .flat_map(|(_, qs)| qs.iter().map(|q| q.name().to_string()))
        .collect()
}

#[tokio::test]
async fn exclusive_divert_takes_matching_messages() {
    let b = broker(true).await;
    let routed = b
        .post_office()
        .route(&message(1, "red"), "src", None, Default::default())
        .unwrap();
    assert_eq!(queues(&routed), ["dst"]);
    let copy = routed[0].0.message.core();
    assert_ne!(copy.message_id, 1);
    assert_eq!(
        copy.properties.get(&"tagged".into()),
        Some(&PropertyValue::Bool(true))
    );
    assert_eq!(
        copy.properties.get(&HDR_ORIGINAL_ADDRESS.into()),
        Some(&PropertyValue::String(SimpleString::from("src")))
    );
    // A message the filter does not match stays on its address.
    let routed = b
        .post_office()
        .route(&message(2, "blue"), "src", None, Default::default())
        .unwrap();
    assert_eq!(queues(&routed), ["src"]);
}

#[tokio::test]
async fn non_exclusive_divert_copies() {
    let b = broker(false).await;
    let routed = b
        .post_office()
        .route(&message(1, "red"), "src", None, Default::default())
        .unwrap();
    assert_eq!(queues(&routed), ["src", "dst"]);
    assert_eq!(routed[0].0.message_id(), 1);
}

/// `_AMQ_ROUTE_TO` (as `artemis data imp` sends): the message goes to the
/// queues it names, not round robin, and loses the header.
#[tokio::test]
async fn route_to_ids_pick_the_queues() {
    let b = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let po = b.post_office();
    let spec = |name: &str| {
        let mut q = QueueSpec::new(name, "to");
        q.routing_type = Some(RoutingType::Anycast);
        q.auto_create_address = Some(true);
        po.create_queue(q, false).unwrap()
    };
    let (_a, b2, c) = (spec("a"), spec("b"), spec("c"));
    let mut ids = b2.id().to_be_bytes().to_vec();
    ids.extend(c.id().to_be_bytes());
    ids.extend(12345_i64.to_be_bytes());
    let mut core = CoreMessage {
        message_id: 1,
        address: Some(SimpleString::from("to")),
        ..Default::default()
    };
    core.properties
        .insert("_AMQ_ROUTE_TO".into(), PropertyValue::Bytes(ids));
    let routed = po
        .route(
            &MessageRef::new(ServerMessage::new(core)),
            "to",
            None,
            Default::default(),
        )
        .unwrap();
    assert_eq!(queues(&routed), ["b", "c"]);
    assert!(
        routed[0]
            .0
            .message
            .core()
            .properties
            .get(&"_AMQ_ROUTE_TO".into())
            .is_none()
    );
}

/// An anycast address with a queue that takes nothing now: every message
/// goes to the queue that does, none is lost to the other's turn.
#[tokio::test]
async fn a_queue_that_takes_nothing_gives_up_its_turn() {
    for (case, set) in [
        (
            "disabled",
            (|q: &mut QueueSpec| q.enabled = Some(false)) as fn(&mut QueueSpec),
        ),
        ("purging", |q: &mut QueueSpec| {
            q.purge_on_no_consumers = Some(true)
        }),
    ] {
        let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
        let mut address = anycast("orders");
        let mut idle = QueueSpec::new("idle", "orders");
        idle.routing_type = Some(RoutingType::Anycast);
        set(&mut idle);
        address.queues.insert(0, idle);
        config.addresses = vec![address];
        let b = Broker::start(config).await.unwrap();
        let mut to = Vec::new();
        for id in 1..=4 {
            let m = MessageRef::new(ServerMessage::new(CoreMessage {
                message_id: id,
                address: Some(SimpleString::from("orders")),
                ..Default::default()
            }));
            let routed = b
                .post_office()
                .route(&m, "orders", None, Default::default())
                .unwrap();
            to.extend(queues(&routed));
        }
        assert_eq!(to, ["orders"; 4], "{case}");
        b.stop().unwrap();
    }
}

/// A broker whose addresses send what reaches no queue to `DLQ`, with
/// anycast queues `names` (a filter for any given one).
async fn dla_broker(queues: &[(&str, Option<&str>)]) -> Arc<Broker> {
    use saranga_server::settings::{AddressSettings, UniformSettings};
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.settings = Arc::new(UniformSettings(Arc::new(AddressSettings {
        send_to_dla_on_no_route: true,
        dead_letter_address: Some("DLQ".into()),
        ..Default::default()
    })));
    let b = Broker::start(config).await.unwrap();
    for (name, filter) in queues.iter().chain([&("DLQ", None)]) {
        let mut spec = QueueSpec::new(*name, *name);
        spec.auto_create_address = Some(true);
        spec.routing_type = Some(RoutingType::Anycast);
        spec.filter_string = filter.map(str::to_owned);
        b.post_office().create_queue(spec, false).unwrap();
    }
    b
}

/// `send-to-dla-on-no-route` takes a message the address's queues all turn
/// away (`simpleRoute` gives no bindings when no queue took it), not only
/// one sent to an address with no queue.
#[tokio::test]
async fn a_message_every_filter_rejects_goes_to_the_dead_letter_address() {
    let b = dla_broker(&[("orders", Some("color = 'red'"))]).await;
    let to_orders = |id, color| {
        let mut core = message(id, color).message.core().clone();
        core.address = Some(SimpleString::from("orders"));
        MessageRef::new(ServerMessage::new(core))
    };
    let routed = b
        .post_office()
        .route(&to_orders(1, "blue"), "orders", None, Default::default())
        .unwrap();
    assert_eq!(queues(&routed), ["DLQ"]);
    // One the filter takes goes to its queue only.
    let routed = b
        .post_office()
        .route(&to_orders(2, "red"), "orders", None, Default::default())
        .unwrap();
    assert_eq!(queues(&routed), ["orders"]);
}

/// As in Artemis, a divert's copy routes on its own: a message only an
/// exclusive divert took reached no queue of its address, and goes to the
/// dead-letter address as well.
#[tokio::test]
async fn a_message_only_a_divert_took_also_goes_to_the_dead_letter_address() {
    let b = dla_broker(&[("src", None), ("dst", None)]).await;
    b.post_office()
        .deploy_divert(
            &DivertDeployment {
                filter: None,
                transformer: None,
                ..divert(true)
            },
            &Default::default(),
        )
        .unwrap();
    let routed = b
        .post_office()
        .route(&message(1, "red"), "src", None, Default::default())
        .unwrap();
    let mut names = queues(&routed);
    names.sort();
    assert_eq!(names, ["DLQ", "dst"]);
}

/// Non-exclusive diverts sharing a routing name are one binding group: each
/// message goes through one of them, in turn (`BindingsImpl` routing-name
/// groups), not through all.
#[tokio::test]
async fn diverts_sharing_a_routing_name_take_turns() {
    let b = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    for name in ["src", "dst1", "dst2"] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.routing_type = Some(RoutingType::Anycast);
        b.post_office().create_queue(spec, false).unwrap();
    }
    for name in ["dst1", "dst2"] {
        b.post_office()
            .deploy_divert(
                &DivertDeployment {
                    name: name.to_owned(),
                    routing_name: Some("shared".to_owned()),
                    address: "src".to_owned(),
                    forwarding_address: name.to_owned(),
                    exclusive: false,
                    filter: None,
                    routing_type: DivertRoutingType::Strip,
                    transformer: None,
                },
                &Default::default(),
            )
            .unwrap();
    }
    let mut seen = Vec::new();
    for id in 1..=4 {
        let routed = b
            .post_office()
            .route(&message(id, "red"), "src", None, Default::default())
            .unwrap();
        let names = queues(&routed);
        assert_eq!(names.len(), 2, "the source and one copy: {names:?}");
        assert_eq!(names[0], "src");
        seen.push(names[1].clone());
    }
    assert_eq!(seen, ["dst1", "dst2", "dst1", "dst2"]);
}
