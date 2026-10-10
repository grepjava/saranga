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
        .route(&message(1, "red"), "src", None)
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
        .route(&message(2, "blue"), "src", None)
        .unwrap();
    assert_eq!(queues(&routed), ["src"]);
}

#[tokio::test]
async fn non_exclusive_divert_copies() {
    let b = broker(false).await;
    let routed = b
        .post_office()
        .route(&message(1, "red"), "src", None)
        .unwrap();
    assert_eq!(queues(&routed), ["src", "dst"]);
    assert_eq!(routed[0].0.message_id(), 1);
}
