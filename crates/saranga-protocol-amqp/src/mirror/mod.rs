//! Mirroring (`connect/mirror`): a broker connection's `<mirror>` sends
//! everything its broker routes, acknowledges, adds and removes to the
//! peer ([`source`], `AMQPMirrorControllerSource`), and a link the peer
//! attaches to `$ACTIVEMQ_ARTEMIS_MIRROR...` applies it here ([`target`],
//! `AMQPMirrorControllerTarget`).
//!
//! On the link go the messages themselves, each with delivery annotations
//! naming its broker and its ID there, and control messages whose message
//! annotations say what happened (`AMQPMirrorMessageFactory`).

pub mod source;
pub mod target;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{AddressInfo, RoutingTypes};
use saranga_server::queue::AckReason;
use saranga_server::{Name, RoutingType};

use crate::Composite;
use crate::codec;
use crate::javamap::{JavaMap, Kind};
use crate::message::{AmqpMessage, Annotations};
use crate::messaging::{Header, section};
use crate::types::{Descriptor, Symbol, Value};

/// `AMQPMirrorControllerSource`'s annotations.
pub const EVENT_TYPE: &str = "x-opt-amq-mr-ev-type";
pub const ACK_REASON: &str = "x-opt-amq-mr-ack-reason";
pub const ADDRESS: &str = "x-opt-amq-mr-adr";
pub const QUEUE: &str = "x-opt-amq-mr-qu";
pub const BROKER_ID: &str = saranga_server::mirror::BROKER_ID;
pub const INTERNAL_ID: &str = saranga_server::mirror::INTERNAL_ID;
pub const INTERNAL_DESTINATION: &str = "x-opt-amq-mr-dst";
pub const TARGET_QUEUES: &str = "x-opt-amq-mr-trg-q";

/// The events.
pub const ADD_ADDRESS: &str = "addAddress";
pub const DELETE_ADDRESS: &str = "deleteAddress";
pub const CREATE_QUEUE: &str = "createQueue";
pub const DELETE_QUEUE: &str = "deleteQueue";
pub const POST_ACK: &str = "postAck";

/// `MIRROR_CAPABILITY`: what a mirror link desires and its target offers.
pub const MIRROR_CAPABILITY: &str = "amq.mirror";

/// `AMQPMirrorMessageFactory.createMessage`: a durable message to the
/// store-and-forward address `to`, its annotations saying what happened,
/// with `body` as its value.
pub fn control_message(
    to: &str,
    address: Option<&str>,
    queue: Option<&str>,
    event: &str,
    broker_id: Option<&str>,
    body: Option<Value>,
    reason: AckReason,
) -> AmqpMessage {
    let mut data = Vec::new();
    Header {
        durable: Some(true),
        ..Default::default()
    }
    .encode(&mut data);
    let mut annotations = Annotations::new(Kind::Hash(None));
    annotations.insert(Symbol::from(EVENT_TYPE), Value::symbol(event));
    if reason != AckReason::Normal {
        annotations.insert(
            Symbol::from(ACK_REASON),
            Value::Byte(saranga_server::mirror::reason_value(reason) as i8),
        );
    }
    if let Some(a) = address {
        annotations.insert(Symbol::from(ADDRESS), Value::string(a));
    }
    if let Some(b) = broker_id {
        annotations.insert(Symbol::from(BROKER_ID), Value::string(b));
    }
    if let Some(q) = queue {
        annotations.insert(Symbol::from(QUEUE), Value::string(q));
    }
    codec::encode(
        &Value::described(
            Descriptor::Code(section::MESSAGE_ANNOTATIONS),
            Value::Map(
                annotations
                    .iter()
                    .map(|(k, v)| (Value::Symbol(k.clone()), v.clone()))
                    .collect(),
            ),
        ),
        &mut data,
    );
    if let Some(body) = body {
        crate::message::encode_section(section::AMQP_VALUE, body, &mut data);
    }
    let now = saranga_server::message::now_millis();
    let mut m = AmqpMessage::new(0, data, now).expect("a message this end encoded");
    m.set_address(SimpleString::from(to));
    m
}

/// `AMQPMirrorControllerSource.setProtocolData` (as `validateProtocolData`
/// works it out again for a reference without it): the delivery
/// annotations a message on the mirror link carries. The broker it came
/// from (none for this broker's own), its ID there and, unless the `to`
/// field says it, its address. None for the store-and-forward queue's own
/// control messages.
pub fn delivery_annotations(message: &ServerMessage, snf_address: &str) -> Option<Annotations> {
    let address = message.core().address.as_ref().map(|a| a.to_string_lossy());
    if address.as_deref() == Some(snf_address) {
        return None;
    }
    let mut da = Annotations::new(Kind::Hash(None));
    if let Some(b) = saranga_server::mirror::server_id(message) {
        da.insert(Symbol::from(BROKER_ID), Value::string(b));
    }
    da.insert(
        Symbol::from(INTERNAL_ID),
        Value::Long(saranga_server::mirror::internal_id(message)),
    );
    if let Some(address) = address {
        let to = message
            .native()
            .and_then(|n| n.as_any().downcast_ref::<crate::native::AmqpNative>())
            .and_then(|n| n.message.properties().and_then(|p| p.to.clone()));
        if to.as_deref() != Some(address.as_str()) {
            da.insert(Symbol::from(INTERNAL_DESTINATION), Value::string(address));
        }
    }
    Some(da)
}

/// `AddressInfo.toJSON`.
pub fn address_json(info: &AddressInfo) -> String {
    use saranga_server::management::Json;
    let types: Vec<RoutingType> = [RoutingType::Multicast, RoutingType::Anycast]
        .into_iter()
        .filter(|t| info.routing_types.contains(*t))
        .collect();
    let mut o = Json::object()
        .with("id", info.id)
        .with("name", &*info.name)
        .with("auto-created", info.auto_created)
        .with("temporary", info.temporary)
        .with("internal", info.internal);
    if let Some(first) = types.first() {
        o.put("firstSeen", i64::from(first.to_byte()));
    }
    o.put(
        "routingTypes",
        Json::Array(
            types
                .iter()
                .map(|t| Json::from(i64::from(t.to_byte())))
                .collect(),
        ),
    );
    o.put("created-timestamp", saranga_server::message::now_millis());
    o.to_string()
}

/// `AddressInfo.fromJSON`.
pub fn address_from_json(text: &str) -> Result<AddressInfo, String> {
    use saranga_server::management::Json;
    let json = Json::parse(text).map_err(|e| format!("{text}: {e}"))?;
    let Json::Object(members) = &json else {
        return Err(format!("not an address: {text}"));
    };
    let mut info = AddressInfo {
        id: 0,
        name: Name::from(""),
        routing_types: RoutingTypes::default(),
        auto_created: false,
        temporary: false,
        internal: false,
    };
    let flag = |v: &Json| matches!(v, Json::Bool(true)) || v.to_string() == "true";
    for (k, v) in members {
        match k.as_str() {
            "name" => {
                if let Json::String(s) = v {
                    info.name = Name::from(s.as_str());
                }
            }
            "auto-created" => info.auto_created = flag(v),
            "temporary" => info.temporary = flag(v),
            "internal" => info.internal = flag(v),
            "routingTypes" => {
                if let Json::Array(types) = v {
                    for t in types {
                        if let Some(t) = t
                            .to_string()
                            .parse::<i8>()
                            .ok()
                            .and_then(RoutingType::from_byte)
                        {
                            info.routing_types.add(t);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(info)
}

/// A broker property kept on a mirrored message: `x-opt-amq-mr-id` as a
/// long, `x-opt-amq-bkr-id` as a string.
pub fn set_mirror_properties(m: &mut AmqpMessage, internal_id: i64, broker_id: &str) {
    m.set_broker_property(INTERNAL_ID, PropertyValue::Long(internal_id));
    m.set_broker_property(
        BROKER_ID,
        PropertyValue::String(SimpleString::from(broker_id)),
    );
}

/// An annotation map's entries by name (delivery or message annotations).
pub fn annotation_map(v: Option<&Value>) -> JavaMap<Symbol, Value> {
    let mut out = JavaMap::new(Kind::Linked);
    if let Some(Value::Map(entries)) = v {
        for (k, v) in entries {
            let key = match k {
                Value::Symbol(s) => s.clone(),
                Value::String(s) => Symbol(s.clone()),
                _ => continue,
            };
            out.insert(key, v.clone());
        }
    }
    out
}
