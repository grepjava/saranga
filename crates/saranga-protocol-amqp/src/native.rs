//! AMQP messages in the broker ([`NativeMessage`]): what the broker reads
//! from them, how filters see them, and how the broker's changes reach
//! their encoding.

use std::any::Any;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::{NativeDecoder, NativeMessage, now_millis};
use saranga_storage::message::PersistedMessage;

use crate::convert;
use crate::message::{ADDRESS_PROPERTY, AmqpMessage};
use crate::types::Value;

const HDR_ROUTING_TYPE: &str = "_AMQ_ROUTING_TYPE";
const HDR_GROUP_ID: &str = "_AMQ_GROUP_ID";
const HDR_GROUP_SEQUENCE: &str = "_AMQ_GROUP_SEQUENCE";
const HDR_SCHEDULED_DELIVERY_TIME: &str = "_AMQ_SCHED_DELIVERY";
const HDR_VALIDATED_USER: &str = "_AMQ_VALIDATED_USER";
const HDR_INGRESS_TIMESTAMP: &str = "_AMQ_INGRESS_TIMESTAMP";
const HDR_ROUTE_TO_IDS: &str = "_AMQ_ROUTE_TO";
const ORIG_PREFIX: &str = "_AMQ_ORIG_";

/// An AMQP message held by the broker.
#[derive(Debug, Clone)]
pub struct AmqpNative {
    pub message: AmqpMessage,
    /// `getScheduledDeliveryTime`, fixed when first read as in Artemis.
    scheduled: i64,
}

impl AmqpNative {
    pub fn new(message: AmqpMessage) -> Self {
        let scheduled = message.scheduled_delivery_time(now_millis());
        Self { message, scheduled }
    }

    pub fn arc(message: AmqpMessage) -> Arc<dyn NativeMessage> {
        Arc::new(Self::new(message))
    }
}

/// The decoder `saranga run` registers for the AMQP persisters.
pub fn decoder() -> Arc<NativeDecoder> {
    Arc::new(|record: &PersistedMessage| match record {
        PersistedMessage::Amqp(r) => Some(
            AmqpMessage::from_record(r, now_millis())
                .map(AmqpNative::arc)
                .map_err(|e| format!("AMQP message {}: {e}", r.message_id)),
        ),
        PersistedMessage::AmqpLarge(r) => Some(Err(format!(
            "AMQP large message {}: AMQP large messages are not supported yet",
            r.message_id
        ))),
        PersistedMessage::Core(_) => None,
    })
}

/// An application property as the broker reads it
/// (`getApplicationObjectProperty`: unsigned numbers as `long`, binary as
/// bytes).
fn view_property(v: &Value) -> Option<PropertyValue> {
    Some(match v {
        Value::Null => PropertyValue::Null,
        Value::Boolean(b) => PropertyValue::Bool(*b),
        Value::Byte(b) => PropertyValue::Byte(*b),
        Value::Short(s) => PropertyValue::Short(*s),
        Value::Int(i) => PropertyValue::Int(*i),
        Value::Long(l) => PropertyValue::Long(*l),
        Value::Float(f) => PropertyValue::Float(*f),
        Value::Double(d) => PropertyValue::Double(*d),
        Value::Char(c) => PropertyValue::Char(*c as u16),
        Value::String(s) => PropertyValue::String(SimpleString::from(s.as_str())),
        Value::Symbol(s) => PropertyValue::String(SimpleString::from(s.as_str())),
        Value::Binary(b) => PropertyValue::Bytes(b.clone()),
        Value::Ubyte(_) | Value::Ushort(_) | Value::Uint(_) | Value::Ulong(_) => {
            PropertyValue::Long(v.as_i64()?)
        }
        _ => return None,
    })
}

/// A value as a filter reads it.
fn filter_value(v: &Value) -> Option<saranga_filter::Value> {
    use saranga_filter::Value as F;
    Some(match v {
        Value::Boolean(b) => F::Bool(*b),
        Value::Byte(b) => F::Byte(*b),
        Value::Short(s) => F::Short(*s),
        Value::Int(i) => F::Int(*i),
        Value::Long(l) => F::Long(*l),
        Value::Float(f) => F::Float(*f),
        Value::Double(d) => F::Double(*d),
        Value::Char(c) => F::Char(*c as u16),
        Value::String(s) => F::String(s.clone()),
        Value::Symbol(s) => F::String(s.0.clone()),
        Value::Binary(b) => F::Bytes(b.clone()),
        Value::Ubyte(_) | Value::Ushort(_) | Value::Uint(_) | Value::Ulong(_) => {
            F::Long(v.as_i64()?)
        }
        Value::Timestamp(t) => F::Long(*t),
        _ => return None,
    })
}

/// An AMQP value from a Core property, for an application property the
/// broker puts.
fn amqp_value(v: &PropertyValue) -> Value {
    crate::message::property_to_value(v)
}

/// `Object.toString` of an AMQP message ID, as `getUserID().toString()`.
fn id_text(v: &Value) -> Option<String> {
    Some(match v {
        Value::String(s) => s.clone(),
        Value::Uuid(u) => saranga_storage::java::uuid(u),
        Value::Ulong(u) => u.to_string(),
        Value::Binary(b) => {
            // Binary.toString: printable ASCII as is, the rest escaped.
            let mut s = String::from("\"");
            for &c in b {
                if (32..127).contains(&c) && c != b'\\' {
                    s.push(c as char);
                } else {
                    s.push_str(&format!("\\x{c:02x}"));
                }
            }
            s.push('"');
            s
        }
        _ => return None,
    })
}

impl NativeMessage for AmqpNative {
    fn view(&self) -> CoreMessage {
        let m = &self.message;
        let mut core = CoreMessage {
            message_id: m.message_id(),
            address: m.address(),
            durable: m.is_durable(),
            priority: m.priority(),
            expiration: m.expiration(),
            timestamp: m.timestamp(),
            ..Default::default()
        };
        let props = &mut core.properties;
        for (k, v) in m.application_properties() {
            if let Some(p) = view_property(&v) {
                props.insert(SimpleString::from(k.as_str()), p);
            }
        }
        if let Some(extra) = m.extra_properties() {
            for (k, v) in extra.iter() {
                if k.to_string_lossy() != ADDRESS_PROPERTY {
                    props.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(t) = m.routing_type() {
            props.insert(HDR_ROUTING_TYPE.into(), PropertyValue::Byte(t));
        }
        if let Some(p) = m.properties() {
            if let Some(g) = &p.group_id {
                props.insert(
                    HDR_GROUP_ID.into(),
                    PropertyValue::String(SimpleString::from(g.as_str())),
                );
            }
            if let Some(s) = p.group_sequence {
                props.insert(HDR_GROUP_SEQUENCE.into(), PropertyValue::Int(s as i32));
            }
        }
        if self.scheduled > 0 {
            props.insert(
                HDR_SCHEDULED_DELIVERY_TIME.into(),
                PropertyValue::Long(self.scheduled),
            );
        }
        core
    }

    fn to_core(&self) -> CoreMessage {
        convert::to_core(&self.message, now_millis())
    }

    fn persisted(&self) -> PersistedMessage {
        self.message.persisted()
    }

    fn encode_size(&self) -> usize {
        self.message.encode_size()
    }

    /// `FilterImpl` over an `AMQPMessage`: the `AMQ` header fields, `m.`
    /// for message annotations, then `getObjectPropertyForFilter`.
    fn filter_value(&self, id: &str) -> Option<saranga_filter::Value> {
        use saranga_filter::Value as F;
        let m = &self.message;
        if id.starts_with("AMQ") {
            let header = match id {
                "AMQUserID" => {
                    let text = match m.properties().and_then(|p| p.message_id.as_ref()) {
                        Some(v) => id_text(v),
                        None => m
                            .application_property("NATIVE_MESSAGE_ID")
                            .and_then(|v| v.as_str().map(str::to_owned)),
                    };
                    text.map(|t| {
                        F::String(if t.starts_with("ID:") {
                            t
                        } else {
                            format!("ID:{t}")
                        })
                    })
                }
                "AMQPriority" => Some(F::Int(i32::from(m.priority()))),
                "AMQTimestamp" => Some(F::Long(m.timestamp())),
                "AMQDurable" => Some(F::String(
                    if m.is_durable() {
                        "DURABLE"
                    } else {
                        "NON_DURABLE"
                    }
                    .into(),
                )),
                "AMQExpiration" => Some(F::Long(m.expiration())),
                "AMQSize" => Some(F::Int(m.encode_size() as i32)),
                "AMQAddress" => m.address().map(|a| F::String(a.to_string_lossy())),
                "AMQGroupID" => m
                    .properties()
                    .and_then(|p| p.group_id.clone())
                    .map(F::String),
                _ => None,
            };
            if header.is_some() {
                return header;
            }
        }
        if let Some(name) = id.strip_prefix("m.") {
            return m.annotation_lookup(name).and_then(filter_value);
        }
        let props = m.properties();
        let value = match id {
            "JMSType" => props.and_then(|p| p.subject.clone()).map(F::String),
            "JMSXGroupID" => props.and_then(|p| p.group_id.clone()).map(F::String),
            "JMSXGroupSeq" => Some(F::Int(
                props.and_then(|p| p.group_sequence).unwrap_or(0) as i32
            )),
            "JMSXUserID" => props
                .and_then(|p| p.user_id.as_ref())
                .map(|u| F::String(String::from_utf8_lossy(u).into_owned())),
            "JMSCorrelationID" => {
                props
                    .and_then(|p| p.correlation_id.as_ref())
                    .and_then(|c| match c {
                        Value::Binary(b) => Some(F::Bytes(b.clone())),
                        Value::String(s) => Some(F::String(s.clone())),
                        other => convert::to_message_id_string(other).map(F::String),
                    })
            }
            _ => m.application_property(id).as_ref().and_then(filter_value),
        };
        value.or_else(|| {
            m.extra_properties()?
                .get(&SimpleString::from(id))
                .and_then(saranga_filter::Value::from_property)
        })
    }

    /// Replays the broker's changes as Artemis makes them through the
    /// `Message` API: `copy`, `setAddress`, `referenceOriginalMessage`,
    /// `setExpiration`, `setRoutingType`, `setBrokerProperty`, property
    /// puts, then `reencode` (which the send path never calls for a mere
    /// change of address or routing type).
    fn updated(&self, before: &CoreMessage, after: &CoreMessage) -> Arc<dyn NativeMessage> {
        let now = now_millis();
        // A message without an ID is getting one (`setMessageID`); one
        // whose ID changes is a copy (`copy(newID)`).
        let mut m = if before.message_id == 0 {
            let mut m = self.message.clone();
            m.set_message_id(after.message_id);
            m
        } else if after.message_id != before.message_id {
            self.message.copy(after.message_id, now)
        } else {
            self.message.clone()
        };
        let get = |c: &CoreMessage, k: &SimpleString| c.properties.get(k).cloned();
        let changed = |k: &SimpleString| get(before, k) != get(after, k);
        let mut reencode = false;
        if after.address != before.address
            && let Some(a) = &after.address
        {
            m.set_address(a.clone());
        }
        // referenceOriginalMessage: the original's names, then no expiry.
        let mut referenced = false;
        for (k, v) in after.properties.entries() {
            if k.to_string_lossy().starts_with(ORIG_PREFIX) && changed(k) {
                m.set_broker_property(&k.to_string_lossy(), v.clone());
                referenced = true;
            }
        }
        if referenced {
            m.set_expiration(0);
            reencode = true;
        }
        if after.expiration != before.expiration || (referenced && after.expiration != 0) {
            m.set_expiration(after.expiration);
            reencode = true;
        }
        // A copy gets its routing type set whenever it has one (a divert's
        // or a move's), even one equal to what the original's annotations
        // give.
        let rt = SimpleString::from(HDR_ROUTING_TYPE);
        let copied = before.message_id != 0 && after.message_id != before.message_id;
        if changed(&rt) || (copied && get(after, &rt).is_some()) {
            m.set_routing_type(match get(after, &rt) {
                Some(PropertyValue::Byte(b)) => Some(b),
                _ => None,
            });
        }
        if after.durable != before.durable {
            m.set_durable(after.durable);
            reencode = true;
        }
        if after.priority != before.priority {
            m.set_priority(after.priority);
            reencode = true;
        }
        for (k, v) in after.properties.entries() {
            let key = k.to_string_lossy();
            if !changed(k) || key.starts_with(ORIG_PREFIX) {
                continue;
            }
            match key.as_str() {
                HDR_ROUTING_TYPE | HDR_GROUP_ID | HDR_GROUP_SEQUENCE | HDR_VALIDATED_USER => {}
                HDR_SCHEDULED_DELIVERY_TIME => {
                    if let PropertyValue::Long(t) = v {
                        m.set_scheduled_delivery_time(*t);
                        reencode = true;
                    }
                }
                HDR_INGRESS_TIMESTAMP => {
                    if let PropertyValue::Long(t) = v {
                        m.set_annotation(crate::message::annotation::INGRESS_TIME, Value::Long(*t));
                        reencode = true;
                    }
                }
                k if k.starts_with("_AMQ_") => {
                    m.set_broker_property(k, v.clone());
                    reencode = true;
                }
                k => {
                    m.put_property(k, amqp_value(v));
                    reencode = true;
                }
            }
        }
        for (k, _) in before.properties.entries() {
            if after.properties.get(k).is_some() {
                continue;
            }
            let key = k.to_string_lossy();
            match key.as_str() {
                HDR_ROUTING_TYPE
                | HDR_GROUP_ID
                | HDR_GROUP_SEQUENCE
                | HDR_VALIDATED_USER
                | HDR_SCHEDULED_DELIVERY_TIME => {}
                k if k.starts_with(HDR_ROUTE_TO_IDS) || k.starts_with("_AMQ_") => {
                    m.remove_extra_property(k);
                }
                k => {
                    m.remove_property(k);
                    reencode = true;
                }
            }
        }
        if reencode {
            m.reencode(now);
        }
        AmqpNative::arc(m)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
