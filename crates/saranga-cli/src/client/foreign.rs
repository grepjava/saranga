//! A message one JMS client received, sent through the other's producer,
//! as `transfer` sends it between protocols: Artemis's
//! `ActiveMQMessageProducer` copying a Qpid message with the
//! `ActiveMQXxxMessage(foreign)` constructors, and Qpid's `JmsSession.send`
//! copying an Artemis message with `JmsMessageTransformation`.

use saranga_buffer::java_hash::{hash_map_order, string_hash};
use saranga_buffer::{JString, Reader, TypedProperties, Writer};
use saranga_client::{CoreMessage, MessageExt, PropertyValue, SimpleString, message_type};
use saranga_protocol_amqp::types::Value;

use super::amqp::jms::JmsError;
use super::amqp::message::{AmqpJmsMessage, Body, kind};
use super::amqp::properties::{check_name, set_property};
use super::jms::{Setter, set_core_property};
use super::jmsapi::serialized_string;

/// An exception text (`class: message`) a message accessor gave, as the
/// exception it stands for.
fn exception(text: &str) -> JmsError {
    const CLASSES: [&str; 6] = [
        "java.lang.IllegalStateException",
        "javax.jms.JMSException",
        "javax.jms.MessageFormatException",
        "java.lang.ClassCastException",
        "org.apache.qpid.jms.exceptions.IdConversionException",
        "java.lang.UnsupportedOperationException",
    ];
    let (class, message) = text
        .split_once(": ")
        .unwrap_or(("javax.jms.JMSException", text));
    JmsError {
        class: CLASSES
            .iter()
            .find(|c| **c == class)
            .copied()
            .unwrap_or("javax.jms.JMSException"),
        message: message.to_owned(),
    }
}

/// The class proton-j decodes an AMQP value to.
fn proton_class(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Boolean(_) => "java.lang.Boolean",
        Value::Ubyte(_) => "org.apache.qpid.proton.amqp.UnsignedByte",
        Value::Ushort(_) => "org.apache.qpid.proton.amqp.UnsignedShort",
        Value::Uint(_) => "org.apache.qpid.proton.amqp.UnsignedInteger",
        Value::Ulong(_) => "org.apache.qpid.proton.amqp.UnsignedLong",
        Value::Byte(_) => "java.lang.Byte",
        Value::Short(_) => "java.lang.Short",
        Value::Int(_) => "java.lang.Integer",
        Value::Long(_) => "java.lang.Long",
        Value::Float(_) => "java.lang.Float",
        Value::Double(_) => "java.lang.Double",
        Value::Decimal32(_) => "org.apache.qpid.proton.amqp.Decimal32",
        Value::Decimal64(_) => "org.apache.qpid.proton.amqp.Decimal64",
        Value::Decimal128(_) => "org.apache.qpid.proton.amqp.Decimal128",
        Value::Char(_) => "java.lang.Character",
        Value::Timestamp(_) => "java.util.Date",
        Value::Uuid(_) => "java.util.UUID",
        Value::Binary(_) => "org.apache.qpid.proton.amqp.Binary",
        Value::String(_) => "java.lang.String",
        Value::Symbol(_) => "org.apache.qpid.proton.amqp.Symbol",
        Value::List(_) => "java.util.ArrayList",
        Value::Map(_) => "java.util.LinkedHashMap",
        Value::Array(_) => "[Ljava.lang.Object;",
        Value::Described(..) => "org.apache.qpid.proton.amqp.UnknownDescribedType",
    }
}

/// `TypedProperties.setObjectProperty`'s types; `byte[]` is a `Binary`
/// that `getObject` copied out.
fn typed_value(v: &Value, binary: bool) -> Result<PropertyValue, JmsError> {
    Ok(match v {
        Value::Null => PropertyValue::Null,
        Value::Boolean(b) => PropertyValue::Bool(*b),
        Value::Byte(b) => PropertyValue::Byte(*b),
        Value::Short(s) => PropertyValue::Short(*s),
        Value::Int(i) => PropertyValue::Int(*i),
        Value::Long(l) => PropertyValue::Long(*l),
        Value::Float(f) => PropertyValue::Float(*f),
        Value::Double(d) => PropertyValue::Double(*d),
        // A Character is one UTF-16 unit.
        Value::Char(c) => PropertyValue::Char(*c as u16),
        Value::String(s) => PropertyValue::String(s.as_str().into()),
        Value::Binary(b) if binary => PropertyValue::Bytes(b.clone()),
        other => {
            return Err(JmsError {
                class: "javax.jms.MessageFormatException",
                message: format!("class {} is not a valid property type", proton_class(other)),
            });
        }
    })
}

/// Indexes of `names` in the order a `java.util.HashSet<String>` built by
/// adding them in this order iterates them.
fn string_set_order(names: &[String], capacity: Option<(usize, f32)>) -> Vec<usize> {
    let hashes: Vec<i32> = names.iter().map(|n| string_hash(n)).collect();
    hash_map_order(&hashes, capacity)
}

/// `SimpleString.hashCode`: over the UTF-16LE bytes, signed.
fn simple_hash(s: &SimpleString) -> i32 {
    s.as_bytes().iter().fold(0i32, |h, &b| {
        h.wrapping_mul(31).wrapping_add(i32::from(b as i8))
    })
}

/// The keys of a decoded `TypedProperties` (a `new HashMap<>(n, 1.0f)` of
/// the wire's entries) in its iteration order.
fn decoded_map_order(entries: &[(SimpleString, PropertyValue)]) -> Vec<usize> {
    let hashes: Vec<i32> = entries.iter().map(|(k, _)| simple_hash(k)).collect();
    hash_map_order(&hashes, Some((entries.len(), 1.0)))
}

fn string_value(v: &PropertyValue) -> Option<String> {
    match v {
        PropertyValue::String(s) => Some(s.to_string_lossy()),
        _ => None,
    }
}

// ------------------------------------------------- Qpid message to Core

/// `JmsMessage.getObjectProperty` through the interceptors.
fn qpid_property(m: &AmqpJmsMessage, name: &str) -> Value {
    let string = |s: Option<String>| s.map_or(Value::Null, Value::String);
    match name {
        "JMSDestination" => string(m.destination().map(|d| d.address().to_owned())),
        "JMSReplyTo" => string(m.reply_to()),
        "JMSType" => string(m.jms_type()),
        "JMSDeliveryMode" => Value::String(
            if m.persistent() {
                "PERSISTENT"
            } else {
                "NON_PERSISTENT"
            }
            .to_owned(),
        ),
        "JMSPriority" => Value::Int(i32::from(m.priority())),
        "JMSMessageID" => string(m.jms_message_id()),
        "JMSTimestamp" => Value::Long(m.timestamp()),
        "JMSCorrelationID" => string(m.correlation_id()),
        "JMSExpiration" => Value::Long(m.expiration()),
        "JMSRedelivered" => Value::Boolean(m.redelivered()),
        "JMSXDeliveryCount" => Value::Int(m.jmsx_delivery_count()),
        "JMSXGroupID" => string(m.group_id()),
        "JMSXGroupSeq" => Value::Int(m.group_sequence()),
        "JMSXUserID" => match m.user_id() {
            Some(u) => Value::String(u),
            None => m.property("JMSXUserID").cloned().unwrap_or(Value::Null),
        },
        "JMS_AMQP_ACK_TYPE" | "JMS_AMQP_TTL" => Value::Null,
        "JMSDeliveryTime" => Value::Long(m.delivery_time()),
        "JMS_AMQP_REPLY_TO_GROUP_ID" => string(
            m.properties
                .as_ref()
                .and_then(|p| p.reply_to_group_id.clone()),
        ),
        "JMS_AMQP_TYPED_ENCODING" => {
            if typed_object(m) {
                Value::Boolean(true)
            } else {
                Value::Null
            }
        }
        _ => m.property(name).cloned().unwrap_or(Value::Null),
    }
}

/// Whether a received object message has the AMQP-typed delegate: its
/// content type is not the serialized object's.
fn typed_object(m: &AmqpJmsMessage) -> bool {
    m.kind == kind::OBJECT
        && m.properties
            .as_ref()
            .and_then(|p| p.content_type.as_ref())
            .map(|c| c.as_str())
            != Some(super::amqp::message::SERIALIZED_JAVA_OBJECT)
}

/// `JmsMessagePropertyIntercepter.getPropertyNames(message, true)`.
fn qpid_property_names(m: &AmqpJmsMessage) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    // The interceptors that are not headers, in their map's order.
    if m.user_id().is_some() {
        names.push("JMSXUserID".to_owned());
    }
    if m.group_sequence() != 0 {
        names.push("JMSXGroupSeq".to_owned());
    }
    if m.group_id().is_some() {
        names.push("JMSXGroupID".to_owned());
    }
    names.push("JMSXDeliveryCount".to_owned());
    // The facade's: its own interceptors, then the application properties.
    let mut inner: Vec<String> = Vec::new();
    if m.properties
        .as_ref()
        .and_then(|p| p.reply_to_group_id.as_deref())
        .is_some_and(|g| !g.is_empty())
    {
        inner.push("JMS_AMQP_REPLY_TO_GROUP_ID".to_owned());
    }
    if typed_object(m) {
        inner.push("JMS_AMQP_TYPED_ENCODING".to_owned());
    }
    for (k, _) in &m.application_properties {
        if !inner.contains(k) {
            inner.push(k.clone());
        }
    }
    for i in string_set_order(&inner, None) {
        let name = &inner[i];
        if check_name(name).is_ok() && !names.contains(name) {
            names.push(name.clone());
        }
    }
    string_set_order(&names, None)
        .into_iter()
        .map(|i| names[i].clone())
        .collect()
}

/// `ActiveMQMessageProducer.doSendx` for a Qpid message: the producer's
/// headers set on it, then the Core message the converting constructor of
/// its type makes. The send sets the rest.
pub fn qpid_to_core(
    src: &mut AmqpJmsMessage,
    persistent: bool,
    now: i64,
) -> Result<CoreMessage, JmsError> {
    src.set_persistent(persistent);
    src.set_priority(4);
    src.set_expiration(0);
    src.set_timestamp(now);
    let core_type = match src.kind {
        kind::BYTES => message_type::BYTES,
        kind::MAP => message_type::MAP,
        kind::OBJECT => message_type::OBJECT,
        kind::STREAM => message_type::STREAM,
        kind::TEXT => message_type::TEXT,
        _ => message_type::DEFAULT,
    };
    let mut m = saranga_client::message::new_message(core_type, true);
    m.set_timestamp(src.timestamp());
    // The correlation ID: a binary one as bytes, else its string form.
    let correlation = src
        .properties
        .as_ref()
        .and_then(|p| p.correlation_id.clone());
    match correlation {
        Some(Value::Binary(b)) if !b.is_empty() => {
            m.put_property("JMSCorrelationID", PropertyValue::Bytes(b));
        }
        _ => {
            if let Some(c) = src.correlation_id() {
                m.put_property("JMSCorrelationID", PropertyValue::String(c.as_str().into()));
            }
        }
    }
    if let Some(reply_to) = src.reply_to() {
        return Err(JmsError {
            class: "javax.jms.InvalidDestinationException",
            message: format!("Foreign destination {reply_to}"),
        });
    }
    m.set_durable(src.persistent());
    m.set_expiration(src.expiration());
    m.set_priority(src.priority());
    if let Some(t) = src.jms_type() {
        m.put_property("JMSType", PropertyValue::String(t.as_str().into()));
    }
    for name in qpid_property_names(src) {
        let value = typed_value(&qpid_property(src, &name), false)?;
        set_core_property(&mut m, &name, &value, Setter::Object)?;
    }
    match src.kind {
        kind::BYTES => {
            m.body = src.bytes_body().map_err(|e| exception(&e))?.into();
        }
        kind::TEXT => {
            let text = src.text().map_err(|e| exception(&e))?;
            m.set_text(text.as_deref());
        }
        kind::MAP => m.body = core_map_body(src)?.into(),
        kind::OBJECT => m.body = core_object_body(src)?.into(),
        kind::STREAM => m.body = core_stream_body(src)?.into(),
        _ => {}
    }
    Ok(m)
}

/// `ActiveMQMapMessage(foreign)`: each name of the Qpid map in its order,
/// put as `setObject` puts it, then encoded in the map's own order.
fn core_map_body(src: &AmqpJmsMessage) -> Result<Vec<u8>, JmsError> {
    let entries = match &src.body {
        Some(Body::Value(Value::Map(entries))) => entries.clone(),
        _ => Vec::new(),
    };
    let mut map = TypedProperties::new();
    for (k, v) in &entries {
        let Value::String(name) = k else {
            return Err(JmsError {
                class: "java.lang.ClassCastException",
                message: format!(
                    "class {0} cannot be cast to class java.lang.String ({0} {1})",
                    proton_class(k),
                    if proton_class(k).starts_with("java.") {
                        "and java.lang.String are in module java.base of loader 'bootstrap'"
                    } else {
                        "is in unnamed module of loader 'app'; java.lang.String is in module java.base of loader 'bootstrap'"
                    }
                ),
            });
        };
        if name.is_empty() {
            return Err(JmsError {
                class: "java.lang.IllegalArgumentException",
                message: "Map key name must not be the empty string".to_owned(),
            });
        }
        map.insert(SimpleString::from(name.as_str()), typed_value(v, true)?);
    }
    map.sort_as_java_hash_map();
    let mut w = Writer::new();
    map.encode(&mut w);
    Ok(w.into_vec())
}

/// `ActiveMQObjectMessage(foreign)`: the object's serialization, behind
/// its length. A serialized object is copied as it is; a typed one can be
/// serialized here when it is a string.
fn core_object_body(src: &AmqpJmsMessage) -> Result<Vec<u8>, JmsError> {
    let data = if typed_object(src) {
        match &src.body {
            None | Some(Body::Value(Value::Null)) => None,
            Some(Body::Value(Value::String(s))) => Some(serialized_string(s)),
            Some(Body::Data(_)) => {
                return Err(JmsError {
                    class: "java.lang.UnsupportedOperationException",
                    message: "Data support still to be added".to_owned(),
                });
            }
            Some(_) => {
                return Err(JmsError::jms(
                    "Failed to serialize object: an AMQP-typed object other than a String",
                ));
            }
        }
    } else {
        match &src.body {
            None => None,
            Some(Body::Data(_)) => src.serialized_object().map(<[u8]>::to_vec),
            Some(Body::Value(_)) => {
                return Err(JmsError {
                    class: "javax.jms.MessageFormatException",
                    message: "Failed to read object: Unexpected body type: AmqpValue".to_owned(),
                });
            }
            Some(Body::Sequence(_)) => {
                return Err(JmsError {
                    class: "javax.jms.MessageFormatException",
                    message: "Failed to read object: Unexpected body type: AmqpSequence".to_owned(),
                });
            }
        }
    };
    let mut body = Vec::new();
    if let Some(d) = data {
        body.extend_from_slice(&(d.len() as i32).to_be_bytes());
        body.extend_from_slice(&d);
    }
    Ok(body)
}

/// `ActiveMQStreamMessage(foreign)`: `readObject` until the end, each
/// value written with its type byte.
fn core_stream_body(src: &AmqpJmsMessage) -> Result<Vec<u8>, JmsError> {
    use saranga_buffer::constants as c;
    let items = match &src.body {
        Some(Body::Sequence(items)) | Some(Body::Value(Value::List(items))) => items.clone(),
        None => Vec::new(),
        Some(_) => {
            return Err(JmsError {
                class: "java.lang.IllegalStateException",
                message: "Unexpected message body type.".to_owned(),
            });
        }
    };
    let mut w = Writer::new();
    for item in &items {
        match item {
            Value::Null => {
                w.u8(c::STRING).nullable_string(None);
            }
            Value::String(s) => {
                w.u8(c::STRING)
                    .nullable_string(Some(&JString::from_code_units(s.encode_utf16().collect())));
            }
            Value::Boolean(b) => {
                w.u8(c::BOOLEAN).bool(*b);
            }
            Value::Byte(b) => {
                w.u8(c::BYTE).i8(*b);
            }
            Value::Short(s) => {
                w.u8(c::SHORT).i16(*s);
            }
            Value::Int(i) => {
                w.u8(c::INT).i32(*i);
            }
            Value::Long(l) => {
                w.u8(c::LONG).i64(*l);
            }
            Value::Float(f) => {
                w.u8(c::FLOAT).f32(*f);
            }
            Value::Double(d) => {
                w.u8(c::DOUBLE).f64(*d);
            }
            Value::Char(ch) => {
                w.u8(c::CHAR).i16(*ch as u16 as i16);
            }
            Value::Binary(b) => {
                w.u8(c::BYTES).i32(b.len() as i32).bytes(b);
            }
            _ => {
                return Err(JmsError {
                    class: "javax.jms.MessageFormatException",
                    message: "Unknown type found in stream".to_owned(),
                });
            }
        }
    }
    Ok(w.into_vec())
}

// ------------------------------------------------- Core message to Qpid

/// `MessageUtil.getPropertyNames` of a received Core message: its keys in
/// the decoded map's order, through a `HashSet` of them, each mapped to
/// its JMS name or hidden, then `JMSXDeliveryCount`, all in a
/// `HashSet<String>`.
fn core_property_names(m: &CoreMessage) -> Vec<String> {
    let entries = m.properties.entries();
    let decoded: Vec<usize> = decoded_map_order(entries);
    let keys: Vec<&SimpleString> = decoded.iter().map(|i| &entries[*i].0).collect();
    let hashes: Vec<i32> = keys.iter().map(|k| simple_hash(k)).collect();
    let capacity = ((keys.len() as f32 / 0.75) as usize + 1).max(16);
    let mut names: Vec<String> = Vec::new();
    for i in hash_map_order(&hashes, Some((capacity, 0.75))) {
        let key = keys[i].to_string_lossy();
        let name = match key.as_str() {
            "_AMQ_GROUP_ID" => "JMSXGroupID".to_owned(),
            "_AMQ_GROUP_SEQUENCE" => "JMSXGroupSeq".to_owned(),
            "_AMQ_VALIDATED_USER" => "JMSXUserID".to_owned(),
            k if (!k.starts_with("JMS") || k.starts_with("JMSX") || k.starts_with("JMS_"))
                && !k.starts_with("__AMQ_CID")
                && k != "_AMQ_ROUTING_TYPE"
                && !k.starts_with("_AMQ_ROUTE_TO") =>
            {
                k.to_owned()
            }
            _ => continue,
        };
        if !names.contains(&name) {
            names.push(name);
        }
    }
    if !names.iter().any(|n| n == "JMSXDeliveryCount") {
        names.push("JMSXDeliveryCount".to_owned());
    }
    string_set_order(&names, None)
        .into_iter()
        .map(|i| names[i].clone())
        .collect()
}

/// `ActiveMQMessage.getObjectProperty` (`MessageUtil.getObjectProperty`).
fn core_property(m: &CoreMessage, name: &str, delivery_count: i32) -> PropertyValue {
    let string = |key: &str| {
        m.property(key)
            .and_then(string_value)
            .map_or(PropertyValue::Null, |s| {
                PropertyValue::String(s.as_str().into())
            })
    };
    match name {
        "JMSXDeliveryCount" => PropertyValue::Int(delivery_count),
        "JMSXGroupID" => string("_AMQ_GROUP_ID"),
        "JMSXGroupSeq" => match m.property("_AMQ_GROUP_SEQUENCE") {
            Some(PropertyValue::Int(i)) => PropertyValue::Int(*i),
            _ => PropertyValue::Int(0),
        },
        "JMSXUserID" => string("_AMQ_VALIDATED_USER"),
        _ => m.property(name).cloned().unwrap_or(PropertyValue::Null),
    }
}

/// A Core property value as an AMQP map or stream value.
fn amqp_value(v: &PropertyValue) -> Value {
    match v {
        PropertyValue::Bytes(b) => Value::Binary(b.clone()),
        other => super::amqp::properties::amqp_value(other),
    }
}

/// `JmsSession.send`'s preparation of an Artemis message, then
/// `JmsMessageTransformation.transformMessage`: the Qpid message of its
/// type with its body, headers and properties copied. The send sets the
/// rest.
pub fn core_to_qpid(
    src: &mut CoreMessage,
    persistent: bool,
    now: i64,
) -> Result<AmqpJmsMessage, JmsError> {
    src.set_durable(persistent);
    src.set_priority(4);
    // `setJMSRedelivered(false)`: a delivery count of 1.
    let delivery_count = 1;
    src.set_timestamp(now);
    src.set_expiration(0);
    let mut m = match src.message_type {
        message_type::BYTES => AmqpJmsMessage::bytes(&src.body),
        message_type::MAP => AmqpJmsMessage::map(qpid_map_entries(src)?),
        message_type::OBJECT => {
            let data = src
                .body
                .get(4..)
                .filter(|d| !d.is_empty())
                .map(<[u8]>::to_vec);
            AmqpJmsMessage::object(data)
        }
        message_type::STREAM => AmqpJmsMessage::stream(qpid_stream_items(src)),
        message_type::TEXT => {
            let text = src.text().ok().flatten();
            AmqpJmsMessage::new_text(text.as_deref())
        }
        _ => AmqpJmsMessage::message(),
    };
    // `copyProperties`.
    let correlation = src.property("JMSCorrelationID").and_then(string_value);
    m.set_correlation_id(correlation.as_deref())
        .map_err(|e| exception(&e))?;
    if let Some(reply_to) = src.property("JMSReplyTo").and_then(string_value) {
        let (address, type_byte) = if let Some(a) = reply_to.strip_prefix("queue://") {
            (a, 0)
        } else if let Some(a) = reply_to.strip_prefix("topic://") {
            (a, 1)
        } else if let Some(a) = reply_to.strip_prefix("temp-queue://") {
            (a, 2)
        } else if let Some(a) = reply_to.strip_prefix("temp-topic://") {
            (a, 3)
        } else {
            return Err(JmsError::jms(format!(
                "Unresolvable destination: Both queue and topic names are null: ActiveMQDestination [address={reply_to}, name=null, type =DESTINATION]"
            )));
        };
        m.set_reply_to(Some(address.to_owned()), type_byte);
    }
    m.set_persistent(src.durable);
    m.set_type(src.property("JMSType").and_then(string_value));
    m.set_expiration(src.expiration);
    m.set_priority(i32::from(src.priority));
    m.set_timestamp(src.timestamp);
    for name in core_property_names(src) {
        let value = core_property(src, &name, delivery_count);
        set_property(&mut m, &name, &value)?;
    }
    Ok(m)
}

/// `getMapNames` (a `HashSet<String>` sized to the map, filled in the
/// decoded map's order) and each value, into the `LinkedHashMap` Qpid's
/// map body is.
fn qpid_map_entries(src: &CoreMessage) -> Result<Vec<(Value, Value)>, JmsError> {
    let map = TypedProperties::decode(&mut Reader::new(&src.body)).unwrap_or_default();
    let entries = map.entries();
    let decoded = decoded_map_order(entries);
    let names: Vec<String> = decoded
        .iter()
        .map(|i| entries[*i].0.to_string_lossy())
        .collect();
    let mut out = Vec::new();
    for i in string_set_order(&names, Some((names.len(), 0.75))) {
        let (_, v) = &entries[decoded[i]];
        if names[i].is_empty() {
            return Err(JmsError {
                class: "java.lang.IllegalArgumentException",
                message: "Map key name must not be the empty string".to_owned(),
            });
        }
        out.push((Value::String(names[i].clone()), amqp_value(v)));
    }
    Ok(out)
}

/// `readObject` until a null or the end (`while ((obj = readObject()) !=
/// null)`).
fn qpid_stream_items(src: &CoreMessage) -> Vec<Value> {
    use saranga_buffer::constants as c;
    let mut r = Reader::new(&src.body);
    let mut items = Vec::new();
    while !r.is_empty() {
        let Ok(t) = r.u8() else { break };
        let item = match t {
            c::BOOLEAN => r.bool().map(Value::Boolean),
            c::BYTE => r.i8().map(Value::Byte),
            c::SHORT => r.i16().map(Value::Short),
            c::CHAR => r.u16().map(|u| Value::Char(u32::from(u))),
            c::INT => r.i32().map(Value::Int),
            c::LONG => r.i64().map(Value::Long),
            c::FLOAT => r.f32().map(Value::Float),
            c::DOUBLE => r.f64().map(Value::Double),
            c::STRING => match r.nullable_string() {
                Ok(Some(s)) => Ok(Value::String(s.to_string_lossy())),
                _ => break,
            },
            c::BYTES => match r.i32() {
                Ok(n) => r
                    .bytes(n.max(0) as usize)
                    .map(|b| Value::Binary(b.to_vec())),
                Err(e) => Err(e),
            },
            _ => break,
        };
        match item {
            Ok(v) => items.push(v),
            Err(_) => break,
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_qpid_text_message_becomes_a_core_one() {
        let mut q = AmqpJmsMessage::new_text(Some("hi"));
        set_property(&mut q, "count", &PropertyValue::Long(3)).unwrap();
        set_property(&mut q, "JMSXGroupID", &PropertyValue::String("g".into())).unwrap();
        let c = qpid_to_core(&mut q, true, 1000).unwrap();
        assert_eq!(c.message_type, message_type::TEXT);
        assert_eq!(c.text().unwrap().as_deref(), Some("hi"));
        assert_eq!(c.property("count"), Some(&PropertyValue::Long(3)));
        assert_eq!(c.string_property("_AMQ_GROUP_ID").as_deref(), Some("g"));
        assert_eq!(
            c.property("JMSXDeliveryCount"),
            Some(&PropertyValue::Int(1))
        );
    }

    #[test]
    fn a_core_message_becomes_a_qpid_one() {
        let mut c = saranga_client::message::text_message("hi");
        c.put_property("count", PropertyValue::Long(3));
        c.put_string_property("_AMQ_GROUP_ID", "g");
        c.put_string_property("JMSReplyTo", "queue://r");
        let q = core_to_qpid(&mut c, true, 1000).unwrap();
        assert_eq!(q.text().unwrap().as_deref(), Some("hi"));
        assert_eq!(q.property("count"), Some(&Value::Long(3)));
        assert_eq!(q.group_id().as_deref(), Some("g"));
        assert_eq!(q.reply_to().as_deref(), Some("r"));
        assert_eq!(q.reply_to_type_byte(), Some(0));
        let mut bytes = saranga_client::message::bytes_message(b"ab");
        bytes.put_property("b", PropertyValue::Bytes(vec![1]));
        let e = core_to_qpid(&mut bytes, true, 1000).unwrap_err();
        assert_eq!(e.class, "javax.jms.MessageFormatException");
    }
}
