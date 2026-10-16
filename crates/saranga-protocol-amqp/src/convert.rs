//! Conversion between AMQP and Core messages (`AmqpCoreConverter`,
//! `CoreAmqpConverter`, `AMQPMessageIdHelper`, `AMQPContentTypeSupport` and
//! the `CoreMessageWrapper`s).
//!
//! What Artemis cannot convert (a property or stream item of a type Core
//! has no place for) fails the whole conversion there; here that value is
//! left out.

use saranga_buffer::{JString, PropertyValue, Reader, SimpleString, TypedProperties, Writer};
use saranga_message::{CoreMessage, message_type};

use crate::codec;
use crate::composite::Timestamp;
use crate::javamap::{JavaHash, JavaMap, Kind};
use crate::message::{AmqpMessage, annotation, dest_type, encode_section};
use crate::messaging::{Header, Properties, section};
use crate::types::{Descriptor, Symbol, Value};

/// `JMS_AMQP_*` property names (`AMQPMessageSupport`).
pub mod jms_amqp {
    pub const PREFIX: &str = "JMS_AMQP_";
    pub const HEADER: &str = "JMS_AMQP_HEADER";
    pub const HEADER_DURABLE: &str = "JMS_AMQP_HEADERDURABLE";
    pub const HEADER_PRIORITY: &str = "JMS_AMQP_HEADERPRIORITY";
    pub const PROPERTIES: &str = "JMS_AMQP_PROPERTIES";
    pub const NATIVE: &str = "JMS_AMQP_NATIVE";
    pub const FIRST_ACQUIRER: &str = "JMS_AMQP_FirstAcquirer";
    pub const CONTENT_TYPE: &str = "JMS_AMQP_ContentType";
    pub const CONTENT_ENCODING: &str = "JMS_AMQP_ContentEncoding";
    pub const REPLYTO_GROUP_ID: &str = "JMS_AMQP_ReplyToGroupID";
    pub const DELIVERY_ANNOTATION_PREFIX: &str = "JMS_AMQP_DA_";
    pub const MESSAGE_ANNOTATION_PREFIX: &str = "JMS_AMQP_MA_";
    pub const FOOTER_PREFIX: &str = "JMS_AMQP_FT_";
    pub const ENCODED_DELIVERY_ANNOTATION_PREFIX: &str = "JMS_AMQP_ENCODED_DA_";
    pub const ENCODED_MESSAGE_ANNOTATION_PREFIX: &str = "JMS_AMQP_ENCODED_MA_";
    pub const ENCODED_FOOTER_PREFIX: &str = "JMS_AMQP_ENCODED_FT_";
    pub const ORIGINAL_ENCODING: &str = "JMS_AMQP_ORIGINAL_ENCODING";
}

/// `JMS_AMQP_ORIGINAL_ENCODING` values.
mod encoding {
    pub const UNKNOWN: i16 = 0;
    pub const NULL: i16 = 1;
    pub const DATA: i16 = 2;
    pub const SEQUENCE: i16 = 3;
    pub const VALUE_NULL: i16 = 4;
    pub const VALUE_STRING: i16 = 5;
    pub const VALUE_BINARY: i16 = 6;
    pub const VALUE_MAP: i16 = 7;
    pub const VALUE_LIST: i16 = 8;
}

/// `x-opt-jms-msg-type` values.
mod jms_type {
    pub const MESSAGE: i8 = 0;
    pub const OBJECT: i8 = 1;
    pub const MAP: i8 = 2;
    pub const BYTES: i8 = 3;
    pub const STREAM: i8 = 4;
    pub const TEXT: i8 = 5;
}

const SERIALIZED_JAVA_OBJECT: &str = "application/x-java-serialized-object";
const OCTET_STREAM: &str = "application/octet-stream";
const NATIVE_MESSAGE_ID: &str = "NATIVE_MESSAGE_ID";
const JMSX_DELIVERY_COUNT: &str = "JMSXDeliveryCount";
const JMSX_GROUP_ID: &str = "JMSXGroupID";
const JMSX_GROUP_SEQ: &str = "JMSXGroupSeq";
const JMSX_USER_ID: &str = "JMSXUserID";
const JMS_TYPE: &str = "JMSType";
const JMS_CORRELATION_ID: &str = "JMSCorrelationID";
const JMS_REPLY_TO: &str = "JMSReplyTo";
const HDR_GROUP_ID: &str = "_AMQ_GROUP_ID";
const HDR_GROUP_SEQUENCE: &str = "_AMQ_GROUP_SEQUENCE";
const HDR_VALIDATED_USER: &str = "_AMQ_VALIDATED_USER";
const HDR_SCHEDULED_DELIVERY_TIME: &str = "_AMQ_SCHED_DELIVERY";
const HDR_INGRESS_TIMESTAMP: &str = "_AMQ_INGRESS_TIMESTAMP";
const HDR_ROUTING_TYPE: &str = "_AMQ_ROUTING_TYPE";
const HDR_ROUTE_TO_IDS: &str = "_AMQ_ROUTE_TO";
const CONNECTION_ID: &str = "__AMQ_CID";

const QUEUE_PREFIX: &str = "queue://";
const TOPIC_PREFIX: &str = "topic://";
const TEMP_QUEUE_PREFIX: &str = "temp-queue://";
const TEMP_TOPIC_PREFIX: &str = "temp-topic://";

impl JavaHash for String {
    fn java_hash(&self) -> i32 {
        let units: Vec<u16> = self.encode_utf16().collect();
        saranga_storage::java::string_hash(&units)
    }
}

// ------------------------------------------------------------ ID helpers

/// `AMQPMessageIdHelper` prefixes.
mod id_prefix {
    pub const JMS: &str = "ID:";
    pub const STRING: &str = "AMQP_STRING:";
    pub const UUID: &str = "AMQP_UUID:";
    pub const ULONG: &str = "AMQP_ULONG:";
    pub const BINARY: &str = "AMQP_BINARY:";
    pub const NO_PREFIX: &str = "AMQP_NO_PREFIX:";
}

fn has_type_encoding_prefix(s: &str) -> bool {
    use id_prefix::*;
    let rest = &s[JMS.len()..];
    [BINARY, UUID, ULONG, STRING, NO_PREFIX]
        .iter()
        .any(|p| rest.starts_with(p))
}

/// `java.util.UUID.toString`.
pub fn uuid_string(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// `AMQPMessageIdHelper.toMessageIdString`.
pub fn to_message_id_string(id: &Value) -> Option<String> {
    use id_prefix::*;
    match id {
        Value::String(s) => Some(if !s.starts_with(JMS) {
            format!("{JMS}{NO_PREFIX}{s}")
        } else if has_type_encoding_prefix(s) {
            format!("{JMS}{STRING}{s}")
        } else {
            s.clone()
        }),
        other => convert_to_id_string(other),
    }
}

/// `toCorrelationIdStringOrBytes`.
fn to_correlation_id(id: &Value) -> Option<PropertyValue> {
    use id_prefix::*;
    match id {
        Value::String(s) => Some(PropertyValue::String(
            if s.starts_with(JMS) && has_type_encoding_prefix(s) {
                format!("{JMS}{STRING}{s}")
            } else {
                s.clone()
            }
            .as_str()
            .into(),
        )),
        Value::Binary(b) => Some(PropertyValue::Bytes(b.clone())),
        other => convert_to_id_string(other).map(|s| PropertyValue::String(s.as_str().into())),
    }
}

fn convert_to_id_string(id: &Value) -> Option<String> {
    use id_prefix::*;
    match id {
        Value::Uuid(u) => Some(format!("{JMS}{UUID}{}", uuid_string(u))),
        Value::Ulong(v) => Some(format!("{JMS}{ULONG}{v}")),
        Value::Binary(b) => Some(format!(
            "{JMS}{BINARY}{}",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        )),
        _ => None,
    }
}

/// `AMQPMessageIdHelper.toIdObject`: `None` if the string does not parse
/// (`ActiveMQAMQPIllegalStateException`).
fn to_id_object(s: &str) -> Option<Value> {
    use id_prefix::*;
    let Some(rest) = s.strip_prefix(JMS) else {
        return Some(Value::String(s.to_owned()));
    };
    if let Some(v) = rest.strip_prefix(NO_PREFIX) {
        Some(Value::String(v.to_owned()))
    } else if let Some(v) = rest.strip_prefix(UUID) {
        parse_uuid(v).map(Value::Uuid)
    } else if let Some(v) = rest.strip_prefix(ULONG) {
        // UnsignedLong.valueOf(String) goes through BigInteger.
        v.parse::<u64>().ok().map(Value::Ulong)
    } else if let Some(v) = rest.strip_prefix(STRING) {
        Some(Value::String(v.to_owned()))
    } else if let Some(v) = rest.strip_prefix(BINARY) {
        if v.len() % 2 != 0 {
            return None;
        }
        (0..v.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&v[i..i + 2], 16).ok())
            .collect::<Option<Vec<u8>>>()
            .map(Value::Binary)
    } else {
        Some(Value::String(s.to_owned()))
    }
}

/// `UUID.fromString`.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 {
        return None;
    }
    let msb = (u64::from_str_radix(parts[0], 16).ok()? << 32)
        | (u64::from_str_radix(parts[1], 16).ok()? << 16)
        | u64::from_str_radix(parts[2], 16).ok()?;
    let lsb =
        (u64::from_str_radix(parts[3], 16).ok()? << 48) | u64::from_str_radix(parts[4], 16).ok()?;
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&msb.to_be_bytes());
    out[8..].copy_from_slice(&lsb.to_be_bytes());
    Some(out)
}

// --------------------------------------------------------- content types

/// `AMQPContentTypeSupport.parseContentTypeForTextualCharset`: the charset
/// of textual content, `None` for content that is not text or a charset
/// Java does not have.
pub fn textual_charset(content_type: Option<&str>) -> Option<String> {
    let ct = content_type?.trim();
    if ct.is_empty() {
        return None;
    }
    let (ty, rest) = ct.split_once('/')?;
    let ty = ty.to_lowercase().trim().to_owned();
    let rest_lower = rest.to_lowercase();
    let (sub, params) = match rest_lower.split_once(';') {
        Some((s, p)) => (s.trim().to_owned(), Some(p.to_owned())),
        None => (rest_lower.trim().to_owned(), None),
    };
    if sub.is_empty() {
        return None;
    }
    let textual = ty == "text"
        || (ty == "application"
            && (matches!(
                sub.as_str(),
                "xml" | "json" | "javascript" | "xml-dtd" | "ecmascript"
            ) || sub.ends_with("+xml")
                || sub.ends_with("+json")));
    if !textual {
        return None;
    }
    let mut charset = None;
    for p in params.iter().flat_map(|p| p.split(';')) {
        if let Some((name, value)) = p.trim().split_once('=')
            && name.trim().eq_ignore_ascii_case("charset")
        {
            let v = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(value);
            charset = Some(v.to_uppercase());
            break;
        }
    }
    let charset = charset.unwrap_or_else(|| "UTF-8".to_owned());
    // Charset.forName: the charsets every Java has.
    const KNOWN: [&str; 9] = [
        "UTF-8",
        "UTF8",
        "US-ASCII",
        "ASCII",
        "ISO-8859-1",
        "UTF-16",
        "UTF-16BE",
        "UTF-16LE",
        "UTF-32",
    ];
    KNOWN.contains(&charset.as_str()).then_some(charset)
}

fn is_utf8(charset: &str) -> bool {
    charset == "UTF-8" || charset == "UTF8"
}

// ------------------------------------------------------------ AMQP -> Core

fn put(core: &mut CoreMessage, key: &str, value: PropertyValue) {
    core.properties.insert(SimpleString::from(key), value);
}

fn string_value(s: &str) -> PropertyValue {
    PropertyValue::String(SimpleString::from(s))
}

/// `MessageUtil.setObjectProperty` with the JMSX names Core keeps
/// elsewhere.
fn set_object_property(core: &mut CoreMessage, key: &str, value: PropertyValue) {
    match key {
        JMSX_GROUP_ID => put(core, HDR_GROUP_ID, to_string_property(value)),
        JMSX_GROUP_SEQ => {
            if let Some(i) = int_of(&value) {
                put(core, HDR_GROUP_SEQUENCE, PropertyValue::Int(i));
            }
        }
        JMSX_USER_ID => put(core, HDR_VALIDATED_USER, to_string_property(value)),
        _ => put(core, key, value),
    }
}

fn to_string_property(v: PropertyValue) -> PropertyValue {
    match v {
        PropertyValue::String(_) | PropertyValue::Null => v,
        other => string_value(&property_text(&other)),
    }
}

fn int_of(v: &PropertyValue) -> Option<i32> {
    match v {
        PropertyValue::Int(i) => Some(*i),
        PropertyValue::Long(l) => Some(*l as i32),
        PropertyValue::Short(s) => Some((*s).into()),
        PropertyValue::Byte(b) => Some((*b).into()),
        PropertyValue::String(s) => s.to_string_lossy().parse().ok(),
        _ => None,
    }
}

fn property_text(v: &PropertyValue) -> String {
    match v {
        PropertyValue::Null => "null".into(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Byte(b) => b.to_string(),
        PropertyValue::Short(s) => s.to_string(),
        PropertyValue::Int(i) => i.to_string(),
        PropertyValue::Long(l) => l.to_string(),
        PropertyValue::Float(f) => saranga_storage::java::float(*f),
        PropertyValue::Double(d) => saranga_storage::java::double(*d),
        PropertyValue::Char(c) => String::from_utf16_lossy(&[*c]),
        PropertyValue::String(s) => s.to_string_lossy(),
        PropertyValue::Bytes(b) => format!("{b:?}"),
    }
}

/// An AMQP value as a Core property (`AmqpCoreConverter.setProperty`), or
/// `None` where Core has no such type.
fn to_property(value: &Value) -> Option<PropertyValue> {
    Some(match value {
        Value::Null => PropertyValue::Null,
        Value::Boolean(b) => PropertyValue::Bool(*b),
        Value::Byte(b) => PropertyValue::Byte(*b),
        Value::Short(s) => PropertyValue::Short(*s),
        Value::Int(i) => PropertyValue::Int(*i),
        Value::Long(l) => PropertyValue::Long(*l),
        Value::Float(f) => PropertyValue::Float(*f),
        Value::Double(d) => PropertyValue::Double(*d),
        Value::Char(c) => PropertyValue::Char(*c as u16),
        Value::String(s) => string_value(s),
        Value::Ulong(v) => PropertyValue::Long(*v as i64),
        Value::Uint(v) => match i32::try_from(*v) {
            Ok(i) => PropertyValue::Int(i),
            Err(_) => PropertyValue::Long(i64::from(*v)),
        },
        Value::Ushort(v) => match i16::try_from(*v) {
            Ok(s) => PropertyValue::Short(s),
            Err(_) => PropertyValue::Int(i32::from(*v)),
        },
        Value::Ubyte(v) => match i8::try_from(*v) {
            Ok(b) => PropertyValue::Byte(b),
            Err(_) => PropertyValue::Short(i16::from(*v)),
        },
        Value::Symbol(s) => string_value(s.as_str()),
        Value::Binary(b) => PropertyValue::Bytes(b.clone()),
        Value::Decimal32(_) | Value::Decimal64(_) | Value::Decimal128(_) => {
            // Decimal*.floatValue/doubleValue: not decoded here.
            return None;
        }
        _ => return None,
    })
}

/// `setProperty`, falling back to the encoded value under `encoded_key`
/// (`encodeUnsupportedMessagePropertyType`).
fn set_or_encode(core: &mut CoreMessage, key: &str, encoded_key: &str, value: &Value) {
    match to_property(value) {
        Some(p) => set_object_property(core, key, p),
        None => put(
            core,
            encoded_key,
            PropertyValue::Bytes(codec::to_vec(value)),
        ),
    }
}

fn write_text(w: &mut Writer, text: Option<&str>) {
    w.nullable_simple_string(text.map(SimpleString::from).as_ref());
}

/// `writeNullableString`, as stream messages hold strings.
fn write_string(w: &mut Writer, text: Option<&str>) {
    let s = text.map(|t| JString::from_code_units(t.encode_utf16().collect()));
    w.nullable_string(s.as_ref());
}

/// A stream message body item (`CoreStreamMessageWrapper.writeObject`).
fn write_stream_item(w: &mut Writer, v: &Value) {
    use saranga_buffer::constants as c;
    match v {
        Value::String(s) => {
            w.u8(c::STRING);
            write_string(w, Some(s));
        }
        Value::Null => {
            w.u8(c::STRING);
            write_string(w, None);
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
        other => tracing::debug!("stream item {other:?} has no Core type"),
    }
}

/// A map message body entry (`CoreMapMessageWrapper.setObject`).
fn map_entry(v: &Value) -> Option<PropertyValue> {
    Some(match v {
        Value::Uint(u) => PropertyValue::Int(*u as i32),
        Value::Ushort(u) => PropertyValue::Short(*u as i16),
        Value::Ubyte(u) => PropertyValue::Byte(*u as i8),
        Value::Ulong(u) => PropertyValue::Long(*u as i64),
        Value::Symbol(_) | Value::Decimal32(_) | Value::Decimal64(_) | Value::Decimal128(_) => {
            return None;
        }
        other => to_property(other)?,
    })
}

/// `AMQPMessage.toCore` (`AmqpCoreConverter.toCore`).
pub fn to_core(m: &AmqpMessage, now: i64) -> CoreMessage {
    let mut core = CoreMessage {
        message_id: m.message_id(),
        ..Default::default()
    };
    let properties = m.properties();
    let properties = properties.as_deref();
    let content_type = properties
        .and_then(|p| p.content_type.as_ref())
        .map(Symbol::as_str);
    let is = |expected: &str| content_type == Some(expected);
    let mut body = Writer::new();
    let mut original = None;
    match m.body() {
        None => {
            core.message_type = if is(SERIALIZED_JAVA_OBJECT) {
                // CoreObjectMessageWrapper.encode with no payload.
                body.i32(0);
                message_type::OBJECT
            } else if is(OCTET_STREAM) || content_type.is_none() {
                message_type::BYTES
            } else if textual_charset(content_type).is_some() {
                write_text(&mut body, None);
                message_type::TEXT
            } else {
                message_type::DEFAULT
            };
            original = Some(encoding::NULL);
        }
        Some((section::DATA, Value::Binary(payload))) => {
            core.message_type = if is(SERIALIZED_JAVA_OBJECT) {
                body.i32(payload.len() as i32).bytes(&payload);
                message_type::OBJECT
            } else if is(OCTET_STREAM) {
                body.bytes(&payload);
                message_type::BYTES
            } else {
                match textual_charset(content_type)
                    .filter(|c| is_utf8(c))
                    .and_then(|_| String::from_utf8(payload.clone()).ok())
                {
                    Some(text) => {
                        write_text(&mut body, Some(&text));
                        message_type::TEXT
                    }
                    None => {
                        body.bytes(&payload);
                        message_type::BYTES
                    }
                }
            };
            original = Some(encoding::DATA);
        }
        Some((section::AMQP_SEQUENCE, Value::List(items))) => {
            for i in &items {
                write_stream_item(&mut body, i);
            }
            core.message_type = message_type::STREAM;
            original = Some(encoding::SEQUENCE);
        }
        Some((section::AMQP_VALUE, value)) => match value {
            Value::Null => {
                write_text(&mut body, None);
                core.message_type = message_type::TEXT;
                original = Some(encoding::VALUE_NULL);
            }
            Value::String(s) => {
                write_text(&mut body, Some(&s));
                core.message_type = message_type::TEXT;
                original = Some(encoding::VALUE_STRING);
            }
            Value::Binary(b) => {
                if is(SERIALIZED_JAVA_OBJECT) {
                    body.i32(b.len() as i32).bytes(&b);
                    core.message_type = message_type::OBJECT;
                } else {
                    body.bytes(&b);
                    core.message_type = message_type::BYTES;
                }
                original = Some(encoding::VALUE_BINARY);
            }
            Value::List(items) => {
                for i in &items {
                    write_stream_item(&mut body, i);
                }
                core.message_type = message_type::STREAM;
                original = Some(encoding::VALUE_LIST);
            }
            Value::Map(entries) => {
                // CoreMapMessageWrapper: a new TypedProperties.
                let mut java = JavaMap::new(Kind::Hash(None));
                for (k, v) in &entries {
                    let key = match k {
                        Value::String(s) => s.clone(),
                        Value::Symbol(s) => s.0.clone(),
                        _ => continue,
                    };
                    if let Some(p) = map_entry(v) {
                        java.insert(SimpleString::from(key.as_str()), p);
                    }
                }
                let mut map = TypedProperties::new();
                for (k, v) in java.iter() {
                    map.insert(k.clone(), v.clone());
                }
                if java.is_empty() {
                    map = empty_typed_properties();
                }
                let mut w = Writer::new();
                map.encode(&mut w);
                body.bytes(w.as_slice());
                core.message_type = message_type::MAP;
                original = Some(encoding::VALUE_MAP);
            }
            other => {
                // Any other value: the encoded section as bytes.
                let mut s = Vec::new();
                encode_section(section::AMQP_VALUE, other, &mut s);
                body.bytes(&s);
                core.message_type = message_type::BYTES;
            }
        },
        Some(_) => core.message_type = message_type::BYTES,
    }
    core.body = body.into_vec().into();
    if let Some(o) = original {
        put(
            &mut core,
            jms_amqp::ORIGINAL_ENCODING,
            PropertyValue::Short(o),
        );
    }
    core.expiration = m.expiration();

    // processHeader
    match m.header() {
        Some(h) => {
            put(&mut core, jms_amqp::HEADER, PropertyValue::Bool(true));
            if let Some(d) = h.durable {
                put(
                    &mut core,
                    jms_amqp::HEADER_DURABLE,
                    PropertyValue::Bool(true),
                );
                core.durable = d;
            }
            core.priority = match h.priority {
                Some(p) => {
                    put(
                        &mut core,
                        jms_amqp::HEADER_PRIORITY,
                        PropertyValue::Bool(true),
                    );
                    p
                }
                None => 4,
            };
            if let Some(f) = h.first_acquirer {
                put(&mut core, jms_amqp::FIRST_ACQUIRER, PropertyValue::Bool(f));
            }
            if let Some(c) = h.delivery_count {
                put(
                    &mut core,
                    JMSX_DELIVERY_COUNT,
                    PropertyValue::Long(i64::from(c) + 1),
                );
            }
        }
        None => core.priority = 4,
    }

    // processMessageAnnotations
    if let Some(ma) = m.message_annotations() {
        for (k, v) in ma.iter() {
            let key = k.as_str();
            if key == annotation::DELIVERY_TIME && !v.is_null() {
                if let Some(t) = v.as_i64() {
                    put(
                        &mut core,
                        HDR_SCHEDULED_DELIVERY_TIME,
                        PropertyValue::Long(t),
                    );
                }
            } else if key == annotation::DELIVERY_DELAY && !v.is_null() {
                if let Some(d) = v.as_i64().filter(|&d| d > 0) {
                    put(
                        &mut core,
                        HDR_SCHEDULED_DELIVERY_TIME,
                        PropertyValue::Long(now + d),
                    );
                }
            } else if key == annotation::INGRESS_TIME
                && !v.is_null()
                && let Some(t) = v.as_i64()
            {
                put(&mut core, HDR_INGRESS_TIMESTAMP, PropertyValue::Long(t));
            }
            set_or_encode(
                &mut core,
                &format!("{}{key}", jms_amqp::MESSAGE_ANNOTATION_PREFIX),
                &format!("{}{key}", jms_amqp::ENCODED_MESSAGE_ANNOTATION_PREFIX),
                v,
            );
        }
    }

    // processApplicationProperties
    for (k, v) in m.application_properties() {
        match to_property(&v) {
            Some(p) => set_object_property(&mut core, &k, p),
            None => tracing::debug!(
                "message {}: application property {k} has no Core type",
                m.message_id()
            ),
        }
    }

    // processProperties
    if let Some(p) = properties {
        if let Some(id) = &p.message_id {
            if let Some(s) = to_message_id_string(id) {
                put(&mut core, NATIVE_MESSAGE_ID, string_value(&s));
            }
            core.user_id = Some(match id {
                Value::Uuid(u) => *u,
                _ => *uuid::Uuid::new_v4().as_bytes(),
            });
        }
        if let Some(u) = &p.user_id {
            put(
                &mut core,
                HDR_VALIDATED_USER,
                string_value(&String::from_utf8_lossy(u)),
            );
        }
        if let Some(to) = &p.to {
            core.address = Some(SimpleString::from(to.as_str()));
        }
        if let Some(s) = &p.subject {
            put(&mut core, JMS_TYPE, string_value(s));
        }
        if let Some(r) = &p.reply_to {
            let prefix = match queue_annotation(m, annotation::JMS_REPLY_TO) {
                dest_type::TEMP_QUEUE => TEMP_QUEUE_PREFIX,
                dest_type::TOPIC => TOPIC_PREFIX,
                dest_type::TEMP_TOPIC => TEMP_TOPIC_PREFIX,
                _ => QUEUE_PREFIX,
            };
            put(
                &mut core,
                JMS_REPLY_TO,
                string_value(&format!("{prefix}{r}")),
            );
        }
        if let Some(c) = &p.correlation_id {
            let v = to_correlation_id(c).unwrap_or_else(|| string_value(&format!("{c:?}")));
            put(&mut core, JMS_CORRELATION_ID, v);
        }
        if let Some(t) = &p.content_type {
            put(&mut core, jms_amqp::CONTENT_TYPE, string_value(t.as_str()));
        }
        if let Some(e) = &p.content_encoding {
            put(
                &mut core,
                jms_amqp::CONTENT_ENCODING,
                string_value(e.as_str()),
            );
        }
        if let Some(Timestamp(t)) = p.creation_time {
            core.timestamp = t;
        }
        if let Some(g) = &p.group_id {
            put(&mut core, HDR_GROUP_ID, string_value(g));
        }
        if let Some(s) = p.group_sequence {
            put(&mut core, HDR_GROUP_SEQUENCE, PropertyValue::Int(s as i32));
        }
        if let Some(g) = &p.reply_to_group_id {
            put(&mut core, jms_amqp::REPLYTO_GROUP_ID, string_value(g));
        }
        if let Some(Timestamp(t)) = p.absolute_expiry_time {
            core.expiration = t;
        }
    }

    // processFooter
    if let Some(Value::Map(entries)) = m.footer() {
        for (k, v) in &entries {
            let key = match k {
                Value::Symbol(s) => s.0.clone(),
                Value::String(s) => s.clone(),
                _ => continue,
            };
            set_or_encode(
                &mut core,
                &format!("{}{key}", jms_amqp::FOOTER_PREFIX),
                &format!("{}{key}", jms_amqp::ENCODED_FOOTER_PREFIX),
                v,
            );
        }
    }

    // processExtraProperties
    if let Some(extra) = m.extra_properties() {
        for (k, v) in extra.iter() {
            if !k.eq_str(crate::message::ADDRESS_PROPERTY) {
                core.properties.insert(k.clone(), v.clone());
            }
        }
    }

    if m.header().is_some() && core.expiration == 0 {
        let ttl = m.header().and_then(|h| h.ttl).map_or(0, i64::from);
        core.expiration = if ttl == 0 { 0 } else { now + ttl };
    }
    core.durable = m.is_durable();
    core.priority = m.priority();
    core.address = m.address();
    match m.routing_type() {
        Some(t) => put(&mut core, HDR_ROUTING_TYPE, PropertyValue::Byte(t)),
        None => {
            core.properties
                .remove(&SimpleString::from(HDR_ROUTING_TYPE));
        }
    }
    core
}

/// `parseQueueAnnotation`: the destination type annotation, queue by
/// default.
fn queue_annotation(m: &AmqpMessage, name: &str) -> i8 {
    match m.annotation(name).as_ref().and_then(Value::as_i64) {
        Some(v) => v as i8,
        None => dest_type::QUEUE,
    }
}

// ------------------------------------------------------------ Core -> AMQP

fn property<'a>(core: &'a CoreMessage, key: &str) -> Option<&'a PropertyValue> {
    core.properties.get_str(key)
}

fn string_property(core: &CoreMessage, key: &str) -> Option<String> {
    property(core, key).and_then(|v| match v {
        PropertyValue::Null => None,
        PropertyValue::String(s) => Some(s.to_string_lossy()),
        other => Some(property_text(other)),
    })
}

/// A Core property as an AMQP value (`getObjectProperty`, with `byte[]`
/// as `Binary`).
fn property_value(v: &PropertyValue) -> Value {
    match v {
        PropertyValue::Null => Value::Null,
        PropertyValue::Bool(b) => Value::Boolean(*b),
        PropertyValue::Byte(b) => Value::Byte(*b),
        PropertyValue::Bytes(b) => Value::Binary(b.clone()),
        PropertyValue::Short(s) => Value::Short(*s),
        PropertyValue::Int(i) => Value::Int(*i),
        PropertyValue::Long(l) => Value::Long(*l),
        PropertyValue::Float(f) => Value::Float(*f),
        PropertyValue::Double(d) => Value::Double(*d),
        PropertyValue::Char(c) => Value::Char(u32::from(*c)),
        PropertyValue::String(s) => Value::String(s.to_string_lossy()),
    }
}

/// `MessageUtil.getPropertyNames`: the JMS view of a Core message's
/// property names.
fn property_names(core: &CoreMessage) -> Vec<String> {
    let mut names = Vec::new();
    for (k, _) in core.properties.entries() {
        let name = k.to_string_lossy();
        let mapped = match name.as_str() {
            HDR_GROUP_ID => JMSX_GROUP_ID.to_owned(),
            HDR_GROUP_SEQUENCE => JMSX_GROUP_SEQ.to_owned(),
            HDR_VALIDATED_USER => JMSX_USER_ID.to_owned(),
            n if (!n.starts_with("JMS") || n.starts_with("JMSX") || n.starts_with("JMS_"))
                && !n.starts_with(CONNECTION_ID)
                && n != HDR_ROUTING_TYPE
                && !n.starts_with(HDR_ROUTE_TO_IDS) =>
            {
                n.to_owned()
            }
            _ => continue,
        };
        if !names.contains(&mapped) {
            names.push(mapped);
        }
    }
    if !names.iter().any(|n| n == JMSX_DELIVERY_COUNT) {
        names.push(JMSX_DELIVERY_COUNT.to_owned());
    }
    // A HashSet<String>.
    let hashes: Vec<i32> = names.iter().map(JavaHash::java_hash).collect();
    saranga_storage::java::hash_map_order(&hashes, None)
        .into_iter()
        .map(|i| names[i].clone())
        .collect()
}

/// A `TypedProperties` that is not null and has no entries.
fn empty_typed_properties() -> TypedProperties {
    let mut p = TypedProperties::new();
    let k = SimpleString::from("x");
    p.insert(k.clone(), PropertyValue::Null);
    p.remove(&k);
    p
}

/// `AMQPMessageSupport.destinationType`.
fn destination_type(d: &str) -> i8 {
    if d.starts_with(QUEUE_PREFIX) {
        dest_type::QUEUE
    } else if d.starts_with(TOPIC_PREFIX) {
        dest_type::TOPIC
    } else if d.starts_with(TEMP_QUEUE_PREFIX) {
        dest_type::TEMP_QUEUE
    } else if d.starts_with(TEMP_TOPIC_PREFIX) {
        dest_type::TEMP_TOPIC
    } else {
        dest_type::QUEUE
    }
}

/// `AMQPMessageSupport.toAddress`: without the JMS prefix.
fn to_address(d: &str) -> String {
    for p in [
        QUEUE_PREFIX,
        TOPIC_PREFIX,
        TEMP_QUEUE_PREFIX,
        TEMP_TOPIC_PREFIX,
    ] {
        if let Some(rest) = d.strip_prefix(p) {
            return rest.to_owned();
        }
    }
    d.to_owned()
}

/// A stream message body item (`streamReadObject`).
fn read_stream_item(r: &mut Reader<'_>) -> Option<Value> {
    use saranga_buffer::constants as c;
    let t = r.u8().ok()?;
    Some(match t {
        c::BOOLEAN => Value::Boolean(r.bool().ok()?),
        c::BYTE => Value::Byte(r.i8().ok()?),
        c::SHORT => Value::Short(r.i16().ok()?),
        c::CHAR => Value::Char(u32::from(r.u16().ok()?)),
        c::INT => Value::Int(r.i32().ok()?),
        c::LONG => Value::Long(r.i64().ok()?),
        c::FLOAT => Value::Float(r.f32().ok()?),
        c::DOUBLE => Value::Double(r.f64().ok()?),
        c::STRING => match r.nullable_string().ok()? {
            Some(s) => Value::String(s.to_string_lossy()),
            None => Value::Null,
        },
        c::BYTES => {
            let n = r.i32().ok()?;
            Value::Binary(r.bytes(n.max(0) as usize).ok()?.to_vec())
        }
        _ => return None,
    })
}

/// The body section and the `x-opt-jms-msg-type` of a Core message
/// (`createAMQPSection`).
fn core_body(core: &CoreMessage, properties: &mut Properties) -> (i8, Option<(u64, Value)>) {
    let original = match property(core, jms_amqp::ORIGINAL_ENCODING) {
        Some(PropertyValue::Short(s)) => *s,
        _ => encoding::UNKNOWN,
    };
    let mut r = Reader::new(&core.body);
    match core.message_type {
        message_type::TEXT => {
            let text = r
                .nullable_simple_string()
                .ok()
                .flatten()
                .map(|s| s.to_string_lossy());
            let body = match original {
                encoding::NULL => None,
                encoding::DATA => Some((
                    section::DATA,
                    Value::Binary(text.map(String::into_bytes).unwrap_or_default()),
                )),
                _ => Some((section::AMQP_VALUE, text.map_or(Value::Null, Value::String))),
            };
            (jms_type::TEXT, body)
        }
        message_type::BYTES => {
            let payload = Value::Binary(core.body.to_vec());
            let body = match original {
                encoding::NULL => None,
                encoding::VALUE_BINARY => Some((section::AMQP_VALUE, payload)),
                _ => Some((section::DATA, payload)),
            };
            (jms_type::BYTES, body)
        }
        message_type::MAP => {
            // The body's TypedProperties, a HashMap once decoded, whose
            // names getMapNames collects into a HashSet<String>.
            let map = TypedProperties::decode(&mut r).unwrap_or_default();
            let decoded = JavaMap::decoded(map.entries().to_vec());
            let mut names: JavaMap<String, PropertyValue> = JavaMap::new(Kind::Hash(None));
            for (k, v) in decoded.iter() {
                names.insert(k.to_string_lossy(), v.clone());
            }
            let entries = names
                .iter()
                .map(|(k, v)| (Value::String(k.clone()), property_value(v)))
                .collect();
            (
                jms_type::MAP,
                Some((section::AMQP_VALUE, Value::Map(entries))),
            )
        }
        message_type::STREAM => {
            let mut items = Vec::new();
            while !r.is_empty() {
                match read_stream_item(&mut r) {
                    Some(v) => items.push(v),
                    None => break,
                }
            }
            let code = if original == encoding::SEQUENCE {
                section::AMQP_SEQUENCE
            } else {
                section::AMQP_VALUE
            };
            (jms_type::STREAM, Some((code, Value::List(items))))
        }
        message_type::OBJECT => {
            properties.content_type = Some(Symbol::from(SERIALIZED_JAVA_OBJECT));
            let payload = r
                .i32()
                .ok()
                .and_then(|n| r.bytes(n.max(0) as usize).ok())
                .map(<[u8]>::to_vec)
                .unwrap_or_default();
            let code = if original == encoding::VALUE_BINARY {
                section::AMQP_VALUE
            } else {
                section::DATA
            };
            (jms_type::OBJECT, Some((code, Value::Binary(payload))))
        }
        _ => {
            let body = if r.is_empty() {
                None
            } else {
                r.nullable_simple_string()
                    .ok()
                    .flatten()
                    .map(|s| (section::AMQP_VALUE, Value::String(s.to_string_lossy())))
            };
            (jms_type::MESSAGE, body)
        }
    }
}

/// Decodes a value Core kept encoded (`decodeEmbeddedAMQPType`).
fn decode_embedded(v: &PropertyValue) -> Value {
    match v {
        PropertyValue::Bytes(b) => codec::from_slice(b).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// `CoreAmqpConverter.fromCore`: the AMQP message a Core message becomes
/// for an AMQP consumer.
pub fn from_core(core: &CoreMessage, now: i64) -> AmqpMessage {
    let mut header: Option<Header> = None;
    let mut properties = Properties::default();
    let mut da: Option<JavaMap<Symbol, Value>> = None;
    let mut ma: JavaMap<Symbol, Value> = JavaMap::new(Kind::Hash(None));
    let mut ap: Option<JavaMap<String, Value>> = None;
    let mut footer: Option<JavaMap<Symbol, Value>> = None;

    let (msg_type, body) = core_body(core, &mut properties);
    ma.insert(
        Symbol::from(annotation::JMS_MSG_TYPE),
        Value::Byte(msg_type),
    );
    if core.durable {
        header.get_or_insert_with(Header::default).durable = Some(true);
    }
    let priority = core.priority;
    if priority != 4 {
        header.get_or_insert_with(Header::default).priority = Some(priority);
    }
    if let Some(t) = string_property(core, JMS_TYPE) {
        properties.subject = Some(t);
    }
    match string_property(core, NATIVE_MESSAGE_ID) {
        Some(id) => {
            properties.message_id = Some(to_id_object(&id).unwrap_or(Value::String(id)));
        }
        None => {
            if let Some(u) = &core.user_id {
                properties.message_id = Some(Value::String(format!("ID:{}", uuid_string(u))));
            }
        }
    }
    // getDestination: the address with its routing type's JMS prefix.
    if let Some(a) = core.address.as_ref().map(SimpleString::to_string_lossy)
        && !a.is_empty()
    {
        let prefix = match property(core, HDR_ROUTING_TYPE) {
            Some(PropertyValue::Byte(0)) => TOPIC_PREFIX,
            _ => QUEUE_PREFIX,
        };
        let dest = if a.starts_with(prefix) {
            a
        } else {
            format!("{prefix}{a}")
        };
        properties.to = Some(to_address(&dest));
        ma.insert(
            Symbol::from(annotation::JMS_DEST),
            Value::Byte(destination_type(&dest)),
        );
    }
    let reply_to = string_property(core, JMS_REPLY_TO);
    if let Some(r) = &reply_to {
        properties.reply_to = Some(to_address(r));
        ma.insert(
            Symbol::from(annotation::JMS_REPLY_TO),
            Value::Byte(destination_type(r)),
        );
    }
    if let Some(PropertyValue::Long(t)) = property(core, HDR_SCHEDULED_DELIVERY_TIME)
        && *t > 0
    {
        ma.insert(Symbol::from(annotation::DELIVERY_TIME), Value::Long(*t));
    }
    properties.correlation_id = match property(core, JMS_CORRELATION_ID) {
        Some(PropertyValue::String(s)) => {
            let s = s.to_string_lossy();
            Some(to_id_object(&s).unwrap_or(Value::String(s)))
        }
        Some(PropertyValue::Bytes(b)) => Some(Value::Binary(b.clone())),
        Some(PropertyValue::Null) | None => None,
        Some(other) => Some(property_value(other)),
    };
    if core.expiration != 0 {
        let ttl = (core.expiration - now).max(1);
        header.get_or_insert_with(Header::default).ttl = Some(ttl as u32);
        properties.absolute_expiry_time = Some(Timestamp(core.expiration));
    }
    if core.timestamp != 0 {
        properties.creation_time = Some(Timestamp(core.timestamp));
    }

    for key in property_names(core) {
        if key.starts_with("JMSX") {
            match key.as_str() {
                JMSX_USER_ID => {
                    if let Some(u) = string_property(core, HDR_VALIDATED_USER) {
                        properties.user_id = Some(u.into_bytes());
                    }
                    continue;
                }
                JMSX_GROUP_ID => {
                    properties.group_id = string_property(core, HDR_GROUP_ID);
                    continue;
                }
                JMSX_GROUP_SEQ => {
                    let seq = property(core, HDR_GROUP_SEQUENCE)
                        .and_then(int_of)
                        .unwrap_or(0);
                    properties.group_sequence = Some(seq as u32);
                    continue;
                }
                _ => {}
            }
        } else if key.starts_with(jms_amqp::PREFIX) {
            let value = || property(core, &key).cloned().unwrap_or(PropertyValue::Null);
            let full = key.as_str();
            if full == jms_amqp::NATIVE {
                continue;
            } else if full == jms_amqp::FIRST_ACQUIRER {
                let v = matches!(value(), PropertyValue::Bool(true));
                header.get_or_insert_with(Header::default).first_acquirer = Some(v);
                continue;
            } else if full == jms_amqp::HEADER {
                header.get_or_insert_with(Header::default);
                continue;
            } else if full == jms_amqp::HEADER_DURABLE {
                header.get_or_insert_with(Header::default).durable = Some(core.durable);
                continue;
            } else if full == jms_amqp::HEADER_PRIORITY {
                header.get_or_insert_with(Header::default).priority = Some(priority);
                continue;
            } else if full.starts_with(jms_amqp::PROPERTIES) {
                continue;
            } else if let Some(name) = full.strip_prefix(jms_amqp::DELIVERY_ANNOTATION_PREFIX) {
                da.get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
                    .insert(Symbol::from(name), property_value(&value()));
                continue;
            } else if let Some(name) =
                full.strip_prefix(jms_amqp::ENCODED_DELIVERY_ANNOTATION_PREFIX)
            {
                da.get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
                    .insert(Symbol::from(name), decode_embedded(&value()));
                continue;
            } else if let Some(name) = full.strip_prefix(jms_amqp::MESSAGE_ANNOTATION_PREFIX) {
                ma.insert(Symbol::from(name), property_value(&value()));
                continue;
            } else if let Some(name) =
                full.strip_prefix(jms_amqp::ENCODED_MESSAGE_ANNOTATION_PREFIX)
            {
                ma.insert(Symbol::from(name), decode_embedded(&value()));
                continue;
            } else if full == jms_amqp::CONTENT_TYPE {
                properties.content_type = string_property(core, &key).map(Symbol::from);
                continue;
            } else if full == jms_amqp::CONTENT_ENCODING {
                properties.content_encoding = string_property(core, &key).map(Symbol::from);
                continue;
            } else if full == jms_amqp::REPLYTO_GROUP_ID {
                properties.reply_to_group_id = string_property(core, &key);
                continue;
            } else if full == jms_amqp::ORIGINAL_ENCODING {
                continue;
            } else if let Some(name) = full.strip_prefix(jms_amqp::ENCODED_FOOTER_PREFIX) {
                footer
                    .get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
                    .insert(Symbol::from(name), decode_embedded(&value()));
                continue;
            } else if let Some(name) = full.strip_prefix(jms_amqp::FOOTER_PREFIX) {
                footer
                    .get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
                    .insert(Symbol::from(name), property_value(&value()));
                continue;
            }
        } else if key == NATIVE_MESSAGE_ID || key.ends_with(HDR_SCHEDULED_DELIVERY_TIME) {
            continue;
        } else if key == HDR_INGRESS_TIMESTAMP {
            if let Some(PropertyValue::Long(t)) = property(core, &key) {
                ma.insert(Symbol::from(annotation::INGRESS_TIME), Value::Long(*t));
            }
            continue;
        }
        let v = property(core, &key).map_or(Value::Null, property_value);
        ap.get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
            .insert(key, v);
    }

    // AMQPStandardMessage.createMessage
    let mut data = Vec::new();
    if let Some(h) = &header {
        crate::composite::Composite::encode(h, &mut data);
    }
    let symbol_map = |m: &JavaMap<Symbol, Value>| {
        Value::Map(
            m.iter()
                .map(|(k, v)| (Value::Symbol(k.clone()), v.clone()))
                .collect(),
        )
    };
    if let Some(d) = &da {
        encode_section(section::DELIVERY_ANNOTATIONS, symbol_map(d), &mut data);
    }
    encode_section(section::MESSAGE_ANNOTATIONS, symbol_map(&ma), &mut data);
    crate::composite::Composite::encode(&properties, &mut data);
    if let Some(a) = &ap {
        encode_section(
            section::APPLICATION_PROPERTIES,
            Value::Map(
                a.iter()
                    .map(|(k, v)| (Value::String(k.clone()), v.clone()))
                    .collect(),
            ),
            &mut data,
        );
    }
    if let Some((code, value)) = body {
        codec::encode(&Value::described(Descriptor::Code(code), value), &mut data);
    }
    if let Some(f) = &footer {
        encode_section(section::FOOTER, symbol_map(f), &mut data);
    }
    let mut m = AmqpMessage::new(0, data, now).expect("a message this module encoded decodes");
    m.set_message_id(core.message_id);
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip() {
        for id in [
            Value::String("ID:abc".into()),
            Value::String("plain".into()),
            Value::Ulong(5),
            Value::Uuid([7; 16]),
            Value::Binary(vec![0x0a, 0xbc]),
        ] {
            let s = to_message_id_string(&id).unwrap();
            assert_eq!(to_id_object(&s), Some(id), "{s}");
        }
    }

    #[test]
    fn textual_content_types() {
        assert_eq!(
            textual_charset(Some("text/plain")).as_deref(),
            Some("UTF-8")
        );
        assert_eq!(
            textual_charset(Some("application/json;charset=iso-8859-1")).as_deref(),
            Some("ISO-8859-1")
        );
        assert_eq!(textual_charset(Some("application/octet-stream")), None);
        assert_eq!(textual_charset(Some("text")), None);
    }
}
