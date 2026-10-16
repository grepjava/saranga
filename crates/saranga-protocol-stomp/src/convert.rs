//! Between STOMP frames and Core messages (`StompUtils`,
//! `VersionedStompFrameHandler.onSend` and `createMessageFrame`).

use saranga_buffer::{PropertyValue, Reader, SimpleString, Writer};
use saranga_message::{CoreMessage, message_type};
use saranga_server::RoutingType;
use saranga_server::message as m;
use saranga_storage::java;

use crate::frame::{Frame, Kind, names};

/// `MessageUtil.CORRELATIONID_HEADER_NAME`.
pub const JMS_CORRELATION_ID: &str = "JMSCorrelationID";
/// `MessageUtil.TYPE_HEADER_NAME`.
pub const JMS_TYPE: &str = "JMSType";
/// `MessageUtil.REPLYTO_HEADER_NAME`.
pub const JMS_REPLY_TO: &str = "JMSReplyTo";
/// `MessageUtil.JMSXGROUPID`.
pub const JMSX_GROUP_ID: &str = "JMSXGroupID";
/// `Message.HDR_CONTENT_TYPE`.
pub const HDR_CONTENT_TYPE: &str = "_AMQ_CONTENT_TYPE";
/// `Message.HDR_PREFIX`.
pub const HDR_PREFIX: &str = "_AMQ_PREFIX";
/// `Message.HDR_INGRESS_TIMESTAMP`.
pub const HDR_INGRESS_TIMESTAMP: &str = "_AMQ_INGRESS_TIMESTAMP";
/// `MessageUtil.CONNECTION_ID_PROPERTY_NAME`.
pub const CONNECTION_ID_PROPERTY: &str = "__AMQ_CID";

/// `DEFAULT_MESSAGE_PRIORITY`.
const DEFAULT_PRIORITY: i8 = 4;

/// A header that does not parse (`NumberFormatException`).
#[derive(Debug)]
pub struct BadHeader(pub String);

fn put_string(message: &mut CoreMessage, name: &str, value: &str) {
    message
        .properties
        .insert(name.into(), PropertyValue::String(value.into()));
}

/// `Byte.parseByte`, `Long.parseLong`: an optional `+` or `-`, then
/// decimal digits.
fn java_parse<T: std::str::FromStr>(name: &str, value: &str) -> Result<T, BadHeader> {
    let ok = !value.is_empty()
        && value
            .strip_prefix(['+', '-'])
            .unwrap_or(value)
            .bytes()
            .all(|b| b.is_ascii_digit());
    value
        .parse::<T>()
        .ok()
        .filter(|_| ok)
        .ok_or_else(|| BadHeader(format!("For input string: \"{value}\" ({name})")))
}

/// `copyStandardHeadersFromFrameToMessage`: the known headers set the
/// message's fields; every other header becomes a string property.
pub fn frame_to_message(
    frame: &Frame,
    message: &mut CoreMessage,
    prefix: Option<&str>,
    now: i64,
) -> Result<(), BadHeader> {
    let mut rest: Vec<(&str, &str)> = frame
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut take = |name: &str| {
        rest.iter()
            .position(|(k, _)| *k == name)
            .map(|i| rest.remove(i).1)
    };
    let priority = match take(names::PRIORITY) {
        Some(p) => java_parse::<i8>(names::PRIORITY, p)?,
        None => DEFAULT_PRIORITY,
    };
    message.priority = priority as u8;
    if let Some(p) = take(names::PERSISTENT) {
        message.durable = p.eq_ignore_ascii_case("true");
    }
    if let Some(v) = take(names::CORRELATION_ID) {
        put_string(message, JMS_CORRELATION_ID, v);
    }
    if let Some(v) = take(names::TYPE) {
        put_string(message, JMS_TYPE, v);
    }
    if let Some(v) = take(JMSX_GROUP_ID) {
        put_string(message, m::HDR_GROUP_ID, v);
    }
    if let Some(v) = take(names::CONTENT_TYPE) {
        put_string(message, HDR_CONTENT_TYPE, v);
    }
    if let Some(v) = take(names::REPLY_TO) {
        put_string(message, JMS_REPLY_TO, v);
    }
    if let Some(v) = take(names::EXPIRES) {
        message.expiration = java_parse(names::EXPIRES, v)?;
    }
    if let Some(v) = take(names::AMQ_SCHEDULED_DELAY) {
        let delay: i64 = java_parse(names::AMQ_SCHEDULED_DELAY, v)?;
        if delay > 0 {
            message.properties.insert(
                m::HDR_SCHEDULED_DELIVERY_TIME.into(),
                PropertyValue::Long(now + delay),
            );
        }
    }
    if let Some(v) = take(names::AMQ_SCHEDULED_TIME) {
        let time: i64 = java_parse(names::AMQ_SCHEDULED_TIME, v)?;
        if time > 0 {
            message.properties.insert(
                m::HDR_SCHEDULED_DELIVERY_TIME.into(),
                PropertyValue::Long(time),
            );
        }
    }
    if let Some(prefix) = prefix {
        put_string(message, HDR_PREFIX, prefix);
    }
    for (name, value) in rest {
        put_string(message, name, value);
    }
    Ok(())
}

/// The frame's body as the message's: bytes when it has a
/// `content-length`, text otherwise.
pub fn set_body(frame: &Frame, message: &mut CoreMessage) {
    if frame.has_header(names::CONTENT_LENGTH) {
        message.message_type = message_type::BYTES;
        message.body = frame.body.clone().into();
    } else {
        message.message_type = message_type::TEXT;
        let text = SimpleString::from(frame.body_text());
        let mut w = Writer::new();
        w.nullable_simple_string(Some(&text));
        message.body = w.into_vec().into();
    }
}

/// `Object.toString` of a property value.
pub fn property_text(value: &PropertyValue) -> String {
    match value {
        PropertyValue::Null => String::new(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Byte(v) => v.to_string(),
        PropertyValue::Short(v) => v.to_string(),
        PropertyValue::Int(v) => v.to_string(),
        PropertyValue::Long(v) => v.to_string(),
        PropertyValue::Float(v) => java::float(*v),
        PropertyValue::Double(v) => java::double(*v),
        PropertyValue::Char(c) => java::utf16([*c]),
        PropertyValue::String(s) => java::simple_string(s),
        // `byte[].toString()`: the type and an identity hash.
        PropertyValue::Bytes(b) => format!(
            "[B@{:x}",
            java::string_hash(&b.iter().map(|&x| u16::from(x)).collect::<Vec<_>>()) as u32
        ),
    }
}

fn property<'a>(message: &'a CoreMessage, name: &str) -> Option<&'a PropertyValue> {
    message.properties.get_str(name)
}

/// `getStringProperty`: any value as text, `None` for none.
fn string_property(message: &CoreMessage, name: &str) -> Option<String> {
    match property(message, name)? {
        PropertyValue::Null => None,
        v => Some(property_text(v)),
    }
}

/// The properties in the order Artemis's `HashMap` iterates them.
fn properties_in_java_order(message: &CoreMessage) -> Vec<&(SimpleString, PropertyValue)> {
    let entries = message.properties.entries();
    let hashes: Vec<i32> = entries
        .iter()
        .map(|(k, _)| java::simple_string_hash(k))
        .collect();
    java::hash_map_order(&hashes, None)
        .into_iter()
        .map(|i| &entries[i])
        .collect()
}

/// The properties a MESSAGE frame does not repeat as headers.
const NOT_HEADERS: [&str; 8] = [
    JMS_REPLY_TO,
    HDR_CONTENT_TYPE,
    m::HDR_VALIDATED_USER,
    m::HDR_ROUTING_TYPE,
    HDR_PREFIX,
    JMS_TYPE,
    JMS_CORRELATION_ID,
    names::DESTINATION,
];

/// `createMessageFrame` with `populateFrameBodyFromMessage` and
/// `copyStandardHeadersFromMessageToFrame`. Fails when a text body does
/// not decode.
pub fn message_frame(
    kind: Kind,
    subscription: &str,
    ack_header: bool,
    consumer_id: i64,
    message: &CoreMessage,
    delivery_count: i32,
) -> Result<Frame, String> {
    let mut frame = Frame::new(names::MESSAGE, kind);
    frame.add_header(names::SUBSCRIPTION, subscription);
    if !message.body.is_empty() {
        if property(message, names::CONTENT_LENGTH).is_some()
            || message.message_type == message_type::BYTES
        {
            frame.add_header(names::CONTENT_LENGTH, &message.body.len().to_string());
            frame.set_body(message.body.clone());
        } else {
            let mut r = Reader::new(&message.body);
            let text = r
                .nullable_simple_string()
                .map_err(|e| format!("the body is not text: {e}"))?;
            if let Some(text) = text {
                frame.set_body(java::simple_string(&text).into_bytes());
            }
        }
    }
    let message_id = format!("{consumer_id},{}", message.message_id);
    frame.add_header(names::MESSAGE_ID, &message_id);
    // copyStandardHeadersFromMessageToFrame
    let prefix = string_property(message, HDR_PREFIX).unwrap_or_default();
    let address = message
        .address
        .as_ref()
        .map(java::simple_string)
        .unwrap_or_else(|| "null".to_owned());
    frame.add_header(names::DESTINATION, &format!("{prefix}{address}"));
    if let Some(v) = property(message, JMS_CORRELATION_ID).filter(|v| **v != PropertyValue::Null) {
        frame.add_header(names::CORRELATION_ID, &property_text(v));
    }
    frame.add_header(names::EXPIRES, &message.expiration.to_string());
    frame.add_header(names::REDELIVERED, &(delivery_count > 1).to_string());
    frame.add_header(names::PRIORITY, &(message.priority as i8).to_string());
    frame.add_header(names::PERSISTENT, &message.durable.to_string());
    if let Some(v) = string_property(message, JMS_REPLY_TO) {
        frame.add_header(names::REPLY_TO, &v);
    }
    frame.add_header(names::TIMESTAMP, &message.timestamp.to_string());
    if let Some(v) = property(message, JMS_TYPE).filter(|v| **v != PropertyValue::Null) {
        frame.add_header(names::TYPE, &property_text(v));
    }
    if let Some(v) = string_property(message, HDR_CONTENT_TYPE) {
        frame.add_header(names::CONTENT_TYPE, &v);
    }
    if let Some(v) = string_property(message, m::HDR_VALIDATED_USER) {
        frame.add_header(names::VALIDATED_USER, &v);
    }
    if let Some(v) = property(message, m::HDR_ROUTING_TYPE) {
        let t = match v {
            PropertyValue::Byte(b) => RoutingType::from_byte(*b).map(RoutingType::name),
            _ => None,
        };
        frame.add_header(names::DESTINATION_TYPE, t.unwrap_or("null"));
    }
    if let Some(v) = property(message, HDR_INGRESS_TIMESTAMP) {
        frame.add_header(names::INGRESS_TIMESTAMP, &property_text(v));
    }
    for (name, value) in properties_in_java_order(message) {
        let name = java::simple_string(name);
        if NOT_HEADERS.contains(&name.as_str()) {
            continue;
        }
        frame.add_header(&name, &property_text(value));
    }
    if ack_header {
        frame.add_header(names::ACK_HEADER, &message_id);
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::{Rules, decode};

    fn frame(bytes: &[u8]) -> Frame {
        let (d, _) = decode(Rules::V12, bytes).unwrap().unwrap();
        Frame::decoded(d.command, Kind::V12, d.headers, d.body)
    }

    #[test]
    fn send_headers_become_message_fields() {
        let f = frame(
            b"SEND\ndestination:q\npriority:7\npersistent:TRUE\ncorrelation-id:c1\nexpires:99\nfoo:bar\nreceipt:r\n\nhello\0",
        );
        let mut msg = CoreMessage::default();
        frame_to_message(&f, &mut msg, None, 1000).unwrap();
        set_body(&f, &mut msg);
        assert_eq!(msg.priority, 7);
        assert!(msg.durable);
        assert_eq!(msg.expiration, 99);
        assert_eq!(msg.message_type, message_type::TEXT);
        let names: Vec<String> = msg
            .properties
            .entries()
            .iter()
            .map(|(k, _)| k.to_string_lossy())
            .collect();
        assert_eq!(names, ["JMSCorrelationID", "destination", "foo", "receipt"]);
        // The text body as a nullable SimpleString.
        let mut r = Reader::new(&msg.body);
        assert_eq!(
            r.nullable_simple_string()
                .unwrap()
                .unwrap()
                .to_string_lossy(),
            "hello"
        );
        let bad = frame(b"SEND\npriority:x\n\n\0");
        assert!(frame_to_message(&bad, &mut CoreMessage::default(), None, 0).is_err());
    }

    #[test]
    fn message_frame_headers() {
        let f = frame(b"SEND\ndestination:q\ncontent-length:3\nk:v\n\nabc\0");
        let mut msg = CoreMessage {
            message_id: 5,
            address: Some("q".into()),
            durable: true,
            timestamp: 7,
            ..Default::default()
        };
        frame_to_message(&f, &mut msg, None, 0).unwrap();
        set_body(&f, &mut msg);
        let out = message_frame(Kind::V12, "sub-0", true, 3, &msg, 2).unwrap();
        let text = String::from_utf8(out.encode()).unwrap();
        assert!(text.starts_with(
            "MESSAGE\nsubscription:sub-0\ncontent-length:3\nmessage-id:3,5\ndestination:q\nexpires:0\nredelivered:true\npriority:4\npersistent:true\ntimestamp:7\n"
        ), "{text}");
        assert!(text.contains("\nk:v\n"));
        assert!(text.ends_with("ack:3,5\n\nabc\0\n"), "{text}");
    }
}
