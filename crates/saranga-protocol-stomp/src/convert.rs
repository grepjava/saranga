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
/// `content-length`, text otherwise. The frame keeps no body.
pub fn set_body(frame: &mut Frame, message: &mut CoreMessage) {
    if frame.has_header(names::CONTENT_LENGTH) {
        message.message_type = message_type::BYTES;
        message.body = std::mem::take(&mut frame.body).into();
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

/// The headers [`message_frame`] writes besides the properties, but for
/// `destination`, which no property is written as, and `content-length`,
/// which [`message_bytes`] replaces as a 1.0 `Frame` does.
const STANDARD_HEADERS: [&str; 15] = [
    names::SUBSCRIPTION,
    names::MESSAGE_ID,
    names::CORRELATION_ID,
    names::EXPIRES,
    names::REDELIVERED,
    names::PRIORITY,
    names::PERSISTENT,
    names::REPLY_TO,
    names::TIMESTAMP,
    names::TYPE,
    names::CONTENT_TYPE,
    names::VALIDATED_USER,
    names::DESTINATION_TYPE,
    names::INGRESS_TIMESTAMP,
    names::ACK_HEADER,
];

/// `message_frame(..).encode()`, written straight to bytes rather than
/// through a [`Frame`], which holds each header as two `String`s. A
/// property named as a header already written is written again in 1.1 and
/// 1.2 ([`Frame::add_header`]), but replaces it in 1.0, which only a
/// `Frame` does: such a 1.0 message is made that way.
pub fn message_bytes(
    kind: Kind,
    subscription: &str,
    ack_header: bool,
    consumer_id: i64,
    message: &CoreMessage,
    delivery_count: i32,
) -> Result<Vec<u8>, String> {
    let properties = properties_in_java_order(message);
    if !matches!(kind, Kind::V11 | Kind::V12)
        && properties
            .iter()
            .any(|(k, _)| STANDARD_HEADERS.iter().any(|h| k.eq_str(h)))
    {
        return message_frame(
            kind,
            subscription,
            ack_header,
            consumer_id,
            message,
            delivery_count,
        )
        .map(|f| f.encode());
    }
    let mut text = None;
    let bytes_body = !message.body.is_empty()
        && (property(message, names::CONTENT_LENGTH).is_some()
            || message.message_type == message_type::BYTES);
    if !message.body.is_empty() && !bytes_body {
        let mut r = Reader::new(&message.body);
        let t = r
            .nullable_simple_string()
            .map_err(|e| format!("the body is not text: {e}"))?;
        text = t.map(|t| java::simple_string(&t).into_bytes());
    }
    let body: &[u8] = match &text {
        Some(t) => t,
        None if bytes_body => &message.body,
        None => &[],
    };
    let mut out = Vec::with_capacity(320 + body.len() + 32 * properties.len());
    let mut h = Headers {
        out: &mut out,
        escaped: matches!(kind, Kind::V11 | Kind::V12),
    };
    h.out.extend_from_slice(names::MESSAGE.as_bytes());
    h.out.push(b'\n');
    h.str(names::SUBSCRIPTION, subscription);
    // A `content-length` property (a SEND's header) is that header again,
    // which a 1.0 frame writes in place of the first and the others not.
    let length_property = property(message, names::CONTENT_LENGTH);
    if bytes_body {
        match length_property {
            Some(v) if kind == Kind::V10 => h.property(names::CONTENT_LENGTH, v),
            _ => h.int(names::CONTENT_LENGTH, body.len() as i64),
        }
    }
    h.message_id(names::MESSAGE_ID, consumer_id, message.message_id);
    // copyStandardHeadersFromMessageToFrame
    h.key(names::DESTINATION);
    if let Some(prefix) = property(message, HDR_PREFIX).filter(|v| **v != PropertyValue::Null) {
        h.value(prefix);
    }
    match &message.address {
        Some(a) => h.simple(a),
        None => h.out.extend_from_slice(b"null"),
    }
    h.out.push(b'\n');
    if let Some(v) = property(message, JMS_CORRELATION_ID).filter(|v| **v != PropertyValue::Null) {
        h.property(names::CORRELATION_ID, v);
    }
    h.int(names::EXPIRES, message.expiration);
    h.bool(names::REDELIVERED, delivery_count > 1);
    h.int(names::PRIORITY, (message.priority as i8).into());
    h.bool(names::PERSISTENT, message.durable);
    if let Some(v) = property(message, JMS_REPLY_TO).filter(|v| **v != PropertyValue::Null) {
        h.property(names::REPLY_TO, v);
    }
    h.int(names::TIMESTAMP, message.timestamp);
    if let Some(v) = property(message, JMS_TYPE).filter(|v| **v != PropertyValue::Null) {
        h.property(names::TYPE, v);
    }
    if let Some(v) = property(message, HDR_CONTENT_TYPE).filter(|v| **v != PropertyValue::Null) {
        h.property(names::CONTENT_TYPE, v);
    }
    if let Some(v) = property(message, m::HDR_VALIDATED_USER).filter(|v| **v != PropertyValue::Null)
    {
        h.property(names::VALIDATED_USER, v);
    }
    if let Some(v) = property(message, m::HDR_ROUTING_TYPE) {
        let t = match v {
            PropertyValue::Byte(b) => RoutingType::from_byte(*b).map(RoutingType::name),
            _ => None,
        };
        h.str(names::DESTINATION_TYPE, t.unwrap_or("null"));
    }
    if let Some(v) = property(message, HDR_INGRESS_TIMESTAMP) {
        h.property(names::INGRESS_TIMESTAMP, v);
    }
    for (name, value) in properties {
        if NOT_HEADERS.iter().any(|n| name.eq_str(n))
            || (bytes_body && name.eq_str(names::CONTENT_LENGTH))
        {
            continue;
        }
        h.simple(name);
        h.out.push(b':');
        h.value(value);
        h.out.push(b'\n');
    }
    if ack_header {
        h.message_id(names::ACK_HEADER, consumer_id, message.message_id);
    }
    if !body.is_empty() && !bytes_body && kind != Kind::V10 {
        h.int(names::CONTENT_LENGTH, body.len() as i64);
    }
    out.push(b'\n');
    out.extend_from_slice(body);
    out.extend_from_slice(&[0, b'\n']);
    Ok(out)
}

/// `Long.toString`, without the formatting machinery.
fn push_int(out: &mut Vec<u8>, v: i64) {
    let mut digits = [0u8; 20];
    let mut i = digits.len();
    let mut n = v.unsigned_abs();
    loop {
        i -= 1;
        digits[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if v < 0 {
        out.push(b'-');
    }
    out.extend_from_slice(&digits[i..]);
}

fn push_bool(out: &mut Vec<u8>, v: bool) {
    out.extend_from_slice(if v { b"true" } else { b"false" });
}

/// Headers written as [`Frame::encode`] writes them.
struct Headers<'a> {
    out: &'a mut Vec<u8>,
    escaped: bool,
}

impl Headers<'_> {
    fn key(&mut self, key: &str) {
        self.text(key);
        self.out.push(b':');
    }

    fn text(&mut self, s: &str) {
        if self.escaped {
            crate::frame::escape_into(self.out, s);
        } else {
            self.out.extend_from_slice(s.as_bytes());
        }
    }

    fn str(&mut self, key: &str, value: &str) {
        self.key(key);
        self.text(value);
        self.out.push(b'\n');
    }

    fn int(&mut self, key: &str, value: i64) {
        self.key(key);
        push_int(self.out, value);
        self.out.push(b'\n');
    }

    fn bool(&mut self, key: &str, value: bool) {
        self.key(key);
        push_bool(self.out, value);
        self.out.push(b'\n');
    }

    /// A `message-id`: the consumer's ID and the message's.
    fn message_id(&mut self, key: &str, consumer_id: i64, message_id: i64) {
        self.key(key);
        push_int(self.out, consumer_id);
        self.out.push(b',');
        push_int(self.out, message_id);
        self.out.push(b'\n');
    }

    fn property(&mut self, key: &str, value: &PropertyValue) {
        self.key(key);
        self.value(value);
        self.out.push(b'\n');
    }

    /// [`property_text`], without a `String` for the common kinds.
    fn value(&mut self, value: &PropertyValue) {
        match value {
            PropertyValue::String(s) => self.simple(s),
            PropertyValue::Bool(v) => push_bool(self.out, *v),
            PropertyValue::Byte(v) => push_int(self.out, (*v).into()),
            PropertyValue::Short(v) => push_int(self.out, (*v).into()),
            PropertyValue::Int(v) => push_int(self.out, (*v).into()),
            PropertyValue::Long(v) => push_int(self.out, *v),
            v => self.text(&property_text(v)),
        }
    }

    /// [`java::simple_string`]: ASCII is copied unit by unit.
    fn simple(&mut self, s: &SimpleString) {
        let units = s.as_bytes().as_chunks::<2>().0;
        if units.iter().all(|&[lo, hi]| hi == 0 && lo < 0x80) {
            for &[b, _] in units {
                match (self.escaped, b) {
                    (true, b'\n') => self.out.extend_from_slice(b"\\n"),
                    (true, b'\r') => self.out.extend_from_slice(b"\\r"),
                    (true, b'\\') => self.out.extend_from_slice(b"\\\\"),
                    (true, b':') => self.out.extend_from_slice(b"\\c"),
                    _ => self.out.push(b),
                }
            }
        } else {
            self.text(&java::simple_string(s));
        }
    }
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
        let mut f = frame(
            b"SEND\ndestination:q\npriority:7\npersistent:TRUE\ncorrelation-id:c1\nexpires:99\nfoo:bar\nreceipt:r\n\nhello\0",
        );
        let mut msg = CoreMessage::default();
        frame_to_message(&f, &mut msg, None, 1000).unwrap();
        set_body(&mut f, &mut msg);
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
        let mut f = frame(b"SEND\ndestination:q\ncontent-length:3\nk:v\n\nabc\0");
        let mut msg = CoreMessage {
            message_id: 5,
            address: Some("q".into()),
            durable: true,
            timestamp: 7,
            ..Default::default()
        };
        frame_to_message(&f, &mut msg, None, 0).unwrap();
        set_body(&mut f, &mut msg);
        let out = message_frame(Kind::V12, "sub-0", true, 3, &msg, 2).unwrap();
        let text = String::from_utf8(out.encode()).unwrap();
        assert!(text.starts_with(
            "MESSAGE\nsubscription:sub-0\ncontent-length:3\nmessage-id:3,5\ndestination:q\nexpires:0\nredelivered:true\npriority:4\npersistent:true\ntimestamp:7\n"
        ), "{text}");
        assert!(text.contains("\nk:v\n"));
        assert!(text.ends_with("ack:3,5\n\nabc\0\n"), "{text}");
    }

    #[test]
    fn message_bytes_is_message_frame_encoded() {
        use PropertyValue as V;
        let text = |s: &str| {
            let mut w = Writer::new();
            w.nullable_simple_string(Some(&SimpleString::from(s)));
            w.into_vec()
        };
        let props: Vec<Vec<(&str, V)>> = vec![
            vec![],
            vec![("k", V::String("a:b\nc\\d\re".into()))],
            vec![
                ("h\u{e9}llo", V::String("\u{65e5}\u{672c}".into())),
                ("x:y", V::Int(-3)),
            ],
            vec![
                (JMS_CORRELATION_ID, V::String("c1".into())),
                (JMS_TYPE, V::Long(9)),
                (JMS_REPLY_TO, V::String("r".into())),
                (HDR_CONTENT_TYPE, V::String("text/plain".into())),
                (m::HDR_VALIDATED_USER, V::String("u".into())),
                (m::HDR_ROUTING_TYPE, V::Byte(1)),
                (HDR_INGRESS_TIMESTAMP, V::Long(12)),
                (HDR_PREFIX, V::String("pre.".into())),
            ],
            vec![
                (JMS_CORRELATION_ID, V::Null),
                (JMS_TYPE, V::Null),
                (m::HDR_ROUTING_TYPE, V::Int(3)),
                (HDR_INGRESS_TIMESTAMP, V::Null),
                ("b", V::Bool(true)),
                ("y", V::Byte(-1)),
                ("s", V::Short(7)),
                ("f", V::Float(1.5)),
                ("d", V::Double(0.1)),
                ("c", V::Char(0x41)),
                ("n", V::Null),
                ("bytes", V::Bytes(vec![1, 2])),
            ],
            vec![("expires", V::String("x".into())), ("ack", V::Int(1))],
            vec![("subscription", V::Null), ("content-length", V::Int(2))],
            vec![
                ("content-length", V::String("3".into())),
                ("destination", V::Int(1)),
            ],
            vec![
                ("content-length", V::Null),
                ("receipt", V::String("r".into())),
            ],
            vec![
                ("destination-type", V::String("ANYCAST".into())),
                (m::HDR_ROUTING_TYPE, V::Byte(1)),
                ("content-length", V::String("1024".into())),
            ],
        ];
        let bodies: Vec<(u8, Vec<u8>)> = vec![
            (message_type::BYTES, b"abc".to_vec()),
            (message_type::BYTES, vec![]),
            (message_type::TEXT, text("hi:\n")),
            (message_type::TEXT, text("")),
            (message_type::TEXT, vec![0]),
        ];
        for p in &props {
            for (t, body) in &bodies {
                for address in [None, Some("q:1")] {
                    let mut msg = CoreMessage {
                        message_id: 5,
                        address: address.map(Into::into),
                        durable: true,
                        timestamp: 7,
                        expiration: 99,
                        priority: 200,
                        message_type: *t,
                        body: body.clone().into(),
                        ..Default::default()
                    };
                    for (k, v) in p {
                        msg.properties.insert((*k).into(), v.clone());
                    }
                    for kind in [Kind::V10, Kind::V11, Kind::V12] {
                        for (ack, count) in [(false, 1), (true, 2)] {
                            let frame =
                                message_frame(kind, "s:1", ack, 3, &msg, count).map(|f| f.encode());
                            let bytes = message_bytes(kind, "s:1", ack, 3, &msg, count);
                            assert_eq!(frame, bytes, "{kind:?} {p:?} {t} {address:?}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn push_int_is_to_string() {
        for v in [0, 7, -1, 10, -10, 1_700_000_000_000, i64::MAX, i64::MIN] {
            let mut out = Vec::new();
            push_int(&mut out, v);
            assert_eq!(out, v.to_string().into_bytes());
        }
    }
}
