//! Building and reading messages the way the Artemis JMS client does.
//! Ported from `ClientSessionImpl.createMessage`, `ClientMessageImpl`,
//! `ActiveMQTextMessage` / `TextMessageUtil`, `ActiveMQBytesMessage` and the
//! header properties of `MessageUtil` and `ActiveMQMessageProducer`.
//!
//! Body layouts (checked against the `core-basic` golden capture):
//! - TEXT (type 3): a nullable `SimpleString`, i.e. `0x01`, an i32 byte
//!   length and UTF-16LE text, or a single `0x00` for a null text.
//! - BYTES (type 4): the raw bytes.

use std::time::{SystemTime, UNIX_EPOCH};

use saranga_buffer::{PropertyValue, Reader, SimpleString, Writer};
use saranga_message::{CoreMessage, message_type};

use crate::session::RoutingType;
use crate::uuid;

/// `MessageUtil.CORRELATIONID_HEADER_NAME`.
pub const JMS_CORRELATION_ID: &str = "JMSCorrelationID";
/// `MessageUtil.TYPE_HEADER_NAME`.
pub const JMS_TYPE: &str = "JMSType";
/// `MessageUtil.REPLYTO_HEADER_NAME`.
pub const JMS_REPLY_TO: &str = "JMSReplyTo";
/// `ActiveMQConnection.CONNECTION_ID_PROPERTY_NAME`, set on every JMS send.
pub const CONNECTION_ID: &str = "__AMQ_CID";
/// `Message.HDR_ROUTING_TYPE`: the JMS producer sets ANYCAST for queues and
/// MULTICAST for topics.
pub const HDR_ROUTING_TYPE: &str = "_AMQ_ROUTING_TYPE";
/// `Message.HDR_DUPLICATE_DETECTION_ID`.
pub const HDR_DUPLICATE_DETECTION_ID: &str = "_AMQ_DUPL_ID";
/// `Message.HDR_SCHEDULED_DELIVERY_TIME`.
pub const HDR_SCHEDULED_DELIVERY_TIME: &str = "_AMQ_SCHED_DELIVERY";
/// `Message.HDR_GROUP_ID`.
pub const HDR_GROUP_ID: &str = "_AMQ_GROUP_ID";
/// `Message.HDR_LARGE_COMPRESSED`: the body is zlib-compressed.
pub const HDR_LARGE_COMPRESSED: &str = "_AMQ_LARGE_COMPRESSED";
/// `Message.HDR_LARGE_BODY_SIZE`: the (uncompressed) size of a large body.
pub const HDR_LARGE_BODY_SIZE: &str = "_AMQ_LARGE_SIZE";

/// The default JMS and Core priority.
pub const DEFAULT_PRIORITY: u8 = 4;

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// `CoreMessage.getHeadersAndPropertiesEncodeSize`.
pub(crate) fn headers_and_properties_encode_size(message: &CoreMessage) -> i32 {
    let mut w = Writer::new();
    message.encode_headers_and_properties(&mut w);
    w.len() as i32
}

/// `ClientSession.createMessage(type, durable)`: timestamp now, priority 4,
/// no expiration, empty body.
pub fn new_message(message_type: u8, durable: bool) -> CoreMessage {
    CoreMessage {
        message_type,
        durable,
        timestamp: now_millis(),
        priority: DEFAULT_PRIORITY,
        ..CoreMessage::default()
    }
}

/// A durable TEXT message, as `Session.createTextMessage(text)` makes it.
pub fn text_message(text: &str) -> CoreMessage {
    let mut m = new_message(message_type::TEXT, true);
    m.set_text(Some(text));
    m
}

/// A durable BYTES message.
pub fn bytes_message(bytes: &[u8]) -> CoreMessage {
    let mut m = new_message(message_type::BYTES, true);
    m.body = bytes.to_vec().into();
    m
}

/// Accessors for message headers, properties and bodies.
pub trait MessageExt {
    /// Writes a TEXT body (`TextMessageUtil.writeBodyText`).
    fn set_text(&mut self, text: Option<&str>) -> &mut Self;
    /// Reads a TEXT body (`TextMessageUtil.readBodyText`).
    fn text(&self) -> saranga_buffer::Result<Option<String>>;
    fn set_durable(&mut self, durable: bool) -> &mut Self;
    fn set_priority(&mut self, priority: u8) -> &mut Self;
    /// Absolute expiration time in epoch milliseconds, 0 for none.
    fn set_expiration(&mut self, expiration: i64) -> &mut Self;
    fn set_timestamp(&mut self, timestamp: i64) -> &mut Self;
    fn set_user_id(&mut self, user_id: Option<[u8; 16]>) -> &mut Self;
    /// Sets a new time-based user ID, as the JMS producer does for the
    /// message ID.
    fn set_new_user_id(&mut self) -> &mut Self;
    /// The JMS message ID: `ID:` and the user ID as a UUID.
    fn jms_message_id(&self) -> Option<String>;
    fn put_property(&mut self, key: &str, value: PropertyValue) -> &mut Self;
    fn put_string_property(&mut self, key: &str, value: &str) -> &mut Self;
    fn property(&self, key: &str) -> Option<&PropertyValue>;
    fn string_property(&self, key: &str) -> Option<String>;
    fn remove_property(&mut self, key: &str) -> Option<PropertyValue>;
    /// `Message.setRoutingType`: the `_AMQ_ROUTING_TYPE` byte property.
    fn set_routing_type(&mut self, routing_type: RoutingType) -> &mut Self;
    /// `Message.isExpired`.
    fn is_expired(&self) -> bool;
}

impl MessageExt for CoreMessage {
    fn set_text(&mut self, text: Option<&str>) -> &mut Self {
        let mut w = Writer::new();
        w.nullable_simple_string(text.map(SimpleString::from).as_ref());
        self.body = w.into_vec().into();
        self
    }

    fn text(&self) -> saranga_buffer::Result<Option<String>> {
        if self.body.is_empty() {
            return Ok(None);
        }
        let mut r = Reader::new(&self.body);
        Ok(r.nullable_simple_string()?.map(|s| s.to_string_lossy()))
    }

    fn set_durable(&mut self, durable: bool) -> &mut Self {
        self.durable = durable;
        self
    }

    fn set_priority(&mut self, priority: u8) -> &mut Self {
        self.priority = priority;
        self
    }

    fn set_expiration(&mut self, expiration: i64) -> &mut Self {
        self.expiration = expiration;
        self
    }

    fn set_timestamp(&mut self, timestamp: i64) -> &mut Self {
        self.timestamp = timestamp;
        self
    }

    fn set_user_id(&mut self, user_id: Option<[u8; 16]>) -> &mut Self {
        self.user_id = user_id;
        self
    }

    fn set_new_user_id(&mut self) -> &mut Self {
        self.user_id = Some(uuid::time_based());
        self
    }

    fn jms_message_id(&self) -> Option<String> {
        self.user_id
            .as_ref()
            .map(|id| format!("ID:{}", uuid::to_string(id)))
    }

    fn put_property(&mut self, key: &str, value: PropertyValue) -> &mut Self {
        self.properties.insert(key.into(), value);
        self
    }

    fn put_string_property(&mut self, key: &str, value: &str) -> &mut Self {
        self.put_property(key, PropertyValue::String(value.into()))
    }

    fn property(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(&key.into())
    }

    fn string_property(&self, key: &str) -> Option<String> {
        match self.property(key)? {
            PropertyValue::String(s) => Some(s.to_string_lossy()),
            _ => None,
        }
    }

    fn remove_property(&mut self, key: &str) -> Option<PropertyValue> {
        self.properties.remove(&key.into())
    }

    fn set_routing_type(&mut self, routing_type: RoutingType) -> &mut Self {
        self.put_property(HDR_ROUTING_TYPE, PropertyValue::Byte(routing_type.wire()))
    }

    fn is_expired(&self) -> bool {
        self.expiration != 0 && now_millis() > self.expiration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_body_matches_golden() {
        // core-basic: the first TEXT message ("hello saranga").
        let golden = [
            1, 0, 0, 0, 26, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 115, 0, 97, 0, 114, 0,
            97, 0, 110, 0, 103, 0, 97, 0,
        ];
        let m = text_message("hello saranga");
        assert_eq!(m.body[..], golden);
        assert_eq!(m.message_type, 3);
        assert!(m.durable);
        assert_eq!(m.priority, 4);
        assert_eq!(m.text().unwrap().as_deref(), Some("hello saranga"));

        let mut n = new_message(message_type::TEXT, true);
        n.set_text(None);
        assert_eq!(n.body[..], [0]);
        assert_eq!(n.text().unwrap(), None);
    }

    #[test]
    fn bytes_body_is_raw() {
        let m = bytes_message(&[0, 1, 2, 3, 255]);
        assert_eq!(m.message_type, 4);
        assert_eq!(m.body[..], [0, 1, 2, 3, 255]);
    }

    #[test]
    fn properties_and_headers() {
        let mut m = text_message("x");
        m.put_string_property(JMS_CORRELATION_ID, "corr-1")
            .put_property("pInt", PropertyValue::Int(-5))
            .set_routing_type(RoutingType::Anycast)
            .set_priority(9)
            .set_expiration(1)
            .set_new_user_id();
        assert_eq!(
            m.string_property(JMS_CORRELATION_ID).as_deref(),
            Some("corr-1")
        );
        assert_eq!(m.property("pInt"), Some(&PropertyValue::Int(-5)));
        assert_eq!(m.property(HDR_ROUTING_TYPE), Some(&PropertyValue::Byte(1)));
        assert!(m.is_expired());
        assert!(m.jms_message_id().unwrap().starts_with("ID:"));
        assert_eq!(m.remove_property("pInt"), Some(PropertyValue::Int(-5)));
        assert_eq!(m.property("pInt"), None);
    }
}
