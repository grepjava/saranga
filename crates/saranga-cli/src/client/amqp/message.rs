//! Qpid JMS's messages over AMQP (`AmqpJmsMessageFacade` and its text,
//! bytes, map, object and stream kinds, `AmqpCodec`): what a JMS message
//! sends as, and what a received one reads as.

use saranga_buffer::java_hash::{hash_map_order, string_hash};
use saranga_protocol_amqp::codec::{self, Decoder};
use saranga_protocol_amqp::composite::{Composite, Timestamp};
use saranga_protocol_amqp::message::section_code;
use saranga_protocol_amqp::messaging::{Header, Properties, section};
use saranga_protocol_amqp::types::{Descriptor, Symbol, Value};

use crate::client::jms::Destination;

/// `x-opt-jms-msg-type` values (`JmsMessageFacade`).
pub mod kind {
    pub const MESSAGE: u8 = 0;
    pub const OBJECT: u8 = 1;
    pub const MAP: u8 = 2;
    pub const BYTES: u8 = 3;
    pub const STREAM: u8 = 4;
    pub const TEXT: u8 = 5;
}

pub const JMS_MSG_TYPE: &str = "x-opt-jms-msg-type";
pub const JMS_DEST: &str = "x-opt-jms-dest";
pub const JMS_REPLY_TO: &str = "x-opt-jms-reply-to";
const LEGACY_REPLY_TO_TYPE: &str = "x-opt-reply-type";
pub const DELIVERY_TIME: &str = "x-opt-delivery-time";
const TO_TYPE: &str = "x-opt-to-type";
pub const SERIALIZED_JAVA_OBJECT: &str = "application/x-java-serialized-object";
pub const OCTET_STREAM: &str = "application/octet-stream";
/// `NULL_OBJECT_BODY`: a serialized `null`.
const NULL_OBJECT: [u8; 5] = [0xac, 0xed, 0x00, 0x05, 0x70];

/// A message's body section.
#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    Value(Value),
    Data(Vec<u8>),
    Sequence(Vec<Value>),
}

/// A JMS message over AMQP, its sections as the facade keeps them.
#[derive(Debug, Clone)]
pub struct AmqpJmsMessage {
    /// Which JMS message type it is.
    pub kind: u8,
    durable: bool,
    priority: Option<u8>,
    ttl: Option<u32>,
    /// `JMS_AMQP_TTL`: the header's ttl whatever the producer's.
    ttl_override: Option<u32>,
    /// An `ObjectMessage` with the AMQP-typed delegate.
    typed_object: bool,
    first_acquirer: bool,
    delivery_count: u32,
    pub delivery_annotations: Option<Value>,
    /// In the order the map iterates them.
    pub annotations: Vec<(Symbol, Value)>,
    pub properties: Option<Properties>,
    /// In the order the map iterates them.
    pub application_properties: Vec<(String, Value)>,
    pub body: Option<Body>,
    pub footer: Option<Value>,
    /// Where a received message was consumed from.
    pub consumer_destination: Option<Destination>,
    /// Receipt time plus the header's ttl, without an expiry time.
    synthetic_expiration: Option<i64>,
    /// The destination a send set (`setJMSDestination`).
    sent_to: Option<Destination>,
    /// The type byte of a reply-to `setJMSReplyTo` set: queue 0, topic 1,
    /// temporary queue 2, temporary topic 3.
    reply_to_type: Option<i8>,
    /// `System.identityHashCode`, for `toString`.
    identity: u32,
}

/// `Integer.toHexString(System.identityHashCode(o))` stands in for an
/// object's identity: a random 31-bit number, as HotSpot draws it.
pub(crate) fn identity() -> u32 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0x9e37_79b9_7f4a_7c15, |d| d.as_nanos() as u64)
            | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);
    (x >> 33) as u32 & 0x7fff_ffff
}

impl AmqpJmsMessage {
    /// `facade.initialize`: persistent, and the type's empty body.
    fn new(kind: u8) -> Self {
        let body = match kind {
            kind::TEXT => Some(Body::Value(Value::Null)),
            kind::BYTES => Some(Body::Data(Vec::new())),
            kind::MAP => Some(Body::Value(Value::Map(Vec::new()))),
            kind::STREAM => Some(Body::Sequence(Vec::new())),
            _ => None,
        };
        let mut m = Self {
            kind,
            durable: true,
            priority: None,
            ttl: None,
            ttl_override: None,
            typed_object: false,
            first_acquirer: false,
            delivery_count: 0,
            delivery_annotations: None,
            annotations: Vec::new(),
            properties: None,
            application_properties: Vec::new(),
            body,
            footer: None,
            consumer_destination: None,
            synthetic_expiration: None,
            sent_to: None,
            reply_to_type: None,
            identity: identity(),
        };
        if kind == kind::OBJECT {
            // AmqpSerializedObjectDelegate.
            m.props().content_type = Some(Symbol::from(SERIALIZED_JAVA_OBJECT));
        }
        m
    }

    /// `createMessage()`.
    pub fn message() -> Self {
        Self::new(kind::MESSAGE)
    }

    /// `createTextMessage(text)`.
    pub fn new_text(text: Option<&str>) -> Self {
        let mut m = Self::new(kind::TEXT);
        if let Some(t) = text {
            m.body = Some(Body::Value(Value::String(t.to_owned())));
        }
        m
    }

    /// `createBytesMessage()` and `writeBytes`.
    pub fn bytes(data: &[u8]) -> Self {
        let mut m = Self::new(kind::BYTES);
        m.body = Some(Body::Data(data.to_vec()));
        m
    }

    /// `createMapMessage()` with these entries, in insertion order.
    pub fn map(entries: Vec<(Value, Value)>) -> Self {
        let mut m = Self::new(kind::MAP);
        m.body = Some(Body::Value(Value::Map(entries)));
        m
    }

    /// `createStreamMessage()` with these items.
    pub fn stream(items: Vec<Value>) -> Self {
        let mut m = Self::new(kind::STREAM);
        m.body = Some(Body::Sequence(items));
        m
    }

    /// `createObjectMessage(o)`: the Java serialization of the object.
    pub fn object(serialized: Option<Vec<u8>>) -> Self {
        let mut m = Self::new(kind::OBJECT);
        m.body = serialized.map(Body::Data);
        m
    }

    fn props(&mut self) -> &mut Properties {
        self.properties.get_or_insert_with(Properties::default)
    }

    /// `setApplicationProperty`: in a map's insertion order.
    pub fn set_application_property(&mut self, name: &str, value: Value) {
        match self
            .application_properties
            .iter_mut()
            .find(|(k, _)| k == name)
        {
            Some(entry) => entry.1 = value,
            None => self.application_properties.push((name.to_owned(), value)),
        }
    }

    /// The properties section, made only when a value needs it, as
    /// `lazyCreateProperties` makes it.
    fn props_for(&mut self, set: bool) -> Option<&mut Properties> {
        if set {
            Some(self.props())
        } else {
            self.properties.as_mut()
        }
    }

    /// `setType`: the subject.
    pub fn set_type(&mut self, value: Option<String>) {
        if let Some(p) = self.props_for(value.is_some()) {
            p.subject = value;
        }
    }

    /// `setGroupId`.
    pub fn set_group_id(&mut self, value: Option<String>) {
        if let Some(p) = self.props_for(value.is_some()) {
            p.group_id = value;
        }
    }

    /// `setGroupSequence`: a negative one wraps into the upper uint range.
    pub fn set_group_sequence(&mut self, value: i32) {
        if let Some(p) = self.props_for(value != 0) {
            p.group_sequence = (value != 0).then_some(value as u32);
        }
    }

    /// `setUserId`: the UTF-8 bytes.
    pub fn set_user_id(&mut self, value: Option<&str>) {
        if let Some(p) = self.props_for(value.is_some()) {
            p.user_id = value.map(|v| v.as_bytes().to_vec());
        }
    }

    /// `setReplyToGroupId`.
    pub fn set_reply_to_group_id(&mut self, value: Option<String>) {
        if let Some(p) = self.props_for(value.is_some()) {
            p.reply_to_group_id = value;
        }
    }

    /// `setCorrelationId`: a JMS ID converted to the AMQP type it names.
    pub fn set_correlation_id(&mut self, value: Option<&str>) -> Result<(), String> {
        let id = match value {
            Some(v) if v.starts_with("ID:") => Some(to_id_object(v)?),
            Some(v) => Some(Value::String(v.to_owned())),
            None => None,
        };
        if let Some(p) = self.props_for(id.is_some()) {
            p.correlation_id = id;
        }
        Ok(())
    }

    /// `setMessageId(String)`.
    pub fn set_message_id(&mut self, value: Option<&str>) -> Result<(), String> {
        let id = value.map(to_id_object).transpose()?;
        if let Some(p) = self.props_for(id.is_some()) {
            p.message_id = id;
        }
        Ok(())
    }

    /// `setTimestamp`: the creation time, none for 0.
    pub fn set_timestamp(&mut self, value: i64) {
        if let Some(p) = self.props_for(value != 0) {
            p.creation_time = (value != 0).then_some(Timestamp(value));
        }
    }

    /// `setExpiration`: the absolute expiry time, none for 0.
    pub fn set_expiration(&mut self, value: i64) {
        self.synthetic_expiration = None;
        if let Some(p) = self.props_for(value != 0) {
            p.absolute_expiry_time = (value != 0).then_some(Timestamp(value));
        }
    }

    /// `header.setPriority(int)`: 4 is no priority, others clamped to 0..9.
    pub fn set_priority(&mut self, priority: i32) {
        self.priority = (priority != 4).then(|| priority.clamp(0, 9) as u8);
    }

    pub fn set_persistent(&mut self, persistent: bool) {
        self.durable = persistent;
    }

    /// `setRedelivered`: a delivery count of 1, or none.
    pub fn set_redelivered(&mut self, redelivered: bool) {
        if redelivered {
            if !self.redelivered() {
                self.delivery_count = 1;
            }
        } else if self.redelivered() {
            self.delivery_count = 0;
        }
    }

    /// `setDeliveryCount`: `JMSXDeliveryCount` is one more than the
    /// header's count.
    pub fn set_delivery_count(&mut self, count: i32) {
        self.delivery_count = count.wrapping_sub(1) as u32;
    }

    /// `setDeliveryTime(time, true)`: the annotation, none for 0.
    pub fn set_delivery_time(&mut self, time: i64) {
        self.annotations
            .retain(|(k, _)| k.as_str() != DELIVERY_TIME);
        if time != 0 {
            self.annotations
                .push((Symbol::from(DELIVERY_TIME), Value::Long(time)));
        }
    }

    /// `setAmqpTimeToLiveOverride`.
    pub fn set_ttl_override(&mut self, ttl: Option<u32>) {
        self.ttl_override = ttl;
    }

    /// `setUseAmqpTypedEncoding`: the object kept, encoded the other way.
    /// The serialized objects this client makes are strings.
    pub fn set_typed_object(&mut self, typed: bool) -> Result<(), String> {
        if typed == self.typed_object {
            return Ok(());
        }
        let object = self.object_string()?;
        self.typed_object = typed;
        if typed {
            if let Some(p) = self.properties.as_mut() {
                p.content_type = None;
            }
            self.body = Some(Body::Value(object.map_or(Value::Null, Value::String)));
        } else {
            self.props().content_type = Some(Symbol::from(SERIALIZED_JAVA_OBJECT));
            self.body = object.map(|o| Body::Data(crate::client::jmsapi::serialized_string(&o)));
        }
        Ok(())
    }

    /// `ObjectMessage.getObject` for a string, or none.
    fn object_string(&self) -> Result<Option<String>, String> {
        match &self.body {
            None => Ok(None),
            Some(Body::Value(Value::Null)) => Ok(None),
            Some(Body::Value(Value::String(s))) => Ok(Some(s.clone())),
            Some(Body::Data(d)) if d.as_slice() == NULL_OBJECT => Ok(None),
            Some(Body::Data(d)) => crate::client::jmsapi::deserialized_string(d)
                .map(Some)
                .ok_or_else(|| "javax.jms.JMSException: the object is not a String".to_owned()),
            Some(_) => Err("javax.jms.JMSException: the object is not a String".to_owned()),
        }
    }

    /// `getJMSCorrelationID` (`AmqpMessageIdHelper.toCorrelationIdString`).
    pub fn correlation_id(&self) -> Option<String> {
        let id = self.properties.as_ref()?.correlation_id.as_ref()?;
        match id {
            Value::String(s) if !s.starts_with("ID:") => Some(s.clone()),
            other => id_string(other),
        }
    }

    /// `getJMSType`.
    pub fn jms_type(&self) -> Option<String> {
        self.properties.as_ref()?.subject.clone()
    }

    /// `getGroupId`, `getGroupSequence` and `getUserId`.
    pub fn group_id(&self) -> Option<String> {
        self.properties.as_ref()?.group_id.clone()
    }

    pub fn group_sequence(&self) -> i32 {
        self.properties
            .as_ref()
            .and_then(|p| p.group_sequence)
            .map_or(0, |s| s as i32)
    }

    pub fn user_id(&self) -> Option<String> {
        self.properties
            .as_ref()?
            .user_id
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }

    /// `getDeliveryTime`: the annotation's time, else 0.
    pub fn delivery_time(&self) -> i64 {
        self.annotations
            .iter()
            .find(|(k, _)| k.as_str() == DELIVERY_TIME)
            .and_then(|(_, v)| match v {
                Value::Timestamp(t) => Some(*t),
                v => v.as_i64(),
            })
            .unwrap_or(0)
    }

    /// An application property, as the facade holds it.
    pub fn property(&self, name: &str) -> Option<&Value> {
        self.application_properties
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v)
    }

    /// `JmsSession.send`'s preparation of the message, then `onSend`: the
    /// delivery mode, priority, destination, timestamps, expiration and
    /// message ID, then the header's ttl and the type's content type.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send(
        &mut self,
        persistent: bool,
        priority: u8,
        destination: &Destination,
        now: i64,
        ttl: i64,
        message_id: Option<String>,
        timestamp: bool,
    ) {
        self.durable = persistent;
        self.set_priority(i32::from(priority));
        self.set_redelivered(false);
        self.props().to = Some(destination.address().to_owned());
        // `setDeliveryTime(now, false)` drops a delivery time a received
        // message carried.
        self.annotations.retain(|(k, _)| {
            k.as_str() != JMS_DEST && k.as_str() != TO_TYPE && k.as_str() != DELIVERY_TIME
        });
        self.props().creation_time = timestamp.then_some(Timestamp(now));
        self.props().absolute_expiry_time = (ttl > 0).then_some(Timestamp(now + ttl));
        self.synthetic_expiration = None;
        self.props().message_id = message_id.map(Value::String);
        // Nobody sends as another user.
        self.props().user_id = None;
        // `onSend`: the override, else the producer's time to live.
        let ttl = self.ttl_override.map_or(ttl, i64::from);
        self.ttl = (ttl > 0 && ttl < 0xffff_ffff).then_some(ttl as u32);
        match self.kind {
            kind::BYTES => {
                if self.props().content_type.is_none() {
                    self.props().content_type = Some(Symbol::from(OCTET_STREAM));
                }
                if self.body.is_none() {
                    self.body = Some(Body::Data(Vec::new()));
                }
            }
            kind::OBJECT if self.typed_object => {
                if let Some(p) = self.properties.as_mut() {
                    p.content_type = None;
                }
                if self.body.is_none() {
                    self.body = Some(Body::Value(Value::Null));
                }
            }
            kind::OBJECT => {
                self.props().content_type = Some(Symbol::from(SERIALIZED_JAVA_OBJECT));
                if self.body.is_none() {
                    self.body = Some(Body::Data(NULL_OBJECT.to_vec()));
                }
            }
            _ => {}
        }
        // The destination's type, for the annotations the encoding adds.
        self.consumer_destination = None;
        self.sent_to = Some(destination.clone());
    }

    /// `AmqpCodec.encodeMessage`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if self.durable
            || self.priority.is_some()
            || self.ttl.is_some()
            || self.first_acquirer
            || self.delivery_count > 0
        {
            Header {
                durable: Some(self.durable),
                priority: self.priority,
                ttl: self.ttl,
                first_acquirer: self.first_acquirer.then_some(true),
                delivery_count: (self.delivery_count > 0).then_some(self.delivery_count),
            }
            .encode(&mut out);
        }
        if let Some(da) = &self.delivery_annotations
            && da.as_map().is_some_and(|m| !m.is_empty())
        {
            section_value(section::DELIVERY_ANNOTATIONS, da.clone(), &mut out);
        }
        // The annotations, with the destination and type ones put last as
        // the codec puts them.
        let mut annotations = self.annotations.clone();
        let dest_type = self
            .destination()
            .map(|d| if d.is_queue() { 0i8 } else { 1 });
        let mut put = |key: &str, value: Option<Value>| {
            annotations.retain(|(k, _)| k.as_str() != key);
            if let Some(v) = value {
                annotations.push((Symbol::from(key), v));
            }
        };
        put(JMS_REPLY_TO, self.reply_to_type_byte().map(Value::Byte));
        put(JMS_DEST, dest_type.map(Value::Byte));
        put(JMS_MSG_TYPE, Some(Value::Byte(self.kind as i8)));
        let ordered = java_map_order(annotations, |k| k.as_str());
        section_value(
            section::MESSAGE_ANNOTATIONS,
            Value::Map(
                ordered
                    .into_iter()
                    .map(|(k, v)| (Value::Symbol(k), v))
                    .collect(),
            ),
            &mut out,
        );
        if let Some(p) = &self.properties {
            p.encode(&mut out);
        }
        if !self.application_properties.is_empty() {
            let ordered = java_map_order(self.application_properties.clone(), |k| k.as_str());
            section_value(
                section::APPLICATION_PROPERTIES,
                Value::Map(
                    ordered
                        .into_iter()
                        .map(|(k, v)| (Value::String(k), v))
                        .collect(),
                ),
                &mut out,
            );
        }
        match &self.body {
            Some(Body::Value(v)) => section_value(section::AMQP_VALUE, v.clone(), &mut out),
            Some(Body::Data(d)) => section_value(section::DATA, Value::Binary(d.clone()), &mut out),
            Some(Body::Sequence(s)) => {
                section_value(section::AMQP_SEQUENCE, Value::List(s.clone()), &mut out)
            }
            None => {}
        }
        if let Some(f) = &self.footer
            && f.as_map().is_some_and(|m| !m.is_empty())
        {
            section_value(section::FOOTER, f.clone(), &mut out);
        }
        out
    }

    /// `AmqpCodec.decodeMessage` and `facade.initialize(consumer)`: the
    /// sections, the type from the annotation or the body, and what the
    /// facade works out on receipt.
    pub fn decode(
        bytes: &[u8],
        consumer_destination: Option<&Destination>,
        now: i64,
    ) -> Result<Self, String> {
        let mut header = None;
        let mut delivery_annotations = None;
        let mut annotations = Vec::new();
        let mut properties = None;
        let mut application_properties = Vec::new();
        let mut body = None;
        let mut footer = None;
        let mut d = Decoder::new(bytes);
        while d.has_remaining() {
            let v = d.read().map_err(|e| e.to_string())?;
            let Value::Described(desc, inner) = &v else {
                return Err("Unknown Message Section forced decode abort.".to_owned());
            };
            match section_code(desc) {
                Some(section::HEADER) => {
                    header = Some(Header::from_value(v.clone()).map_err(|e| e.to_string())?)
                }
                Some(section::DELIVERY_ANNOTATIONS) => {
                    delivery_annotations = Some((**inner).clone())
                }
                Some(section::MESSAGE_ANNOTATIONS) => {
                    annotations = match &**inner {
                        Value::Map(m) => m
                            .iter()
                            .filter_map(|(k, v)| match k {
                                Value::Symbol(s) => Some((s.clone(), v.clone())),
                                Value::String(s) => Some((Symbol::from(s.as_str()), v.clone())),
                                _ => None,
                            })
                            .collect(),
                        _ => Vec::new(),
                    }
                }
                Some(section::PROPERTIES) => {
                    properties = Some(Properties::from_value(v.clone()).map_err(|e| e.to_string())?)
                }
                Some(section::APPLICATION_PROPERTIES) => {
                    application_properties = match &**inner {
                        Value::Map(m) => m
                            .iter()
                            .filter_map(|(k, v)| k.as_str().map(|k| (k.to_owned(), v.clone())))
                            .collect(),
                        _ => Vec::new(),
                    }
                }
                Some(section::DATA) => {
                    body = Some(Body::Data(inner.as_binary().unwrap_or_default().to_vec()))
                }
                Some(section::AMQP_SEQUENCE) => {
                    body = Some(Body::Sequence(inner.as_list().unwrap_or_default().to_vec()))
                }
                Some(section::AMQP_VALUE) => body = Some(Body::Value((**inner).clone())),
                Some(section::FOOTER) => footer = Some((**inner).clone()),
                _ => return Err("Unknown Message Section forced decode abort.".to_owned()),
            }
        }
        let content_type = properties
            .as_ref()
            .and_then(|p| p.content_type.as_ref())
            .map(|s| s.as_str().to_owned());
        let annotated = annotations
            .iter()
            .find(|(k, _)| k.as_str() == JMS_MSG_TYPE)
            .map(|(_, v)| v.clone())
            .filter(|v| !v.is_null());
        let kind = match annotated {
            Some(Value::Byte(b)) => match b {
                0..=5 => b as u8,
                other => {
                    return Err(format!(
                        "Invalid JMS Message Type annotation value found in message: {other}"
                    ));
                }
            },
            Some(other) => {
                return Err(format!(
                    "java.lang.ClassCastException: {other:?} cannot be cast to java.lang.Byte"
                ));
            }
            None => kind_without_annotation(body.as_ref(), content_type.as_deref()),
        };
        let header = header.unwrap_or_default();
        let mut m = Self {
            kind,
            durable: header.durable == Some(true),
            priority: header.priority.filter(|p| *p != 4),
            ttl: header.ttl.filter(|t| *t > 0),
            ttl_override: None,
            typed_object: false,
            first_acquirer: header.first_acquirer == Some(true),
            delivery_count: header.delivery_count.unwrap_or(0),
            delivery_annotations,
            annotations,
            properties,
            application_properties,
            body,
            footer,
            consumer_destination: consumer_destination.cloned(),
            synthetic_expiration: None,
            identity: identity(),
            sent_to: None,
            reply_to_type: None,
        };
        if m.properties
            .as_ref()
            .and_then(|p| p.absolute_expiry_time)
            .is_none()
            && let Some(ttl) = m.ttl
        {
            m.synthetic_expiration = Some(now + i64::from(ttl));
        }
        m.annotations.retain(|(k, _)| k.as_str() != JMS_MSG_TYPE);
        Ok(m)
    }

    /// `getJMSMessageID` (`AmqpMessageIdHelper.toMessageIdString`).
    pub fn jms_message_id(&self) -> Option<String> {
        id_string(self.properties.as_ref()?.message_id.as_ref()?)
    }

    /// `getJMSDestination`: from the annotation and `to`, else the
    /// consumer's destination.
    pub fn destination(&self) -> Option<Destination> {
        if let Some(d) = &self.sent_to {
            return Some(d.clone());
        }
        let to = self.properties.as_ref().and_then(|p| p.to.clone());
        let Some(address) = to else {
            return self.consumer_destination.clone();
        };
        let annotated = self
            .annotations
            .iter()
            .find(|(k, _)| k.as_str() == JMS_DEST)
            .and_then(|(_, v)| v.as_i64());
        Some(match annotated {
            Some(0 | 2) => Destination::Queue(address),
            Some(1 | 3) => Destination::Topic(address),
            _ => match &self.consumer_destination {
                Some(Destination::Topic(_)) => Destination::Topic(address),
                _ => Destination::Queue(address),
            },
        })
    }

    /// `setJMSReplyTo`: the address, its type annotated when sent.
    pub fn set_reply_to(&mut self, address: Option<String>, type_byte: i8) {
        self.annotations
            .retain(|(k, _)| k.as_str() != JMS_REPLY_TO && k.as_str() != LEGACY_REPLY_TO_TYPE);
        self.reply_to_type = address.is_some().then_some(type_byte);
        if let Some(p) = self.props_for(address.is_some()) {
            p.reply_to = address;
        }
    }

    /// The reply-to address, if any.
    pub fn reply_to(&self) -> Option<String> {
        self.properties.as_ref()?.reply_to.clone()
    }

    /// `getJMSReplyTo`'s type (`AmqpDestinationHelper.getJmsReplyTo`): the
    /// one set, else the annotation's, else the consumer destination's.
    pub fn reply_to_type_byte(&self) -> Option<i8> {
        self.reply_to()?;
        if let Some(t) = self.reply_to_type {
            return Some(t);
        }
        let annotated = self
            .annotations
            .iter()
            .find(|(k, _)| k.as_str() == JMS_REPLY_TO)
            .and_then(|(_, v)| v.as_i64())
            .filter(|t| (0..=3).contains(t));
        if let Some(t) = annotated {
            return Some(t as i8);
        }
        match &self.consumer_destination {
            Some(Destination::Topic(_)) => Some(1),
            Some(Destination::Queue(_)) => Some(0),
            None => None,
        }
    }

    /// `getJMSDeliveryMode`: persistent when the header says durable.
    pub fn persistent(&self) -> bool {
        self.durable
    }

    /// `getJMSPriority`.
    pub fn priority(&self) -> u8 {
        self.priority.unwrap_or(4).min(9)
    }

    /// `getJMSTimestamp`.
    pub fn timestamp(&self) -> i64 {
        self.properties
            .as_ref()
            .and_then(|p| p.creation_time)
            .map_or(0, |t| t.0)
    }

    /// `getJMSExpiration`.
    pub fn expiration(&self) -> i64 {
        self.properties
            .as_ref()
            .and_then(|p| p.absolute_expiry_time)
            .map(|t| t.0)
            .or(self.synthetic_expiration)
            .unwrap_or(0)
    }

    /// `getJMSRedelivered`.
    pub fn redelivered(&self) -> bool {
        (self.delivery_count as i32) > 0
    }

    /// `JMSXDeliveryCount`.
    pub fn jmsx_delivery_count(&self) -> i32 {
        self.delivery_count as i32 + 1
    }

    /// `TextMessage.getText`, or the unchecked exception it throws.
    pub fn text(&self) -> Result<Option<String>, String> {
        match &self.body {
            None => Ok(None),
            Some(Body::Data(d)) if d.is_empty() => Ok(Some(String::new())),
            Some(Body::Data(d)) => String::from_utf8(d.clone())
                .map(Some)
                .map_err(|_| "javax.jms.JMSException: Cannot decode String in UTF-8".to_owned()),
            Some(Body::Value(Value::Null)) => Ok(None),
            Some(Body::Value(Value::String(s))) => Ok(Some(s.clone())),
            Some(Body::Value(v)) => Err(format!(
                "java.lang.IllegalStateException: Unexpected amqp-value body content type: {}",
                java_simple_name(v)
            )),
            Some(Body::Sequence(_)) => Err(
                "java.lang.IllegalStateException: Unexpected message body type: AmqpSequence"
                    .to_owned(),
            ),
        }
    }

    /// `BytesMessage`'s body (`getBinaryFromBody`).
    pub fn bytes_body(&self) -> Result<Vec<u8>, String> {
        match &self.body {
            None => Ok(Vec::new()),
            Some(Body::Data(d)) => Ok(d.clone()),
            Some(Body::Value(Value::Null)) => Ok(Vec::new()),
            Some(Body::Value(Value::Binary(b))) => Ok(b.clone()),
            Some(Body::Value(v)) => Err(format!(
                "java.lang.IllegalStateException: Unexpected amqp-value body content type: {}",
                java_simple_name(v)
            )),
            Some(Body::Sequence(_)) => Err(
                "java.lang.IllegalStateException: Unexpected body content type: AmqpSequence"
                    .to_owned(),
            ),
        }
    }

    /// The serialized object of an `ObjectMessage`'s serialized body.
    pub fn serialized_object(&self) -> Option<&[u8]> {
        match &self.body {
            Some(Body::Data(d)) if d.as_slice() != NULL_OBJECT => Some(d),
            _ => None,
        }
    }

    /// `JmsMessage.toString`: the facade's class and identity.
    pub fn to_jms_string(&self) -> String {
        let facade = |name: &str| {
            format!(
                "org.apache.qpid.jms.provider.amqp.message.{name}@{:x}",
                self.identity
            )
        };
        match self.kind {
            kind::TEXT => format!(
                "JmsTextMessage {{ {} }}",
                facade("AmqpJmsTextMessageFacade")
            ),
            kind::BYTES => format!(
                "JmsBytesMessage {{ {} }}",
                facade("AmqpJmsBytesMessageFacade")
            ),
            kind::MAP => format!("JmsMapMessage {{ {} }}", facade("AmqpJmsMapMessageFacade")),
            kind::STREAM => {
                format!(
                    "JmsStreamMessage {{ {} }}",
                    facade("AmqpJmsStreamMessageFacade")
                )
            }
            kind::OBJECT => format!(
                "JmsObjectMessageFacade {{ {} }}",
                facade("AmqpJmsObjectMessageFacade")
            ),
            _ => format!("JmsMessage {{ {} }}", facade("AmqpJmsMessageFacade")),
        }
    }
}

/// `AmqpCodec.createWithoutAnnotation`: the type from the body section
/// and the content type.
fn kind_without_annotation(body: Option<&Body>, content_type: Option<&str>) -> u8 {
    let textual = content_type.is_some_and(is_textual);
    match body {
        None => match content_type {
            Some(SERIALIZED_JAVA_OBJECT) => kind::OBJECT,
            Some(OCTET_STREAM) | None => kind::BYTES,
            _ if textual => kind::TEXT,
            _ => kind::MESSAGE,
        },
        Some(Body::Data(_)) => match content_type {
            Some(OCTET_STREAM) | None => kind::BYTES,
            Some(SERIALIZED_JAVA_OBJECT) => kind::OBJECT,
            _ if textual => kind::TEXT,
            _ => kind::BYTES,
        },
        Some(Body::Value(Value::Null | Value::String(_))) => kind::TEXT,
        Some(Body::Value(Value::Binary(_))) => kind::BYTES,
        Some(Body::Value(_)) | Some(Body::Sequence(_)) => kind::OBJECT,
    }
}

/// `ContentTypeSupport.parseContentTypeForTextualCharset` finding a
/// charset: `text/*`, the textual `application/*` types, `+xml` and
/// `+json`.
fn is_textual(content_type: &str) -> bool {
    let Some((t, rest)) = content_type.split_once('/') else {
        return false;
    };
    let t = t.trim().to_ascii_lowercase();
    let subtype = rest
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if t.is_empty() || subtype.is_empty() {
        return false;
    }
    t == "text"
        || (t == "application"
            && matches!(
                subtype.as_str(),
                "xml" | "json" | "javascript" | "xml-dtd" | "ecmascript"
            ))
        || subtype.ends_with("+xml")
        || subtype.ends_with("+json")
}

/// The simple name of the Java class proton-j decodes `v` to.
fn java_simple_name(v: &Value) -> &'static str {
    match v {
        Value::Map(_) => "HashMap",
        Value::List(_) => "ArrayList",
        Value::Array(_) => "Object[]",
        Value::Boolean(_) => "Boolean",
        Value::Byte(_) => "Byte",
        Value::Short(_) => "Short",
        Value::Int(_) => "Integer",
        Value::Long(_) => "Long",
        Value::Float(_) => "Float",
        Value::Double(_) => "Double",
        Value::Char(_) => "Character",
        Value::Ubyte(_) => "UnsignedByte",
        Value::Ushort(_) => "UnsignedShort",
        Value::Uint(_) => "UnsignedInteger",
        Value::Ulong(_) => "UnsignedLong",
        Value::Timestamp(_) => "Date",
        Value::Uuid(_) => "UUID",
        Value::Binary(_) => "Binary",
        Value::Symbol(_) => "Symbol",
        _ => "Object",
    }
}

/// `AmqpMessageIdHelper.toMessageIdString`.
pub fn id_string(id: &Value) -> Option<String> {
    const TYPED: [&str; 5] = ["BINARY:", "UUID:", "ULONG:", "STRING:", "NO_PREFIX:"];
    Some(match id {
        Value::String(s) => {
            if !s.starts_with("ID:") {
                format!("ID:AMQP_NO_PREFIX:{s}")
            } else if s[3..].starts_with("AMQP_") && TYPED.iter().any(|t| s[8..].starts_with(t)) {
                format!("ID:AMQP_STRING:{s}")
            } else {
                s.clone()
            }
        }
        Value::Uuid(u) => format!("ID:AMQP_UUID:{}", uuid_text(u)),
        Value::Ulong(n) => format!("ID:AMQP_ULONG:{n}"),
        Value::Binary(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
            format!("ID:AMQP_BINARY:{hex}")
        }
        Value::Null => return None,
        _ => return None,
    })
}

/// `AmqpMessageIdHelper.toIdObject`: a JMS ID's AMQP value, as its
/// encoding prefix names it; any other string as it is.
pub fn to_id_object(id: &str) -> Result<Value, String> {
    let fail = || {
        "org.apache.qpid.jms.exceptions.IdConversionException: Unable to convert ID value"
            .to_owned()
    };
    let Some(rest) = id.strip_prefix("ID:") else {
        return Ok(Value::String(id.to_owned()));
    };
    Ok(if let Some(s) = rest.strip_prefix("AMQP_NO_PREFIX:") {
        Value::String(s.to_owned())
    } else if let Some(s) = rest.strip_prefix("AMQP_UUID:") {
        Value::Uuid(java_uuid(s).ok_or_else(fail)?)
    } else if let Some(s) = rest.strip_prefix("AMQP_ULONG:") {
        Value::Ulong(java_unsigned_long(s).ok_or_else(fail)?)
    } else if let Some(s) = rest.strip_prefix("AMQP_STRING:") {
        Value::String(s.to_owned())
    } else if let Some(s) = rest.strip_prefix("AMQP_BINARY:") {
        let units: Vec<u16> = s.encode_utf16().collect();
        if !units.len().is_multiple_of(2) {
            return Err(fail());
        }
        let nibble = |u: u16| char::from_u32(u32::from(u)).and_then(|c| c.to_digit(16));
        let mut bytes = Vec::with_capacity(units.len() / 2);
        for pair in units.chunks(2) {
            bytes.push(
                (nibble(pair[0]).ok_or_else(fail)? << 4 | nibble(pair[1]).ok_or_else(fail)?) as u8,
            );
        }
        Value::Binary(bytes)
    } else {
        Value::String(id.to_owned())
    })
}

/// `Character.digit(c, 16)`: the decimal digits Java reads, and the Latin
/// and fullwidth letters a to f.
fn java_hex_digit(unit: u16) -> Option<u64> {
    let c = char::from_u32(u32::from(unit))?;
    if let Some(d) = saranga_filter::java_parse_int::<i64>(&c.to_string()).filter(|d| *d >= 0) {
        return Some(d as u64);
    }
    match unit {
        0x41..=0x46 => Some(u64::from(unit - 0x41 + 10)),
        0x61..=0x66 => Some(u64::from(unit - 0x61 + 10)),
        0xFF21..=0xFF26 => Some(u64::from(unit - 0xFF21 + 10)),
        0xFF41..=0xFF46 => Some(u64::from(unit - 0xFF41 + 10)),
        _ => None,
    }
}

/// `Long.parseLong(s, begin, end, 16)`.
fn java_parse_hex_long(units: &[u16]) -> Option<i64> {
    let (negative, digits) = match units.first()? {
        0x2D => (true, &units[1..]),
        0x2B => (false, &units[1..]),
        _ => (false, units),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: i128 = 0;
    for u in digits {
        v = v * 16 + i128::from(java_hex_digit(*u)?);
        if v > i128::from(i64::MAX) + 1 {
            return None;
        }
    }
    i64::try_from(if negative { -v } else { v }).ok()
}

/// `UUID.fromString`: the fast form of 36 characters, else its lenient
/// one of five dash-separated hexadecimal numbers.
fn java_uuid(s: &str) -> Option<[u8; 16]> {
    let units: Vec<u16> = s.encode_utf16().collect();
    let len = units.len();
    if len > 36 {
        return None;
    }
    let dash = |from: usize| -> Option<usize> {
        units
            .iter()
            .skip(from)
            .position(|u| *u == 0x2D)
            .map(|i| i + from)
    };
    let d1 = dash(0)?;
    let d2 = dash(d1 + 1)?;
    let d3 = dash(d2 + 1)?;
    let d4 = dash(d3 + 1)?;
    if dash(d4 + 1).is_some() {
        return None;
    }
    let mut most = (java_parse_hex_long(&units[..d1])? as u64) & 0xffff_ffff;
    most = most << 16 | (java_parse_hex_long(&units[d1 + 1..d2])? as u64) & 0xffff;
    most = most << 16 | (java_parse_hex_long(&units[d2 + 1..d3])? as u64) & 0xffff;
    let mut least = (java_parse_hex_long(&units[d3 + 1..d4])? as u64) & 0xffff;
    least = least << 48 | (java_parse_hex_long(&units[d4 + 1..])? as u64) & 0xffff_ffff_ffff;
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&most.to_be_bytes());
    out[8..].copy_from_slice(&least.to_be_bytes());
    Some(out)
}

/// `UnsignedLong.valueOf(String)`: `new BigInteger(s)` within 0 to 2^64.
fn java_unsigned_long(s: &str) -> Option<u64> {
    let (negative, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: u128 = 0;
    for c in digits.chars() {
        let d = saranga_filter::java_parse_int::<i64>(&c.to_string()).filter(|d| *d >= 0)?;
        v = (v * 10 + d as u128).min(u128::from(u64::MAX) + 1);
    }
    if negative && v != 0 {
        return None;
    }
    u64::try_from(v).ok()
}

fn uuid_text(u: &[u8; 16]) -> String {
    let h: Vec<String> = u.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        h[0..4].concat(),
        h[4..6].concat(),
        h[6..8].concat(),
        h[8..10].concat(),
        h[10..16].concat()
    )
}

/// Entries in the order a `new HashMap<>()` they were put into in this
/// order iterates them.
fn java_map_order<K, V>(entries: Vec<(K, V)>, key: impl Fn(&K) -> &str) -> Vec<(K, V)> {
    let hashes: Vec<i32> = entries.iter().map(|(k, _)| string_hash(key(k))).collect();
    let order = hash_map_order(&hashes, None);
    let mut slots: Vec<Option<(K, V)>> = entries.into_iter().map(Some).collect();
    order.into_iter().filter_map(|i| slots[i].take()).collect()
}

fn section_value(code: u64, body: Value, out: &mut Vec<u8>) {
    codec::encode(&Value::described(Descriptor::Code(code), body), out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_persistent_text_message_encodes_as_qpid_encodes_it() {
        let mut m = AmqpJmsMessage::new_text(Some("hi"));
        m.prepare_send(
            true,
            4,
            &Destination::Queue("q".into()),
            1000,
            0,
            Some("ID:x:1:1:1-1".into()),
            true,
        );
        let back = AmqpJmsMessage::decode(&m.encode(), None, 0).unwrap();
        assert_eq!(back.kind, kind::TEXT);
        assert_eq!(back.text().unwrap().as_deref(), Some("hi"));
        assert!(back.persistent());
        assert_eq!(back.jms_message_id().as_deref(), Some("ID:x:1:1:1-1"));
        assert_eq!(back.destination(), Some(Destination::Queue("q".into())));
        assert_eq!(back.timestamp(), 1000);
    }

    #[test]
    fn non_persistent_default_priority_has_no_header() {
        let mut m = AmqpJmsMessage::bytes(b"ab");
        m.prepare_send(false, 4, &Destination::Topic("t".into()), 1, 0, None, false);
        let bytes = m.encode();
        // The first section is the message annotations.
        assert_eq!(&bytes[..3], &[0x00, 0x53, 0x72]);
        let back = AmqpJmsMessage::decode(&bytes, None, 0).unwrap();
        assert_eq!(back.kind, kind::BYTES);
        assert_eq!(back.bytes_body().unwrap(), b"ab");
        assert!(!back.persistent());
    }

    #[test]
    fn message_ids_read_as_qpid_reads_them() {
        assert_eq!(
            id_string(&Value::String("abc".into())).unwrap(),
            "ID:AMQP_NO_PREFIX:abc"
        );
        assert_eq!(
            id_string(&Value::String("ID:AMQP_UUID:x".into())).unwrap(),
            "ID:AMQP_STRING:ID:AMQP_UUID:x"
        );
        assert_eq!(id_string(&Value::Ulong(7)).unwrap(), "ID:AMQP_ULONG:7");
        assert_eq!(
            id_string(&Value::Binary(vec![0xab, 1])).unwrap(),
            "ID:AMQP_BINARY:AB01"
        );
    }

    #[test]
    fn types_without_the_annotation() {
        assert_eq!(kind_without_annotation(None, None), kind::BYTES);
        assert_eq!(
            kind_without_annotation(Some(&Body::Data(vec![])), Some("text/plain")),
            kind::TEXT
        );
        assert_eq!(
            kind_without_annotation(Some(&Body::Value(Value::Map(vec![]))), None),
            kind::OBJECT
        );
        assert!(is_textual("application/vnd.x+json; charset=utf-8"));
        assert!(!is_textual("application/octet-stream"));
    }
}
