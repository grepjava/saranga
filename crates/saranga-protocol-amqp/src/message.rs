//! AMQP messages as the broker keeps them: in their AMQP encoding, as
//! `AMQPStandardMessage` and `AMQPMessage` do.
//!
//! The decoded header, message annotations and properties are the
//! broker's working copy; the encoding changes only when the broker calls
//! [`reencode`](AmqpMessage::reencode), as in Artemis. Application
//! properties are decoded on demand. Maps iterate as the Java maps Artemis
//! keeps them in, so a re-encoded message matches Artemis byte for byte.

use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString, TypedProperties};
use saranga_buffer::{Reader, Writer};
use saranga_storage::ids::persister;
use saranga_storage::message::{AmqpLargeMessageRecord, AmqpMessageRecord, PersistedMessage};

use crate::codec::{self, Decoder, Result};
use crate::composite::{Composite, Timestamp};
use crate::javamap::{JavaMap, Kind};
use crate::messaging::{Header, Properties, section};
use crate::types::{Descriptor, Symbol, Value};

/// `AMQPMessage.ADDRESS_PROPERTY`: where `setAddress` keeps the address.
pub const ADDRESS_PROPERTY: &str = "_AMQ_AD";
/// `AMQPMessage.DEFAULT_MESSAGE_PRIORITY`.
pub const DEFAULT_PRIORITY: u8 = 4;
/// `AMQPMessage.MAX_MESSAGE_PRIORITY`.
pub const MAX_PRIORITY: u8 = 9;

/// Message annotations Artemis reads (`AMQPMessageSupport`).
pub mod annotation {
    pub const JMS_MSG_TYPE: &str = "x-opt-jms-msg-type";
    pub const JMS_DEST: &str = "x-opt-jms-dest";
    pub const JMS_REPLY_TO: &str = "x-opt-jms-reply-to";
    pub const DELIVERY_TIME: &str = "x-opt-delivery-time";
    pub const DELIVERY_DELAY: &str = "x-opt-delivery-delay";
    pub const ROUTING_TYPE: &str = "x-opt-routing-type";
    pub const INGRESS_TIME: &str = "x-opt-ingress-time";
    pub const X_OPT_PREFIX: &str = "x-opt-";
}

/// `x-opt-jms-dest` and `x-opt-jms-reply-to` values.
pub mod dest_type {
    pub const QUEUE: i8 = 0;
    pub const TOPIC: i8 = 1;
    pub const TEMP_QUEUE: i8 = 2;
    pub const TEMP_TOPIC: i8 = 3;
}

/// `AMQPMessageSupport.toAnnotationName`: `_AMQ_ORIG_ADDRESS` becomes
/// `x-opt-ORIG-ADDRESS`.
pub fn to_annotation_name(key: &str) -> String {
    if key.starts_with(annotation::X_OPT_PREFIX) {
        return key.to_owned();
    }
    match key.strip_prefix("_AMQ_") {
        Some(rest) => format!("{}{}", annotation::X_OPT_PREFIX, rest.replace('_', "-")),
        None => key.to_owned(),
    }
}

/// Message annotations: a `LinkedHashMap` once decoded, a `HashMap` when
/// the broker makes them.
pub type Annotations = JavaMap<Symbol, Value>;

/// Extra properties: a `TypedProperties`, whose map is a `HashMap`.
pub type ExtraProperties = JavaMap<SimpleString, PropertyValue>;

/// An AMQP message (`AMQPStandardMessage`).
#[derive(Debug, Clone)]
pub struct AmqpMessage {
    /// The transfer's `message-format`.
    pub message_format: i64,
    message_id: i64,
    address: Option<SimpleString>,
    /// The encoded sections.
    data: Arc<[u8]>,
    /// Properties the broker carries with the message, kept only in its
    /// storage (`extraProperties`).
    extra: Option<ExtraProperties>,
    expiration: i64,
    /// The expiration came from the journal record, not the sections.
    expiration_reload: bool,
    /// Read back from storage and not yet looked at: durable unless its
    /// header says otherwise (`RELOAD_PERSISTENCE`).
    reloaded: bool,
    priority: u8,

    // Where the sections are, from the last scan (`scanMessageData`).
    header: Option<Header>,
    encoded_header_size: usize,
    delivery_annotations: Option<Value>,
    encoded_delivery_annotations_size: usize,
    message_annotations: Option<Annotations>,
    properties: Option<Properties>,
    application_properties_position: Option<usize>,
    /// Decoded application properties, once something read or changed
    /// them.
    application_properties: Option<Vec<(String, Value)>>,
    remaining_body_position: Option<usize>,
    /// `connectionID`: the sending connection's container ID, which no-local
    /// filters read. Not stored.
    connection_id: Option<String>,

    // Where the other sections start, for a large message's record.
    header_position: Option<usize>,
    delivery_annotations_position: Option<usize>,
    message_annotations_position: Option<usize>,
    properties_position: Option<usize>,

    /// An `AMQPLargeMessage`: the encoding is its file's, which the broker
    /// never rewrites; changes to the sections are kept in its record.
    large: bool,
    /// `isReencoded`: a large message's sections changed.
    reencoded: bool,
}

impl AmqpMessage {
    /// A message received from a client: its encoded sections.
    pub fn new(message_format: i64, data: Vec<u8>, now: i64) -> Result<Self> {
        let mut m = Self::empty(message_format, data.into());
        m.scan(now)?;
        Ok(m)
    }

    fn empty(message_format: i64, data: Arc<[u8]>) -> Self {
        Self {
            message_format,
            message_id: 0,
            address: None,
            data,
            extra: None,
            expiration: 0,
            expiration_reload: false,
            reloaded: false,
            priority: DEFAULT_PRIORITY,
            header: None,
            encoded_header_size: 0,
            delivery_annotations: None,
            encoded_delivery_annotations_size: 0,
            message_annotations: None,
            properties: None,
            application_properties_position: None,
            application_properties: None,
            remaining_body_position: None,
            connection_id: None,
            header_position: None,
            delivery_annotations_position: None,
            message_annotations_position: None,
            properties_position: None,
            large: false,
            reencoded: false,
        }
    }

    /// Makes this a large message (`AMQPLargeMessage`): its application
    /// properties are decoded now, as Artemis parses them.
    pub fn make_large(&mut self) {
        self.large = true;
        if self.application_properties.is_none() && self.application_properties_position.is_some() {
            self.application_properties = Some(self.application_properties());
        }
    }

    pub fn is_large(&self) -> bool {
        self.large
    }

    /// A large message read back from its record and its file
    /// (`AMQPLargeMessagePersister.decode`, `readSavedEncoding`).
    pub fn from_large_record(
        record: &AmqpLargeMessageRecord,
        data: Vec<u8>,
        now: i64,
    ) -> Result<Self> {
        let mut m = Self::empty(record.message_format, data.into());
        m.message_id = record.message_id;
        m.large = true;
        m.extra = record
            .extra_properties
            .as_ref()
            .map(|p| JavaMap::decoded(p.entries().to_vec()));
        if let Some(a) = &record.address {
            m.set_address(a.clone());
        }
        let bad = |e: saranga_buffer::DecodeError| codec::CodecError::Invalid(e.to_string());
        let mut r = Reader::new(&record.saved_encoding);
        let pos = |r: &mut Reader<'_>| -> Result<Option<usize>> {
            let p = r.i32().map_err(bad)?;
            Ok(usize::try_from(p).ok())
        };
        let value = |r: &mut Reader<'_>| -> Result<Value> {
            let mut d = Decoder::new(r.rest());
            let v = d.read()?;
            r.bytes(d.position()).map_err(bad)?;
            Ok(v)
        };
        m.header_position = pos(&mut r)?;
        m.encoded_header_size = r.i32().map_err(bad)?.max(0) as usize;
        if let v @ Value::Described(..) = value(&mut r)? {
            let h = <Header as Composite>::from_value(v)?;
            if let Some(p) = h.priority {
                m.priority = p.min(MAX_PRIORITY);
            }
            m.header = Some(h);
        }
        m.delivery_annotations_position = pos(&mut r)?;
        m.encoded_delivery_annotations_size = r.i32().map_err(bad)?.max(0) as usize;
        m.message_annotations_position = pos(&mut r)?;
        if let Value::Described(_, body) = value(&mut r)? {
            m.message_annotations = Some(JavaMap::linked(symbol_map(*body)?));
        }
        m.properties_position = pos(&mut r)?;
        if let v @ Value::Described(..) = value(&mut r)? {
            m.properties = Some(<Properties as Composite>::from_value(v)?);
        }
        m.application_properties_position = pos(&mut r)?;
        m.remaining_body_position = pos(&mut r)?;
        if let Value::Described(_, body) = value(&mut r)? {
            m.application_properties = Some(string_map(*body)?);
        }
        if let Some(Timestamp(t)) = m.properties.as_ref().and_then(|p| p.absolute_expiry_time)
            && t > 0
        {
            m.expiration = t;
        } else if let Some(ttl) = m.header.as_ref().and_then(|h| h.ttl) {
            m.expiration = now + i64::from(ttl);
        }
        if r.remaining() >= 8 {
            m.expiration = r.i64().map_err(bad)?;
            m.expiration_reload = true;
        }
        if !r.is_empty() {
            m.reencoded = r.bool().map_err(bad)?;
        }
        m.reloaded = record.durable;
        Ok(m)
    }

    /// `saveEncoding`: the positions and the decoded sections.
    fn saved_encoding(&self) -> Vec<u8> {
        let mut w = Writer::new();
        let pos = |w: &mut Writer, p: Option<usize>| {
            w.i32(p.map_or(-1, |p| p as i32));
        };
        let value = |w: &mut Writer, v: Option<Vec<u8>>| {
            w.bytes(&v.unwrap_or_else(|| vec![codec::code::NULL]));
        };
        pos(&mut w, self.header_position);
        w.i32(self.encoded_header_size as i32);
        value(&mut w, self.header.as_ref().map(Composite::to_bytes));
        pos(&mut w, self.delivery_annotations_position);
        w.i32(self.encoded_delivery_annotations_size as i32);
        pos(&mut w, self.message_annotations_position);
        value(
            &mut w,
            self.message_annotations.as_ref().map(|ma| {
                let mut out = Vec::new();
                encode_annotations(section::MESSAGE_ANNOTATIONS, ma, &mut out);
                out
            }),
        );
        pos(&mut w, self.properties_position);
        value(&mut w, self.properties.as_ref().map(Composite::to_bytes));
        pos(&mut w, self.application_properties_position);
        pos(&mut w, self.remaining_body_position);
        value(
            &mut w,
            self.application_properties.as_ref().map(|ap| {
                let mut out = Vec::new();
                encode_section(
                    section::APPLICATION_PROPERTIES,
                    Value::Map(
                        ap.iter()
                            .map(|(k, v)| (Value::String(k.clone()), v.clone()))
                            .collect(),
                    ),
                    &mut out,
                );
                out
            }),
        );
        w.i64(self.expiration);
        w.byte_buf_bool(self.reencoded);
        w.into_vec()
    }

    /// `setConnectionID`.
    pub fn set_connection_id(&mut self, id: Option<String>) {
        self.connection_id = id;
    }

    /// `getConnectionID`.
    pub fn connection_id(&self) -> Option<&str> {
        self.connection_id.as_deref()
    }

    /// A message read back from the journal or a page
    /// (`AMQPMessagePersister*.decode`).
    pub fn from_record(record: &AmqpMessageRecord, now: i64) -> Result<Self> {
        let mut m = Self::empty(record.message_format, record.data.clone().into());
        m.message_id = record.message_id;
        m.extra = record
            .extra_properties
            .as_ref()
            .map(|p| JavaMap::decoded(p.entries().to_vec()));
        if let Some(e) = record.expiration {
            m.expiration = e;
            m.expiration_reload = true;
        }
        m.reloaded = true;
        m.scan(now)?;
        // reloadAddress: the persisted address, without touching the extra
        // properties.
        m.address = record.address.clone();
        Ok(m)
    }

    /// `scanMessageData`: finds the sections and decodes the header, the
    /// annotations and the properties.
    fn scan(&mut self, now: i64) -> Result<()> {
        self.header = None;
        self.message_annotations = None;
        self.properties = None;
        self.application_properties = None;
        if !self.expiration_reload {
            self.expiration = 0;
        }
        self.priority = DEFAULT_PRIORITY;
        self.encoded_header_size = 0;
        self.delivery_annotations = None;
        self.encoded_delivery_annotations_size = 0;
        self.application_properties_position = None;
        self.remaining_body_position = None;
        self.header_position = None;
        self.delivery_annotations_position = None;
        self.message_annotations_position = None;
        self.properties_position = None;
        let data = self.data.clone();
        let mut d = Decoder::new(&data);
        while d.has_remaining() {
            let start = d.position();
            let value = d.read()?;
            let size = d.position() - start;
            let Value::Described(desc, body) = value else {
                self.remaining_body_position = Some(start);
                break;
            };
            match section_code(&desc) {
                Some(section::HEADER) => {
                    self.header_position = Some(start);
                    let header = Header::from_described(&desc, *body)?;
                    if let Some(ttl) = header.ttl
                        && !self.expiration_reload
                    {
                        self.expiration = now + i64::from(ttl);
                    }
                    if let Some(p) = header.priority {
                        self.priority = p.min(MAX_PRIORITY);
                    }
                    self.header = Some(header);
                    self.encoded_header_size = size;
                }
                Some(section::DELIVERY_ANNOTATIONS) => {
                    self.delivery_annotations_position = Some(start);
                    self.delivery_annotations = Some(*body);
                    self.encoded_delivery_annotations_size = size;
                }
                Some(section::MESSAGE_ANNOTATIONS) => {
                    self.message_annotations_position = Some(start);
                    self.message_annotations = Some(JavaMap::linked(symbol_map(*body)?));
                }
                Some(section::PROPERTIES) => {
                    self.properties_position = Some(start);
                    let p = Properties::from_described(&desc, *body)?;
                    if let Some(Timestamp(t)) = p.absolute_expiry_time
                        && t > 0
                        && !self.expiration_reload
                    {
                        self.expiration = t;
                    }
                    self.properties = Some(p);
                }
                Some(section::APPLICATION_PROPERTIES) => {
                    self.application_properties_position = Some(start);
                    if d.has_remaining() {
                        self.remaining_body_position = Some(d.position());
                    }
                    break;
                }
                _ => {
                    // The body or the footer: kept as they are.
                    self.remaining_body_position = Some(start);
                    break;
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------ reading

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn message_id(&self) -> i64 {
        self.message_id
    }

    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    pub fn properties(&self) -> Option<&Properties> {
        self.properties.as_ref()
    }

    pub fn message_annotations(&self) -> Option<&Annotations> {
        self.message_annotations.as_ref()
    }

    pub fn extra_properties(&self) -> Option<&ExtraProperties> {
        self.extra.as_ref()
    }

    pub fn delivery_annotations(&self) -> Option<&Value> {
        self.delivery_annotations.as_ref()
    }

    /// `getAddressSimpleString`: the address set, else the one kept in the
    /// extra properties, else the `to` field.
    pub fn address(&self) -> Option<SimpleString> {
        if let Some(a) = &self.address {
            return Some(a.clone());
        }
        if let Some(PropertyValue::String(a)) = self
            .extra
            .as_ref()
            .and_then(|e| e.get(&SimpleString::from(ADDRESS_PROPERTY)))
        {
            return Some(a.clone());
        }
        self.properties
            .as_ref()
            .and_then(|p| p.to.as_deref())
            .map(SimpleString::from)
    }

    /// `isDurable`.
    pub fn is_durable(&self) -> bool {
        match self.header.as_ref().and_then(|h| h.durable) {
            Some(d) => d,
            None => self.reloaded,
        }
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn expiration(&self) -> i64 {
        self.expiration
    }

    /// `getTimestamp`: the creation time.
    pub fn timestamp(&self) -> i64 {
        self.properties
            .as_ref()
            .and_then(|p| p.creation_time)
            .map_or(0, |t| t.0)
    }

    /// A message annotation by name.
    pub fn annotation(&self, name: &str) -> Option<&Value> {
        self.message_annotations.as_ref()?.get(&Symbol::from(name))
    }

    /// `getMessageAnnotation(String)`: the annotation under the name as
    /// `toAnnotationName` makes it, else under the name itself.
    pub fn annotation_lookup(&self, name: &str) -> Option<&Value> {
        self.annotation(&to_annotation_name(name))
            .or_else(|| self.annotation(name))
    }

    /// `getRoutingType`: `x-opt-routing-type`, else from `x-opt-jms-dest`.
    pub fn routing_type(&self) -> Option<i8> {
        if let Some(v) = self.annotation(annotation::ROUTING_TYPE) {
            return v.as_i64().map(|t| t as i8);
        }
        let t = self.annotation(annotation::JMS_DEST)?.as_i64()? as i8;
        match t {
            dest_type::QUEUE | dest_type::TEMP_QUEUE => Some(1),
            dest_type::TOPIC | dest_type::TEMP_TOPIC => Some(0),
            _ => None,
        }
    }

    /// `getScheduledDeliveryTime`: `x-opt-delivery-time`, else now plus
    /// `x-opt-delivery-delay`, else 0.
    pub fn scheduled_delivery_time(&self, now: i64) -> i64 {
        if let Some(t) = self
            .annotation(annotation::DELIVERY_TIME)
            .and_then(Value::as_i64)
        {
            return t;
        }
        if let Some(d) = self
            .annotation(annotation::DELIVERY_DELAY)
            .and_then(Value::as_i64)
        {
            return now + d;
        }
        0
    }

    /// The application properties, decoded (`lazyDecodeApplicationProperties`).
    pub fn application_properties(&self) -> Vec<(String, Value)> {
        if let Some(a) = &self.application_properties {
            return a.clone();
        }
        let Some(pos) = self.application_properties_position else {
            return Vec::new();
        };
        let mut d = Decoder::new(&self.data[pos..]);
        match d.read() {
            Ok(Value::Described(_, body)) => string_map(*body).unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    pub fn application_property(&self, key: &str) -> Option<Value> {
        self.application_properties()
            .into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// `getBody`: the first `amqp-sequence`, `amqp-value` or `data`
    /// section after the application properties.
    pub fn body(&self) -> Option<(u64, Value)> {
        self.scan_section(&[section::AMQP_SEQUENCE, section::AMQP_VALUE, section::DATA])
    }

    /// `getFooter`.
    pub fn footer(&self) -> Option<Value> {
        self.scan_section(&[section::FOOTER]).map(|(_, v)| v)
    }

    fn scan_section(&self, codes: &[u64]) -> Option<(u64, Value)> {
        let pos = self.remaining_body_position?;
        let mut d = Decoder::new(&self.data[pos..]);
        while d.has_remaining() {
            let Ok(Value::Described(desc, body)) = d.read() else {
                return None;
            };
            if let Some(c) = section_code(&desc)
                && codes.contains(&c)
            {
                return Some((c, *body));
            }
        }
        None
    }

    /// `getEncodeSize`: the encoding without its delivery annotations.
    pub fn encode_size(&self) -> usize {
        self.data.len() - self.encoded_delivery_annotations_size
    }

    // ------------------------------------------------------------ changing

    pub fn set_message_id(&mut self, id: i64) {
        self.message_id = id;
    }

    /// `setAddress`: also kept in the extra properties.
    pub fn set_address(&mut self, address: SimpleString) {
        self.address = Some(address.clone());
        self.extra_mut()
            .insert(ADDRESS_PROPERTY.into(), PropertyValue::String(address));
    }

    fn extra_mut(&mut self) -> &mut ExtraProperties {
        // createExtraProperties: a new TypedProperties, whose HashMap the
        // first put makes.
        self.extra
            .get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
    }

    fn annotations_mut(&mut self) -> &mut Annotations {
        // getMessageAnnotationsMap(true): a new HashMap.
        self.message_annotations
            .get_or_insert_with(|| JavaMap::new(Kind::Hash(None)))
    }

    /// `setMessageAnnotation`.
    pub fn set_annotation(&mut self, name: &str, value: Value) {
        self.annotations_mut().insert(Symbol::from(name), value);
    }

    /// `removeMessageAnnotation`.
    pub fn remove_annotation(&mut self, name: &str) -> Option<Value> {
        self.message_annotations
            .as_mut()?
            .remove(&Symbol::from(name))
    }

    /// `setExpiration`: an absolute expiry time replaces any TTL.
    pub fn set_expiration(&mut self, expiration: i64) {
        match &mut self.properties {
            Some(p) => {
                p.absolute_expiry_time = (expiration > 0).then_some(Timestamp(expiration));
            }
            None if expiration > 0 => {
                self.properties = Some(Properties {
                    absolute_expiry_time: Some(Timestamp(expiration)),
                    ..Default::default()
                });
            }
            None => {}
        }
        if let Some(h) = &mut self.header {
            h.ttl = None;
        }
        self.expiration = expiration.max(0);
    }

    /// `setRoutingType`.
    pub fn set_routing_type(&mut self, routing_type: Option<i8>) {
        match routing_type {
            None => {
                // removeMessageAnnotation on a missing map does nothing.
                self.remove_annotation(annotation::ROUTING_TYPE);
            }
            Some(t) => self.set_annotation(annotation::ROUTING_TYPE, Value::Byte(t)),
        }
    }

    /// `setBrokerProperty`: an `x-opt-` annotation and an extra property.
    pub fn set_broker_property(&mut self, key: &str, value: PropertyValue) {
        self.set_annotation(&to_annotation_name(key), property_to_value(&value));
        self.extra_mut().insert(SimpleString::from(key), value);
    }

    /// `removeExtraBytesProperty` and the like.
    pub fn remove_extra_property(&mut self, key: &str) -> Option<PropertyValue> {
        self.extra.as_mut()?.remove(&SimpleString::from(key))
    }

    fn application_properties_mut(&mut self) -> &mut Vec<(String, Value)> {
        if self.application_properties.is_none() {
            self.application_properties = Some(self.application_properties());
        }
        self.application_properties.as_mut().expect("just set")
    }

    /// `putObjectProperty` and the typed puts: an application property.
    pub fn put_property(&mut self, key: &str, value: Value) {
        let props = self.application_properties_mut();
        match props.iter_mut().find(|(k, _)| k == key) {
            Some(e) => e.1 = value,
            None => props.push((key.to_owned(), value)),
        }
    }

    /// `removeProperty`.
    pub fn remove_property(&mut self, key: &str) -> Option<Value> {
        let props = self.application_properties_mut();
        let i = props.iter().position(|(k, _)| k == key)?;
        Some(props.remove(i).1)
    }

    /// `setScheduledDeliveryTime`.
    pub fn set_scheduled_delivery_time(&mut self, time: i64) {
        if time > 0 {
            self.set_annotation(annotation::DELIVERY_TIME, Value::Long(time));
            self.remove_annotation(annotation::DELIVERY_DELAY);
        } else {
            self.remove_annotation(annotation::DELIVERY_TIME);
            self.remove_annotation(annotation::DELIVERY_DELAY);
        }
    }

    /// `setDurable`.
    pub fn set_durable(&mut self, durable: bool) {
        self.header.get_or_insert_with(Header::default).durable = Some(durable);
    }

    /// `setPriority`.
    pub fn set_priority(&mut self, priority: u8) {
        self.priority = priority.min(MAX_PRIORITY);
        self.header.get_or_insert_with(Header::default).priority = Some(priority);
    }

    /// `reencode`: writes the working copy of the sections over the
    /// encoding (`encodeMessage`), then scans it again.
    pub fn reencode(&mut self, now: i64) {
        if let (Some(p), Some(a)) = (&mut self.properties, &self.address) {
            p.to = Some(a.to_string_lossy());
        }
        if self.large {
            // AMQPLargeMessage.reencode: the file stays; the sections go
            // out from the record.
            self.reencoded = true;
            return;
        }
        let mut out = Vec::with_capacity(self.data.len() + 64);
        if let Some(h) = &self.header {
            h.encode(&mut out);
        }
        // Delivery annotations are not kept.
        if let Some(ma) = &self.message_annotations {
            encode_annotations(section::MESSAGE_ANNOTATIONS, ma, &mut out);
        }
        if let Some(p) = &self.properties {
            p.encode(&mut out);
        }
        if let Some(ap) = &self.application_properties {
            codec::encode(
                &Value::described(
                    Descriptor::Code(section::APPLICATION_PROPERTIES),
                    Value::Map(
                        ap.iter()
                            .map(|(k, v)| (Value::String(k.clone()), v.clone()))
                            .collect(),
                    ),
                ),
                &mut out,
            );
            if let Some(pos) = self.remaining_body_position {
                out.extend(&self.data[pos..]);
            }
        } else if let Some(pos) = self.application_properties_position {
            out.extend(&self.data[pos..]);
        } else if let Some(pos) = self.remaining_body_position {
            out.extend(&self.data[pos..]);
        }
        self.data = out.into();
        // The broker's working copy goes: what is read next comes from the
        // new encoding.
        if self.scan(now).is_err() {
            tracing::warn!(
                "message {}: re-encoded message does not decode",
                self.message_id
            );
        }
    }

    /// `copy(newID)`: the same encoding with a copy of the extra
    /// properties, scanned afresh.
    pub fn copy(&self, new_id: i64, now: i64) -> Self {
        let mut c = Self::empty(self.message_format, self.data.clone());
        c.message_id = new_id;
        // A large message's copy starts without extra properties.
        c.extra = if self.large {
            None
        } else {
            self.extra.as_ref().map(JavaMap::hash_copy)
        };
        if self.large {
            if c.scan(now).is_err() {
                return self.clone();
            }
            c.make_large();
            return c;
        }
        if c.scan(now).is_err() {
            // The original scanned; the bytes are the same.
            return self.clone();
        }
        c
    }

    // --------------------------------------------------------- persisting

    /// The record `AMQPMessagePersisterV3` writes.
    pub fn record(&self) -> AmqpMessageRecord {
        let extra = self.extra.as_ref().map(|e| {
            let mut p = TypedProperties::new();
            for (k, v) in e.iter() {
                p.insert(k.clone(), v.clone());
            }
            if e.is_empty() {
                // An empty map still encodes as NOT_NULL with no entries.
                p = empty_properties();
            }
            p
        });
        AmqpMessageRecord {
            persister_id: persister::AMQP_MESSAGE_V3,
            message_id: self.message_id,
            message_format: self.message_format,
            address: self.address(),
            data: self.data.to_vec(),
            extra_properties: extra,
            expiration: Some(self.expiration),
        }
    }

    pub fn persisted(&self) -> PersistedMessage {
        if self.large {
            return PersistedMessage::AmqpLarge(self.large_record());
        }
        PersistedMessage::Amqp(self.record())
    }

    /// The record `AMQPLargeMessagePersister` writes.
    pub fn large_record(&self) -> AmqpLargeMessageRecord {
        AmqpLargeMessageRecord {
            message_id: self.message_id,
            durable: self.is_durable(),
            message_format: self.message_format,
            address: self.address(),
            extra_properties: self.record().extra_properties,
            saved_encoding: self.saved_encoding(),
        }
    }

    /// `getSendBuffer(deliveryCount)`: the encoding a consumer receives.
    /// A redelivery carries a header with the delivery count, and delivery
    /// annotations are dropped.
    pub fn send_buffer(&self, delivery_count: i32) -> Vec<u8> {
        if self.large {
            return self.large_send_buffer();
        }
        if delivery_count <= 1 && self.delivery_annotations.is_none() {
            return self.data.to_vec();
        }
        // createDeliveryCopy
        let mut out = Vec::with_capacity(self.data.len() + 16);
        let header = match &self.header {
            Some(h) => Some(h.clone()),
            None if delivery_count > 1 => Some(Header::default()),
            None => None,
        };
        if let Some(mut h) = header {
            h.delivery_count = Some((delivery_count - 1).max(0) as u32);
            h.encode(&mut out);
        }
        let skip = self.encoded_header_size + self.encoded_delivery_annotations_size;
        out.extend(&self.data[skip.min(self.data.len())..]);
        out
    }
}

impl AmqpMessage {
    /// `AMQPLargeMessageWriter`: the header as stored (its delivery count
    /// is not updated), then the file after the header and delivery
    /// annotations; a re-encoded message's annotations, properties and
    /// application properties come from the record.
    fn large_send_buffer(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.data.len() + 64);
        if let Some(h) = &self.header {
            h.encode(&mut out);
        }
        let mut position = self.encoded_header_size + self.encoded_delivery_annotations_size;
        if self.reencoded
            && let Some(body) = self.remaining_body_position
        {
            if let Some(ma) = &self.message_annotations {
                encode_annotations(section::MESSAGE_ANNOTATIONS, ma, &mut out);
            }
            if let Some(p) = &self.properties {
                p.encode(&mut out);
            }
            if let Some(ap) = &self.application_properties {
                encode_section(
                    section::APPLICATION_PROPERTIES,
                    Value::Map(
                        ap.iter()
                            .map(|(k, v)| (Value::String(k.clone()), v.clone()))
                            .collect(),
                    ),
                    &mut out,
                );
            }
            position = body;
        }
        out.extend(&self.data[position.min(self.data.len())..]);
        out
    }
}

/// An empty `TypedProperties` that is not null.
fn empty_properties() -> TypedProperties {
    let mut p = TypedProperties::new();
    let k = SimpleString::from("x");
    p.insert(k.clone(), PropertyValue::Null);
    p.remove(&k);
    p
}

/// The section descriptor code, from a code or a name.
pub fn section_code(d: &Descriptor) -> Option<u64> {
    match d {
        Descriptor::Code(c) => Some(*c),
        Descriptor::Name(n) => Some(match n.as_str() {
            "amqp:header:list" => section::HEADER,
            "amqp:delivery-annotations:map" => section::DELIVERY_ANNOTATIONS,
            "amqp:message-annotations:map" => section::MESSAGE_ANNOTATIONS,
            "amqp:properties:list" => section::PROPERTIES,
            "amqp:application-properties:map" => section::APPLICATION_PROPERTIES,
            "amqp:data:binary" => section::DATA,
            "amqp:amqp-sequence:list" => section::AMQP_SEQUENCE,
            "amqp:amqp-value:*" => section::AMQP_VALUE,
            "amqp:footer:map" => section::FOOTER,
            _ => return None,
        }),
    }
}

/// A map with symbol keys (annotations).
fn symbol_map(v: Value) -> Result<Vec<(Symbol, Value)>> {
    match v {
        Value::Map(entries) => Ok(entries
            .into_iter()
            .filter_map(|(k, v)| match k {
                Value::Symbol(s) => Some((s, v)),
                Value::String(s) => Some((Symbol(s), v)),
                _ => None,
            })
            .collect()),
        Value::Null => Ok(Vec::new()),
        other => codec::invalid(format!("annotations {other:?}")),
    }
}

/// A map with string keys (application properties).
fn string_map(v: Value) -> Result<Vec<(String, Value)>> {
    match v {
        Value::Map(entries) => Ok(entries
            .into_iter()
            .filter_map(|(k, v)| match k {
                Value::String(s) => Some((s, v)),
                Value::Symbol(s) => Some((s.0, v)),
                _ => None,
            })
            .collect()),
        Value::Null => Ok(Vec::new()),
        other => codec::invalid(format!("application properties {other:?}")),
    }
}

/// Writes annotations in their Java map's order.
fn encode_annotations(code: u64, map: &Annotations, out: &mut Vec<u8>) {
    codec::encode(
        &Value::described(
            Descriptor::Code(code),
            Value::Map(
                map.iter()
                    .map(|(k, v)| (Value::Symbol(k.clone()), v.clone()))
                    .collect(),
            ),
        ),
        out,
    );
}

/// A broker property as an annotation value (`setMessageAnnotation`:
/// `SimpleString` becomes `String`).
pub fn property_to_value(v: &PropertyValue) -> Value {
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

/// Encodes one section.
pub fn encode_section(code: u64, body: Value, out: &mut Vec<u8>) {
    codec::encode(&Value::described(Descriptor::Code(code), body), out);
}
