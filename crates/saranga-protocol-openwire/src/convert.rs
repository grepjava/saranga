//! Between OpenWire and Core messages: Artemis's
//! `OpenWireMessageConverter` (`inbound` and `createMessageDispatch`).

use std::io::{Read, Write as _};

use saranga_buffer::constants as dc;
use saranga_buffer::{JString, PropertyValue, Reader, SimpleString, TypedProperties, Writer};
use saranga_message::{CoreMessage, message_type};
use saranga_storage::java;

use crate::WireFormat;
use crate::codec::{CodecError, Input, Structure};
use crate::commands::Destination;
use crate::commands::{
    BrokerId, ConsumerId, DataStructure, Message, MessageDispatch, MessageId, ProducerId,
};
use crate::destination::QUEUE_TYPE;
use crate::primitive::{self as p, Primitive, PrimitiveMap};

const AMQ_PREFIX: &str = "__HDR_";
pub const AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY: &str = "__HDR_dlqDeliveryFailureCause";
pub const AMQ_MSG_ARRIVAL: &str = "__HDR_ARRIVAL";
pub const AMQ_MSG_BROKER_IN_TIME: &str = "__HDR_BROKER_IN_TIME";
pub const AMQ_MSG_BROKER_PATH: &str = "__HDR_BROKER_PATH";
pub const AMQ_MSG_CLUSTER: &str = "__HDR_CLUSTER";
pub const AMQ_MSG_COMMAND_ID: &str = "__HDR_COMMAND_ID";
pub const AMQ_MSG_DATASTRUCTURE: &str = "__HDR_DATASTRUCTURE";
pub const AMQ_MSG_MESSAGE_ID: &str = "__HDR_MESSAGE_ID";
pub const AMQ_MSG_ORIG_DESTINATION: &str = "__HDR_ORIG_DESTINATION";
pub const AMQ_MSG_PRODUCER_ID: &str = "__HDR_PRODUCER_ID";
pub const AMQ_MSG_REPLY_TO: &str = "__HDR_REPLY_TO";
pub const AMQ_MSG_USER_ID: &str = "__HDR_USER_ID";
pub const AMQ_MSG_DROPPABLE: &str = "__HDR_DROPPABLE";
pub const AMQ_MSG_COMPRESSED: &str = "__HDR_COMPRESSED";
pub const JMS_TYPE_PROPERTY: &str = "JMSType";
pub const JMS_CORRELATION_ID_PROPERTY: &str = "JMSCorrelationID";
pub const JMS_REPLY_TO: &str = "JMSReplyTo";
/// `ScheduledMessage.AMQ_SCHEDULED_DELAY`.
pub const AMQ_SCHEDULED_DELAY: &str = "AMQ_SCHEDULED_DELAY";
/// `ActiveMQMessage.DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY`.
pub const DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY: &str = "dlqDeliveryFailureCause";
const HDR_NOTIFICATION_TYPE: &str = "_AMQ_NotifType";
const HDR_LAST_VALUE_NAME: &str = "_AMQ_LVQ_NAME";
const HDR_INGRESS_TIMESTAMP: &str = "_AMQ_INGRESS_TIMESTAMP";
const HDR_GROUP_ID: &str = "_AMQ_GROUP_ID";
const HDR_GROUP_SEQUENCE: &str = "_AMQ_GROUP_SEQUENCE";
const HDR_SCHEDULED_DELIVERY_TIME: &str = "_AMQ_SCHED_DELIVERY";
const JMSX_DELIVERY_COUNT: &str = "JMSXDeliveryCount";

/// What Artemis would have thrown while converting.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConvertError(pub String);

impl From<CodecError> for ConvertError {
    fn from(e: CodecError) -> Self {
        ConvertError(e.to_string())
    }
}

type R<T> = Result<T, ConvertError>;

fn err(s: impl Into<String>) -> ConvertError {
    ConvertError(s.into())
}

fn put(core: &mut CoreMessage, key: &str, v: PropertyValue) {
    core.properties.insert(SimpleString::from(key), v);
}

fn put_string(core: &mut CoreMessage, key: &str, v: &str) {
    put(core, key, PropertyValue::String(SimpleString::from(v)));
}

/// `toCoreType`.
pub fn to_core_type(data_type: u8) -> R<u8> {
    Ok(match data_type {
        29 => return Err(err("We don't support BLOB type yet!")),
        24 => message_type::BYTES,
        25 => message_type::MAP,
        26 => message_type::OBJECT,
        27 => message_type::STREAM,
        28 => message_type::TEXT,
        23 => message_type::DEFAULT,
        t => return Err(err(format!("Unknown ActiveMQ Artemis message type: {t}"))),
    })
}

/// `InflaterInputStream` over everything.
fn inflate(data: &[u8]) -> R<Vec<u8>> {
    let mut out = Vec::new();
    flate2::read::ZlibDecoder::new(data)
        .read_to_end(&mut out)
        .map_err(|e| err(e.to_string()))?;
    Ok(out)
}

/// `DeflaterOutputStream(out, true)`: written and sync-flushed, then
/// closed (`finish`) or, where Artemis takes the bytes before closing it,
/// left open.
fn deflate(data: &[u8], finish: bool) -> Vec<u8> {
    let mut c = flate2::Compress::new(flate2::Compression::default(), true);
    let mut out = Vec::new();
    compress_all(&mut c, data, flate2::FlushCompress::Sync, &mut out);
    if finish {
        compress_all(&mut c, &[], flate2::FlushCompress::Finish, &mut out);
    }
    out
}

/// Feeds all of `input` and flushes, growing `out` as needed.
fn compress_all(
    c: &mut flate2::Compress,
    input: &[u8],
    flush: flate2::FlushCompress,
    out: &mut Vec<u8>,
) {
    let start = c.total_in();
    loop {
        out.reserve(4096);
        let done = (c.total_in() - start) as usize;
        let status = c.compress_vec(&input[done..], out, flush);
        let finished = matches!(status, Ok(flate2::Status::StreamEnd) | Err(_));
        // Done when all input is in and the output did not fill up.
        if finished
            || ((c.total_in() - start) as usize == input.len() && out.len() < out.capacity())
        {
            break;
        }
    }
}

/// `Message.getProperties()`: the marshalled properties read back (an
/// empty map when there are none).
pub fn message_properties(msg: &Message) -> R<PrimitiveMap> {
    match &msg.marshalled_properties {
        None => Ok(PrimitiveMap::new()),
        Some(b) => p::read_map(&mut Input::new(b))?
            .ok_or_else(|| err("NullPointerException: null properties")),
    }
}

/// `CoreMessage.putObjectProperty` of a value read from a primitive map;
/// what a property cannot hold goes in as its `toString()`.
fn put_primitive(core: &mut CoreMessage, key: &str, v: &Primitive) {
    let pv = match v {
        Primitive::Map(_) | Primitive::List(_) => {
            PropertyValue::String(SimpleString::from(v.java_string()))
        }
        other => primitive_property(other).expect("not a map or list"),
    };
    put(core, key, pv);
}

/// `TypedProperties.setObjectProperty`: `None` for a map or a list.
fn primitive_property(v: &Primitive) -> Option<PropertyValue> {
    Some(match v {
        Primitive::Null => PropertyValue::Null,
        Primitive::Bool(b) => PropertyValue::Bool(*b),
        Primitive::Byte(b) => PropertyValue::Byte(*b),
        Primitive::Char(c) => PropertyValue::Char(*c),
        Primitive::Short(s) => PropertyValue::Short(*s),
        Primitive::Int(i) => PropertyValue::Int(*i),
        Primitive::Long(l) => PropertyValue::Long(*l),
        Primitive::Float(f) => PropertyValue::Float(*f),
        Primitive::Double(d) => PropertyValue::Double(*d),
        Primitive::String(s) => PropertyValue::String(SimpleString::from(s.as_str())),
        Primitive::Bytes(b) => PropertyValue::Bytes(b.clone()),
        Primitive::Map(_) | Primitive::List(_) => return None,
    })
}

fn join_brokers(ids: &[BrokerId]) -> String {
    ids.iter()
        .map(|b| b.value.clone().unwrap_or_else(|| "null".into()))
        .collect::<Vec<_>>()
        .join(",")
}

/// `ProducerId.toString()`.
pub fn producer_id_string(id: &ProducerId) -> String {
    format!(
        "{}:{}:{}",
        id.connection_id.as_deref().unwrap_or("null"),
        id.session_id,
        id.value
    )
}

/// `MessageId.toString()` of an id read from the wire.
pub fn message_id_string(id: &MessageId) -> String {
    match &id.text_view {
        Some(t) if t.starts_with("ID:") => t.clone(),
        Some(t) => format!("ID:{t}"),
        None => format!(
            "{}:{}",
            id.producer_id
                .as_ref()
                .map_or_else(|| "null".into(), producer_id_string),
            id.producer_sequence_id
        ),
    }
}

/// `Long.parseLong`.
fn parse_long(s: &str) -> R<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err(format!("For input string: \"{s}\"")));
    }
    s.parse()
        .map_err(|_| err(format!("For input string: \"{s}\"")))
}

/// `new ProducerId(String)`.
pub fn parse_producer_id(key: &str) -> R<ProducerId> {
    let mut id = ProducerId::default();
    let mut key = key;
    if let Some(p) = key.rfind(':') {
        id.value = parse_long(&key[p + 1..])?;
        key = &key[..p];
    }
    if let Some(p) = key.rfind(':') {
        id.session_id = parse_long(&key[p + 1..])?;
        key = &key[..p];
    }
    id.connection_id = Some(key.to_owned());
    Ok(id)
}

/// `new MessageId(String)`.
pub fn parse_message_id(key: &str) -> R<MessageId> {
    let p = key.rfind(':').ok_or_else(|| err("NumberFormatException"))?;
    Ok(MessageId {
        producer_sequence_id: parse_long(&key[p + 1..])?,
        producer_id: Some(parse_producer_id(&key[..p])?),
        ..Default::default()
    })
}

/// `inbound`: the Core message for a message an OpenWire producer sent.
/// The address and routing are the caller's.
pub fn inbound(msg: &Message, wf: &mut WireFormat, now: i64) -> R<CoreMessage> {
    let mut core = CoreMessage::default();
    if let Some(t) = &msg.type_ {
        put_string(&mut core, JMS_TYPE_PROPERTY, t);
    }
    core.durable = msg.persistent;
    core.expiration = msg.expiration;
    core.priority = msg.priority as u8;
    core.timestamp = msg.timestamp;
    let core_type = to_core_type(msg.data_type)?;
    core.message_type = core_type;

    let mut body = Writer::new();
    match &msg.content {
        None => match core_type {
            message_type::TEXT => {
                body.nullable_string(None);
            }
            message_type::MAP => {
                body.u8(dc::NULL);
            }
            _ => {}
        },
        Some(contents) => {
            let compressed = msg.compressed;
            if compressed {
                put(&mut core, AMQ_MSG_COMPRESSED, PropertyValue::Bool(true));
            }
            match core_type {
                message_type::TEXT => {
                    let data = if compressed {
                        inflate(contents)?
                    } else {
                        contents.clone()
                    };
                    let text = p::read_utf8(&mut Input::new(&data))?;
                    body.nullable_simple_string(text.map(SimpleString::from).as_ref());
                }
                message_type::MAP => {
                    let data = if compressed {
                        inflate(contents)?
                    } else {
                        contents.clone()
                    };
                    let map = p::read_map(&mut Input::new(&data))?
                        .ok_or_else(|| err("NullPointerException: null map"))?;
                    let mut entries = Vec::new();
                    for (k, v) in map.iter() {
                        let pv = primitive_property(v).ok_or_else(|| {
                            err(format!("{} is not a valid property type", java_class(v)))
                        })?;
                        entries.push((SimpleString::from(k.as_str()), pv));
                    }
                    // A new TypedProperties: a HashMap<SimpleString, _>.
                    let mut props = TypedProperties::new();
                    for (k, v) in in_hash_map_order(entries, &[]) {
                        props.insert(k, v);
                    }
                    props.encode(&mut body);
                }
                message_type::OBJECT => {
                    let data = if compressed {
                        inflate(contents)?
                    } else {
                        contents.clone()
                    };
                    body.i32(data.len() as i32).bytes(&data);
                }
                message_type::STREAM => {
                    let data = if compressed {
                        inflate(contents)?
                    } else {
                        contents.clone()
                    };
                    write_stream(&data, &mut body)?;
                }
                message_type::BYTES => {
                    let data = if compressed {
                        inflate_bytes_message(contents)?
                    } else {
                        contents.clone()
                    };
                    body.bytes(&data);
                }
                _ => {
                    let data = if compressed {
                        inflate(contents)?
                    } else {
                        contents.clone()
                    };
                    body.bytes(&data);
                }
            }
        }
    }
    core.body = body.into_vec();

    put(&mut core, AMQ_MSG_ARRIVAL, PropertyValue::Long(msg.arrival));
    put(
        &mut core,
        AMQ_MSG_BROKER_IN_TIME,
        PropertyValue::Long(msg.broker_in_time),
    );
    if let Some(b) = &msg.broker_path {
        put_string(&mut core, AMQ_MSG_BROKER_PATH, &join_brokers(b));
    }
    if let Some(c) = &msg.cluster {
        put_string(&mut core, AMQ_MSG_CLUSTER, &join_brokers(c));
    }
    put(
        &mut core,
        AMQ_MSG_COMMAND_ID,
        PropertyValue::Int(msg.command_id),
    );
    if let Some(c) = &msg.correlation_id {
        put_string(&mut core, JMS_CORRELATION_ID_PROPERTY, c);
    }
    if let Some(ds) = &msg.data_structure {
        // Marshalled with the connection's own format, size prefix, cache
        // and all.
        let bytes = wf.marshal(Some(ds))?;
        put(
            &mut core,
            AMQ_MSG_DATASTRUCTURE,
            PropertyValue::Bytes(bytes),
        );
    }
    if let Some(g) = &msg.group_id {
        put_string(&mut core, HDR_GROUP_ID, g);
    }
    put(
        &mut core,
        HDR_GROUP_SEQUENCE,
        PropertyValue::Int(msg.group_sequence),
    );
    if let Some(id) = &msg.message_id {
        put_string(&mut core, AMQ_MSG_MESSAGE_ID, &message_id_string(id));
    }
    core.user_id = Some(saranga_client::uuid::time_based());
    if let Some(id) = &msg.producer_id {
        put_string(&mut core, AMQ_MSG_PRODUCER_ID, &producer_id_string(id));
    }
    let props = message_properties(msg)?;
    for (k, v) in props.iter() {
        put_primitive(&mut core, k, v);
    }
    if let Some(r) = &msg.reply_to {
        put_string(
            &mut core,
            JMS_REPLY_TO,
            &format!("{}{}", r.qualified_prefix(), r.physical_name()),
        );
    }
    if let Some(u) = &msg.user_id {
        put_string(&mut core, AMQ_MSG_USER_ID, u);
    }
    put(
        &mut core,
        AMQ_MSG_DROPPABLE,
        PropertyValue::Bool(msg.droppable),
    );
    if let Some(d) = &msg.original_destination {
        put_string(&mut core, AMQ_MSG_ORIG_DESTINATION, &d.qualified_name());
    }
    let mut removed = Vec::new();
    if let Some(Primitive::Long(delay)) = props.get(AMQ_SCHEDULED_DELAY) {
        put(
            &mut core,
            HDR_SCHEDULED_DELIVERY_TIME,
            PropertyValue::Long(now + delay),
        );
        let key = SimpleString::from(AMQ_SCHEDULED_DELAY);
        if core.properties.remove(&key).is_some() {
            removed.push(key);
        }
    }
    // Artemis keeps them in a new HashMap: encode them in its order.
    let entries = core.properties.entries().to_vec();
    let mut ordered = TypedProperties::new();
    for (k, v) in in_hash_map_order(entries, &removed) {
        ordered.insert(k, v);
    }
    core.properties = ordered;
    Ok(core)
}

/// Entries put in this order into a `new HashMap<SimpleString, _>()`, as
/// it iterates them. `removed` were put after them all and taken out
/// again: they still grew the table.
fn in_hash_map_order(
    entries: Vec<(SimpleString, PropertyValue)>,
    removed: &[SimpleString],
) -> Vec<(SimpleString, PropertyValue)> {
    let mut hashes: Vec<i32> = entries
        .iter()
        .map(|(k, _)| java::simple_string_hash(k))
        .collect();
    hashes.extend(removed.iter().map(java::simple_string_hash));
    let n = entries.len();
    let mut slots: Vec<Option<(SimpleString, PropertyValue)>> =
        entries.into_iter().map(Some).collect();
    java::hash_map_order(&hashes, None)
        .into_iter()
        .filter(|&i| i < n)
        .filter_map(|i| slots[i].take())
        .collect()
}

fn java_class(v: &Primitive) -> &'static str {
    match v {
        Primitive::Map(_) => "class java.util.HashMap",
        Primitive::List(_) => "class java.util.ArrayList",
        _ => "class java.lang.Object",
    }
}

/// `writeCompressedBytesType`: an int of the uncompressed length, then
/// the compressed bytes.
fn inflate_bytes_message(contents: &[u8]) -> R<Vec<u8>> {
    if contents.len() < 4 {
        return Err(err("java.io.EOFException"));
    }
    let length = i32::from_be_bytes(contents[..4].try_into().unwrap());
    if length < 0 {
        return Err(err("java.lang.NegativeArraySizeException"));
    }
    let mut d = flate2::Decompress::new(true);
    let mut out = vec![0u8; length as usize];
    d.decompress(&contents[4..], &mut out, flate2::FlushDecompress::None)
        .map_err(|e| err(e.to_string()))?;
    out.truncate(d.total_out() as usize);
    Ok(out)
}

/// `writeStreamType`: OpenWire stream items as Core's.
fn write_stream(data: &[u8], body: &mut Writer) -> R<()> {
    let mut input = Input::new(data);
    let eof = |_| err("java.io.EOFException");
    while input.position() < data.len() {
        match input.u8()? {
            p::BOOLEAN_TYPE => {
                let v = input.u8().map_err(eof)? != 0;
                body.u8(dc::BOOLEAN).bool(v);
            }
            p::BYTE_TYPE => {
                let v = input.u8().map_err(eof)?;
                body.u8(dc::BYTE).u8(v);
            }
            p::BYTE_ARRAY_TYPE => {
                let len = input.i32().map_err(eof)?;
                if len < 0 {
                    return Err(err("java.lang.NegativeArraySizeException"));
                }
                // read() fills what is there; the rest stays zero.
                let n = (len as usize).min(data.len() - input.position());
                let mut bytes = input.take(n)?.to_vec();
                bytes.resize(len as usize, 0);
                body.u8(dc::BYTES).i32(len).bytes(&bytes);
            }
            p::CHAR_TYPE => {
                let v = input.i16().map_err(eof)?;
                body.u8(dc::CHAR).i16(v);
            }
            p::DOUBLE_TYPE => {
                let v = input.i64().map_err(eof)?;
                body.u8(dc::DOUBLE).i64(v);
            }
            p::FLOAT_TYPE => {
                let v = input.i32().map_err(eof)?;
                body.u8(dc::FLOAT).i32(v);
            }
            p::INTEGER_TYPE => {
                let v = input.i32().map_err(eof)?;
                body.u8(dc::INT).i32(v);
            }
            p::LONG_TYPE => {
                let v = input.i64().map_err(eof)?;
                body.u8(dc::LONG).i64(v);
            }
            p::SHORT_TYPE => {
                let v = input.i16().map_err(eof)?;
                body.u8(dc::SHORT).i16(v);
            }
            p::STRING_TYPE => {
                let s = input.utf()?;
                body.u8(dc::STRING)
                    .nullable_string(Some(&JString::from(s.as_str())));
            }
            p::BIG_STRING_TYPE => {
                let s = p::read_utf8(&mut input)?;
                body.u8(dc::STRING)
                    .nullable_string(s.as_deref().map(JString::from).as_ref());
            }
            p::NULL => {
                body.u8(dc::STRING).nullable_string(None);
            }
            // Something it does not know: skipped, a byte at a time.
            _ => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- outbound

/// What `createMessageDispatch` needs to know about the consumer.
pub struct DispatchTarget<'a> {
    pub consumer_id: &'a ConsumerId,
    /// `AMQConsumer.getOpenwireDestination()`.
    pub destination: &'a Destination,
    /// The broker's node id (`UUID.toString()`).
    pub server_node_id: &'a str,
    /// Whether Artemis would hold the message's properties as decoded
    /// (`new HashMap<>(n, 1.0f)`, from the wire or the journal) rather
    /// than as put (a new `HashMap`, kept in Saranga in its order).
    pub decoded_properties: bool,
}

fn property<'a>(core: &'a CoreMessage, key: &str) -> Option<&'a PropertyValue> {
    core.properties.get(&SimpleString::from(key))
}

fn string_of(v: &PropertyValue) -> Option<String> {
    Some(match v {
        PropertyValue::Null => return None,
        PropertyValue::String(s) => s.to_string_lossy(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Byte(b) => b.to_string(),
        PropertyValue::Short(b) => b.to_string(),
        PropertyValue::Int(b) => b.to_string(),
        PropertyValue::Long(b) => b.to_string(),
        PropertyValue::Float(f) => java::float(*f),
        PropertyValue::Double(f) => java::double(*f),
        PropertyValue::Char(c) => java::utf16([*c]),
        PropertyValue::Bytes(_) => "[B@0".into(),
    })
}

fn simple_string_property(core: &CoreMessage, key: &str) -> Option<String> {
    match property(core, key) {
        Some(PropertyValue::String(s)) => Some(s.to_string_lossy()),
        _ => None,
    }
}

/// `String.split(",")`: trailing empty strings dropped.
fn java_split(s: &str) -> Vec<String> {
    let mut parts: Vec<String> = s.split(',').map(str::to_owned).collect();
    while parts.len() > 1 && parts.last().is_some_and(String::is_empty) {
        parts.pop();
    }
    if parts.len() == 1 && parts[0].is_empty() && !s.is_empty() {
        parts.clear();
    }
    parts
}

fn broker_ids(s: &str) -> Vec<BrokerId> {
    java_split(s)
        .into_iter()
        .map(|v| BrokerId { value: Some(v) })
        .collect()
}

/// Core's properties in the order Artemis's `HashMap` iterates them: as
/// kept for properties put in this broker, re-hashed into a table of
/// `n` for decoded ones.
fn properties_in_java_order(
    core: &CoreMessage,
    decoded: bool,
) -> Vec<&(SimpleString, PropertyValue)> {
    let entries = core.properties.entries();
    if !decoded {
        return entries.iter().collect();
    }
    let hashes: Vec<i32> = entries
        .iter()
        .map(|(k, _)| java::simple_string_hash(k))
        .collect();
    java::hash_map_order(&hashes, Some((entries.len(), 1.0)))
        .into_iter()
        .map(|i| &entries[i])
        .collect()
}

/// The `ActiveMQMessage` being built: its own fields and, once a
/// property is set, its property map.
struct Outgoing {
    msg: Message,
    properties: Option<PrimitiveMap>,
}

impl Outgoing {
    fn set_property(&mut self, name: &str, v: Primitive) {
        self.properties
            .get_or_insert_with(PrimitiveMap::new)
            .insert(name.to_owned(), v);
    }

    /// `ActiveMQMessage.setObjectProperty`: JMS header names set the
    /// header; a value that does not convert is skipped (Artemis logs it).
    fn set_object_property(&mut self, name: &str, v: Primitive) {
        if matches!(v, Primitive::Null) {
            self.set_property(name, v);
            return;
        }
        let m = &mut self.msg;
        match name {
            "JMSXDeliveryCount" => {
                if let Some(i) = to_int(&v) {
                    m.redelivery_counter = i.wrapping_sub(1);
                }
            }
            "JMSXGroupID" => {
                if let Some(s) = to_string(&v) {
                    m.group_id = Some(s);
                }
            }
            "JMSXGroupSeq" => {
                if let Some(i) = to_int(&v) {
                    m.group_sequence = i;
                }
            }
            "JMSCorrelationID" => {
                if let Some(s) = to_string(&v) {
                    m.correlation_id = Some(s);
                }
            }
            "JMSDeliveryMode" => {
                let mode = match &v {
                    Primitive::String(s) if s.eq_ignore_ascii_case("PERSISTENT") => Some(2),
                    Primitive::String(s) if s.eq_ignore_ascii_case("NON_PERSISTENT") => Some(1),
                    Primitive::Bool(b) => Some(if *b { 2 } else { 1 }),
                    other => to_int(other),
                };
                if let Some(mode) = mode {
                    m.persistent = mode == 2;
                }
            }
            "JMSExpiration" => {
                if let Some(l) = to_long(&v) {
                    m.expiration = l;
                }
            }
            "JMSPriority" => {
                if let Some(i) = to_int(&v) {
                    m.priority = i as i8;
                }
            }
            "JMSRedelivered" => {
                if let Some(b) = to_bool(&v) {
                    m.redelivery_counter = b as i32;
                }
            }
            "JMSReplyTo" => {
                if let Primitive::String(s) = &v {
                    m.reply_to = Destination::create(s, QUEUE_TYPE);
                }
            }
            "JMSTimestamp" => {
                if let Some(l) = to_long(&v) {
                    m.timestamp = l;
                }
            }
            "JMSType" => {
                if let Some(s) = to_string(&v) {
                    m.type_ = Some(s);
                }
            }
            _ => self.set_property(name, v),
        }
    }
}

/// `TypeConversionSupport.convert(value, Integer.class)`.
fn to_int(v: &Primitive) -> Option<i32> {
    match v {
        Primitive::Int(i) => Some(*i),
        Primitive::Byte(b) => Some(*b as i32),
        Primitive::Short(s) => Some(*s as i32),
        Primitive::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn to_long(v: &Primitive) -> Option<i64> {
    match v {
        Primitive::Long(l) => Some(*l),
        Primitive::Int(i) => Some(*i as i64),
        Primitive::Byte(b) => Some(*b as i64),
        Primitive::Short(s) => Some(*s as i64),
        Primitive::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn to_bool(v: &Primitive) -> Option<bool> {
    match v {
        Primitive::Bool(b) => Some(*b),
        Primitive::String(s) => Some(s.eq_ignore_ascii_case("true")),
        _ => None,
    }
}

fn to_string(v: &Primitive) -> Option<String> {
    match v {
        Primitive::String(s) => Some(s.clone()),
        Primitive::Bool(_)
        | Primitive::Byte(_)
        | Primitive::Short(_)
        | Primitive::Int(_)
        | Primitive::Long(_)
        | Primitive::Float(_)
        | Primitive::Double(_) => Some(v.java_string()),
        _ => None,
    }
}

/// A Core property as `setAMQMsgObjectProperties` hands it to the message.
fn outgoing_value(name: &str, v: &PropertyValue) -> Primitive {
    match v {
        PropertyValue::String(s) => Primitive::String(s.to_string_lossy()),
        PropertyValue::Bytes(b) => Primitive::String(bytes_to_hex(b)),
        PropertyValue::Long(l) if name == JMSX_DELIVERY_COUNT => Primitive::Int(*l as i32),
        PropertyValue::Null => Primitive::Null,
        PropertyValue::Bool(b) => Primitive::Bool(*b),
        PropertyValue::Byte(b) => Primitive::Byte(*b),
        PropertyValue::Short(s) => Primitive::Short(*s),
        PropertyValue::Int(i) => Primitive::Int(*i),
        PropertyValue::Long(l) => Primitive::Long(*l),
        PropertyValue::Float(f) => Primitive::Float(*f),
        PropertyValue::Double(d) => Primitive::Double(*d),
        PropertyValue::Char(c) => Primitive::Char(*c),
    }
}

/// `ByteUtil.bytesToHex`.
fn bytes_to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

/// `createMessageDispatch`: what a consumer gets for a Core message
/// delivered for the `delivery_count`th time.
pub fn message_dispatch(
    core: &CoreMessage,
    delivery_count: i32,
    target: &DispatchTarget<'_>,
    wf: &mut WireFormat,
    delivery_sequence_id: i64,
) -> R<MessageDispatch> {
    let mut message = to_amq_message(core, delivery_count, target, wf)?;
    if let Some(id) = message.message_id.as_mut() {
        id.broker_sequence_id = delivery_sequence_id;
    }
    let destination = message.destination.clone();
    Ok(MessageDispatch {
        consumer_id: Some(target.consumer_id.clone()),
        redelivery_counter: delivery_count - 1,
        message: Some(Box::new(message)),
        destination,
        ..Default::default()
    })
}

fn to_amq_message(
    core: &CoreMessage,
    delivery_count: i32,
    target: &DispatchTarget<'_>,
    wf: &mut WireFormat,
) -> R<Message> {
    let compressed = matches!(
        property(core, AMQ_MSG_COMPRESSED),
        Some(PropertyValue::Bool(true))
    );
    let mut r = Reader::new(&core.body);
    let (data_type, content) = match core.message_type {
        message_type::BYTES => {
            let mut b = core.body.clone();
            if compressed {
                b = deflate_bytes_message(&b);
            }
            (24, Some(b))
        }
        message_type::MAP => {
            let bytes = if core.body.is_empty() {
                None
            } else {
                let props = TypedProperties::decode(&mut r).map_err(|e| err(e.to_string()))?;
                let entries = props.entries();
                // getMap(): a HashMap<String, Object>(size) filled in the
                // order the decoded HashMap<SimpleString, _>(n, 1.0) iterates.
                let hashes: Vec<i32> = entries
                    .iter()
                    .map(|(k, _)| java::simple_string_hash(k))
                    .collect();
                let mut map = PrimitiveMap::with_capacity(entries.len());
                for i in java::hash_map_order(&hashes, Some((entries.len(), 1.0))) {
                    let (k, v) = &entries[i];
                    map.insert(k.to_string_lossy(), map_value(v));
                }
                let mut out = Vec::new();
                p::write_map(&mut out, Some(&map))?;
                Some(if compressed { deflate(&out, true) } else { out })
            };
            (25, bytes)
        }
        message_type::OBJECT => {
            let bytes = if core.body.is_empty() {
                None
            } else {
                let len = r.i32().map_err(|e| err(e.to_string()))?;
                if len < 0 {
                    return Err(err("java.lang.NegativeArraySizeException"));
                }
                let b = r
                    .bytes(len as usize)
                    .map_err(|e| err(e.to_string()))?
                    .to_vec();
                Some(if compressed { deflate(&b, true) } else { b })
            };
            (26, bytes)
        }
        message_type::STREAM => {
            let out = read_core_stream(&mut r)?;
            (27, Some(if compressed { deflate(&out, true) } else { out }))
        }
        message_type::TEXT => {
            let text = r.nullable_simple_string().map_err(|e| err(e.to_string()))?;
            let bytes = text.map(|t| {
                let mut out = Vec::new();
                p::write_utf8(&mut out, Some(&t.to_string_lossy()));
                if compressed {
                    deflate(&out, false)
                } else {
                    out
                }
            });
            (28, bytes)
        }
        message_type::DEFAULT => {
            let b = core.body.clone();
            (23, Some(if compressed { deflate(&b, false) } else { b }))
        }
        t => return Err(err(format!("Unknown message type: {t}"))),
    };

    let mut out = Outgoing {
        msg: Message {
            data_type,
            ..Default::default()
        },
        properties: None,
    };
    let m = &mut out.msg;
    if let Some(t) = property(core, JMS_TYPE_PROPERTY).and_then(string_of) {
        m.type_ = Some(t);
    }
    m.persistent = core.durable;
    m.expiration = core.expiration;
    m.priority = core.priority as i8;
    m.timestamp = core.timestamp;
    m.broker_in_time = match property(core, AMQ_MSG_BROKER_IN_TIME) {
        Some(PropertyValue::Long(l)) => *l,
        _ => 0,
    };
    m.compressed = compressed;
    m.arrival = match property(core, AMQ_MSG_ARRIVAL) {
        Some(PropertyValue::Long(l)) => *l,
        _ => 0,
    };
    if let Some(path) = simple_string_property(core, AMQ_MSG_BROKER_PATH).filter(|s| !s.is_empty())
    {
        m.broker_path = Some(broker_ids(&path));
    }
    if let Some(path) = simple_string_property(core, AMQ_MSG_CLUSTER).filter(|s| !s.is_empty()) {
        m.cluster = Some(broker_ids(&path));
    }
    m.command_id = match property(core, AMQ_MSG_COMMAND_ID) {
        Some(PropertyValue::Int(i)) => *i,
        _ => -1,
    };
    match property(core, JMS_CORRELATION_ID_PROPERTY) {
        Some(PropertyValue::String(s)) => m.correlation_id = Some(s.to_string_lossy()),
        Some(PropertyValue::Bytes(b)) => {
            // A strict decoder: malformed bytes leave it unset.
            if let Ok(s) = std::str::from_utf8(b) {
                m.correlation_id = Some(s.to_owned());
            }
        }
        _ => {}
    }
    if let Some(PropertyValue::Bytes(ds)) = property(core, AMQ_MSG_DATASTRUCTURE) {
        m.data_structure = wf.unmarshal(ds)?.map(Box::new);
    }
    m.destination = Some(to_amq_address(core, target.destination));
    if let Some(g) = property(core, HDR_GROUP_ID).and_then(string_of) {
        m.group_id = Some(g);
    }
    m.group_sequence = match property(core, HDR_GROUP_SEQUENCE) {
        Some(PropertyValue::Int(i)) => *i,
        _ => 0,
    };
    m.message_id = Some(match property(core, AMQ_MSG_MESSAGE_ID) {
        Some(PropertyValue::String(s)) => parse_message_id(&s.to_string_lossy())?,
        Some(PropertyValue::Bytes(b)) => match wf.unmarshal(b)? {
            Some(DataStructure::MessageId(id)) => id,
            Some(other) => {
                return Err(CodecError::class_cast(other.class_name(), "MessageId").into());
            }
            None => MessageId::default(),
        },
        _ => {
            // ARTEMIS-3776: ids above Integer.MAX_VALUE split in two.
            let id = core.message_id;
            let producer = format!(
                "ID:{}:-1:-1:{}",
                target.server_node_id,
                id / i32::MAX as i64
            );
            MessageId {
                producer_id: Some(parse_producer_id(&producer)?),
                producer_sequence_id: id % i32::MAX as i64,
                ..Default::default()
            }
        }
    });
    match property(core, AMQ_MSG_ORIG_DESTINATION) {
        Some(PropertyValue::String(s)) => {
            m.original_destination = Destination::create(&s.to_string_lossy(), QUEUE_TYPE);
        }
        Some(PropertyValue::Bytes(b)) => {
            m.original_destination = wf.unmarshal(b)?.map(Structure_from).transpose()?;
        }
        _ => {}
    }
    match property(core, AMQ_MSG_PRODUCER_ID) {
        Some(PropertyValue::String(s)) if !s.is_empty() => {
            m.producer_id = Some(parse_producer_id(&s.to_string_lossy())?);
        }
        Some(PropertyValue::Bytes(b)) => {
            m.producer_id = wf.unmarshal(b)?.map(Structure_from).transpose()?;
        }
        _ => {}
    }
    m.redelivery_counter = delivery_count - 1;
    match property(core, AMQ_MSG_REPLY_TO) {
        Some(PropertyValue::String(s)) => {
            m.reply_to = Destination::create(&s.to_string_lossy(), QUEUE_TYPE);
        }
        Some(PropertyValue::Bytes(b)) => {
            m.reply_to = wf.unmarshal(b)?.map(Structure_from).transpose()?;
        }
        _ => {}
    }
    if let Some(u) = simple_string_property(core, AMQ_MSG_USER_ID).filter(|s| !s.is_empty()) {
        m.user_id = Some(u);
    }
    if let Some(PropertyValue::Bool(b)) = property(core, AMQ_MSG_DROPPABLE) {
        m.droppable = *b;
    }
    if let Some(cause) = simple_string_property(core, AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY) {
        out.set_object_property(
            DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY,
            Primitive::String(cause),
        );
    }
    if let Some(lvq) = simple_string_property(core, HDR_LAST_VALUE_NAME) {
        out.set_object_property(HDR_LAST_VALUE_NAME, Primitive::String(lvq));
    }
    if let Some(PropertyValue::Long(t)) = property(core, HDR_INGRESS_TIMESTAMP) {
        out.set_object_property(HDR_INGRESS_TIMESTAMP, Primitive::Long(*t));
    }
    let notification = property(core, HDR_NOTIFICATION_TYPE).is_some();
    for (k, v) in properties_in_java_order(core, target.decoded_properties) {
        let name = k.to_string_lossy();
        if name.is_empty() {
            continue;
        }
        if !notification && (name.starts_with("_AMQ") || name.starts_with(AMQ_PREFIX)) {
            continue;
        }
        if name == JMS_CORRELATION_ID_PROPERTY {
            continue;
        }
        out.set_object_property(&name, outgoing_value(&name, v));
    }
    let mut msg = out.msg;
    msg.content = content;
    // Message.beforeMarshall: the property map goes on the wire marshalled.
    if let Some(props) = &out.properties {
        let mut b = Vec::new();
        p::write_map(&mut b, Some(props))?;
        msg.marshalled_properties = Some(b);
    }
    Ok(msg)
}

#[allow(non_snake_case)]
fn Structure_from<T: Structure>(ds: DataStructure) -> Result<T, ConvertError> {
    T::from_ds(ds).map_err(Into::into)
}

/// A map message body value as `TypedProperties.getMap()` gives it.
fn map_value(v: &PropertyValue) -> Primitive {
    match v {
        PropertyValue::Bytes(b) => Primitive::Bytes(b.clone()),
        other => outgoing_value("", other),
    }
}

/// `OpenWireUtil.toAMQAddress`: the message's address, as the consumer's
/// kind of destination, unless it is the consumer's own.
fn to_amq_address(core: &CoreMessage, actual: &Destination) -> Destination {
    match &core.address {
        None => actual.clone(),
        Some(a) => {
            let a = a.to_string_lossy();
            if a == actual.physical_name() {
                actual.clone()
            } else if actual.is_queue() {
                Destination::queue(a)
            } else {
                Destination::topic(a)
            }
        }
    }
}

/// `toAMQMessageCompressedBytesType`: the length, then the deflated bytes.
fn deflate_bytes_message(b: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    let _ = e.write_all(b);
    let c = e.finish().unwrap_or_default();
    let mut out = (b.len() as i32).to_be_bytes().to_vec();
    out.extend_from_slice(&c);
    out
}

/// `toAMQMessageStreamType`: Core stream items as OpenWire's, up to the
/// first it does not know.
fn read_core_stream(r: &mut Reader<'_>) -> R<Vec<u8>> {
    let e = |x: saranga_buffer::DecodeError| err(x.to_string());
    let mut out = Vec::new();
    while r.remaining() > 0 {
        match r.u8().map_err(e)? {
            dc::BOOLEAN => p::write_primitive(&mut out, &Primitive::Bool(r.bool().map_err(e)?))?,
            dc::BYTE => p::write_primitive(&mut out, &Primitive::Byte(r.i8().map_err(e)?))?,
            dc::BYTES => {
                let len = r.i32().map_err(e)?;
                if len < 0 {
                    return Err(err("java.lang.NegativeArraySizeException"));
                }
                let b = r.bytes(len as usize).map_err(e)?.to_vec();
                p::write_primitive(&mut out, &Primitive::Bytes(b))?;
            }
            dc::CHAR => p::write_primitive(&mut out, &Primitive::Char(r.i16().map_err(e)? as u16))?,
            dc::DOUBLE => p::write_primitive(&mut out, &Primitive::Double(r.f64().map_err(e)?))?,
            dc::FLOAT => p::write_primitive(&mut out, &Primitive::Float(r.f32().map_err(e)?))?,
            dc::INT => p::write_primitive(&mut out, &Primitive::Int(r.i32().map_err(e)?))?,
            dc::LONG => p::write_primitive(&mut out, &Primitive::Long(r.i64().map_err(e)?))?,
            dc::SHORT => p::write_primitive(&mut out, &Primitive::Short(r.i16().map_err(e)?))?,
            dc::STRING => match r.nullable_string().map_err(e)? {
                None => p::write_primitive(&mut out, &Primitive::Null)?,
                Some(s) => p::write_string(&mut out, &s.to_string_lossy())?,
            },
            _ => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids() {
        let id = parse_message_id("ID:host-1-2-3:4:5:6").unwrap();
        assert_eq!(id.producer_sequence_id, 6);
        let p = id.producer_id.as_ref().unwrap();
        assert_eq!(p.connection_id.as_deref(), Some("ID:host-1-2-3"));
        assert_eq!((p.session_id, p.value), (4, 5));
        assert_eq!(message_id_string(&id), "ID:host-1-2-3:4:5:6");
        assert_eq!(java_split("a,b,,"), vec!["a", "b"]);
        assert_eq!(java_split(",a"), vec!["", "a"]);
    }
}
