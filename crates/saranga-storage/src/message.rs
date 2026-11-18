//! Message records: `ADD_MESSAGE_PROTOCOL` (any protocol, led by a
//! persister ID: `MessagePersister`, `CoreMessagePersister`, the AMQP
//! persisters) and `ADD_LARGE_MESSAGE` (`LargeMessagePersister`).
//!
//! AMQP messages keep their AMQP encoding opaque; only the persister's own
//! header fields are parsed, so the record re-encodes exactly.

use saranga_buffer::{DecodeError, Reader, Result, SimpleString, TypedProperties, Writer};
use saranga_message::CoreMessage;

use crate::codec::RecordCodec;
use crate::ids::persister;

/// A Core message as `CoreMessagePersister` stores it: persister ID 1, the
/// message ID, the address, then `CoreMessage.persist`: the size of the
/// message buffer and the buffer (end-of-body position, body, headers and
/// properties).
///
/// The message ID and address in front override the ones inside the
/// buffer when Artemis reloads the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMessageRecord {
    pub message_id: i64,
    pub address: Option<SimpleString>,
    pub message: CoreMessage,
    /// Bytes inside the message buffer after the properties. Artemis never
    /// writes any; kept so that any buffer re-encodes exactly.
    pub extra: Vec<u8>,
}

impl CoreMessageRecord {
    /// Wraps a message to persist, as `CoreMessagePersister.encode` does.
    pub fn new(message: CoreMessage) -> Self {
        Self {
            message_id: message.message_id,
            address: message.address.clone(),
            message,
            extra: Vec::new(),
        }
    }

    /// `CoreMessage.getPersistSize() - 4`: the size of the message buffer
    /// (`getEncodeSize()`, printed as the message's `size`).
    pub fn buffer_size(&self) -> usize {
        let mut w = Writer::new();
        self.message.encode(&mut w);
        w.len() + self.extra.len()
    }

    /// The message as Artemis sees it after `CoreMessagePersister.decode`:
    /// the record's message ID and address replace the buffer's.
    pub fn to_message(&self) -> CoreMessage {
        let mut m = self.message.clone();
        m.message_id = self.message_id;
        m.address = self.address.clone();
        m
    }

    /// Reads after the persister ID byte.
    fn read_body(r: &mut Reader<'_>) -> Result<Self> {
        let message_id = r.i64()?;
        let address = r.nullable_simple_string()?;
        let size = r.len("message buffer")?;
        let buf = r.bytes(size)?;
        let mut inner = Reader::new(buf);
        let message = CoreMessage::decode(&mut inner)?;
        Ok(Self {
            message_id,
            address,
            message,
            extra: inner.rest().to_vec(),
        })
    }

    fn write_body(&self, w: &mut Writer) {
        let mut buf = Writer::new();
        self.message.encode(&mut buf);
        buf.bytes(&self.extra);
        w.i64(self.message_id)
            .nullable_simple_string(self.address.as_ref())
            .byte_array(buf.as_slice());
    }
}

/// An AMQP message (`AMQPMessagePersister`, `AMQPMessagePersisterV2` or
/// `AMQPMessagePersisterV3`). The AMQP encoding itself is opaque.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpMessageRecord {
    /// [`persister::AMQP_MESSAGE`], [`persister::AMQP_MESSAGE_V2`] or
    /// [`persister::AMQP_MESSAGE_V3`].
    pub persister_id: u8,
    pub message_id: i64,
    /// The AMQP message format (`message-format` of the transfer).
    pub message_format: i64,
    pub address: Option<SimpleString>,
    /// The AMQP-encoded message (`AMQPStandardMessage.persist`).
    pub data: Vec<u8>,
    /// V2 and V3: the broker-side extra properties; `None` is written as a
    /// size of 0.
    pub extra_properties: Option<TypedProperties>,
    /// V3: the expiration time.
    pub expiration: Option<i64>,
}

impl AmqpMessageRecord {
    fn read_body(persister_id: u8, r: &mut Reader<'_>) -> Result<Self> {
        let message_id = r.i64()?;
        let message_format = r.i64()?;
        let address = r.nullable_simple_string()?;
        let data = r.byte_array()?;
        let mut extra_properties = None;
        let mut expiration = None;
        if persister_id >= persister::AMQP_MESSAGE_V2 {
            extra_properties = read_sized_properties(r)?;
        }
        if persister_id == persister::AMQP_MESSAGE_V3 {
            expiration = Some(r.i64()?);
        }
        Ok(Self {
            persister_id,
            message_id,
            message_format,
            address,
            data,
            extra_properties,
            expiration,
        })
    }

    fn write_body(&self, w: &mut Writer) {
        w.i64(self.message_id)
            .i64(self.message_format)
            .nullable_simple_string(self.address.as_ref())
            .byte_array(&self.data);
        if self.persister_id >= persister::AMQP_MESSAGE_V2 {
            write_sized_properties(w, self.extra_properties.as_ref());
        }
        // Only V3 has the field, so only V3 writes it.
        if self.persister_id == persister::AMQP_MESSAGE_V3 {
            w.i64(self.expiration.unwrap_or(0));
        }
    }
}

/// An AMQP large message's header (`AMQPLargeMessagePersister`). The body
/// lives in the large-messages directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpLargeMessageRecord {
    pub message_id: i64,
    pub durable: bool,
    pub message_format: i64,
    pub address: Option<SimpleString>,
    pub extra_properties: Option<TypedProperties>,
    /// The saved AMQP encoding of the header sections, then the expiration
    /// and the re-encoded flag when present. Kept opaque.
    pub saved_encoding: Vec<u8>,
}

impl AmqpLargeMessageRecord {
    fn read_body(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            message_id: r.i64()?,
            durable: r.bool()?,
            message_format: r.i64()?,
            address: r.nullable_simple_string()?,
            extra_properties: read_sized_properties(r)?,
            saved_encoding: read_saved_encoding(r)?,
        })
    }

    fn write_body(&self, w: &mut Writer) {
        w.i64(self.message_id)
            .bool(self.durable)
            .i64(self.message_format)
            .nullable_simple_string(self.address.as_ref());
        write_sized_properties(w, self.extra_properties.as_ref());
        w.bytes(&self.saved_encoding);
    }
}

/// `readSavedEncoding`, then the expiration when 8 bytes are left and the
/// re-encoded flag when one is (`AMQPLargeMessagePersister.decode`), kept as
/// they were written. Only what is the record's is read: in a page the
/// message's queue IDs follow it.
fn read_saved_encoding(r: &mut Reader<'_>) -> Result<Vec<u8>> {
    let start = r.rest();
    let before = r.remaining();
    saranga_message::read_amqp_saved_encoding(r)?;
    if r.remaining() >= 8 {
        r.i64()?;
    }
    if r.remaining() > 0 {
        r.bool()?;
    }
    Ok(start[..before - r.remaining()].to_vec())
}

/// An i32 size, then (when not 0) `TypedProperties` of that size.
fn read_sized_properties(r: &mut Reader<'_>) -> Result<Option<TypedProperties>> {
    let size = r.i32()?;
    if size == 0 {
        return Ok(None);
    }
    let mismatch = || {
        DecodeError::Invalid(format!(
            "extra properties size {size} does not match their encoding"
        ))
    };
    // Read within the size given, never into what follows.
    let len = usize::try_from(size).map_err(|_| mismatch())?;
    let mut within = Reader::new(r.bytes(len)?);
    let props = TypedProperties::decode(&mut within)?;
    if !within.is_empty() {
        return Err(mismatch());
    }
    Ok(Some(props))
}

fn write_sized_properties(w: &mut Writer, props: Option<&TypedProperties>) {
    match props {
        None => {
            w.i32(0);
        }
        Some(p) => {
            let mut buf = Writer::new();
            p.encode(&mut buf);
            w.byte_array(buf.as_slice());
        }
    }
}

/// `ADD_MESSAGE_PROTOCOL`: `MessagePersister` reads the persister ID byte and
/// hands the rest to that persister.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistedMessage {
    Core(CoreMessageRecord),
    Amqp(AmqpMessageRecord),
    AmqpLarge(AmqpLargeMessageRecord),
}

impl PersistedMessage {
    pub fn persister_id(&self) -> u8 {
        match self {
            Self::Core(_) => persister::CORE_MESSAGE,
            Self::Amqp(m) => m.persister_id,
            Self::AmqpLarge(_) => persister::AMQP_LARGE_MESSAGE,
        }
    }

    pub fn message_id(&self) -> i64 {
        match self {
            Self::Core(m) => m.message_id,
            Self::Amqp(m) => m.message_id,
            Self::AmqpLarge(m) => m.message_id,
        }
    }

    pub fn is_large(&self) -> bool {
        matches!(self, Self::AmqpLarge(_))
    }

    /// Whether the message is durable, where its record says.
    pub fn is_durable(&self) -> bool {
        match self {
            Self::Core(m) => m.message.durable,
            Self::Amqp(_) => true,
            Self::AmqpLarge(m) => m.durable,
        }
    }
}

impl RecordCodec for PersistedMessage {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        match r.u8()? {
            persister::CORE_MESSAGE => CoreMessageRecord::read_body(r).map(Self::Core),
            id @ (persister::AMQP_MESSAGE
            | persister::AMQP_MESSAGE_V2
            | persister::AMQP_MESSAGE_V3) => AmqpMessageRecord::read_body(id, r).map(Self::Amqp),
            persister::AMQP_LARGE_MESSAGE => {
                AmqpLargeMessageRecord::read_body(r).map(Self::AmqpLarge)
            }
            id => Err(DecodeError::Invalid(format!(
                "couldn't find factory for type={id}"
            ))),
        }
    }

    fn write(&self, w: &mut Writer) {
        w.u8(self.persister_id());
        match self {
            Self::Core(m) => m.write_body(w),
            Self::Amqp(m) => m.write_body(w),
            Self::AmqpLarge(m) => m.write_body(w),
        }
    }
}

/// `ADD_LARGE_MESSAGE`: a Core large message's headers and properties
/// (`CoreMessage.encodeHeadersAndProperties`). There is no persister ID
/// byte. The body is the `<id>.msg` file in the large-messages directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeMessageRecord {
    /// The message; its body is always empty.
    pub message: CoreMessage,
}

impl RecordCodec for LargeMessageRecord {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            message: CoreMessage::decode_headers_and_properties(r)?,
        })
    }
    fn write(&self, w: &mut Writer) {
        self.message.encode_headers_and_properties(w);
    }
}

#[cfg(test)]
mod tests {
    use saranga_buffer::PropertyValue;

    use super::*;

    fn core_message() -> CoreMessage {
        let mut m = CoreMessage {
            message_id: 7,
            address: Some("q".into()),
            user_id: Some([1; 16]),
            message_type: saranga_message::message_type::TEXT,
            durable: true,
            timestamp: 1_000,
            priority: 4,
            body: vec![1, 0, 0, 0, 2, 0x61, 0].into(),
            ..Default::default()
        };
        m.properties.insert("k".into(), PropertyValue::Int(1));
        m
    }

    fn amqp(persister_id: u8, expiration: Option<i64>) -> AmqpMessageRecord {
        AmqpMessageRecord {
            persister_id,
            message_id: 1,
            message_format: 0,
            address: None,
            data: vec![0x00, 0x53, 0x77, 0x40],
            extra_properties: None,
            expiration,
        }
    }

    /// The expiration is V3's field: written by V3 only, always.
    #[test]
    fn only_v3_writes_the_expiration() {
        let v3 = PersistedMessage::Amqp(amqp(persister::AMQP_MESSAGE_V3, None));
        let back = PersistedMessage::decode(&v3.encode()).unwrap();
        let PersistedMessage::Amqp(a) = back else {
            unreachable!()
        };
        assert_eq!(a.expiration, Some(0));
        let v2 = PersistedMessage::Amqp(amqp(persister::AMQP_MESSAGE_V2, Some(5)));
        let PersistedMessage::Amqp(a) = PersistedMessage::decode(&v2.encode()).unwrap() else {
            unreachable!()
        };
        assert_eq!(a.expiration, None);
    }

    /// Extra properties are read within their size, not into the expiration
    /// that follows them.
    #[test]
    fn extra_properties_are_read_within_their_size() {
        let mut props = TypedProperties::default();
        props.insert("k".into(), PropertyValue::Int(1));
        let mut rec = amqp(persister::AMQP_MESSAGE_V3, Some(9));
        rec.extra_properties = Some(props);
        let mut bytes = PersistedMessage::Amqp(rec.clone()).encode();
        assert_eq!(
            PersistedMessage::decode(&bytes).unwrap(),
            PersistedMessage::Amqp(rec)
        );
        // The size field sits right before the properties: one too small.
        let at = bytes.len() - 8 - (props_len(&bytes) as usize) - 4;
        let size = i32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
        bytes[at..at + 4].copy_from_slice(&(size - 1).to_be_bytes());
        assert!(PersistedMessage::decode(&bytes).is_err());
        bytes[at..at + 4].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(PersistedMessage::decode(&bytes).is_err());
    }

    fn props_len(_bytes: &[u8]) -> i32 {
        let mut props = TypedProperties::default();
        props.insert("k".into(), PropertyValue::Int(1));
        let mut w = Writer::new();
        props.encode(&mut w);
        w.as_slice().len() as i32
    }

    #[test]
    fn core_record_layout() {
        let rec = PersistedMessage::Core(CoreMessageRecord::new(core_message()));
        let bytes = rec.encode();
        assert_eq!(bytes[0], persister::CORE_MESSAGE);
        assert_eq!(&bytes[1..9], &7i64.to_be_bytes());
        assert_eq!(PersistedMessage::decode(&bytes).unwrap(), rec);
        let PersistedMessage::Core(c) = &rec else {
            unreachable!()
        };
        // id, address, size, buffer.
        assert_eq!(bytes.len(), 1 + 8 + 1 + 4 + 2 + 4 + c.buffer_size());
    }

    #[test]
    fn outer_id_and_address_win() {
        let mut c = CoreMessageRecord::new(core_message());
        c.message_id = 99;
        c.address = None;
        let m = c.to_message();
        assert_eq!(m.message_id, 99);
        assert_eq!(m.address, None);
    }

    #[test]
    fn amqp_persister_versions() {
        let mut extra = TypedProperties::new();
        extra.insert("x-opt".into(), PropertyValue::Long(5));
        for (id, props, exp) in [
            (persister::AMQP_MESSAGE, None, None),
            (persister::AMQP_MESSAGE_V2, Some(extra.clone()), None),
            (persister::AMQP_MESSAGE_V2, None, None),
            (persister::AMQP_MESSAGE_V3, Some(extra.clone()), Some(1234)),
        ] {
            let rec = PersistedMessage::Amqp(AmqpMessageRecord {
                persister_id: id,
                message_id: 3,
                message_format: 0,
                address: Some("a".into()),
                data: vec![0, 0x53, 0x70],
                extra_properties: props,
                expiration: exp,
            });
            assert_eq!(PersistedMessage::decode(&rec.encode()).unwrap(), rec);
        }
        let large = PersistedMessage::AmqpLarge(AmqpLargeMessageRecord {
            message_id: 1,
            durable: true,
            message_format: 0,
            address: None,
            extra_properties: Some(extra),
            // `saveEncoding`: 2, 3, 1 and 2 ints, each followed by a value
            // (null here), then the expiration and the re-encoded flag.
            saved_encoding: [2, 3, 1, 2]
                .into_iter()
                .flat_map(|ints| {
                    let mut part = vec![0u8; 4 * ints];
                    part.push(0x40);
                    part
                })
                .chain(5i64.to_be_bytes())
                .chain([1])
                .collect(),
        });
        assert_eq!(PersistedMessage::decode(&large.encode()).unwrap(), large);
        assert!(PersistedMessage::decode(&[9, 0]).is_err());
    }

    #[test]
    fn large_message_record() {
        let mut m = core_message();
        m.body.clear();
        let rec = LargeMessageRecord { message: m };
        assert_eq!(LargeMessageRecord::decode(&rec.encode()).unwrap(), rec);
    }
}
