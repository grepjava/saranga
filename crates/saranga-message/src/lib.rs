//! Artemis Core messages. Ported from `CoreMessage`.

pub use bytes::Bytes;
use saranga_buffer::{DecodeError, Reader, Result, SimpleString, TypedProperties, Writer};

/// `PacketImpl.PACKET_HEADERS_SIZE` (i32 length + type byte + i64 channel).
/// `CoreMessage` offsets its end-of-body position by this much because the
/// message buffer was designed to sit inside a packet.
pub const BUFFER_HEADER_SPACE: i32 = 13;

/// `Message` type bytes.
pub mod message_type {
    pub const DEFAULT: u8 = 0;
    pub const OBJECT: u8 = 2;
    pub const TEXT: u8 = 3;
    pub const BYTES: u8 = 4;
    pub const MAP: u8 = 5;
    pub const STREAM: u8 = 6;
}

/// A Core message: body plus headers and properties.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoreMessage {
    pub message_id: i64,
    pub address: Option<SimpleString>,
    /// Time-based UUID bytes (the JMS message ID).
    pub user_id: Option<[u8; 16]>,
    pub message_type: u8,
    pub durable: bool,
    pub expiration: i64,
    pub timestamp: i64,
    pub priority: u8,
    pub properties: TypedProperties,
    /// The raw body buffer. Its layout depends on `message_type`
    /// (for TEXT it holds a nullable SimpleString). Shared, so that a copy
    /// of the message, or the broker's encoding of it, holds no other.
    pub body: Bytes,
}

impl CoreMessage {
    /// Decodes a full message buffer, as carried by `SESS_SEND` and
    /// `SESS_RECEIVE_MSG` (`CoreMessage.receiveBuffer`):
    /// i32 end-of-body position, body, then headers and properties.
    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let end_of_body = r.i32()?;
        let body_len = end_of_body
            .checked_sub(BUFFER_HEADER_SPACE)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                DecodeError::Invalid(format!("bad end-of-body position {end_of_body}"))
            })?;
        let body = Bytes::copy_from_slice(r.bytes(body_len)?);
        let mut msg = Self::decode_headers_and_properties(r)?;
        msg.body = body;
        Ok(msg)
    }

    pub fn encode(&self, w: &mut Writer) {
        w.i32(self.body.len() as i32 + BUFFER_HEADER_SPACE)
            .bytes(&self.body);
        self.encode_headers_and_properties(w);
    }

    /// `CoreMessage.decodeHeadersAndProperties`. The body is left empty.
    pub fn decode_headers_and_properties(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            message_id: r.i64()?,
            address: r.nullable_simple_string()?,
            user_id: r.nullable(|r| r.array::<16>())?,
            message_type: r.u8()?,
            durable: r.bool()?,
            expiration: r.i64()?,
            timestamp: r.i64()?,
            priority: r.u8()?,
            properties: TypedProperties::decode(r)?,
            body: Bytes::new(),
        })
    }

    /// `CoreMessage.encodeHeadersAndProperties`.
    pub fn encode_headers_and_properties(&self, w: &mut Writer) {
        w.i64(self.message_id)
            .nullable_simple_string(self.address.as_ref())
            .nullable(self.user_id.as_ref(), |w, id| w.bytes(id))
            .u8(self.message_type)
            .byte_buf_bool(self.durable)
            .i64(self.expiration)
            .i64(self.timestamp)
            .u8(self.priority);
        self.properties.encode(w);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_buffer::PropertyValue;

    #[test]
    fn roundtrip() {
        let mut msg = CoreMessage {
            message_id: 42,
            address: Some("golden.q1".into()),
            user_id: Some([7; 16]),
            message_type: message_type::TEXT,
            durable: true,
            expiration: 0,
            timestamp: 1_700_000_000_000,
            priority: 4,
            body: vec![1, 2, 3].into(),
            ..Default::default()
        };
        msg.properties.insert("k".into(), PropertyValue::Int(1));
        let mut w = Writer::new();
        msg.encode(&mut w);
        assert_eq!(&w.as_slice()[..4], &16i32.to_be_bytes());
        let mut r = Reader::new(w.as_slice());
        assert_eq!(CoreMessage::decode(&mut r).unwrap(), msg);
        assert!(r.is_empty());
    }
}
