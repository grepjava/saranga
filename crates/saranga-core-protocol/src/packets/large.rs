//! Large-message packets. A large message travels as one packet with the
//! message headers and properties only (`SessionSendLargeMessage` /
//! `SessionReceiveLargeMessage`), followed by continuation packets carrying
//! the body in chunks. Each chunk is an i32-length-prefixed byte array
//! (`SessionContinuationMessage`), so the trailing fields sit right after it.

use saranga_buffer::{Reader, Result, Writer};
use saranga_message::CoreMessage;

use crate::{Context, PacketCodec, types};

/// The `SessionContinuationMessage` fields: an i32-length-prefixed body chunk
/// and whether more chunks follow.
fn read_chunk(r: &mut Reader<'_>) -> Result<(Vec<u8>, bool)> {
    Ok((r.byte_array()?, r.bool()?))
}

fn write_chunk(w: &mut Writer, body: &[u8], continues: bool) {
    w.byte_array(body).bool(continues);
}

/// The `SessionSendContinuationMessage` fields: the chunk, the total body
/// size (on the wire only on the last chunk) and `requiresResponse`.
fn read_send_chunk(r: &mut Reader<'_>) -> Result<(Vec<u8>, bool, i64, bool)> {
    let (body, continues) = read_chunk(r)?;
    let message_body_size = if continues { -1 } else { r.i64()? };
    Ok((body, continues, message_body_size, r.bool()?))
}

fn write_send_chunk(
    w: &mut Writer,
    body: &[u8],
    continues: bool,
    message_body_size: i64,
    requires_response: bool,
) {
    write_chunk(w, body, continues);
    if !continues {
        w.i64(message_body_size);
    }
    w.bool(requires_response);
}

/// `SessionSendContinuationMessage` (versions before 130): a body chunk of
/// a large message being sent.
///
/// `message_body_size` is on the wire only when `continues` is false (the
/// last chunk); it decodes as -1 otherwise and is then not encoded.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendContinuationMessage {
    pub body: Vec<u8>,
    pub continues: bool,
    pub message_body_size: i64,
    pub requires_response: bool,
}

impl PacketCodec for SessionSendContinuationMessage {
    const TYPE: i8 = types::SESS_SEND_CONTINUATION;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (body, continues, message_body_size, requires_response) = read_send_chunk(r)?;
        Ok(Self {
            body,
            continues,
            message_body_size,
            requires_response,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        write_send_chunk(
            w,
            &self.body,
            self.continues,
            self.message_body_size,
            self.requires_response,
        );
    }
}

/// Reads the `_V2` correlation ID, present only if 8 bytes remain (Java
/// always writes it).
fn read_correlation_id(r: &mut Reader<'_>) -> Result<Option<i64>> {
    if r.remaining() >= 8 {
        r.i64().map(Some)
    } else {
        Ok(None)
    }
}

/// `SessionSendContinuationMessage_V2` (130..134): adds the correlation ID,
/// read only if present. See [`SessionSendContinuationMessage`] for
/// `message_body_size`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendContinuationMessageV2 {
    pub body: Vec<u8>,
    pub continues: bool,
    pub message_body_size: i64,
    pub requires_response: bool,
    pub correlation_id: Option<i64>,
}

impl PacketCodec for SessionSendContinuationMessageV2 {
    const TYPE: i8 = types::SESS_SEND_CONTINUATION;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (body, continues, message_body_size, requires_response) = read_send_chunk(r)?;
        Ok(Self {
            body,
            continues,
            message_body_size,
            requires_response,
            correlation_id: read_correlation_id(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        write_send_chunk(
            w,
            &self.body,
            self.continues,
            self.message_body_size,
            self.requires_response,
        );
        if let Some(id) = self.correlation_id {
            w.i64(id);
        }
    }
}

/// `SessionSendContinuationMessage_V3` (134+): adds the producer (sender)
/// ID after the correlation ID. As in Java, the correlation ID is read only
/// if at least 8 bytes remain (before the sender ID is read).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendContinuationMessageV3 {
    pub body: Vec<u8>,
    pub continues: bool,
    pub message_body_size: i64,
    pub requires_response: bool,
    pub correlation_id: Option<i64>,
    pub sender_id: i32,
}

impl PacketCodec for SessionSendContinuationMessageV3 {
    const TYPE: i8 = types::SESS_SEND_CONTINUATION;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (body, continues, message_body_size, requires_response) = read_send_chunk(r)?;
        Ok(Self {
            body,
            continues,
            message_body_size,
            requires_response,
            correlation_id: read_correlation_id(r)?,
            sender_id: r.i32()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        write_send_chunk(
            w,
            &self.body,
            self.continues,
            self.message_body_size,
            self.requires_response,
        );
        if let Some(id) = self.correlation_id {
            w.i64(id);
        }
        w.i32(self.sender_id);
    }
}

/// `SessionReceiveContinuationMessage`: a body chunk of a large message
/// being delivered to a consumer.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionReceiveContinuationMessage {
    pub body: Vec<u8>,
    pub continues: bool,
    pub consumer_id: i64,
}

impl PacketCodec for SessionReceiveContinuationMessage {
    const TYPE: i8 = types::SESS_RECEIVE_CONTINUATION;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (body, continues) = read_chunk(r)?;
        Ok(Self {
            body,
            continues,
            consumer_id: r.i64()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        write_chunk(w, &self.body, self.continues);
        w.i64(self.consumer_id);
    }
}

/// `SessionSendLargeMessage`: the first packet of a large message sent by a
/// client. Holds only the message headers and properties
/// (`CoreMessage.encodeHeadersAndProperties`); `message.body` is unused.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendLargeMessage {
    pub message: CoreMessage,
}

impl PacketCodec for SessionSendLargeMessage {
    const TYPE: i8 = types::SESS_SEND_LARGE;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            message: CoreMessage::decode_headers_and_properties(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        self.message.encode_headers_and_properties(w);
    }
}

/// `SessionReceiveLargeMessage`: the first packet of a large message
/// delivered to a consumer, as the broker encodes it. The message holds only
/// headers and properties; `message.body` is unused. (Java skips the message
/// when it is null, but the decoder always reads one, so it is required here.)
///
/// Decoding yields [`SessionReceiveClientLargeMessage`], as in Java.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionReceiveLargeMessage {
    pub consumer_id: i64,
    pub delivery_count: i32,
    /// The total body size, sent ahead of the body chunks.
    pub large_message_size: i64,
    pub message: CoreMessage,
}

impl PacketCodec for SessionReceiveLargeMessage {
    const TYPE: i8 = types::SESS_RECEIVE_LARGE_MSG;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            consumer_id: r.i64()?,
            delivery_count: r.i32()?,
            large_message_size: r.i64()?,
            message: CoreMessage::decode_headers_and_properties(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i64(self.consumer_id)
            .i32(self.delivery_count)
            .i64(self.large_message_size);
        self.message.encode_headers_and_properties(w);
    }
}

/// `SessionReceiveClientLargeMessage`: `SESS_RECEIVE_LARGE_MSG` as the
/// client decodes it (`ClientPacketDecoder`). Same wire form as
/// [`SessionReceiveLargeMessage`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionReceiveClientLargeMessage {
    pub consumer_id: i64,
    pub delivery_count: i32,
    /// The total body size, sent ahead of the body chunks.
    pub large_message_size: i64,
    pub message: CoreMessage,
}

impl From<SessionReceiveLargeMessage> for SessionReceiveClientLargeMessage {
    fn from(p: SessionReceiveLargeMessage) -> Self {
        Self {
            consumer_id: p.consumer_id,
            delivery_count: p.delivery_count,
            large_message_size: p.large_message_size,
            message: p.message,
        }
    }
}

impl From<SessionReceiveClientLargeMessage> for SessionReceiveLargeMessage {
    fn from(p: SessionReceiveClientLargeMessage) -> Self {
        Self {
            consumer_id: p.consumer_id,
            delivery_count: p.delivery_count,
            large_message_size: p.large_message_size,
            message: p.message,
        }
    }
}

impl PacketCodec for SessionReceiveClientLargeMessage {
    const TYPE: i8 = types::SESS_RECEIVE_LARGE_MSG;

    fn decode(r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        SessionReceiveLargeMessage::decode(r, ctx).map(Into::into)
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i64(self.consumer_id)
            .i32(self.delivery_count)
            .i64(self.large_message_size);
        self.message.encode_headers_and_properties(w);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Direction, read_frame};

    /// `SessionReceiveLargeMessage` is never decoded, so it has no fixture of
    /// its own: check it against the broker-encoded client fixture.
    #[test]
    fn receive_large_message_matches_broker_encoding() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/packets/large/SessionReceiveClientLargeMessage@c-136.bin"
        );
        let bytes = std::fs::read(path).unwrap();
        let (frame, _) = read_frame(&bytes).unwrap().unwrap();
        let ctx = Context::new(Direction::ToClient, 136);
        let mut r = Reader::new(frame.body);
        let p = SessionReceiveLargeMessage::decode(&mut r, &ctx).unwrap();
        assert!(r.is_empty());
        assert_eq!(p.consumer_id, 21);
        assert_eq!(p.large_message_size, 1_048_576);
        let mut w = Writer::new();
        p.encode(&mut w, &ctx);
        assert_eq!(w.as_slice(), frame.body);
    }
}
