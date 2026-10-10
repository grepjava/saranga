//! Packets that carry a whole `CoreMessage`: sends and deliveries. The
//! message fills the body up to a fixed-size block of trailing fields.

use saranga_buffer::{DecodeError, Reader, Result, Writer};
use saranga_message::CoreMessage;

use crate::{Context, PacketCodec, types};

/// Splits the packet body into the message buffer and the trailing fields,
/// decoding the message strictly.
fn split_message<'a>(r: &mut Reader<'a>, trailer: usize) -> Result<(CoreMessage, Reader<'a>)> {
    let rest = r.rest();
    let split = rest
        .len()
        .checked_sub(trailer)
        .ok_or(DecodeError::UnexpectedEof {
            needed: trailer,
            remaining: rest.len(),
        })?;
    let (msg, tail) = rest.split_at(split);
    let mut mr = Reader::new(msg);
    let message = CoreMessage::decode(&mut mr)?;
    if !mr.is_empty() {
        return Err(DecodeError::Invalid(format!(
            "{} unexpected bytes after message",
            mr.remaining()
        )));
    }
    r.bytes(rest.len())?;
    Ok((message, Reader::new(tail)))
}

/// `SessionSendMessage` (versions before 130).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendMessage {
    pub message: CoreMessage,
    pub requires_response: bool,
}

impl PacketCodec for SessionSendMessage {
    const TYPE: i8 = types::SESS_SEND;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (message, mut t) = split_message(r, 1)?;
        Ok(Self {
            message,
            requires_response: t.bool()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        self.message.encode(w);
        w.bool(self.requires_response);
    }
}

/// `SessionSendMessage_V2` (130..134): adds the correlation ID.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendMessageV2 {
    pub message: CoreMessage,
    pub requires_response: bool,
    pub correlation_id: i64,
}

impl PacketCodec for SessionSendMessageV2 {
    const TYPE: i8 = types::SESS_SEND;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (message, mut t) = split_message(r, 9)?;
        Ok(Self {
            message,
            requires_response: t.bool()?,
            correlation_id: t.i64()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        self.message.encode(w);
        w.bool(self.requires_response).i64(self.correlation_id);
    }
}

/// `SessionSendMessage_V3` (134+): adds the producer (sender) ID.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendMessageV3 {
    pub message: CoreMessage,
    pub requires_response: bool,
    pub correlation_id: i64,
    pub sender_id: i32,
}

impl PacketCodec for SessionSendMessageV3 {
    const TYPE: i8 = types::SESS_SEND;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (message, mut t) = split_message(r, 13)?;
        Ok(Self {
            message,
            requires_response: t.bool()?,
            correlation_id: t.i64()?,
            sender_id: t.i32()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        self.message.encode(w);
        w.bool(self.requires_response)
            .i64(self.correlation_id)
            .i32(self.sender_id);
    }
}

/// `SessionReceiveMessage`: a delivery to a consumer.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionReceiveMessage {
    pub message: CoreMessage,
    pub consumer_id: i64,
    pub delivery_count: i32,
}

impl PacketCodec for SessionReceiveMessage {
    const TYPE: i8 = types::SESS_RECEIVE_MSG;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (message, mut t) = split_message(r, 12)?;
        Ok(Self {
            message,
            consumer_id: t.i64()?,
            delivery_count: t.i32()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        self.message.encode(w);
        w.i64(self.consumer_id).i32(self.delivery_count);
    }
}
