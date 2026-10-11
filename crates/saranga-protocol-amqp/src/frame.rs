//! Frames (part 2.3 of the specification) and protocol headers.

use crate::codec::{CodecError, Decoder, Result, invalid};
use crate::sasl::SaslFrame;
use crate::transport::Performative;

/// The protocol header for AMQP itself.
pub const AMQP_HEADER: [u8; 8] = *b"AMQP\x00\x01\x00\x00";
/// The protocol header for the SASL layer.
pub const SASL_HEADER: [u8; 8] = *b"AMQP\x03\x01\x00\x00";
/// The protocol header for the TLS layer.
pub const TLS_HEADER: [u8; 8] = *b"AMQP\x02\x01\x00\x00";

/// `MIN-MAX-FRAME-SIZE`.
pub const MIN_MAX_FRAME_SIZE: u32 = 512;

pub const TYPE_AMQP: u8 = 0;
pub const TYPE_SASL: u8 = 1;

/// A frame's body.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // one per frame, short-lived
pub enum FrameBody {
    /// A performative and, for a transfer, the message bytes after it.
    Amqp(Performative, Vec<u8>),
    Sasl(SaslFrame),
    /// An empty frame: a heartbeat.
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub channel: u16,
    pub body: FrameBody,
}

/// The size of the next frame in `buf`, once its 4 size bytes are there.
pub fn frame_size(buf: &[u8]) -> Option<u32> {
    buf.get(..4)
        .map(|b| u32::from_be_bytes(b.try_into().expect("4 bytes")))
}

/// Decodes one complete frame (`buf` holds exactly its bytes).
pub fn decode(buf: &[u8]) -> Result<Frame> {
    if buf.len() < 8 {
        return invalid("frame shorter than its header");
    }
    let doff = usize::from(buf[4]) * 4;
    if doff < 8 || doff > buf.len() {
        return invalid(format!("data offset {doff}"));
    }
    let kind = buf[5];
    let channel = u16::from_be_bytes([buf[6], buf[7]]);
    let body = &buf[doff..];
    if body.is_empty() {
        return Ok(Frame {
            channel,
            body: FrameBody::Empty,
        });
    }
    let mut d = Decoder::new(body);
    let value = d.read()?;
    let body = match kind {
        TYPE_AMQP => FrameBody::Amqp(Performative::from_value(value)?, d.rest().to_vec()),
        TYPE_SASL => {
            if d.has_remaining() {
                return invalid("bytes after a SASL frame body");
            }
            FrameBody::Sasl(SaslFrame::from_value(value)?)
        }
        other => return Err(CodecError::Invalid(format!("frame type {other}"))),
    };
    Ok(Frame { channel, body })
}

fn header(out: &mut Vec<u8>, kind: u8, channel: u16) -> usize {
    let start = out.len();
    out.extend([0, 0, 0, 0, 2, kind]);
    out.extend(channel.to_be_bytes());
    start
}

fn finish(out: &mut [u8], start: usize) {
    let size = (out.len() - start) as u32;
    out[start..start + 4].copy_from_slice(&size.to_be_bytes());
}

/// Writes an AMQP frame.
pub fn encode_amqp(out: &mut Vec<u8>, channel: u16, performative: &Performative, payload: &[u8]) {
    let start = header(out, TYPE_AMQP, channel);
    performative.encode(out);
    out.extend(payload);
    finish(out, start);
}

/// Writes a SASL frame.
pub fn encode_sasl(out: &mut Vec<u8>, frame: &SaslFrame) {
    let start = header(out, TYPE_SASL, 0);
    frame.encode(out);
    finish(out, start);
}

/// Writes an empty frame (a heartbeat).
pub fn encode_empty(out: &mut Vec<u8>) {
    out.extend([0, 0, 0, 8, 2, 0, 0, 0]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{Close, Open};

    #[test]
    fn frames_round_trip() {
        let open = Performative::Open(Open {
            container_id: "c".into(),
            ..Default::default()
        });
        let mut out = Vec::new();
        encode_amqp(&mut out, 3, &open, b"");
        assert_eq!(frame_size(&out), Some(out.len() as u32));
        assert_eq!(
            decode(&out).unwrap(),
            Frame {
                channel: 3,
                body: FrameBody::Amqp(open, vec![])
            }
        );
        let mut hb = Vec::new();
        encode_empty(&mut hb);
        assert_eq!(decode(&hb).unwrap().body, FrameBody::Empty);
        let mut close = Vec::new();
        encode_amqp(&mut close, 0, &Performative::Close(Close::default()), b"");
        assert_eq!(close, [0, 0, 0, 12, 2, 0, 0, 0, 0, 0x53, 0x18, 0x45]);
    }
}
