//! The Artemis Core protocol: frames, packet types and their codecs.
//!
//! Ported from `org.apache.activemq.artemis.core.protocol.core.impl`
//! (`PacketImpl`, `PacketDecoder`, `ServerPacketDecoder`,
//! `ClientPacketDecoder`) and the `wireformat` package. Each packet struct is
//! named after its Java class and keeps every field needed to re-encode it
//! byte for byte.

#[macro_use]
mod macros;

mod context;
mod frame;
mod packet;
pub mod packets;
pub mod transport;
pub mod types;

pub use context::{Context, Direction, version};
pub use frame::{FRAME_HEADER_SIZE, HANDSHAKE, RawFrame, encode_frame, read_frame};
pub use packet::Packet;
pub use saranga_buffer::{DecodeError, Result};

use saranga_buffer::{Reader, Writer};

/// Encoding and decoding of one packet body (everything after the channel ID).
pub trait PacketCodec: Sized {
    /// The packet type byte (`PacketImpl` constant).
    const TYPE: i8;

    fn decode(r: &mut Reader<'_>, ctx: &Context) -> Result<Self>;
    fn encode(&self, w: &mut Writer, ctx: &Context);
}

/// An optional value preceded by an `ActiveMQBuffer` boolean flag (`0xFF` when
/// present), as opposed to `Option<T>`, whose NOT_NULL marker is `0x01`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Flagged<T>(pub Option<T>);

impl<T: saranga_buffer::Codec> saranga_buffer::Codec for Flagged<T> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        if r.bool()? {
            T::read(r).map(|v| Self(Some(v)))
        } else {
            Ok(Self(None))
        }
    }

    fn write(&self, w: &mut Writer) {
        w.bool(self.0.is_some());
        if let Some(v) = &self.0 {
            v.write(w);
        }
    }
}
