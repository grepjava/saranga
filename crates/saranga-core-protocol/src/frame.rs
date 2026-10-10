use saranga_buffer::{DecodeError, Reader, Result, Writer};

/// Bytes a Core client sends once, before its first frame.
pub const HANDSHAKE: &[u8] = b"ARTEMIS";

/// i32 length + type byte + i64 channel ID (`PacketImpl.PACKET_HEADERS_SIZE`).
pub const FRAME_HEADER_SIZE: usize = 13;

/// One undecoded frame. The i32 length prefix counts everything after itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawFrame<'a> {
    pub packet_type: i8,
    pub channel_id: i64,
    pub body: &'a [u8],
}

/// Splits one frame off the front of `buf`. Returns the frame and the number
/// of bytes it occupied, or `None` if `buf` does not yet hold a whole frame.
pub fn read_frame(buf: &[u8]) -> Result<Option<(RawFrame<'_>, usize)>> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes(buf[..4].try_into().expect("4 bytes"));
    let len = usize::try_from(len)
        .ok()
        .filter(|&l| l >= FRAME_HEADER_SIZE - 4)
        .ok_or_else(|| DecodeError::Invalid(format!("bad frame length {len}")))?;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let mut r = Reader::new(&buf[4..4 + len]);
    let packet_type = r.i8()?;
    let channel_id = r.i64()?;
    let frame = RawFrame {
        packet_type,
        channel_id,
        body: r.rest(),
    };
    Ok(Some((frame, 4 + len)))
}

/// Writes a complete frame: header, then whatever `body` writes.
pub fn encode_frame(packet_type: i8, channel_id: i64, body: impl FnOnce(&mut Writer)) -> Vec<u8> {
    let mut w = Writer::new();
    w.i32(0).i8(packet_type).i64(channel_id);
    body(&mut w);
    let len = (w.len() - 4) as i32;
    w.set_i32(0, len);
    w.into_vec()
}
