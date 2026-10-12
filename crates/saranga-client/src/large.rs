//! Large message bodies. Ported from `LargeMessageControllerImpl`,
//! `CompressedLargeMessageControllerImpl`, `DeflaterReader` and
//! `InflaterWriter`.
//!
//! A large message arrives as its headers (`SessionReceiveLargeMessage`),
//! queued at once like any message, then its body in continuation packets.
//! The body is buffered in memory, as `ClientLargeMessageImpl` does when no
//! output stream is set. Until the body is read, the chunks' credits are
//! held; once `receive` takes the message they are returned, and so is the
//! credit of each chunk that arrives after.

use std::io::{Read, Write};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use tokio::sync::Notify;

use crate::error::{Error, Result};

/// `DeflaterReader` with `Deflater.setLevel(level)`: the whole body
/// zlib-compressed, as `java.util.zip.Deflater` writes it.
pub(crate) fn deflate(body: &[u8], level: i32) -> Result<Vec<u8>> {
    let compression = match level {
        -1 => Compression::default(),
        0..=9 => Compression::new(level as u32),
        _ => {
            return Err(Error::InvalidArgument(format!(
                "invalid compression level {level}"
            )));
        }
    };
    let mut encoder = ZlibEncoder::new(Vec::new(), compression);
    encoder.write_all(body)?;
    Ok(encoder.finish()?)
}

/// `InflaterWriter`: a zlib-compressed body inflated. `size_hint` is the
/// body size from `_AMQ_LARGE_SIZE`, when known.
pub(crate) fn inflate(body: &[u8], size_hint: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(size_hint);
    ZlibDecoder::new(body).read_to_end(&mut out)?;
    Ok(out)
}

#[derive(Debug, Default)]
struct ControllerState {
    body: Vec<u8>,
    /// The flow-control size of the chunks that arrived before the body
    /// was read (`largeMessageData`), and whether the last of them said
    /// more follow.
    held_credits: i32,
    held_continues: bool,
    /// The body is being read (`outStream` is set): each chunk's credits go
    /// back as it arrives.
    reading: bool,
    /// The chunks are dropped rather than kept (`discardBody`).
    discard: bool,
    /// The last chunk arrived (`streamEnded`).
    ended: bool,
    /// The consumer was cleared or closed (`cancel`).
    interrupted: bool,
    /// A chunk arrived since the last wait (`packetAdded`).
    packet_added: bool,
}

/// `LargeMessageControllerImpl`: the body of one large message.
#[derive(Debug, Default)]
pub(crate) struct LargeMessageController {
    state: Mutex<ControllerState>,
    notify: Notify,
}

impl LargeMessageController {
    fn state(&self) -> MutexGuard<'_, ControllerState> {
        self.state.lock().expect("large message")
    }

    /// `addPacket`. Returns the credits to give back now, with whether
    /// the slow-consumer credit may be discounted (the last chunk).
    pub(crate) fn add_packet(
        &self,
        chunk: Vec<u8>,
        flow_control_size: i32,
        continues: bool,
    ) -> Option<(i32, bool)> {
        let credits = {
            let mut st = self.state();
            st.packet_added = true;
            if st.interrupted {
                return None;
            }
            if !st.discard {
                if st.body.is_empty() {
                    st.body = chunk;
                } else {
                    st.body.extend_from_slice(&chunk);
                }
            }
            if !continues {
                st.ended = true;
            }
            if st.reading {
                Some((flow_control_size, !continues))
            } else {
                st.held_credits += flow_control_size;
                st.held_continues = continues;
                None
            }
        };
        self.notify.notify_waiters();
        credits
    }

    /// `setOutputStream`: from now on chunks are consumed as they arrive.
    /// Returns the credits of the chunks held so far; with `discard`
    /// (`discardBody`), what was kept is dropped as well.
    pub(crate) fn start_reading(&self, discard: bool) -> Option<(i32, bool)> {
        let mut st = self.state();
        st.reading = true;
        if discard {
            st.discard = true;
            st.body = Vec::new();
        }
        let credits = std::mem::take(&mut st.held_credits);
        (credits > 0).then_some((credits, !st.held_continues))
    }

    /// `waitCompletion(0)`: waits for the last chunk, failing when none
    /// arrives within `read_timeout`, then hands over the body.
    pub(crate) async fn wait_completion(&self, read_timeout: Duration) -> Result<Vec<u8>> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut st = self.state();
                if st.interrupted {
                    return Err(Error::large_message_interrupted());
                }
                if st.ended {
                    return Ok(std::mem::take(&mut st.body));
                }
                st.packet_added = false;
            }
            if tokio::time::timeout(read_timeout, notified).await.is_err()
                && !self.state().packet_added
            {
                return Err(Error::timeout_on_large_message());
            }
        }
    }

    /// `cancel`: the chunks not consumed are dropped and anyone waiting for
    /// the body is interrupted. Returns their credits.
    pub(crate) fn cancel(&self) -> i32 {
        let credits = {
            let mut st = self.state();
            st.interrupted = true;
            st.ended = true;
            st.body = Vec::new();
            std::mem::take(&mut st.held_credits)
        };
        self.notify.notify_waiters();
        credits
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorType;

    #[test]
    fn deflate_round_trip() {
        let body: Vec<u8> = (0..100_000u32).map(|i| (i % 7) as u8).collect();
        for level in [-1, 0, 1, 9] {
            let compressed = deflate(&body, level).unwrap();
            // A zlib stream (`new Deflater()` has the zlib wrapper).
            assert_eq!(compressed[0], 0x78);
            assert_eq!(inflate(&compressed, body.len()).unwrap(), body);
        }
        assert!(deflate(&body, 10).is_err());
        assert!(deflate(&body, -2).is_err());
    }

    #[tokio::test]
    async fn credits_are_held_until_read() {
        let c = LargeMessageController::default();
        assert_eq!(c.add_packet(vec![1, 2], 30, true), None);
        assert_eq!(c.add_packet(vec![3], 20, true), None);
        assert_eq!(c.start_reading(false), Some((50, false)));
        assert_eq!(c.add_packet(vec![4], 10, false), Some((10, true)));
        let body = c.wait_completion(Duration::from_secs(1)).await.unwrap();
        assert_eq!(body, [1, 2, 3, 4]);

        let c = LargeMessageController::default();
        assert_eq!(c.add_packet(vec![1], 30, true), None);
        assert_eq!(c.cancel(), 30);
        let err = c.wait_completion(Duration::from_secs(1)).await.unwrap_err();
        assert_eq!(err.kind(), Some(ErrorType::LargeMessageInterrupted));

        let c = LargeMessageController::default();
        c.start_reading(false);
        let err = c
            .wait_completion(Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), Some(ErrorType::LargeMessageErrorBody));
    }
}
