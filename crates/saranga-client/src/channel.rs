//! Channels and blocking calls. Ported from `ChannelImpl` (with a
//! confirmation window of -1, the client default: no resend cache and no
//! async response cache).
//!
//! A blocking call (`ChannelImpl.sendBlocking`) takes the next correlation ID
//! from a per-channel counter that starts at -1 and decrements before use, so
//! the first call gets -2. Only packets that carry a correlation ID on the
//! wire keep it (`SessionSendMessage_V2/V3`, `SessionCommitMessage_V2`); for
//! every other packet `getCorrelationID()` stays -1. A response completes the
//! call when it is an `EXCEPTION`, or when its type is the expected one and
//! its correlation ID equals the request's. Anything else is dropped.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Mutex, Weak};

use saranga_core_protocol::{Packet, types};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::session::SessionInner;

struct Pending {
    expected: i8,
    correlation_id: i64,
    tx: oneshot::Sender<Result<Packet>>,
}

pub(crate) struct Channel {
    pub(crate) id: i64,
    /// `sendBlockingLock`: one blocking call at a time.
    pub(crate) blocking: tokio::sync::Mutex<()>,
    blocking_correlation_id: AtomicI64,
    pending: Mutex<Option<Pending>>,
    closed: AtomicBool,
    /// The handler for non-response packets (`ClientSessionPacketHandler`).
    session: Mutex<Option<Weak<SessionInner>>>,
}

impl Channel {
    pub(crate) fn new(id: i64) -> Self {
        Self {
            id,
            blocking: tokio::sync::Mutex::new(()),
            blocking_correlation_id: AtomicI64::new(-1),
            pending: Mutex::new(None),
            closed: AtomicBool::new(false),
            session: Mutex::new(None),
        }
    }

    pub(crate) fn set_session(&self, session: Weak<SessionInner>) {
        *self.session.lock().expect("channel") = Some(session);
    }

    pub(crate) fn session(&self) -> Option<std::sync::Arc<SessionInner>> {
        self.session
            .lock()
            .expect("channel")
            .as_ref()
            .and_then(Weak::upgrade)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// `blockingCorrelationID.decrementAndGet()`.
    pub(crate) fn next_correlation_id(&self) -> i64 {
        self.blocking_correlation_id.fetch_sub(1, Ordering::SeqCst) - 1
    }

    /// Registers the call about to be written. The caller holds `blocking`.
    pub(crate) fn begin(
        &self,
        expected: i8,
        correlation_id: i64,
    ) -> oneshot::Receiver<Result<Packet>> {
        let (tx, rx) = oneshot::channel();
        *self.pending.lock().expect("channel") = Some(Pending {
            expected,
            correlation_id,
            tx,
        });
        rx
    }

    pub(crate) fn abandon(&self) {
        self.pending.lock().expect("channel").take();
    }

    /// `ChannelImpl.handlePacket` for a response packet.
    pub(crate) fn handle_response(&self, packet: Packet) {
        let mut pending = self.pending.lock().expect("channel");
        let Some(p) = pending.as_ref() else {
            return;
        };
        let ty = packet.packet_type();
        if ty == types::EXCEPTION {
            let p = pending.take().expect("checked");
            let _ = p.tx.send(Err(exception_to_error(&packet)));
        } else if ty == p.expected && correlation_id(&packet) == p.correlation_id {
            let p = pending.take().expect("checked");
            let _ = p.tx.send(Ok(packet));
        }
    }

    /// `ChannelImpl.close` + `returnBlocking`: fails any call in progress.
    pub(crate) fn close(&self, cause: &Error) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(p) = self.pending.lock().expect("channel").take() {
            let _ = p.tx.send(Err(Error::unblocking_a_call(cause)));
        }
    }
}

/// `Packet.getCorrelationID()`: -1 unless the packet carries one.
pub(crate) fn correlation_id(packet: &Packet) -> i64 {
    match packet {
        Packet::NullResponseMessageV2(p) => p.correlation_id.unwrap_or(-1),
        Packet::ActiveMQExceptionMessageV2(p) => p.correlation_id.unwrap_or(-1),
        Packet::SessionSendMessageV2(p) => p.correlation_id,
        Packet::SessionSendMessageV3(p) => p.correlation_id,
        Packet::SessionCommitMessageV2(p) => p.correlation_id.unwrap_or(-1),
        _ => -1,
    }
}

/// `Packet.setCorrelationID()`: a no-op for packets without one.
pub(crate) fn set_correlation_id(packet: &mut Packet, id: i64) {
    match packet {
        Packet::SessionSendMessageV2(p) => p.correlation_id = id,
        Packet::SessionSendMessageV3(p) => p.correlation_id = id,
        Packet::SessionCommitMessageV2(p) => p.correlation_id = Some(id),
        _ => {}
    }
}

/// `Packet.isResponse()`.
pub(crate) fn is_response(packet_type: i8) -> bool {
    matches!(
        packet_type,
        types::EXCEPTION
            | types::NULL_RESPONSE
            | types::CREATESESSION_RESP
            | types::REATTACH_SESSION_RESP
            | types::CONNECT_RESPONSE
            | types::SESS_QUEUEQUERY_RESP
            | types::SESS_QUEUEQUERY_RESP_V2
            | types::SESS_QUEUEQUERY_RESP_V3
            | types::SESS_BINDINGQUERY_RESP
            | types::SESS_BINDINGQUERY_RESP_V2
            | types::SESS_BINDINGQUERY_RESP_V3
            | types::SESS_BINDINGQUERY_RESP_V4
            | types::SESS_BINDINGQUERY_RESP_V5
            | types::SESS_XA_RESP
            | types::SESS_XA_INDOUBT_XIDS_RESP
            | types::SESS_XA_SET_TIMEOUT_RESP
            | types::SESS_XA_GET_TIMEOUT_RESP
    )
}

fn exception_to_error(packet: &Packet) -> Error {
    let (code, message) = match packet {
        Packet::ActiveMQExceptionMessage(p) => (p.code, p.message.as_ref()),
        Packet::ActiveMQExceptionMessageV2(p) => (p.code, p.message.as_ref()),
        _ => unreachable!("not an exception packet"),
    };
    Error::ActiveMq {
        code,
        message: message.map(|m| m.to_string_lossy()).unwrap_or_default(),
    }
}

/// `ChannelImpl.supports(packetType, version)`.
pub(crate) fn supports(packet_type: i8, version: i32) -> bool {
    use saranga_core_protocol::version as v;
    match packet_type {
        types::CLUSTER_TOPOLOGY_V2 => version >= 122,
        types::DISCONNECT_CONSUMER => version >= 124,
        types::CLUSTER_TOPOLOGY_V3 | types::DISCONNECT_V2 => version >= 125,
        types::SESS_QUEUEQUERY_RESP_V2 | types::SESS_BINDINGQUERY_RESP_V2 => version >= 126,
        types::SESS_BINDINGQUERY_RESP_V3 => version >= 127,
        types::SESS_QUEUEQUERY_RESP_V3 | types::SESS_BINDINGQUERY_RESP_V4 => {
            version >= v::ADDRESSING_CHANGE
        }
        types::CLUSTER_TOPOLOGY_V4 | types::CREATESESSION_V2 | types::DISCONNECT_V3 => {
            version >= v::ARTEMIS_2_18_0
        }
        types::SESS_BINDINGQUERY_RESP_V5 => version >= v::ARTEMIS_2_29_0,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_core_protocol::packets::connection::*;
    use saranga_core_protocol::packets::session::*;

    #[test]
    fn correlation_ids_count_down_from_minus_two() {
        let ch = Channel::new(10);
        assert_eq!(ch.next_correlation_id(), -2);
        assert_eq!(ch.next_correlation_id(), -3);
    }

    #[test]
    fn response_matching() {
        let ch = Channel::new(10);
        // A send with correlation -7 ignores a NullResponse for -1.
        let mut rx = ch.begin(types::NULL_RESPONSE, -7);
        ch.handle_response(
            NullResponseMessageV2 {
                correlation_id: Some(-1),
            }
            .into(),
        );
        assert!(rx.try_recv().is_err());
        // Wrong type, right correlation: ignored.
        ch.handle_response(
            SessionQueueQueryResponseMessageV3 {
                ..Default::default()
            }
            .into(),
        );
        assert!(rx.try_recv().is_err());
        ch.handle_response(
            NullResponseMessageV2 {
                correlation_id: Some(-7),
            }
            .into(),
        );
        assert!(rx.try_recv().unwrap().is_ok());

        // An exception completes the call whatever its correlation.
        let mut rx = ch.begin(types::SESS_QUEUEQUERY_RESP_V3, -1);
        ch.handle_response(
            ActiveMQExceptionMessageV2 {
                code: 100,
                message: Some("AMQ229017: Queue q does not exist".into()),
                correlation_id: Some(-99),
            }
            .into(),
        );
        let err = rx.try_recv().unwrap().unwrap_err();
        assert_eq!(err.code(), Some(100));
    }

    #[test]
    fn correlation_only_on_carrying_packets() {
        let mut p: Packet = SessionCommitMessageV2::default().into();
        set_correlation_id(&mut p, -5);
        assert_eq!(correlation_id(&p), -5);
        let mut p: Packet = RollbackMessage::default().into();
        set_correlation_id(&mut p, -5);
        assert_eq!(correlation_id(&p), -1);
    }
}
