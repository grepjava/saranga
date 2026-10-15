//! Channels and blocking calls. Ported from `ChannelImpl`.
//!
//! With a confirmation window (`confirmationWindowSize` other than -1, the
//! client default), a session's channel keeps the async response cache
//! (`ResponseCache`): a send that wants a response takes the cache's next
//! correlation ID (from 1), and its handler is told when the response
//! arrives. The channel also confirms what it receives, with a
//! `PACKETS_CONFIRMED` each time the packets add up to the window. There is
//! no resend cache: after a failover a session is recreated, not
//! reattached, and the sends still awaiting their response fail
//! (`ResponseCache.errorAll`).
//!
//! A blocking call (`ChannelImpl.sendBlocking`) takes the next correlation ID
//! from a per-channel counter that starts at -1 and decrements before use, so
//! the first call gets -2. Only packets that carry a correlation ID on the
//! wire keep it (`SessionSendMessage_V2/V3`, `SessionCommitMessage_V2`); for
//! every other packet `getCorrelationID()` stays -1. A response completes the
//! call when it is an `EXCEPTION`, or when its type is the expected one and
//! its correlation ID equals the request's. Anything else is dropped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use saranga_core_protocol::packets::connection::PacketsConfirmedMessage;
use saranga_core_protocol::{Packet, types};
use saranga_message::CoreMessage;
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::session::{SendAcknowledgementHandler, SessionInner};

/// A send in the async response cache, and the message its handler is
/// told of (none for a large message's chunks before the last).
pub(crate) struct AsyncSend {
    pub(crate) message: Option<CoreMessage>,
    pub(crate) handler: Option<Arc<dyn SendAcknowledgementHandler>>,
}

/// `ChannelImpl.confWindowSize`, `lastConfirmedCommandID` and
/// `receivedBytes`.
struct Confirmations {
    window: i32,
    last_confirmed: i32,
    received_bytes: i64,
}

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
    /// Or where they go, for a channel the application handles itself.
    raw: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Packet>>>,
    /// The confirmation window, when there is one.
    confirmations: Mutex<Option<Confirmations>>,
    /// `ResponseCache`: the sends awaiting their response.
    async_sends: Mutex<HashMap<i64, AsyncSend>>,
    /// `ResponseCache.sequence`.
    async_sequence: AtomicI64,
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
            raw: Mutex::new(None),
            confirmations: Mutex::new(None),
            async_sends: Mutex::new(HashMap::new()),
            async_sequence: AtomicI64::new(0),
        }
    }

    /// Gives the channel a confirmation window (-1 for none), its count
    /// starting again as for a new session (`clearCommands`).
    pub(crate) fn set_confirmation_window(&self, window: i32) {
        *self.confirmations.lock().expect("channel") = (window != -1).then_some(Confirmations {
            window,
            last_confirmed: -1,
            received_bytes: 0,
        });
    }

    /// `ResponseCache.nextCorrelationID` and `add`: the send is kept under
    /// its new correlation ID until its response arrives.
    pub(crate) fn add_async_send(&self, send: AsyncSend) -> i64 {
        let id = self.async_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.async_sends.lock().expect("channel").insert(id, send);
        id
    }

    /// `ResponseCache.remove`: the send was never written.
    pub(crate) fn remove_async_send(&self, correlation_id: i64) {
        self.async_sends
            .lock()
            .expect("channel")
            .remove(&correlation_id);
    }

    /// `ChannelImpl.confirm`: one more packet received. Returns the
    /// `PACKETS_CONFIRMED` to send when the window is full.
    pub(crate) fn confirm(&self, packet_size: usize) -> Option<Packet> {
        let mut c = self.confirmations.lock().expect("channel");
        let c = c.as_mut()?;
        c.last_confirmed += 1;
        c.received_bytes += packet_size as i64;
        if c.received_bytes < i64::from(c.window) {
            return None;
        }
        c.received_bytes = 0;
        Some(
            PacketsConfirmedMessage {
                command_id: c.last_confirmed,
            }
            .into(),
        )
    }

    /// `ResponseCache.handleResponse`: the response of an async send tells
    /// its handler that the send was acknowledged, or that it failed.
    fn handle_async_response(&self, packet: &Packet) {
        let correlation_id = match packet {
            Packet::NullResponseMessageV2(p) => p.correlation_id,
            Packet::ActiveMQExceptionMessageV2(p) => p.correlation_id,
            _ => None,
        };
        let Some(id) = correlation_id else {
            return;
        };
        let Some(send) = self.async_sends.lock().expect("channel").remove(&id) else {
            return;
        };
        let result = match packet {
            Packet::ActiveMQExceptionMessageV2(_) => Err(exception_to_error(packet)),
            _ => Ok(()),
        };
        call_send_ack(send, result);
    }

    /// `ResponseCache.errorAll`: no response will come.
    fn error_all(&self, cause: &Error) {
        let sends: Vec<AsyncSend> = {
            let mut all = self.async_sends.lock().expect("channel");
            let mut ids: Vec<i64> = all.keys().copied().collect();
            ids.sort_unstable();
            ids.into_iter().filter_map(|id| all.remove(&id)).collect()
        };
        for send in sends {
            call_send_ack(send, Err(cause.clone()));
        }
    }

    /// Hands the channel's packets to `tx` from now on.
    pub(crate) fn set_raw(&self, tx: tokio::sync::mpsc::UnboundedSender<Packet>) {
        *self.raw.lock().expect("channel") = Some(tx);
    }

    /// Hands a packet to the application, if it handles the channel;
    /// otherwise gives it back.
    pub(crate) fn handle_raw(&self, packet: Packet) -> std::result::Result<(), Box<Packet>> {
        match self.raw.lock().expect("channel").as_ref() {
            Some(tx) => {
                let _ = tx.send(packet);
                Ok(())
            }
            None => Err(Box::new(packet)),
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
        self.handle_async_response(&packet);
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
        self.return_blocking(cause);
    }

    /// `ChannelImpl.returnBlocking`: the call in progress fails with
    /// `UNBLOCKED`, as its response will never come.
    pub(crate) fn return_blocking(&self, cause: &Error) {
        self.error_all(cause);
        if let Some(p) = self.pending.lock().expect("channel").take() {
            let _ = p.tx.send(Err(Error::unblocking_a_call(cause)));
        }
    }
}

/// `ActiveMQSessionContext.responseHandler`'s `callSendAck`.
fn call_send_ack(send: AsyncSend, result: Result<()>) {
    if let (Some(handler), Some(message)) = (send.handler, send.message) {
        match result {
            Ok(()) => handler.send_acknowledged(&message),
            Err(e) => handler.send_failed(&message, &e),
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
        Packet::SessionSendContinuationMessageV2(p) => p.correlation_id.unwrap_or(-1),
        Packet::SessionSendContinuationMessageV3(p) => p.correlation_id.unwrap_or(-1),
        Packet::SessionCommitMessageV2(p) => p.correlation_id.unwrap_or(-1),
        _ => -1,
    }
}

/// `Packet.setCorrelationID()`: a no-op for packets without one.
pub(crate) fn set_correlation_id(packet: &mut Packet, id: i64) {
    match packet {
        Packet::SessionSendMessageV2(p) => p.correlation_id = id,
        Packet::SessionSendMessageV3(p) => p.correlation_id = id,
        Packet::SessionSendContinuationMessageV2(p) => p.correlation_id = Some(id),
        Packet::SessionSendContinuationMessageV3(p) => p.correlation_id = Some(id),
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
            | types::CLUSTER_CONNECT_REPLY
            | types::QUORUM_VOTE_REPLY
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
