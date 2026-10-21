//! A Core session: one task handling one channel's packets in order.
//! Ported from `ServerSessionPacketHandler` (dispatch, responses and
//! confirmations); the work itself is the protocol-neutral
//! [`ServerSession`].

use std::collections::HashMap;
use std::sync::Arc;

use saranga_buffer::{JString, SimpleString};
use saranga_core_protocol::packets::connection::{
    ActiveMQExceptionMessage, ActiveMQExceptionMessageV2, NullResponseMessage,
    NullResponseMessageV2, PacketsConfirmedMessage,
};
use saranga_core_protocol::packets::session::{
    BindingQueryDefaults, SessionBindingQueryResponseMessage, SessionBindingQueryResponseMessageV2,
    SessionBindingQueryResponseMessageV3, SessionBindingQueryResponseMessageV4,
    SessionBindingQueryResponseMessageV5, SessionQueueQueryResponseMessage,
    SessionQueueQueryResponseMessageV2, SessionQueueQueryResponseMessageV3,
};
use saranga_core_protocol::packets::xa::{
    SessionXAGetInDoubtXidsResponseMessage, SessionXAGetTimeoutResponseMessage,
    SessionXAResponseMessage, SessionXASetTimeoutResponseMessage,
};
use saranga_core_protocol::{ErrorType, Flagged, Packet, version};
use saranga_message::CoreMessage;
use saranga_security::Authenticated;
use tokio::sync::mpsc;

use super::{ConnShared, CoreDeliverySink};
use crate::large::LargeWriter;
use crate::postoffice::{BindingQueryResult, QueueQueryResult, QueueSpec, RoutingTypes};
use crate::queue::AckResult;
use crate::session::{AckPending, Completion, ServerSession, SessionOptions};
use crate::transaction::{TxError, xa_code};
use crate::{Broker, BrokerError, Result, RoutingType};

/// An embedded message and its large message's file.
type Embedded = (
    Arc<dyn crate::message::NativeMessage>,
    Option<Arc<crate::large::LargeBody>>,
);

/// What the connection hands a session task.
pub(crate) enum SessionInput {
    /// A packet and its size on the wire, for confirmations.
    Packet(Box<Packet>, usize),
    ConnectionFailed,
    /// Management closes the session (`closeSessionWithID`).
    Close(bool),
}

/// The fields of `CreateSessionMessage` / `_V2` the session keeps.
pub(crate) struct CreateSession {
    pub name: String,
    pub channel_id: i64,
    pub version: i32,
    pub username: Option<String>,
    pub password: Option<String>,
    pub min_large_message_size: i32,
    pub xa: bool,
    pub auto_commit_sends: bool,
    pub auto_commit_acks: bool,
    pub confirmation_window_size: i32,
    pub pre_acknowledge: bool,
    pub default_address: Option<String>,
    pub client_id: Option<String>,
}

/// `ChannelImpl.confirm` state, used when the client set a confirmation
/// window.
struct Confirmations {
    window: i32,
    last_command_id: i32,
    received_bytes: i64,
}

/// A request's confirmation and response, sent once the journal writes it
/// lined up are done (`OperationContext.executeOnCompletion` with
/// `doConfirmAndResponse`).
struct Done {
    completion: Completion,
    request: Request,
    response: Option<Packet>,
    flush: bool,
}

/// Sends confirmations and responses in request order, each after the
/// journal writes its request lined up. The session goes on with the next
/// packets meanwhile, as Artemis's handler does.
async fn completer(
    conn: Arc<ConnShared>,
    channel_id: i64,
    mut confirmations: Confirmations,
    mut rx: mpsc::UnboundedReceiver<Done>,
) {
    while let Some(done) = rx.recv().await {
        let response = match done.completion.wait().await {
            // OperationContext.onError: the client gets the I/O error.
            Err(e) => Some(exception_packet(
                done.request,
                &BrokerError::new(ErrorType::IoError, e.to_string()),
            )),
            Ok(()) => done.response,
        };
        let failed = matches!(
            response,
            Some(Packet::ActiveMQExceptionMessage(_) | Packet::ActiveMQExceptionMessageV2(_))
        );
        if !failed && let Some(id) = confirmations.confirm(done.request, done.flush) {
            conn.send(channel_id, PacketsConfirmedMessage { command_id: id });
        }
        if let Some(response) = response {
            conn.send(channel_id, response);
        }
    }
}

impl Confirmations {
    /// `ChannelImpl.confirm` / `flushConfirmations`: the command ID to
    /// confirm, when it is time to send one.
    fn confirm(&mut self, request: Request, flush: bool) -> Option<i32> {
        if self.window == -1 || !request.requires_confirmations {
            return None;
        }
        self.last_command_id += 1;
        self.received_bytes += request.size as i64;
        if self.received_bytes >= i64::from(self.window) || (flush && self.received_bytes != 0) {
            self.received_bytes = 0;
            return Some(self.last_command_id);
        }
        None
    }
}

/// `convertToExceptionPacket`.
fn exception_packet(request: Request, e: &BrokerError) -> Packet {
    let message = Some(JString::from(e.message.as_str()));
    if request.response_async {
        ActiveMQExceptionMessageV2 {
            code: e.kind.code(),
            message,
            correlation_id: Some(request.correlation_id),
        }
        .into()
    } else {
        ActiveMQExceptionMessage {
            code: e.kind.code(),
            message,
        }
        .into()
    }
}

pub(crate) struct CoreSession {
    session: ServerSession,
    broker: Arc<Broker>,
    conn: Arc<ConnShared>,
    channel_id: i64,
    /// The size of the chunks large messages are sent to consumers in.
    min_large_message_size: i32,
    /// The client's producer IDs and the names the session knows them by.
    producers: HashMap<i32, Arc<str>>,
    /// The large message being received (`currentLargeMessage`): its
    /// headers, and its body so far.
    large: Option<(CoreMessage, LargeWriter)>,
    completer: mpsc::UnboundedSender<Done>,
    /// Whether the client confirms commands (a confirmation window).
    confirms: bool,
    /// Where the queues answer the acknowledgements sent without waiting
    /// ([`ack_now`](Self::ack_now)), until [`run`](Self::run) takes it.
    acked: mpsc::UnboundedSender<Acked>,
    acked_rx: Option<mpsc::UnboundedReceiver<Acked>>,
    /// How many of those are not yet answered.
    in_flight: usize,
}

/// An acknowledgement the session did not wait for, as its queue answered.
type Acked = (Request, AckPending, AckResult);

/// How a request packet wants its response (`Packet.getCorrelationID`,
/// `isResponseAsync`).
#[derive(Clone, Copy)]
struct Request {
    correlation_id: i64,
    response_async: bool,
    requires_confirmations: bool,
    size: usize,
}

impl Request {
    fn of(packet: &Packet, size: usize) -> Self {
        let (correlation_id, response_async) = match packet {
            Packet::SessionSendMessageV2(p) => (p.correlation_id, true),
            Packet::SessionSendMessageV3(p) => (p.correlation_id, true),
            Packet::SessionCommitMessageV2(p) => (p.correlation_id.unwrap_or(-1), true),
            Packet::SessionSendContinuationMessageV2(p) => (p.correlation_id.unwrap_or(-1), true),
            Packet::SessionSendContinuationMessageV3(p) => (p.correlation_id.unwrap_or(-1), true),
            _ => (-1, false),
        };
        let requires_confirmations = match packet {
            Packet::SessionAddMetaDataMessage(_)
            | Packet::SessionRequestProducerCreditsMessage(_) => false,
            Packet::SessionAddMetaDataMessageV2(p) => p.requires_confirmation,
            Packet::SessionUniqueAddMetaDataMessage(p) => p.requires_confirmation,
            _ => true,
        };
        Self {
            correlation_id,
            response_async,
            requires_confirmations,
            size,
        }
    }
}

impl CoreSession {
    pub(crate) fn new(
        broker: Arc<Broker>,
        conn: Arc<ConnShared>,
        request: CreateSession,
        authenticated: Authenticated,
    ) -> Self {
        let (completer_tx, completer_rx) = mpsc::unbounded_channel();
        let (acked, acked_rx) = mpsc::unbounded_channel();
        let confirms = request.confirmation_window_size != -1;
        tokio::spawn(completer(
            conn.clone(),
            request.channel_id,
            Confirmations {
                window: request.confirmation_window_size,
                last_command_id: -1,
                received_bytes: 0,
            },
            completer_rx,
        ));
        let session = ServerSession::new(
            broker.clone(),
            conn.client.clone(),
            SessionOptions {
                name: request.name,
                username: request.username,
                password: request.password,
                xa: request.xa,
                auto_commit_sends: request.auto_commit_sends,
                auto_commit_acks: request.auto_commit_acks,
                pre_acknowledge: request.pre_acknowledge,
                default_address: request.default_address,
            },
            authenticated,
        );
        if request.version < version::ARTEMIS_2_28_0 {
            session.entry().set_legacy_producers();
        }
        Self {
            session,
            broker,
            conn,
            channel_id: request.channel_id,
            min_large_message_size: request.min_large_message_size,
            producers: HashMap::new(),
            large: None,
            completer: completer_tx,
            confirms,
            acked,
            acked_rx: Some(acked_rx),
            in_flight: 0,
        }
    }

    /// What management sees of the session.
    pub(crate) fn entry(&self) -> &Arc<crate::registry::SessionEntry> {
        self.session.entry()
    }

    pub(crate) fn blocked_runnables(&self) -> &Arc<crate::paging::BlockedRunnables> {
        self.session.blocked_runnables()
    }

    /// Handles the session's packets; `backlog` counts the bytes of those
    /// read and not yet handled.
    pub(crate) async fn run(
        mut self,
        mut rx: mpsc::UnboundedReceiver<SessionInput>,
        backlog: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mut acked = self.acked_rx.take().expect("run once");
        loop {
            let input = tokio::select! {
                biased;
                Some(a) = acked.recv(), if self.in_flight > 0 => {
                    self.acked(a);
                    continue;
                }
                input = rx.recv() => input,
            };
            let Some(input) = input else {
                break;
            };
            match input {
                SessionInput::Packet(packet, size) => {
                    // Only credits and more such acknowledgements go ahead
                    // of the acknowledgements on their way: anything else
                    // finds them done, as it would have in turn.
                    if !self.ack_now(&packet)
                        && !matches!(*packet, Packet::SessionConsumerFlowCreditMessage(_))
                    {
                        self.settle(&mut acked).await;
                    }
                    self.handle(*packet, size).await;
                    backlog.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
                    if self.session.is_closed() {
                        break;
                    }
                }
                SessionInput::ConnectionFailed => {
                    self.settle(&mut acked).await;
                    self.close(true).await;
                    break;
                }
                SessionInput::Close(_force) => {
                    // ServerSessionImpl.close(failed = true, force).
                    self.settle(&mut acked).await;
                    self.close(true).await;
                    break;
                }
            }
        }
        self.settle(&mut acked).await;
        if !self.session.is_closed() {
            self.close(true).await;
        }
    }

    /// Whether `packet` is an acknowledgement sent to its queue without
    /// waiting for the answer: one outside a transaction that asks for no
    /// response or confirmation. Waiting for each held the consumer's
    /// credits, which follow it, behind everything its queue had to do
    /// first, and so the consumer behind its producer.
    fn ack_now(&self, packet: &Packet) -> bool {
        let (consumer_id, requires_response) = match packet {
            Packet::SessionAcknowledgeMessage(p) => (p.consumer_id, p.requires_response),
            Packet::SessionIndividualAcknowledgeMessage(p) => (p.consumer_id, p.requires_response),
            _ => return false,
        };
        !requires_response
            && !self.confirms
            && self.session.auto_commit_acks()
            && self.session.has_consumer(consumer_id)
    }

    /// Sends an [`ack_now`](Self::ack_now) acknowledgement on its way.
    fn ack_in_flight(
        &mut self,
        request: Request,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
    ) {
        let acked = self.acked.clone();
        let sent = self.session.acknowledge_then(
            consumer_id,
            message_id,
            individual,
            move |pending, result| {
                if let Err(mpsc::error::SendError((_, pending, result))) =
                    acked.send((request, pending, result))
                {
                    pending.finish_alone(result);
                }
            },
        );
        match sent {
            Ok(()) => self.in_flight += 1,
            Err(e) => {
                let response = self.outcome(request, false, Err(e));
                self.respond(request, response, false);
            }
        }
    }

    /// The rest of an acknowledgement sent on its way, as its queue
    /// answered.
    fn acked(&mut self, (request, pending, result): Acked) {
        self.in_flight -= 1;
        let result = self.session.acknowledged(pending, result).map(|_| None);
        let response = self.outcome(request, false, result);
        self.respond(request, response, false);
    }

    /// Waits for the queues to answer every acknowledgement on its way.
    async fn settle(&mut self, acked: &mut mpsc::UnboundedReceiver<Acked>) {
        while self.in_flight > 0 {
            match acked.recv().await {
                Some(a) => self.acked(a),
                None => break,
            }
        }
    }

    async fn close(&mut self, failed: bool) {
        self.session.close(failed).await;
        self.producers.clear();
    }

    fn null_response(&self, request: Request) -> Packet {
        if self
            .conn
            .context()
            .is_version_before_async_response_change()
        {
            NullResponseMessage {}.into()
        } else {
            NullResponseMessageV2 {
                correlation_id: Some(request.correlation_id),
            }
            .into()
        }
    }

    fn exception(&self, request: Request, e: &BrokerError) -> Packet {
        exception_packet(request, e)
    }

    /// `doConfirmAndResponse`, after the journal writes this request lined
    /// up.
    fn respond(&mut self, request: Request, response: Option<Packet>, flush: bool) {
        let _ = self.completer.send(Done {
            completion: self.session.take_completion(),
            request,
            response,
            flush,
        });
    }

    /// The response for a request that may fail: the given success
    /// response (if any is required), or the exception.
    fn outcome(
        &self,
        request: Request,
        requires_response: bool,
        result: Result<Option<Packet>>,
    ) -> Option<Packet> {
        match result {
            Ok(Some(p)) => Some(p),
            Ok(None) if requires_response => Some(self.null_response(request)),
            Ok(None) => None,
            Err(e) if requires_response => Some(self.exception(request, &e)),
            Err(e) => {
                tracing::warn!(session = %self.session.name(), "{e}");
                None
            }
        }
    }

    /// `ServerSessionImpl.send` of a whole message, or of a large one whose
    /// body has arrived.
    async fn send_message(
        &mut self,
        message: CoreMessage,
        large: Option<Arc<crate::large::LargeBody>>,
    ) -> Result<Option<Packet>> {
        // `EmbedMessageUtil.extractEmbedded`: another protocol's message a
        // bridge carried.
        match self.extract_embedded(&message, large.as_ref()) {
            Some((native, None)) => self.session.send_native(native).await.map(|()| None),
            Some((native, Some(body))) => self
                .session
                .send_native_large(native, body)
                .await
                .map(|()| None),
            None => self.session.send(message, large).await.map(|()| None),
        }
    }

    /// The message embedded in an `EMBEDDED_TYPE` message's body or a
    /// `LARGE_EMBEDDED_TYPE` message's `_AMQP_EMBED_LARGE`, with the large
    /// message's file; `None` for any other message, or one that does not
    /// decode (it goes on as it is).
    fn extract_embedded(
        &self,
        message: &CoreMessage,
        large: Option<&Arc<crate::large::LargeBody>>,
    ) -> Option<Embedded> {
        use crate::bridge::{
            AMQP_EMBED_LARGE, EMBED_SIGNATURE, EMBEDDED_TYPE, LARGE_EMBEDDED_TYPE,
        };
        use saranga_storage::message::PersistedMessage;
        let decoders = self.broker.post_office().paging().decoders();
        let decode = |bytes: &[u8], id: i64| -> Option<PersistedMessage> {
            match <PersistedMessage as saranga_storage::RecordCodec>::decode(bytes) {
                Ok(mut record) => {
                    match &mut record {
                        PersistedMessage::Core(m) => m.message_id = id,
                        PersistedMessage::Amqp(m) => m.message_id = id,
                        PersistedMessage::AmqpLarge(m) => m.message_id = id,
                    }
                    Some(record)
                }
                Err(e) => {
                    tracing::warn!("{e}");
                    None
                }
            }
        };
        match message.message_type {
            EMBEDDED_TYPE => {
                let Some(encoded) = message.body.strip_prefix(EMBED_SIGNATURE) else {
                    tracing::trace!(
                        "Message type {EMBEDDED_TYPE} was used for something other than embed messages, ignoring content and treating as a regular message"
                    );
                    return None;
                };
                // A new ID comes with the send.
                let record = decode(encoded, 0)?;
                match decoders.decode(&record) {
                    Ok(m) => m.native().cloned().map(|n| (n, None)),
                    Err(e) => {
                        tracing::warn!("{e}");
                        None
                    }
                }
            }
            LARGE_EMBEDDED_TYPE => {
                let body = large?.clone();
                let encoded = match message
                    .properties
                    .get(&saranga_buffer::SimpleString::from(AMQP_EMBED_LARGE))
                {
                    Some(saranga_buffer::PropertyValue::Bytes(b)) => b.clone(),
                    _ => return None,
                };
                // The message is its file's.
                let record = decode(&encoded, message.message_id)?;
                match decoders.decode_large(&record, body.clone()) {
                    Ok(m) => m.native().cloned().map(|n| (n, Some(body))),
                    Err(e) => {
                        tracing::warn!("{e}");
                        None
                    }
                }
            }
            _ => None,
        }
    }

    async fn acknowledge(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
    ) -> Result<Option<Packet>> {
        self.session
            .acknowledge(consumer_id, message_id, individual)
            .await
            .map(|_| None)
    }

    async fn handle(&mut self, packet: Packet, size: usize) {
        let request = Request::of(&packet, size);
        if self.ack_now(&packet) {
            match packet {
                Packet::SessionAcknowledgeMessage(p) => {
                    self.ack_in_flight(request, p.consumer_id, p.message_id, false)
                }
                Packet::SessionIndividualAcknowledgeMessage(p) => {
                    self.ack_in_flight(request, p.consumer_id, p.message_id, true)
                }
                _ => unreachable!("ack_now is for acknowledgements"),
            }
            return;
        }
        let (requires_response, result, flush) = match packet {
            Packet::SessionSendMessage(p) => (
                p.requires_response,
                self.send_message(p.message, None).await,
                false,
            ),
            Packet::SessionSendMessageV2(p) => (
                p.requires_response,
                self.send_message(p.message, None).await,
                false,
            ),
            Packet::SessionSendMessageV3(p) => {
                self.session
                    .set_sender(self.producers.get(&p.sender_id).cloned());
                let result = self.send_message(p.message, None).await;
                self.session.set_sender(None);
                (p.requires_response, result, false)
            }
            Packet::SessionSendMessage1X(p) => (
                p.requires_response,
                self.send_message(p.message, None).await,
                false,
            ),
            Packet::SessionAcknowledgeMessage(p) => (
                p.requires_response,
                self.acknowledge(p.consumer_id, p.message_id, false).await,
                false,
            ),
            Packet::SessionIndividualAcknowledgeMessage(p) => (
                p.requires_response,
                self.acknowledge(p.consumer_id, p.message_id, true).await,
                false,
            ),
            Packet::SessionExpireMessage(p) => {
                let result = self.session.expire(p.consumer_id, p.message_id);
                (false, result.map(|()| None), false)
            }
            Packet::SessionRequestProducerCreditsMessage(p) => {
                // The connection answers these as it reads them; this is
                // for any that reach the session all the same.
                super::producer_credits(
                    &self.broker,
                    &self.conn,
                    self.channel_id,
                    p,
                    self.session.blocked_runnables(),
                );
                (false, Ok(None), false)
            }
            Packet::SessionConsumerFlowCreditMessage(p) => {
                self.session.consumer_credits(p.consumer_id, p.credits);
                (false, Ok(None), false)
            }
            Packet::SessionCreateConsumerMessage(p) => {
                let queue_name = p.queue_name.to_string_lossy();
                let sink = self.sink_factory();
                let queue_name = self.session.remove_prefix(&queue_name).to_owned();
                let result = self
                    .session
                    .create_consumer(
                        p.id,
                        &queue_name,
                        p.filter_string.map(|f| f.to_string_lossy()),
                        p.priority,
                        p.browse_only,
                        sink,
                    )
                    .await;
                let result = result.map(|()| {
                    p.requires_response
                        .then(|| self.queue_query_response(&p.queue_name.to_string_lossy()))
                });
                (p.requires_response, result, false)
            }
            Packet::CreateAddressMessage(p) => {
                let mut types = RoutingTypes::default();
                p.routing_types
                    .iter()
                    .filter_map(|&b| RoutingType::from_byte(b))
                    .for_each(|t| types.add(t));
                let result = self
                    .session
                    .create_address(&p.address.to_string_lossy(), types, p.auto_created)
                    .map(|()| None);
                (p.requires_response, result, false)
            }
            Packet::CreateQueueMessage(p) => {
                let address = p.address.to_string_lossy();
                let mut spec = QueueSpec::new(p.queue_name.to_string_lossy(), address.as_str());
                spec.routing_type = Some(legacy_routing_type(&address));
                spec.filter_string = p.filter_string.map(|f| f.to_string_lossy());
                spec.durable = p.durable;
                spec.temporary = p.temporary;
                self.session.unprefix_queue(&mut spec);
                (
                    p.requires_response,
                    self.session.create_queue(spec).map(|_| None),
                    false,
                )
            }
            Packet::CreateQueueMessageV2(p) => {
                let mut spec =
                    QueueSpec::new(p.queue_name.to_string_lossy(), p.address.to_string_lossy());
                spec.routing_type = RoutingType::from_byte(p.routing_type);
                spec.filter_string = p.filter_string.map(|f| f.to_string_lossy());
                spec.durable = p.durable;
                spec.temporary = p.temporary;
                spec.auto_created = p.auto_created;
                spec.max_consumers = Some(p.max_consumers);
                spec.purge_on_no_consumers = Some(p.purge_on_no_consumers);
                spec.exclusive = p.exclusive.0;
                spec.last_value = p.last_value.0;
                spec.last_value_key = p.last_value_key.map(|k| k.to_string_lossy());
                spec.non_destructive = p.non_destructive.0;
                spec.consumers_before_dispatch = p.consumers_before_dispatch.0;
                spec.delay_before_dispatch = p.delay_before_dispatch.0;
                spec.group_rebalance = p.group_rebalance.0;
                spec.group_buckets = p.group_buckets.0;
                spec.auto_delete = p.auto_delete.0;
                spec.auto_delete_delay = p.auto_delete_delay.0;
                spec.auto_delete_message_count = p.auto_delete_message_count.0;
                spec.group_first_key = p.group_first_key.map(|k| k.to_string_lossy());
                spec.ring_size = p.ring_size.0;
                spec.enabled = p.enabled.0;
                spec.group_rebalance_pause_dispatch = p.group_rebalance_pause_dispatch.0;
                self.session.unprefix_queue(&mut spec);
                (
                    p.requires_response,
                    self.session.create_queue(spec).map(|_| None),
                    false,
                )
            }
            Packet::CreateSharedQueueMessage(p) => {
                let address = p.address.to_string_lossy();
                let mut spec = QueueSpec::new(p.queue_name.to_string_lossy(), address.as_str());
                spec.routing_type = Some(RoutingType::Multicast);
                spec.filter_string = p.filter_string.map(|f| f.to_string_lossy());
                spec.durable = p.durable;
                (
                    p.requires_response,
                    self.session.create_shared_queue(spec).map(|()| None),
                    false,
                )
            }
            Packet::CreateSharedQueueMessageV2(p) => {
                let mut spec =
                    QueueSpec::new(p.queue_name.to_string_lossy(), p.address.to_string_lossy());
                spec.routing_type = RoutingType::from_byte(p.routing_type);
                spec.filter_string = p.filter_string.map(|f| f.to_string_lossy());
                spec.durable = p.durable;
                spec.max_consumers = p.max_consumers.0;
                spec.purge_on_no_consumers = p.purge_on_no_consumers.0;
                spec.exclusive = p.exclusive.0;
                spec.last_value = p.last_value.0;
                spec.last_value_key = p.last_value_key.map(|k| k.to_string_lossy());
                spec.non_destructive = p.non_destructive.0;
                spec.consumers_before_dispatch = p.consumers_before_dispatch.0;
                spec.delay_before_dispatch = p.delay_before_dispatch.0;
                spec.group_rebalance = p.group_rebalance.0;
                spec.group_buckets = p.group_buckets.0;
                spec.auto_delete = p.auto_delete.0;
                spec.auto_delete_delay = p.auto_delete_delay.0;
                spec.auto_delete_message_count = p.auto_delete_message_count.0;
                spec.group_first_key = p.group_first_key.map(|k| k.to_string_lossy());
                spec.ring_size = p.ring_size.0;
                spec.enabled = p.enabled.0;
                spec.group_rebalance_pause_dispatch = p.group_rebalance_pause_dispatch.0;
                (
                    p.requires_response,
                    self.session.create_shared_queue(spec).map(|()| None),
                    false,
                )
            }
            Packet::SessionDeleteQueueMessage(p) => (
                true,
                self.session
                    .delete_queue(&p.queue_name.to_string_lossy())
                    .await
                    .map(|()| None),
                false,
            ),
            Packet::SessionQueueQueryMessage(p) => {
                let response =
                    self.queue_query_response_legacy(&p.queue_name.to_string_lossy(), true);
                (true, Ok(Some(response)), false)
            }
            Packet::SessionBindingQueryMessage(p) => {
                let response = self.binding_query_response(&p.address.to_string_lossy());
                (true, Ok(Some(response)), false)
            }
            Packet::SessionCommitMessage(_) | Packet::SessionCommitMessageV2(_) => {
                (true, self.session.commit().map(|()| None), false)
            }
            Packet::RollbackMessage(p) => {
                self.session
                    .rollback(p.consider_last_message_as_delivered, false)
                    .await;
                (true, Ok(None), false)
            }
            Packet::SessionStart(_) => {
                self.session.set_started(true).await;
                (false, Ok(None), false)
            }
            Packet::SessionStop(_) => {
                self.session.set_started(false).await;
                (true, Ok(None), false)
            }
            Packet::SessionCloseMessage(_) => {
                self.close(false).await;
                (true, Ok(None), true)
            }
            Packet::SessionConsumerCloseMessage(p) => {
                self.session.close_consumer(p.consumer_id, false).await;
                (true, Ok(None), false)
            }
            Packet::SessionForceConsumerDelivery(p) => {
                self.session.force_delivery(p.consumer_id, p.sequence);
                (false, Ok(None), false)
            }
            Packet::SessionAddMetaDataMessage(p) => {
                self.broker.add_session_metadata(
                    self.session.name(),
                    &p.key.to_string_lossy(),
                    &p.data.to_string_lossy(),
                );
                (true, Ok(None), false)
            }
            Packet::SessionAddMetaDataMessageV2(p) => {
                self.broker.add_session_metadata(
                    self.session.name(),
                    &p.key.to_string_lossy(),
                    &p.data.to_string_lossy(),
                );
                (p.requires_confirmation, Ok(None), false)
            }
            Packet::SessionUniqueAddMetaDataMessage(p) => {
                let (key, data) = (p.key.to_string_lossy(), p.data.to_string_lossy());
                let result =
                    if self
                        .broker
                        .add_unique_session_metadata(self.session.name(), &key, &data)
                    {
                        Ok(None)
                    } else {
                        Err(BrokerError::duplicate_metadata(&key, &data))
                    };
                (true, result, false)
            }
            Packet::CreateProducerMessage(p) => {
                if self.producers.contains_key(&p.id) {
                    tracing::warn!(
                        "AMQ222316: Unable to create producer with ID {}: one already exists in session {}",
                        p.id,
                        self.session.name()
                    );
                } else {
                    // PRODUCER_ID_PREFIX and a new UUID.
                    let name = format!("artemis:sender:ID:{}", uuid::Uuid::new_v4());
                    let address = p.address.map(|a| a.to_string_lossy());
                    self.session.add_producer(&name, "CORE", address.as_deref());
                    self.producers.insert(p.id, name.into());
                }
                (false, Ok(None), false)
            }
            Packet::RemoveProducerMessage(p) => {
                match self.producers.remove(&p.id) {
                    Some(name) => self.session.remove_producer(&name),
                    None => tracing::warn!(
                        "AMQ222317: Unable to remove producer with ID {}: it does not exist in session {}",
                        p.id,
                        self.session.name()
                    ),
                }
                (false, Ok(None), false)
            }
            Packet::SessionXAStartMessage(p) => {
                (true, Self::xa_response(self.session.xa_start(p.xid)), false)
            }
            Packet::SessionXAEndMessage(p) => {
                (true, Self::xa_response(self.session.xa_end(p.xid)), false)
            }
            Packet::SessionXAPrepareMessage(p) => (
                true,
                Self::xa_response(self.session.xa_prepare(p.xid)),
                false,
            ),
            Packet::SessionXACommitMessage(p) => (
                true,
                Self::xa_response(self.session.xa_commit(p.xid, p.one_phase)),
                false,
            ),
            Packet::SessionXARollbackMessage(p) => {
                let result = self.session.xa_rollback(p.xid).await;
                (true, Self::xa_response(result), false)
            }
            Packet::SessionXAJoinMessage(p) => {
                (true, Self::xa_response(self.session.xa_join(p.xid)), false)
            }
            Packet::SessionXAResumeMessage(p) => (
                true,
                Self::xa_response(self.session.xa_resume(p.xid)),
                false,
            ),
            Packet::SessionXASuspend(_) => {
                (true, Self::xa_response(self.session.xa_suspend()), false)
            }
            Packet::SessionXAForgetMessage(p) => (
                true,
                Self::xa_response(self.session.xa_forget(p.xid)),
                false,
            ),
            Packet::SessionXAAfterFailedMessage(p) => {
                self.session.xa_failed(p.xid);
                // No response for this one.
                (false, Ok(None), false)
            }
            Packet::SessionXAInDoubtXids(_) => {
                let xids = self.broker.resource_manager().in_doubt();
                (
                    true,
                    Ok(Some(SessionXAGetInDoubtXidsResponseMessage { xids }.into())),
                    false,
                )
            }
            Packet::SessionXAGetTimeout(_) => (
                true,
                Ok(Some(
                    SessionXAGetTimeoutResponseMessage {
                        timeout_seconds: self.broker.resource_manager().timeout_seconds(),
                    }
                    .into(),
                )),
                false,
            ),
            Packet::SessionXASetTimeoutMessage(p) => {
                self.session.xa_set_timeout(p.timeout_seconds);
                (
                    true,
                    Ok(Some(SessionXASetTimeoutResponseMessage { ok: true }.into())),
                    false,
                )
            }
            Packet::SessionSendLargeMessage(p) => (false, self.send_large(p.message), false),
            Packet::SessionSendContinuationMessage(p) => (
                p.requires_response,
                self.send_continuation(p.body, p.continues, p.message_body_size)
                    .await,
                false,
            ),
            Packet::SessionSendContinuationMessageV2(p) => (
                p.requires_response,
                self.send_continuation(p.body, p.continues, p.message_body_size)
                    .await,
                false,
            ),
            Packet::SessionSendContinuationMessageV3(p) => {
                self.session
                    .set_sender(self.producers.get(&p.sender_id).cloned());
                let result = self
                    .send_continuation(p.body, p.continues, p.message_body_size)
                    .await;
                self.session.set_sender(None);
                (p.requires_response, result, false)
            }
            // `slowPacketHandler` has no case for it: confirmed, with no
            // response.
            other => {
                tracing::debug!(session = %self.session.name(), "ignored packet {}", other.name());
                (false, Ok(None), false)
            }
        };
        let response = self.outcome(request, requires_response, result);
        self.respond(request, response, flush);
    }

    /// `sendLarge`: a large message's headers. Its body follows in
    /// continuations.
    fn send_large(&mut self, mut message: CoreMessage) -> Result<Option<Packet>> {
        let id = self.broker.post_office().ids().next();
        message.message_id = id;
        // onLargeMessageCreate: a durable message's headers must fit a
        // journal record.
        if message.durable
            && let Some(max) = self.broker.max_record_size()
        {
            let mut w = saranga_buffer::Writer::new();
            message.encode_headers_and_properties(&mut w);
            if w.len() > max {
                tracing::warn!(
                    "AMQ224099: Message with ID {id} has a header too large. More information available on debug level for class {}",
                    module_path!()
                );
                return Err(BrokerError::new(
                    ErrorType::IoError,
                    format!(
                        "AMQ149005: Message of {} bytes is bigger than the max record size of {max} bytes. You should try to move large application properties to the message body.",
                        w.len()
                    ),
                ));
            }
        }
        let writer = self
            .broker
            .large_messages()
            .create(id, message.durable)
            .map_err(|e| BrokerError::new(ErrorType::IoError, e.to_string()))?;
        if let Some((old, _)) = self.large.replace((message, writer)) {
            tracing::warn!(
                "AMQ222106: Replacing incomplete LargeMessage with ID={}",
                old.message_id
            );
        }
        Ok(None)
    }

    /// `sendContinuations`: a chunk of the large message's body; with the
    /// last one the message is sent.
    async fn send_continuation(
        &mut self,
        body: Vec<u8>,
        continues: bool,
        body_size: i64,
    ) -> Result<Option<Packet>> {
        let Some((_, writer)) = &mut self.large else {
            return Err(BrokerError::new(
                ErrorType::IllegalState,
                "AMQ229030: large-message not initialized on server",
            ));
        };
        if let Err(e) = writer.append(&body) {
            self.large = None;
            return Err(BrokerError::new(ErrorType::IoError, e.to_string()));
        }
        if continues {
            return Ok(None);
        }
        let (mut message, writer) = self.large.take().expect("checked above");
        let body = writer
            .finish()
            .map_err(|e| BrokerError::new(ErrorType::IoError, e.to_string()))?;
        if body_size >= 0 {
            message.properties.insert(
                crate::message::HDR_LARGE_BODY_SIZE.into(),
                saranga_buffer::PropertyValue::Long(body_size),
            );
        }
        self.send_message(message, Some(body)).await
    }

    /// `createSessionXAResponseMessage` and
    /// `onActiveMQXAExceptionWhileHandlePacket`: XA requests are never
    /// response-async, so the answer is always the V1 message.
    fn xa_response(result: Result<(), TxError>) -> Result<Option<Packet>> {
        match result {
            Ok(()) => Ok(Some(
                SessionXAResponseMessage {
                    error: false,
                    response_code: xa_code::XA_OK,
                    message: None,
                }
                .into(),
            )),
            Err(TxError::Xa(e)) => Ok(Some(
                SessionXAResponseMessage {
                    error: true,
                    response_code: e.code,
                    message: e.message.as_deref().map(JString::from),
                }
                .into(),
            )),
            Err(TxError::Broker(e)) => Err(e),
        }
    }

    /// Makes a consumer's `CoreDeliverySink` once its queue is known.
    fn sink_factory(
        &self,
    ) -> impl FnOnce(&crate::queue::Queue) -> Arc<dyn crate::queue::DeliverySink> + use<> {
        let conn = self.conn.clone();
        let channel_id = self.channel_id;
        let chunk_size = self.min_large_message_size.max(1) as usize;
        let legacy = self.legacy_jms();
        move |queue| {
            Arc::new(CoreDeliverySink {
                conn,
                channel_id,
                chunk_size,
                legacy_prefix: legacy.then_some(
                    if queue.config().routing_type == RoutingType::Anycast {
                        OLD_QUEUE_PREFIX
                    } else {
                        OLD_TOPIC_PREFIX
                    },
                ),
            })
        }
    }

    /// A 1.x JMS client (`JMS_SESSION_IDENTIFIER_PROPERTY` set by a client
    /// older than `ADDRESSING_CHANGE_VERSION`): it names queues and topics
    /// with the old prefixes.
    fn legacy_jms(&self) -> bool {
        self.conn.context().is_version_before_address_change()
            && self
                .broker
                .session_metadata(self.session.name(), JMS_SESSION_IDENTIFIER)
                .is_some()
    }

    fn queue_query_response(&self, name: &str) -> Packet {
        self.queue_query_response_legacy(name, false)
    }

    /// With `legacy` (`SESS_QUEUEQUERY`), an old client gets the address
    /// with the old prefix of the queue's routing type.
    fn queue_query_response_legacy(&self, name: &str, legacy: bool) -> Packet {
        let mut r = self.session.queue_query(name);
        if legacy && r.exists && self.conn.context().is_version_before_address_change() {
            r.address = old_prefixed_address(&r.address, r.routing_type).into();
        }
        let v = self.conn.version();
        if v >= version::ADDRESSING_CHANGE {
            queue_query_v3(r).into()
        } else if v >= 126 {
            SessionQueueQueryResponseMessageV2 {
                exists: r.exists,
                durable: r.durable,
                temporary: r.temporary,
                consumer_count: r.consumer_count,
                message_count: r.message_count,
                filter_string: r.filter_string.as_deref().map(SimpleString::from),
                address: Some(SimpleString::from(&*r.address)),
                name: Some(SimpleString::from(&*r.name)),
                auto_create_queues: r.auto_create_queues,
            }
            .into()
        } else {
            SessionQueueQueryResponseMessage {
                exists: r.exists,
                durable: r.durable,
                temporary: r.temporary,
                consumer_count: r.consumer_count,
                message_count: r.message_count,
                filter_string: r.filter_string.as_deref().map(SimpleString::from),
                address: Some(SimpleString::from(&*r.address)),
                name: Some(SimpleString::from(&*r.name)),
            }
            .into()
        }
    }

    fn binding_query_response(&self, address: &str) -> Packet {
        let ctx = self.conn.context();
        let r = self
            .session
            .binding_query(address, ctx.is_version_new_fqqn());
        let v = ctx.version;
        // An old JMS client asking about jms.queue.X or jms.topic.X gets the
        // queue names with that prefix (`convertQueueNames`).
        let legacy_prefix = (r.exists && self.legacy_jms())
            .then(|| {
                [OLD_QUEUE_PREFIX, OLD_TOPIC_PREFIX]
                    .into_iter()
                    .find(|p| address.starts_with(p))
            })
            .flatten();
        let names: Vec<SimpleString> = r
            .queue_names
            .iter()
            .map(|n| match legacy_prefix {
                Some(p) if !n.starts_with(p) => SimpleString::from(format!("{p}{n}").as_str()),
                _ => SimpleString::from(&**n),
            })
            .collect();
        if v >= version::ARTEMIS_2_29_0 {
            SessionBindingQueryResponseMessageV5 {
                exists: r.exists,
                queue_names: names,
                auto_create_queues: r.auto_create_queues,
                auto_create_addresses: r.auto_create_addresses,
                defaults: binding_defaults(&r),
                supports_multicast: r.routing_types.is_some_and(|t| t.multicast),
                supports_anycast: r.routing_types.is_some_and(|t| t.anycast),
                sections: SessionBindingQueryResponseMessageV5::SECTIONS,
            }
            .into()
        } else if v >= version::ADDRESSING_CHANGE {
            SessionBindingQueryResponseMessageV4 {
                exists: r.exists,
                queue_names: names,
                auto_create_queues: r.auto_create_queues,
                auto_create_addresses: r.auto_create_addresses,
                defaults: binding_defaults(&r),
                sections: SessionBindingQueryResponseMessageV4::SECTIONS,
            }
            .into()
        } else if v >= 127 {
            SessionBindingQueryResponseMessageV3 {
                exists: r.exists,
                queue_names: names,
                auto_create_queues: r.auto_create_queues,
                auto_create_addresses: r.auto_create_addresses,
            }
            .into()
        } else if v >= 126 {
            SessionBindingQueryResponseMessageV2 {
                exists: r.exists,
                queue_names: names,
                auto_create_queues: r.auto_create_queues,
            }
            .into()
        } else {
            SessionBindingQueryResponseMessage {
                exists: r.exists,
                queue_names: names,
            }
            .into()
        }
    }
}

/// `ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY`.
const JMS_SESSION_IDENTIFIER: &str = "jms-session";
/// `PacketImpl.OLD_QUEUE_PREFIX` and `OLD_TOPIC_PREFIX`.
const OLD_QUEUE_PREFIX: &str = "jms.queue.";
const OLD_TOPIC_PREFIX: &str = "jms.topic.";

/// `QueueAbstractPacket.getOldPrefixedAddress`.
fn old_prefixed_address(address: &str, routing_type: RoutingType) -> String {
    let prefix = match routing_type {
        RoutingType::Multicast => OLD_TOPIC_PREFIX,
        RoutingType::Anycast => OLD_QUEUE_PREFIX,
    };
    if address.starts_with(prefix) {
        address.to_owned()
    } else {
        format!("{prefix}{address}")
    }
}

/// `ServerSessionPacketHandler.getRoutingTypeFromAddress`, for the
/// pre-2.0 `CREATE_QUEUE` packet.
fn legacy_routing_type(address: &str) -> RoutingType {
    if address.starts_with("jms.queue.") || address.starts_with("jms.tempqueue.") {
        RoutingType::Anycast
    } else {
        RoutingType::Multicast
    }
}

fn queue_query_v3(r: QueueQueryResult) -> SessionQueueQueryResponseMessageV3 {
    let ss = |s: Option<String>| s.as_deref().map(SimpleString::from);
    SessionQueueQueryResponseMessageV3 {
        exists: r.exists,
        durable: r.durable,
        temporary: r.temporary,
        consumer_count: r.consumer_count,
        message_count: r.message_count,
        filter_string: ss(r.filter_string),
        address: Some(SimpleString::from(&*r.address)),
        name: Some(SimpleString::from(&*r.name)),
        auto_create_queues: r.auto_create_queues,
        auto_created: r.auto_created,
        purge_on_no_consumers: r.purge_on_no_consumers,
        routing_type: r.routing_type.to_byte(),
        max_consumers: r.max_consumers,
        exclusive: Flagged(r.exclusive),
        last_value: Flagged(r.last_value),
        default_consumer_window_size: Flagged(r.default_consumer_window_size),
        last_value_key: ss(r.last_value_key),
        non_destructive: Flagged(r.non_destructive),
        consumers_before_dispatch: Flagged(r.consumers_before_dispatch),
        delay_before_dispatch: Flagged(r.delay_before_dispatch),
        group_rebalance: Flagged(r.group_rebalance),
        group_buckets: Flagged(r.group_buckets),
        auto_delete: Flagged(r.auto_delete),
        auto_delete_delay: Flagged(r.auto_delete_delay),
        auto_delete_message_count: Flagged(r.auto_delete_message_count),
        group_first_key: ss(r.group_first_key),
        ring_size: Flagged(r.ring_size),
        enabled: Flagged(r.enabled),
        group_rebalance_pause_dispatch: Flagged(r.group_rebalance_pause_dispatch),
        configuration_managed: Flagged(r.configuration_managed),
        sections: SessionQueueQueryResponseMessageV3::SECTIONS,
    }
}

fn binding_defaults(r: &BindingQueryResult) -> BindingQueryDefaults {
    BindingQueryDefaults {
        default_purge_on_no_consumers: r.default_purge_on_no_consumers,
        default_max_consumers: r.default_max_consumers,
        default_exclusive: Flagged(Some(r.default_exclusive)),
        default_last_value: Flagged(Some(r.default_last_value)),
        default_last_value_key: r.default_last_value_key.as_deref().map(SimpleString::from),
        default_non_destructive: Flagged(Some(r.default_non_destructive)),
        default_consumers_before_dispatch: Flagged(Some(r.default_consumers_before_dispatch)),
        default_delay_before_dispatch: Flagged(Some(r.default_delay_before_dispatch)),
    }
}
