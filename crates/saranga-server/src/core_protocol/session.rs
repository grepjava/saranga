//! A Core session: one task handling one channel's packets in order.
//! Ported from `ServerSessionPacketHandler` (dispatch, responses and
//! confirmations) and `ServerSessionImpl` (sends, consumers, acknowledgement
//! and local transactions).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use saranga_buffer::{JString, SimpleString};
use saranga_core_protocol::packets::connection::{
    ActiveMQExceptionMessage, ActiveMQExceptionMessageV2, NullResponseMessage,
    NullResponseMessageV2, PacketsConfirmedMessage,
};
use saranga_core_protocol::packets::session::{
    BindingQueryDefaults, SessionBindingQueryResponseMessage, SessionBindingQueryResponseMessageV2,
    SessionBindingQueryResponseMessageV3, SessionBindingQueryResponseMessageV4,
    SessionBindingQueryResponseMessageV5, SessionProducerCreditsMessage,
    SessionQueueQueryResponseMessage, SessionQueueQueryResponseMessageV2,
    SessionQueueQueryResponseMessageV3,
};
use saranga_core_protocol::packets::xa::{
    SessionXAGetInDoubtXidsResponseMessage, SessionXAGetTimeoutResponseMessage,
    SessionXAResponseMessage, SessionXASetTimeoutResponseMessage, Xid,
};
use saranga_core_protocol::{ErrorType, Flagged, Packet, version};
use saranga_message::CoreMessage;
use tokio::sync::mpsc;

use super::{ConnShared, CoreDeliverySink};
use crate::large::{LargeBody, LargeWriter};
use crate::message::{MessageRef, ServerMessage, routing_type};
use crate::postoffice::{
    BindingQueryResult, QueueQueryResult, QueueSpec, RoutingTypes, queue_name_of,
};
use crate::queue::{ConsumerId, ConsumerRegistration, Queue};
use crate::storage::{Pending, Storage};
use crate::transaction::{self, Outcome, Transaction, TxError, TxRef, TxState, XaError, xa_code};
use crate::{Broker, BrokerError, Name, Result, RoutingType};

/// What the connection hands a session task.
pub(crate) enum SessionInput {
    /// A packet and its size on the wire, for confirmations.
    Packet(Box<Packet>, usize),
    ConnectionFailed,
}

/// The fields of `CreateSessionMessage` / `_V2` the session keeps.
pub(crate) struct CreateSession {
    pub name: String,
    pub channel_id: i64,
    pub version: i32,
    pub username: Option<String>,
    pub min_large_message_size: i32,
    pub xa: bool,
    pub auto_commit_sends: bool,
    pub auto_commit_acks: bool,
    pub confirmation_window_size: i32,
    pub pre_acknowledge: bool,
    pub default_address: Option<String>,
    pub client_id: Option<String>,
}

struct ServerConsumer {
    id: ConsumerId,
    queue: Queue,
    browse_only: bool,
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
    waits: Vec<Pending>,
    /// Queue updates that must follow the journal writes
    /// (`afterCompleteOperations`): routed references, committed outcomes.
    actions: Vec<Box<dyn FnOnce() + Send>>,
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
        let mut failure = None;
        for w in done.waits {
            if let Err(e) = w.wait().await {
                failure.get_or_insert(e);
            }
        }
        if failure.is_none() {
            for action in done.actions {
                action();
            }
        }
        let response = match failure {
            // OperationContext.onError: the client gets the I/O error.
            Some(e) => Some(exception_packet(
                done.request,
                &BrokerError::new(ErrorType::IoError, e.to_string()),
            )),
            None => done.response,
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
    broker: Arc<Broker>,
    conn: Arc<ConnShared>,
    channel_id: i64,
    name: String,
    /// Kept for security checks (Phase 6).
    #[allow(dead_code)]
    username: Option<String>,
    /// The size of the chunks large messages are sent to consumers in.
    min_large_message_size: i32,
    /// XA sessions work in the transaction of the last `xaStart`, or in
    /// none.
    xa: bool,
    auto_commit_sends: bool,
    auto_commit_acks: bool,
    pre_acknowledge: bool,
    default_address: Option<Name>,
    started: bool,
    closed: bool,
    consumers: BTreeMap<i64, ServerConsumer>,
    producers: HashMap<i32, Option<String>>,
    /// The current transaction: always one for a local session.
    tx: Option<TxRef>,
    /// An XA branch ended but not yet prepared, committed or rolled back
    /// (`pendingTX`), rolled back if the client fails.
    pending_tx: Option<TxRef>,
    timeout_seconds: i32,
    /// The large message being received (`currentLargeMessage`): its
    /// headers, and its body so far.
    large: Option<(CoreMessage, LargeWriter)>,
    /// Journal writes lined up by the request being handled.
    ops: Vec<Pending>,
    /// Queue updates to make once those writes are done.
    actions: Vec<Box<dyn FnOnce() + Send>>,
    completer: mpsc::UnboundedSender<Done>,
}

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
    pub(crate) fn new(broker: Arc<Broker>, conn: Arc<ConnShared>, request: CreateSession) -> Self {
        let tx =
            (!request.xa).then(|| Transaction::new_ref(broker.post_office().ids().next(), None));
        let (completer_tx, completer_rx) = mpsc::unbounded_channel();
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
        Self {
            broker,
            conn,
            channel_id: request.channel_id,
            name: request.name,
            username: request.username,
            min_large_message_size: request.min_large_message_size,
            xa: request.xa,
            auto_commit_sends: request.auto_commit_sends,
            auto_commit_acks: request.auto_commit_acks,
            pre_acknowledge: request.pre_acknowledge,
            default_address: request.default_address.map(Into::into),
            started: false,
            closed: false,
            consumers: BTreeMap::new(),
            producers: HashMap::new(),
            tx,
            pending_tx: None,
            timeout_seconds: -1,
            large: None,
            ops: Vec::new(),
            actions: Vec::new(),
            completer: completer_tx,
        }
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::UnboundedReceiver<SessionInput>) {
        while let Some(input) = rx.recv().await {
            match input {
                SessionInput::Packet(packet, size) => {
                    self.handle(*packet, size).await;
                    if self.closed {
                        break;
                    }
                }
                SessionInput::ConnectionFailed => {
                    self.close(true).await;
                    break;
                }
            }
        }
        if !self.closed {
            self.close(true).await;
        }
    }

    fn send(&self, packet: impl Into<Packet>) {
        self.conn.send(self.channel_id, packet);
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

    fn storage(&self) -> Option<Arc<Storage>> {
        self.broker.post_office().ids().storage().cloned()
    }

    /// `newTransaction`, with its journal ID.
    fn new_tx(&self, xid: Option<Xid>) -> TxRef {
        Transaction::new_ref(self.broker.post_office().ids().next(), xid)
    }

    /// Journals a finished transaction's record, then applies it to the
    /// queues: once the record is written when `defer` (commits), at once
    /// otherwise (rollbacks).
    fn finish(&mut self, mut outcome: Outcome, defer: bool) -> Result<()> {
        let storage = self.storage();
        if let Some(p) = outcome.write(storage.as_deref())? {
            self.ops.push(p);
        }
        if defer && storage.is_some() {
            self.actions.push(Box::new(move || outcome.apply()));
        } else {
            outcome.apply();
        }
        Ok(())
    }

    fn exception(&self, request: Request, e: &BrokerError) -> Packet {
        exception_packet(request, e)
    }

    /// `doConfirmAndResponse`, after the journal writes this request lined
    /// up.
    fn respond(&mut self, request: Request, response: Option<Packet>, flush: bool) {
        let _ = self.completer.send(Done {
            waits: std::mem::take(&mut self.ops),
            actions: std::mem::take(&mut self.actions),
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
                tracing::warn!(session = %self.name, "{e}");
                None
            }
        }
    }

    async fn handle(&mut self, packet: Packet, size: usize) {
        let request = Request::of(&packet, size);
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
            Packet::SessionSendMessageV3(p) => (
                p.requires_response,
                self.send_message(p.message, None).await,
                false,
            ),
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
                // ServerSessionImpl.expire
                let result = match self.consumers.get(&p.consumer_id) {
                    Some(c) => {
                        c.queue.expire(c.id, p.message_id);
                        Ok(None)
                    }
                    None => Err(BrokerError::consumer_doesnt_exist(p.consumer_id)),
                };
                (false, result, false)
            }
            Packet::SessionRequestProducerCreditsMessage(p) => {
                // requestProducerCredits: a full address under BLOCK grants
                // them once memory is freed, under FAIL refuses them.
                let store = self
                    .broker
                    .post_office()
                    .paging_store(&p.address.to_string_lossy());
                let (conn, channel) = (self.conn.clone(), self.channel_id);
                let (credits, address) = (p.credits, p.address.clone());
                let grant = Box::new(move || {
                    conn.send(channel, SessionProducerCreditsMessage { credits, address });
                });
                if let crate::paging::Credits::Failed = store.request_credits(grant) {
                    self.send(
                        saranga_core_protocol::packets::session::SessionProducerCreditsFailMessage {
                            credits: p.credits,
                            address: p.address,
                        },
                    );
                }
                (false, Ok(None), false)
            }
            Packet::SessionConsumerFlowCreditMessage(p) => {
                if let Some(c) = self.consumers.get(&p.consumer_id) {
                    c.queue.credits(c.id, p.credits);
                }
                (false, Ok(None), false)
            }
            Packet::SessionCreateConsumerMessage(p) => {
                let result = self
                    .create_consumer(
                        p.id,
                        &p.queue_name.to_string_lossy(),
                        p.filter_string.map(|f| f.to_string_lossy()),
                        p.priority,
                        p.browse_only,
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
                    .broker
                    .post_office()
                    .add_or_update_address(
                        &p.address.to_string_lossy(),
                        types,
                        p.auto_created,
                        false,
                    )
                    .map(|_| None);
                (p.requires_response, result, false)
            }
            Packet::CreateQueueMessage(p) => {
                let address = p.address.to_string_lossy();
                let mut spec = QueueSpec::new(p.queue_name.to_string_lossy(), address.as_str());
                spec.routing_type = Some(legacy_routing_type(&address));
                spec.filter_string = p.filter_string.map(|f| f.to_string_lossy());
                spec.durable = p.durable;
                spec.temporary = p.temporary;
                (
                    p.requires_response,
                    self.create_queue(spec).map(|()| None),
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
                (
                    p.requires_response,
                    self.create_queue(spec).map(|()| None),
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
                    self.create_shared_queue(spec).map(|()| None),
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
                    self.create_shared_queue(spec).map(|()| None),
                    false,
                )
            }
            Packet::SessionDeleteQueueMessage(p) => (
                true,
                self.delete_queue(&p.queue_name.to_string_lossy())
                    .map(|()| None),
                false,
            ),
            Packet::SessionQueueQueryMessage(p) => {
                let response = self.queue_query_response(&p.queue_name.to_string_lossy());
                (true, Ok(Some(response)), false)
            }
            Packet::SessionBindingQueryMessage(p) => {
                let response = self.binding_query_response(&p.address.to_string_lossy());
                (true, Ok(Some(response)), false)
            }
            Packet::SessionCommitMessage(_) | Packet::SessionCommitMessageV2(_) => {
                (true, self.commit().map(|()| None), false)
            }
            Packet::RollbackMessage(p) => {
                self.rollback(p.consider_last_message_as_delivered, false)
                    .await;
                (true, Ok(None), false)
            }
            Packet::SessionStart(_) => {
                self.set_started(true).await;
                (false, Ok(None), false)
            }
            Packet::SessionStop(_) => {
                self.set_started(false).await;
                (true, Ok(None), false)
            }
            Packet::SessionCloseMessage(_) => {
                self.close(false).await;
                (true, Ok(None), true)
            }
            Packet::SessionConsumerCloseMessage(p) => {
                self.close_consumer(p.consumer_id, false).await;
                (true, Ok(None), false)
            }
            Packet::SessionForceConsumerDelivery(p) => {
                if let Some(c) = self.consumers.get(&p.consumer_id) {
                    c.queue.force_delivery(c.id, p.sequence);
                }
                (false, Ok(None), false)
            }
            Packet::SessionAddMetaDataMessage(p) => {
                self.broker.add_session_metadata(
                    &self.name,
                    &p.key.to_string_lossy(),
                    &p.data.to_string_lossy(),
                );
                (true, Ok(None), false)
            }
            Packet::SessionAddMetaDataMessageV2(p) => {
                self.broker.add_session_metadata(
                    &self.name,
                    &p.key.to_string_lossy(),
                    &p.data.to_string_lossy(),
                );
                (p.requires_confirmation, Ok(None), false)
            }
            Packet::SessionUniqueAddMetaDataMessage(p) => {
                let (key, data) = (p.key.to_string_lossy(), p.data.to_string_lossy());
                let result = if self
                    .broker
                    .add_unique_session_metadata(&self.name, &key, &data)
                {
                    Ok(None)
                } else {
                    Err(BrokerError::duplicate_metadata(&key, &data))
                };
                (true, result, false)
            }
            Packet::CreateProducerMessage(p) => {
                self.producers
                    .entry(p.id)
                    .or_insert_with(|| p.address.map(|a| a.to_string_lossy()));
                (false, Ok(None), false)
            }
            Packet::RemoveProducerMessage(p) => {
                self.producers.remove(&p.id);
                (false, Ok(None), false)
            }
            Packet::SessionXAStartMessage(p) => {
                (true, Self::xa_response(self.xa_start(p.xid)), false)
            }
            Packet::SessionXAEndMessage(p) => (true, Self::xa_response(self.xa_end(p.xid)), false),
            Packet::SessionXAPrepareMessage(p) => {
                (true, Self::xa_response(self.xa_prepare(p.xid)), false)
            }
            Packet::SessionXACommitMessage(p) => (
                true,
                Self::xa_response(self.xa_commit(p.xid, p.one_phase)),
                false,
            ),
            Packet::SessionXARollbackMessage(p) => {
                let result = self.xa_rollback(p.xid).await;
                (true, Self::xa_response(result), false)
            }
            Packet::SessionXAJoinMessage(p) => {
                (true, Self::xa_response(self.xa_join(p.xid)), false)
            }
            Packet::SessionXAResumeMessage(p) => {
                (true, Self::xa_response(self.xa_resume(p.xid)), false)
            }
            Packet::SessionXASuspend(_) => (true, Self::xa_response(self.xa_suspend()), false),
            Packet::SessionXAForgetMessage(_) => {
                // Heuristic completions come with management (Phase 8):
                // there is never one to forget.
                let e = XaError {
                    code: xa_code::XAER_NOTA,
                    message: None,
                };
                (true, Self::xa_response(Err(e.into())), false)
            }
            Packet::SessionXAAfterFailedMessage(p) => {
                self.xa_failed(p.xid);
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
                // xaSetTimeout
                self.timeout_seconds = p.timeout_seconds;
                if let Some(tx) = &self.tx {
                    transaction::lock(tx).timeout_seconds = p.timeout_seconds;
                }
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
            Packet::SessionSendContinuationMessageV3(p) => (
                p.requires_response,
                self.send_continuation(p.body, p.continues, p.message_body_size)
                    .await,
                false,
            ),
            other => {
                tracing::warn!(session = %self.name, "unsupported packet {}", other.name());
                let e = BrokerError::new(
                    ErrorType::UnsupportedPacket,
                    format!("{} is not supported yet", other.name()),
                );
                (true, Err(e), false)
            }
        };
        let response = self.outcome(request, requires_response, result);
        self.respond(request, response, flush);
    }

    /// `PostOfficeImpl.processRoute` with `PagingStore.page`: the queues of
    /// an address that pages get the message in its pages instead of a
    /// reference; a full address under DROP drops it, under FAIL refuses
    /// the send.
    fn page_routed(
        &mut self,
        routed: crate::postoffice::Routed,
        tx: Option<&TxRef>,
    ) -> Result<crate::postoffice::Routed> {
        let (routed, pending) = self.broker.post_office().page_routed(routed, tx)?;
        self.ops.extend(pending);
        Ok(routed)
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

    /// `LargeServerMessageImpl.checkLargeMessage`: a message too big for a
    /// journal record becomes a large message.
    fn check_large_message(&self, message: &mut CoreMessage) -> Result<Option<Arc<LargeBody>>> {
        let Some(max) = self.broker.max_record_size() else {
            return Ok(None);
        };
        let mut w = saranga_buffer::Writer::new();
        message.encode(&mut w);
        if w.len() + 512 <= max {
            return Ok(None);
        }
        let id = self.broker.post_office().ids().next();
        let io = |e: std::io::Error| BrokerError::new(ErrorType::IoError, e.to_string());
        let mut writer = self
            .broker
            .large_messages()
            .create(id, message.durable)
            .map_err(io)?;
        for chunk in message.body.chunks(crate::large::CHUNK_SIZE) {
            writer.append(chunk).map_err(io)?;
        }
        let size = message.body.len() as i64;
        message.body = Vec::new();
        message.message_id = id;
        let key = SimpleString::from(crate::message::HDR_LARGE_BODY_SIZE);
        if message.properties.get(&key).is_none() {
            message
                .properties
                .insert(key, saranga_buffer::PropertyValue::Long(size));
        }
        Ok(Some(writer.finish().map_err(io)?))
    }

    /// `ServerSessionImpl.send`: assigns the ID, routes, and either adds to
    /// the queues or holds the message in the transaction.
    async fn send_message(
        &mut self,
        mut message: CoreMessage,
        large: Option<Arc<LargeBody>>,
    ) -> Result<Option<Packet>> {
        let large = match large {
            Some(body) => Some(body),
            None => {
                let body = self.check_large_message(&mut message)?;
                if body.is_none() {
                    message.message_id = self.broker.post_office().ids().next();
                }
                body
            }
        };
        let address: Name = match &message.address {
            Some(a) => {
                let a: Name = a.to_string_lossy().into();
                self.default_address.get_or_insert_with(|| a.clone());
                a
            }
            None => {
                let a = self
                    .default_address
                    .clone()
                    .ok_or_else(BrokerError::no_address)?;
                message.address = Some(SimpleString::from(&*a));
                a
            }
        };
        let routing_type = routing_type(&message);
        // PostOfficeImpl.route: the address's expiry settings apply first.
        let settings = self
            .broker
            .post_office()
            .settings(crate::postoffice::split_fqqn(&address).0);
        crate::message::apply_expiry_delay(&mut message, &settings, crate::message::now_millis());
        let scheduled_time = crate::message::scheduled_delivery_time(&message);
        let duplicate_id = crate::duplicate::duplicate_id_bytes(&message);
        let mut r = MessageRef::new(match large {
            Some(body) => ServerMessage::new_large(message, body),
            None => ServerMessage::new(message),
        });
        r.scheduled_time = scheduled_time;
        // doSend: outside a transaction when auto-committing or when an XA
        // session has none.
        let tx = if self.auto_commit_sends {
            None
        } else {
            self.tx.clone()
        };
        let storage = self.storage();
        // checkDuplicateID: a send outside a transaction gets one of its own
        // so the ID is journaled with the message.
        let mut own_tx: Option<(i64, bool)> = None;
        if let Some(id) = duplicate_id {
            let po = self.broker.post_office();
            let cache = po.duplicate_cache(crate::postoffice::split_fqqn(&address).0);
            let mut c = crate::duplicate::lock(&cache);
            if c.contains(&id) {
                tracing::warn!(
                    "AMQ222059: Duplicate message detected - message will not be routed. Message information:\n{}",
                    r.message.message_id()
                );
                if let Some(tx) = &tx {
                    transaction::lock(tx).mark_rollback_only(&BrokerError::new(
                        ErrorType::DuplicateIdRejected,
                        format!(
                            "Duplicate message detected - message will not be routed. Message information:{}",
                            r.message.message_id()
                        ),
                    ));
                }
                return Ok(None);
            }
            let record = po.ids().next();
            match &tx {
                None => {
                    let jtx = po.ids().next();
                    c.store_tx(jtx, &id, record).map_err(io_error)?;
                    own_tx = Some((jtx, c.is_persistent()));
                    c.add_in_memory(id, record);
                }
                Some(tx) => {
                    let mut t = transaction::lock(tx);
                    c.store_tx(t.id, &id, record).map_err(io_error)?;
                    t.persistent |= c.is_persistent();
                    t.duplicates.push(crate::duplicate::PendingId {
                        cache: cache.clone(),
                        id,
                        record,
                    });
                }
            }
        }
        let routed = match self
            .broker
            .post_office()
            .route(&r, &address, routing_type)
            .and_then(|routed| self.page_routed(routed, tx.as_ref()))
        {
            Ok(routed) => routed,
            Err(e) => {
                if let Some(tx) = &tx {
                    transaction::lock(tx).mark_rollback_only(&e);
                }
                if let (Some((jtx, true)), Some(s)) = (own_tx, &storage) {
                    let _ = s.rollback(jtx);
                }
                return Err(e);
            }
        };
        // processRouteToDurableQueues, for the message and each divert
        // copy: the message record with its first durable reference, then
        // one reference per durable queue.
        match tx {
            None => {
                if let Some(s) = &storage {
                    for (r, queues) in &routed {
                        let message = &r.message;
                        let scheduled_time = r.scheduled_time;
                        let durable: Vec<&Queue> =
                            queues.iter().filter(|q| q.is_durable_ref(r)).collect();
                        let last = durable.len().saturating_sub(1);
                        for (i, q) in durable.into_iter().enumerate() {
                            let mid = message.message_id();
                            if let Some((jtx, persistent)) = &mut own_tx {
                                if message.durable_up() == 1 {
                                    s.store_message_tx(*jtx, message).map_err(io_error)?;
                                }
                                s.store_reference_tx(*jtx, q.id(), mid).map_err(io_error)?;
                                if scheduled_time > 0 {
                                    s.update_scheduled_delivery_time_tx(
                                        *jtx,
                                        q.id(),
                                        mid,
                                        scheduled_time,
                                    )
                                    .map_err(io_error)?;
                                }
                                *persistent = true;
                                continue;
                            }
                            if message.durable_up() == 1 {
                                s.store_message(message).map_err(io_error)?;
                            }
                            self.ops.push(
                                s.store_reference(q.id(), mid, i == last)
                                    .map_err(io_error)?,
                            );
                            if scheduled_time > 0
                                && let Some(p) = s
                                    .update_scheduled_delivery_time(q.id(), mid, scheduled_time, 0)
                                    .map_err(io_error)?
                            {
                                self.ops.push(p);
                            }
                        }
                    }
                    if let Some((jtx, true)) = own_tx {
                        self.ops.push(s.commit(jtx).map_err(io_error)?);
                    }
                    // The references join their queues once journaled.
                    self.actions.push(Box::new(move || {
                        for (r, queues) in routed {
                            for q in queues {
                                q.add(r.clone());
                            }
                        }
                    }));
                } else {
                    for (r, queues) in routed {
                        for q in queues {
                            q.add(r.clone());
                        }
                    }
                }
            }
            Some(tx) => {
                let mut t = transaction::lock(&tx);
                for (r, queues) in routed {
                    let message = &r.message;
                    for q in queues.iter().filter(|q| q.is_durable_ref(&r)) {
                        let Some(s) = &storage else { break };
                        if message.durable_up() == 1 {
                            s.store_message_tx(t.id, message).map_err(io_error)?;
                        }
                        s.store_reference_tx(t.id, q.id(), message.message_id())
                            .map_err(io_error)?;
                        if r.scheduled_time > 0 {
                            s.update_scheduled_delivery_time_tx(
                                t.id,
                                q.id(),
                                message.message_id(),
                                r.scheduled_time,
                            )
                            .map_err(io_error)?;
                        }
                        t.persistent = true;
                    }
                    t.sends.extend(queues.into_iter().map(|q| (q, r.clone())));
                }
            }
        }
        Ok(None)
    }

    async fn acknowledge(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
    ) -> Result<Option<Packet>> {
        let tx = if self.auto_commit_acks {
            None
        } else {
            self.tx.clone()
        };
        let Some(c) = self.consumers.get(&consumer_id) else {
            let e = BrokerError::consumer_doesnt_exist(consumer_id);
            if let Some(tx) = &self.tx {
                transaction::lock(tx).mark_rollback_only(&e);
            }
            return Err(e);
        };
        let queue = c.queue.clone();
        match queue.ack(c.id, message_id, individual, tx.is_some()).await {
            Ok(refs) => {
                // QueueImpl.acknowledge
                let storage = self.storage();
                let _ = storage;
                let mut refs = refs;
                match tx {
                    Some(tx) => {
                        let mut t = transaction::lock(&tx);
                        let id = t.id;
                        for r in &mut refs {
                            if queue.store_ack_tx(id, r, &mut t.paging).map_err(io_error)? {
                                t.persistent = true;
                            }
                        }
                        t.acks.extend(refs.into_iter().map(|r| (queue.clone(), r)));
                    }
                    None => {
                        for r in &mut refs {
                            if let Some(p) = queue.store_ack(r).map_err(io_error)? {
                                self.ops.push(p);
                            }
                            queue.post_acknowledge(r);
                        }
                    }
                }
                Ok(None)
            }
            Err(failed) => {
                let e = BrokerError::consumer_no_reference(consumer_id, message_id, queue.name());
                match tx {
                    Some(tx) => {
                        let mut t = transaction::lock(&tx);
                        t.acks
                            .extend(failed.refs.into_iter().map(|r| (queue.clone(), r)));
                        t.mark_rollback_only(&e);
                    }
                    None => queue.reinsert(failed.refs),
                }
                Err(e)
            }
        }
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

    fn current_xid_is(&self, xid: &Xid) -> bool {
        self.tx
            .as_ref()
            .is_some_and(|t| transaction::lock(t).xid.as_ref() == Some(xid))
    }

    /// `xaStart`: a transaction still open on the session is rolled back
    /// (unless prepared) and replaced.
    fn xa_start(&mut self, xid: Xid) -> Result<(), TxError> {
        if let Some(old) = self.tx.take() {
            let mut t = transaction::lock(&old);
            tracing::warn!(
                "AMQ222210: Session already had a transaction {} when xaStart {} was called",
                t.xid
                    .as_ref()
                    .map(transaction::xid_string)
                    .unwrap_or_default(),
                transaction::xid_string(&xid)
            );
            if t.state != TxState::Prepared {
                if let Some(x) = &t.xid {
                    self.broker.resource_manager().remove(x);
                }
                if let Ok(outcome) = t.rollback() {
                    drop(t);
                    self.finish(outcome, false)?;
                }
            }
        }
        let tx = self.new_tx(Some(xid.clone()));
        transaction::lock(&tx).timeout_seconds = self.timeout_seconds;
        self.tx = Some(tx.clone());
        if !self.broker.resource_manager().put(xid.clone(), tx) {
            return Err(XaError::new(
                xa_code::XAER_DUPID,
                format!(
                    "Cannot start, there is already a xid {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        Ok(())
    }

    /// `xaEnd`.
    fn xa_end(&mut self, xid: Xid) -> Result<(), TxError> {
        if self.current_xid_is(&xid) {
            let tx = self.tx.clone().expect("current transaction");
            let state = transaction::lock(&tx).state;
            match state {
                TxState::Suspended => Err(XaError::new(
                    xa_code::XAER_PROTO,
                    "Cannot end, transaction is suspended",
                )
                .into()),
                TxState::RolledBack => {
                    self.tx = None;
                    Err(XaError::new(
                        xa_code::XAER_PROTO,
                        "Cannot end, transaction is rolled back",
                    )
                    .into())
                }
                _ => {
                    self.pending_tx = self.tx.take();
                    Ok(())
                }
            }
        } else {
            // Ending a suspended branch (JTA 1.1, 3.4.4).
            let Some(tx) = self.broker.resource_manager().get(&xid) else {
                return Err(XaError::new(
                    xa_code::XAER_NOTA,
                    format!(
                        "Cannot find suspended transaction to end {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into());
            };
            let mut t = transaction::lock(&tx);
            match t.state {
                TxState::Active => Ok(()),
                TxState::Suspended => {
                    t.state = TxState::Active;
                    Ok(())
                }
                _ => Err(XaError::new(
                    xa_code::XAER_PROTO,
                    format!(
                        "Transaction is not suspended {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into()),
            }
        }
    }

    fn not_found(xid: &Xid) -> TxError {
        XaError::new(
            xa_code::XAER_NOTA,
            format!(
                "Cannot find xid in resource manager: {}",
                transaction::xid_string(xid)
            ),
        )
        .into()
    }

    /// `xaPrepare`.
    fn xa_prepare(&mut self, xid: Xid) -> Result<(), TxError> {
        if self.current_xid_is(&xid) {
            let current = self
                .tx
                .as_ref()
                .and_then(|t| transaction::lock(t).xid.clone());
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot commit, session is currently doing work in a transaction {}",
                    current
                        .as_ref()
                        .map(transaction::xid_string)
                        .unwrap_or_default()
                ),
            )
            .into());
        }
        let Some(tx) = self.broker.resource_manager().get(&xid) else {
            return Err(Self::not_found(&xid));
        };
        let mut t = transaction::lock(&tx);
        match t.state {
            TxState::Suspended => Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot prepare transaction, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into()),
            TxState::Prepared => {
                tracing::warn!(
                    "AMQ222098: Ignoring prepare on xid as already called : {}",
                    transaction::xid_string(&xid)
                );
                Ok(())
            }
            _ => match t.prepare() {
                Ok(()) => {
                    let (id, xid) = (t.id, t.xid.clone().expect("XA transaction"));
                    if let Some(s) = self.storage() {
                        t.paging.before_commit(&s, id).map_err(io_error)?;
                        drop(t);
                        self.ops.push(s.prepare(id, &xid).map_err(io_error)?);
                    }
                    Ok(())
                }
                Err(failed) => {
                    let (e, outcome) = *failed;
                    drop(t);
                    self.finish(outcome, false)?;
                    Err(e.into())
                }
            },
        }
    }

    /// `xaCommit`.
    fn xa_commit(&mut self, xid: Xid, one_phase: bool) -> Result<(), TxError> {
        self.pending_tx = None;
        if self.current_xid_is(&xid) {
            let current = self
                .tx
                .as_ref()
                .and_then(|t| transaction::lock(t).xid.clone());
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot commit, session is currently doing work in transaction {}",
                    current
                        .as_ref()
                        .map(transaction::xid_string)
                        .unwrap_or_default()
                ),
            )
            .into());
        }
        let rm = self.broker.resource_manager();
        let Some(tx) = rm.remove(&xid) else {
            return Err(Self::not_found(&xid));
        };
        let mut t = transaction::lock(&tx);
        if t.state == TxState::Suspended {
            drop(t);
            rm.put(xid.clone(), tx);
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot commit transaction, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        let result = t.commit(one_phase);
        drop(t);
        match result {
            Ok(outcome) => {
                self.finish(outcome, true)?;
                Ok(())
            }
            Err(failed) => {
                let (e, outcome) = *failed;
                self.finish(outcome, false)?;
                Err(e.into())
            }
        }
    }

    /// `xaRollback`: rolls back the branch, and like Artemis also takes
    /// back what this session's consumers are delivering.
    async fn xa_rollback(&mut self, xid: Xid) -> Result<(), TxError> {
        self.pending_tx = None;
        if self.current_xid_is(&xid) {
            let current = self
                .tx
                .as_ref()
                .and_then(|t| transaction::lock(t).xid.clone());
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot roll back, session is currently doing work in a transaction {}",
                    current
                        .as_ref()
                        .map(transaction::xid_string)
                        .unwrap_or_default()
                ),
            )
            .into());
        }
        let rm = self.broker.resource_manager();
        let Some(tx) = rm.remove(&xid) else {
            return Err(Self::not_found(&xid));
        };
        if transaction::lock(&tx).state == TxState::Suspended {
            rm.put(xid.clone(), tx);
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot rollback transaction, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        self.do_rollback(false, false, &tx).await?;
        Ok(())
    }

    /// `xaJoin`.
    fn xa_join(&mut self, xid: Xid) -> Result<(), TxError> {
        let Some(tx) = self.broker.resource_manager().get(&xid) else {
            return Err(Self::not_found(&xid));
        };
        if transaction::lock(&tx).state == TxState::Suspended {
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot join tx, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        self.tx = Some(tx);
        Ok(())
    }

    /// `xaResume`.
    fn xa_resume(&mut self, xid: Xid) -> Result<(), TxError> {
        if let Some(current) = &self.tx {
            let x = transaction::lock(current).xid.clone();
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot resume, session is currently doing work in a transaction {}",
                    x.as_ref().map(transaction::xid_string).unwrap_or_default()
                ),
            )
            .into());
        }
        let Some(tx) = self.broker.resource_manager().get(&xid) else {
            return Err(Self::not_found(&xid));
        };
        {
            let mut t = transaction::lock(&tx);
            if t.state != TxState::Suspended {
                return Err(XaError::new(
                    xa_code::XAER_PROTO,
                    format!(
                        "Cannot resume transaction, it is not suspended {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into());
            }
            t.state = TxState::Active;
        }
        self.tx = Some(tx);
        Ok(())
    }

    /// `xaSuspend`.
    fn xa_suspend(&mut self) -> Result<(), TxError> {
        let Some(tx) = &self.tx else {
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                "Cannot suspend, session is not doing work in a transaction ",
            )
            .into());
        };
        let mut t = transaction::lock(tx);
        match t.state {
            TxState::Suspended => {
                let x = t
                    .xid
                    .as_ref()
                    .map(transaction::xid_string)
                    .unwrap_or_default();
                Err(XaError::new(
                    xa_code::XAER_PROTO,
                    format!("Cannot suspend, transaction is already suspended {x}"),
                )
                .into())
            }
            TxState::Active => {
                t.state = TxState::Suspended;
                drop(t);
                self.tx = None;
                Ok(())
            }
            _ => Err(BrokerError::internal("Can only suspend active transaction").into()),
        }
    }

    /// `xaFailed`: after a failover the branch can only roll back.
    fn xa_failed(&mut self, xid: Xid) {
        let rm = self.broker.resource_manager();
        let tx = rm.get(&xid).unwrap_or_else(|| {
            let tx =
                Transaction::new_ref(self.broker.post_office().ids().next(), Some(xid.clone()));
            rm.put(xid, tx.clone());
            tx
        });
        let effective = transaction::lock(&tx).is_effective();
        if effective {
            self.tx = None;
        } else {
            transaction::lock(&tx).mark_rollback_only(&BrokerError::new(
                ErrorType::GenericException,
                "Can't commit as a Failover happened during the operation",
            ));
            self.tx = Some(tx);
        }
    }

    /// `ServerSessionImpl.createConsumer`.
    async fn create_consumer(
        &mut self,
        consumer_id: i64,
        queue_name: &str,
        filter: Option<String>,
        priority: i32,
        browse_only: bool,
    ) -> Result<()> {
        let post_office = self.broker.post_office();
        let name = queue_name_of(queue_name);
        let queue = post_office
            .queue(name)
            .ok_or_else(|| BrokerError::no_such_queue(name))?;
        let filter = post_office.filters().create(filter.as_deref())?;
        let c = queue.config();
        if !browse_only && c.max_consumers != -1 && queue.consumer_count() >= c.max_consumers {
            return Err(BrokerError::max_consumer_limit_reached(&c.address, &c.name));
        }
        let id = self.broker.next_consumer_id();
        queue.add_consumer(ConsumerRegistration {
            id,
            client_id: consumer_id,
            sink: Arc::new(CoreDeliverySink {
                conn: self.conn.clone(),
                channel_id: self.channel_id,
                chunk_size: self.min_large_message_size.max(1) as usize,
            }),
            filter,
            priority,
            browse_only,
            pre_acknowledge: self.pre_acknowledge,
            started: self.started,
        });
        self.consumers.insert(
            consumer_id,
            ServerConsumer {
                id,
                queue,
                browse_only,
            },
        );
        Ok(())
    }

    async fn close_consumer(&mut self, consumer_id: i64, failed: bool) {
        let Some(c) = self.consumers.remove(&consumer_id) else {
            tracing::debug!(consumer_id, "closing an unknown consumer");
            return;
        };
        c.queue.remove_consumer(c.id, failed).await;
        if c.queue.consumer_count() == 0 {
            self.broker.post_office().check_auto_delete(&c.queue);
        }
    }

    /// `ServerSessionImpl.createQueue`.
    fn create_queue(&mut self, spec: QueueSpec) -> Result<()> {
        let temporary = spec.temporary;
        let queue = self.broker.post_office().create_queue(spec, false)?;
        if temporary {
            self.conn.add_temp_queue(queue.name().clone());
        }
        Ok(())
    }

    /// `CREATE_SHARED_QUEUE`: creates the queue unless one with the same
    /// address and filter exists.
    fn create_shared_queue(&mut self, spec: QueueSpec) -> Result<()> {
        let existing = self.broker.post_office().queue_query(&spec.name);
        if existing.exists
            && existing.address == spec.address
            && existing.filter_string == spec.filter_string
        {
            return Ok(());
        }
        self.broker
            .post_office()
            .create_queue(spec, false)
            .map(|_| ())
    }

    fn delete_queue(&mut self, name: &str) -> Result<()> {
        let post_office = self.broker.post_office();
        let name = queue_name_of(name);
        let auto_delete_address = post_office
            .queue(name)
            .map(|q| {
                post_office
                    .settings(&q.config().address)
                    .auto_delete_addresses
            })
            .ok_or_else(|| BrokerError::no_such_queue(name))?;
        post_office.delete_queue(name, true, auto_delete_address)?;
        self.conn.remove_temp_queue(name);
        Ok(())
    }

    /// `ServerSessionImpl.commit`: commits the current transaction (a
    /// rollback-only one rolls back and fails instead) and starts the next.
    fn commit(&mut self) -> Result<()> {
        let tx = self.tx.take();
        if !self.xa {
            self.tx = Some(self.new_tx(None));
        }
        let Some(tx) = tx else {
            return Ok(());
        };
        let result = transaction::lock(&tx).commit(true);
        match result {
            Ok(outcome) => self.finish(outcome, true),
            Err(failed) => {
                let (e, outcome) = *failed;
                self.finish(outcome, false)?;
                Err(e)
            }
        }
    }

    /// `ServerSessionImpl.rollback`.
    async fn rollback(&mut self, last_as_delivered: bool, failed: bool) {
        let tx = match self.tx.take() {
            Some(tx) => tx,
            None => self.new_tx(None),
        };
        if let Err(e) = self.do_rollback(failed, last_as_delivered, &tx).await {
            tracing::warn!(session = %self.name, "rollback: {e}");
        }
        if !self.xa {
            self.tx = Some(self.new_tx(None));
        }
    }

    /// `doRollback`: takes back what this session's consumers are
    /// delivering and rolls the transaction back, returning both to the
    /// queues.
    async fn do_rollback(
        &mut self,
        failed: bool,
        last_as_delivered: bool,
        tx: &TxRef,
    ) -> Result<(), BrokerError> {
        let was_started = self.started;
        let mut cancelled = Vec::new();
        for c in self.consumers.values() {
            if c.browse_only {
                continue;
            }
            let refs = c
                .queue
                .cancel_delivering(c.id, last_as_delivered, failed)
                .await;
            cancelled.extend(refs.into_iter().map(|r| (c.queue.clone(), r)));
        }
        let result = {
            let mut t = transaction::lock(tx);
            // What was delivered after a transaction already rolled back
            // (timed out) goes back on its own.
            if t.state == TxState::RolledBack {
                Ok(Outcome::default())
            } else {
                t.rollback()
            }
        };
        let (mut outcome, result) = match result {
            Ok(o) => (o, Ok(())),
            Err(e) => (Outcome::default(), Err(e)),
        };
        outcome.cancel.extend(cancelled);
        let finished = self.finish(outcome, false);
        if was_started && !failed {
            for c in self.consumers.values() {
                c.queue.set_started(c.id, true).await;
            }
        }
        result.and(finished)
    }

    async fn set_started(&mut self, started: bool) {
        self.started = started;
        for c in self.consumers.values() {
            c.queue.set_started(c.id, started).await;
        }
    }

    /// `ServerSessionImpl.doClose`. A failed client's transactions roll back
    /// unless prepared; a clean close rolls back only a local transaction,
    /// leaving XA branches to the transaction manager. Then the consumers
    /// close.
    async fn close(&mut self, failed: bool) {
        if self.closed {
            return;
        }
        self.closed = true;
        if failed {
            for tx in [self.tx.take(), self.pending_tx.take()]
                .into_iter()
                .flatten()
            {
                let (xid, outcome) = {
                    let mut t = transaction::lock(&tx);
                    (t.xid.clone(), t.try_rollback())
                };
                if let Some(outcome) = outcome {
                    if let Err(e) = self.finish(outcome, false) {
                        tracing::warn!(session = %self.name, "AMQ222207: Unable to rollback on close: {e}");
                    }
                    if let Some(xid) = xid {
                        self.broker.resource_manager().remove(&xid);
                    }
                }
            }
        } else if self
            .tx
            .as_ref()
            .is_some_and(|t| transaction::lock(t).xid.is_none())
        {
            self.rollback(false, false).await;
        }
        let ids: Vec<i64> = self.consumers.keys().copied().collect();
        for id in ids {
            self.close_consumer(id, failed).await;
        }
        self.producers.clear();
        self.broker.remove_session_metadata(&self.name);
    }

    fn queue_query_response(&self, name: &str) -> Packet {
        let r = self.broker.post_office().queue_query(name);
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
            .broker
            .post_office()
            .binding_query(address, ctx.is_version_new_fqqn());
        let v = ctx.version;
        let names: Vec<SimpleString> = r
            .queue_names
            .iter()
            .map(|n| SimpleString::from(&**n))
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

/// A journal failure as the client sees it (`ActiveMQIOErrorException`).
fn io_error(e: saranga_journal::JournalError) -> BrokerError {
    BrokerError::new(ErrorType::IoError, e.to_string())
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
