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
use saranga_core_protocol::{ErrorType, Flagged, Packet, version};
use saranga_message::CoreMessage;
use tokio::sync::mpsc;

use super::{ConnShared, CoreDeliverySink};
use crate::message::{MessageRef, ServerMessage, routing_type};
use crate::postoffice::{
    BindingQueryResult, QueueQueryResult, QueueSpec, RoutingTypes, queue_name_of,
};
use crate::queue::{ConsumerId, ConsumerRegistration, Queue};
use crate::storage::Pending;
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

/// A local transaction (`TransactionImpl` with its `RefsOperation`):
/// routed sends wait here until commit; acknowledged references are held
/// until commit removes them or rollback returns them.
#[derive(Default)]
struct Transaction {
    sends: Vec<(Queue, MessageRef)>,
    acks: Vec<(Queue, MessageRef)>,
    rollback_only: Option<BrokerError>,
}

impl Transaction {
    fn mark_rollback_only(&mut self, e: &BrokerError) {
        self.rollback_only.get_or_insert_with(|| e.clone());
    }
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
    /// Large messages arrive in Phase 5.
    #[allow(dead_code)]
    min_large_message_size: i32,
    /// XA sessions start without a transaction (Phase 3).
    xa: bool,
    auto_commit_sends: bool,
    auto_commit_acks: bool,
    pre_acknowledge: bool,
    default_address: Option<Name>,
    started: bool,
    closed: bool,
    consumers: BTreeMap<i64, ServerConsumer>,
    producers: HashMap<i32, Option<String>>,
    tx: Transaction,
    /// Journal writes lined up by the request being handled.
    ops: Vec<Pending>,
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
            tx: Transaction::default(),
            ops: Vec::new(),
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

    fn exception(&self, request: Request, e: &BrokerError) -> Packet {
        exception_packet(request, e)
    }

    /// `doConfirmAndResponse`, after the journal writes this request lined
    /// up.
    fn respond(&mut self, request: Request, response: Option<Packet>, flush: bool) {
        let _ = self.completer.send(Done {
            waits: std::mem::take(&mut self.ops),
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
                self.send_message(p.message).await,
                false,
            ),
            Packet::SessionSendMessageV2(p) => (
                p.requires_response,
                self.send_message(p.message).await,
                false,
            ),
            Packet::SessionSendMessageV3(p) => (
                p.requires_response,
                self.send_message(p.message).await,
                false,
            ),
            Packet::SessionSendMessage1X(p) => (
                p.requires_response,
                self.send_message(p.message).await,
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
                // Expiry addresses come with the Phase 4 queue semantics;
                // until then an expired message is simply removed.
                (
                    false,
                    self.acknowledge(p.consumer_id, p.message_id, true).await,
                    false,
                )
            }
            Packet::SessionRequestProducerCreditsMessage(p) => {
                // No paging yet, so credits are always granted.
                self.send(SessionProducerCreditsMessage {
                    credits: p.credits,
                    address: p.address,
                });
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
            other => {
                if self.xa {
                    tracing::warn!(session = %self.name, "XA arrives in Phase 3");
                }
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

    /// `ServerSessionImpl.send`: assigns the ID, routes, and either adds to
    /// the queues or holds the message in the transaction.
    async fn send_message(&mut self, mut message: CoreMessage) -> Result<Option<Packet>> {
        message.message_id = self.broker.post_office().ids().next();
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
        let r = MessageRef::new(ServerMessage::new(message));
        let queues = match self.broker.post_office().route(&r, &address, routing_type) {
            Ok(queues) => queues,
            Err(e) => {
                if !self.auto_commit_sends {
                    self.tx.mark_rollback_only(&e);
                }
                return Err(e);
            }
        };
        if self.auto_commit_sends {
            for q in queues {
                q.add(r.clone());
            }
        } else {
            self.tx
                .sends
                .extend(queues.into_iter().map(|q| (q, r.clone())));
        }
        Ok(None)
    }

    async fn acknowledge(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
    ) -> Result<Option<Packet>> {
        let transactional = !self.auto_commit_acks;
        let Some(c) = self.consumers.get(&consumer_id) else {
            let e = BrokerError::consumer_doesnt_exist(consumer_id);
            if transactional {
                self.tx.mark_rollback_only(&e);
            }
            return Err(e);
        };
        let queue = c.queue.clone();
        match queue.ack(c.id, message_id, individual, transactional).await {
            Ok(refs) => {
                self.tx
                    .acks
                    .extend(refs.into_iter().map(|r| (queue.clone(), r)));
                Ok(None)
            }
            Err(failed) => {
                let e = BrokerError::consumer_no_reference(consumer_id, message_id, queue.name());
                if transactional {
                    self.tx
                        .acks
                        .extend(failed.refs.into_iter().map(|r| (queue.clone(), r)));
                    self.tx.mark_rollback_only(&e);
                } else {
                    queue.reinsert(failed.refs);
                }
                Err(e)
            }
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

    /// `TransactionImpl.commit`: a transaction marked rollback-only rolls
    /// back and fails instead.
    fn commit(&mut self) -> Result<()> {
        let tx = std::mem::take(&mut self.tx);
        if let Some(e) = tx.rollback_only {
            return_refs(tx.acks);
            return Err(e);
        }
        for (q, r) in tx.sends {
            q.add(r);
        }
        let mut acked: Vec<(Queue, usize)> = Vec::new();
        for (q, _) in tx.acks {
            match acked.iter_mut().find(|(a, _)| *a == q) {
                Some((_, n)) => *n += 1,
                None => acked.push((q, 1)),
            }
        }
        for (q, n) in acked {
            q.acked(n);
        }
        Ok(())
    }

    /// `ServerSessionImpl.rollback`: drops the transaction's sends and
    /// returns its acknowledged and in-flight messages to their queues.
    async fn rollback(&mut self, last_as_delivered: bool, failed: bool) {
        let was_started = self.started;
        let tx = std::mem::take(&mut self.tx);
        let mut refs = tx.acks;
        for c in self.consumers.values() {
            if c.browse_only {
                continue;
            }
            let cancelled = c
                .queue
                .cancel_delivering(c.id, last_as_delivered, failed)
                .await;
            refs.extend(cancelled.into_iter().map(|r| (c.queue.clone(), r)));
        }
        return_refs(refs);
        if was_started && !failed {
            for c in self.consumers.values() {
                c.queue.set_started(c.id, true).await;
            }
        }
    }

    async fn set_started(&mut self, started: bool) {
        self.started = started;
        for c in self.consumers.values() {
            c.queue.set_started(c.id, started).await;
        }
    }

    /// `ServerSessionImpl.doClose`: rolls back the local transaction, then
    /// closes the consumers.
    async fn close(&mut self, failed: bool) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.rollback(false, failed).await;
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

/// Returns references to their queues, each queue's in one batch.
fn return_refs(refs: Vec<(Queue, MessageRef)>) {
    let mut by_queue: Vec<(Queue, Vec<MessageRef>)> = Vec::new();
    for (q, r) in refs {
        match by_queue.iter_mut().find(|(b, _)| *b == q) {
            Some((_, list)) => list.push(r),
            None => by_queue.push((q, vec![r])),
        }
    }
    for (q, list) in by_queue {
        q.reinsert(list);
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
