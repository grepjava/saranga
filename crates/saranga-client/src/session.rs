//! Sessions. Ported from `ClientSessionImpl` and `ActiveMQSessionContext`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use saranga_buffer::SimpleString;
use saranga_core_protocol::packets::connection::{SessionStart, SessionStop};
use saranga_core_protocol::packets::large::{
    SessionSendContinuationMessage, SessionSendContinuationMessageV2,
    SessionSendContinuationMessageV3, SessionSendLargeMessage,
};
use saranga_core_protocol::packets::session::*;
use saranga_core_protocol::{FRAME_HEADER_SIZE, Flagged, Packet, types};
use saranga_message::CoreMessage;

use crate::channel::{Channel, supports};
use crate::connection::{ConnInner, Factory};
use crate::consumer::{Consumer, ConsumerOptions, ConsumerShared};
use crate::defaults;
use crate::error::{Error, ErrorType, Result};
use crate::message;
use crate::producer::{CreditManager, Producer, ProducerShared};

/// Session settings, with the `ServerLocatorImpl` defaults a plain
/// `ClientSessionFactory.createSession()` uses (auto-commit sends and acks).
#[derive(Debug, Clone)]
pub struct SessionOptions {
    pub username: Option<String>,
    pub password: Option<String>,
    /// Sent in `CreateSessionMessage_V2` (the JMS connection's client ID).
    pub client_id: Option<String>,
    pub xa: bool,
    pub auto_commit_sends: bool,
    pub auto_commit_acks: bool,
    pub pre_acknowledge: bool,
    pub block_on_acknowledge: bool,
    pub block_on_durable_send: bool,
    pub block_on_non_durable_send: bool,
    /// Cumulative acks are only sent once this many bytes of messages have
    /// been acknowledged (or on commit, rollback and close). The JMS client
    /// uses 0 for AUTO_ACKNOWLEDGE sessions.
    pub ack_batch_size: i32,
    pub consumer_window_size: i32,
    pub producer_window_size: i32,
    /// Messages whose body exceeds this are sent as large messages: the
    /// headers, then the body in chunks of this size.
    pub min_large_message_size: i32,
    /// Large message bodies are sent zlib-compressed (`_AMQ_LARGE_COMPRESSED`);
    /// one that compresses below `min_large_message_size` goes as a regular
    /// message.
    pub compress_large_messages: bool,
    /// The `Deflater` level for compressed bodies: 0 to 9, or -1 for the
    /// default.
    pub compression_level: i32,
    /// `confirmationWindowSize`: -1 (the default) for none. With one,
    /// every send asks for a response, which the session's
    /// [`SendAcknowledgementHandler`] is told of, and the session confirms
    /// what it receives whenever this many bytes have arrived.
    pub confirmation_window_size: i32,
}

/// `SendAcknowledgementHandler`: told when the broker has a message a
/// session sent, with a confirmation window
/// ([`SessionOptions::confirmation_window_size`]).
pub trait SendAcknowledgementHandler: Send + Sync {
    /// The broker received the message (its response arrived).
    fn send_acknowledged(&self, message: &CoreMessage);

    /// The broker answered the send with an exception, or the connection
    /// failed before it answered. Ignored by default.
    fn send_failed(&self, message: &CoreMessage, error: &Error) {
        let _ = (message, error);
    }
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            username: None,
            password: None,
            client_id: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: defaults::PRE_ACKNOWLEDGE,
            block_on_acknowledge: defaults::BLOCK_ON_ACKNOWLEDGE,
            block_on_durable_send: defaults::BLOCK_ON_DURABLE_SEND,
            block_on_non_durable_send: defaults::BLOCK_ON_NON_DURABLE_SEND,
            ack_batch_size: defaults::ACK_BATCH_SIZE,
            consumer_window_size: defaults::CONSUMER_WINDOW_SIZE,
            producer_window_size: defaults::PRODUCER_WINDOW_SIZE,
            min_large_message_size: defaults::MIN_LARGE_MESSAGE_SIZE,
            compress_large_messages: defaults::COMPRESS_LARGE_MESSAGES,
            compression_level: defaults::COMPRESSION_LEVEL,
            confirmation_window_size: defaults::CONFIRMATION_WINDOW_SIZE,
        }
    }
}

impl SessionOptions {
    /// A transacted session: neither sends nor acks auto-commit.
    pub fn transacted() -> Self {
        Self {
            auto_commit_sends: false,
            auto_commit_acks: false,
            ..Self::default()
        }
    }
}

/// `RoutingType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RoutingType {
    Multicast,
    Anycast,
}

impl RoutingType {
    /// `RoutingType.getType()`.
    pub fn wire(self) -> i8 {
        match self {
            Self::Multicast => ROUTING_TYPE_MULTICAST,
            Self::Anycast => ROUTING_TYPE_ANYCAST,
        }
    }

    pub fn from_wire(b: i8) -> Option<Self> {
        match b {
            ROUTING_TYPE_MULTICAST => Some(Self::Multicast),
            ROUTING_TYPE_ANYCAST => Some(Self::Anycast),
            _ => None,
        }
    }
}

fn wire_routing(r: Option<RoutingType>) -> i8 {
    r.map_or(ROUTING_TYPE_NONE, RoutingType::wire)
}

/// `QueueConfiguration`, with the fields the Core create-queue packets carry.
/// `None` means "not set": the broker's address settings decide.
#[derive(Debug, Clone, Default)]
pub struct QueueConfiguration {
    pub name: String,
    /// Defaults to the queue name.
    pub address: Option<String>,
    pub routing_type: Option<RoutingType>,
    pub filter_string: Option<String>,
    /// Defaults to true.
    pub durable: Option<bool>,
    /// Defaults to false.
    pub temporary: Option<bool>,
    /// Defaults to false.
    pub auto_created: Option<bool>,
    pub max_consumers: Option<i32>,
    pub purge_on_no_consumers: Option<bool>,
    pub exclusive: Option<bool>,
    pub group_rebalance: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub group_buckets: Option<i32>,
    pub group_first_key: Option<String>,
    pub last_value: Option<bool>,
    pub last_value_key: Option<String>,
    pub non_destructive: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
}

impl QueueConfiguration {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    pub fn routing_type(mut self, routing_type: RoutingType) -> Self {
        self.routing_type = Some(routing_type);
        self
    }

    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter_string = Some(filter.into());
        self
    }

    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = Some(durable);
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = Some(temporary);
        self
    }

    pub fn auto_created(mut self, auto_created: bool) -> Self {
        self.auto_created = Some(auto_created);
        self
    }

    fn address_or_name(&self) -> SimpleString {
        self.address.as_deref().unwrap_or(&self.name).into()
    }
}

fn ss(s: &Option<String>) -> Option<SimpleString> {
    s.as_deref().map(Into::into)
}

fn ss_string(s: &Option<SimpleString>) -> Option<String> {
    s.as_ref().map(SimpleString::to_string_lossy)
}

/// `ClientSession.QueueQuery`, from the queue-query response.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueQuery {
    pub exists: bool,
    pub durable: bool,
    pub temporary: bool,
    pub consumer_count: i32,
    pub message_count: i64,
    pub filter_string: Option<String>,
    pub address: Option<String>,
    pub name: Option<String>,
    pub auto_create_queues: bool,
    pub auto_created: bool,
    pub purge_on_no_consumers: bool,
    pub routing_type: Option<RoutingType>,
    pub max_consumers: i32,
    pub exclusive: Option<bool>,
    pub last_value: Option<bool>,
    pub default_consumer_window_size: Option<i32>,
    pub non_destructive: Option<bool>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
    pub configuration_managed: Option<bool>,
}

impl QueueQuery {
    pub(crate) fn from_packet(packet: Packet) -> Result<Self> {
        Ok(match packet {
            Packet::SessionQueueQueryResponseMessageV3(p) => Self {
                exists: p.exists,
                durable: p.durable,
                temporary: p.temporary,
                consumer_count: p.consumer_count,
                message_count: p.message_count,
                filter_string: ss_string(&p.filter_string),
                address: ss_string(&p.address),
                name: ss_string(&p.name),
                auto_create_queues: p.auto_create_queues,
                auto_created: p.auto_created,
                purge_on_no_consumers: p.purge_on_no_consumers,
                routing_type: RoutingType::from_wire(p.routing_type),
                max_consumers: p.max_consumers,
                exclusive: p.exclusive.0,
                last_value: p.last_value.0,
                default_consumer_window_size: p.default_consumer_window_size.0,
                non_destructive: p.non_destructive.0,
                ring_size: p.ring_size.0,
                enabled: p.enabled.0,
                configuration_managed: p.configuration_managed.0,
            },
            Packet::SessionQueueQueryResponseMessageV2(p) => Self {
                exists: p.exists,
                durable: p.durable,
                temporary: p.temporary,
                consumer_count: p.consumer_count,
                message_count: p.message_count,
                filter_string: ss_string(&p.filter_string),
                address: ss_string(&p.address),
                name: ss_string(&p.name),
                auto_create_queues: p.auto_create_queues,
                max_consumers: defaults::MAX_QUEUE_CONSUMERS,
                ..Self::default()
            },
            Packet::SessionQueueQueryResponseMessage(p) => Self {
                exists: p.exists,
                durable: p.durable,
                temporary: p.temporary,
                consumer_count: p.consumer_count,
                message_count: p.message_count,
                filter_string: ss_string(&p.filter_string),
                address: ss_string(&p.address),
                name: ss_string(&p.name),
                max_consumers: defaults::MAX_QUEUE_CONSUMERS,
                ..Self::default()
            },
            other => return Err(Error::UnexpectedResponse(other.name())),
        })
    }
}

/// `ClientSession.AddressQuery`, from the binding-query response.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AddressQuery {
    pub exists: bool,
    pub queue_names: Vec<String>,
    pub auto_create_queues: bool,
    pub auto_create_addresses: bool,
    pub default_purge_on_no_consumers: bool,
    pub default_max_consumers: i32,
    pub default_exclusive: Option<bool>,
    pub default_last_value: Option<bool>,
    pub default_last_value_key: Option<String>,
    pub default_non_destructive: Option<bool>,
    pub default_consumers_before_dispatch: Option<i32>,
    pub default_delay_before_dispatch: Option<i64>,
    pub supports_multicast: bool,
    pub supports_anycast: bool,
}

impl AddressQuery {
    /// `ActiveMQSessionContext.addressQuery`: which fields each response
    /// version fills, and the defaults for the rest.
    fn from_packet(packet: Packet) -> Result<Self> {
        let names = |n: &[SimpleString]| n.iter().map(SimpleString::to_string_lossy).collect();
        let with_defaults = |d: &BindingQueryDefaults, base: Self| Self {
            default_purge_on_no_consumers: d.default_purge_on_no_consumers,
            default_max_consumers: d.default_max_consumers,
            default_exclusive: d.default_exclusive.0,
            default_last_value: d.default_last_value.0,
            default_last_value_key: ss_string(&d.default_last_value_key),
            default_non_destructive: d.default_non_destructive.0,
            default_consumers_before_dispatch: d.default_consumers_before_dispatch.0,
            default_delay_before_dispatch: d.default_delay_before_dispatch.0,
            ..base
        };
        let base = Self {
            default_purge_on_no_consumers: defaults::PURGE_ON_NO_CONSUMERS,
            default_max_consumers: defaults::MAX_QUEUE_CONSUMERS,
            supports_multicast: true,
            supports_anycast: true,
            ..Self::default()
        };
        Ok(match packet {
            Packet::SessionBindingQueryResponseMessageV5(p) => with_defaults(
                &p.defaults,
                Self {
                    exists: p.exists,
                    queue_names: names(&p.queue_names),
                    auto_create_queues: p.auto_create_queues,
                    auto_create_addresses: p.auto_create_addresses,
                    supports_multicast: p.supports_multicast,
                    supports_anycast: p.supports_anycast,
                    ..base
                },
            ),
            Packet::SessionBindingQueryResponseMessageV4(p) => with_defaults(
                &p.defaults,
                Self {
                    exists: p.exists,
                    queue_names: names(&p.queue_names),
                    auto_create_queues: p.auto_create_queues,
                    auto_create_addresses: p.auto_create_addresses,
                    ..base
                },
            ),
            Packet::SessionBindingQueryResponseMessageV3(p) => Self {
                exists: p.exists,
                queue_names: names(&p.queue_names),
                auto_create_queues: p.auto_create_queues,
                auto_create_addresses: p.auto_create_addresses,
                ..base
            },
            Packet::SessionBindingQueryResponseMessageV2(p) => Self {
                exists: p.exists,
                queue_names: names(&p.queue_names),
                auto_create_queues: p.auto_create_queues,
                ..base
            },
            Packet::SessionBindingQueryResponseMessage(p) => Self {
                exists: p.exists,
                queue_names: names(&p.queue_names),
                ..base
            },
            other => return Err(Error::UnexpectedResponse(other.name())),
        })
    }
}

#[derive(Default)]
struct SessionState {
    started: bool,
    closed: bool,
    in_close: bool,
    consumers: BTreeMap<i64, Arc<ConsumerShared>>,
    producers: BTreeMap<i32, Arc<ProducerShared>>,
    /// `workDone`: a message was sent or received since the last commit
    /// or rollback.
    work_done: bool,
    /// `rollbackOnly`: the session failed over with work in its
    /// transaction, so the next commit rolls back.
    rollback_only: bool,
    /// `resetCreditManager`: producer credits are asked for again once the
    /// failover is done.
    reset_credit_manager: bool,
}

pub(crate) struct SessionInner {
    factory: Arc<Factory>,
    /// `ActiveMQSessionContext.remotingConnection`: replaced on failover.
    conn: Mutex<Arc<ConnInner>>,
    pub(crate) channel: Arc<Channel>,
    weak_self: Weak<SessionInner>,
    /// Renamed on failover, so the server cannot close the recreated
    /// session under the old name.
    name: Mutex<String>,
    /// `ClientSessionImpl.metadata`: sent again after a failover.
    metadata: Mutex<Vec<(String, String)>>,
    pub(crate) options: SessionOptions,
    server_version: i32,
    state: Mutex<SessionState>,
    /// `ActiveMQSessionContext.idGenerator` (consumer IDs, from 0).
    next_consumer_id: AtomicI64,
    /// `ClientSessionImpl.producerIDs` (incrementAndGet, so from 1).
    next_producer_id: AtomicI32,
    pub(crate) credits: Mutex<CreditManager>,
    /// `ActiveMQSessionContext.sendAckHandler`.
    send_ack_handler: Mutex<Option<Arc<dyn SendAcknowledgementHandler>>>,
}

impl SessionInner {
    pub(crate) fn new(
        factory: Arc<Factory>,
        conn: Arc<ConnInner>,
        channel: Arc<Channel>,
        name: String,
        options: SessionOptions,
        server_version: i32,
    ) -> Arc<Self> {
        let window = options.producer_window_size;
        Arc::new_cyclic(|weak_self| Self {
            factory,
            conn: Mutex::new(conn),
            channel,
            weak_self: weak_self.clone(),
            name: Mutex::new(name),
            metadata: Mutex::new(Vec::new()),
            options,
            server_version,
            state: Mutex::new(SessionState::default()),
            next_consumer_id: AtomicI64::new(0),
            next_producer_id: AtomicI32::new(0),
            credits: Mutex::new(CreditManager::new(window)),
            send_ack_handler: Mutex::new(None),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, SessionState> {
        self.state.lock().expect("session")
    }

    /// The connection the session is on.
    pub(crate) fn conn(&self) -> Arc<ConnInner> {
        self.conn.lock().expect("session").clone()
    }

    /// `ClientSessionImpl.checkClosed`, reporting a connection failure as
    /// its own error when the session cannot fail over.
    pub(crate) fn check_closed(&self) -> Result<()> {
        if !self.factory.can_failover()
            && let Some(e) = self.conn().failure()
        {
            return Err(e);
        }
        let st = self.state();
        if st.closed || st.in_close {
            return Err(Error::session_closed());
        }
        Ok(())
    }

    /// `ChannelImpl.send` without waiting: on a connection that failed, the
    /// failure.
    pub(crate) fn send(&self, packet: impl Into<Packet>) -> Result<()> {
        self.conn().send(self.channel.id, &packet.into())
    }

    /// `ChannelImpl.send`: after a failover in progress.
    pub(crate) async fn send_async(&self, packet: impl Into<Packet>) -> Result<()> {
        self.factory.wait_failover().await;
        self.send(packet)
    }

    /// `ChannelImpl.sendBlocking`: after a failover in progress.
    pub(crate) async fn send_blocking(
        &self,
        packet: impl Into<Packet>,
        expected: i8,
    ) -> Result<Packet> {
        self.factory.wait_failover().await;
        self.conn()
            .send_blocking(&self.channel, packet.into(), expected)
            .await
    }

    /// `ClientSessionImpl.workDone`.
    pub(crate) fn work_done(&self) {
        self.state().work_done = true;
    }

    fn consumers(&self) -> Vec<Arc<ConsumerShared>> {
        self.state().consumers.values().cloned().collect()
    }

    pub(crate) fn remove_consumer(&self, id: i64) {
        self.state().consumers.remove(&id);
    }

    pub(crate) fn remove_producer(&self, id: i32) {
        self.state().producers.remove(&id);
    }

    /// `ActiveMQSessionContext.sendConsumerCredits`.
    pub(crate) fn send_consumer_credits(&self, consumer_id: i64, credits: i32) {
        let _ = self.send(SessionConsumerFlowCreditMessage {
            consumer_id,
            credits,
        });
    }

    /// `ClientSessionImpl.acknowledge` + `ActiveMQSessionContext.sendACK`.
    pub(crate) async fn acknowledge(&self, consumer_id: i64, message_id: i64) -> Result<()> {
        if self.options.pre_acknowledge {
            return Ok(());
        }
        self.check_closed()?;
        let block = self.options.block_on_acknowledge;
        let packet = SessionAcknowledgeMessage {
            consumer_id,
            message_id,
            requires_response: block,
        };
        if block {
            self.send_blocking(packet, types::NULL_RESPONSE).await?;
            Ok(())
        } else {
            self.send_async(packet).await
        }
    }

    /// `ClientSessionImpl.individualAcknowledge`.
    pub(crate) async fn individual_acknowledge(
        &self,
        consumer_id: i64,
        message_id: i64,
    ) -> Result<()> {
        if self.options.pre_acknowledge {
            return Ok(());
        }
        self.check_closed()?;
        let block = self.options.block_on_acknowledge;
        let packet = SessionIndividualAcknowledgeMessage {
            consumer_id,
            message_id,
            requires_response: block,
        };
        if block {
            self.send_blocking(packet, types::NULL_RESPONSE).await?;
            Ok(())
        } else {
            self.send_async(packet).await
        }
    }

    /// `ClientSessionImpl.expire`.
    pub(crate) fn expire(&self, consumer_id: i64, message_id: i64) -> Result<()> {
        self.check_closed()?;
        if !self.options.pre_acknowledge {
            self.send(SessionExpireMessage {
                consumer_id,
                message_id,
            })?;
        }
        Ok(())
    }

    /// `ActiveMQSessionContext.sendFullMessage`: the send packet for the
    /// negotiated version, blocking or not.
    pub(crate) async fn send_full_message(
        &self,
        message: CoreMessage,
        send_blocking: bool,
        sender_id: i32,
    ) -> Result<()> {
        let ctx = self.conn().context();
        // From the async response change on, a confirmation window makes
        // every send ask for its response.
        let cached = !ctx.is_version_before_async_response_change() && self.windowed();
        let requires_response = send_blocking || cached;
        let correlation_id = if cached {
            let message = self.send_ack_handler().map(|h| (h, message.clone()));
            self.async_send(message)
        } else {
            0
        };
        let packet: Packet = if ctx.is_version_before_address_change() {
            let end_of_message_position = legacy_end_of_message_position(&message);
            SessionSendMessage1X {
                message,
                end_of_message_position,
                requires_response: send_blocking,
            }
            .into()
        } else if ctx.is_version_before_async_response_change() {
            saranga_core_protocol::packets::message::SessionSendMessage {
                message,
                requires_response: send_blocking,
            }
            .into()
        } else if ctx.is_before_producer_metrics_changed() {
            saranga_core_protocol::packets::message::SessionSendMessageV2 {
                message,
                requires_response,
                correlation_id,
            }
            .into()
        } else {
            saranga_core_protocol::packets::message::SessionSendMessageV3 {
                message,
                requires_response,
                correlation_id,
                sender_id,
            }
            .into()
        };
        self.send_maybe_blocking(packet, send_blocking, correlation_id)
            .await
    }

    /// Whether the session has a confirmation window.
    fn windowed(&self) -> bool {
        self.options.confirmation_window_size != -1
    }

    pub(crate) fn send_ack_handler(&self) -> Option<Arc<dyn SendAcknowledgementHandler>> {
        self.send_ack_handler.lock().expect("session").clone()
    }

    /// Puts a send in the channel's async response cache, with its handler
    /// and message when there is a handler. Returns its correlation ID.
    fn async_send(
        &self,
        message: Option<(Arc<dyn SendAcknowledgementHandler>, CoreMessage)>,
    ) -> i64 {
        let (handler, message) = match message {
            Some((h, m)) => (Some(h), Some(m)),
            None => (None, None),
        };
        self.channel
            .add_async_send(crate::channel::AsyncSend { message, handler })
    }

    /// `ChannelImpl.sendBlocking` or `send`; a send that was never written
    /// leaves the async response cache.
    async fn send_maybe_blocking(
        &self,
        packet: Packet,
        blocking: bool,
        correlation_id: i64,
    ) -> Result<()> {
        let result = if blocking {
            self.send_blocking(packet, types::NULL_RESPONSE)
                .await
                .map(|_| ())
        } else {
            self.send_async(packet).await
        };
        if result.is_err() && correlation_id > 0 {
            self.channel.remove_async_send(correlation_id);
        }
        result
    }

    /// `ActiveMQSessionContext.sendInitialChunkOnLargeMessage`: the headers
    /// and properties (`SessionSendLargeMessage`, not blocking). Returns
    /// their encode size, the credits the header takes.
    pub(crate) async fn send_initial_chunk_on_large_message(
        &self,
        message: CoreMessage,
    ) -> Result<i32> {
        let size = message::headers_and_properties_encode_size(&message);
        self.send_async(SessionSendLargeMessage { message }).await?;
        Ok(size)
    }

    /// `ActiveMQSessionContext.sendLargeMessageChunk`: a body chunk in the
    /// continuation packet for the negotiated version. Only the last chunk
    /// of a blocking send waits for the broker; with a confirmation window
    /// every chunk asks for a response, and the handler is told of
    /// `message` (the headers) when the last one's arrives. Returns the
    /// packet size (`getPacketSize`), the credits the chunk takes.
    pub(crate) async fn send_large_message_chunk(
        &self,
        message: &CoreMessage,
        message_body_size: i64,
        send_blocking: bool,
        last_chunk: bool,
        chunk: Vec<u8>,
        sender_id: i32,
    ) -> Result<i32> {
        let ctx = self.conn().context();
        let requires_response = last_chunk && send_blocking;
        let cached = !ctx.is_version_before_async_response_change() && self.windowed();
        let correlation_id = if cached {
            let message = self
                .send_ack_handler()
                .filter(|_| last_chunk)
                .map(|h| (h, message.clone()));
            Some(self.async_send(message))
        } else {
            None
        };
        // The frame header, the chunk with its length, `continues`, the body
        // size on the last chunk and `requiresResponse`
        // (`SessionSendContinuationMessage.expectedEncodeSize`).
        let mut size = FRAME_HEADER_SIZE + 4 + chunk.len() + 1 + 1;
        if last_chunk {
            size += 8;
        }
        let packet: Packet = if ctx.is_version_before_async_response_change() {
            SessionSendContinuationMessage {
                body: chunk,
                continues: !last_chunk,
                message_body_size,
                requires_response,
            }
            .into()
        } else if ctx.is_before_producer_metrics_changed() {
            size += 8;
            SessionSendContinuationMessageV2 {
                body: chunk,
                continues: !last_chunk,
                message_body_size,
                requires_response: requires_response || cached,
                correlation_id: Some(correlation_id.unwrap_or(0)),
            }
            .into()
        } else {
            size += 8 + 4;
            SessionSendContinuationMessageV3 {
                body: chunk,
                continues: !last_chunk,
                message_body_size,
                requires_response: requires_response || cached,
                correlation_id: Some(correlation_id.unwrap_or(0)),
                sender_id,
            }
            .into()
        };
        self.send_maybe_blocking(packet, requires_response, correlation_id.unwrap_or(0))
            .await?;
        Ok(size as i32)
    }

    async fn flush_acks(&self) -> Result<()> {
        for c in self.consumers() {
            c.flush_acks(self).await?;
        }
        Ok(())
    }

    async fn start(&self) -> Result<()> {
        self.check_closed()?;
        if self.state().started {
            return Ok(());
        }
        for c in self.consumers() {
            c.start();
        }
        self.send_async(SessionStart::default()).await?;
        self.state().started = true;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.check_closed()?;
        if !self.state().started {
            return Ok(());
        }
        for c in self.consumers() {
            c.stop();
        }
        self.send_blocking(SessionStop::default(), types::NULL_RESPONSE)
            .await?;
        self.state().started = false;
        Ok(())
    }

    /// `ClientSessionImpl.close`: closes consumers and producers, then the
    /// session. Errors are swallowed, as in Java.
    pub(crate) async fn close(&self) {
        if self.state().closed {
            return;
        }
        let (consumers, producers) = {
            let st = self.state();
            (
                st.consumers.values().cloned().collect::<Vec<_>>(),
                st.producers.values().cloned().collect::<Vec<_>>(),
            )
        };
        for c in consumers {
            c.close(self).await;
        }
        for p in producers {
            p.close(self);
        }
        self.credits.lock().expect("credits").close();
        self.state().in_close = true;
        let _ = self
            .send_blocking(SessionCloseMessage::default(), types::NULL_RESPONSE)
            .await;
        self.state().closed = true;
        self.channel.close(&Error::session_closed());
        self.conn().remove_channel(self.channel.id);
    }

    /// The connection failed: everything waiting on this session wakes up
    /// with the failure.
    pub(crate) fn connection_failed(&self, err: &Error) {
        let consumers = {
            let mut st = self.state();
            st.closed = true;
            std::mem::take(&mut st.consumers)
        };
        self.channel.close(err);
        for c in consumers.values() {
            c.fail(err);
        }
        self.credits.lock().expect("credits").close();
    }

    /// `ActiveMQSessionContext.transferConnection`: the session's channel
    /// moves to a new connection.
    pub(crate) fn transfer_connection(&self, conn: &Arc<ConnInner>) {
        conn.add_channel(self.channel.clone());
        *self.conn.lock().expect("session") = conn.clone();
    }

    /// `ClientSessionImpl.handleFailover`: the session is recreated on the
    /// new connection under a new name, with its consumers (and their
    /// non-durable or auto-created queues), and started again if it was;
    /// a transaction with work in it can now only roll back.
    pub(crate) async fn handle_failover(&self, conn: &Arc<ConnInner>, cause: &Error) -> Result<()> {
        let (in_close, started) = {
            let st = self.state();
            if st.closed {
                return Ok(());
            }
            (st.in_close, st.started)
        };
        self.transfer_connection(conn);
        let name = crate::uuid::to_string(&crate::uuid::time_based());
        *self.name.lock().expect("session") = name.clone();
        let consumers = self.consumers();
        for c in &consumers {
            c.clear_at_failover();
        }
        if !in_close {
            self.recreate_session(conn, &name).await?;
            for c in &consumers {
                if !c.is_closed() {
                    self.recreate_consumer_on_server(c, started)?;
                }
            }
            {
                let mut st = self.state();
                if (!self.options.auto_commit_acks || !self.options.auto_commit_sends)
                    && st.work_done
                {
                    st.rollback_only = true;
                }
            }
            if started {
                for c in &consumers {
                    c.clear_at_failover();
                    c.start();
                }
                self.send(SessionStart::default())?;
            }
            self.state().reset_credit_manager = true;
        }
        self.channel.return_blocking(cause);
        Ok(())
    }

    /// `ActiveMQSessionContext.recreateSession`: retried while the server is
    /// still starting.
    async fn recreate_session(&self, conn: &ConnInner, name: &str) -> Result<()> {
        loop {
            match self
                .factory
                .send_create_session(conn, name, self.channel.id, &self.options)
                .await
            {
                Err(e) if e.kind() == Some(ErrorType::SessionCreationRejected) => {
                    tracing::warn!(
                        "AMQ212075: Session {name} was rejected while the server was starting, retrying"
                    );
                    if self.state().in_close {
                        return Err(e);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                other => return other.map(|_| ()),
            }
        }
    }

    /// `ActiveMQSessionContext.recreateConsumerOnServer`.
    fn recreate_consumer_on_server(&self, c: &ConsumerShared, started: bool) -> Result<()> {
        let q = c.queue_info();
        // Non-durable and auto-created queues may be gone after the failover.
        if !q.durable || q.auto_created {
            self.send(CreateQueueMessageV2 {
                address: q.address.as_deref().unwrap_or_default().into(),
                queue_name: q.name.as_deref().unwrap_or_default().into(),
                filter_string: ss(&q.filter_string),
                durable: q.durable,
                temporary: q.temporary,
                requires_response: false,
                auto_created: q.auto_created,
                routing_type: wire_routing(q.routing_type),
                max_consumers: q.max_consumers,
                purge_on_no_consumers: q.purge_on_no_consumers,
                exclusive: Flagged(q.exclusive),
                last_value: Flagged(q.last_value),
                last_value_key: None,
                non_destructive: Flagged(q.non_destructive),
                consumers_before_dispatch: Flagged(None),
                delay_before_dispatch: Flagged(None),
                group_rebalance: Flagged(None),
                group_buckets: Flagged(None),
                auto_delete: Flagged(None),
                auto_delete_delay: Flagged(None),
                auto_delete_message_count: Flagged(None),
                group_first_key: None,
                ring_size: Flagged(q.ring_size),
                enabled: Flagged(q.enabled),
                group_rebalance_pause_dispatch: Flagged(None),
                sections: CreateQueueMessageV2::SECTIONS,
            })?;
        }
        self.send(SessionCreateConsumerMessage {
            id: c.id,
            queue_name: c.queue_name().clone(),
            filter_string: c.filter().cloned(),
            browse_only: c.is_browse_only(),
            requires_response: false,
            priority: c.priority(),
        })?;
        let window = c.client_window_size();
        self.send(SessionConsumerFlowCreditMessage {
            consumer_id: c.id,
            credits: if window != 0 { window } else { 1 },
        })?;
        // A forced delivery the old server never answered would leave
        // `receive` waiting.
        let forced = c.force_delivery_count();
        if started && forced > 0 {
            self.send(SessionForceConsumerDelivery {
                consumer_id: c.id,
                sequence: forced - 1,
            })?;
        }
        Ok(())
    }

    /// `ClientSessionImpl.postHandleFailover` after a successful failover:
    /// producer credits are asked for again and the metadata sent again.
    pub(crate) fn post_handle_failover(&self) {
        let reset = {
            let mut st = self.state();
            if st.closed {
                return;
            }
            std::mem::take(&mut st.reset_credit_manager)
        };
        if reset {
            self.credits.lock().expect("credits").reset(self);
        }
        let metadata = self.metadata.lock().expect("session").clone();
        for (key, data) in metadata {
            let _ = self.send(SessionAddMetaDataMessageV2 {
                key: key.as_str().into(),
                data: data.as_str().into(),
                requires_confirmation: false,
            });
        }
    }

    /// `ClientSessionPacketHandler.handlePacket`.
    pub(crate) fn handle_packet(&self, packet: Packet, packet_size: i32) {
        let consumer = |id: i64| self.state().consumers.get(&id).cloned();
        match packet {
            Packet::SessionReceiveMessage(p) => {
                if let Some(c) = consumer(p.consumer_id) {
                    c.handle_message(p.message, p.delivery_count, packet_size);
                }
            }
            Packet::SessionReceiveMessage1X(p) => {
                if let Some(c) = consumer(p.consumer_id) {
                    c.handle_message(p.message, p.delivery_count, packet_size);
                }
            }
            Packet::SessionReceiveClientLargeMessage(p) => {
                if let Some(c) = consumer(p.consumer_id) {
                    c.handle_large_message(p.message, p.delivery_count, packet_size);
                }
            }
            Packet::SessionReceiveContinuationMessage(p) => {
                if let Some(c) = consumer(p.consumer_id) {
                    c.handle_large_message_continuation(self, p.body, packet_size, p.continues);
                }
            }
            Packet::SessionProducerCreditsMessage(p) => {
                self.credits
                    .lock()
                    .expect("credits")
                    .receive_credits(&p.address, p.credits);
            }
            Packet::SessionProducerCreditsFailMessage(p) => {
                self.credits
                    .lock()
                    .expect("credits")
                    .receive_fail_credits(&p.address, p.credits);
            }
            Packet::DisconnectConsumerMessage(p) => {
                // handleConsumerDisconnect: close the consumer on another task.
                if let (Some(c), Some(session)) =
                    (consumer(p.consumer_id), self.weak_self.upgrade())
                {
                    tokio::spawn(async move { c.close(&session).await });
                }
            }
            _ => {}
        }
    }
}

/// A Core session (`ClientSession`). Cloning gives another handle to the
/// same session.
#[derive(Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

impl Session {
    pub(crate) fn from_inner(inner: Arc<SessionInner>) -> Self {
        Self { inner }
    }

    /// The session name (a time-based UUID, a new one after a failover).
    pub fn name(&self) -> String {
        self.inner.name.lock().expect("session").clone()
    }

    /// The version from `CreateSessionResponseMessage`.
    pub fn server_version(&self) -> i32 {
        self.inner.server_version
    }

    pub fn options(&self) -> &SessionOptions {
        &self.inner.options
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state().closed
    }

    /// `setSendAcknowledgementHandler`: told of each message sent from now
    /// on once the broker has it. Only with a confirmation window.
    pub fn set_send_acknowledgement_handler(
        &self,
        handler: Arc<dyn SendAcknowledgementHandler>,
    ) -> Result<()> {
        if !self.inner.windowed() {
            return Err(Error::InvalidArgument(
                "You can't set confirmationHandler on a connection with confirmation-window-size < 0. Look at the documentation for more information.".to_owned(),
            ));
        }
        *self.inner.send_ack_handler.lock().expect("session") = Some(handler);
        Ok(())
    }

    /// `SESS_START` (not blocking); starts message delivery.
    pub async fn start(&self) -> Result<()> {
        self.inner.start().await
    }

    /// `SESS_STOP` (blocking).
    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    pub async fn close(&self) {
        self.inner.close().await
    }

    /// `SessionAddMetaDataMessageV2` (blocking). Kept, to be sent again
    /// after a failover.
    pub async fn add_metadata(&self, key: &str, data: &str) -> Result<()> {
        self.inner.check_closed()?;
        {
            let mut metadata = self.inner.metadata.lock().expect("session");
            metadata.retain(|(k, _)| k != key);
            metadata.push((key.to_owned(), data.to_owned()));
        }
        self.inner
            .send_blocking(
                SessionAddMetaDataMessageV2 {
                    key: key.into(),
                    data: data.into(),
                    requires_confirmation: true,
                },
                types::NULL_RESPONSE,
            )
            .await?;
        Ok(())
    }

    /// `SessionUniqueAddMetaDataMessage` (blocking). Fails with
    /// `DUPLICATE_METADATA` if another session has the same entry.
    pub async fn add_unique_metadata(&self, key: &str, data: &str) -> Result<()> {
        self.inner.check_closed()?;
        self.inner
            .send_blocking(
                SessionUniqueAddMetaDataMessage {
                    key: key.into(),
                    data: data.into(),
                    requires_confirmation: true,
                },
                types::NULL_RESPONSE,
            )
            .await?;
        Ok(())
    }

    /// `CreateAddressMessage` (blocking). Routing types are sent in
    /// `EnumSet` order.
    pub async fn create_address(
        &self,
        address: &str,
        routing_types: &[RoutingType],
        auto_created: bool,
    ) -> Result<()> {
        self.inner.check_closed()?;
        if self
            .inner
            .conn()
            .context()
            .is_version_before_address_change()
        {
            return Ok(());
        }
        let mut types_set = routing_types.to_vec();
        types_set.sort();
        types_set.dedup();
        self.inner
            .send_blocking(
                CreateAddressMessage {
                    address: address.into(),
                    routing_types: types_set.into_iter().map(RoutingType::wire).collect(),
                    requires_response: true,
                    auto_created,
                },
                types::NULL_RESPONSE,
            )
            .await?;
        Ok(())
    }

    /// `ActiveMQSessionContext.createQueue`: `CreateQueueMessage_V2` with
    /// max-consumers and purge-on-no-consumers defaulted when unset.
    pub async fn create_queue(&self, config: QueueConfiguration) -> Result<()> {
        self.inner.check_closed()?;
        let durable = config.durable.unwrap_or(true);
        let temporary = config.temporary.unwrap_or(false);
        if durable && temporary {
            return Err(Error::amq(
                ErrorType::InternalError,
                "AMQ219003: Queue can not be both durable and temporary",
            ));
        }
        let max_consumers = config
            .max_consumers
            .unwrap_or(defaults::MAX_QUEUE_CONSUMERS);
        let purge_on_no_consumers = config
            .purge_on_no_consumers
            .unwrap_or(defaults::PURGE_ON_NO_CONSUMERS);
        let packet: Packet = if self
            .inner
            .conn()
            .context()
            .is_version_before_address_change()
        {
            CreateQueueMessage {
                address: config.address_or_name(),
                queue_name: config.name.as_str().into(),
                filter_string: ss(&config.filter_string),
                durable,
                temporary,
                requires_response: true,
            }
            .into()
        } else {
            CreateQueueMessageV2 {
                address: config.address_or_name(),
                queue_name: config.name.as_str().into(),
                filter_string: ss(&config.filter_string),
                durable,
                temporary,
                requires_response: true,
                auto_created: config.auto_created.unwrap_or(false),
                routing_type: wire_routing(config.routing_type),
                max_consumers,
                purge_on_no_consumers,
                exclusive: Flagged(config.exclusive),
                last_value: Flagged(config.last_value),
                last_value_key: ss(&config.last_value_key),
                non_destructive: Flagged(config.non_destructive),
                consumers_before_dispatch: Flagged(config.consumers_before_dispatch),
                delay_before_dispatch: Flagged(config.delay_before_dispatch),
                group_rebalance: Flagged(config.group_rebalance),
                group_buckets: Flagged(config.group_buckets),
                auto_delete: Flagged(config.auto_delete),
                auto_delete_delay: Flagged(config.auto_delete_delay),
                auto_delete_message_count: Flagged(config.auto_delete_message_count),
                group_first_key: ss(&config.group_first_key),
                ring_size: Flagged(config.ring_size),
                enabled: Flagged(config.enabled),
                group_rebalance_pause_dispatch: Flagged(config.group_rebalance_pause_dispatch),
                sections: CreateQueueMessageV2::SECTIONS,
            }
            .into()
        };
        self.inner
            .send_blocking(packet, types::NULL_RESPONSE)
            .await?;
        Ok(())
    }

    /// `CreateSharedQueueMessage_V2` (blocking). Unlike `create_queue`,
    /// nothing is defaulted on the client.
    pub async fn create_shared_queue(&self, config: QueueConfiguration) -> Result<()> {
        self.inner.check_closed()?;
        self.inner
            .send_blocking(
                CreateSharedQueueMessageV2 {
                    address: config.address_or_name(),
                    queue_name: config.name.as_str().into(),
                    filter_string: ss(&config.filter_string),
                    durable: config.durable.unwrap_or(true),
                    routing_type: wire_routing(config.routing_type),
                    requires_response: true,
                    max_consumers: Flagged(config.max_consumers),
                    purge_on_no_consumers: Flagged(config.purge_on_no_consumers),
                    exclusive: Flagged(config.exclusive),
                    last_value: Flagged(config.last_value),
                    last_value_key: ss(&config.last_value_key),
                    non_destructive: Flagged(config.non_destructive),
                    consumers_before_dispatch: Flagged(config.consumers_before_dispatch),
                    delay_before_dispatch: Flagged(config.delay_before_dispatch),
                    group_rebalance: Flagged(config.group_rebalance),
                    group_buckets: Flagged(config.group_buckets),
                    auto_delete: Flagged(config.auto_delete),
                    auto_delete_delay: Flagged(config.auto_delete_delay),
                    auto_delete_message_count: Flagged(config.auto_delete_message_count),
                    group_first_key: ss(&config.group_first_key),
                    ring_size: Flagged(config.ring_size),
                    enabled: Flagged(config.enabled),
                    group_rebalance_pause_dispatch: Flagged(config.group_rebalance_pause_dispatch),
                    sections: CreateSharedQueueMessageV2::SECTIONS,
                },
                types::NULL_RESPONSE,
            )
            .await?;
        Ok(())
    }

    /// `SessionDeleteQueueMessage` (blocking).
    pub async fn delete_queue(&self, queue: &str) -> Result<()> {
        self.inner.check_closed()?;
        self.inner
            .send_blocking(
                SessionDeleteQueueMessage {
                    queue_name: queue.into(),
                },
                types::NULL_RESPONSE,
            )
            .await?;
        Ok(())
    }

    fn queue_query_response_type(&self) -> i8 {
        if self
            .inner
            .conn()
            .context()
            .is_version_before_address_change()
        {
            types::SESS_QUEUEQUERY_RESP_V2
        } else {
            types::SESS_QUEUEQUERY_RESP_V3
        }
    }

    /// `SessionQueueQueryMessage` (blocking).
    pub async fn queue_query(&self, queue: &str) -> Result<QueueQuery> {
        self.inner.check_closed()?;
        let response = self
            .inner
            .send_blocking(
                SessionQueueQueryMessage {
                    queue_name: queue.into(),
                },
                self.queue_query_response_type(),
            )
            .await?;
        QueueQuery::from_packet(response)
    }

    /// `SessionBindingQueryMessage` (blocking). The expected response
    /// version follows the server version.
    pub async fn address_query(&self, address: &str) -> Result<AddressQuery> {
        self.inner.check_closed()?;
        let v = self.inner.server_version;
        let expected = [
            types::SESS_BINDINGQUERY_RESP_V5,
            types::SESS_BINDINGQUERY_RESP_V4,
            types::SESS_BINDINGQUERY_RESP_V3,
            types::SESS_BINDINGQUERY_RESP_V2,
        ]
        .into_iter()
        .find(|t| supports(*t, v))
        .unwrap_or(types::SESS_BINDINGQUERY_RESP);
        let response = self
            .inner
            .send_blocking(
                SessionBindingQueryMessage {
                    address: address.into(),
                },
                expected,
            )
            .await?;
        AddressQuery::from_packet(response)
    }

    /// Creates a producer, sending to `address` (or to the address given on
    /// each send when `None`).
    pub async fn create_producer(&self, address: Option<&str>) -> Result<Producer> {
        let inner = &self.inner;
        inner.check_closed()?;
        let id = inner.next_producer_id.fetch_add(1, Ordering::SeqCst) + 1;
        let address: Option<SimpleString> = address.map(Into::into);
        let o = &inner.options;
        // The named producer's reference on its address credits; this
        // requests the first credits.
        if let Some(a) = &address {
            inner
                .credits
                .lock()
                .expect("credits")
                .get_credits(inner, a, false);
        }
        let shared = Arc::new(ProducerShared::new(
            id,
            address.clone(),
            o.auto_commit_sends && o.block_on_non_durable_send,
            o.auto_commit_sends && o.block_on_durable_send,
        ));
        inner.state().producers.insert(id, shared.clone());
        // ActiveMQSessionContext.createProducer
        if !inner.conn().context().is_before_producer_metrics_changed() {
            inner
                .send_async(CreateProducerMessage { id, address })
                .await?;
        }
        Ok(Producer::new(inner.clone(), shared))
    }

    /// Creates a consumer with the session's consumer window size.
    pub async fn create_consumer(
        &self,
        queue: &str,
        filter: Option<&str>,
        browse_only: bool,
    ) -> Result<Consumer> {
        self.create_consumer_with(
            queue,
            ConsumerOptions {
                filter: filter.map(Into::into),
                browse_only,
                ..ConsumerOptions::default()
            },
        )
        .await
    }

    /// `ActiveMQSessionContext.createConsumer` +
    /// `ClientSessionImpl.internalCreateConsumer`.
    pub async fn create_consumer_with(
        &self,
        queue: &str,
        options: ConsumerOptions,
    ) -> Result<Consumer> {
        let inner = &self.inner;
        inner.check_closed()?;
        let window_size = options
            .window_size
            .unwrap_or(inner.options.consumer_window_size);
        let id = inner.next_consumer_id.fetch_add(1, Ordering::SeqCst);
        let filter: Option<SimpleString> = options.filter.as_deref().map(Into::into);
        let response = inner
            .send_blocking(
                SessionCreateConsumerMessage {
                    id,
                    queue_name: queue.into(),
                    filter_string: filter.clone(),
                    browse_only: options.browse_only,
                    requires_response: true,
                    priority: options.priority,
                },
                self.queue_query_response_type(),
            )
            .await?;
        let queue_info = QueueQuery::from_packet(response)?;
        // The window actually used: the address-settings default when the
        // caller asked for the client default.
        let initial_window = if window_size == defaults::CONSUMER_WINDOW_SIZE {
            queue_info
                .default_consumer_window_size
                .unwrap_or(defaults::CONSUMER_WINDOW_SIZE)
        } else {
            window_size
        };
        let client_window_size = calc_window_size(initial_window)?;
        let shared = Arc::new(ConsumerShared::new(
            id,
            queue.into(),
            filter,
            options.browse_only,
            options.priority,
            initial_window,
            client_window_size,
            inner.options.ack_batch_size,
            queue_info,
        ));
        inner.state().consumers.insert(id, shared.clone());
        // Credits start the consumer, even for window -1.
        if client_window_size != 0 {
            inner.send_consumer_credits(id, initial_window);
        }
        Ok(Consumer::new(inner.clone(), shared))
    }

    /// `ClientSessionImpl.commit`: flushes pending acks, then
    /// `SessionCommitMessage_V2` (blocking). After a failover with work in
    /// the transaction, it rolls back instead and fails with
    /// `TRANSACTION_ROLLED_BACK`; when the failover interrupted the commit
    /// itself, with `TRANSACTION_OUTCOME_UNKNOWN`.
    pub async fn commit(&self) -> Result<()> {
        let inner = &self.inner;
        inner.check_closed()?;
        if inner.state().rollback_only {
            return self.rollback_on_failover(true).await;
        }
        inner.flush_acks().await?;
        if inner.state().rollback_only {
            return self.rollback_on_failover(true).await;
        }
        let packet: Packet = if inner.conn().context().is_version_support_commit_v2() {
            SessionCommitMessageV2::default().into()
        } else {
            SessionCommitMessage::default().into()
        };
        if let Err(e) = inner.send_blocking(packet, types::NULL_RESPONSE).await {
            let interrupted = matches!(
                e.kind(),
                Some(ErrorType::Unblocked | ErrorType::ConnectionTimedOut)
            );
            if interrupted || inner.state().rollback_only {
                return self.rollback_on_failover(false).await;
            }
            return Err(e);
        }
        if inner.state().rollback_only {
            return self.rollback_on_failover(false).await;
        }
        inner.state().work_done = false;
        Ok(())
    }

    /// `ClientSessionImpl.rollbackOnFailover`.
    async fn rollback_on_failover(&self, outcome_known: bool) -> Result<()> {
        self.rollback(false).await?;
        Err(if outcome_known {
            Error::tx_rolled_back()
        } else {
            Error::tx_outcome_unknown()
        })
    }

    /// `ClientSessionImpl.rollback`: stops the session, returns the credits
    /// of buffered messages and drops them, flushes acks, sends
    /// `RollbackMessage` (blocking) and restarts the session.
    pub async fn rollback(&self, consider_last_message_as_delivered: bool) -> Result<()> {
        let inner = &self.inner;
        inner.check_closed()?;
        let was_started = inner.state().started;
        if was_started {
            inner.stop().await?;
        }
        for c in inner.consumers() {
            c.clear(inner);
        }
        let result = async {
            inner.flush_acks().await?;
            inner
                .send_blocking(
                    RollbackMessage {
                        consider_last_message_as_delivered,
                    },
                    types::NULL_RESPONSE,
                )
                .await
        }
        .await;
        if was_started {
            inner.start().await?;
        }
        result?;
        inner.state().rollback_only = false;
        Ok(())
    }
}

/// `ActiveMQSessionContext.calcWindowSize`: the client keeps half the window.
pub(crate) fn calc_window_size(window_size: i32) -> Result<i32> {
    match window_size {
        -1 => Ok(-1),
        0 => Ok(0),
        1 => Ok(1),
        w if w > 1 => Ok(w >> 1),
        w => Err(Error::InvalidArgument(format!(
            "AMQ219035: Invalid window size {w}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_sizes() {
        assert_eq!(calc_window_size(-1).unwrap(), -1);
        assert_eq!(calc_window_size(0).unwrap(), 0);
        assert_eq!(calc_window_size(1).unwrap(), 1);
        assert_eq!(calc_window_size(1024 * 1024).unwrap(), 512 * 1024);
        assert!(calc_window_size(-2).is_err());
    }
}
