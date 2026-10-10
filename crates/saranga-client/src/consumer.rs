//! Consumers. Ported from `ClientConsumerImpl`.
//!
//! Flow control: the session sends the initial window as credits right
//! after creating the consumer (even -1, to start it). The client window is
//! half of that (`calcWindowSize`). Each message's flow-control size is the
//! size of the `SESS_RECEIVE_MSG` frame that carried it, length prefix
//! included (`PacketImpl.getPacketSize` after decode). When a message is
//! taken by `receive`, its size is added to `creditsToSend`, and once that
//! reaches the client window the whole amount is sent back. With window 0
//! (slow consumer) each `receive` first sends 1 credit, and the credits
//! returned for the message are one less to make up for it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_core_protocol::FRAME_HEADER_SIZE;
use saranga_core_protocol::packets::session::{
    SessionConsumerCloseMessage, SessionForceConsumerDelivery,
};
use saranga_core_protocol::types;
use saranga_message::CoreMessage;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::defaults;
use crate::error::{Error, Result};
use crate::message::MessageExt;
use crate::session::{QueueQuery, SessionInner};

/// `ClientConsumerImpl.NUM_PRIORITIES`.
const NUM_PRIORITIES: usize = 10;

/// `ClientConsumerImpl.FORCED_DELIVERY_MESSAGE`: the property on the marker
/// message the broker sends back for `SessionForceConsumerDelivery`.
pub const FORCED_DELIVERY_MESSAGE: &str = "_hornetq.forced.delivery.seq";

/// Per-consumer settings for [`crate::Session::create_consumer_with`].
#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    pub filter: Option<String>,
    pub browse_only: bool,
    /// The consumer window. `None` uses the session's; the client default
    /// (1 MiB) is replaced by the queue's address-settings default.
    pub window_size: Option<i32>,
    pub priority: i32,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            filter: None,
            browse_only: false,
            window_size: None,
            priority: defaults::CONSUMER_PRIORITY,
        }
    }
}

/// A received message.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub message: CoreMessage,
    pub delivery_count: i32,
    flow_control_size: i32,
    encode_size: i32,
    forced_delivery_seq: Option<i64>,
}

impl Delivery {
    pub fn message_id(&self) -> i64 {
        self.message.message_id
    }

    /// The size counted against the consumer window: the whole frame.
    pub fn flow_control_size(&self) -> i32 {
        self.flow_control_size
    }

    /// `Message.getEncodeSize()`, counted against the ack batch size.
    pub fn encode_size(&self) -> i32 {
        self.encode_size
    }
}

struct ConsumerState {
    buffer: [VecDeque<Delivery>; NUM_PRIORITIES],
    closing: bool,
    closed: bool,
    stopped: bool,
    failure: Option<Error>,
    credits_to_send: i32,
    ack_bytes: i32,
    last_acked: Option<i64>,
    ack_individually: bool,
    force_delivery_count: i64,
}

impl ConsumerState {
    /// `PriorityLinkedListImpl.poll`: highest priority first.
    fn poll(&mut self) -> Option<Delivery> {
        self.buffer.iter_mut().rev().find_map(VecDeque::pop_front)
    }

    fn len(&self) -> usize {
        self.buffer.iter().map(VecDeque::len).sum()
    }
}

pub(crate) struct ConsumerShared {
    pub(crate) id: i64,
    queue_name: SimpleString,
    filter: Option<SimpleString>,
    browse_only: bool,
    initial_window: i32,
    client_window_size: i32,
    ack_batch_size: i32,
    queue_info: QueueQuery,
    state: Mutex<ConsumerState>,
    notify: Notify,
}

impl ConsumerShared {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: i64,
        queue_name: SimpleString,
        filter: Option<SimpleString>,
        browse_only: bool,
        initial_window: i32,
        client_window_size: i32,
        ack_batch_size: i32,
        queue_info: QueueQuery,
    ) -> Self {
        Self {
            id,
            queue_name,
            filter,
            browse_only,
            initial_window,
            client_window_size,
            ack_batch_size,
            queue_info,
            state: Mutex::new(ConsumerState {
                buffer: Default::default(),
                closing: false,
                closed: false,
                stopped: false,
                failure: None,
                credits_to_send: 0,
                ack_bytes: 0,
                last_acked: None,
                ack_individually: false,
                force_delivery_count: 0,
            }),
            notify: Notify::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, ConsumerState> {
        self.state.lock().expect("consumer")
    }

    /// `ClientConsumerImpl.handleRegularMessage`.
    pub(crate) fn handle_message(&self, mut message: CoreMessage, delivery_count: i32, size: i32) {
        let mut st = self.state();
        if st.closing {
            return;
        }
        if message.address.is_none() {
            message.address = self.queue_info.address.as_deref().map(Into::into);
        }
        let forced_delivery_seq = match message.property(FORCED_DELIVERY_MESSAGE) {
            Some(PropertyValue::Long(seq)) => Some(*seq),
            _ => None,
        };
        // Messages of different priorities are consumed out of order, so
        // acks can no longer be cumulative.
        if !st.ack_individually && message.priority != 4 && forced_delivery_seq.is_none() {
            st.ack_individually = true;
        }
        let priority = (message.priority as usize).min(NUM_PRIORITIES - 1);
        // The message buffer is the frame minus its header and the trailing
        // consumer ID and delivery count (`copyMessageBuffer`).
        let encode_size = size - FRAME_HEADER_SIZE as i32 - 12;
        st.buffer[priority].push_back(Delivery {
            message,
            delivery_count,
            flow_control_size: size,
            encode_size,
            forced_delivery_seq,
        });
        drop(st);
        self.notify.notify_waiters();
    }

    /// A large message (not supported): returns its credits and drops it.
    pub(crate) fn discard_unsupported(&self, session: &SessionInner, size: i32) {
        let mut st = self.state();
        self.flow_control(&mut st, session, size, false);
    }

    /// `ClientConsumerImpl.flowControl`.
    fn flow_control(
        &self,
        st: &mut ConsumerState,
        session: &SessionInner,
        message_bytes: i32,
        discount_slow_consumer: bool,
    ) {
        if self.client_window_size < 0 {
            return;
        }
        st.credits_to_send += message_bytes;
        if st.credits_to_send >= self.client_window_size {
            let credits = if self.client_window_size == 0 && discount_slow_consumer {
                // The credit sent up front to fire the slow consumer.
                st.credits_to_send - 1
            } else {
                st.credits_to_send
            };
            st.credits_to_send = 0;
            if credits > 0 {
                session.send_consumer_credits(self.id, credits);
            }
        }
    }

    fn flow_control_before_consumption(
        &self,
        st: &mut ConsumerState,
        session: &SessionInner,
        d: &Delivery,
    ) {
        if d.flow_control_size != 0 {
            self.flow_control(st, session, d.flow_control_size, true);
        }
    }

    /// `startSlowConsumer`: one credit to get one message.
    fn start_slow_consumer(&self, session: &SessionInner) {
        session.send_consumer_credits(self.id, 1);
    }

    /// `resetIfSlowConsumer`.
    fn reset_if_slow_consumer(&self, session: &SessionInner) {
        if self.client_window_size == 0 {
            session.send_consumer_credits(self.id, 0);
        }
    }

    fn check_closed(&self) -> Result<()> {
        let st = self.state();
        if let Some(e) = &st.failure {
            return Err(e.clone());
        }
        if st.closed {
            return Err(Error::consumer_closed());
        }
        Ok(())
    }

    /// `ClientConsumerImpl.receive(timeout, forcingDelivery)`. `None` waits
    /// without limit (Java's `Long.MAX_VALUE`).
    async fn receive(
        &self,
        session: &SessionInner,
        timeout: Option<Duration>,
        forcing_delivery: bool,
    ) -> Result<Option<Delivery>> {
        self.check_closed()?;
        if self.client_window_size == 0 {
            self.start_slow_consumer(session);
        }
        let deadline = timeout.map(|t| Instant::now() + t);
        let mut delivery_forced = false;
        loop {
            let mut call_force_delivery = false;
            let m = loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let mut st = self.state();
                    if let Some(e) = &st.failure {
                        return Err(e.clone());
                    }
                    if st.closed {
                        break None;
                    }
                    if !st.stopped
                        && let Some(m) = st.poll()
                    {
                        break Some(m);
                    }
                    if forcing_delivery {
                        if st.stopped {
                            break None;
                        }
                        // Force delivery only once per call.
                        if !delivery_forced {
                            call_force_delivery = true;
                            break None;
                        }
                    }
                }
                match deadline {
                    Some(d) if Instant::now() >= d => break None,
                    Some(d) => {
                        let _ = tokio::time::timeout_at(d, notified).await;
                    }
                    None => notified.await,
                }
            };

            if call_force_delivery {
                let sequence = {
                    let mut st = self.state();
                    st.force_delivery_count += 1;
                    st.force_delivery_count - 1
                };
                session.send(SessionForceConsumerDelivery {
                    consumer_id: self.id,
                    sequence,
                })?;
                delivery_forced = true;
                continue;
            }

            let Some(m) = m else {
                self.reset_if_slow_consumer(session);
                return Ok(None);
            };

            if let Some(seq) = m.forced_delivery_seq {
                // Only the answer to this call's forced delivery means "empty".
                let count = self.state().force_delivery_count;
                if forcing_delivery && delivery_forced && seq == count - 1 {
                    self.reset_if_slow_consumer(session);
                    return Ok(None);
                }
                continue;
            }

            let expired = m.message.is_expired();
            self.flow_control_before_consumption(&mut self.state(), session, &m);
            if expired {
                session.expire(self.id, m.message.message_id)?;
                if self.client_window_size == 0 {
                    self.start_slow_consumer(session);
                }
                if deadline.is_none_or(|d| Instant::now() < d) {
                    continue;
                }
                return Ok(None);
            }
            return Ok(Some(m));
        }
    }

    /// `ClientConsumerImpl.acknowledge`: cumulative acks are batched up to
    /// the ack batch size, unless acks have become individual.
    async fn acknowledge(&self, session: &SessionInner, d: &Delivery) -> Result<()> {
        enum Action {
            Individual,
            Ack(i64),
            Deferred,
        }
        let action = {
            let mut st = self.state();
            if st.ack_individually {
                Action::Individual
            } else {
                st.ack_bytes += d.encode_size;
                if st.ack_bytes >= self.ack_batch_size {
                    st.ack_bytes = 0;
                    st.last_acked = None;
                    Action::Ack(d.message_id())
                } else {
                    st.last_acked = Some(d.message_id());
                    Action::Deferred
                }
            }
        };
        match action {
            Action::Individual => self.individual_acknowledge(session, d).await,
            Action::Ack(id) => session.acknowledge(self.id, id).await,
            Action::Deferred => Ok(()),
        }
    }

    async fn individual_acknowledge(&self, session: &SessionInner, d: &Delivery) -> Result<()> {
        self.flush_acks(session).await?;
        session
            .individual_acknowledge(self.id, d.message_id())
            .await
    }

    /// `ClientConsumerImpl.flushAcks`.
    pub(crate) async fn flush_acks(&self, session: &SessionInner) -> Result<()> {
        let last = {
            let mut st = self.state();
            let last = st.last_acked.take();
            if last.is_some() {
                // doAck
                st.ack_bytes = 0;
            }
            last
        };
        match last {
            Some(id) => session.acknowledge(self.id, id).await,
            None => Ok(()),
        }
    }

    pub(crate) fn start(&self) {
        self.state().stopped = false;
        self.notify.notify_waiters();
    }

    pub(crate) fn stop(&self) {
        // stop() does not affect browsers.
        if !self.browse_only {
            self.state().stopped = true;
        }
    }

    /// `ClientConsumerImpl.clear`: credits go back for every buffered
    /// message, then the buffer is dropped.
    pub(crate) fn clear(&self, session: &SessionInner) {
        let mut st = self.state();
        let buffered: Vec<Delivery> = st.buffer.iter_mut().flat_map(|q| q.drain(..)).collect();
        for d in &buffered {
            self.flow_control_before_consumption(&mut st, session, d);
        }
    }

    /// `ClientConsumerImpl.doCleanUp(true)`: errors are swallowed.
    pub(crate) async fn close(&self, session: &SessionInner) {
        {
            let mut st = self.state();
            if st.closed {
                return;
            }
            st.closing = true;
            st.closed = true;
        }
        self.notify.notify_waiters();
        let _ = self.flush_acks(session).await;
        for q in self.state().buffer.iter_mut() {
            q.clear();
        }
        let _ = session
            .send_blocking(
                SessionConsumerCloseMessage {
                    consumer_id: self.id,
                },
                types::NULL_RESPONSE,
            )
            .await;
        session.remove_consumer(self.id);
    }

    pub(crate) fn fail(&self, err: &Error) {
        {
            let mut st = self.state();
            st.failure = Some(err.clone());
            st.closed = true;
        }
        self.notify.notify_waiters();
    }
}

/// A Core consumer (`ClientConsumer`).
pub struct Consumer {
    session: Arc<SessionInner>,
    shared: Arc<ConsumerShared>,
}

impl Consumer {
    pub(crate) fn new(session: Arc<SessionInner>, shared: Arc<ConsumerShared>) -> Self {
        Self { session, shared }
    }

    pub fn id(&self) -> i64 {
        self.shared.id
    }

    pub fn queue_name(&self) -> String {
        self.shared.queue_name.to_string_lossy()
    }

    pub fn filter(&self) -> Option<String> {
        self.shared
            .filter
            .as_ref()
            .map(SimpleString::to_string_lossy)
    }

    pub fn is_browse_only(&self) -> bool {
        self.shared.browse_only
    }

    /// The queue as reported when the consumer was created.
    pub fn queue_info(&self) -> &QueueQuery {
        &self.shared.queue_info
    }

    /// The window sent as initial credits (`getInitialWindowSize`).
    pub fn initial_window_size(&self) -> i32 {
        self.shared.initial_window
    }

    /// The credit threshold (`getClientWindowSize`).
    pub fn client_window_size(&self) -> i32 {
        self.shared.client_window_size
    }

    /// Messages buffered and not yet received.
    pub fn buffered(&self) -> usize {
        self.shared.state().len()
    }

    pub fn is_closed(&self) -> bool {
        self.shared.state().closed
    }

    /// `ClientConsumer.receive(timeout)`: waits up to `timeout` (forever for
    /// `Duration::ZERO`, as Java's 0); if nothing came, forces a delivery to
    /// find out whether the queue is really empty, as Java does.
    pub async fn receive(&self, timeout: Duration) -> Result<Option<Delivery>> {
        let timeout = (!timeout.is_zero()).then_some(timeout);
        let m = self.shared.receive(&self.session, timeout, false).await?;
        if m.is_none() && !self.shared.state().closed {
            return self.shared.receive(&self.session, None, true).await;
        }
        Ok(m)
    }

    /// `ClientConsumer.receiveImmediate`: a buffered message, or else a
    /// forced delivery round trip.
    pub async fn receive_immediate(&self) -> Result<Option<Delivery>> {
        self.shared.receive(&self.session, None, true).await
    }

    /// `ClientMessage.acknowledge`: a cumulative ack (`SessionAcknowledgeMessage`),
    /// batched by the session's ack batch size.
    pub async fn acknowledge(&self, delivery: &Delivery) -> Result<()> {
        self.shared.acknowledge(&self.session, delivery).await
    }

    /// `ClientMessage.individualAcknowledge` (`SessionIndividualAcknowledgeMessage`).
    pub async fn individual_acknowledge(&self, delivery: &Delivery) -> Result<()> {
        self.shared
            .individual_acknowledge(&self.session, delivery)
            .await
    }

    /// Sends a batched cumulative ack now.
    pub async fn flush_acks(&self) -> Result<()> {
        self.shared.flush_acks(&self.session).await
    }

    /// Flushes acks and closes the consumer (`SessionConsumerCloseMessage`, blocking).
    pub async fn close(&self) {
        self.shared.close(&self.session).await
    }
}
