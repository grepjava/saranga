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
//!
//! A large message counts its header packet when `receive` takes it (never
//! discounted, as its chunks follow), then each chunk as its body is read
//! (see [`crate::large`]).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
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
use crate::large::{self, LargeMessageController};
use crate::message::{self, HDR_LARGE_BODY_SIZE, HDR_LARGE_COMPRESSED, MessageExt};
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
    /// The body of a large message, until `receive` has read it.
    large: Option<Arc<LargeMessageController>>,
    /// It came as a large message (`ClientLargeMessageInternal`).
    was_large: bool,
}

impl Delivery {
    /// Whether it came as a large message, its body streamed in chunks.
    pub fn is_large(&self) -> bool {
        self.was_large
    }

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
    /// The large message whose chunks are arriving
    /// (`currentLargeMessageController`).
    current_large: Option<Arc<LargeMessageController>>,
    /// `failedOver`: the session failed over while `receive` waited.
    failed_over: bool,
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
    priority: i32,
    initial_window: i32,
    client_window_size: i32,
    ack_batch_size: i32,
    queue_info: QueueQuery,
    state: Mutex<ConsumerState>,
    notify: Notify,
    /// `manualFlowManagement`: the owner sends the credits.
    manual_flow: AtomicBool,
}

impl ConsumerShared {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: i64,
        queue_name: SimpleString,
        filter: Option<SimpleString>,
        browse_only: bool,
        priority: i32,
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
            priority,
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
                current_large: None,
                failed_over: false,
            }),
            notify: Notify::new(),
            manual_flow: AtomicBool::new(false),
        }
    }

    fn state(&self) -> MutexGuard<'_, ConsumerState> {
        self.state.lock().expect("consumer")
    }

    /// `ClientConsumerImpl.handleMessage`.
    pub(crate) fn handle_message(&self, mut message: CoreMessage, delivery_count: i32, size: i32) {
        let mut st = self.state();
        if st.closing {
            return;
        }
        // The message buffer is the frame minus its header and the trailing
        // consumer ID and delivery count (`copyMessageBuffer`).
        let mut encode_size = size - FRAME_HEADER_SIZE as i32 - 12;
        // handleCompressedMessageSentAsRegular: a large body that compressed
        // below the minimum. Its credits stay those of a regular message.
        if is_compressed(&message) && inflate_body(&mut message) {
            encode_size = full_encode_size(&message);
        }
        self.handle_regular_message(&mut st, message, delivery_count, size, encode_size, None);
    }

    /// `ClientConsumerImpl.handleLargeMessage`: the headers are queued at
    /// once; the body follows in continuation packets.
    pub(crate) fn handle_large_message(
        &self,
        message: CoreMessage,
        delivery_count: i32,
        size: i32,
    ) {
        let mut st = self.state();
        if st.closing {
            return;
        }
        let controller = Arc::new(LargeMessageController::default());
        st.current_large = Some(controller.clone());
        // ClientLargeMessageImpl.getEncodeSize, until the body is read.
        let encode_size = 4 + 4 + message::headers_and_properties_encode_size(&message);
        self.handle_regular_message(
            &mut st,
            message,
            delivery_count,
            size,
            encode_size,
            Some(controller),
        );
    }

    /// `ClientConsumerImpl.handleLargeMessageContinuation`.
    pub(crate) fn handle_large_message_continuation(
        &self,
        session: &SessionInner,
        chunk: Vec<u8>,
        size: i32,
        continues: bool,
    ) {
        let mut st = self.state();
        if st.closing {
            return;
        }
        let Some(controller) = st.current_large.clone() else {
            // No message to add to: the credits go straight back.
            self.flow_control(&mut st, session, size, false);
            return;
        };
        if !continues {
            st.current_large = None;
        }
        if let Some((credits, discount)) = controller.add_packet(chunk, size, continues) {
            self.flow_control(&mut st, session, credits, discount);
        }
    }

    /// `ClientConsumerImpl.handleRegularMessage`.
    fn handle_regular_message(
        &self,
        st: &mut ConsumerState,
        mut message: CoreMessage,
        delivery_count: i32,
        size: i32,
        encode_size: i32,
        large: Option<Arc<LargeMessageController>>,
    ) {
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
        st.buffer[priority].push_back(Delivery {
            message,
            delivery_count,
            flow_control_size: size,
            encode_size,
            forced_delivery_seq,
            was_large: large.is_some(),
            large,
        });
        self.notify.notify_waiters();
    }

    /// `ClientLargeMessageImpl.checkBuffer` (`saveBuffer`): gives back the
    /// credits of the chunks that came so far, waits for the rest of the
    /// body and inflates it if it was compressed.
    async fn read_large_body(
        &self,
        session: &SessionInner,
        d: &mut Delivery,
        controller: &LargeMessageController,
    ) -> Result<()> {
        if let Some((credits, discount)) = controller.start_reading(false) {
            self.flow_control(&mut self.state(), session, credits, discount);
        }
        d.message.body = controller
            .wait_completion(session.conn().call_timeout)
            .await?
            .into();
        if is_compressed(&d.message) {
            inflate_body(&mut d.message);
        }
        d.encode_size = full_encode_size(&d.message);
        Ok(())
    }

    /// `resetLargeMessageController`: cancels the message whose chunks are
    /// arriving, giving back the credits of those not read.
    fn reset_large_message_controller(&self, st: &mut ConsumerState, session: &SessionInner) {
        if let Some(controller) = st.current_large.take() {
            let credits = controller.cancel();
            self.flow_control(st, session, credits, false);
        }
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
        if self.manual_flow.load(AtomicOrdering::Acquire) {
            return;
        }
        // A large message's chunks follow, so its header is not discounted.
        if d.flow_control_size != 0 {
            self.flow_control(st, session, d.flow_control_size, d.large.is_none());
        }
    }

    /// `startSlowConsumer`: one credit to get one message.
    fn start_slow_consumer(&self, session: &SessionInner) {
        if self.manual_flow.load(AtomicOrdering::Acquire) {
            return;
        }
        session.send_consumer_credits(self.id, 1);
    }

    /// `resetIfSlowConsumer`.
    fn reset_if_slow_consumer(&self, session: &SessionInner) {
        if self.client_window_size == 0 && !self.manual_flow.load(AtomicOrdering::Acquire) {
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
        let mut deadline = timeout.map(|t| Instant::now() + t);
        let mut delivery_forced = false;
        loop {
            let mut call_force_delivery = false;
            let m = loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let mut st = self.state();
                    // Closed or failed while waiting: Artemis's receive
                    // returns null; only the next call fails (checkClosed).
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

            {
                let mut st = self.state();
                if std::mem::take(&mut st.failed_over) && m.is_none() && !st.closed {
                    // Failed over with nothing received: wait again, the
                    // whole timeout.
                    drop(st);
                    delivery_forced = false;
                    deadline = timeout.map(|t| Instant::now() + t);
                    continue;
                }
            }

            let Some(mut m) = m else {
                self.reset_if_slow_consumer(session);
                return Ok(None);
            };
            session.work_done();

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
                // discardBody: the chunks are consumed and dropped.
                if let Some(controller) = m.large.take()
                    && let Some((credits, discount)) = controller.start_reading(true)
                {
                    self.flow_control(&mut self.state(), session, credits, discount);
                }
                session.expire(self.id, m.message.message_id)?;
                if self.client_window_size == 0 {
                    self.start_slow_consumer(session);
                }
                if deadline.is_none_or(|d| Instant::now() < d) {
                    continue;
                }
                return Ok(None);
            }
            if let Some(controller) = m.large.take() {
                self.read_large_body(session, &mut m, &controller).await?;
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
            if let Some(controller) = &d.large {
                let credits = controller.cancel();
                self.flow_control(&mut st, session, credits, false);
            }
            self.flow_control_before_consumption(&mut st, session, d);
        }
        self.reset_large_message_controller(&mut st, session);
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
            self.reset_large_message_controller(&mut st, session);
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

    /// `ClientConsumerImpl.clearAtFailover`: the buffer and the state of
    /// acks and credits are dropped, and the consumer stays stopped until
    /// the session starts it on the new connection.
    pub(crate) fn clear_at_failover(&self) {
        {
            let mut st = self.state();
            for q in st.buffer.iter_mut() {
                q.clear();
            }
            st.stopped = true;
            if let Some(controller) = st.current_large.take() {
                controller.cancel();
            }
            st.last_acked = None;
            st.credits_to_send = 0;
            st.failed_over = true;
            st.ack_individually = false;
        }
        self.notify.notify_waiters();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.state().closed
    }

    pub(crate) fn queue_name(&self) -> &SimpleString {
        &self.queue_name
    }

    pub(crate) fn filter(&self) -> Option<&SimpleString> {
        self.filter.as_ref()
    }

    pub(crate) fn is_browse_only(&self) -> bool {
        self.browse_only
    }

    pub(crate) fn priority(&self) -> i32 {
        self.priority
    }

    pub(crate) fn client_window_size(&self) -> i32 {
        self.client_window_size
    }

    pub(crate) fn queue_info(&self) -> &QueueQuery {
        &self.queue_info
    }

    pub(crate) fn force_delivery_count(&self) -> i64 {
        self.state().force_delivery_count
    }

    pub(crate) fn fail(&self, err: &Error) {
        {
            let mut st = self.state();
            st.failure = Some(err.clone());
            st.closed = true;
            if let Some(controller) = st.current_large.take() {
                controller.cancel();
            }
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

    /// `setManualFlowMessageHandler`: the consumer sends no credits as it
    /// consumes; [`Consumer::flow_control`] sends them. For a consumer of
    /// window 0.
    pub fn set_manual_flow(&self) {
        self.shared.manual_flow.store(true, AtomicOrdering::Release);
    }

    /// `flowControl(credits, false)`: credits for the broker to send more.
    pub fn flow_control(&self, credits: i32) {
        self.session.send_consumer_credits(self.shared.id, credits);
    }

    /// `resetIfSlowConsumer` of a consumer of window 0: the broker stops
    /// sending.
    pub fn reset_if_slow_consumer(&self) {
        if self.shared.client_window_size == 0 {
            self.session.send_consumer_credits(self.shared.id, 0);
        }
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

/// `ClientMessageImpl.isCompressed`: `_AMQ_LARGE_COMPRESSED` is true.
fn is_compressed(message: &CoreMessage) -> bool {
    matches!(
        message.property(HDR_LARGE_COMPRESSED),
        Some(PropertyValue::Bool(true))
    )
}

/// Inflates a compressed body in place and clears `_AMQ_LARGE_COMPRESSED`
/// (`unsetCompressionPropertyIfNeeded`). A body that does not inflate is
/// left as it came, still marked compressed. Returns whether it inflated.
fn inflate_body(message: &mut CoreMessage) -> bool {
    let size_hint = match message.property(HDR_LARGE_BODY_SIZE) {
        Some(PropertyValue::Long(n)) => (*n).clamp(0, i32::MAX as i64) as usize,
        _ => 0,
    };
    match large::inflate(&message.body, size_hint) {
        Ok(body) => {
            message.body = body.into();
            message.put_property(HDR_LARGE_COMPRESSED, PropertyValue::Bool(false));
            true
        }
        Err(_) => false,
    }
}

/// `CoreMessage.getEncodeSize` of a message holding its whole body: the
/// end-of-body position, the body, then headers and properties.
fn full_encode_size(message: &CoreMessage) -> i32 {
    4 + message.body.len() as i32 + message::headers_and_properties_encode_size(message)
}
