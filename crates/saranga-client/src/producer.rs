//! Producers and producer flow control. Ported from `ClientProducerImpl`,
//! `AbstractProducerCreditsImpl`, `ClientProducerCreditsImpl` and
//! `ClientProducerCreditManagerImpl`.
//!
//! Credits are per address and shared by the session's producers. Each
//! address keeps half the producer window (`windowSize / 2`, 32 KiB by
//! default). On creation it asks for that much; before every send it asks
//! for `max(messageSize, window) - arriving` more whenever
//! `balance + arriving <= max(messageSize, window)`, then waits until the
//! balance covers the message's encode size.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_core_protocol::packets::session::{
    RemoveProducerMessage, SessionRequestProducerCreditsMessage,
};
use saranga_message::CoreMessage;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::large;
use crate::message::{self, HDR_LARGE_BODY_SIZE, HDR_LARGE_COMPRESSED, MessageExt};
use crate::session::SessionInner;

/// `ClientProducerCreditManagerImpl.MAX_ANONYMOUS_CREDITS_CACHE_SIZE`.
const MAX_ANONYMOUS_CREDITS_CACHE_SIZE: usize = 1000;

#[derive(Default)]
struct CreditState {
    /// The semaphore's available permits.
    balance: i64,
    pending_credits: i32,
    arriving: i32,
    closed: bool,
    ref_count: i32,
    server_responded_with_fail: bool,
}

/// `ClientProducerCreditsImpl`: the credits of one address.
pub(crate) struct ProducerCredits {
    address: SimpleString,
    window_size: i32,
    state: Mutex<CreditState>,
    notify: Notify,
}

impl ProducerCredits {
    fn new(address: SimpleString, window_size: i32) -> Self {
        Self {
            address,
            window_size: window_size / 2,
            state: Mutex::new(CreditState::default()),
            notify: Notify::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, CreditState> {
        self.state.lock().expect("credits")
    }

    /// `init`: the first request is for the whole (halved) window.
    fn init(&self, session: &SessionInner) {
        self.check_credits(session, self.window_size);
    }

    /// `AbstractProducerCreditsImpl.checkCredits`.
    fn check_credits(&self, session: &SessionInner, credits: i32) {
        let needed = credits.max(self.window_size);
        let to_request = {
            let st = self.state();
            if st.balance + st.arriving as i64 <= needed as i64 {
                needed - st.arriving
            } else {
                -1
            }
        };
        if to_request > 0 {
            self.request_credits(session, to_request);
        }
    }

    fn request_credits(&self, session: &SessionInner, credits: i32) {
        {
            let mut st = self.state();
            st.pending_credits += credits;
            st.arriving += credits;
        }
        let _ = session.send(SessionRequestProducerCreditsMessage {
            credits,
            address: self.address.clone(),
        });
    }

    /// `acquireCredits`: may wait for the broker to grant credits.
    pub(crate) async fn acquire(&self, session: &SessionInner, credits: i32) -> Result<()> {
        self.check_credits(session, credits);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut st = self.state();
                if st.balance >= credits as i64 {
                    st.balance -= credits as i64;
                    break;
                }
                // A closed producer's credits release every waiter.
                if st.closed {
                    break;
                }
            }
            // ClientProducerCreditsImpl.actualAcquire: a word every 10 s.
            while tokio::time::timeout(Duration::from_secs(10), notified.as_mut())
                .await
                .is_err()
            {
                tracing::warn!(
                    "AMQ212054: Destination address={} is blocked. If the system is configured to block make sure you consume messages on this configuration.",
                    self.address.to_string_lossy()
                );
            }
        }
        // ClientProducerCreditsImpl.afterAcquired
        let mut st = self.state();
        st.pending_credits -= credits;
        if st.server_responded_with_fail {
            st.server_responded_with_fail = false;
            // Drop what is left so the next send asks the broker again.
            st.balance = 0;
            st.pending_credits = 0;
            st.arriving = 0;
            return Err(Error::address_is_full(
                &self.address.to_string_lossy(),
                credits,
            ));
        }
        Ok(())
    }

    fn receive_credits(&self, credits: i32) {
        {
            let mut st = self.state();
            st.arriving -= credits;
            st.balance += credits as i64;
        }
        self.notify.notify_waiters();
    }

    fn receive_fail_credits(&self, credits: i32) {
        self.state().server_responded_with_fail = true;
        self.receive_credits(credits);
    }

    fn close(&self) {
        self.state().closed = true;
        self.notify.notify_waiters();
    }

    fn arriving(&self) -> i32 {
        self.state().arriving
    }
}

/// `ClientProducerCreditManagerImpl`.
pub(crate) struct CreditManager {
    window_size: i32,
    credits: HashMap<SimpleString, Arc<ProducerCredits>>,
    /// Addresses only anonymous producers use, oldest first.
    anonymous: Vec<SimpleString>,
}

impl CreditManager {
    pub(crate) fn new(window_size: i32) -> Self {
        Self {
            window_size,
            credits: HashMap::new(),
            anonymous: Vec::new(),
        }
    }

    /// `getCredits`. `None` when flow control is off (window -1).
    pub(crate) fn get_credits(
        &mut self,
        session: &SessionInner,
        address: &SimpleString,
        anon: bool,
    ) -> Option<Arc<ProducerCredits>> {
        if self.window_size == -1 {
            return None;
        }
        let (credits, need_init) = match self.credits.get(address) {
            Some(c) => (c.clone(), false),
            None => {
                let c = Arc::new(ProducerCredits::new(address.clone(), self.window_size));
                self.credits.insert(address.clone(), c.clone());
                (c, true)
            }
        };
        if anon {
            self.add_to_anonymous(address);
        } else {
            credits.state().ref_count += 1;
            self.anonymous.retain(|a| a != address);
        }
        if need_init {
            credits.init(session);
        }
        Some(credits)
    }

    /// `returnCredits`: the last named producer of an address is gone.
    pub(crate) fn return_credits(&mut self, address: &SimpleString) {
        let Some(c) = self.credits.get(address).cloned() else {
            return;
        };
        let remaining = {
            let mut st = c.state();
            st.ref_count -= 1;
            st.ref_count
        };
        if remaining == 0 {
            self.add_to_anonymous(address);
        }
    }

    fn add_to_anonymous(&mut self, address: &SimpleString) {
        if !self.anonymous.contains(address) {
            self.anonymous.push(address.clone());
        }
        if self.anonymous.len() > MAX_ANONYMOUS_CREDITS_CACHE_SIZE {
            let mut i = 0;
            while i < self.anonymous.len()
                && self.anonymous.len() > MAX_ANONYMOUS_CREDITS_CACHE_SIZE
            {
                let a = &self.anonymous[i];
                match self.credits.get(a) {
                    Some(c) if c.arriving() > 0 => i += 1,
                    _ => {
                        let a = self.anonymous.remove(i);
                        if let Some(c) = self.credits.remove(&a) {
                            c.close();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn receive_credits(&self, address: &SimpleString, credits: i32) {
        if let Some(c) = self.credits.get(address) {
            c.receive_credits(credits);
        }
    }

    pub(crate) fn receive_fail_credits(&self, address: &SimpleString, credits: i32) {
        if let Some(c) = self.credits.get(address) {
            c.receive_fail_credits(credits);
        }
    }

    pub(crate) fn close(&mut self) {
        self.window_size = -1;
        for c in self.credits.values() {
            c.close();
        }
        self.credits.clear();
        self.anonymous.clear();
    }
}

pub(crate) struct ProducerShared {
    id: i32,
    address: Option<SimpleString>,
    block_on_non_durable_send: bool,
    block_on_durable_send: bool,
    closed: AtomicBool,
}

impl ProducerShared {
    pub(crate) fn new(
        id: i32,
        address: Option<SimpleString>,
        block_on_non_durable_send: bool,
        block_on_durable_send: bool,
    ) -> Self {
        Self {
            id,
            address,
            block_on_non_durable_send,
            block_on_durable_send,
            closed: AtomicBool::new(false),
        }
    }

    /// `ClientProducerImpl.doCleanup`.
    pub(crate) fn close(&self, session: &SessionInner) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(a) = &self.address {
            session.credits.lock().expect("credits").return_credits(a);
        }
        session.remove_producer(self.id);
        if !session.conn.context().is_before_producer_metrics_changed() {
            let _ = session.send(RemoveProducerMessage { id: self.id });
        }
    }
}

/// A Core producer (`ClientProducer`).
pub struct Producer {
    session: Arc<SessionInner>,
    shared: Arc<ProducerShared>,
}

impl Producer {
    pub(crate) fn new(session: Arc<SessionInner>, shared: Arc<ProducerShared>) -> Self {
        Self { session, shared }
    }

    /// The producer ID (the `senderID` of each send).
    pub fn id(&self) -> i32 {
        self.shared.id
    }

    pub fn address(&self) -> Option<String> {
        self.shared
            .address
            .as_ref()
            .map(SimpleString::to_string_lossy)
    }

    /// Whether sends of durable messages wait for the broker: the session's
    /// `block_on_durable_send`, but only when sends auto-commit.
    pub fn is_block_on_durable_send(&self) -> bool {
        self.shared.block_on_durable_send
    }

    pub fn is_block_on_non_durable_send(&self) -> bool {
        self.shared.block_on_non_durable_send
    }

    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::SeqCst)
    }

    /// Sends to the producer's address.
    ///
    /// The send blocks (waits for the broker's response) when the message
    /// is durable and the producer blocks on durable sends, or non-durable
    /// and it blocks on non-durable sends. It first waits for producer
    /// credits for the message's encode size.
    ///
    /// A message whose body exceeds the session's `min_large_message_size`
    /// is sent as a large message: its headers and properties, then its
    /// body in chunks of `min_large_message_size` bytes, compressed first
    /// when the session compresses large messages.
    pub async fn send(&self, message: CoreMessage) -> Result<()> {
        self.do_send(None, message).await
    }

    /// Sends to `address` (for producers created without one, or to
    /// override it).
    pub async fn send_to(&self, address: &str, message: CoreMessage) -> Result<()> {
        self.do_send(Some(address.into()), message).await
    }

    /// `ClientProducerImpl.doSend`.
    async fn do_send(&self, address: Option<SimpleString>, mut message: CoreMessage) -> Result<()> {
        if self.is_closed() {
            return Err(Error::producer_closed());
        }
        let session = &self.session;
        session.check_closed()?;
        let address = address
            .or_else(|| self.shared.address.clone())
            .ok_or_else(|| Error::InvalidArgument("no address to send to".into()))?;
        // The body buffer's writer index includes its 4-byte offset.
        let is_large =
            4 + message.body.len() as i64 > session.options.min_large_message_size as i64;
        message.address = Some(address.clone());
        // Encoded as Java's client encodes them: in its HashMap's order.
        message.properties.sort_as_java_hash_map();
        let credits = session
            .credits
            .lock()
            .expect("credits")
            .get_credits(session, &address, true);
        let send_blocking = if message.durable {
            self.shared.block_on_durable_send
        } else {
            self.shared.block_on_non_durable_send
        };
        if is_large {
            self.large_message_send(message, send_blocking, credits.as_deref())
                .await
        } else {
            self.send_regular_message(message, send_blocking, credits.as_deref())
                .await
        }
    }

    /// `ClientProducerImpl.sendRegularMessage`.
    async fn send_regular_message(
        &self,
        message: CoreMessage,
        send_blocking: bool,
        credits: Option<&ProducerCredits>,
    ) -> Result<()> {
        let session = &self.session;
        // getCreditsOnSendingFull: the message's encode size.
        let mut w = Writer::new();
        message.encode(&mut w);
        if let Some(credits) = credits {
            credits.acquire(session, w.len() as i32).await?;
        }
        session
            .send_full_message(message, send_blocking, self.shared.id)
            .await
    }

    /// `ClientProducerImpl.largeMessageSend`, `largeMessageSendBuffered` and
    /// `largeMessageSendStreamed`: the header, then the body in chunks of
    /// `min_large_message_size`, each sent and then paid for in credits. As
    /// Java reads the body until end of stream, a body that is a multiple
    /// of the chunk size ends with an empty last chunk.
    async fn large_message_send(
        &self,
        mut message: CoreMessage,
        send_blocking: bool,
        credits: Option<&ProducerCredits>,
    ) -> Result<()> {
        let session = &self.session;
        let options = &session.options;
        let header_size = message::headers_and_properties_encode_size(&message);
        if header_size >= options.min_large_message_size {
            return Err(Error::header_size_too_big(header_size));
        }
        let chunk_size = options.min_large_message_size as usize;
        let mut body = std::mem::take(&mut message.body);
        // The size the broker records: the bytes read from the body, before
        // any compression.
        let message_body_size = body.len() as i64;
        if options.compress_large_messages {
            message.put_property(HDR_LARGE_COMPRESSED, PropertyValue::Bool(true));
            body = large::deflate(&body, options.compression_level)?;
            // Compressed below the minimum, it is sent as a regular message
            // carrying its real size.
            if body.len() < chunk_size {
                message.put_property(HDR_LARGE_BODY_SIZE, PropertyValue::Long(message_body_size));
                message.body = body;
                return self
                    .send_regular_message(message, send_blocking, credits)
                    .await;
            }
        }
        // sendInitialLargeMessageHeader: the credits are taken after the send.
        let used = session.send_initial_chunk_on_large_message(message)?;
        if let Some(credits) = credits {
            credits.acquire(session, used).await?;
        }
        let mut chunks = body.chunks_exact(chunk_size);
        for chunk in &mut chunks {
            let used = session
                .send_large_message_chunk(
                    message_body_size,
                    send_blocking,
                    false,
                    chunk.to_vec(),
                    self.shared.id,
                )
                .await?;
            if let Some(credits) = credits {
                credits.acquire(session, used).await?;
            }
        }
        let used = session
            .send_large_message_chunk(
                message_body_size,
                send_blocking,
                true,
                chunks.remainder().to_vec(),
                self.shared.id,
            )
            .await?;
        if let Some(credits) = credits {
            credits.acquire(session, used).await?;
        }
        Ok(())
    }

    /// Closes the producer (`RemoveProducerMessage`, not blocking).
    pub fn close(&self) {
        self.shared.close(&self.session);
    }
}
