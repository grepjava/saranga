//! Qpid JMS's sessions, producers, consumers and local transactions over
//! the AMQP engine (`JmsSession`, `AmqpProducerBuilder`,
//! `AmqpFixedProducer`, `AmqpConsumerBuilder`, `AmqpConsumer`,
//! `AmqpTransactionCoordinator`, `JmsLocalTransactionContext`), for what
//! the CLI's commands do with them.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_protocol_amqp::composite::{Composite, Fields};
use saranga_protocol_amqp::messaging::{
    DeliveryState, Modified, Source, Target, TargetKind, TerminusDurability, TerminusExpiryPolicy,
};
use saranga_protocol_amqp::transaction::{
    Coordinator, Declare, Declared, Discharge, TransactionalState,
};
use saranga_protocol_amqp::transport::{
    Attach, Error as AmqpCondition, ReceiverSettleMode, Role, SenderSettleMode,
};
use saranga_protocol_amqp::types::{Descriptor, Symbol, Value};
use tokio::sync::Mutex as AsyncMutex;

use super::connection::AmqpJmsConnection;
use super::engine::{self, Delivery, Failure, LinkEvent, Scope};
use super::message::AmqpJmsMessage;
use crate::client::jms::{Destination, now_millis};

/// A JMS exception: its class and message, as `toString` prints them.
#[derive(Debug, Clone, PartialEq)]
pub struct JmsError {
    pub class: &'static str,
    pub message: String,
}

impl std::fmt::Display for JmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for JmsError {}

impl JmsError {
    pub fn jms(message: impl Into<String>) -> Self {
        Self {
            class: "javax.jms.JMSException",
            message: message.into(),
        }
    }

    pub fn illegal_state(message: impl Into<String>) -> Self {
        Self {
            class: "javax.jms.IllegalStateException",
            message: message.into(),
        }
    }

    /// `toString`: the class and the message.
    pub fn to_java_string(&self) -> String {
        format!("{}: {}", self.class, self.message)
    }
}

pub type Result<T> = std::result::Result<T, JmsError>;

/// `AmqpSupport.extractErrorMessage`.
pub fn error_message(e: Option<&AmqpCondition>) -> String {
    match e {
        None => "Unknown error from remote peer".to_owned(),
        Some(e) => {
            let base = e
                .description
                .clone()
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| {
                    "Received error from remote peer without description".to_owned()
                });
            format!("{base} [condition = {}]", e.condition)
        }
    }
}

/// `AmqpSupport.convertToNonFatalException`, then the JMS exception the
/// provider exception becomes.
pub fn condition_error(e: Option<&AmqpCondition>) -> JmsError {
    let class = match e.map(|e| e.condition.as_str()) {
        Some("amqp:unauthorized-access") => "javax.jms.JMSSecurityException",
        Some("amqp:resource-limit-exceeded") => "javax.jms.ResourceAllocationException",
        Some("amqp:not-found") => "javax.jms.InvalidDestinationException",
        Some("amqp:transaction:rollback") => "javax.jms.TransactionRolledBackException",
        _ => "javax.jms.JMSException",
    };
    JmsError {
        class,
        message: error_message(e),
    }
}

/// The failure of an engine operation as the JMS layer reports it.
pub fn failure_error(f: &Failure) -> JmsError {
    match f {
        Failure::Remote(_, e) => condition_error(e.as_ref()),
        Failure::Io(m) => JmsError::jms(m.clone()),
        Failure::Closed => JmsError::illegal_state("The Connection is closed"),
    }
}

/// The outcomes every link offers.
fn outcomes() -> Vec<Symbol> {
    [
        "amqp:accepted:list",
        "amqp:rejected:list",
        "amqp:released:list",
        "amqp:modified:list",
    ]
    .into_iter()
    .map(Symbol::from)
    .collect()
}

/// `AmqpDestinationHelper.toTypeCapability`.
fn type_capability(d: &Destination) -> Symbol {
    Symbol::from(if d.is_queue() { "queue" } else { "topic" })
}

/// `AmqpTransferTagGenerator`: tags counted from 0 in their fewest
/// big-endian bytes; a pooled one is taken back once its send succeeded.
#[derive(Default)]
struct Tags {
    next: u64,
    pool: VecDeque<Vec<u8>>,
}

impl Tags {
    fn take(&mut self) -> Vec<u8> {
        if let Some(t) = self.pool.pop_front() {
            return t;
        }
        let n = self.next;
        self.next += 1;
        let bytes = n.to_be_bytes();
        let first = bytes.iter().position(|b| *b != 0).unwrap_or(7);
        bytes[first..].to_vec()
    }

    fn give_back(&mut self, tag: Vec<u8>) {
        if self.pool.len() < 1024 {
            self.pool.push_back(tag);
        }
    }
}

/// A session's local transaction (`JmsLocalTransactionContext` over
/// `AmqpTransactionCoordinator`).
struct Transaction {
    coordinator: engine::Link,
    tags: Tags,
    txn_id: Option<Vec<u8>>,
}

impl Transaction {
    /// Sends a control message (`Declare` or `Discharge`) and waits for the
    /// coordinator's outcome.
    fn send_control(
        &mut self,
        body: Value,
    ) -> tokio::sync::oneshot::Receiver<engine::Result<DeliveryState>> {
        let mut payload = Vec::new();
        saranga_protocol_amqp::codec::encode(
            &Value::described(
                Descriptor::Code(saranga_protocol_amqp::messaging::section::AMQP_VALUE),
                body,
            ),
            &mut payload,
        );
        // The coordinator's tags are never given back.
        let tag = self.tags.take();
        self.coordinator
            .send(tag, payload, false, None)
            .expect("unsettled")
    }

    fn declare(&mut self) -> tokio::sync::oneshot::Receiver<engine::Result<DeliveryState>> {
        self.send_control(Declare { global_id: None }.to_value())
    }
}

/// What `declare` and `discharge` outcomes become (`AmqpTransactionCoordinator`).
fn declared(r: engine::Result<DeliveryState>) -> Result<Vec<u8>> {
    match r {
        Ok(DeliveryState::Declared(Declared { txn_id })) => Ok(txn_id),
        Ok(DeliveryState::Rejected(r)) => Err(in_doubt(r.error.as_ref())),
        Ok(_) => Err(JmsError::jms("Declare outcome was not Declared")),
        Err(f) => Err(failure_error(&f)),
    }
}

fn in_doubt(e: Option<&AmqpCondition>) -> JmsError {
    JmsError {
        class: "org.apache.qpid.jms.JmsTransactionInDoubtException",
        message: error_message(e),
    }
}

/// A JMS session over an AMQP session.
pub struct AmqpJmsSession {
    pub(super) conn: Arc<AmqpJmsConnection>,
    session: engine::Session,
    /// `<connectionId>:<n>`.
    pub id: String,
    pub transacted: bool,
    producers: AtomicU64,
    consumers: AtomicU64,
    tx: Option<AsyncMutex<Transaction>>,
    /// The consumers that acknowledged in the current transaction.
    participants: Mutex<Vec<Arc<ConsumerState>>>,
}

impl AmqpJmsSession {
    /// `createSession`: the begin, and for a transacted session the
    /// coordinator and the first transaction's declare.
    pub(super) async fn begin(
        conn: Arc<AmqpJmsConnection>,
        n: u64,
        transacted: bool,
    ) -> Result<Self> {
        let session = conn.begin_session().await?;
        let id = format!("{}:{n}", conn.id);
        let tx = if transacted {
            let coordinator = session
                .attach(Attach {
                    name: format!("qpid-jms:coordinator:{id}"),
                    role: Role::Sender,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: Some(Source {
                        outcomes: Some(outcomes()),
                        ..Source::default()
                    }),
                    target: Some(TargetKind::Coordinator(Coordinator {
                        capabilities: Some(vec![Symbol::from("amqp:local-transactions")]),
                    })),
                    ..Attach::default()
                })
                .await
                .map_err(|f| match f {
                    Failure::Remote(_, None) => JmsError::jms("Open failed unexpectedly."),
                    f => failure_error(&f),
                })?;
            if coordinator.remote.target.is_none() {
                return Err(wait_refusal(coordinator, "Open failed unexpectedly.").await);
            }
            let mut t = Transaction {
                coordinator,
                tags: Tags::default(),
                txn_id: None,
            };
            let outcome = t.declare();
            t.txn_id = Some(declared(outcome.await.unwrap_or(Err(Failure::Closed)))?);
            Some(AsyncMutex::new(t))
        } else {
            None
        };
        Ok(Self {
            conn,
            session,
            id,
            transacted,
            producers: AtomicU64::new(0),
            consumers: AtomicU64::new(0),
            tx,
            participants: Mutex::new(Vec::new()),
        })
    }

    async fn txn_state(&self, outcome: Option<DeliveryState>) -> Option<DeliveryState> {
        let tx = self.tx.as_ref()?.lock().await;
        let txn_id = tx.txn_id.clone()?;
        Some(DeliveryState::Transactional(TransactionalState {
            txn_id,
            outcome: outcome.map(Box::new),
        }))
    }

    /// `createProducer(destination)`: a sender to it.
    pub async fn producer(&self, dest: &Destination) -> Result<AmqpProducer> {
        let n = self.producers.fetch_add(1, Ordering::SeqCst) + 1;
        let id = format!("{}:{n}", self.id);
        let address = self.conn.address(dest);
        let mut desired = None;
        if !self.conn.offers("DELAYED_DELIVERY") {
            desired = Some(vec![Symbol::from("DELAYED_DELIVERY")]);
        }
        let link = self
            .session
            .attach(Attach {
                name: format!("qpid-jms:sender:{id}:{address}"),
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: Some(Source {
                    address: Some(id.clone()),
                    outcomes: Some(outcomes()),
                    ..Source::default()
                }),
                target: Some(TargetKind::Target(Target {
                    address: Some(address),
                    capabilities: Some(vec![type_capability(dest)]),
                    ..Target::default()
                })),
                desired_capabilities: desired,
                ..Attach::default()
            })
            .await
            .map_err(|f| self.conn.open_failed(&f))?;
        if link.remote.target.is_none() {
            return Err(self
                .conn
                .warn_open_failed(wait_refusal(link, "Link creation was refused").await));
        }
        Ok(AmqpProducer {
            link: Arc::new(link),
            id,
            seq: AtomicU64::new(0),
            tags: Arc::new(Mutex::new(Tags::default())),
            dest: dest.clone(),
            in_flight: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// `JmsSession.send`: the message prepared and encoded, then sent; a
    /// persistent message outside a transaction waits for its outcome.
    #[allow(clippy::too_many_arguments)]
    pub async fn send(
        &self,
        producer: &AmqpProducer,
        message: &mut AmqpJmsMessage,
        persistent: bool,
        priority: u8,
        ttl: i64,
        message_id: bool,
        timestamp: bool,
    ) -> Result<()> {
        let settle = self
            .send_completion(
                producer, message, persistent, priority, ttl, message_id, timestamp,
            )
            .await;
        if persistent && !self.transacted {
            settle.await
        } else {
            // An asynchronous send's failure has no listener to go to.
            let handle = tokio::spawn(settle);
            let mut in_flight = producer.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            in_flight.retain(|h| !h.is_finished());
            in_flight.push(handle);
            Ok(())
        }
    }

    /// The send of `send(message, completionListener)`: always asynchronous,
    /// the returned future completing with the outcome.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_completion(
        &self,
        producer: &AmqpProducer,
        message: &mut AmqpJmsMessage,
        persistent: bool,
        priority: u8,
        ttl: i64,
        message_id: bool,
        timestamp: bool,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let now = now_millis();
        let seq = producer.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let id = message_id.then(|| format!("{}-{seq}", producer.id));
        message.prepare_send(
            persistent,
            priority,
            &producer.dest,
            now,
            ttl,
            id,
            timestamp,
        );
        let payload = message.encode();
        let state = self.txn_state(None).await;
        let tag = producer
            .tags
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let outcome = producer
            .link
            .send(tag.clone(), payload, false, state)
            .expect("unsettled");
        let tags = producer.tags.clone();
        async move {
            let r = match outcome.await.unwrap_or(Err(Failure::Closed)) {
                Ok(state) => send_outcome(state),
                Err(f) => Err(match f {
                    Failure::Remote(Scope::Link, None) => JmsError::jms(
                        "Producer closed remotely before message transfer result was notified",
                    ),
                    f => failure_error(&f),
                }),
            };
            if r.is_ok() {
                tags.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .give_back(tag);
            }
            r
        }
    }

    /// `createConsumer`, `createDurableSubscriber` or a shared consumer
    /// (`kind`), with a selector.
    pub async fn consumer(
        &self,
        dest: &Destination,
        selector: Option<&str>,
        kind: ConsumerKind,
    ) -> Result<AmqpConsumer> {
        let selector = selector.map(str::trim).filter(|s| !s.is_empty());
        let n = self.consumers.fetch_add(1, Ordering::SeqCst) + 1;
        let id = format!("{}:{n}", self.id);
        let address = self.conn.address(dest);
        let (durable, shared, browser) = match &kind {
            ConsumerKind::Plain => (false, false, false),
            ConsumerKind::Browser => (false, false, true),
            ConsumerKind::Durable { .. } => (true, false, false),
            ConsumerKind::Shared { durable, .. } => (*durable, true, false),
        };
        if matches!(kind, ConsumerKind::Durable { .. }) && !self.conn.explicit_client_id {
            return Err(JmsError::illegal_state(
                "You must specify a unique clientID for the Connection to use a DurableSubscriber",
            ));
        }
        let name = match &kind {
            ConsumerKind::Plain | ConsumerKind::Browser => {
                format!("qpid-jms:receiver:{id}:{address}")
            }
            ConsumerKind::Durable { name, no_local: _ } => self.conn.subscriptions.reserve(
                name,
                true,
                false,
                dest,
                selector,
                self.conn.explicit_client_id,
            )?,
            ConsumerKind::Shared { name, durable } => self.conn.subscriptions.reserve(
                name,
                *durable,
                true,
                dest,
                selector,
                self.conn.explicit_client_id,
            )?,
        };
        let mut capabilities = vec![type_capability(dest)];
        if shared {
            capabilities.push(Symbol::from("shared"));
            if !self.conn.explicit_client_id {
                capabilities.push(Symbol::from("global"));
            }
        }
        let mut filter = Vec::new();
        if let Some(s) = selector {
            filter.push((
                Symbol::from("jms-selector"),
                Value::described(Descriptor::Code(0x0000_468C_0000_0004), Value::string(s)),
            ));
        }
        if let ConsumerKind::Durable { no_local: true, .. } = kind {
            filter.push((
                Symbol::from("no-local"),
                Value::described(
                    Descriptor::Code(0x0000_468C_0000_0003),
                    Value::string("NoLocalFilter{}"),
                ),
            ));
        }
        let source = Source {
            address: Some(address),
            durable: if durable {
                TerminusDurability::UnsettledState
            } else {
                TerminusDurability::None
            },
            expiry_policy: if durable {
                TerminusExpiryPolicy::Never
            } else {
                TerminusExpiryPolicy::LinkDetach
            },
            distribution_mode: (durable || browser).then(|| Symbol::from("copy")),
            filter: (!filter.is_empty()).then_some(Fields(filter)),
            default_outcome: Some(DeliveryState::Modified(Modified {
                delivery_failed: Some(true),
                undeliverable_here: None,
                message_annotations: None,
            })),
            outcomes: Some(outcomes()),
            capabilities: Some(capabilities),
            ..Source::default()
        };
        let desired =
            (shared && !self.conn.offers("SHARED-SUBS")).then(|| vec![Symbol::from("SHARED-SUBS")]);
        let attached = self
            .session
            .attach(Attach {
                name: name.clone(),
                role: Role::Receiver,
                snd_settle_mode: if browser {
                    SenderSettleMode::Settled
                } else {
                    SenderSettleMode::Unsettled
                },
                rcv_settle_mode: ReceiverSettleMode::First,
                source: Some(source),
                target: Some(TargetKind::Target(Target::default())),
                desired_capabilities: desired,
                ..Attach::default()
            })
            .await;
        let link = match attached {
            Ok(link) => link,
            Err(f) => {
                self.conn.subscriptions.release(&name);
                return Err(self.conn.open_failed(&f));
            }
        };
        if link.remote.source.is_none() {
            self.conn.subscriptions.release(&name);
            return Err(self
                .conn
                .warn_open_failed(wait_refusal(link, "Link creation was refused").await));
        }
        let state = Arc::new(ConsumerState {
            prefetch: self.conn.prefetch(dest, durable, browser),
            counts: Mutex::new(Counts::default()),
        });
        let consumer = AmqpConsumer {
            link: AsyncMutex::new(link),
            dest: dest.clone(),
            name,
            durable,
            browser,
            state,
            queue: AsyncMutex::new(VecDeque::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
        };
        // init(): credit at once, even before the connection starts.
        consumer.send_flow_if_needed().await;
        Ok(consumer)
    }

    /// `createBrowser(queue, selector)`: its consumer is made with the
    /// enumeration.
    pub async fn browser(
        &self,
        queue: &Destination,
        selector: Option<&str>,
    ) -> Result<AmqpConsumer> {
        self.consumer(queue, selector, ConsumerKind::Browser).await
    }

    /// `receive` with the session's acknowledgement: accepted at once,
    /// in the transaction for a transacted session.
    pub async fn receive(
        &self,
        consumer: &AmqpConsumer,
        timeout: Option<Duration>,
    ) -> Result<Option<AmqpJmsMessage>> {
        match self.next(consumer, timeout, true).await? {
            Some((message, delivery)) => {
                self.acknowledge(consumer, delivery).await;
                Ok(Some(message))
            }
            None => Ok(None),
        }
    }

    /// What a `MessageListener` is given: a prefetched message, waited
    /// for up to `wait` with no drain, to be acknowledged once handled.
    pub async fn listened(
        &self,
        consumer: &AmqpConsumer,
        wait: Duration,
    ) -> Result<Option<(AmqpJmsMessage, Delivery)>> {
        self.next(consumer, Some(wait), false).await
    }

    /// The next message that has not expired, undecodable ones refused.
    async fn next(
        &self,
        consumer: &AmqpConsumer,
        timeout: Option<Duration>,
        pull: bool,
    ) -> Result<Option<(AmqpJmsMessage, Delivery)>> {
        loop {
            let delivery = if pull {
                consumer.dequeue(timeout).await?
            } else {
                consumer.dispatched(timeout.unwrap_or_default()).await?
            };
            let Some(delivery) = delivery else {
                return Ok(None);
            };
            let message = match AmqpJmsMessage::decode(
                &delivery.payload,
                Some(&consumer.dest),
                now_millis(),
            ) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(target: "org.apache.qpid.jms.provider.amqp.AmqpConsumer", "Error on transform: {e}");
                    consumer
                        .settle(&delivery, Some(modified_failed_undeliverable()))
                        .await;
                    continue;
                }
            };
            // localMessageExpiry.
            let expiration = message.expiration();
            if !consumer.browser && expiration > 0 && now_millis() > expiration {
                consumer
                    .settle(&delivery, Some(modified_failed_undeliverable()))
                    .await;
                continue;
            }
            return Ok(Some((message, delivery)));
        }
    }

    /// The session's acknowledgement of a delivered message: accepted at
    /// once, in the transaction for a transacted session.
    pub async fn acknowledge(&self, consumer: &AmqpConsumer, delivery: Delivery) {
        if !delivery.settled && !consumer.browser {
            match self.txn_state(Some(DeliveryState::Accepted)).await {
                Some(state) => {
                    consumer.settle(&delivery, Some(state)).await;
                    self.participants
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(consumer.state.clone());
                }
                None if self.transacted => {}
                None => {
                    consumer
                        .settle(&delivery, Some(DeliveryState::Accepted))
                        .await
                }
            }
        } else {
            consumer.acked_locally().await;
        }
    }

    /// `commit()`: the discharge, and the next transaction's declare sent
    /// straight after it.
    pub async fn commit(&self) -> Result<()> {
        self.discharge(true, true).await
    }

    /// `rollback()`: the discharge with `fail`, then the next declare.
    pub async fn rollback(&self) -> Result<()> {
        self.discharge(false, true).await
    }

    async fn discharge(&self, commit: bool, next: bool) -> Result<()> {
        let Some(tx) = &self.tx else {
            return Err(JmsError::illegal_state("Not a transacted session"));
        };
        let mut tx = tx.lock().await;
        let Some(txn_id) = tx.txn_id.take() else {
            return Err(JmsError::illegal_state(if commit {
                "Commit called with no active Transaction."
            } else {
                "Rollback called with no active Transaction."
            }));
        };
        let discharged = tx.send_control(
            Discharge {
                txn_id,
                fail: Some(!commit),
            }
            .to_value(),
        );
        let declare = next.then(|| tx.declare());
        let discharge_result = match discharged.await.unwrap_or(Err(Failure::Closed)) {
            Ok(DeliveryState::Rejected(r)) => {
                let cause = condition_error(r.error.as_ref());
                Err(
                    if commit && cause.class != "javax.jms.TransactionRolledBackException" {
                        JmsError {
                            class: "javax.jms.TransactionRolledBackException",
                            message: cause.message,
                        }
                    } else {
                        in_doubt(r.error.as_ref())
                    },
                )
            }
            Ok(_) => Ok(()),
            Err(f) => Err(failure_error(&f)),
        };
        if !commit {
            // postRollback: the prefetch of the consumers in it released.
            let participants =
                std::mem::take(&mut *self.participants.lock().unwrap_or_else(|e| e.into_inner()));
            drop(participants);
        } else {
            self.participants
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
        }
        let declare_result = match declare {
            Some(d) => declared(d.await.unwrap_or(Err(Failure::Closed))).map(Some),
            None => Ok(None),
        };
        discharge_result?;
        tx.txn_id = declare_result?;
        Ok(())
    }

    /// `close()`: a transacted session's transaction rolled back, then the
    /// end. Its links are not detached.
    pub async fn close(&self) {
        if self.tx.is_some() {
            let _ = self.discharge(false, false).await;
        }
        let _ = self.session.end(None).await;
    }
}

/// What a sent message's outcome means (`AmqpFixedProducer.applyDeliveryStateUpdate`).
fn send_outcome(state: DeliveryState) -> Result<()> {
    match state {
        DeliveryState::Accepted | DeliveryState::AcceptedNew => Ok(()),
        DeliveryState::Transactional(t) => match t.outcome {
            Some(inner) => send_outcome(*inner),
            None => Ok(()),
        },
        DeliveryState::Rejected(r) => Err(condition_error(r.error.as_ref())),
        DeliveryState::Released => Err(JmsError::jms("Delivery failed: released by receiver")),
        DeliveryState::Modified(_) => Err(JmsError::jms("Delivery failed: failure at remote")),
        _ => Ok(()),
    }
}

/// `MODIFIED_FAILED_UNDELIVERABLE`.
fn modified_failed_undeliverable() -> DeliveryState {
    DeliveryState::Modified(Modified {
        delivery_failed: Some(true),
        undeliverable_here: Some(true),
        message_annotations: None,
    })
}

/// A refused attach (no source or target): the peer's detach awaited,
/// and the error it gave, else `default`.
async fn wait_refusal(mut link: engine::Link, default: &str) -> JmsError {
    loop {
        match link.events.recv().await {
            Some(LinkEvent::Detached { error, closed }) => {
                if error.is_some() {
                    return condition_error(error.as_ref());
                }
                let _ = closed;
                return if default == "Link creation was refused" {
                    JmsError {
                        class: "javax.jms.InvalidDestinationException",
                        message: default.to_owned(),
                    }
                } else {
                    JmsError::jms(default)
                };
            }
            Some(LinkEvent::Failed(f)) => return failure_error(&f),
            Some(_) => continue,
            None => return JmsError::jms(default),
        }
    }
}

/// A producer's link.
pub struct AmqpProducer {
    link: Arc<engine::Link>,
    /// `<connectionId>:<session>:<n>`.
    pub id: String,
    seq: AtomicU64,
    tags: Arc<Mutex<Tags>>,
    pub dest: Destination,
    /// Asynchronous sends whose outcome is still to come.
    in_flight: Arc<Mutex<Vec<tokio::task::JoinHandle<Result<()>>>>>,
}

impl AmqpProducer {
    /// `close()`: once the sends in flight are settled, the link closed.
    pub async fn close(&self) {
        let pending =
            std::mem::take(&mut *self.in_flight.lock().unwrap_or_else(|e| e.into_inner()));
        for p in pending {
            let _ = p.await;
        }
        let _ = self.link.detach(true, None).await;
    }
}

/// Which consumer `createXxx` made.
#[derive(Debug, Clone)]
pub enum ConsumerKind {
    Plain,
    Browser,
    /// `createDurableSubscriber(topic, name, selector, noLocal)`.
    Durable {
        name: String,
        no_local: bool,
    },
    /// `createSharedConsumer` / `createSharedDurableConsumer`.
    Shared {
        name: String,
        durable: bool,
    },
}

#[derive(Default)]
struct Counts {
    /// Credit granted and not yet used.
    credit: u32,
    /// Messages handed to the application and not finally acknowledged.
    dispatched: u32,
    /// Messages marked delivered and not finally acknowledged.
    delivered: u32,
}

/// A consumer's credit bookkeeping (`AmqpConsumer`).
pub struct ConsumerState {
    prefetch: u32,
    counts: Mutex<Counts>,
}

/// A consumer's link and its local queue.
pub struct AmqpConsumer {
    link: AsyncMutex<engine::Link>,
    pub dest: Destination,
    pub name: String,
    durable: bool,
    browser: bool,
    state: Arc<ConsumerState>,
    queue: AsyncMutex<VecDeque<Delivery>>,
    closed: std::sync::atomic::AtomicBool,
}

impl AmqpConsumer {
    fn counts(&self) -> std::sync::MutexGuard<'_, Counts> {
        self.state.counts.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `sendFlowIfNeeded`: credit topped up to the prefetch once half is
    /// used and too few are waiting.
    async fn send_flow_if_needed(&self) {
        let prefetch = self.state.prefetch;
        let grant = {
            let mut c = self.counts();
            if f64::from(c.credit) > f64::from(prefetch) * 0.5 {
                None
            } else {
                let potential = c.credit + c.dispatched.saturating_sub(c.delivered);
                if f64::from(potential) <= f64::from(prefetch) * 0.7 {
                    c.credit += prefetch - potential.min(prefetch);
                    Some(c.credit)
                } else {
                    None
                }
            }
        };
        if let Some(credit) = grant {
            self.link.lock().await.flow(credit, false);
        }
    }

    /// Takes what the link has delivered into the local queue, waiting up
    /// to `wait` for something when the queue is empty. Whether the link
    /// drained.
    async fn fill(&self, wait: Option<Duration>) -> Result<bool> {
        let mut link = self.link.lock().await;
        let mut queue = self.queue.lock().await;
        let mut drained = false;
        loop {
            let event = if !queue.is_empty() {
                match link.events.try_recv() {
                    Ok(e) => e,
                    Err(_) => return Ok(drained),
                }
            } else {
                match wait {
                    Some(w) => match tokio::time::timeout(w, link.events.recv()).await {
                        Ok(Some(e)) => e,
                        Ok(None) => {
                            return Err(JmsError::illegal_state("The MessageConsumer is closed"));
                        }
                        Err(_) => return Ok(drained),
                    },
                    None => match link.events.recv().await {
                        Some(e) => e,
                        None => {
                            return Err(JmsError::illegal_state("The MessageConsumer is closed"));
                        }
                    },
                }
            };
            match event {
                LinkEvent::Delivery(d) => {
                    let mut c = self.counts();
                    c.credit = c.credit.saturating_sub(1);
                    c.dispatched += 1;
                    drop(c);
                    queue.push_back(d);
                }
                LinkEvent::Drained => {
                    self.counts().credit = 0;
                    drained = true;
                    if queue.is_empty() {
                        return Ok(true);
                    }
                }
                LinkEvent::Flow => {}
                LinkEvent::Detached { error, .. } => {
                    return Err(JmsError {
                        class: "javax.jms.IllegalStateException",
                        message: "The MessageConsumer was closed due to an unrecoverable error."
                            .to_owned(),
                    }
                    .with_cause(error.as_ref()));
                }
                LinkEvent::Failed(f) => return Err(failure_error(&f)),
            }
        }
    }

    /// `AmqpConsumer.pull(0)`: one credit if none is left, then a drain,
    /// waited for, then the credit refilled.
    async fn pull_now(&self) -> Result<()> {
        {
            let link = self.link.lock().await;
            let credit = {
                let mut c = self.counts();
                if c.credit == 0 {
                    c.credit = 1;
                }
                c.credit
            };
            link.flow(credit, true);
        }
        loop {
            if self.fill(Some(Duration::from_secs(60))).await? {
                break;
            }
            if !self.queue.lock().await.is_empty() && self.counts().credit == 0 {
                break;
            }
        }
        self.send_flow_if_needed().await;
        Ok(())
    }

    /// `JmsMessageConsumer.dequeue`: the next message, waiting `timeout`
    /// (none: for ever; zero: not at all), with a drain when a wait ends
    /// empty.
    async fn dequeue(&self, timeout: Option<Duration>) -> Result<Option<Delivery>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(JmsError::illegal_state("The MessageConsumer is closed"));
        }
        if let Some(d) = self.queue.lock().await.pop_front() {
            return Ok(Some(d));
        }
        match timeout {
            None => {
                self.fill(None).await?;
            }
            Some(t) => {
                if !t.is_zero() {
                    self.fill(Some(t)).await?;
                }
                if self.queue.lock().await.is_empty() {
                    self.pull_now().await?;
                }
            }
        }
        Ok(self.queue.lock().await.pop_front())
    }

    /// A message as a listener's dispatch takes it: prefetched, waited for
    /// up to `wait`, never pulled.
    async fn dispatched(&self, wait: Duration) -> Result<Option<Delivery>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(JmsError::illegal_state("The MessageConsumer is closed"));
        }
        if let Some(d) = self.queue.lock().await.pop_front() {
            return Ok(Some(d));
        }
        self.fill(Some(wait)).await?;
        Ok(self.queue.lock().await.pop_front())
    }

    /// The final disposition of a delivery, settled.
    async fn settle(&self, d: &Delivery, state: Option<DeliveryState>) {
        if !d.settled {
            self.link
                .lock()
                .await
                .disposition(d.delivery_id, d.delivery_id, true, state);
        }
        self.acked_locally().await;
    }

    async fn acked_locally(&self) {
        {
            let mut c = self.counts();
            c.dispatched = c.dispatched.saturating_sub(1);
        }
        self.send_flow_if_needed().await;
    }

    /// `close()`: detached (closed unless durable), the prefetched messages
    /// left with the link.
    pub async fn close(&self, conn: &AmqpJmsConnection) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let link = self.link.lock().await;
        let _ = link.detach(!self.durable, None).await;
        conn.subscriptions.release(&self.name);
    }
}

impl JmsError {
    fn with_cause(mut self, e: Option<&AmqpCondition>) -> Self {
        if e.is_some() {
            self.message = format!("{} {}", self.message, error_message(e));
        }
        self
    }
}

/// `AmqpSubscriptionTracker`: the link names of durable and shared
/// subscriptions, and the clashes it refuses.
#[derive(Default)]
pub struct Subscriptions {
    inner: Mutex<Vec<SubEntry>>,
}

struct SubEntry {
    name: String,
    durable: bool,
    shared: bool,
    topic: String,
    selector: Option<String>,
    total: u32,
    active: Vec<String>,
}

impl Subscriptions {
    /// `reserveNextSubscriptionLinkName`.
    fn reserve(
        &self,
        name: &str,
        durable: bool,
        shared: bool,
        dest: &Destination,
        selector: Option<&str>,
        explicit: bool,
    ) -> Result<String> {
        if name.contains('|') {
            return Err(JmsError::jms(
                "Subscription name must not contain '|' character.",
            ));
        }
        let mut all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = all
            .iter()
            .find(|e| e.name == name && e.durable && !e.active.is_empty())
        {
            if !shared || !e.shared {
                return Err(JmsError::jms(if e.shared {
                    format!("A shared durable subscription is already active with name '{name}'")
                } else {
                    format!(
                        "A non-shared durable subscription is already active with name '{name}'"
                    )
                }));
            }
            if e.topic != dest.address() || e.selector.as_deref() != selector {
                return Err(JmsError::jms(
                    "Subscription details dont match existing subscriber.",
                ));
            }
        }
        let entry = match all
            .iter_mut()
            .position(|e| e.name == name && e.durable == durable && e.shared == shared)
        {
            Some(i) => &mut all[i],
            None => {
                all.push(SubEntry {
                    name: name.to_owned(),
                    durable,
                    shared,
                    topic: dest.address().to_owned(),
                    selector: selector.map(str::to_owned),
                    total: 0,
                    active: Vec::new(),
                });
                all.last_mut().expect("pushed")
            }
        };
        entry.total += 1;
        let n = entry.total;
        let link = match (durable, shared, explicit) {
            (true, false, _) => name.to_owned(),
            (true, true, true) if n == 1 => name.to_owned(),
            (true, true, true) => format!("{name}|{n}"),
            (true, true, false) if n == 1 => format!("{name}|global"),
            (true, true, false) => format!("{name}|global{n}"),
            (false, _, true) => format!("{name}|volatile{n}"),
            (false, _, false) => format!("{name}|global-volatile{n}"),
        };
        entry.active.push(link.clone());
        Ok(link)
    }

    fn release(&self, link: &str) {
        let mut all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for e in all.iter_mut() {
            e.active.retain(|l| l != link);
        }
        all.retain(|e| !e.active.is_empty());
    }
}
