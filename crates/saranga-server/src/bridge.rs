//! Core bridges (`<bridges>`): a bridge consumes a queue of this broker and
//! sends what it takes to an address of another broker, over a Core
//! connection of its own, acknowledging each message on the queue once the
//! other broker has it.
//!
//! Ported from `BridgeImpl`, `ClusterManager.deployBridge` and
//! `destroyBridge`, and `ActiveMQServerImpl.deployBridge`, `destroyBridge`
//! and `recoverStoredBridges`.
//!
//! The bridge is a consumer of its queue: the queue hands it a message only
//! while it is connected and not already sending one (`handle` answering
//! `BUSY` otherwise). The message is sent with the bridge's producer, on a
//! session with the bridge's confirmation window, and acknowledged on the
//! queue when its send is acknowledged. When the connection fails, what is
//! being sent goes back to the queue and the bridge connects again after
//! its retry interval: to the node it was connected to, its primary or its
//! backup, `reconnect-attempts-same-node` times, then through its static
//! connectors or its discovery group. With `use-duplicate-detection`, each
//! message carries `_AMQ_BRIDGE_DUP` (this node's ID and the message's), so
//! a message sent again is dropped by the other broker; without it, sends
//! block.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_buffer::json::Json;
use saranga_buffer::{PropertyValue, SimpleString};
use saranga_client::discovery::DiscoveryConfig;
use saranga_client::{
    Connection, ConnectionOptions, Connector, CoreMessage, Error as ClientError, ErrorType,
    Producer, SendAcknowledgementHandler, Session, SessionOptions,
};
use saranga_config::{BridgeConfig, ComponentRoutingType};
use saranga_core_protocol::transport::TransportConfiguration;
use tokio::sync::{Notify, mpsc, watch};

use crate::message::{self, ServerMessage};
use crate::notifications::{self, NotificationType};
use crate::postoffice::{Transformer, TransformerFactory};
use crate::queue::{ConsumerMetrics, ConsumerRegistration, DeliverySink, FilterRef, Queue};
use crate::{Broker, BrokerError, RoutingType};

/// `Message.EMBEDDED_TYPE`: another protocol's message carried in a Core
/// message's body.
pub const EMBEDDED_TYPE: u8 = 7;

/// `Message.LARGE_EMBEDDED_TYPE`: another protocol's large message, its
/// encoding in `_AMQP_EMBED_LARGE`.
pub const LARGE_EMBEDDED_TYPE: u8 = 8;

/// `EmbedMessageUtil.AMQP_ENCODE_PROPERTY`.
pub const AMQP_EMBED_LARGE: &str = "_AMQP_EMBED_LARGE";

/// `EmbedMessageUtil.signature`.
pub const EMBED_SIGNATURE: &[u8] = b"EMB";

/// `BridgeImpl.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Starting,
    Started,
    Pausing,
    Paused,
    Stopping,
    Stopped,
}

impl State {
    fn name(self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Started => "STARTED",
            Self::Pausing => "PAUSING",
            Self::Paused => "PAUSED",
            Self::Stopping => "STOPPING",
            Self::Stopped => "STOPPED",
        }
    }
}

/// Where a bridge connects: its static connectors or its discovery group.
#[derive(Debug, Clone)]
pub enum Target {
    Static(Vec<TransportConfiguration>),
    Discovery(DiscoveryConfig),
}

impl Target {
    /// `ServerLocatorImpl.toString`.
    fn describe(&self, name: &str) -> String {
        match self {
            Self::Static(tcs) => format!(
                "ServerLocatorImpl (identity=Bridge {name}) [initialConnectors=[{}], discoveryGroupConfiguration=null]",
                tcs.iter()
                    .map(crate::replication::quorum::transport_to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Discovery(d) => format!(
                "ServerLocatorImpl (identity=Bridge {name}) [discoveryGroupConfiguration=DiscoveryGroupConfiguration{{name='{}', refreshTimeout={}, discoveryInitialWaitTimeout={}}}]",
                d.name,
                d.refresh_timeout.as_millis(),
                d.initial_wait_timeout.as_millis()
            ),
        }
    }
}

/// `BridgeMetrics`.
#[derive(Debug, Default)]
pub struct Metrics {
    pending_acknowledgement: AtomicI64,
    acknowledged: AtomicI64,
}

impl Metrics {
    pub fn messages_pending_acknowledgement(&self) -> i64 {
        self.pending_acknowledgement.load(Ordering::SeqCst)
    }

    pub fn messages_acknowledged(&self) -> i64 {
        self.acknowledged.load(Ordering::SeqCst)
    }
}

/// A message the queue handed over, to send.
struct Outgoing {
    message_id: i64,
    /// The message, or a native message's view.
    core: CoreMessage,
    native: Option<Arc<dyn message::NativeMessage>>,
    large: Option<Arc<crate::large::LargeBody>>,
}

/// What a connection ended with.
enum Ended {
    /// The bridge is stopping.
    Stop,
    /// The connection failed (`connectionFailed`), or the bridge disconnected
    /// itself (`disconnect`).
    Failed(ClientError),
}

/// Why a connection attempt did not give a connected bridge.
enum ConnectError {
    /// `SESSION_CREATION_REJECTED`: the other broker is starting; not
    /// counted as an attempt.
    Rejected,
    /// The forwarding address could not be queried; not counted either.
    Query(ClientError),
    /// The forwarding address does not exist there (`AMQ222097`).
    NoAddress,
    /// Anything else (`AMQ224091`).
    Failed(ClientError),
    Stopping,
}

/// A connected bridge: its connection, its two sessions and its producer.
struct Link {
    conn: Connection,
    session: Session,
    session_consumer: Session,
    producer: Producer,
    /// A cluster bridge's notification consumer, handing what it receives
    /// to the flow record.
    notifications: Option<tokio::task::JoinHandle<()>>,
}

impl Link {
    async fn close(self) {
        if let Some(t) = &self.notifications {
            t.abort();
        }
        self.producer.close();
        self.session.close().await;
        self.session_consumer.close().await;
        self.conn.close().await;
    }
}

/// What the bridge's tasks and its queue sink share.
struct Inner {
    name: String,
    queue: Queue,
    state: Mutex<State>,
    /// `refs`: the IDs of the messages handed over and not yet
    /// acknowledged, in the order they came.
    refs: Mutex<Vec<i64>>,
    /// `pendingAcks`.
    pending_acks: AtomicI64,
    acks_done: Notify,
    /// A send in progress: the queue holds the next message back.
    busy: AtomicBool,
    /// Where the sink puts what the queue hands over, while connected.
    outbox: Mutex<Option<mpsc::UnboundedSender<Outgoing>>>,
    /// The queue consumer the bridge is now, 0 when it is none.
    consumer_id: AtomicU64,
    connected: AtomicBool,
    /// The forwarding connection's remote address, while connected.
    remote: Mutex<Option<String>>,
    metrics: Metrics,
    /// Acknowledged message IDs, for the acknowledging task.
    acks: mpsc::UnboundedSender<i64>,
}

impl Inner {
    fn state(&self) -> State {
        *self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_state(&self, s: State) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = s;
    }

    fn refs(&self) -> std::sync::MutexGuard<'_, Vec<i64>> {
        self.refs.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remove_ref(&self, message_id: i64) -> bool {
        let mut refs = self.refs();
        match refs.iter().position(|id| *id == message_id) {
            Some(i) => {
                refs.remove(i);
                true
            }
            None => false,
        }
    }

    fn count_down(&self) {
        if self.pending_acks.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.pending_acks.store(0, Ordering::SeqCst);
            self.acks_done.notify_waiters();
        }
    }
}

/// The bridge as its queue's consumer.
struct Sink(Arc<Inner>);

impl DeliverySink for Sink {
    /// `handle`: the reference is the bridge's until its send is
    /// acknowledged.
    fn deliver(
        &self,
        _consumer_id: i64,
        message: &Arc<ServerMessage>,
        _delivery_count: i32,
    ) -> i64 {
        let inner = &self.0;
        inner.busy.store(true, Ordering::SeqCst);
        inner.refs().push(message.message_id());
        inner.pending_acks.fetch_add(1, Ordering::SeqCst);
        let out = Outgoing {
            message_id: message.message_id(),
            core: message.core().clone(),
            native: message.native().cloned(),
            large: message.native().and(message.large().cloned()),
        };
        let sent = inner
            .outbox
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|tx| tx.send(out).is_ok());
        if !sent {
            // Disconnected meanwhile: the reference goes back with the
            // others when the bridge stops consuming.
            inner.busy.store(false, Ordering::SeqCst);
        }
        0
    }

    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        0
    }

    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }

    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    /// A large message comes whole, its body read from its file; the
    /// producer sends it in chunks again.
    fn supports_large(&self) -> bool {
        false
    }

    /// `handle`'s `BUSY`: not started, or a send in progress.
    fn has_credits(&self, _message: &ServerMessage) -> bool {
        self.0.state() == State::Started && !self.0.busy.load(Ordering::SeqCst)
    }
}

/// The session's send acknowledgements (`sendAcknowledged`, `sendFailed`).
struct Acks(Weak<Bridge>);

impl SendAcknowledgementHandler for Acks {
    fn send_acknowledged(&self, message: &CoreMessage) {
        let Some(bridge) = self.0.upgrade() else {
            return;
        };
        let inner = &bridge.inner;
        let state = inner.state();
        if !matches!(state, State::Started | State::Stopping | State::Pausing) {
            tracing::debug!(
                "Bridge {} state is {}. Ignoring call to sendAcknowledged.",
                inner.name,
                state.name()
            );
            return;
        }
        if inner.remove_ref(message.message_id) {
            let _ = inner.acks.send(message.message_id);
        }
    }

    fn send_failed(&self, message: &CoreMessage, error: &ClientError) {
        let Some(bridge) = self.0.upgrade() else {
            return;
        };
        tracing::warn!(
            "AMQ224143: Bridge {} failed to send {}: {} {}",
            bridge.inner.name,
            message_string(message),
            exception_name(error),
            exception_message(error)
        );
        if error.kind() == Some(ErrorType::AddressFull) {
            bridge.disconnect(error.clone());
        }
    }
}

/// `ActiveMQException.getClass().getSimpleName()` of a client error.
fn exception_name(e: &ClientError) -> String {
    match e.kind() {
        None | Some(ErrorType::GenericException) => "ActiveMQException".to_owned(),
        Some(ErrorType::Unblocked) => "ActiveMQUnBlockedException".to_owned(),
        Some(k) => format!("ActiveMQ{k:?}Exception"),
    }
}

fn exception_message(e: &ClientError) -> String {
    match e {
        ClientError::ActiveMq { message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// `CoreMessage.toString`'s gist.
fn message_string(m: &CoreMessage) -> String {
    format!(
        "CoreMessage[messageID={}, durable={}, address={}]",
        m.message_id,
        m.durable,
        m.address
            .as_ref()
            .map_or_else(|| "null".to_owned(), SimpleString::to_string_lossy)
    )
}

/// The 16 bytes of a node ID in its text form.
fn uuid_bytes(node_id: &str) -> [u8; 16] {
    let hex: Vec<u8> = node_id
        .bytes()
        .filter(u8::is_ascii_hexdigit)
        .map(|b| (b as char).to_digit(16).unwrap_or(0) as u8)
        .collect();
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).take(16).enumerate() {
        out[i] = (pair[0] << 4) | pair.get(1).copied().unwrap_or(0);
    }
    out
}

/// `BridgeImpl.getDuplicateBytes`: the node's UUID, then the message ID.
pub fn duplicate_bytes(node_uuid: &[u8; 16], message_id: i64) -> Vec<u8> {
    let mut b = Vec::with_capacity(24);
    b.extend_from_slice(node_uuid);
    b.extend_from_slice(&message_id.to_be_bytes());
    b
}

/// A deployed bridge (`BridgeImpl`).
pub struct Bridge {
    config: BridgeConfig,
    target: Target,
    node_id: String,
    node_uuid: [u8; 16],
    transformer: Option<Arc<dyn Transformer>>,
    filter: Option<FilterRef>,
    broker: Weak<Broker>,
    inner: Arc<Inner>,
    /// The task that connects and forwards, while the bridge is started.
    run: Mutex<Option<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>>,
    /// Asks the forwarding loop to drop the connection (`disconnect`).
    disconnect: watch::Sender<Option<ClientError>>,
    acker: tokio::task::JoinHandle<()>,
    /// A cluster connection's bridge to another node
    /// (`ClusterConnectionBridge`).
    cluster: Option<crate::cluster_bridge::ClusterLink>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.acker.abort();
        if let Some((stop, task)) = self.run.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = stop.send(true);
            task.abort();
        }
    }
}

impl Bridge {
    /// `new BridgeImpl`: the transformer is made and the filter parsed now.
    pub(crate) fn new(
        broker: &Arc<Broker>,
        config: BridgeConfig,
        target: Target,
        queue: Queue,
        transformers: &std::collections::HashMap<String, TransformerFactory>,
    ) -> Result<Arc<Self>, BrokerError> {
        let transformer = match &config.transformer {
            None => None,
            Some(t) => match transformers.get(&t.class_name) {
                Some(make) => Some(make(&t.properties.iter().cloned().collect())),
                None => {
                    return Err(BrokerError::internal(format!(
                        "no transformer is registered as {}",
                        t.class_name
                    )));
                }
            },
        };
        Self::build(broker, config, target, queue, transformer, None)
    }

    /// `new ClusterConnectionBridge`: a bridge of a cluster connection to
    /// the node of `link`, consuming its store-and-forward queue.
    pub(crate) fn new_cluster(
        broker: &Arc<Broker>,
        config: BridgeConfig,
        queue: Queue,
        link: crate::cluster_bridge::ClusterLink,
    ) -> Result<Arc<Self>, BrokerError> {
        let target = Target::Static(vec![link.connector.clone()]);
        Self::build(broker, config, target, queue, None, Some(link))
    }

    fn build(
        broker: &Arc<Broker>,
        config: BridgeConfig,
        target: Target,
        queue: Queue,
        transformer: Option<Arc<dyn Transformer>>,
        cluster: Option<crate::cluster_bridge::ClusterLink>,
    ) -> Result<Arc<Self>, BrokerError> {
        let filter = broker
            .post_office()
            .filters()
            .create(config.filter.as_deref())?;
        let (acks_tx, mut acks_rx) = mpsc::unbounded_channel::<i64>();
        let inner = Arc::new(Inner {
            name: config.name.clone(),
            queue,
            state: Mutex::new(State::Stopped),
            refs: Mutex::new(Vec::new()),
            pending_acks: AtomicI64::new(0),
            acks_done: Notify::new(),
            busy: AtomicBool::new(false),
            outbox: Mutex::new(None),
            consumer_id: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            remote: Mutex::new(None),
            metrics: Metrics::default(),
            acks: acks_tx,
        });
        // sendAcknowledged's queue work: the acknowledgement, journaled.
        let acker = {
            let inner = inner.clone();
            let broker = Arc::downgrade(broker);
            tokio::spawn(async move {
                while let Some(id) = acks_rx.recv().await {
                    let consumer = inner.consumer_id.load(Ordering::SeqCst);
                    acknowledge(&inner, consumer, id).await;
                    if let Some(b) = broker.upgrade() {
                        b.post_office()
                            .plugins()
                            .each(|p| p.after_acknowledge_bridge(&inner.name, id));
                    }
                }
            })
        };
        Ok(Arc::new(Self {
            node_uuid: uuid_bytes(broker.node_id()),
            node_id: broker.node_id().to_owned(),
            config,
            target,
            transformer,
            filter,
            broker: Arc::downgrade(broker),
            inner,
            run: Mutex::new(None),
            disconnect: watch::channel(None).0,
            acker,
            cluster,
        }))
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn config(&self) -> &BridgeConfig {
        &self.config
    }

    pub fn queue(&self) -> &Queue {
        &self.inner.queue
    }

    pub fn state(&self) -> State {
        self.inner.state()
    }

    /// `isStarted`: starting or started.
    pub fn is_started(&self) -> bool {
        matches!(self.state(), State::Starting | State::Started)
    }

    /// `isConnected`.
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::SeqCst)
    }

    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// `getForwardingConnection().getRemoteAddress()`, while connected.
    pub fn remote_address(&self) -> Option<String> {
        self.inner
            .remote
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `BridgeImpl.toString`.
    pub fn describe(&self) -> String {
        let queue = match self.broker.upgrade() {
            Some(b) => crate::audit::queue_display(&b, &self.inner.queue),
            None => format!("QueueImpl[name={}]", self.inner.queue.name()),
        };
        format!(
            "{}@{:x} [name={}, queue={queue} targetConnector={}]",
            if self.cluster.is_some() {
                "ClusterConnectionBridge"
            } else {
                "BridgeImpl"
            },
            crate::audit::identity(("bridge", &self.config.name)),
            self.config.name,
            self.target.describe(&self.config.name)
        )
    }

    fn notify(&self, kind: NotificationType) {
        if let Some(b) = self.broker.upgrade() {
            let name = self.config.name.clone();
            b.post_office().notify(kind, move |_| {
                vec![("name", notifications::string(Some(&name)))]
            });
        }
    }

    /// `start`: connects on a task of its own and tells `BRIDGE_STARTED`.
    pub fn start(self: &Arc<Self>) -> Result<(), BrokerError> {
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            match *state {
                State::Stopping | State::Pausing => {
                    return Err(cannot_be(&self.config.name, "started", *state));
                }
                State::Starting | State::Started => return Ok(()),
                State::Stopped | State::Paused => {}
            }
            *state = State::Starting;
        }
        tracing::debug!("Bridge {} is starting", self.config.name);
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(self.clone().run(stop_rx));
        if let Some((old, _)) = self
            .run
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace((stop_tx, task))
        {
            let _ = old.send(true);
        }
        self.notify(NotificationType::BridgeStarted);
        Ok(())
    }

    /// `stop` and `StopRunnable`: no more messages are taken, the sends
    /// awaiting acknowledgement are waited for (`pending-ack-timeout`), and
    /// the connection is closed. Returns once stopped.
    pub async fn stop(self: &Arc<Self>) -> Result<(), BrokerError> {
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            match *state {
                State::Pausing => return Err(cannot_be(&self.config.name, "stopped", *state)),
                State::Stopping | State::Stopped => return Ok(()),
                _ => {}
            }
            *state = State::Stopping;
        }
        tracing::debug!("Bridge {} is stopping", self.config.name);
        let run = self.run.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some((stop, task)) = run {
            let _ = stop.send(true);
            let _ = task.await;
        }
        // Not connected (or given up): nothing to wait for.
        if self.inner.state() != State::Stopped {
            self.remove_consumer().await;
            self.inner.set_state(State::Stopped);
            self.stopped();
        }
        Ok(())
    }

    /// The end of `StopRunnable`.
    fn stopped(&self) {
        self.notify(NotificationType::BridgeStopped);
        tracing::info!("AMQ221029: Stopped bridge {}", self.config.name);
    }

    /// `disconnect`: the connection is dropped, and made again.
    fn disconnect(&self, cause: ClientError) {
        let _ = self.disconnect.send(Some(cause));
    }

    /// `ConnectRunnable`, `scheduleRetryConnect` and the forwarding while
    /// connected, until the bridge stops or gives up.
    async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let mut retry_count: i32 = 0;
        let mut attempts_in_use = self.config.initial_connect_attempts;
        // `targetNode`: the primary and backup of the node connected to.
        let mut target_node: Option<(Connector, Option<Connector>)> = None;
        loop {
            if *stop.borrow() {
                return;
            }
            retry_count += 1;
            let attempt = self.connect(target_node.as_ref(), retry_count);
            let connected = tokio::select! {
                r = attempt => r,
                _ = stopped(&mut stop) => Err(ConnectError::Stopping),
            };
            let retry = match connected {
                Ok(link) => {
                    // afterConnect.
                    retry_count = 0;
                    attempts_in_use = self.config.reconnect_attempts;
                    if let Some(c) = link.conn.connector() {
                        target_node = Some((c, link.conn.backup_connector()));
                    }
                    match self.forward(link, &mut stop).await {
                        Some(()) => return,
                        None => None,
                    }
                }
                Err(ConnectError::Stopping) => return,
                Err(ConnectError::Rejected) => {
                    tracing::warn!(
                        "AMQ222098: Server is starting, retry to create the session for bridge {}",
                        self.config.name
                    );
                    retry_count -= 1;
                    Some(self.retry_interval())
                }
                Err(ConnectError::Query(e)) => {
                    tracing::warn!(
                        "AMQ222096: Error on querying binding on bridge {}. Retrying in 100 milliseconds: {e}",
                        self.config.name
                    );
                    retry_count -= 1;
                    Some(Duration::from_millis(100))
                }
                Err(ConnectError::NoAddress) => {
                    tracing::warn!(
                        "AMQ222097: Address {} does not have any bindings, retry #({retry_count})",
                        self.config
                            .forwarding_address
                            .as_deref()
                            .unwrap_or_default()
                    );
                    None
                }
                Err(ConnectError::Failed(e)) => {
                    tracing::warn!(
                        "AMQ224091: Bridge {} is unable to connect to destination. Retrying",
                        self.describe()
                    );
                    tracing::debug!("Underlying bridge connection failure: {e}");
                    if matches!(
                        e.kind(),
                        Some(ErrorType::ClusterSecurityException | ErrorType::SecurityException)
                    ) && let Some(b) = self.broker.upgrade()
                    {
                        // IncomingInterceptorLookingForExceptionMessage.
                        tracing::error!(
                            "AMQ224058: Stopping ClusterManager. As it failed to authenticate with the cluster: {}",
                            exception_message(&e)
                        );
                        tokio::spawn(async move { b.stop_cluster_manager().await });
                    }
                    None
                }
            };
            let delay = match retry {
                Some(d) => d,
                // scheduleRetryConnect.
                None => {
                    let state = self.inner.state();
                    if matches!(state, State::Stopping | State::Pausing) {
                        tracing::info!(
                            "AMQ221028: Bridge is {}, will not retry",
                            if state == State::Stopping {
                                "stopping"
                            } else {
                                "pausing"
                            }
                        );
                        return;
                    }
                    if attempts_in_use >= 0 && retry_count > attempts_in_use {
                        tracing::warn!(
                            "AMQ222101: Bridge {} achieved {retry_count} maxattempts={} it will stop retrying to reconnect",
                            self.config.name,
                            self.config.reconnect_attempts
                        );
                        return;
                    }
                    self.backoff(retry_count)
                }
            };
            tracing::debug!(
                "Scheduling retry for bridge {} in {} milliseconds",
                self.config.name,
                delay.as_millis()
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = stopped(&mut stop) => return,
            }
        }
    }

    fn retry_interval(&self) -> Duration {
        Duration::from_millis(self.config.retry_interval.max(0) as u64)
    }

    /// `scheduleRetryConnect`'s delay: the retry interval times the
    /// multiplier to the power of the attempts, at most the maximum.
    fn backoff(&self, retry_count: i32) -> Duration {
        let c = &self.config;
        let mut timeout =
            (c.retry_interval as f64 * c.retry_interval_multiplier.powi(retry_count)) as i64;
        if timeout == 0 {
            timeout = c.retry_interval;
        }
        if timeout > c.max_retry_interval {
            timeout = c.max_retry_interval;
        }
        Duration::from_millis(timeout.max(0) as u64)
    }

    /// The bridge's `ServerLocator` settings: no reconnection of its own,
    /// one attempt per connector.
    fn connection_options(&self) -> ConnectionOptions {
        let c = &self.config;
        let millis = |v: i64| (v >= 0).then(|| Duration::from_millis(v as u64));
        ConnectionOptions {
            client_failure_check_period: millis(c.client_failure_check_period),
            connection_ttl: millis(c.connection_ttl),
            call_timeout: Duration::from_millis(c.call_timeout.max(0) as u64),
            user: c.user.clone(),
            password: c.password.clone(),
            ha: c.ha,
            reconnect_attempts: 0,
            initial_connect_attempts: 0,
            retry_interval: self.retry_interval(),
            retry_interval_multiplier: c.retry_interval_multiplier,
            max_retry_interval: Duration::from_millis(c.max_retry_interval.max(0) as u64),
            // A cluster bridge's locator shares the cluster's topology.
            topology_listener: self.cluster.as_ref().and_then(|l| l.topology_hook()),
            ..ConnectionOptions::default()
        }
    }

    /// `createSessionFactory`: the node connected to before, alternating
    /// its primary and backup, while `reconnect-attempts-same-node`
    /// allows; otherwise the initial connectors or the discovery group.
    async fn session_factory(
        &self,
        target_node: Option<&(Connector, Option<Connector>)>,
        retry_count: i32,
    ) -> Result<Connection, ClientError> {
        // ClusterConnectionBridge.createSessionFactory: the node as the
        // topology has it now, else the connector it was first found at.
        if let Some(link) = &self.cluster {
            let connector = link.target_connector();
            let connectors = Connector::from_transport(&connector).into_iter().collect();
            return Connection::connect_to(connectors, self.connection_options()).await;
        }
        let same_node = self.config.reconnect_attempts_on_same_node;
        if let Some((primary, backup)) = target_node
            && (same_node < 0 || retry_count <= same_node)
        {
            let configs: Vec<Connector> = std::iter::once(primary.clone())
                .chain(backup.clone())
                .collect();
            let node_try = ((retry_count - 1).max(0) as usize) % configs.len();
            return Connection::connect_to(
                vec![configs[node_try].clone()],
                self.connection_options(),
            )
            .await;
        }
        match &self.target {
            Target::Static(tcs) => {
                let connectors = tcs.iter().filter_map(Connector::from_transport).collect();
                Connection::connect_to(connectors, self.connection_options()).await
            }
            Target::Discovery(d) => {
                Connection::connect_discovery(d.clone(), self.connection_options()).await
            }
        }
    }

    /// `ConnectRunnable.run`: the connection, its sessions (pre-acknowledging,
    /// with the bridge's confirmation window), the forwarding address
    /// checked, and the producer.
    async fn connect(
        self: &Arc<Self>,
        target_node: Option<&(Connector, Option<Connector>)>,
        retry_count: i32,
    ) -> Result<Link, ConnectError> {
        if matches!(self.inner.state(), State::Stopping | State::Pausing) {
            return Err(ConnectError::Stopping);
        }
        tracing::debug!(
            "Connecting {} to its destination [{}]",
            self.config.name,
            self.node_id
        );
        let failed = |e: ClientError| {
            if e.kind() == Some(ErrorType::SessionCreationRejected) {
                ConnectError::Rejected
            } else {
                ConnectError::Failed(e)
            }
        };
        let conn = self
            .session_factory(target_node, retry_count)
            .await
            .map_err(failed)?;
        let c = &self.config;
        let options = SessionOptions {
            username: c.user.clone(),
            password: c.password.clone(),
            client_id: c.client_id.clone(),
            pre_acknowledge: true,
            ack_batch_size: 1,
            block_on_durable_send: !c.use_duplicate_detection,
            block_on_non_durable_send: !c.use_duplicate_detection,
            producer_window_size: c.producer_window_size,
            min_large_message_size: c.min_large_message_size,
            confirmation_window_size: c.confirmation_window_size,
            ..SessionOptions::default()
        };
        let sessions = async {
            let session = conn.create_session(options.clone()).await?;
            let session_consumer = conn.create_session(options).await?;
            Ok::<_, ClientError>((session, session_consumer))
        };
        let (session, session_consumer) = match sessions.await {
            Ok(s) => s,
            Err(e) => {
                conn.close().await;
                return Err(failed(e));
            }
        };
        if let Some(address) = &c.forwarding_address {
            match session.address_query(address).await {
                Err(e) => {
                    conn.close().await;
                    return Err(ConnectError::Query(e));
                }
                Ok(q) if !q.exists => {
                    conn.close().await;
                    return Err(ConnectError::NoAddress);
                }
                Ok(_) => {}
            }
        }
        let producer = match session.create_producer(None).await {
            Ok(p) => p,
            Err(e) => {
                conn.close().await;
                return Err(failed(e));
            }
        };
        if let Err(e) =
            session.set_send_acknowledgement_handler(Arc::new(Acks(Arc::downgrade(self))))
        {
            tracing::debug!("bridge {}: {e}", c.name);
        }
        // A cluster bridge announces this node over its connection
        // (`onConnection`) and learns of the other node's queues
        // (`afterConnect`).
        let notifications = match &self.cluster {
            None => None,
            Some(link) => {
                link.on_connection(&conn).await;
                match link.setup_notification_consumer(&session_consumer).await {
                    Ok(task) => Some(task),
                    Err(e) => {
                        producer.close();
                        conn.close().await;
                        return Err(failed(e));
                    }
                }
            }
        };
        Ok(Link {
            conn,
            session,
            session_consumer,
            producer,
            notifications,
        })
    }

    /// Connected: the bridge consumes its queue and sends what it takes,
    /// until the connection fails (`None`, to connect again) or the bridge
    /// stops (`Some`).
    async fn forward(self: &Arc<Self>, link: Link, stop: &mut watch::Receiver<bool>) -> Option<()> {
        let inner = &self.inner;
        let (tx, mut rx) = mpsc::unbounded_channel();
        *inner.outbox.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
        inner.busy.store(false, Ordering::SeqCst);
        self.disconnect.send_replace(None);
        let mut disconnect = self.disconnect.subscribe();
        let mut stopping = stop.clone();
        let stopping_already = {
            let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
            let stopping = *state == State::Stopping;
            if !stopping {
                *state = State::Started;
            }
            stopping
        };
        if stopping_already {
            self.finish_stop(link).await;
            return Some(());
        }
        inner.connected.store(true, Ordering::SeqCst);
        *inner.remote.lock().unwrap_or_else(|e| e.into_inner()) = link.conn.remote_address();
        self.add_consumer();
        tracing::info!("AMQ221027: Bridge {} is connected", self.describe());
        let ended = loop {
            tokio::select! {
                biased;
                _ = stopped(stop) => break Ended::Stop,
                _ = link.conn.failed() => {
                    break Ended::Failed(link.conn.failure().unwrap_or_else(|| ClientError::InvalidArgument("connection failed".to_owned())));
                }
                _ = disconnected(&mut disconnect) => {
                    let cause = self.disconnect.borrow().clone().expect("waited for");
                    break Ended::Failed(cause);
                }
                Some(out) = rx.recv() => {
                    // A send waiting for credits gives way to a stop or a
                    // failure; what it held goes back to the queue.
                    tokio::select! {
                        r = self.send(&link, out) => if let Err(e) = r {
                            break Ended::Failed(e);
                        },
                        _ = stopped(&mut stopping) => break Ended::Stop,
                        _ = link.conn.failed() => {
                            break Ended::Failed(link.conn.failure().unwrap_or_else(|| ClientError::InvalidArgument("connection failed".to_owned())));
                        }
                    }
                }
            }
        };
        *inner.outbox.lock().unwrap_or_else(|e| e.into_inner()) = None;
        inner.connected.store(false, Ordering::SeqCst);
        *inner.remote.lock().unwrap_or_else(|e| e.into_inner()) = None;
        match ended {
            Ended::Stop => {
                self.finish_stop(link).await;
                Some(())
            }
            // A cluster bridge whose node went away for good stops here.
            Ended::Failed(e) => self.connection_failed(link, &e).await.then_some(()),
        }
    }

    /// One message out (`beforeForward` and `deliverStandardMessage`).
    async fn send(&self, link: &Link, out: Outgoing) -> Result<(), ClientError> {
        let inner = &self.inner;
        let id = out.message_id;
        let dest = self
            .config
            .forwarding_address
            .clone()
            .or_else(|| out.core.address.as_ref().map(SimpleString::to_string_lossy))
            .unwrap_or_default();
        let message = self.before_forward(out, &dest);
        let plugins = self
            .broker
            .upgrade()
            .map(|b| b.post_office().plugins().clone());
        if let Some(p) = &plugins {
            p.each(|p| p.before_deliver_bridge(&self.config.name, id));
        }
        let result = link.producer.send_to(&dest, message).await;
        inner.busy.store(false, Ordering::SeqCst);
        match result {
            Ok(()) => {
                inner
                    .metrics
                    .pending_acknowledgement
                    .fetch_add(1, Ordering::SeqCst);
                if let Some(p) = &plugins {
                    p.each(|p| p.after_deliver_bridge(&self.config.name, id));
                }
                inner.queue.prompt_delivery();
                Ok(())
            }
            Err(e) => {
                if e.kind() == Some(ErrorType::AddressFull) {
                    tracing::warn!(
                        "AMQ222703: Address \"{dest}\" is full. Bridge {} will disconnect",
                        self.config.name
                    );
                } else {
                    tracing::warn!(
                        "AMQ222094: Bridge unable to send message Reference[{id}]:{}, will try again once bridge reconnects: {e}",
                        if self.config.forwarding_address.is_some() {
                            "RELIABLE"
                        } else {
                            "NON-RELIABLE"
                        }
                    );
                }
                if inner.remove_ref(id) {
                    inner.count_down();
                }
                Err(e)
            }
        }
    }

    /// `beforeForward`: a copy keeping the message's ID, with the bridge's
    /// duplicate ID, address and routing type, transformed, and another
    /// protocol's message embedded in a Core one.
    fn before_forward(&self, out: Outgoing, dest: &str) -> CoreMessage {
        let mut core = out.core.clone();
        // ClusterConnectionBridge.beforeForward: of the queue IDs each
        // bridge's binding added, only this bridge's go on, as the IDs the
        // other node routes to.
        if self.cluster.is_some() {
            let route_to = crate::remote_binding::HDR_ROUTE_TO_IDS;
            let mine = SimpleString::from(format!("{route_to}{}", self.config.name).as_str());
            let ids = core.properties.get(&mine).cloned();
            let names: Vec<SimpleString> = core
                .properties
                .entries()
                .iter()
                .map(|(k, _)| k.clone())
                .filter(|k| k.to_string_lossy().starts_with(route_to))
                .collect();
            for k in &names {
                core.properties.remove(k);
            }
            match ids {
                Some(ids @ PropertyValue::Bytes(_)) => {
                    core.properties.insert(SimpleString::from(route_to), ids);
                }
                _ => tracing::warn!(
                    "AMQ229258: Invalid cluster bridge message! No queue IDs defined in the property {}",
                    mine.to_string_lossy()
                ),
            }
        }
        if self.config.use_duplicate_detection {
            core.properties.insert(
                message::HDR_BRIDGE_DUPLICATE_ID.into(),
                PropertyValue::Bytes(duplicate_bytes(&self.node_uuid, out.message_id)),
            );
        }
        core.address = Some(SimpleString::from(dest));
        let key = SimpleString::from(message::HDR_ROUTING_TYPE);
        match self.config.routing_type {
            ComponentRoutingType::Anycast => {
                core.properties
                    .insert(key, PropertyValue::Byte(RoutingType::Anycast.to_byte()));
            }
            ComponentRoutingType::Multicast => {
                core.properties
                    .insert(key, PropertyValue::Byte(RoutingType::Multicast.to_byte()));
            }
            ComponentRoutingType::Strip => {
                core.properties.remove(&key);
            }
            ComponentRoutingType::Pass => {}
        }
        if let Some(t) = &self.transformer {
            core = t.transform(core);
        }
        match out.native {
            None => core,
            Some(native) => {
                let native = native.updated(&out.core, &core);
                embed(native.as_ref(), out.large.as_deref(), out.message_id)
            }
        }
    }

    /// `queue.addConsumer(this)` and `deliverAsync`.
    fn add_consumer(&self) {
        let Some(b) = self.broker.upgrade() else {
            return;
        };
        let id = b.next_consumer_id();
        self.inner.consumer_id.store(id, Ordering::SeqCst);
        self.inner.queue.add_consumer(ConsumerRegistration {
            id,
            client_id: id as i64,
            sink: Arc::new(Sink(self.inner.clone())),
            filter: self.filter.clone(),
            priority: 0,
            browse_only: false,
            pre_acknowledge: false,
            started: true,
            metrics: Arc::new(ConsumerMetrics::default()),
        });
        self.inner.queue.credits(id, -1);
    }

    /// `queue.removeConsumer(this)`: the references being sent go back to
    /// the queue (`cancelRefs`), this delivery not counted.
    async fn remove_consumer(&self) {
        let id = self.inner.consumer_id.swap(0, Ordering::SeqCst);
        if id != 0 {
            self.inner.queue.remove_consumer(id, false).await;
        }
        let cancelled = std::mem::take(&mut *self.inner.refs());
        for _ in &cancelled {
            self.inner.count_down();
        }
        self.inner.busy.store(false, Ordering::SeqCst);
    }

    /// `connectionFailed` and `fail`: the bridge stops consuming, the
    /// connection is cleaned up, and a retry follows, unless a cluster
    /// bridge's node said it was going (true).
    async fn connection_failed(&self, link: Link, e: &ClientError) -> bool {
        if self.broker.upgrade().is_some_and(|b| !b.is_stopped()) {
            if e.kind() == Some(ErrorType::Disconnected) {
                tracing::info!("AMQ224134: Connection closed with failedOver=false");
            } else {
                tracing::warn!("AMQ222095: Connection failed with failedOver=false");
            }
        }
        if let Some(t) = &link.notifications {
            t.abort();
        }
        link.producer.close();
        link.conn.close().await;
        self.remove_consumer().await;
        // ClusterConnectionBridge.fail and tryScheduleRetryReconnect.
        match &self.cluster {
            None => false,
            Some(l) => l.fail(e.kind() == Some(ErrorType::Disconnected)),
        }
    }

    /// `StopRunnable` while connected: the bridge stops consuming, waits for
    /// what it sent to be acknowledged, then closes its sessions.
    async fn finish_stop(&self, link: Link) {
        let inner = &self.inner;
        tracing::debug!("stopping bridge {}", self.describe());
        let id = inner.consumer_id.load(Ordering::SeqCst);
        if id != 0 {
            // No more deliveries; the references the bridge holds stay its
            // own until acknowledged or cancelled.
            inner.queue.set_started(id, false).await;
        }
        let timeout = Duration::from_millis(self.config.pending_ack_timeout.max(0) as u64);
        let waited = tokio::time::timeout(timeout, async {
            loop {
                let notified = inner.acks_done.notified();
                if inner.pending_acks.load(Ordering::SeqCst) <= 0 {
                    break;
                }
                notified.await;
            }
        })
        .await;
        if waited.is_err() {
            tracing::info!(
                "AMQ221042: Stopping bridge {} timed out waiting for the send acknowledgement of {} messages. Messages may be duplicated between the bridge's source and the target.",
                self.config.name,
                inner.pending_acks.load(Ordering::SeqCst)
            );
        }
        inner.set_state(State::Stopped);
        link.close().await;
        self.remove_consumer().await;
        self.stopped();
    }
}

/// Until the bridge is told to stop.
async fn stopped(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}

/// Until the bridge is told to drop its connection.
async fn disconnected(rx: &mut watch::Receiver<Option<ClientError>>) {
    let _ = rx.wait_for(Option::is_some).await;
}

/// `queue.acknowledge(ref)` for a message the other broker has.
async fn acknowledge(inner: &Inner, consumer: u64, message_id: i64) {
    if consumer == 0 {
        return;
    }
    match inner.queue.ack(consumer, message_id, true, false).await {
        Ok(mut refs) => {
            for r in &mut refs {
                match inner.queue.store_ack(r) {
                    Ok(Some(p)) => {
                        tokio::spawn(async move {
                            let _ = p.wait().await;
                        });
                    }
                    Ok(None) => {}
                    Err(e) => tracing::error!("AMQ224055: Bridge Failed to ack: {e}"),
                }
                inner.queue.post_acknowledge(r);
            }
            inner.metrics.acknowledged.fetch_add(1, Ordering::SeqCst);
        }
        Err(e) => {
            tracing::error!("AMQ224055: Bridge Failed to ack: message {message_id}");
            inner.queue.reinsert(e.refs);
        }
    }
    inner.count_down();
}

/// `EmbedMessageUtil.embedAsCoreMessage`: another protocol's message as a
/// Core message carrying its persister's encoding, in its body
/// (`EMBEDDED_TYPE`) or, for a large message, in `_AMQP_EMBED_LARGE` with
/// the body as the Core message's (`LARGE_EMBEDDED_TYPE`).
fn embed(
    native: &dyn message::NativeMessage,
    large: Option<&crate::large::LargeBody>,
    message_id: i64,
) -> CoreMessage {
    let encoding = saranga_storage::RecordCodec::encode(&native.persisted());
    let view = native.view();
    let mut core = CoreMessage {
        message_id,
        durable: view.durable,
        address: view.address.clone(),
        ..CoreMessage::default()
    };
    match large {
        Some(body) => {
            core.message_type = LARGE_EMBEDDED_TYPE;
            core.properties
                .insert(AMQP_EMBED_LARGE.into(), PropertyValue::Bytes(encoding));
            core.body = body
                .read_all()
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        "AMQ222110: Error on resetting large message deliver - {message_id}: {e}"
                    );
                    Vec::new()
                })
                .into();
        }
        None => {
            core.message_type = EMBEDDED_TYPE;
            let mut body = EMBED_SIGNATURE.to_vec();
            body.extend_from_slice(&encoding);
            core.body = body.into();
        }
    }
    core
}

/// The connectors and discovery groups bridges may name
/// (`Configuration.getConnectorConfigurations` and
/// `getDiscoveryGroupConfigurations`).
#[derive(Debug, Clone, Default)]
pub struct Targets {
    /// Each connector, named.
    pub connectors: Vec<TransportConfiguration>,
    pub discovery_groups: Vec<DiscoveryConfig>,
}

impl Targets {
    pub fn from_file(config: &saranga_config::BrokerConfig) -> Self {
        Self {
            connectors: config
                .connectors
                .iter()
                .filter_map(|c| {
                    let name = c.name.as_deref()?;
                    Some(crate::cluster::transport_configuration(name, c))
                })
                .collect(),
            discovery_groups: config
                .discovery_groups
                .iter()
                .filter_map(|g| crate::discovery::discovery_config(config, &g.name))
                .collect(),
        }
    }
}

/// The bridges of a broker (`ClusterManager.bridges`) and their journal
/// records (`mapPersistedBridgeConfigurations`).
#[derive(Default)]
pub struct Bridges {
    deployed: Mutex<Vec<Arc<Bridge>>>,
    records: Mutex<std::collections::HashMap<String, i64>>,
    targets: Mutex<Targets>,
    /// Journaled bridges, recovered, to deploy once the configured ones
    /// are.
    recovered: Mutex<Vec<BridgeConfig>>,
}

impl Bridges {
    pub fn new(targets: Targets) -> Self {
        Self {
            targets: Mutex::new(targets),
            ..Self::default()
        }
    }

    fn deployed(&self) -> std::sync::MutexGuard<'_, Vec<Arc<Bridge>>> {
        self.deployed.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn records(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, i64>> {
        self.records.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A bridge record found in the bindings journal.
    pub fn reload_record(&self, record: i64, config: BridgeConfig) {
        self.records().insert(config.name.clone(), record);
        self.recovered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(config);
    }

    /// The connectors a reload brings.
    pub fn set_targets(&self, targets: Targets) {
        *self.targets.lock().unwrap_or_else(|e| e.into_inner()) = targets;
    }

    /// `addConnectorConfiguration`: a connector of that name is replaced.
    pub fn add_connector(&self, connector: TransportConfiguration) {
        let mut targets = self.targets.lock().unwrap_or_else(|e| e.into_inner());
        targets.connectors.retain(|c| c.name != connector.name);
        targets.connectors.push(connector);
    }

    /// The connectors and discovery groups that can be named.
    pub fn targets(&self) -> Targets {
        self.targets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn all(&self) -> Vec<Arc<Bridge>> {
        self.deployed().clone()
    }

    /// `getBridgeControlNames`: in the order of the `ConcurrentHashMap`
    /// holding the controls.
    pub fn names(&self) -> Vec<String> {
        let names: Vec<String> = self
            .deployed()
            .iter()
            .map(|b| b.name().to_owned())
            .collect();
        let hashes: Vec<i32> = names
            .iter()
            .map(|n| {
                n.encode_utf16()
                    .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
            })
            .collect();
        saranga_buffer::java_hash::hash_map_order(&hashes, None)
            .into_iter()
            .map(|i| names[i].clone())
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<Arc<Bridge>> {
        self.deployed().iter().find(|b| b.name() == name).cloned()
    }
}

impl Broker {
    pub fn bridges(&self) -> &Bridges {
        &self.bridges
    }

    /// `ClusterManager.deployBridge`: false, with a warning, when the
    /// bridge cannot be deployed; one bridge per `concurrency`, each
    /// started.
    pub fn deploy_bridge(self: &Arc<Self>, config: &BridgeConfig) -> Result<bool, BrokerError> {
        let Some(queue_name) = &config.queue_name else {
            tracing::warn!(
                "AMQ222122: Must specify a queue name for each bridge. This one {} will not be deployed.",
                config.name
            );
            return Ok(false);
        };
        if config.forwarding_address.is_none() {
            tracing::warn!(
                "AMQ222123: Forward address is not specified on bridge {}. Will use original message address instead",
                config.name
            );
        }
        if self.bridges.get(&config.name).is_some() {
            tracing::warn!(
                "AMQ222124: There is already a bridge with name {} deployed. This one will not be deployed.",
                config.name
            );
            return Ok(false);
        }
        let Some(queue) = self.post_office.queue(queue_name) else {
            tracing::warn!(
                "AMQ222125: No queue found with name {queue_name} bridge {} will not be deployed.",
                config.name
            );
            return Ok(false);
        };
        self.post_office
            .plugins()
            .check(|p| p.before_deploy_bridge(config))?;
        let target = {
            let targets = self
                .bridges
                .targets
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match &config.discovery_group {
                Some(group) => match targets.discovery_groups.iter().find(|d| d.name == *group) {
                    Some(d) => Target::Discovery(d.clone()),
                    None => {
                        tracing::warn!(
                            "AMQ222126: No discovery group found with name {group} bridge will not be deployed."
                        );
                        return Ok(false);
                    }
                },
                None => {
                    let mut tcs = Vec::new();
                    for name in config.static_connectors.iter().flatten() {
                        match targets
                            .connectors
                            .iter()
                            .find(|t| t.name.to_string_lossy() == *name)
                        {
                            Some(t) => tcs.push(t.clone()),
                            None => {
                                tracing::warn!(
                                    "AMQ222226: Connection configuration is null for connectorName {name}"
                                );
                                tracing::warn!(
                                    "AMQ222175: Bridge {} could not find configured connectors",
                                    config.name
                                );
                                return Ok(false);
                            }
                        }
                    }
                    Target::Static(tcs)
                }
            }
        };
        if !config.use_duplicate_detection {
            tracing::debug!(
                "Bridge {} is configured to not use duplicate detecion, it will send messages synchronously",
                config.name
            );
        }
        for i in 0..config.concurrency.max(1) {
            let mut c = config.clone();
            if config.concurrency > 1 {
                c.name = format!("{}-{i}", config.name);
            }
            c.parent_name = config.name.clone();
            let bridge = Bridge::new(self, c, target.clone(), queue.clone(), &self.transformers)?;
            self.bridges.deployed().push(bridge.clone());
            bridge.start()?;
            self.post_office
                .plugins()
                .each(|p| p.after_deploy_bridge(&bridge));
        }
        Ok(true)
    }

    /// `ActiveMQServerImpl.deployBridge`: deployed, then journaled
    /// (`storeBridgeConfiguration`).
    pub fn deploy_bridge_persisted(
        self: &Arc<Self>,
        config: &BridgeConfig,
    ) -> Result<bool, BrokerError> {
        if !self.deploy_bridge(config)? {
            return Ok(false);
        }
        if let Some(s) = &self.storage {
            let old = self.bridges.records().get(&config.name).copied();
            let body = saranga_storage::RecordCodec::encode(&to_record(config));
            let id = s
                .store_configuration(crate::storage::ids::BRIDGE_RECORD, body, old)
                .map_err(|e| BrokerError::internal(e.to_string()))?;
            self.bridges.records().insert(config.name.clone(), id);
        }
        Ok(true)
    }

    /// `ClusterManager.destroyBridge`: the bridges named `name` or
    /// `name-<n>` are stopped and forgotten.
    async fn undeploy_bridge(&self, name: &str) {
        let removed: Vec<Arc<Bridge>> = {
            let mut deployed = self.bridges.deployed();
            let (gone, kept) = std::mem::take(&mut *deployed).into_iter().partition(|b| {
                let n = b.name();
                n == name
                    || n.strip_prefix(name)
                        .and_then(|r| r.strip_prefix('-'))
                        .is_some_and(|d| !d.is_empty() && d.bytes().all(|c| c.is_ascii_digit()))
            });
            *deployed = kept;
            gone
        };
        for b in removed {
            if let Err(e) = b.stop().await {
                tracing::warn!("{e}");
            }
        }
    }

    /// `ActiveMQServerImpl.destroyBridge`: every bridge deployed under the
    /// name, and its record.
    pub async fn destroy_bridge(&self, name: &str) {
        let matching: Vec<String> = self
            .bridges
            .all()
            .iter()
            .filter(|b| b.config().parent_name == name)
            .map(|b| b.name().to_owned())
            .collect();
        for n in matching {
            self.undeploy_bridge(&n).await;
            self.delete_bridge_record(name);
        }
    }

    /// `deleteBridgeConfiguration`.
    fn delete_bridge_record(&self, name: &str) {
        let old = self.bridges.records().remove(name);
        if let (Some(old), Some(s)) = (old, &self.storage)
            && let Err(e) = s.delete_binding_record(old)
        {
            tracing::warn!("deleting the record of bridge {name}: {e}");
        }
    }

    /// `ClusterManager.start`'s `deployConfiguredBridges`, then
    /// `recoverStoredBridges`.
    pub(crate) fn deploy_bridges(self: &Arc<Self>, configured: &[BridgeConfig]) {
        for c in configured {
            if let Err(e) = self.deploy_bridge(c) {
                tracing::warn!("AMQ222118: unable to start Bridge {}: {e}", c.name);
            }
        }
        self.recover_stored_bridges();
    }

    /// `recoverStoredBridges`: a journaled bridge not deployed is.
    fn recover_stored_bridges(self: &Arc<Self>) {
        let recovered = self
            .bridges
            .recovered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for c in recovered {
            let missing = !self
                .bridges
                .all()
                .iter()
                .any(|b| b.config().parent_name == c.name);
            if missing && let Err(e) = self.deploy_bridge_persisted(&c) {
                tracing::warn!("AMQ222118: unable to start Bridge {}: {e}", c.name);
            }
        }
    }

    /// The bridges part of `deployReloadableConfigFromConfiguration`: a
    /// configured bridge that changed is deployed again, a new one
    /// deployed, one no longer configured destroyed.
    pub(crate) async fn reload_bridges(self: &Arc<Self>, configured: &[BridgeConfig]) {
        tracing::info!("AMQ221056: Reloading configuration: bridges");
        for new in configured {
            let mut new = new.clone();
            new.parent_name = new.name.clone();
            let existing = self
                .bridges
                .all()
                .into_iter()
                .find(|b| b.config().parent_name == new.name);
            match existing {
                Some(b) if b.config().configuration_managed && !b.config().same_as(&new) => {
                    self.destroy_bridge(&new.name).await;
                    if let Err(e) = self.deploy_bridge_persisted(&new) {
                        tracing::warn!("AMQ222118: unable to start Bridge {}: {e}", new.name);
                    }
                }
                Some(_) => {}
                None => {
                    if let Err(e) = self.deploy_bridge_persisted(&new) {
                        tracing::warn!("AMQ222118: unable to start Bridge {}: {e}", new.name);
                    }
                }
            }
        }
        for b in self.bridges.all() {
            let parent = b.config().parent_name.clone();
            if b.config().configuration_managed && !configured.iter().any(|c| c.name == parent) {
                self.destroy_bridge(&parent).await;
            }
        }
        self.recover_stored_bridges();
    }

    /// `ClusterManager.stop`: the bridges, broadcast groups and cluster
    /// connection stop, as when a bridge fails to authenticate.
    pub async fn stop_cluster_manager(&self) {
        if let Some(c) = &self.cluster {
            c.stop();
        }
        for g in &self.broadcast_groups {
            g.stop();
        }
        let bridges = std::mem::take(&mut *self.bridges.deployed());
        for b in bridges {
            let _ = b.stop().await;
        }
    }

    /// The bridges' part of [`Broker::stop`]: each is asked to stop.
    pub(crate) fn stop_bridges(&self) {
        let bridges = std::mem::take(&mut *self.bridges.deployed());
        for b in bridges {
            tokio::spawn(async move {
                let _ = b.stop().await;
            });
        }
    }
}

/// `PersistedBridgeConfiguration` of a bridge configuration
/// (`BridgeConfiguration.encode`).
pub fn to_record(c: &BridgeConfig) -> saranga_storage::codec::PersistedBridgeConfiguration {
    use saranga_storage::codec::{PersistedBridgeConfiguration, TransformerConfiguration};
    let s = |v: &str| Some(saranga_buffer::JString::from(v));
    let o = |v: &Option<String>| v.as_deref().map(saranga_buffer::JString::from);
    PersistedBridgeConfiguration {
        name: s(&c.name),
        parent_name: s(&c.parent_name),
        queue_name: o(&c.queue_name),
        forwarding_address: o(&c.forwarding_address),
        filter_string: o(&c.filter),
        discovery_group_name: o(&c.discovery_group),
        ha: Some(c.ha),
        retry_interval: Some(c.retry_interval),
        retry_interval_multiplier: Some(c.retry_interval_multiplier),
        initial_connect_attempts: Some(c.initial_connect_attempts),
        reconnect_attempts: Some(c.reconnect_attempts),
        reconnect_attempts_on_same_node: Some(c.reconnect_attempts_on_same_node),
        use_duplicate_detection: Some(c.use_duplicate_detection),
        confirmation_window_size: Some(c.confirmation_window_size),
        producer_window_size: Some(c.producer_window_size),
        client_failure_check_period: Some(c.client_failure_check_period),
        user: o(&c.user),
        password: o(&c.password),
        connection_ttl: Some(c.connection_ttl),
        max_retry_interval: Some(c.max_retry_interval),
        min_large_message_size: Some(c.min_large_message_size),
        call_timeout: Some(c.call_timeout),
        concurrency: Some(c.concurrency),
        configuration_managed: Some(c.configuration_managed),
        routing_type: routing_type_byte(c.routing_type),
        transformer: c.transformer.as_ref().map(|t| TransformerConfiguration {
            class_name: t.class_name.as_str().into(),
            properties: t.properties.iter().map(|(k, v)| (s(k), s(v))).collect(),
        }),
        static_connectors: c.static_connectors.iter().flatten().map(|n| s(n)).collect(),
        pending_ack_timeout: Some(Some(c.pending_ack_timeout)),
        client_id: Some(o(&c.client_id)),
    }
}

/// The bridge configuration a `PersistedBridgeConfiguration` records
/// (`BridgeConfiguration.decode`).
pub fn from_record(r: &saranga_storage::codec::PersistedBridgeConfiguration) -> BridgeConfig {
    let text = |v: &Option<saranga_buffer::JString>| v.as_ref().map(|s| s.to_string_lossy());
    let mut c = BridgeConfig::new(&text(&r.name).unwrap_or_default());
    c.parent_name = text(&r.parent_name).unwrap_or_default();
    c.queue_name = text(&r.queue_name);
    c.forwarding_address = text(&r.forwarding_address);
    c.filter = text(&r.filter_string);
    c.discovery_group = text(&r.discovery_group_name);
    c.ha = r.ha.unwrap_or(c.ha);
    c.retry_interval = r.retry_interval.unwrap_or(c.retry_interval);
    c.retry_interval_multiplier = r
        .retry_interval_multiplier
        .unwrap_or(c.retry_interval_multiplier);
    c.initial_connect_attempts = r
        .initial_connect_attempts
        .unwrap_or(c.initial_connect_attempts);
    c.reconnect_attempts = r.reconnect_attempts.unwrap_or(c.reconnect_attempts);
    c.reconnect_attempts_on_same_node = r
        .reconnect_attempts_on_same_node
        .unwrap_or(c.reconnect_attempts_on_same_node);
    c.use_duplicate_detection = r
        .use_duplicate_detection
        .unwrap_or(c.use_duplicate_detection);
    c.confirmation_window_size = r
        .confirmation_window_size
        .unwrap_or(c.confirmation_window_size);
    c.producer_window_size = r.producer_window_size.unwrap_or(c.producer_window_size);
    c.client_failure_check_period = r
        .client_failure_check_period
        .unwrap_or(c.client_failure_check_period);
    c.user = text(&r.user);
    c.password = text(&r.password);
    c.connection_ttl = r.connection_ttl.unwrap_or(c.connection_ttl);
    c.max_retry_interval = r.max_retry_interval.unwrap_or(c.max_retry_interval);
    c.min_large_message_size = r.min_large_message_size.unwrap_or(c.min_large_message_size);
    c.call_timeout = r.call_timeout.unwrap_or(c.call_timeout);
    c.concurrency = r.concurrency.unwrap_or(c.concurrency);
    c.configuration_managed = r.configuration_managed.unwrap_or(c.configuration_managed);
    c.routing_type = routing_type_of_byte(r.routing_type);
    c.transformer = r
        .transformer
        .as_ref()
        .map(|t| saranga_config::TransformerConfig {
            class_name: t.class_name.to_string_lossy(),
            properties: t
                .properties
                .iter()
                .map(|(k, v)| (text(k).unwrap_or_default(), text(v).unwrap_or_default()))
                .collect(),
        });
    // A count of 0 reads back as no connectors at all.
    c.static_connectors = (!r.static_connectors.is_empty()).then(|| {
        r.static_connectors
            .iter()
            .map(|n| text(n).unwrap_or_default())
            .collect()
    });
    if let Some(Some(t)) = r.pending_ack_timeout {
        c.pending_ack_timeout = t;
    }
    if let Some(id) = &r.client_id {
        c.client_id = text(id);
    }
    c
}

/// `ComponentConfigurationRoutingType.getType`'s byte.
fn routing_type_byte(t: ComponentRoutingType) -> u8 {
    match t {
        ComponentRoutingType::Multicast => 0,
        ComponentRoutingType::Anycast => 1,
        ComponentRoutingType::Strip => 2,
        ComponentRoutingType::Pass => 3,
    }
}

/// `ComponentConfigurationRoutingType.getType`.
fn routing_type_of_byte(b: u8) -> ComponentRoutingType {
    match b {
        0 => ComponentRoutingType::Multicast,
        1 => ComponentRoutingType::Anycast,
        2 => ComponentRoutingType::Strip,
        _ => ComponentRoutingType::Pass,
    }
}

fn routing_type_name(t: ComponentRoutingType) -> &'static str {
    match t {
        ComponentRoutingType::Multicast => "MULTICAST",
        ComponentRoutingType::Anycast => "ANYCAST",
        ComponentRoutingType::Strip => "STRIP",
        ComponentRoutingType::Pass => "PASS",
    }
}

/// `BridgeConfiguration.toJSON`.
pub fn to_json(c: &BridgeConfig) -> Json {
    let nullable = |v: &Option<String>| v.clone().map_or(Json::Null, Json::from);
    let mut j = Json::object();
    j.put("name", c.name.as_str());
    if let Some(q) = &c.queue_name {
        j.put("queue-name", q.as_str());
    }
    if let Some(a) = &c.forwarding_address {
        j.put("forwarding-address", a.as_str());
    }
    if let Some(f) = &c.filter {
        j.put("filter-string", f.as_str());
    }
    if let Some(d) = &c.discovery_group {
        j.put("discovery-group-name", d.as_str());
    }
    j.put("user", nullable(&c.user));
    j.put("password", nullable(&c.password));
    j.put("ha", c.ha);
    j.put("retry-interval", c.retry_interval);
    j.put("retry-interval-multiplier", c.retry_interval_multiplier);
    j.put("initial-connect-attempts", c.initial_connect_attempts);
    j.put("reconnect-attempts", c.reconnect_attempts);
    j.put(
        "reconnect-attempts-on-same-node",
        c.reconnect_attempts_on_same_node,
    );
    j.put("use-duplicate-detection", c.use_duplicate_detection);
    j.put("confirmation-window-size", c.confirmation_window_size);
    j.put("producer-window-size", c.producer_window_size);
    j.put("client-failure-check-period", c.client_failure_check_period);
    j.put("connection-ttl", c.connection_ttl);
    j.put("max-retry-interval", c.max_retry_interval);
    j.put("min-large-message-size", c.min_large_message_size);
    j.put("call-timeout", c.call_timeout);
    j.put("concurrency", c.concurrency);
    j.put("configuration-managed", c.configuration_managed);
    j.put("pending-ack-timeout", c.pending_ack_timeout);
    if let Some(id) = &c.client_id {
        j.put("client-id", id.as_str());
    }
    j.put("routing-type", routing_type_name(c.routing_type));
    if let Some(connectors) = &c.static_connectors {
        j.put(
            "static-connectors",
            Json::Array(connectors.iter().map(|n| Json::from(n.as_str())).collect()),
        );
    }
    if let Some(t) = &c.transformer {
        j.put("transformer-configuration", transformer_json(t));
    }
    j
}

/// `TransformerConfiguration.createJsonObjectBuilder`.
fn transformer_json(t: &saranga_config::TransformerConfig) -> Json {
    let mut props = Json::object();
    for (k, v) in &t.properties {
        props.put(k, v.as_str());
    }
    Json::object()
        .with("class-name", t.class_name.as_str())
        .with("properties", props)
}

/// `BridgeConfiguration.fromJSON`: `None` without a name. Each member goes
/// through `set` as text.
pub fn from_json(text: &str) -> Result<Option<BridgeConfig>, String> {
    let json = Json::parse(text).map_err(|e| e.to_string())?;
    let Some(name) = json.get("name") else {
        return Ok(None);
    };
    let mut c = BridgeConfig::new(&member_text(name).unwrap_or_default());
    if let Json::Object(members) = &json {
        for (k, v) in members {
            set(&mut c, k, member_text(v).as_deref())?;
        }
    }
    Ok(Some(c))
}

/// A member as `fromJSON` hands it to `set`: a string's text, `null` as
/// none, anything else as written.
fn member_text(v: &Json) -> Option<String> {
    match v {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// `NumberFormatException`'s message.
fn bad_number(v: Option<&str>) -> String {
    format!("For input string: \"{}\"", v.unwrap_or("null"))
}

/// `BridgeConfiguration.set`.
fn set(c: &mut BridgeConfig, key: &str, value: Option<&str>) -> Result<(), String> {
    let long = |v: Option<&str>| -> Result<i64, String> {
        v.and_then(|v| v.parse().ok()).ok_or_else(|| bad_number(v))
    };
    let int = |v: Option<&str>| -> Result<i32, String> {
        v.and_then(|v| v.parse().ok()).ok_or_else(|| bad_number(v))
    };
    let boolean = |v: Option<&str>| v.is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let owned = |v: Option<&str>| v.map(str::to_owned);
    match key {
        "name" => c.name = value.unwrap_or_default().to_owned(),
        "queue-name" => c.queue_name = owned(value),
        "forwarding-address" => c.forwarding_address = owned(value),
        "filter-string" => c.filter = owned(value),
        "static-connectors" => {
            let list = Json::parse(value.unwrap_or("null")).map_err(|e| e.to_string())?;
            c.static_connectors = Some(match list {
                Json::Array(items) => items.iter().filter_map(member_text).collect(),
                _ => return Err("static-connectors must be a JSON array".to_owned()),
            });
        }
        "discovery-group-name" => c.discovery_group = owned(value),
        "ha" => c.ha = boolean(value),
        "transformer-configuration" => {
            if let Some(v) = value {
                let t = Json::parse(v).map_err(|e| e.to_string())?;
                if let Some(class_name) = t.get("class-name").and_then(member_text) {
                    let properties = match t.get("properties") {
                        Some(Json::Object(p)) => p
                            .iter()
                            .map(|(k, v)| (k.clone(), member_text(v).unwrap_or_default()))
                            .collect(),
                        _ => Vec::new(),
                    };
                    c.transformer = Some(saranga_config::TransformerConfig {
                        class_name,
                        properties,
                    });
                }
            }
        }
        "retry-interval" => c.retry_interval = long(value)?,
        "retry-interval-multiplier" => {
            c.retry_interval_multiplier = value
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| bad_number(value))?;
        }
        "initial-connect-attempts" => c.initial_connect_attempts = int(value)?,
        "reconnect-attempts" => c.reconnect_attempts = int(value)?,
        "reconnect-attempts-on-same-node" => c.reconnect_attempts_on_same_node = int(value)?,
        "use-duplicate-detection" => c.use_duplicate_detection = boolean(value),
        "confirmation-window-size" => c.confirmation_window_size = int(value)?,
        "producer-window-size" => c.producer_window_size = int(value)?,
        "client-failure-check-period" => c.client_failure_check_period = long(value)?,
        "user" => c.user = owned(value),
        "password" => c.password = owned(value),
        "connection-ttl" => c.connection_ttl = long(value)?,
        "max-retry-interval" => c.max_retry_interval = long(value)?,
        "min-large-message-size" => c.min_large_message_size = int(value)?,
        "call-timeout" => c.call_timeout = long(value)?,
        "routing-type" => {
            let v = value.unwrap_or("null");
            c.routing_type = v.parse().map_err(|_| {
                format!(
                    "No enum constant org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType.{v}"
                )
            })?;
        }
        "concurrency" => c.concurrency = int(value)?,
        "pending-ack-timeout" => c.pending_ack_timeout = long(value)?,
        "client-id" => c.client_id = owned(value),
        _ => {}
    }
    Ok(())
}

/// `bridgeOperationCannotBeExecuted`.
fn cannot_be(name: &str, op: &str, state: State) -> BrokerError {
    BrokerError::new(
        ErrorType::IllegalState,
        format!(
            "AMQ229255: Bridge {name} cannot be {op}. Current state: {}",
            state.name()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_bytes_are_the_node_then_the_message() {
        let uuid = uuid_bytes("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0");
        assert_eq!(
            uuid,
            [
                0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2,
                0xe1, 0xf0
            ]
        );
        let b = duplicate_bytes(&uuid, 258);
        assert_eq!(b.len(), 24);
        assert_eq!(&b[16..], &[0, 0, 0, 0, 0, 0, 1, 2]);
    }
}
