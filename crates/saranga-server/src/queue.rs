//! Queues. Each queue is an actor task that alone owns its message list,
//! its consumers and their delivering lists (ADR 0001). Callers talk to it
//! through [`Queue`]; counters that queries read are atomics, updated in
//! the order the caller's operations happen.
//!
//! Ported from the delivery and acknowledgement paths of `QueueImpl`,
//! `ServerConsumerImpl`, `QueueConsumersImpl` and `RefsOperation`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};

use saranga_buffer::PropertyValue;
use saranga_message::CoreMessage;
use tokio::sync::{mpsc, oneshot};

use crate::message::{MessageRef, ServerMessage};
use crate::{IdGenerator, Name, RoutingType};

/// `ClientConsumerImpl.FORCED_DELIVERY_MESSAGE`.
pub const FORCED_DELIVERY_MESSAGE: &str = "_hornetq.forced.delivery.seq";

/// A broker-wide unique consumer key (the session-local consumer ID is
/// only unique within its session).
pub type ConsumerId = u64;

/// A queue or consumer filter (`Filter`).
pub trait MessageFilter: Send + Sync + fmt::Debug {
    fn matches(&self, message: &CoreMessage) -> bool;
    fn text(&self) -> &str;
}

pub type FilterRef = Arc<dyn MessageFilter>;

/// Where a consumer's deliveries go: the protocol side of a consumer
/// (`SessionCallback.sendMessage`).
pub trait DeliverySink: Send + Sync {
    /// Sends one delivery and returns its size in flow-control credits
    /// (for Core, the size of the whole `SESS_RECEIVE_MSG` packet).
    fn deliver(&self, consumer_id: i64, message: &ServerMessage, delivery_count: i32) -> i64;
}

/// Everything about a queue that is fixed when it is created
/// (`QueueConfiguration` with every default applied).
#[derive(Debug, Clone, PartialEq)]
pub struct QueueConfig {
    pub name: Name,
    pub address: Name,
    pub routing_type: RoutingType,
    pub filter_string: Option<String>,
    pub durable: bool,
    pub temporary: bool,
    pub auto_created: bool,
    pub max_consumers: i32,
    pub purge_on_no_consumers: bool,
    pub exclusive: bool,
    pub last_value: bool,
    pub last_value_key: Option<String>,
    pub non_destructive: bool,
    pub consumers_before_dispatch: i32,
    pub delay_before_dispatch: i64,
    pub group_rebalance: bool,
    pub group_rebalance_pause_dispatch: bool,
    pub group_buckets: i32,
    pub group_first_key: Option<String>,
    pub auto_delete: bool,
    pub auto_delete_delay: i64,
    pub auto_delete_message_count: i64,
    pub ring_size: i64,
    pub enabled: bool,
    pub configuration_managed: bool,
    pub user: Option<String>,
}

/// A consumer as the queue sees it.
pub struct ConsumerRegistration {
    pub id: ConsumerId,
    /// The protocol's own consumer ID, passed back to the sink.
    pub client_id: i64,
    pub sink: Arc<dyn DeliverySink>,
    pub filter: Option<FilterRef>,
    pub priority: i32,
    pub browse_only: bool,
    pub pre_acknowledge: bool,
    pub started: bool,
}

/// An acknowledgement that named a message the consumer was not
/// delivering (`consumerNoReference`). `refs` are those taken off the
/// delivering list before the failure.
#[derive(Debug)]
pub struct AckError {
    pub refs: Vec<MessageRef>,
}

enum Cmd {
    Add(MessageRef),
    Reinsert(Vec<MessageRef>),
    AddConsumer(Box<ConsumerRegistration>),
    RemoveConsumer {
        id: ConsumerId,
        failed: bool,
        done: oneshot::Sender<()>,
    },
    Credits {
        id: ConsumerId,
        credits: i32,
    },
    SetStarted {
        id: ConsumerId,
        started: bool,
        done: oneshot::Sender<()>,
    },
    Ack {
        id: ConsumerId,
        message_id: i64,
        individual: bool,
        transactional: bool,
        reply: oneshot::Sender<Result<Vec<MessageRef>, AckError>>,
    },
    CancelDelivering {
        id: ConsumerId,
        last_as_delivered: bool,
        failed: bool,
        reply: oneshot::Sender<Vec<MessageRef>>,
    },
    ForceDelivery {
        id: ConsumerId,
        sequence: i64,
    },
    /// Takes a queued reference out for a recovered prepared transaction;
    /// it stays counted.
    Remove {
        message_id: i64,
        reply: oneshot::Sender<Option<MessageRef>>,
    },
    Shutdown,
}

struct Shared {
    id: i64,
    config: QueueConfig,
    filter: Option<FilterRef>,
    ids: Arc<IdGenerator>,
    tx: mpsc::UnboundedSender<Cmd>,
    /// `getMessageCount`: queued, delivering and acked-in-transaction.
    message_count: AtomicI64,
    consumer_count: AtomicI32,
    deleted: AtomicBool,
}

/// A handle to a queue actor.
#[derive(Clone)]
pub struct Queue(Arc<Shared>);

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Queue({})", self.0.config.name)
    }
}

impl PartialEq for Queue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Queue {}

impl Queue {
    /// Creates the queue and spawns its actor on the current runtime.
    pub fn spawn(
        id: i64,
        config: QueueConfig,
        filter: Option<FilterRef>,
        ids: Arc<IdGenerator>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let queue = Self(Arc::new(Shared {
            id,
            config,
            filter,
            ids,
            tx,
            message_count: AtomicI64::new(0),
            consumer_count: AtomicI32::new(0),
            deleted: AtomicBool::new(false),
        }));
        let actor = Actor {
            shared: queue.0.clone(),
            messages: MessageList::default(),
            consumers: Vec::new(),
            browsers: Vec::new(),
            round_robin: HashMap::new(),
        };
        tokio::spawn(actor.run(rx));
        queue
    }

    pub fn id(&self) -> i64 {
        self.0.id
    }

    pub fn config(&self) -> &QueueConfig {
        &self.0.config
    }

    pub fn name(&self) -> &Name {
        &self.0.config.name
    }

    pub fn filter(&self) -> Option<&FilterRef> {
        self.0.filter.as_ref()
    }

    pub fn message_count(&self) -> i64 {
        self.0.message_count.load(Ordering::SeqCst)
    }

    pub fn consumer_count(&self) -> i32 {
        self.0.consumer_count.load(Ordering::SeqCst)
    }

    pub fn is_deleted(&self) -> bool {
        self.0.deleted.load(Ordering::SeqCst)
    }

    /// `QueueImpl.isDurable`: its persistent messages are journaled.
    pub fn is_durable(&self) -> bool {
        self.0.config.durable && !self.0.config.temporary
    }

    /// Whether this reference is journaled: a persistent message on a
    /// durable queue.
    pub fn is_durable_ref(&self, r: &MessageRef) -> bool {
        r.message.is_durable() && self.is_durable()
    }

    pub fn ids(&self) -> &Arc<IdGenerator> {
        &self.0.ids
    }

    /// `postAcknowledge`: once no durable queue refers to the message, its
    /// record is deleted (not synced; recovery deletes what a crash missed).
    pub fn post_acknowledge(&self, r: &MessageRef) {
        if self.is_durable_ref(r)
            && r.message.durable_down() == 0
            && let Some(s) = self.0.ids.storage()
            && let Err(e) = s.delete_message(r.message_id())
        {
            tracing::warn!(
                "AMQ222178: Cannot find message {} on the journal: {e}",
                r.message_id()
            );
        }
    }

    /// Takes a queued reference out by message ID (`removeReferenceWithID`).
    pub async fn remove_reference(&self, message_id: i64) -> Option<MessageRef> {
        let (reply, wait) = oneshot::channel();
        self.send(Cmd::Remove { message_id, reply });
        wait.await.ok().flatten()
    }

    fn send(&self, cmd: Cmd) {
        // A send only fails once the actor has stopped, when the queue
        // is deleted; its messages go with it.
        let _ = self.0.tx.send(cmd);
    }

    /// Adds a routed message to the tail (`addTail`).
    pub fn add(&self, r: MessageRef) {
        self.0.message_count.fetch_add(1, Ordering::SeqCst);
        self.send(Cmd::Add(r));
    }

    /// Returns cancelled or rolled-back references, ordered by message ID
    /// (`postRollback` / `addSorted`). They are still counted.
    pub fn reinsert(&self, refs: Vec<MessageRef>) {
        if !refs.is_empty() {
            self.send(Cmd::Reinsert(refs));
        }
    }

    /// Records that `n` references acknowledged in a transaction were
    /// committed.
    pub fn acked(&self, n: usize) {
        self.0.message_count.fetch_sub(n as i64, Ordering::SeqCst);
    }

    pub fn add_consumer(&self, reg: ConsumerRegistration) {
        self.0.consumer_count.fetch_add(1, Ordering::SeqCst);
        self.send(Cmd::AddConsumer(Box::new(reg)));
    }

    /// Removes a consumer and returns its delivering messages to the queue
    /// (`ServerConsumerImpl.close`).
    pub async fn remove_consumer(&self, id: ConsumerId, failed: bool) {
        let (done, wait) = oneshot::channel();
        self.send(Cmd::RemoveConsumer { id, failed, done });
        let _ = wait.await;
        self.0.consumer_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn credits(&self, id: ConsumerId, credits: i32) {
        self.send(Cmd::Credits { id, credits });
    }

    /// Starts or stops a consumer. Returns once every delivery made before
    /// the change has been handed to the consumer's sink.
    pub async fn set_started(&self, id: ConsumerId, started: bool) {
        let (done, wait) = oneshot::channel();
        self.send(Cmd::SetStarted { id, started, done });
        let _ = wait.await;
    }

    /// Acknowledges up to and including `message_id` (cumulative), or just
    /// that message (`individual`), returning the references. Outside a
    /// transaction they are gone from the queue; inside one the transaction
    /// holds them and they stay counted until it completes.
    pub async fn ack(
        &self,
        id: ConsumerId,
        message_id: i64,
        individual: bool,
        transactional: bool,
    ) -> Result<Vec<MessageRef>, AckError> {
        let (reply, wait) = oneshot::channel();
        self.send(Cmd::Ack {
            id,
            message_id,
            individual,
            transactional,
            reply,
        });
        wait.await.unwrap_or(Ok(Vec::new()))
    }

    /// Stops a consumer and takes back everything it is delivering, for a
    /// rollback (`ServerConsumerImpl.cancelRefs`). The caller returns the
    /// references with [`Queue::reinsert`].
    pub async fn cancel_delivering(
        &self,
        id: ConsumerId,
        last_as_delivered: bool,
        failed: bool,
    ) -> Vec<MessageRef> {
        let (reply, wait) = oneshot::channel();
        self.send(Cmd::CancelDelivering {
            id,
            last_as_delivered,
            failed,
            reply,
        });
        wait.await.unwrap_or_default()
    }

    pub fn force_delivery(&self, id: ConsumerId, sequence: i64) {
        self.send(Cmd::ForceDelivery { id, sequence });
    }

    /// Stops the actor. Its messages are dropped.
    pub fn shutdown(&self) {
        self.0.deleted.store(true, Ordering::SeqCst);
        self.send(Cmd::Shutdown);
    }
}

/// The message list, one FIFO per priority (`PriorityLinkedListImpl`),
/// iterated from the highest priority down.
#[derive(Default)]
struct MessageList {
    levels: [VecDeque<MessageRef>; 10],
}

impl MessageList {
    fn push_back(&mut self, r: MessageRef) {
        self.levels[r.message.priority_level()].push_back(r);
    }

    /// `addSorted`: inserts in message ID order within its priority.
    fn insert_sorted(&mut self, r: MessageRef) {
        let level = &mut self.levels[r.message.priority_level()];
        let pos = level
            .iter()
            .position(|m| m.message_id() > r.message_id())
            .unwrap_or(level.len());
        level.insert(pos, r);
    }

    fn find(&self, mut pred: impl FnMut(&MessageRef) -> bool) -> Option<(usize, usize)> {
        (0..10)
            .rev()
            .find_map(|l| self.levels[l].iter().position(&mut pred).map(|i| (l, i)))
    }

    fn remove(&mut self, (level, index): (usize, usize)) -> MessageRef {
        self.levels[level]
            .remove(index)
            .expect("position from find")
    }

    fn iter(&self) -> impl Iterator<Item = &MessageRef> {
        self.levels.iter().rev().flatten()
    }

    fn len(&self) -> usize {
        self.levels.iter().map(VecDeque::len).sum()
    }

    fn clear(&mut self) {
        self.levels.iter_mut().for_each(VecDeque::clear);
    }
}

struct Consumer {
    reg: ConsumerRegistration,
    /// `availableCredits`; `None` when flow control is off.
    credits: Option<i64>,
    started: bool,
    delivering: VecDeque<MessageRef>,
    /// Messages already sent to a browser.
    browsed: HashSet<i64>,
}

impl Consumer {
    fn accepts(&self, r: &MessageRef) -> bool {
        self.reg
            .filter
            .as_ref()
            .is_none_or(|f| f.matches(r.message.core()))
    }

    fn can_take(&self) -> bool {
        self.started && self.credits.is_none_or(|c| c > 0)
    }
}

struct Actor {
    shared: Arc<Shared>,
    messages: MessageList,
    /// Ordered by priority, highest first; equal priorities keep their
    /// arrival order.
    consumers: Vec<Consumer>,
    browsers: Vec<Consumer>,
    /// Next consumer to try within each priority group.
    round_robin: HashMap<i32, usize>,
}

impl Actor {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        while let Some(cmd) = rx.recv().await {
            if !self.handle(cmd) {
                break;
            }
            // Take whatever else is waiting before delivering, so a burst
            // of sends is delivered in one pass.
            let mut running = true;
            while let Ok(cmd) = rx.try_recv() {
                if !self.handle(cmd) {
                    running = false;
                    break;
                }
            }
            if !running {
                break;
            }
            self.deliver();
        }
    }

    /// Handles one command; returns false to stop the actor.
    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::Add(r) => self.messages.push_back(r),
            Cmd::Reinsert(refs) => {
                if self.shared.config.purge_on_no_consumers && self.consumers.is_empty() {
                    self.count_removed(refs.len());
                } else {
                    refs.into_iter()
                        .for_each(|r| self.messages.insert_sorted(r));
                }
            }
            Cmd::AddConsumer(reg) => {
                let consumer = Consumer {
                    credits: Some(0),
                    started: reg.started || reg.browse_only,
                    delivering: VecDeque::new(),
                    browsed: HashSet::new(),
                    reg: *reg,
                };
                if consumer.reg.browse_only {
                    self.browsers.push(consumer);
                } else {
                    let at = self
                        .consumers
                        .iter()
                        .position(|c| c.reg.priority < consumer.reg.priority)
                        .unwrap_or(self.consumers.len());
                    self.consumers.insert(at, consumer);
                }
            }
            Cmd::RemoveConsumer { id, failed, done } => {
                if let Some(c) = self.take_consumer(id) {
                    let refs = cancel(c.delivering, false, failed);
                    refs.into_iter()
                        .for_each(|r| self.messages.insert_sorted(r));
                }
                if self.shared.config.purge_on_no_consumers && self.consumers.is_empty() {
                    self.count_removed(self.messages.len());
                    self.messages.clear();
                }
                let _ = done.send(());
            }
            Cmd::Credits { id, credits } => {
                if let Some(c) = self.consumer_mut(id) {
                    match credits {
                        -1 => c.credits = None,
                        0 => c.credits = Some(0),
                        n => {
                            if let Some(c) = &mut c.credits {
                                *c += i64::from(n);
                            }
                        }
                    }
                }
            }
            Cmd::SetStarted { id, started, done } => {
                if let Some(c) = self.consumer_mut(id) {
                    c.started = started || c.reg.browse_only;
                }
                let _ = done.send(());
            }
            Cmd::Ack {
                id,
                message_id,
                individual,
                transactional,
                reply,
            } => {
                let result = self.ack(id, message_id, individual, transactional);
                let _ = reply.send(result);
            }
            Cmd::CancelDelivering {
                id,
                last_as_delivered,
                failed,
                reply,
            } => {
                let refs = match self.consumer_mut(id) {
                    Some(c) if !c.reg.browse_only => {
                        c.started = false;
                        cancel(std::mem::take(&mut c.delivering), last_as_delivered, failed)
                    }
                    _ => Vec::new(),
                };
                let _ = reply.send(refs);
            }
            Cmd::ForceDelivery { id, sequence } => {
                self.deliver();
                self.force_delivery(id, sequence);
            }
            Cmd::Remove { message_id, reply } => {
                let r = self
                    .messages
                    .find(|r| r.message_id() == message_id)
                    .map(|at| self.messages.remove(at));
                let _ = reply.send(r);
            }
            Cmd::Shutdown => {
                self.delete_all_references();
                return false;
            }
        }
        true
    }

    fn consumer_mut(&mut self, id: ConsumerId) -> Option<&mut Consumer> {
        self.consumers
            .iter_mut()
            .chain(self.browsers.iter_mut())
            .find(|c| c.reg.id == id)
    }

    fn take_consumer(&mut self, id: ConsumerId) -> Option<Consumer> {
        if let Some(i) = self.consumers.iter().position(|c| c.reg.id == id) {
            return Some(self.consumers.remove(i));
        }
        let i = self.browsers.iter().position(|c| c.reg.id == id)?;
        Some(self.browsers.remove(i))
    }

    fn count_removed(&self, n: usize) {
        self.shared
            .message_count
            .fetch_sub(n as i64, Ordering::SeqCst);
    }

    fn ack(
        &mut self,
        id: ConsumerId,
        message_id: i64,
        individual: bool,
        transactional: bool,
    ) -> Result<Vec<MessageRef>, AckError> {
        let Some(c) = self.consumer_mut(id) else {
            return Err(AckError { refs: Vec::new() });
        };
        if c.reg.browse_only {
            return Ok(Vec::new());
        }
        let mut refs = Vec::new();
        if individual {
            match c
                .delivering
                .iter()
                .position(|r| r.message_id() == message_id)
            {
                Some(i) => refs.extend(c.delivering.remove(i)),
                None => return Err(AckError { refs }),
            }
        } else {
            loop {
                let Some(r) = c.delivering.pop_front() else {
                    return Err(AckError { refs });
                };
                let done = r.message_id() == message_id;
                refs.push(r);
                if done {
                    break;
                }
            }
        }
        if !transactional {
            self.count_removed(refs.len());
        }
        Ok(refs)
    }

    /// `QueueImpl.deliver`: hands messages to consumers until none can take
    /// another. Each message goes to the next consumer, round robin, in the
    /// highest priority group that has one able to take it.
    fn deliver(&mut self) {
        while self.messages.len() > 0 {
            let mut handled = false;
            let mut start = 0;
            while start < self.consumers.len() && !handled {
                let priority = self.consumers[start].reg.priority;
                let n = self.consumers[start..]
                    .iter()
                    .take_while(|c| c.reg.priority == priority)
                    .count();
                let next = self.round_robin.get(&priority).copied().unwrap_or(0) % n;
                for i in 0..n {
                    if self.try_deliver(start + (next + i) % n) {
                        self.round_robin.insert(priority, (next + i + 1) % n);
                        handled = true;
                        break;
                    }
                }
                start += n;
            }
            if !handled {
                break;
            }
        }
        self.deliver_to_browsers();
    }

    fn try_deliver(&mut self, index: usize) -> bool {
        let c = &self.consumers[index];
        if !c.can_take() {
            return false;
        }
        let Some(at) = self.messages.find(|r| c.accepts(r)) else {
            return false;
        };
        let mut r = self.messages.remove(at);
        let c = &mut self.consumers[index];
        r.delivery_count += 1;
        let size = c
            .reg
            .sink
            .deliver(c.reg.client_id, &r.message, r.delivery_count);
        if let Some(credits) = &mut c.credits {
            *credits -= size;
        }
        if c.reg.pre_acknowledge {
            self.count_removed(1);
            self.pre_acknowledge(&r);
        } else {
            c.delivering.push_back(r);
        }
        true
    }

    /// A pre-acknowledged delivery is acknowledged at once (`acknowledge`
    /// from `ServerConsumerImpl.proceedDeliver`).
    fn pre_acknowledge(&self, r: &MessageRef) {
        let queue = Queue(self.shared.clone());
        if queue.is_durable_ref(r)
            && let Some(s) = self.shared.ids.storage()
            && let Err(e) = s.store_acknowledge(self.shared.id, r.message_id())
        {
            tracing::warn!(
                "AMQ222178: Cannot find message {} on the journal: {e}",
                r.message_id()
            );
        }
        queue.post_acknowledge(r);
    }

    /// `deleteAllReferences`: the deleted queue's durable messages are
    /// acknowledged so their records go.
    fn delete_all_references(&mut self) {
        let queue = Queue(self.shared.clone());
        let storage = self.shared.ids.storage().cloned();
        let mut refs: Vec<MessageRef> = Vec::new();
        for level in &mut self.messages.levels {
            refs.extend(level.drain(..));
        }
        for c in self.consumers.iter_mut() {
            refs.extend(c.delivering.drain(..));
        }
        for r in &refs {
            if queue.is_durable_ref(r)
                && let Some(s) = &storage
            {
                let _ = s.store_acknowledge(self.shared.id, r.message_id());
            }
            queue.post_acknowledge(r);
        }
    }

    /// Browsers see every message without taking it (`BrowserDeliverer`).
    fn deliver_to_browsers(&mut self) {
        for b in &mut self.browsers {
            while b.can_take() {
                let Some(r) = self
                    .messages
                    .iter()
                    .find(|r| !b.browsed.contains(&r.message_id()) && b.accepts(r))
                else {
                    break;
                };
                b.browsed.insert(r.message_id());
                let size = b
                    .reg
                    .sink
                    .deliver(b.reg.client_id, &r.message, r.delivery_count);
                if let Some(credits) = &mut b.credits {
                    *credits -= size;
                }
            }
        }
    }

    /// `ServerConsumerImpl.forceDelivery`: after any pending delivery, sends
    /// a marker message carrying the sequence so the client knows the queue
    /// had nothing more for it.
    fn force_delivery(&mut self, id: ConsumerId, sequence: i64) {
        let address = self.shared.config.name.clone();
        let message_id = self.shared.ids.next();
        let Some(c) = self.consumer_mut(id) else {
            return;
        };
        let mut core = CoreMessage {
            message_id,
            address: Some(address.as_ref().into()),
            ..Default::default()
        };
        core.properties.insert(
            FORCED_DELIVERY_MESSAGE.into(),
            PropertyValue::Long(sequence),
        );
        c.reg
            .sink
            .deliver(c.reg.client_id, &ServerMessage::new(core), 0);
    }
}

/// Takes a consumer's delivering references back (`cancelRefs`). With
/// `last_as_delivered` the first one keeps its delivery count; the rest
/// had not reached the application, so their count is undone unless the
/// client failed.
fn cancel(
    delivering: VecDeque<MessageRef>,
    last_as_delivered: bool,
    failed: bool,
) -> Vec<MessageRef> {
    let mut keep_first = last_as_delivered;
    delivering
        .into_iter()
        .map(|mut r| {
            if keep_first {
                keep_first = false;
            } else if !failed {
                r.delivery_count -= 1;
            }
            r
        })
        .collect()
}
