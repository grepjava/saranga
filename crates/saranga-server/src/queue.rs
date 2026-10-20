//! Queues. Each queue is an actor task that alone owns its message list,
//! its consumers and their delivering lists (ADR 0001). Callers talk to it
//! through [`Queue`]; counters that queries read are atomics, updated in
//! the order the caller's operations happen.
//!
//! Ported from the delivery and acknowledgement paths of `QueueImpl`,
//! `ServerConsumerImpl`, `QueueConsumersImpl`, `RefsOperation` and
//! `ScheduledDeliveryHandlerImpl`, with redelivery, dead-letter and expiry
//! handling (`checkRedelivery`, `sendToDeadLetterAddress`, `expire`, `move`).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use saranga_buffer::PropertyValue;
use saranga_message::CoreMessage;
use tokio::sync::{mpsc, oneshot};

use crate::large::{LargeBody, LargeReader};
use crate::message::{self, MessageRef, ServerMessage, now_millis};
use crate::paging::{
    PageIterator, PageSubscription, REF_MEMORY_ESTIMATE, TxPaging, memory_estimate,
};
use crate::postoffice::{PostOffice, QueueSpec};
use crate::settings::AddressSettings;
use crate::storage::{Pending, StoreResult};
use crate::{IdGenerator, Name, RoutingType};

#[path = "queue_control.rs"]
mod control;

pub use control::{ConsumerInfo, QueueStats, Selection, SelectionAction};

/// `ClientConsumerImpl.FORCED_DELIVERY_MESSAGE`.
pub const FORCED_DELIVERY_MESSAGE: &str = "_hornetq.forced.delivery.seq";

/// A broker-wide unique consumer key (the session-local consumer ID is
/// only unique within its session).
pub type ConsumerId = u64;

/// A queue or consumer filter (`Filter`).
pub trait MessageFilter: Send + Sync + fmt::Debug {
    fn matches(&self, message: &dyn saranga_filter::Filterable) -> bool;
    fn text(&self) -> &str;
}

pub type FilterRef = Arc<dyn MessageFilter>;

/// Where a consumer's deliveries go: the protocol side of a consumer
/// (`SessionCallback.sendMessage`).
pub trait DeliverySink: Send + Sync {
    /// Sends one delivery and returns its size in flow-control credits
    /// (for Core, the size of the whole `SESS_RECEIVE_MSG` packet).
    fn deliver(&self, consumer_id: i64, message: &Arc<ServerMessage>, delivery_count: i32) -> i64;

    /// Starts a large message's delivery: its headers and body size
    /// (`sendLargeMessage`). Returns the credits used.
    fn deliver_large(
        &self,
        consumer_id: i64,
        message: &ServerMessage,
        body_size: i64,
        delivery_count: i32,
    ) -> i64;

    /// Sends the next chunk of a large message's body
    /// (`sendLargeMessageContinuation`). Returns the credits used.
    fn deliver_continuation(&self, consumer_id: i64, chunk: Vec<u8>, continues: bool) -> i64;

    /// The largest body chunk (the session's `minLargeMessageSize`).
    fn chunk_size(&self) -> usize;

    /// `supportLargeMessage`: whether large messages go out in chunks.
    /// Without it (every protocol but Core) a large message is delivered
    /// whole, its body read from its file.
    fn supports_large(&self) -> bool {
        true
    }

    /// `SessionCallback.hasCredits(consumer, ref)`: whether this message
    /// may go now. A consumer that says no gets nothing more until
    /// [`Queue::prompt_delivery`].
    fn has_credits(&self, _message: &ServerMessage) -> bool {
        true
    }

    /// `ServerConsumer.disconnect`: management removed the consumer's
    /// queue, and the client is told its consumer is gone.
    fn disconnect(&self, _consumer_id: i64, _reason: &str) {}

    /// Whether the broker's plugins let this consumer have the message
    /// (`callBrokerMessagePluginsCanAccept`).
    fn accepts(&self, _message: &ServerMessage) -> bool {
        true
    }
}

/// The most of a large message's body sent in one pass of the queue, so
/// that a consumer without flow control does not hold the queue up.
const LARGE_BYTES_PER_PASS: usize = 1024 * 1024;

/// `MAX_DEPAGE_NUM`: paged messages a queue reads in when neither
/// `max-read-page-messages` nor `max-read-page-bytes` limits it.
const MAX_DEPAGE_NUM: i64 = 1000;

/// A queue's settings (`QueueConfiguration` with every default applied).
/// Name, address, durability and the kind of queue (last-value,
/// temporary) are fixed when it is created; `updateQueue` may change the
/// rest.
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
    /// `internal`: a queue the broker keeps for itself (`$sys.mqtt.sessions`).
    pub internal: bool,
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
    /// What management reports of the consumer's deliveries and acks.
    pub metrics: Arc<ConsumerMetrics>,
}

/// `ServerConsumerImpl.ServerConsumerMetrics`.
#[derive(Debug, Default)]
pub struct ConsumerMetrics {
    in_transit_size: AtomicI64,
    delivered: AtomicI64,
    delivered_size: AtomicI64,
    acknowledged: AtomicI64,
    awaiting_commit: AtomicI32,
    last_delivered: AtomicI64,
    last_acknowledged: AtomicI64,
}

impl ConsumerMetrics {
    /// `addMessage`: a delivery of `size` bytes.
    pub fn add_message(&self, size: i64) {
        self.in_transit_size.fetch_add(size, Ordering::Relaxed);
        self.delivered_size.fetch_add(size, Ordering::Relaxed);
        self.delivered.fetch_add(1, Ordering::Relaxed);
        self.last_delivered.store(now_millis(), Ordering::Relaxed);
    }

    /// `addAcknowledge`: an ack of `size` bytes, in a transaction or not.
    pub fn add_acknowledge(&self, size: i64, transactional: bool) {
        self.in_transit_size.fetch_sub(size, Ordering::Relaxed);
        self.acknowledged.fetch_add(1, Ordering::Relaxed);
        self.last_acknowledged
            .store(now_millis(), Ordering::Relaxed);
        if transactional {
            self.awaiting_commit.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `afterCommit` / `afterRollback` of the transaction holding the acks.
    pub fn transaction_ended(&self) {
        self.awaiting_commit.store(0, Ordering::Relaxed);
    }

    pub fn in_transit_size(&self) -> i64 {
        self.in_transit_size.load(Ordering::Relaxed)
    }

    pub fn delivered(&self) -> i64 {
        self.delivered.load(Ordering::Relaxed)
    }

    pub fn delivered_size(&self) -> i64 {
        self.delivered_size.load(Ordering::Relaxed)
    }

    pub fn acknowledged(&self) -> i64 {
        self.acknowledged.load(Ordering::Relaxed)
    }

    pub fn awaiting_commit(&self) -> i32 {
        self.awaiting_commit.load(Ordering::Relaxed)
    }

    pub fn last_delivered(&self) -> i64 {
        self.last_delivered.load(Ordering::Relaxed)
    }

    pub fn last_acknowledged(&self) -> i64 {
        self.last_acknowledged.load(Ordering::Relaxed)
    }
}

/// An acknowledgement that named a message the consumer was not
/// delivering (`consumerNoReference`). `refs` are those taken off the
/// delivering list before the failure.
#[derive(Debug)]
pub struct AckError {
    pub refs: Vec<MessageRef>,
}

/// What an acknowledgement took off the delivering list, or why it failed.
pub type AckResult = Result<Vec<MessageRef>, AckError>;

/// Where an acknowledgement's answer goes: to a caller waiting for it,
/// or to a function the actor calls, which must not block.
pub enum AckReply {
    Wait(oneshot::Sender<AckResult>),
    Then(Box<dyn FnOnce(AckResult) + Send>),
}

impl AckReply {
    fn send(self, result: AckResult) {
        match self {
            Self::Wait(reply) => {
                let _ = reply.send(result);
            }
            Self::Then(then) => then(result),
        }
    }
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
        reply: AckReply,
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
    /// The client found a delivered message expired (`SESS_EXPIRED`).
    Expire {
        id: ConsumerId,
        message_id: i64,
    },
    /// The expiry reaper's pass (`expireReferences`).
    ExpireScan,
    /// Sends more of the large messages being delivered.
    ResumeLarge,
    /// The queue's pages have something new: read it in.
    Depage,
    /// `updateQueue` replaced the configuration; a new `group-buckets`
    /// starts the groups afresh (`setGroupBuckets`).
    Reconfigure {
        reset_groups: bool,
    },
    /// `promptDelivery`: try delivering again.
    Prompt,
    /// `addRedistributor`.
    AddRedistributor {
        delay: i64,
    },
    /// `cancelRedistributor`.
    CancelRedistributor,
    /// `deleteMatchingReferences`: queued messages the filter accepts (all
    /// with none) are acknowledged.
    DeleteMatching {
        filter: Option<FilterRef>,
        reply: oneshot::Sender<usize>,
    },
    /// The queued messages, for browsing (`iterator`).
    Browse {
        reply: oneshot::Sender<Vec<MessageRef>>,
    },
    /// `individualCancel`: one delivering message goes back.
    CancelOne {
        id: ConsumerId,
        message_id: i64,
        failed: bool,
        /// `rejectConsumer`: never to this consumer again.
        undeliverable: bool,
    },
    /// `ServerConsumerImpl.reject`: a delivering message goes to the
    /// dead-letter address.
    Reject {
        id: ConsumerId,
        message_id: i64,
        /// Properties the dead-letter copy gets.
        properties: Vec<(String, PropertyValue)>,
    },
    /// `backToDelivering`: a reference a rolled back transaction had
    /// acknowledged stays with the consumer as delivered.
    BackToDelivering {
        id: ConsumerId,
        r: MessageRef,
    },
    /// A management operation, run by the actor
    /// ([`control`](self::control)).
    Manage(Box<dyn FnOnce(&mut Actor) + Send>),
    /// The queue is deleted: its messages go with it. The callback is told
    /// whether journaled references were among them.
    Shutdown(Option<Box<dyn FnOnce(bool) + Send>>),
    /// The broker stops: the actor ends, its messages left as stored.
    Stop,
}

/// Why a reference left its queue (`AckReason`), for the queue's
/// statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckReason {
    Normal,
    Killed,
    Expired,
    Replaced,
}

struct Shared {
    id: i64,
    name: Name,
    /// Replaced by `updateQueue`.
    config: RwLock<Arc<QueueConfig>>,
    /// The filter routing applies, with its text. `updateQueue` replaces
    /// it without touching `config.filter_string`, which queue queries
    /// report: Artemis's `setFilter` leaves the queue configuration alone.
    filter: RwLock<(Option<String>, Option<FilterRef>)>,
    ids: Arc<IdGenerator>,
    /// For address settings and for moving messages to the dead-letter and
    /// expiry addresses.
    post_office: Weak<PostOffice>,
    tx: mpsc::UnboundedSender<Cmd>,
    /// `getMessageCount`: queued, delivering and acked-in-transaction,
    /// paged messages aside.
    message_count: AtomicI64,
    /// The queue's cursor on its address's pages (`pageSubscription`).
    paging: Option<Arc<PageSubscription>>,
    consumer_count: AtomicI32,
    /// Each consumer's filter, for routing (`hasMatchingConsumer`).
    consumer_filters: RwLock<Vec<(ConsumerId, Option<FilterRef>)>>,
    deleted: AtomicBool,
    /// When the queue was created, in epoch milliseconds.
    created_at: i64,
    /// When a consumer last left (`consumerRemovedTimestamp`); -1 never.
    consumer_removed_at: AtomicI64,
    /// Whether a message ever left it: acknowledged, expired or killed
    /// (`queueWasUsed`).
    used: AtomicBool,
    /// Found deletable by the last reaper pass (`isSwept`).
    swept: AtomicBool,
    /// Routed references sent to the actor and not yet at its tail: the
    /// page is not read while there are any, so a paged message is not
    /// delivered before one routed ahead of it (Artemis adds to the tail
    /// as it routes).
    adds_pending: AtomicI64,
    /// `messagesAdded`, `messagesAcknowledged`, `messagesExpired`,
    /// `messagesKilled` and `messagesReplaced`.
    messages_added: AtomicI64,
    messages_acknowledged: AtomicI64,
    messages_expired: AtomicI64,
    messages_killed: AtomicI64,
    messages_replaced: AtomicI64,
    /// `paused`: nothing is delivered (browsers aside).
    paused: AtomicBool,
    /// The queue's address is paused (`AddressInfo.isPaused`).
    address_paused: AtomicBool,
    /// The `QUEUE_STATUS_RECORD` of a persisted pause; -1 for none.
    pause_record: AtomicI64,
    /// The actor at work, for the critical analyzer.
    critical: crate::critical::Measure,
}

impl Shared {
    fn config(&self) -> Arc<QueueConfig> {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// A handle to a queue actor.
#[derive(Clone)]
pub struct Queue(Arc<Shared>);

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Queue({})", self.0.name)
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
        post_office: Weak<PostOffice>,
        paging: Option<Arc<PageSubscription>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        if let Some(sub) = &paging {
            let tx = tx.clone();
            sub.set_waker(Box::new(move || {
                let _ = tx.send(Cmd::Depage);
            }));
        }
        let filter_string = config.filter_string.clone();
        let queue = Self(Arc::new(Shared {
            id,
            name: config.name.clone(),
            config: RwLock::new(Arc::new(config)),
            filter: RwLock::new((filter_string, filter)),
            ids,
            post_office,
            tx,
            message_count: AtomicI64::new(0),
            paging,
            consumer_count: AtomicI32::new(0),
            consumer_filters: RwLock::default(),
            deleted: AtomicBool::new(false),
            created_at: crate::message::now_millis(),
            consumer_removed_at: AtomicI64::new(-1),
            used: AtomicBool::new(false),
            swept: AtomicBool::new(false),
            adds_pending: AtomicI64::new(0),
            messages_added: AtomicI64::new(0),
            messages_acknowledged: AtomicI64::new(0),
            messages_expired: AtomicI64::new(0),
            messages_killed: AtomicI64::new(0),
            messages_replaced: AtomicI64::new(0),
            paused: AtomicBool::new(false),
            address_paused: AtomicBool::new(false),
            pause_record: AtomicI64::new(-1),
            critical: Default::default(),
        }));
        let actor = Actor {
            shared: queue.0.clone(),
            messages: MessageList::default(),
            consumers: Vec::new(),
            browsers: Vec::new(),
            round_robin: HashMap::new(),
            scheduled: BTreeMap::new(),
            scheduled_seq: 0,
            expiry_warned: false,
            groups: HashMap::new(),
            exclusive_consumer: None,
            last_values: HashMap::new(),
            dispatching: false,
            dispatch_start: None,
            page_iter: PageIterator::default(),
            removing: None,
            redistributor: false,
            redistribute_at: None,
        };
        tokio::spawn(actor.run(rx));
        queue
    }

    /// The queue's critical path: its actor handling a command.
    pub fn critical(&self) -> &crate::critical::Measure {
        &self.0.critical
    }

    pub fn id(&self) -> i64 {
        self.0.id
    }

    /// The queue's configuration as it is now: `updateQueue` may replace
    /// it.
    pub fn config(&self) -> Arc<QueueConfig> {
        self.0.config()
    }

    pub fn name(&self) -> &Name {
        &self.0.name
    }

    /// `updateQueue`'s setters: the queue goes on with `config` and, if
    /// given, the filter (text and compiled). Its name and address stay.
    pub fn update(&self, config: QueueConfig, filter: Option<(Option<String>, Option<FilterRef>)>) {
        let reset_groups = self.config().group_buckets != config.group_buckets;
        *self.0.config.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(config);
        if let Some(filter) = filter {
            *self.0.filter.write().unwrap_or_else(|e| e.into_inner()) = filter;
        }
        let _ = self.0.tx.send(Cmd::Reconfigure { reset_groups });
    }

    /// The filter routing applies (`getFilter`).
    pub fn filter(&self) -> Option<FilterRef> {
        self.0
            .filter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .clone()
    }

    /// The text of [`Queue::filter`]: what the queue's binding is journalled
    /// with.
    pub fn filter_string(&self) -> Option<String> {
        self.0
            .filter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .clone()
    }

    /// `getMessageCount`: with the paged messages not yet acknowledged.
    pub fn message_count(&self) -> i64 {
        self.0.message_count.load(Ordering::SeqCst)
            + self.0.paging.as_ref().map_or(0, |s| s.message_count())
    }

    /// The queue's page subscription.
    pub fn page_subscription(&self) -> Option<&Arc<PageSubscription>> {
        self.0.paging.as_ref()
    }

    pub fn consumer_count(&self) -> i32 {
        self.0.consumer_count.load(Ordering::SeqCst)
    }

    pub fn created_at(&self) -> i64 {
        self.0.created_at
    }

    /// When a consumer last left, -1 if none ever did.
    pub fn consumer_removed_at(&self) -> i64 {
        self.0.consumer_removed_at.load(Ordering::SeqCst)
    }

    /// Whether a message was ever acknowledged, expired or killed here.
    pub fn was_used(&self) -> bool {
        self.0.used.load(Ordering::SeqCst)
    }

    pub fn swept(&self) -> bool {
        self.0.swept.load(Ordering::SeqCst)
    }

    pub fn set_swept(&self, swept: bool) {
        self.0.swept.store(swept, Ordering::SeqCst);
    }

    pub fn is_deleted(&self) -> bool {
        self.0.deleted.load(Ordering::SeqCst)
    }

    /// `QueueImpl.isDurable`: its persistent messages are journaled.
    pub fn is_durable(&self) -> bool {
        self.0.config().durable && !self.0.config().temporary
    }

    /// Whether this reference is journaled: a persistent message on a
    /// durable queue, not read from a page.
    pub fn is_durable_ref(&self, r: &MessageRef) -> bool {
        r.paged.is_none() && self.is_durable_ref_of(&r.message)
    }

    /// Whether a message routed here is journaled.
    pub fn is_durable_ref_of(&self, m: &ServerMessage) -> bool {
        m.is_durable() && self.is_durable()
    }

    pub fn ids(&self) -> &Arc<IdGenerator> {
        &self.0.ids
    }

    /// The queue's address settings (`cachedAddressSettings`).
    pub fn settings(&self) -> Arc<AddressSettings> {
        match self.0.post_office.upgrade() {
            Some(po) => po.settings(&self.0.config().address),
            None => Arc::default(),
        }
    }

    /// `postAcknowledge`: the reference leaves the queue for good. Once no
    /// durable queue refers to the message, its record is deleted (not
    /// synced; recovery deletes what a crash missed). A paged message is
    /// acknowledged on its page instead.
    pub fn post_acknowledge(&self, r: &MessageRef) -> Option<Pending> {
        self.post_acknowledge_for(r, AckReason::Normal)
    }

    /// [`Queue::post_acknowledge`], counted under `reason`. The mirrors are
    /// told (`preAcknowledge`, `postAcknowledge`) of all but a replaced
    /// last value; with a `sync` mirror, what completes once its peer has
    /// the acknowledgement.
    pub fn post_acknowledge_for(&self, r: &MessageRef, reason: AckReason) -> Option<Pending> {
        let counter = match reason {
            AckReason::Normal => &self.0.messages_acknowledged,
            AckReason::Killed => &self.0.messages_killed,
            AckReason::Expired => &self.0.messages_expired,
            AckReason::Replaced => &self.0.messages_replaced,
        };
        counter.fetch_add(1, Ordering::SeqCst);
        self.0.used.store(true, Ordering::SeqCst);
        let mut mirrored = None;
        if let Some(po) = self.0.post_office.upgrade() {
            po.plugins()
                .each(|p| p.message_acknowledged(r, self, reason));
            if reason != AckReason::Replaced {
                let origin = crate::mirror::origin();
                for m in po.mirrors().all() {
                    if let Some(p) = m.acknowledged(self, r, reason, origin.as_ref()) {
                        mirrored = Some(p);
                    }
                }
            }
        }
        if let Some(position) = &r.paged {
            if let Some(sub) = &self.0.paging
                && let Some(store) = sub.store()
            {
                store.acknowledged(sub, position);
            }
            return mirrored;
        }
        self.0.message_count.fetch_sub(1, Ordering::SeqCst);
        self.ref_down(r);
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
        mirrored
    }

    /// `refUp`: the message's memory counts against its address while
    /// referenced, and each reference against the queue's address. As
    /// Artemis's `processRoute` does, a reference counts from when it is
    /// routed, before it is stored or its transaction commits, so that the
    /// next message is paged once the address is full.
    pub(crate) fn ref_up(&self, r: &MessageRef) {
        let po = self.0.post_office.upgrade();
        let owner = r.message.owner(|| {
            let address = r.message.core().address.as_ref()?.to_string_lossy();
            po.as_ref().map(|po| po.paging_store(&address))
        });
        if r.message.ref_up() == 1
            && let Some(owner) = owner
        {
            owner.add_size(memory_estimate(&r.message), 1);
        }
        if let Some(store) = self.0.paging.as_ref().and_then(|s| s.store()) {
            store.add_size(REF_MEMORY_ESTIMATE, 0);
        }
    }

    /// `refDown`: also for a counted reference that never joins the queue
    /// (its transaction rolled back, `AddOperation.beforeRollback`).
    pub(crate) fn ref_down(&self, r: &MessageRef) {
        if r.message.ref_down() == 0
            && let Some(owner) = r.message.owner(|| None)
        {
            owner.add_size(-memory_estimate(&r.message), -1);
        }
        if let Some(store) = self.0.paging.as_ref().and_then(|s| s.store()) {
            store.add_size(-REF_MEMORY_ESTIMATE, 0);
        }
    }

    /// Journals an acknowledgement outside a transaction: `ACKNOWLEDGE_REF`
    /// for a journaled reference; for a paged one `ACKNOWLEDGE_CURSOR` in a
    /// journal transaction of its own (`PageSubscription.ack`). Returns the
    /// write to wait for.
    pub fn store_ack(&self, r: &mut MessageRef) -> StoreResult<Option<Pending>> {
        if let Some(position) = &mut r.paged {
            let (Some(sub), Some(s)) = (&self.0.paging, self.0.ids.storage()) else {
                return Ok(None);
            };
            let Some(store) = sub.store() else {
                return Ok(None);
            };
            let tx = s.generate_id();
            let mut paging = TxPaging::default();
            paging.manager = store.manager();
            position.ack_record = store.store_ack_tx(sub, tx, position, &mut paging.updates)?;
            if position.ack_record < 0 && paging.updates.is_empty() {
                return Ok(None);
            }
            paging.before_commit(s, tx)?;
            return s.commit(tx).map(Some);
        }
        if self.is_durable_ref(r)
            && let Some(s) = self.0.ids.storage()
        {
            return s.store_acknowledge(self.0.id, r.message_id()).map(Some);
        }
        Ok(None)
    }

    /// Journals an acknowledgement in the journal transaction `tx`
    /// (`storeAcknowledgeTransactional`, or `ackTx` for a paged message,
    /// whose page transaction update `paging` writes before the commit).
    /// True when it wrote to the journal.
    pub fn store_ack_tx(
        &self,
        tx: i64,
        r: &mut MessageRef,
        paging: &mut TxPaging,
    ) -> StoreResult<bool> {
        if let Some(position) = &mut r.paged {
            let Some(sub) = &self.0.paging else {
                return Ok(false);
            };
            let Some(store) = sub.store() else {
                return Ok(false);
            };
            if paging.manager.is_none() {
                paging.manager = store.manager();
            }
            position.ack_record = store.store_ack_tx(sub, tx, position, &mut paging.updates)?;
            return Ok(position.ack_record >= 0 || position.tx_id > 0);
        }
        if self.is_durable_ref(r)
            && let Some(s) = self.0.ids.storage()
        {
            s.store_acknowledge_tx(tx, self.0.id, r.message_id())?;
            return Ok(true);
        }
        Ok(false)
    }

    /// The queue is being deleted: its cursor goes from its address's pages.
    pub fn unsubscribe(&self) {
        if let Some(sub) = &self.0.paging
            && let Some(store) = sub.store()
        {
            store.unsubscribe(sub);
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

    /// Adds a routed message to the tail (`addTail`), counting it.
    pub fn add(&self, r: MessageRef) {
        self.ref_up(&r);
        self.add_counted(r);
    }

    /// Adds a routed message counted already ([`Queue::ref_up`] when it
    /// was routed) to the tail.
    pub fn add_counted(&self, r: MessageRef) {
        self.0.message_count.fetch_add(1, Ordering::SeqCst);
        self.0.messages_added.fetch_add(1, Ordering::SeqCst);
        self.0.adds_pending.fetch_add(1, Ordering::SeqCst);
        self.send(Cmd::Add(r));
    }

    /// Returns cancelled or rolled-back references, ordered by message ID
    /// (`postRollback` / `addSorted`). They are still counted.
    pub fn reinsert(&self, refs: Vec<MessageRef>) {
        if !refs.is_empty() {
            self.send(Cmd::Reinsert(refs));
        }
    }

    pub fn add_consumer(&self, reg: ConsumerRegistration) {
        self.0.consumer_count.fetch_add(1, Ordering::SeqCst);
        self.0
            .consumer_filters
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push((reg.id, reg.filter.clone()));
        self.send(Cmd::AddConsumer(Box::new(reg)));
    }

    /// `hasMatchingConsumer`: a consumer without a filter, or whose filter
    /// accepts the message.
    pub fn has_matching_consumer(&self, message: &dyn saranga_filter::Filterable) -> bool {
        self.0
            .consumer_filters
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|(_, f)| f.as_ref().is_none_or(|f| f.matches(message)))
    }

    /// Removes a consumer and returns its delivering messages to the queue
    /// (`ServerConsumerImpl.close`).
    pub async fn remove_consumer(&self, id: ConsumerId, failed: bool) {
        self.0
            .consumer_filters
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(c, _)| *c != id);
        let (done, wait) = oneshot::channel();
        self.send(Cmd::RemoveConsumer { id, failed, done });
        let _ = wait.await;
        self.0.consumer_count.fetch_sub(1, Ordering::SeqCst);
        self.0
            .consumer_removed_at
            .store(crate::message::now_millis(), Ordering::SeqCst);
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
            reply: AckReply::Wait(reply),
        });
        wait.await.unwrap_or(Ok(Vec::new()))
    }

    /// [`ack`](Self::ack) without waiting: `then` has the answer, on the
    /// queue's task, so that a connection acknowledging message after
    /// message need not wait for each in turn.
    pub fn ack_then(
        &self,
        id: ConsumerId,
        message_id: i64,
        individual: bool,
        transactional: bool,
        then: impl FnOnce(AckResult) + Send + 'static,
    ) {
        self.send(Cmd::Ack {
            id,
            message_id,
            individual,
            transactional,
            reply: AckReply::Then(Box::new(then)),
        });
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

    /// `promptDelivery`: a consumer that refused a message may take it now.
    pub fn prompt_delivery(&self) {
        self.send(Cmd::Prompt);
    }

    /// `deleteMatchingReferences`: acknowledges the queued messages `filter`
    /// accepts, or all of them, and returns how many.
    pub async fn delete_matching(&self, filter: Option<FilterRef>) -> usize {
        let (reply, wait) = oneshot::channel();
        self.send(Cmd::DeleteMatching { filter, reply });
        wait.await.unwrap_or(0)
    }

    /// The messages queued now, in delivery order (`browserIterator`).
    pub async fn browse(&self) -> Vec<MessageRef> {
        let (reply, wait) = oneshot::channel();
        self.send(Cmd::Browse { reply });
        wait.await.unwrap_or_default()
    }

    /// `individualCancel`: a message the consumer is delivering goes back
    /// to the queue; unless the client failed, this delivery does not
    /// count.
    pub fn cancel_one(&self, id: ConsumerId, message_id: i64, failed: bool) {
        self.send(Cmd::CancelOne {
            id,
            message_id,
            failed,
            undeliverable: false,
        });
    }

    /// As [`Queue::cancel_one`], and the message is not given to this
    /// consumer again (`Message.rejectConsumer`).
    pub fn cancel_undeliverable(&self, id: ConsumerId, message_id: i64, failed: bool) {
        self.send(Cmd::CancelOne {
            id,
            message_id,
            failed,
            undeliverable: true,
        });
    }

    /// `ServerConsumerImpl.reject`: a message the consumer is delivering
    /// goes to the dead-letter address now.
    pub fn reject(&self, id: ConsumerId, message_id: i64) {
        self.reject_with(id, message_id, Vec::new());
    }

    /// As [`Queue::reject`], the message first getting `properties`
    /// (OpenWire's poison acknowledgement names the failure's cause).
    pub fn reject_with(
        &self,
        id: ConsumerId,
        message_id: i64,
        properties: Vec<(String, PropertyValue)>,
    ) {
        self.send(Cmd::Reject {
            id,
            message_id,
            properties,
        });
    }

    /// `ServerConsumerImpl.backToDelivering`: the reference goes back to
    /// the consumer's delivering list, or to the queue if the consumer is
    /// gone.
    pub fn back_to_delivering(&self, id: ConsumerId, r: MessageRef) {
        self.send(Cmd::BackToDelivering { id, r });
    }

    /// `ServerSessionImpl.expire`: a delivered message the client found
    /// expired.
    pub fn expire(&self, id: ConsumerId, message_id: i64) {
        self.send(Cmd::Expire { id, message_id });
    }

    /// Expires every queued message whose time has passed.
    pub fn expire_scan(&self) {
        self.send(Cmd::ExpireScan);
    }

    /// `addRedistributor`: once `delay` has passed (at once when it is 0),
    /// if the queue has no consumers or messages none of them takes, its
    /// messages go to other nodes of the cluster that have consumers for
    /// them. Neither an internal queue nor one of the management address is
    /// redistributed.
    pub fn add_redistributor(&self, delay: i64, management_address: &str) {
        let c = self.config();
        if c.internal {
            tracing::debug!("Queue {} is internal, can't be redistributed!", c.name);
            return;
        }
        if c.address.starts_with(management_address) {
            tracing::debug!(
                "Queue {} is a management address, ignoring it for redistribution",
                c.address
            );
            return;
        }
        self.send(Cmd::AddRedistributor { delay });
    }

    /// `cancelRedistributor`.
    pub fn cancel_redistributor(&self) {
        self.send(Cmd::CancelRedistributor);
    }

    /// Stops the actor. Its messages are dropped.
    pub fn shutdown(&self) {
        self.0.deleted.store(true, Ordering::SeqCst);
        self.send(Cmd::Shutdown(None));
    }

    /// [`Queue::shutdown`], then `then` is run by the actor once the
    /// messages are dropped, told whether journaled ones were among them:
    /// `deleteQueue` releases those only when the journal has their
    /// deletes, so until then their size still binds the address. An actor
    /// already stopped runs nothing.
    pub fn shutdown_then(&self, then: Box<dyn FnOnce(bool) + Send>) {
        self.0.deleted.store(true, Ordering::SeqCst);
        self.send(Cmd::Shutdown(Some(then)));
    }

    /// Ends the actor as the broker stops, deleting nothing: its messages
    /// stay in the journal and the pages for the next start.
    pub fn stop(&self) {
        self.send(Cmd::Stop);
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

    fn get(&self, (level, index): (usize, usize)) -> &MessageRef {
        &self.levels[level][index]
    }

    /// `poll`: the head, from the highest priority.
    fn take_first(&mut self) -> Option<MessageRef> {
        self.levels.iter_mut().rev().find_map(VecDeque::pop_front)
    }

    fn push_front(&mut self, r: MessageRef) {
        self.levels[r.message.priority_level()].push_front(r);
    }

    /// Takes out every reference `pred` selects, in list order.
    fn take_where(&mut self, mut pred: impl FnMut(&MessageRef) -> bool) -> Vec<MessageRef> {
        let mut out = Vec::new();
        for level in self.levels.iter_mut().rev() {
            let mut keep = VecDeque::with_capacity(level.len());
            for r in level.drain(..) {
                if pred(&r) {
                    out.push(r);
                } else {
                    keep.push_back(r);
                }
            }
            *level = keep;
        }
        out
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
}

struct Consumer {
    reg: ConsumerRegistration,
    /// `availableCredits`; `None` when flow control is off.
    credits: Option<i64>,
    started: bool,
    delivering: VecDeque<MessageRef>,
    /// Messages already sent to a browser.
    browsed: HashSet<i64>,
    /// The large message being sent (`largeMessageDeliverer`): nothing
    /// else is delivered until it is done.
    large: Option<LargeTransfer>,
    /// Forced-delivery markers waiting for the large message.
    forced: Vec<CoreMessage>,
}

/// A large message part-way to a consumer (`CoreLargeMessageDeliverer`).
/// It holds the body, so the file stays until the transfer ends.
struct LargeTransfer {
    body: Arc<LargeBody>,
    reader: Option<LargeReader>,
    position: u64,
}

impl Consumer {
    fn new(reg: ConsumerRegistration) -> Self {
        Self {
            credits: Some(0),
            started: reg.started || reg.browse_only,
            delivering: VecDeque::new(),
            browsed: HashSet::new(),
            large: None,
            forced: Vec::new(),
            reg,
        }
    }

    fn has_credits(&self) -> bool {
        self.credits.is_none_or(|c| c > 0)
    }

    fn take_credits(&mut self, size: i64) {
        if let Some(credits) = &mut self.credits {
            *credits -= size;
        }
    }

    /// Hands a message to the sink; a large one starts a transfer.
    fn send(&mut self, message: &Arc<ServerMessage>, delivery_count: i32) {
        let id = self.reg.client_id;
        // A native message holds its whole encoding: it goes as it is.
        let size = match message.large().filter(|_| message.native().is_none()) {
            None => self.reg.sink.deliver(id, message, delivery_count),
            Some(body) if !self.reg.sink.supports_large() => {
                let mut core = message.core().clone();
                core.body = body
                    .read_all()
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            "AMQ222110: Error on resetting large message deliver - {}: {e}",
                            message.message_id()
                        );
                        Vec::new()
                    })
                    .into();
                self.reg
                    .sink
                    .deliver(id, &ServerMessage::new(core), delivery_count)
            }
            Some(body) => {
                let size = body.size() as i64;
                let reader = body
                    .reader()
                    .inspect_err(|e| {
                        tracing::warn!(
                            "AMQ222110: Error on resetting large message deliver - {}: {e}",
                            message.message_id()
                        );
                    })
                    .ok();
                self.large = Some(LargeTransfer {
                    body: body.clone(),
                    reader,
                    position: 0,
                });
                self.reg
                    .sink
                    .deliver_large(id, message, size, delivery_count)
            }
        };
        self.reg.metrics.add_message(message.encode_size() as i64);
        self.take_credits(size);
    }

    /// Sends the next chunks of the large message while the consumer has
    /// credits (`CoreLargeMessageDeliverer.deliver`). Returns true when
    /// more could be sent at once.
    fn continue_large(&mut self) -> bool {
        let mut budget = LARGE_BYTES_PER_PASS;
        while self.started && self.has_credits() {
            let Some(t) = &mut self.large else {
                return false;
            };
            if budget == 0 {
                return true;
            }
            let size = t.body.size();
            let len = (size - t.position).min(self.reg.sink.chunk_size().max(1) as u64) as usize;
            let chunk = match t.reader.as_mut().map(|r| r.read_chunk(len)) {
                Some(Ok(chunk)) => chunk,
                Some(Err(e)) => {
                    tracing::warn!(
                        "AMQ222110: Error on resetting large message deliver - {}: {e}",
                        t.body.id()
                    );
                    t.reader = None;
                    vec![0; len]
                }
                None => vec![0; len],
            };
            t.position += len as u64;
            let continues = t.position < size;
            budget = budget.saturating_sub(len.max(1));
            let used = self
                .reg
                .sink
                .deliver_continuation(self.reg.client_id, chunk, continues);
            self.take_credits(used);
            if !continues {
                self.large = None;
                for marker in std::mem::take(&mut self.forced) {
                    self.reg
                        .sink
                        .deliver(self.reg.client_id, &ServerMessage::new(marker), 0);
                }
            }
        }
        false
    }

    fn accepts(&self, r: &MessageRef) -> bool {
        !r.rejected_consumers.contains(&self.reg.id)
            && self
                .reg
                .filter
                .as_ref()
                .is_none_or(|f| f.matches(&*r.message))
            && self.reg.sink.accepts(&r.message)
    }

    fn can_take(&self) -> bool {
        self.started && self.has_credits() && self.large.is_none()
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
    /// References waiting for their delivery time, ordered by time, then
    /// redeliveries before new messages, then arrival
    /// (`ScheduledDeliveryHandlerImpl`).
    scheduled: BTreeMap<(i64, bool, u64), MessageRef>,
    scheduled_seq: u64,
    /// AMQ222147 is logged once per queue.
    expiry_warned: bool,
    /// Message groups and the consumer each is pinned to (`groups`).
    groups: HashMap<GroupKey, ConsumerId>,
    /// The consumer an exclusive queue delivers to (`exclusiveConsumer`).
    exclusive_consumer: Option<ConsumerId>,
    /// Last-value queue: the current message ID for each key.
    last_values: HashMap<String, i64>,
    /// Where the queue is in its address's pages.
    page_iter: PageIterator,
    /// `dispatching`: false while consumers-before-dispatch,
    /// delay-before-dispatch or a group rebalance hold delivery.
    dispatching: bool,
    /// `dispatchStartTime`: when a delay-before-dispatch ends.
    dispatch_start: Option<i64>,
    /// While management removes messages with the message audit log on:
    /// those removed, logged when the removal ends.
    removing: Option<Vec<Arc<ServerMessage>>>,
    /// `redistributor`: the queue's messages go to other nodes of the
    /// cluster that have consumers for them.
    redistributor: bool,
    /// `redistributorFuture`: when a delayed redistributor is added.
    redistribute_at: Option<i64>,
}

/// A message group as `MessageGroups` keys it: by group ID, or by bucket
/// when `group-buckets` is set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKey {
    Id(Vec<u8>),
    Bucket(u32),
}

impl Actor {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        loop {
            let wake = self
                .scheduled
                .keys()
                .next()
                .map(|k| k.0)
                .into_iter()
                .chain(self.dispatch_start.filter(|_| !self.dispatching))
                .chain(self.redistribute_at)
                .min();
            let cmd = match wake {
                None => rx.recv().await,
                Some(at) => {
                    let wait = Duration::from_millis((at - now_millis()).max(0) as u64);
                    tokio::select! {
                        cmd = rx.recv() => cmd,
                        _ = tokio::time::sleep(wait) => {
                            let shared = self.shared.clone();
                            let _critical = shared.critical.enter();
                            self.redistributor_due();
                            self.deliver_scheduled();
                            self.deliver();
                            continue;
                        }
                    }
                }
            };
            let Some(cmd) = cmd else { break };
            let shared = self.shared.clone();
            let _critical = shared.critical.enter();
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
                // try_recv takes no cooperative budget: without this, a
                // steady stream of sends keeps the actor running, and the
                // tasks it woke (a consumer's connection) wait behind it.
                tokio::task::consume_budget().await;
            }
            if !running {
                break;
            }
            self.redistributor_due();
            self.deliver_scheduled();
            self.deliver();
        }
    }

    fn queue(&self) -> Queue {
        Queue(self.shared.clone())
    }

    /// Holds a reference until its delivery time (`checkAndSchedule`);
    /// `tail` marks a new message, which goes after redeliveries due at the
    /// same time.
    fn schedule(&mut self, r: MessageRef, tail: bool) {
        self.scheduled_seq += 1;
        self.scheduled
            .insert((r.scheduled_time, tail, self.scheduled_seq), r);
    }

    /// `ScheduledDeliveryRunnable`: references whose time has come go to the
    /// head of the queue, in delivery-time order.
    fn deliver_scheduled(&mut self) {
        let now = now_millis();
        let mut due = Vec::new();
        while let Some(entry) = self.scheduled.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let mut r = entry.remove();
            r.scheduled_time = 0;
            due.push(r);
        }
        for r in due.into_iter().rev() {
            self.messages.push_front(r);
        }
    }

    /// `addTail`: scheduled references wait for their time. On a
    /// last-value queue the reference replaces the queued one with its key
    /// (`LastValueQueue.addTail`, pruned at once); a ring queue drops its
    /// oldest reference once over its size (`enforceRing`).
    fn add_tail(&mut self, r: MessageRef) {
        if r.scheduled_time > 0 {
            self.schedule(r, true);
            return;
        }
        self.track_last_value(&r, true);
        self.messages.push_back(r);
        let ring = self.shared.config().ring_size;
        if ring != -1 && self.messages.len() as i64 > ring {
            let oldest = self.messages.take_first();
            if let Some(old) = oldest {
                self.kill(old);
            }
        }
    }

    /// `addSorted`: a returned reference goes back in message ID order, or
    /// waits out its redelivery delay. A last-value queue keeps a newer
    /// value over it; a full ring queue drops it.
    fn add_sorted(&mut self, r: MessageRef) {
        if r.scheduled_time > 0 {
            self.schedule(r, false);
            return;
        }
        if !self.track_last_value(&r, false) {
            self.kill_for(r, AckReason::Replaced);
            return;
        }
        let ring = self.shared.config().ring_size;
        if ring != -1 && self.messages.len() as i64 + 1 > ring {
            self.kill(r);
            return;
        }
        self.messages.insert_sorted(r);
    }

    /// `MessageReference.getLastValueProperty`: the queue's last-value key,
    /// falling back to `_AMQ_LVQ_NAME`.
    /// A last-value key alone makes a last-value queue
    /// (`QueueFactoryImpl.lastValueKey`).
    fn last_value_of(&self, r: &MessageRef) -> Option<String> {
        let c = self.shared.config();
        let key = c.last_value_key.as_deref().filter(|k| !k.is_empty());
        if !c.last_value && key.is_none() {
            return None;
        }
        let core = r.message.core();
        key.and_then(|k| message::string_property(core, k))
            .or_else(|| message::string_property(core, message::HDR_LAST_VALUE_NAME))
    }

    /// Records `r` as its key's current value. With `replace` an older
    /// queued value is dropped (`trackLastValue` then `pruneLastValues`);
    /// without, `r` only becomes current if its key has none
    /// (`trackLastValueIfAbsent`). Returns whether `r` is current.
    fn track_last_value(&mut self, r: &MessageRef, replace: bool) -> bool {
        let Some(key) = self.last_value_of(r) else {
            return true;
        };
        let id = r.message_id();
        match self.last_values.get(&key).copied() {
            Some(current) if current != id => {
                if !replace {
                    let queued = self.messages.find(|m| m.message_id() == current).is_some();
                    if queued {
                        return false;
                    }
                }
                self.last_values.insert(key, id);
                if let Some(at) = self.messages.find(|m| m.message_id() == current) {
                    let old = self.messages.remove(at);
                    self.kill_for(old, AckReason::Replaced);
                }
            }
            _ => {
                self.last_values.insert(key, id);
            }
        }
        true
    }

    /// `removeIfCurrent`.
    fn forget_last_value(&mut self, r: &MessageRef) {
        if let Some(key) = self.last_value_of(r)
            && self.last_values.get(&key) == Some(&r.message_id())
        {
            self.last_values.remove(&key);
        }
    }

    /// `canDispatch`.
    fn can_dispatch(&mut self) -> bool {
        if self.dispatching {
            return true;
        }
        if self.consumers.iter().any(|c| !c.delivering.is_empty()) {
            return false;
        }
        if self.consumers.len() as i64 >= i64::from(self.shared.config().consumers_before_dispatch)
        {
            self.dispatching = true;
            return true;
        }
        if self.dispatch_start.is_some_and(|t| t < now_millis()) {
            self.dispatching = true;
            return true;
        }
        false
    }

    /// `stopDispatch`.
    fn stop_dispatch(&mut self) {
        self.dispatching = false;
        self.dispatch_start = None;
    }

    /// References coming back from consumers or a rolled-back transaction
    /// (`RefsOperation.afterRollback`): each is checked for redelivery
    /// (`checkRedelivery`), then the survivors return (`postRollback`).
    fn return_refs(&mut self, refs: Vec<MessageRef>) {
        // A non-destructive queue never took them off its list.
        if self.shared.config().non_destructive {
            return;
        }
        let now = now_millis();
        let back: Vec<MessageRef> = refs
            .into_iter()
            .filter_map(|r| self.check_redelivery(r, now))
            .collect();
        if self.shared.config().purge_on_no_consumers && self.consumers.is_empty() {
            back.into_iter().for_each(|r| self.kill(r));
        } else {
            back.into_iter().for_each(|r| self.add_sorted(r));
        }
    }

    /// `checkRedelivery`: journals the delivery count, then sends the
    /// reference to the dead-letter address once it has used its delivery
    /// attempts, or delays its redelivery. `None` when it has left the queue.
    fn check_redelivery(&mut self, mut r: MessageRef, time_base: i64) -> Option<MessageRef> {
        let queue = self.queue();
        crate::transaction::persist_delivery_count(&queue, &mut r);
        let s = queue.settings();
        if s.max_delivery_attempts > 0 && r.delivery_count >= s.max_delivery_attempts {
            self.send_to_dead_letter_address(r, &s);
            return None;
        }
        if s.redelivery_delay > 0 {
            r.scheduled_time = time_base + redelivery_delay(&s, r.delivery_count);
            if queue.is_durable_ref(&r)
                && let Some(st) = self.shared.ids.storage()
                && let Err(e) = st.update_scheduled_delivery_time(
                    self.shared.id,
                    r.message_id(),
                    r.scheduled_time,
                    r.delivery_count,
                )
            {
                tracing::warn!("updating the scheduled delivery time: {e}");
            }
        }
        Some(r)
    }

    /// `acknowledge` outside any transaction, for a reference the broker
    /// drops (`AckReason.KILLED` / `EXPIRED`).
    fn kill(&mut self, r: MessageRef) {
        self.kill_for(r, AckReason::Killed);
    }

    /// [`Actor::kill`], counted under `reason`.
    fn kill_for(&mut self, mut r: MessageRef, reason: AckReason) {
        self.forget_last_value(&r);
        let queue = self.queue();
        if let Err(e) = queue.store_ack(&mut r) {
            tracing::warn!(
                "AMQ222178: Cannot find message {} on the journal: {e}",
                r.message_id()
            );
        }
        queue.post_acknowledge_for(&r, reason);
    }

    /// `sendToDeadLetterAddress`.
    fn send_to_dead_letter_address(&mut self, r: MessageRef, s: &AddressSettings) {
        let name = &self.shared.name;
        let Some(dla) = s.dead_letter_address.clone() else {
            tracing::warn!(
                "AMQ222150: Sending message {} to Dead Letter Address, but there is no Dead Letter Address configured for queue {name} so dropping it",
                r.message_id()
            );
            self.kill(r);
            return;
        };
        let address = self.shared.config().address.clone();
        self.create_resources(
            &address,
            s.auto_create_dead_letter_resources,
            &dla,
            &s.dead_letter_queue_prefix,
            &s.dead_letter_queue_suffix,
        );
        if !self.has_bindings(&dla) {
            tracing::warn!(
                "AMQ222148: Sending message {} to Dead Letter Address {dla}, but it has no bindings so dropping it",
                r.message_id()
            );
            self.kill(r);
        } else {
            tracing::warn!(
                "AMQ222149: Sending message {} to Dead Letter Address {dla} from {name}",
                r.message_id()
            );
            self.move_to(&dla, r, AckReason::Killed, None);
        }
    }

    /// `expire`: to the expiry address of the message's own address, or
    /// dropped.
    fn expire(&mut self, r: MessageRef) {
        let queue = self.queue();
        let message_address = r
            .message
            .core()
            .address
            .as_ref()
            .map(|a| a.to_string_lossy())
            .unwrap_or_else(|| self.shared.config().address.to_string());
        let s = if *message_address == *self.shared.config().address {
            queue.settings()
        } else {
            match self.shared.post_office.upgrade() {
                Some(po) => po.settings(&message_address),
                None => Arc::default(),
            }
        };
        if let Some(po) = self.shared.post_office.upgrade() {
            let expiry = s.expiry_address.as_deref().filter(|a| !a.is_empty());
            po.plugins().each(|p| p.message_expired(&r, &queue, expiry));
        }
        match s.expiry_address.clone().filter(|a| !a.is_empty()) {
            Some(expiry) => {
                self.create_resources(
                    &message_address,
                    s.auto_create_expiry_resources,
                    &expiry,
                    &s.expiry_queue_prefix,
                    &s.expiry_queue_suffix,
                );
                if self.has_bindings(&expiry) {
                    self.move_to(&expiry, r, AckReason::Expired, None);
                } else {
                    if !self.expiry_warned {
                        self.expiry_warned = true;
                        tracing::warn!(
                            "AMQ222146: Message has expired. No bindings for Expiry Address {expiry} so dropping it"
                        );
                    }
                    self.kill_for(r, AckReason::Expired);
                }
            }
            None => {
                if !self.expiry_warned {
                    self.expiry_warned = true;
                    tracing::warn!(
                        "AMQ222147: Messages are being expired on queue {}, but there is no Expiry Address configured so messages will be dropped.",
                        self.shared.name
                    );
                }
                self.kill_for(r, AckReason::Expired);
            }
        }
    }

    fn has_bindings(&self, address: &str) -> bool {
        self.shared
            .post_office
            .upgrade()
            .is_some_and(|po| po.has_bindings(address))
    }

    /// `createResources`: with auto-creation on, a queue on the dead-letter
    /// or expiry address for this address's messages only.
    fn create_resources(
        &self,
        address: &str,
        auto_create: bool,
        destination: &str,
        prefix: &str,
        suffix: &str,
    ) {
        if !auto_create || address == destination || destination.is_empty() {
            return;
        }
        let Some(po) = self.shared.post_office.upgrade() else {
            return;
        };
        let mut spec = QueueSpec::new(format!("{prefix}{address}{suffix}"), destination);
        spec.filter_string = Some(format!("{} = '{address}'", message::HDR_ORIGINAL_ADDRESS));
        spec.auto_created = true;
        spec.auto_create_address = Some(true);
        if let Err(e) = po.create_queue(spec, true) {
            tracing::debug!("creating a resource queue on {destination}: {e}");
        }
    }

    /// `move`: a copy of the message goes to `address` (to the queue
    /// `target` only, when given) and the reference is acknowledged under
    /// `reason`, in one transaction.
    fn move_to(&mut self, address: &str, r: MessageRef, reason: AckReason, target: Option<Queue>) {
        let expiry = reason == AckReason::Expired;
        let Some(po) = self.shared.post_office.upgrade() else {
            self.kill_for(r, reason);
            return;
        };
        let now = now_millis();
        let mut copy = message::make_copy(
            r.message.core(),
            self.shared.ids.next(),
            address,
            &self.shared.name,
            expiry,
            now,
        );
        message::apply_expiry_delay(&mut copy, &po.settings(address), now);
        let mut copy = MessageRef::new(r.message.copy(copy));
        copy.scheduled_time = message::scheduled_delivery_time(copy.message.core());
        let routed = match target {
            Some(q) => vec![(copy.clone(), vec![q])],
            None => po.route_internal(&copy, address),
        };
        self.commit_move(&po, r, routed, reason, address);
    }

    /// The end of `move` and of a redistribution: the routed copy is
    /// journaled with the reference's acknowledgement in one transaction,
    /// then joins its queues.
    fn commit_move(
        &mut self,
        po: &PostOffice,
        mut r: MessageRef,
        routed: crate::postoffice::Routed,
        reason: AckReason,
        address: &str,
    ) {
        // A paging address takes the copy in its pages. An expired
        // message's copy is not mirrored: the peer expires it too.
        let mirroring = match reason {
            AckReason::Expired => crate::mirror::Mirroring::Disabled,
            _ => crate::mirror::Mirroring::Enabled,
        };
        let routed = match po.page_routed_mirrored(routed.clone(), None, &mirroring) {
            Ok(paged) => paged.routed,
            Err(e) => {
                tracing::warn!("moving message {} to {address}: {e}", r.message_id());
                routed
            }
        };
        let queue = self.queue();
        if let Some(s) = self.shared.ids.storage() {
            let tx = self.shared.ids.next();
            let mut persistent = false;
            let mut written = || -> crate::storage::StoreResult<()> {
                for (m, targets) in &routed {
                    for q in targets.iter().filter(|q| q.is_durable_ref(m)) {
                        if m.message.durable_up() == 1 {
                            s.store_message_tx(tx, &m.message)?;
                        }
                        s.store_reference_tx(tx, q.id(), m.message_id())?;
                        persistent = true;
                    }
                }
                let mut paging = TxPaging::default();
                if queue.store_ack_tx(tx, &mut r, &mut paging)? {
                    persistent = true;
                }
                if persistent {
                    paging.before_commit(s, tx)?;
                    s.commit(tx)?;
                }
                Ok(())
            };
            if let Err(e) = written() {
                tracing::warn!("moving message {} to {address}: {e}", r.message_id());
            }
        }
        for (m, targets) in routed {
            for q in targets {
                q.add(m.clone());
            }
        }
        queue.post_acknowledge_for(&r, reason);
    }

    /// Handles one command; returns false to stop the actor.
    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::Add(r) => {
                self.shared.adds_pending.fetch_sub(1, Ordering::SeqCst);
                self.add_tail(r)
            }
            Cmd::Reinsert(refs) => self.return_refs(refs),
            Cmd::AddConsumer(reg) => {
                let consumer = Consumer::new(*reg);
                if consumer.reg.browse_only {
                    self.browsers.push(consumer);
                } else {
                    self.cancel_redistributor();
                    let c = self.shared.config();
                    if c.group_rebalance {
                        if c.group_rebalance_pause_dispatch {
                            self.stop_dispatch();
                        }
                        self.groups.clear();
                    }
                    if c.delay_before_dispatch >= 0 && self.dispatch_start.is_none() {
                        self.dispatch_start = Some(now_millis() + c.delay_before_dispatch);
                    }
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
                    let browser = c.reg.browse_only;
                    self.return_refs(cancel(c.delivering, false, failed));
                    if !browser {
                        if self.consumers.is_empty() {
                            self.stop_dispatch();
                        }
                        if self.exclusive_consumer == Some(id) {
                            self.exclusive_consumer = None;
                        }
                        self.groups.retain(|_, c| *c != id);
                    }
                }
                if self.shared.config().purge_on_no_consumers && self.consumers.is_empty() {
                    let refs = self.messages.take_where(|_| true);
                    refs.into_iter().for_each(|r| self.kill(r));
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
                reply.send(result);
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
                        c.large = None;
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
            Cmd::ResumeLarge | Cmd::Depage | Cmd::Prompt => {}
            Cmd::AddRedistributor { delay } => self.add_redistributor(delay),
            Cmd::CancelRedistributor => self.cancel_redistributor(),
            Cmd::DeleteMatching { filter, reply } => {
                let refs = self
                    .messages
                    .take_where(|r| filter.as_ref().is_none_or(|f| f.matches(&*r.message)));
                let n = refs.len();
                refs.into_iter().for_each(|r| self.kill(r));
                let _ = reply.send(n);
            }
            Cmd::Browse { reply } => {
                let mut refs: Vec<MessageRef> = self.messages.iter().cloned().collect();
                refs.extend(self.scheduled.values().cloned());
                let _ = reply.send(refs);
            }
            Cmd::CancelOne {
                id,
                message_id,
                failed,
                undeliverable,
            } => {
                let r = self.consumer_mut(id).and_then(|c| {
                    let i = c
                        .delivering
                        .iter()
                        .position(|r| r.message_id() == message_id)?;
                    c.delivering.remove(i)
                });
                if let Some(mut r) = r {
                    if !failed {
                        r.delivery_count -= 1;
                    }
                    if undeliverable {
                        r.rejected_consumers.push(id);
                    }
                    self.return_refs(vec![r]);
                }
            }
            Cmd::BackToDelivering { id, r } => match self.consumer_mut(id) {
                Some(c) => c.delivering.push_back(r),
                None => self.return_refs(vec![r]),
            },
            Cmd::Reject {
                id,
                message_id,
                properties,
            } => {
                let r = self.consumer_mut(id).and_then(|c| {
                    let i = c
                        .delivering
                        .iter()
                        .position(|r| r.message_id() == message_id)?;
                    c.delivering.remove(i)
                });
                if let Some(mut r) = r {
                    if !properties.is_empty() {
                        let mut core = r.message.core().clone();
                        for (k, v) in properties {
                            core.properties.insert(k.as_str().into(), v);
                        }
                        r.message = r.message.with_core(core);
                    }
                    let s = self.queue().settings();
                    self.send_to_dead_letter_address(r, &s);
                }
            }
            Cmd::Reconfigure { reset_groups } => {
                if reset_groups {
                    self.groups.clear();
                }
            }
            Cmd::Remove { message_id, reply } => {
                let mut r = self
                    .messages
                    .find(|r| r.message_id() == message_id)
                    .map(|at| self.messages.remove(at));
                if r.is_none()
                    && let Some(k) = self
                        .scheduled
                        .iter()
                        .find(|(_, r)| r.message_id() == message_id)
                        .map(|(k, _)| *k)
                {
                    r = self.scheduled.remove(&k);
                }
                let _ = reply.send(r);
            }
            Cmd::Expire { id, message_id } => {
                let r = self.consumer_mut(id).and_then(|c| {
                    let i = c
                        .delivering
                        .iter()
                        .position(|r| r.message_id() == message_id)?;
                    c.delivering.remove(i)
                });
                if let Some(r) = r {
                    self.expire(r);
                }
            }
            Cmd::ExpireScan => {
                let now = now_millis();
                let expired = self.messages.take_where(|r| r.message.is_expired(now));
                expired.into_iter().for_each(|r| self.expire(r));
            }
            Cmd::Manage(op) => op(self),
            Cmd::Shutdown(then) => {
                let durable = self.delete_all_references();
                if let Some(then) = then {
                    then(durable);
                }
                return false;
            }
            Cmd::Stop => return false,
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
        for r in &refs {
            c.reg
                .metrics
                .add_acknowledge(r.message.encode_size() as i64, transactional);
        }
        // Acknowledging on a non-destructive queue leaves the message there.
        if self.shared.config().non_destructive {
            return Ok(Vec::new());
        }
        Ok(refs)
    }

    /// `hasUnMatchedPending`: a queued message no consumer's filter takes.
    fn has_unmatched_pending(&self) -> bool {
        !self.consumers.is_empty()
            && self
                .messages
                .find(|r| !self.consumers.iter().any(|c| c.accepts(r)))
                .is_some()
    }

    /// `addRedistributor`.
    fn add_redistributor(&mut self, delay: i64) {
        self.redistribute_at = None;
        if self.redistributor {
            // Delivery is prompted once the command is handled.
            return;
        }
        if delay > 0 {
            if self.consumers.is_empty() || self.has_unmatched_pending() {
                self.redistribute_at = Some(now_millis() + delay);
            }
        } else {
            self.internal_add_redistributor();
        }
    }

    /// `internalAddRedistributor`.
    fn internal_add_redistributor(&mut self) {
        if !self.redistributor && (self.consumers.is_empty() || self.has_unmatched_pending()) {
            tracing::trace!(
                "QueueImpl::Adding redistributor on queue {}",
                self.shared.name
            );
            self.redistributor = true;
        }
    }

    /// `DelayedAddRedistributor`, when its time has come.
    fn redistributor_due(&mut self) {
        if self.redistribute_at.is_some_and(|at| at <= now_millis()) {
            self.redistribute_at = None;
            self.internal_add_redistributor();
        }
    }

    /// `cancelRedistributor`.
    fn cancel_redistributor(&mut self) {
        self.redistribute_at = None;
        self.redistributor = false;
    }

    /// `Redistributor.handle`: each message without a group the post office
    /// finds another binding for (`PostOfficeImpl.redistribute`) goes there
    /// as a copy, acknowledged here in the same transaction. The first it
    /// finds none for leaves the redistributor busy until prompted again.
    fn redistribute(&mut self) {
        if !self.redistributor {
            return;
        }
        let Some(po) = self.shared.post_office.upgrade() else {
            return;
        };
        let queue = self.queue();
        // Messages with a group are not redistributed.
        while let Some(at) = self
            .messages
            .find(|r| message::group_id(r.message.core()).is_none())
        {
            let r = self.messages.get(at);
            if r.message.is_expired(now_millis()) {
                let r = self.messages.remove(at);
                self.expire(r);
                continue;
            }
            tracing::debug!(
                "Redistributing message {}, originatingQueue={}",
                r.message_id(),
                self.shared.name
            );
            let Some(routed) = po.redistribute(r, &queue) else {
                tracing::debug!(
                    "postOffice.redistribute return null for message {}",
                    r.message_id()
                );
                break;
            };
            let r = self.messages.remove(at);
            let address = r
                .message
                .core()
                .address
                .as_ref()
                .map(|a| a.to_string_lossy())
                .unwrap_or_default();
            self.commit_move(&po, r, routed, AckReason::Normal, &address);
        }
    }

    /// `QueueImpl.deliver`: hands messages to consumers until none can take
    /// another. Each message goes to the next consumer, round robin, in the
    /// highest priority group that has one able to take it.
    fn deliver(&mut self) {
        if self.is_paused() {
            self.deliver_to_browsers();
            return;
        }
        self.depage();
        // Large messages part-way out go first.
        let mut more = false;
        for c in self.consumers.iter_mut().chain(self.browsers.iter_mut()) {
            more |= c.continue_large();
        }
        if more {
            self.queue().send(Cmd::ResumeLarge);
        }
        if !self.can_dispatch() {
            self.deliver_to_browsers();
            return;
        }
        while self.messages.len() > 0 {
            // An exclusive queue delivers only to its chosen consumer.
            if self.shared.config().exclusive
                && let Some(id) = self.exclusive_consumer
            {
                match self.consumers.iter().position(|c| c.reg.id == id) {
                    Some(i) if self.try_deliver(i) => continue,
                    Some(_) => break,
                    None => self.exclusive_consumer = None,
                }
            }
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
        self.redistribute();
        self.deliver_to_browsers();
    }

    /// The group a reference belongs to (`extractGroupID`), keyed by bucket
    /// when `group-buckets` is set.
    fn group_of(&self, r: &MessageRef) -> Option<GroupKey> {
        let c = self.shared.config();
        if c.exclusive || c.group_buckets == 0 {
            return None;
        }
        let id = message::group_id(r.message.core())?;
        Some(if c.group_buckets > 0 {
            GroupKey::Bucket((java_hash(&id) & i32::MAX) as u32 % c.group_buckets as u32)
        } else {
            GroupKey::Id(id)
        })
    }

    fn try_deliver(&mut self, index: usize) -> bool {
        let c = &self.consumers[index];
        if !c.can_take() {
            return false;
        }
        let non_destructive = self.shared.config().non_destructive;
        let id = c.reg.id;
        let groups = &self.groups;
        let shared = &self.shared;
        let group_ok = |r: &MessageRef| {
            let c = shared.config();
            if c.exclusive || c.group_buckets == 0 {
                return true;
            }
            let Some(g) = message::group_id(r.message.core()) else {
                return true;
            };
            let key = if c.group_buckets > 0 {
                GroupKey::Bucket((java_hash(&g) & i32::MAX) as u32 % c.group_buckets as u32)
            } else {
                GroupKey::Id(g)
            };
            groups.get(&key).is_none_or(|owner| *owner == id)
        };
        let Some(at) = self.messages.find(|r| {
            c.accepts(r)
                && (!non_destructive || !c.browsed.contains(&r.message_id()))
                && group_ok(r)
        }) else {
            return false;
        };
        // handle: a consumer whose protocol holds this message back is busy.
        if !c.reg.sink.has_credits(&self.messages.get(at).message) {
            return false;
        }
        let mut r = if non_destructive {
            self.messages.get(at).clone()
        } else {
            self.messages.remove(at)
        };
        // An expired message is never delivered (`checkExpired`).
        if r.message.is_expired(now_millis()) {
            if non_destructive {
                r = self.messages.remove(at);
            }
            self.expire(r);
            return true;
        }
        // handleMessageGroup: a new group is pinned to this consumer and its
        // first message flagged; sequence -1 closes the group.
        let mut first = false;
        if self.shared.config().exclusive {
            if self.exclusive_consumer.is_none() {
                self.exclusive_consumer = Some(id);
                first = true;
            }
        } else if let Some(g) = self.group_of(&r) {
            if message::group_sequence(r.message.core()) == -1 {
                self.groups.remove(&g);
            } else if let std::collections::hash_map::Entry::Vacant(e) = self.groups.entry(g) {
                e.insert(id);
                first = true;
            }
        }
        let flagged = match (&self.shared.config().group_first_key, first) {
            (Some(key), true) => {
                let mut core = r.message.core().clone();
                core.properties
                    .insert(key.as_str().into(), PropertyValue::Bool(true));
                Some(r.message.with_core(core))
            }
            _ => None,
        };
        let c = &mut self.consumers[index];
        r.delivery_count += 1;
        c.send(flagged.as_ref().unwrap_or(&r.message), r.delivery_count);
        if c.large.is_some() && c.continue_large() {
            self.shared.tx.send(Cmd::ResumeLarge).ok();
        }
        let c = &mut self.consumers[index];
        if non_destructive {
            c.browsed.insert(r.message_id());
            if !c.reg.pre_acknowledge {
                c.delivering.push_back(r);
            }
            return true;
        }
        if c.reg.pre_acknowledge {
            self.pre_acknowledge(r);
        } else {
            c.delivering.push_back(r);
        }
        true
    }

    /// A pre-acknowledged delivery is acknowledged at once (`acknowledge`
    /// from `ServerConsumerImpl.proceedDeliver`).
    fn pre_acknowledge(&self, mut r: MessageRef) {
        let queue = Queue(self.shared.clone());
        if let Err(e) = queue.store_ack(&mut r) {
            tracing::warn!(
                "AMQ222178: Cannot find message {} on the journal: {e}",
                r.message_id()
            );
        }
        queue.post_acknowledge(&r);
    }

    /// `QueueImpl.depage`: reads paged messages into memory while the
    /// queue holds fewer than `max-read-page-messages` and
    /// `max-read-page-bytes` of them (`needsDepage`).
    fn depage(&mut self) {
        let Some(sub) = self.shared.paging.clone() else {
            return;
        };
        let Some(store) = sub.store() else {
            return;
        };
        if self.shared.adds_pending.load(Ordering::SeqCst) > 0 {
            return;
        }
        let s = self.queue().settings();
        let now = now_millis();
        loop {
            let (n, bytes) = sub.in_memory();
            let full = if s.max_read_page_messages <= 0 && s.max_read_page_bytes <= 0 {
                n >= MAX_DEPAGE_NUM
            } else {
                (s.max_read_page_messages > 0 && n >= s.max_read_page_messages)
                    || (s.max_read_page_bytes > 0 && bytes >= s.max_read_page_bytes)
            };
            if full {
                break;
            }
            let Some(p) = store.next(&sub, &mut self.page_iter) else {
                break;
            };
            sub.read_in(p.position.size);
            let mut r = MessageRef::new(p.message);
            r.paged = Some(p.position);
            let t = message::scheduled_delivery_time(r.message.core());
            if t > now {
                r.scheduled_time = t;
            }
            self.add_tail(r);
        }
    }

    /// `deleteAllReferences`: the deleted queue's durable messages are
    /// acknowledged so their records go. Returns whether any was journaled.
    fn delete_all_references(&mut self) -> bool {
        let queue = Queue(self.shared.clone());
        let storage = self.shared.ids.storage().cloned();
        let mut refs: Vec<MessageRef> = std::mem::take(&mut self.scheduled).into_values().collect();
        for level in &mut self.messages.levels {
            refs.extend(level.drain(..));
        }
        for c in self.consumers.iter_mut() {
            refs.extend(c.delivering.drain(..));
        }
        let _ = storage;
        if crate::audit::message_enabled() {
            let messages: Vec<&ServerMessage> = refs.iter().map(|r| &*r.message).collect();
            let tx_id = self.shared.ids.next();
            crate::audit::removed(&self.shared.name, &messages, tx_id);
        }
        let mut durable = false;
        for mut r in refs {
            // A paged message's cursor goes with the queue's subscription.
            if r.paged.is_none() {
                let _ = queue.store_ack(&mut r);
            }
            durable |= queue.is_durable_ref(&r);
            queue.post_acknowledge(&r);
        }
        durable
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
                let (message, count) = (r.message.clone(), r.delivery_count);
                b.send(&message, count);
                if b.large.is_some() && b.continue_large() {
                    self.shared.tx.send(Cmd::ResumeLarge).ok();
                }
            }
        }
    }

    /// `ServerConsumerImpl.forceDelivery`: after any pending delivery, sends
    /// a marker message carrying the sequence so the client knows the queue
    /// had nothing more for it.
    fn force_delivery(&mut self, id: ConsumerId, sequence: i64) {
        let address = self.shared.name.clone();
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
        // After the large message being sent, if any.
        if c.large.is_some() {
            c.forced.push(core);
            return;
        }
        c.reg
            .sink
            .deliver(c.reg.client_id, &ServerMessage::new(core), 0);
    }
}

/// `SimpleString.hashCode` over the string's bytes.
fn java_hash(bytes: &[u8]) -> i32 {
    bytes.iter().fold(0i32, |h, &b| {
        h.wrapping_mul(31).wrapping_add(i32::from(b as i8))
    })
}

/// `calculateRedeliveryDelay`: the delay grows by the multiplier with each
/// delivery, varied by the collision-avoidance factor, up to the maximum.
fn redelivery_delay(s: &AddressSettings, delivery_count: i32) -> i64 {
    let n = (delivery_count - 1).max(0);
    let mut delay = (s.redelivery_delay as f64 * s.redelivery_multiplier.powi(n)) as i64;
    if s.redelivery_collision_avoidance_factor > 0.0 {
        let random = rand_unit();
        let sign = if rand_unit() < 0.5 { 1.0 } else { -1.0 };
        let variance = sign * s.redelivery_collision_avoidance_factor * random;
        delay += (delay as f64 * variance) as i64;
    }
    delay.min(s.max_redelivery_delay)
}

/// A number in [0, 1) for the collision-avoidance factor; the quality of
/// the randomness does not matter.
fn rand_unit() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(now_millis() as u64);
    (h.finish() >> 11) as f64 / (1u64 << 53) as f64
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
