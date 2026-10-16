//! Mirroring's broker side: the hooks a mirror source has into routing,
//! acknowledgements and address and queue changes (`MirrorController`,
//! called as `PostOfficeImpl` and `ActiveMQServerImpl` call it), the target
//! in use while a mirror target applies what its peer sent
//! (`AMQPMirrorControllerTarget.getControllerInUse`), and the
//! acknowledgements a target could not apply yet (`AckManager`).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_buffer::PropertyValue;
use saranga_storage::codec::{AckRetryRecord, HASH_MAP_RECORD_VERSION, RecordCodec};

use crate::message::MessageRef;
use crate::postoffice::{AddressInfo, PostOffice};
use crate::queue::{AckReason, Queue};
use crate::storage::Pending;

/// `ProtonProtocolManager.MIRROR_ADDRESS`: the prefix of every mirror's
/// store-and-forward queue, and of the address a mirror link attaches to.
pub const MIRROR_ADDRESS: &str = "$ACTIVEMQ_ARTEMIS_MIRROR";

/// `AMQPMirrorControllerSource.INTERNAL_ID`: the ID a message has on the
/// broker it was first sent to, kept as a broker property.
pub const INTERNAL_ID: &str = "x-opt-amq-mr-id";

/// `AMQPMirrorControllerSource.BROKER_ID`: the node ID of that broker.
pub const BROKER_ID: &str = "x-opt-amq-bkr-id";

/// `ProtonProtocolManager.getMirrorAddress`: a broker connection's
/// store-and-forward queue and address.
pub fn mirror_address(connection: &str) -> String {
    format!("{MIRROR_ADDRESS}_{connection}")
}

/// `ReferenceIDSupplier.getServerID`: the broker a message came from, none
/// for one sent to this broker.
pub fn server_id(message: &crate::message::ServerMessage) -> Option<String> {
    match message.core().properties.get_str(BROKER_ID)? {
        PropertyValue::Null => None,
        PropertyValue::String(s) => Some(s.to_string_lossy()),
        _ => crate::message::string_property(message.core(), BROKER_ID),
    }
}

/// `ReferenceIDSupplier.getID`: the message's ID on the broker it came
/// from, else its own.
pub fn internal_id(message: &crate::message::ServerMessage) -> i64 {
    match message.core().properties.get_str(INTERNAL_ID) {
        Some(PropertyValue::Long(v)) => *v,
        Some(PropertyValue::Int(v)) => i64::from(*v),
        _ => message.message_id(),
    }
}

/// The mirror target whose peer's event is being applied
/// (`getControllerInUse`): what a mirror source needs to know not to send
/// it back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Origin {
    /// The peer's node ID (`getRemoteMirrorId`), once its link said it.
    pub remote_id: Option<String>,
    /// An acknowledgement the target retries (`isRetryACK`): never sent on.
    pub retry_ack: bool,
}

thread_local! {
    static ORIGIN: RefCell<Option<Origin>> = const { RefCell::new(None) };
}

/// Runs `f` with `origin` as the target in use (`setControllerInUse`),
/// restoring the one before. `f` must not wait: the target in use is the
/// thread's.
pub fn with_origin<T>(origin: Option<Origin>, f: impl FnOnce() -> T) -> T {
    let before = ORIGIN.with(|o| o.replace(origin));
    let out = f();
    ORIGIN.with(|o| *o.borrow_mut() = before);
    out
}

/// The target in use, if any.
pub fn origin() -> Option<Origin> {
    ORIGIN.with(|o| o.borrow().clone())
}

/// How a route treats the mirrors (`MirrorOption` and `setMirrorSource`).
#[derive(Debug, Clone, Default)]
pub enum Mirroring {
    /// Mirrored as usual.
    #[default]
    Enabled,
    /// Not mirrored (`MirrorOption.disabled`): an expired message's copy,
    /// which the peer makes itself.
    Disabled,
    /// Routed by a mirror target for its peer (`setMirrorSource`).
    From(Origin),
}

impl Mirroring {
    /// The target routing, if any.
    pub fn origin(&self) -> Option<&Origin> {
        match self {
            Self::From(o) => Some(o),
            _ => None,
        }
    }
}

/// A broker's mirror source (`MirrorController`): told of what is routed,
/// acknowledged, added and removed, to send it to its peer.
pub trait MirrorController: Send + Sync {
    /// `addAddress`.
    fn add_address(&self, info: &AddressInfo);
    /// `deleteAddress`.
    fn delete_address(&self, info: &AddressInfo);
    /// `createQueue`.
    fn create_queue(&self, queue: &Queue);
    /// `deleteQueue`.
    fn delete_queue(&self, queue: &Queue);
    /// `sendMessage`: the queue (its store-and-forward queue) the message
    /// goes to as well, if it is mirrored. `origin` is the target routing
    /// it, if one is.
    fn send_message(&self, message: &MessageRef, origin: Option<&Origin>) -> Option<Queue>;
    /// With `sync`, what completes once the peer has taken the message
    /// with this ID from the store-and-forward queue.
    fn sync_wait(&self, message_id: i64) -> Option<Pending>;
    /// `preAcknowledge` and `postAcknowledge`: a reference left `queue`.
    /// With `sync`, what completes once the peer has the acknowledgement.
    fn acknowledged(
        &self,
        queue: &Queue,
        r: &MessageRef,
        reason: AckReason,
        origin: Option<&Origin>,
    ) -> Option<Pending>;
    /// `getRemoteMirrorId`.
    fn remote_mirror_id(&self) -> Option<String>;
}

/// A `sync` mirror's waits (`OperationContext.replicationLineUp`), by the
/// ID of the message in its store-and-forward queue, each completed
/// (`replicationDone`) once the peer took it.
#[derive(Default)]
pub struct SyncWaits(Mutex<HashMap<i64, Vec<saranga_journal::Completion>>>);

impl SyncWaits {
    /// A wait for the message with this ID.
    pub fn wait(&self, message_id: i64) -> Pending {
        let (c, p) = Pending::new();
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(message_id)
            .or_default()
            .push(c);
        p
    }

    /// The peer took the message with this ID.
    pub fn done(&self, message_id: i64) {
        let waiting = self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&message_id);
        for c in waiting.into_iter().flatten() {
            c(Ok(()));
        }
    }
}

/// The mirror sources installed on a broker (`installMirrorController`,
/// `AMQPMirrorControllerAggregation` once there are several).
#[derive(Default)]
pub struct Mirrors(std::sync::RwLock<Vec<Arc<dyn MirrorController>>>);

impl Mirrors {
    pub fn install(&self, controller: Arc<dyn MirrorController>) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push(controller);
    }

    pub fn all(&self) -> Vec<Arc<dyn MirrorController>> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn is_empty(&self) -> bool {
        self.0.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }
}

/// `ActiveMQDefaultConfiguration`'s mirror settings and `broker.xml`'s
/// `mirror-ack-manager-*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckManagerSettings {
    /// `mirror-ack-manager-queue-attempts`: tries on the queue's messages
    /// in memory before its pages are scanned.
    pub queue_attempts: i32,
    /// `mirror-ack-manager-page-attempts`: page scans before giving up.
    pub page_attempts: i32,
    /// `mirror-ack-manager-retry-delay`, in milliseconds.
    pub retry_delay: i64,
    /// `mirror-ack-manager-warn-unacked`.
    pub warn_unacked: bool,
}

impl Default for AckManagerSettings {
    fn default() -> Self {
        Self {
            queue_attempts: 5,
            page_attempts: 2,
            retry_delay: 100,
            warn_unacked: false,
        }
    }
}

/// `AckRetry`: an acknowledgement a target could not apply yet, by the
/// message's broker and ID.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AckRetry {
    /// Its `ACK_RETRY` record, when journaled.
    record: Option<i64>,
    reason: AckReason,
    queue_attempts: i32,
    page_attempts: i32,
}

/// `AckReason.getVal`.
pub fn reason_value(reason: AckReason) -> u8 {
    match reason {
        AckReason::Normal => 0,
        AckReason::Killed => 1,
        AckReason::Expired => 2,
        AckReason::Replaced => 3,
    }
}

/// `AckReason.fromValue`: anything unknown is `NORMAL`.
pub fn reason_from(value: i64) -> AckReason {
    match value {
        1 => AckReason::Killed,
        2 => AckReason::Expired,
        3 => AckReason::Replaced,
        _ => AckReason::Normal,
    }
}

/// The retries of one queue, by node ID and message ID.
type Retries = HashMap<(String, i64), AckRetry>;

/// `AckManager` (one per broker, as `AckManagerProvider` keeps it): applies
/// the acknowledgements a mirror target receives, and keeps those whose
/// message it cannot find yet (still being routed, or delivered to a
/// consumer at the time) to retry every `retry_delay`, first on the
/// queue's messages, then also in its pages, journaling them as `ACK_RETRY`
/// records so that a restart keeps them.
pub struct AckManager {
    post_office: Weak<PostOffice>,
    node_id: String,
    settings: AckManagerSettings,
    retries: Mutex<HashMap<i64, Retries>>,
    /// Wakes the retry task.
    wake: Arc<tokio::sync::Notify>,
    started: std::sync::atomic::AtomicBool,
}

impl AckManager {
    pub fn new(
        post_office: &Arc<PostOffice>,
        node_id: &str,
        settings: AckManagerSettings,
    ) -> Arc<Self> {
        Arc::new(Self {
            post_office: Arc::downgrade(post_office),
            node_id: node_id.to_owned(),
            settings,
            retries: Mutex::default(),
            wake: Arc::default(),
            started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<i64, Retries>> {
        self.retries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `getMirrorAckSize`: the acknowledgements waiting.
    pub fn size(&self) -> usize {
        self.lock().values().map(HashMap::len).sum()
    }

    /// `reload`: the `ACK_RETRY` records recovered, for the queues there
    /// still are.
    pub fn reload(&self, records: Vec<(i64, Vec<u8>)>) {
        let Some(po) = self.post_office.upgrade() else {
            return;
        };
        let queues: HashMap<i64, Queue> = po.queues().into_iter().map(|q| (q.id(), q)).collect();
        let mut retries = self.lock();
        for (id, data) in records {
            let mut r = saranga_buffer::Reader::new(&data);
            let record = match AckRetryRecord::read(&mut r) {
                Ok(record) => record,
                Err(e) => {
                    tracing::warn!("ACK_RETRY record {id}: {e}");
                    continue;
                }
            };
            if !queues.contains_key(&record.queue_id) {
                // The queue is gone: so is the record.
                if let Some(s) = po.ids().storage() {
                    let _ = s.delete_record(id);
                }
                continue;
            }
            let node = String::from_utf8_lossy(&record.node_id).into_owned();
            retries.entry(record.queue_id).or_default().insert(
                (node, record.message_id),
                AckRetry {
                    record: Some(id),
                    reason: reason_from(i64::from(record.reason)),
                    queue_attempts: 0,
                    page_attempts: 0,
                },
            );
        }
    }

    /// Starts the retries (`start`): every `retry_delay` while there are
    /// any.
    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let me = Arc::downgrade(self);
        let delay = Duration::from_millis(self.settings.retry_delay.max(1) as u64);
        tokio::spawn(async move {
            loop {
                let Some(m) = me.upgrade() else { return };
                if m.size() == 0 {
                    let wake = m.wake.clone();
                    let notified = wake.notified();
                    drop(m);
                    notified.await;
                    continue;
                }
                drop(m);
                tokio::time::sleep(delay).await;
                let Some(m) = me.upgrade() else { return };
                m.retry().await;
            }
        });
    }

    /// `ack`: acknowledges the message with `message_id` from `node_id`
    /// (this broker's own when none) on `queue`; one not found is kept to
    /// retry. Runs as `origin` (the target in use).
    pub async fn ack(
        &self,
        node_id: Option<&str>,
        queue: &Queue,
        message_id: i64,
        reason: AckReason,
        origin: Origin,
    ) -> bool {
        let node = node_id.unwrap_or(&self.node_id).to_owned();
        let found = queue
            .ack_supplied(
                vec![((node.clone(), message_id), reason)],
                self.node_id.clone(),
                false,
                origin,
            )
            .await;
        if found.is_empty() {
            if self.settings.warn_unacked && queue.consumer_count() > 0 {
                tracing::warn!(
                    "AMQ111011: There are {} consumers on queue {}, what made the Ack for message with nodeID={}, messageID={} enter a retry list",
                    queue.consumer_count(),
                    queue.name(),
                    node,
                    message_id
                );
            }
            self.add_retry(node, queue, message_id, reason);
            return false;
        }
        true
    }

    /// `addRetry`: journaled as an `ACK_RETRY` record.
    fn add_retry(&self, node: String, queue: &Queue, message_id: i64, reason: AckReason) {
        let record = self.post_office.upgrade().and_then(|po| {
            let s = po.ids().storage()?.clone();
            let id = po.ids().next();
            let body = AckRetryRecord {
                id,
                version: HASH_MAP_RECORD_VERSION,
                queue_id: queue.id(),
                node_id: node.as_bytes().to_vec(),
                message_id,
                reason: reason_value(reason),
            };
            let mut w = saranga_buffer::Writer::new();
            body.write(&mut w);
            match s.add_record(id, saranga_storage::ids::ACK_RETRY, w.into_vec(), false) {
                Ok(_) => Some(id),
                Err(e) => {
                    tracing::warn!("ACK_RETRY record: {e}");
                    None
                }
            }
        });
        let previous = self.lock().entry(queue.id()).or_default().insert(
            (node, message_id),
            AckRetry {
                record,
                reason,
                queue_attempts: 0,
                page_attempts: 0,
            },
        );
        if let Some(AckRetry {
            record: Some(old), ..
        }) = previous
        {
            self.delete_record(old);
        }
        self.wake.notify_one();
    }

    fn delete_record(&self, record: i64) {
        if let Some(po) = self.post_office.upgrade()
            && let Some(s) = po.ids().storage()
            && let Err(e) = s.delete_record(record)
        {
            tracing::debug!("deleting ACK_RETRY record {record}: {e}");
        }
    }

    /// One pass of the retries (`beginRetry` through `endRetry`): each
    /// queue's, on its messages in memory, then in its pages for those
    /// tried there `queue_attempts` times; those still not found after
    /// `page_attempts` scans are given up.
    async fn retry(&self) {
        let Some(po) = self.post_office.upgrade() else {
            return;
        };
        let queue_ids: Vec<i64> = self.lock().keys().copied().collect();
        for queue_id in queue_ids {
            let Some(queue) = po.queues().into_iter().find(|q| q.id() == queue_id) else {
                // The queue went: its retries go too.
                if let Some(gone) = self.lock().remove(&queue_id) {
                    for r in gone.into_values().filter_map(|r| r.record) {
                        self.delete_record(r);
                    }
                }
                continue;
            };
            let wanted: Vec<((String, i64), AckReason)> = self
                .lock()
                .get(&queue_id)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.reason)).collect())
                .unwrap_or_default();
            if wanted.is_empty() {
                continue;
            }
            let retry = Origin {
                remote_id: None,
                retry_ack: true,
            };
            let found = queue
                .ack_supplied(wanted, self.node_id.clone(), false, retry.clone())
                .await;
            let mut scan = Vec::new();
            {
                let mut all = self.lock();
                let Some(m) = all.get_mut(&queue_id) else {
                    continue;
                };
                for key in &found {
                    if let Some(r) = m.remove(key)
                        && let Some(record) = r.record
                    {
                        self.delete_record(record);
                    }
                }
                for (key, r) in m.iter_mut() {
                    r.queue_attempts += 1;
                    if r.queue_attempts >= self.settings.queue_attempts {
                        scan.push((key.clone(), r.reason));
                    }
                }
            }
            if scan.is_empty() {
                continue;
            }
            let found = queue
                .ack_supplied(scan.clone(), self.node_id.clone(), true, retry)
                .await;
            let mut all = self.lock();
            let Some(m) = all.get_mut(&queue_id) else {
                continue;
            };
            for (key, _) in scan {
                if found.contains(&key) {
                    if let Some(r) = m.remove(&key)
                        && let Some(record) = r.record
                    {
                        self.delete_record(record);
                    }
                    continue;
                }
                let Some(r) = m.get_mut(&key) else { continue };
                r.page_attempts += 1;
                if r.page_attempts >= self.settings.page_attempts {
                    if self.settings.warn_unacked {
                        tracing::warn!(
                            "AMQ111012: Acknowledgement retry failed for AckRetry{{nodeID='{}', messageID={}, reason={:?}, pageAttempts={}, queueAttempts={}}} on address {}, queueID={}",
                            key.0,
                            key.1,
                            r.reason,
                            r.page_attempts,
                            r.queue_attempts,
                            queue.config().address,
                            queue_id
                        );
                    }
                    if let Some(record) = m.remove(&key).and_then(|r| r.record) {
                        self.delete_record(record);
                    }
                }
            }
            if m.is_empty() {
                all.remove(&queue_id);
            }
        }
    }
}
