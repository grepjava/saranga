//! Transactions: local and XA. Ported from `TransactionImpl` (states and
//! their checks) with the operations of `RefsOperation`, and from
//! `ResourceManagerImpl` (XA transactions by Xid).
//!
//! A transaction holds the references its sends routed, to add to their
//! queues on commit, and the references its acknowledgements took off their
//! queues, to remove for good on commit or return on rollback.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub use saranga_core_protocol::packets::xa::Xid;

use crate::BrokerError;
use crate::message::MessageRef;
use crate::queue::Queue;
use crate::storage::{Pending, Storage};

/// `XAException` and `XAResource` codes used in XA responses.
pub mod xa_code {
    pub const XA_OK: i32 = 0;
    pub const XA_HEURRB: i32 = 6;
    pub const XA_HEURCOM: i32 = 7;
    pub const XA_RBTIMEOUT: i32 = 106;
    pub const XAER_NOTA: i32 = -4;
    pub const XAER_PROTO: i32 = -6;
    pub const XAER_RMFAIL: i32 = -7;
    pub const XAER_DUPID: i32 = -8;
}

/// `ActiveMQXAException`: answered with an error `SessionXAResponseMessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XaError {
    pub code: i32,
    pub message: Option<String>,
}

impl XaError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
        }
    }
}

/// What an XA operation can fail with: an XA error, or a broker error
/// answered with a plain exception (`ActiveMQIllegalStateException`).
#[derive(Debug, Clone)]
pub enum TxError {
    Xa(XaError),
    Broker(BrokerError),
}

impl From<XaError> for TxError {
    fn from(e: XaError) -> Self {
        Self::Xa(e)
    }
}

impl From<BrokerError> for TxError {
    fn from(e: BrokerError) -> Self {
        Self::Broker(e)
    }
}

/// `Transaction.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Prepared,
    Committed,
    RolledBack,
    Suspended,
    RollbackOnly,
}

impl TxState {
    pub fn name(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Prepared => "PREPARED",
            Self::Committed => "COMMITTED",
            Self::RolledBack => "ROLLEDBACK",
            Self::Suspended => "SUSPENDED",
            Self::RollbackOnly => "ROLLBACK_ONLY",
        }
    }
}

/// A commit or prepare that failed: why, and the rollback it became.
pub type Failed = Box<(BrokerError, Outcome)>;

/// `TransactionImpl`.
#[derive(Debug)]
pub struct Transaction {
    /// The journal transaction ID.
    pub id: i64,
    pub xid: Option<Xid>,
    /// `containsPersistent`: the transaction wrote to the journal.
    pub persistent: bool,
    pub state: TxState,
    /// Routed sends, added to their queues on commit.
    pub sends: Vec<(Queue, MessageRef)>,
    /// Acknowledged references, removed on commit, returned on rollback.
    pub acks: Vec<(Queue, MessageRef)>,
    /// Why the transaction can only roll back.
    pub exception: Option<BrokerError>,
    /// Duplicate IDs that join their caches on commit.
    pub duplicates: Vec<crate::duplicate::PendingId>,
    /// `createTime`, in epoch milliseconds.
    pub created: i64,
    /// The XA timeout the client set; -1 for the resource manager's.
    pub timeout_seconds: i32,
    /// What it did with pages.
    pub paging: crate::paging::TxPaging,
    /// What `sync` mirrors complete once their peers have its messages,
    /// waited for once it commits.
    pub replicated: Vec<Pending>,
}

pub type TxRef = Arc<Mutex<Transaction>>;

/// The record that completes a transaction in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxRecord {
    Commit(i64),
    Rollback(i64),
}

/// What a finished transaction leaves to do: the record to journal, then
/// the queue updates.
#[derive(Debug, Default)]
pub struct Outcome {
    pub record: Option<TxRecord>,
    /// Commit: references to add to their queues.
    pub add: Vec<(Queue, MessageRef)>,
    /// Commit: references acknowledged for good, per queue.
    pub acked: Vec<(Queue, MessageRef)>,
    /// Rollback: references to return to their queues.
    pub cancel: Vec<(Queue, MessageRef)>,
    /// Commit: duplicate IDs for their caches.
    pub duplicates: Vec<crate::duplicate::PendingId>,
    /// The transaction's paging, committed or rolled back with it.
    pub paging: crate::paging::TxPaging,
    pub committed: bool,
    /// Commit: the `sync` mirror waits of its messages.
    pub replicated: Vec<Pending>,
}

impl Outcome {
    /// Journals the commit or rollback record, if the transaction needs
    /// one (`doCommit` / `doRollback`).
    pub fn write(&mut self, storage: Option<&Storage>) -> Result<Option<Pending>, BrokerError> {
        let (Some(storage), Some(record)) = (storage, self.record.take()) else {
            return Ok(None);
        };
        let io = |e: saranga_journal::JournalError| {
            BrokerError::new(saranga_core_protocol::ErrorType::IoError, e.to_string())
        };
        let r = match record {
            TxRecord::Commit(id) => {
                self.paging.before_commit(storage, id).map_err(io)?;
                storage.commit(id)
            }
            TxRecord::Rollback(id) => storage.rollback(id),
        };
        r.map(Some)
            .map_err(|e| BrokerError::new(saranga_core_protocol::ErrorType::IoError, e.to_string()))
    }

    /// Applies the outcome to the queues (`afterCommit` / `afterRollback`):
    /// committed sends join their queues, committed acknowledgements are
    /// final, rolled-back references return to their queues, which check
    /// them for redelivery. Returns the `sync` mirror waits.
    pub fn apply(self) -> Vec<Pending> {
        for d in self.duplicates {
            d.apply();
        }
        for (q, r) in self.add {
            q.add(r);
        }
        if self.committed {
            self.paging.after_commit();
        } else {
            self.paging.after_rollback();
        }
        let mut replicated = self.replicated;
        for (q, r) in self.acked {
            replicated.extend(q.post_acknowledge(&r));
        }
        return_refs(self.cancel);
        replicated
    }
}

/// `StorageManager.updateDeliveryCount` for a reference going back to its
/// queue after a rollback.
pub fn persist_delivery_count(q: &Queue, r: &mut MessageRef) {
    if !q.is_durable_ref(r) {
        return;
    }
    let Some(s) = q.ids().storage() else { return };
    match s.update_delivery_count(q.id(), r.message_id(), r.delivery_count, r.persisted_count) {
        Ok(true) => r.persisted_count = r.delivery_count,
        Ok(false) => {}
        Err(e) => tracing::warn!(
            "AMQ222178: Cannot find message {} on the journal: {e}",
            r.message_id()
        ),
    }
}

/// Returns references to their queues, each queue's in one batch.
pub fn return_refs(refs: Vec<(Queue, MessageRef)>) {
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

fn invalid_state(state: TxState) -> BrokerError {
    BrokerError::illegal_state(format!("Transaction is in invalid state {}", state.name()))
}

impl Transaction {
    pub fn new(id: i64, xid: Option<Xid>) -> Self {
        Self {
            id,
            xid,
            persistent: false,
            state: TxState::Active,
            sends: Vec::new(),
            acks: Vec::new(),
            exception: None,
            duplicates: Vec::new(),
            created: crate::message::now_millis(),
            timeout_seconds: -1,
            paging: crate::paging::TxPaging::default(),
            replicated: Vec::new(),
        }
    }

    pub fn new_ref(id: i64, xid: Option<Xid>) -> TxRef {
        Arc::new(Mutex::new(Self::new(id, xid)))
    }

    /// `markAsRollbackOnly`: the first reason sticks.
    pub fn mark_rollback_only(&mut self, e: &BrokerError) {
        if self.is_effective() {
            return;
        }
        if self.state != TxState::RollbackOnly {
            self.exception = Some(e.clone());
        }
        self.state = TxState::RollbackOnly;
    }

    /// `isEffective`: committed or rolled back.
    pub fn is_effective(&self) -> bool {
        matches!(self.state, TxState::Committed | TxState::RolledBack)
    }

    /// `internalRollback`. A rollback record is journaled for persistent
    /// work, or to end a prepared branch.
    fn internal_rollback(&mut self) -> Outcome {
        let journaled = self.persistent || (self.xid.is_some() && self.state == TxState::Prepared);
        self.state = TxState::RolledBack;
        self.sends.clear();
        self.duplicates.clear();
        self.replicated.clear();
        Outcome {
            record: journaled.then_some(TxRecord::Rollback(self.id)),
            cancel: std::mem::take(&mut self.acks),
            paging: std::mem::take(&mut self.paging),
            ..Outcome::default()
        }
    }

    /// `TransactionImpl.commit(onePhase)`. A rollback-only transaction rolls
    /// back and fails with the reason it was marked.
    pub fn commit(&mut self, one_phase: bool) -> Result<Outcome, Failed> {
        if self.state == TxState::Committed {
            return Ok(Outcome::default());
        }
        if self.state == TxState::RollbackOnly {
            let outcome = self.internal_rollback();
            return match self.exception.clone() {
                Some(e) => Err(Box::new((e, outcome))),
                None => Ok(outcome),
            };
        }
        let valid = if self.xid.is_some() {
            if one_phase {
                self.state == TxState::Active
            } else {
                self.state == TxState::Prepared
            }
        } else {
            self.state == TxState::Active
        };
        if !valid {
            return Err(Box::new((invalid_state(self.state), Outcome::default())));
        }
        // doCommit: a commit record for persistent work (paged messages'
        // page transaction among it), or to end a prepared branch.
        let journaled = self.persistent
            || !self.paging.page_txs.is_empty()
            || (self.xid.is_some() && self.state == TxState::Prepared);
        self.state = TxState::Committed;
        Ok(Outcome {
            record: journaled.then_some(TxRecord::Commit(self.id)),
            add: std::mem::take(&mut self.sends),
            acked: std::mem::take(&mut self.acks),
            cancel: Vec::new(),
            duplicates: std::mem::take(&mut self.duplicates),
            paging: std::mem::take(&mut self.paging),
            committed: true,
            replicated: std::mem::take(&mut self.replicated),
        })
    }

    /// `TransactionImpl.rollback`.
    pub fn rollback(&mut self) -> Result<Outcome, BrokerError> {
        if self.state == TxState::RolledBack {
            return Ok(Outcome::default());
        }
        let valid = if self.xid.is_some() {
            matches!(
                self.state,
                TxState::Prepared | TxState::Active | TxState::RollbackOnly
            )
        } else {
            matches!(self.state, TxState::Active | TxState::RollbackOnly)
        };
        if !valid {
            return Err(invalid_state(self.state));
        }
        Ok(self.internal_rollback())
    }

    /// `tryRollback`: rolls back unless prepared. `None` when it did not.
    pub fn try_rollback(&mut self) -> Option<Outcome> {
        match self.state {
            TxState::RolledBack => Some(Outcome::default()),
            TxState::Prepared => None,
            _ => Some(self.internal_rollback()),
        }
    }

    /// `TransactionImpl.prepare`. A rollback-only transaction rolls back
    /// instead, failing with its reason.
    pub fn prepare(&mut self) -> Result<(), Failed> {
        if self.is_effective() {
            return Ok(());
        }
        if self.state == TxState::RollbackOnly {
            let outcome = self.internal_rollback();
            return match self.exception.clone() {
                Some(e) => Err(Box::new((e, outcome))),
                None => Err(Box::new((invalid_state(TxState::RolledBack), outcome))),
            };
        }
        if self.state != TxState::Active {
            return Err(Box::new((invalid_state(self.state), Outcome::default())));
        }
        if self.xid.is_none() {
            return Err(Box::new((
                BrokerError::illegal_state("Cannot prepare non XA transaction"),
                Outcome::default(),
            )));
        }
        self.state = TxState::Prepared;
        Ok(())
    }
}

/// Locks a transaction; a panic while holding it leaves it usable.
pub fn lock(tx: &TxRef) -> MutexGuard<'_, Transaction> {
    tx.lock().unwrap_or_else(|e| e.into_inner())
}

/// `XidImpl.toString`, used in XA error messages. Java starts with the
/// object's identity hash; this uses a hash of the Xid's content.
pub fn xid_string(xid: &Xid) -> String {
    let rep = |b: &[u8]| {
        b.iter()
            .map(|&x| (x as i8).to_string())
            .collect::<Vec<_>>()
            .join(".")
    };
    let hash = xid
        .branch_qualifier
        .iter()
        .chain(&xid.global_transaction_id)
        .fold(xid.format_id as u32, |h, &b| {
            h.wrapping_mul(31).wrapping_add(b as u32)
        });
    format!(
        "XidImpl ({} bq:{} formatID:{} gtxid:{} base64:{}",
        hash & 0x7fff_ffff,
        rep(&xid.branch_qualifier),
        xid.format_id,
        rep(&xid.global_transaction_id),
        base64_url(&xid_bytes(xid))
    )
}

/// `XidImpl.toByteArray`: branch qualifier, global ID, then the format ID's
/// bytes as Java computes them (`% 0xFF`, not a mask).
pub fn xid_bytes(xid: &Xid) -> Vec<u8> {
    let mut b = xid.branch_qualifier.clone();
    b.extend_from_slice(&xid.global_transaction_id);
    for i in 0..4 {
        b.push(((xid.format_id >> (i * 8)) % 0xFF) as i8 as u8);
    }
    b
}

/// `XidImpl.toBase64String`.
pub fn xid_base64(xid: &Xid) -> String {
    base64_url(&xid_bytes(xid))
}

/// `java.util.Base64.getUrlEncoder()`: URL-safe alphabet with padding.
fn base64_url(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let n = chunk.len();
        let v = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= n {
                out.push(A[((v >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// `ResourceManagerImpl`: XA transactions by Xid, shared by all sessions.
pub struct ResourceManager {
    txs: Mutex<HashMap<Xid, TxRef>>,
    /// `transaction-timeout` in seconds.
    timeout_seconds: i32,
    /// Branches committed or rolled back by management, until the
    /// transaction manager forgets them (`heuristicCompletions`).
    heuristics: Mutex<Vec<Heuristic>>,
}

/// `HeuristicCompletionHolder`.
#[derive(Debug, Clone)]
pub struct Heuristic {
    /// Its `HEURISTIC_COMPLETION` record.
    pub record: i64,
    pub xid: Xid,
    pub commit: bool,
}

impl Default for ResourceManager {
    fn default() -> Self {
        Self::new(Self::TIMEOUT_SECONDS)
    }
}

impl ResourceManager {
    /// `ActiveMQDefaultConfiguration.DEFAULT_TRANSACTION_TIMEOUT` (300 s).
    pub const TIMEOUT_SECONDS: i32 = 300;

    pub fn new(timeout_seconds: i32) -> Self {
        Self {
            txs: Mutex::default(),
            timeout_seconds,
            heuristics: Mutex::default(),
        }
    }

    fn heuristic_list(&self) -> MutexGuard<'_, Vec<Heuristic>> {
        self.heuristics.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `putHeuristicCompletion`.
    pub fn put_heuristic(&self, record: i64, xid: Xid, commit: bool) {
        self.heuristic_list().push(Heuristic {
            record,
            xid,
            commit,
        });
    }

    /// Whether `xid` was completed by management: committed (true) or
    /// rolled back (false).
    pub fn heuristic(&self, xid: &Xid) -> Option<bool> {
        self.heuristic_list()
            .iter()
            .find(|h| &h.xid == xid)
            .map(|h| h.commit)
    }

    /// `removeHeuristicCompletion`: the record to delete, if `xid` was
    /// completed by management.
    pub fn remove_heuristic(&self, xid: &Xid) -> Option<i64> {
        let mut all = self.heuristic_list();
        let i = all.iter().position(|h| &h.xid == xid)?;
        Some(all.remove(i).record)
    }

    /// `getHeuristicCommittedTransactions` / `getHeuristicRolledbackTransactions`.
    pub fn heuristics(&self, commit: bool) -> Vec<Xid> {
        self.heuristic_list()
            .iter()
            .filter(|h| h.commit == commit)
            .map(|h| h.xid.clone())
            .collect()
    }

    /// `getPreparedTransactionsWithCreationTime`, oldest first.
    pub fn prepared(&self) -> Vec<(Xid, TxRef)> {
        let mut all: Vec<(Xid, TxRef, i64)> = self
            .map()
            .iter()
            .filter_map(|(x, tx)| {
                let t = lock(tx);
                (t.state == TxState::Prepared).then(|| (x.clone(), tx.clone(), t.created))
            })
            .collect();
        all.sort_by_key(|(_, _, created)| *created);
        all.into_iter().map(|(x, tx, _)| (x, tx)).collect()
    }

    /// `getTimeoutSeconds`.
    /// `getPreparedTransactionMessageCount`: how many acknowledgements of
    /// the queue `queue_id` prepared transactions hold.
    pub fn prepared_acks_on(&self, queue_id: i64) -> usize {
        let txs: Vec<TxRef> = self
            .txs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        txs.iter()
            .map(|t| {
                let t = lock(t);
                if t.state != TxState::Prepared {
                    return 0;
                }
                t.acks.iter().filter(|(q, _)| q.id() == queue_id).count()
            })
            .sum()
    }

    pub fn timeout_seconds(&self) -> i32 {
        self.timeout_seconds
    }

    /// `TxTimeoutHandler`: takes out every unprepared branch older than its
    /// timeout, marked rollback-only; the caller rolls them back.
    pub fn take_timed_out(&self, now: i64) -> Vec<TxRef> {
        let mut map = self.map();
        let timed_out: Vec<Xid> = map
            .iter()
            .filter(|(_, tx)| {
                let mut t = lock(tx);
                let secs = if t.timeout_seconds == -1 {
                    self.timeout_seconds
                } else {
                    t.timeout_seconds
                };
                let out = t.state != TxState::Prepared && now > t.created + i64::from(secs) * 1000;
                if out {
                    t.mark_rollback_only(&BrokerError::new(
                        saranga_core_protocol::ErrorType::TransactionTimeout,
                        "",
                    ));
                }
                out
            })
            .map(|(x, _)| x.clone())
            .collect();
        timed_out
            .into_iter()
            .filter_map(|x| {
                tracing::warn!(
                    "AMQ222103: transaction with xid {} timed out",
                    xid_string(&x)
                );
                map.remove(&x)
            })
            .collect()
    }

    fn map(&self) -> MutexGuard<'_, HashMap<Xid, TxRef>> {
        self.txs.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn get(&self, xid: &Xid) -> Option<TxRef> {
        self.map().get(xid).cloned()
    }

    /// `putTransaction`: false when the Xid is already known.
    pub fn put(&self, xid: Xid, tx: TxRef) -> bool {
        use std::collections::hash_map::Entry;
        match self.map().entry(xid) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(tx);
                true
            }
        }
    }

    pub fn remove(&self, xid: &Xid) -> Option<TxRef> {
        self.map().remove(xid)
    }

    /// `getInDoubtTransactions`: the prepared ones, then those management
    /// committed, then those it rolled back.
    pub fn in_doubt(&self) -> Vec<Xid> {
        let mut xids: Vec<Xid> = self
            .map()
            .iter()
            .filter(|(_, tx)| lock(tx).state == TxState::Prepared)
            .map(|(x, _)| x.clone())
            .collect();
        xids.extend(self.heuristics(true));
        xids.extend(self.heuristics(false));
        xids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xid(n: u8) -> Xid {
        Xid {
            format_id: 1,
            branch_qualifier: vec![n],
            global_transaction_id: vec![n],
        }
    }

    #[test]
    fn local_commit_rollback_states() {
        let mut t = Transaction::new(1, None);
        assert!(t.commit(false).is_ok());
        assert_eq!(t.state, TxState::Committed);
        assert!(t.rollback().is_err());
        let mut t = Transaction::new(1, None);
        assert!(t.prepare().is_err());
    }

    #[test]
    fn xa_two_phase_needs_prepare() {
        let mut t = Transaction::new(1, Some(xid(1)));
        let (e, _) = *t.commit(false).unwrap_err();
        assert_eq!(e.message, "Transaction is in invalid state ACTIVE");
        t.prepare().unwrap();
        assert!(t.commit(true).is_err());
        assert!(t.try_rollback().is_none());
        let o = t.commit(false).unwrap();
        assert_eq!(o.record, Some(TxRecord::Commit(1)));
    }

    #[test]
    fn only_persistent_or_prepared_work_is_journaled() {
        let mut t = Transaction::new(3, None);
        assert_eq!(t.commit(true).unwrap().record, None);
        let mut t = Transaction::new(4, None);
        t.persistent = true;
        assert_eq!(t.rollback().unwrap().record, Some(TxRecord::Rollback(4)));
        let mut t = Transaction::new(5, Some(xid(5)));
        t.prepare().unwrap();
        assert_eq!(t.rollback().unwrap().record, Some(TxRecord::Rollback(5)));
    }

    #[test]
    fn rollback_only_fails_commit_with_its_reason() {
        let mut t = Transaction::new(1, None);
        t.mark_rollback_only(&BrokerError::illegal_state("first"));
        t.mark_rollback_only(&BrokerError::illegal_state("second"));
        let (e, _) = *t.commit(false).unwrap_err();
        assert_eq!(e.message, "first");
        assert_eq!(t.state, TxState::RolledBack);
    }

    #[test]
    fn xid_formatting() {
        assert_eq!(base64_url(b"ab"), "YWI=");
        assert_eq!(base64_url(&[0xfb, 0xff]), "-_8=");
        let x = Xid {
            format_id: 1,
            branch_qualifier: vec![1, 0xff],
            global_transaction_id: vec![7],
        };
        assert_eq!(xid_bytes(&x), vec![1, 0xff, 7, 1, 0, 0, 0]);
        assert!(xid_string(&x).contains(" bq:1.-1 formatID:1 gtxid:7 base64:Af8HAQAAAA=="));
    }

    #[test]
    fn resource_manager_in_doubt() {
        let rm = ResourceManager::default();
        let a = Transaction::new_ref(1, Some(xid(1)));
        assert!(rm.put(xid(1), a.clone()));
        assert!(!rm.put(xid(1), a.clone()));
        rm.put(xid(2), Transaction::new_ref(2, Some(xid(2))));
        lock(&a).prepare().unwrap();
        assert_eq!(rm.in_doubt(), vec![xid(1)]);
    }
}
