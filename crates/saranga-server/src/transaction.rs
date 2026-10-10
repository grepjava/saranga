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
}

impl Outcome {
    /// Journals the commit or rollback record, if the transaction needs
    /// one (`doCommit` / `doRollback`).
    pub fn write(&mut self, storage: Option<&Storage>) -> Result<Option<Pending>, BrokerError> {
        let (Some(storage), Some(record)) = (storage, self.record.take()) else {
            return Ok(None);
        };
        let r = match record {
            TxRecord::Commit(id) => storage.commit(id),
            TxRecord::Rollback(id) => storage.rollback(id),
        };
        r.map(Some)
            .map_err(|e| BrokerError::new(saranga_core_protocol::ErrorType::IoError, e.to_string()))
    }

    /// Applies the outcome to the queues (`afterCommit` / `afterRollback`):
    /// committed sends join their queues, committed acknowledgements are
    /// final, rolled-back references return to their queues, which check
    /// them for redelivery.
    pub fn apply(self) {
        for d in self.duplicates {
            d.apply();
        }
        for (q, r) in self.add {
            q.add(r);
        }
        let mut acked: Vec<(Queue, usize)> = Vec::new();
        for (q, r) in self.acked {
            q.post_acknowledge(&r);
            match acked.iter_mut().find(|(a, _)| *a == q) {
                Some((_, n)) => *n += 1,
                None => acked.push((q, 1)),
            }
        }
        for (q, n) in acked {
            q.acked(n);
        }
        return_refs(self.cancel);
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
        Outcome {
            record: journaled.then_some(TxRecord::Rollback(self.id)),
            cancel: std::mem::take(&mut self.acks),
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
        // doCommit: a commit record for persistent work, or to end a
        // prepared branch.
        let journaled = self.persistent || (self.xid.is_some() && self.state == TxState::Prepared);
        self.state = TxState::Committed;
        Ok(Outcome {
            record: journaled.then_some(TxRecord::Commit(self.id)),
            add: std::mem::take(&mut self.sends),
            acked: std::mem::take(&mut self.acks),
            cancel: Vec::new(),
            duplicates: std::mem::take(&mut self.duplicates),
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
fn xid_bytes(xid: &Xid) -> Vec<u8> {
    let mut b = xid.branch_qualifier.clone();
    b.extend_from_slice(&xid.global_transaction_id);
    for i in 0..4 {
        b.push(((xid.format_id >> (i * 8)) % 0xFF) as i8 as u8);
    }
    b
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
#[derive(Default)]
pub struct ResourceManager {
    txs: Mutex<HashMap<Xid, TxRef>>,
}

impl ResourceManager {
    /// `ActiveMQDefaultConfiguration.DEFAULT_TRANSACTION_TIMEOUT` (300 s).
    pub const TIMEOUT_SECONDS: i32 = 300;

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

    /// `getInDoubtTransactions`: the prepared ones (heuristic completions
    /// come with management, Phase 8).
    pub fn in_doubt(&self) -> Vec<Xid> {
        self.map()
            .iter()
            .filter(|(_, tx)| lock(tx).state == TxState::Prepared)
            .map(|(x, _)| x.clone())
            .collect()
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
