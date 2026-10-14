//! Shared-nothing replication: a primary sends everything it writes to a
//! backup, which keeps a copy of its data and becomes active on it when the
//! primary fails. Ported from `ReplicationManager`, `ReplicatedJournal`,
//! `ReplicationEndpoint`, `SharedNothingPrimaryActivation` and
//! `SharedNothingBackupActivation`.
//!
//! The primary and its backup talk on channel 2 of the connection the
//! backup registered on. Every packet the primary sends gets exactly one
//! response, in order, so the primary keeps a queue of what waits for each
//! one: a client's answer waits for its journal write and for the backup's
//! response to its replication, or to the last replication before it.

pub mod backup;
pub mod pluggable;
pub mod primary;
pub mod quorum;

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use saranga_core_protocol::Packet;
use saranga_core_protocol::packets::replication::{
    AddOperationType, ReplicationAddMessage, ReplicationAddTXMessage, ReplicationCommitMessage,
    ReplicationDeleteMessage, ReplicationDeleteTXMessage, ReplicationPrepareMessage,
};
use saranga_journal::format::Record;
use saranga_journal::{Completion, JournalError};

use crate::core_protocol::ConnShared;

/// `ChannelImpl.CHANNEL_ID.REPLICATION`.
pub(crate) const CHANNEL_REPLICATION: i64 = 2;

/// `AbstractJournalStorageManager.JournalContent`: the journal a replicated
/// record belongs to.
pub(crate) const BINDINGS: i8 = 0;
pub(crate) const MESSAGES: i8 = 1;

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `done` once every one of `n` parts has run, with the first error.
fn join(done: Completion, n: usize) -> Vec<Completion> {
    struct Join {
        left: usize,
        error: Option<JournalError>,
        done: Option<Completion>,
    }
    let state = Arc::new(Mutex::new(Join {
        left: n,
        error: None,
        done: Some(done),
    }));
    (0..n)
        .map(|_| {
            let state = state.clone();
            Box::new(move |r: Result<(), JournalError>| {
                let finished = {
                    let mut s = lock(&state);
                    if let Err(e) = r
                        && s.error.is_none()
                    {
                        s.error = Some(e);
                    }
                    s.left -= 1;
                    if s.left == 0 {
                        s.done.take().map(|d| (d, s.error.take()))
                    } else {
                        None
                    }
                };
                if let Some((d, error)) = finished {
                    d(error.map_or(Ok(()), Err));
                }
            }) as Completion
        })
        .collect()
}

struct Tokens {
    /// What waits for each response still to come (`pendingTokens`).
    pending: VecDeque<Vec<Completion>>,
    stopped: bool,
}

/// The primary's side of one backup's channel (`ReplicationManager`):
/// packets go out in order, and each response releases what waited for
/// the packet it answers.
pub struct Replicator {
    conn: Arc<ConnShared>,
    tokens: Mutex<Tokens>,
    /// Signalled as responses arrive, for the synchronization's flow
    /// control.
    answered: Condvar,
    /// The backup has its copy (`isReplicaSync`).
    synchronized: std::sync::atomic::AtomicBool,
}

impl Replicator {
    pub(crate) fn new(conn: Arc<ConnShared>) -> Arc<Self> {
        Arc::new(Self {
            conn,
            tokens: Mutex::new(Tokens {
                pending: VecDeque::new(),
                stopped: false,
            }),
            answered: Condvar::new(),
            synchronized: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The backup's channel version (`getChannelVersion`).
    pub(crate) fn version(&self) -> i32 {
        self.conn.version()
    }

    /// `sendReplicatePacket`: `done` runs once the backup has answered.
    pub(crate) fn send(&self, packet: impl Into<Packet>, done: Option<Completion>) {
        let mut t = lock(&self.tokens);
        if t.stopped {
            drop(t);
            if let Some(d) = done {
                d(Ok(()));
            }
            return;
        }
        t.pending.push_back(done.into_iter().collect());
        // Under the lock, so that packets go out in the order of their
        // tokens.
        self.conn.send(CHANNEL_REPLICATION, packet);
    }

    /// `done` runs once the backup has answered everything sent so far.
    pub(crate) fn barrier(&self, done: Completion) {
        let mut t = lock(&self.tokens);
        let stopped = t.stopped;
        match t.pending.back_mut() {
            Some(last) if !stopped => last.push(done),
            _ => {
                drop(t);
                done(Ok(()));
            }
        }
    }

    /// A completion that runs `done` once the local write it is given to
    /// is done and the backup has everything sent so far.
    pub(crate) fn join_barrier(&self, done: Completion) -> Completion {
        let mut parts = join(done, 2);
        let local = parts.pop().expect("two parts");
        self.barrier(parts.pop().expect("two parts"));
        local
    }

    /// `ResponseHandler`: the backup answered the oldest packet.
    pub(crate) fn replicated(&self) {
        let done = {
            let mut t = lock(&self.tokens);
            t.pending.pop_front()
        };
        self.answered.notify_all();
        match done {
            Some(done) => {
                for d in done {
                    d(Ok(()));
                }
            }
            None => tracing::warn!("AMQ222228: Missing replication token on queue"),
        }
    }

    /// `stop` and `clearReplicationTokens`: nothing more is sent, and
    /// everything waiting for the backup is released.
    pub(crate) fn stop(&self) {
        let pending = {
            let mut t = lock(&self.tokens);
            t.stopped = true;
            std::mem::take(&mut t.pending)
        };
        self.answered.notify_all();
        for done in pending.into_iter().flatten() {
            done(Ok(()));
        }
    }

    pub(crate) fn is_synchronized(&self) -> bool {
        self.synchronized.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_stopped(&self) -> bool {
        lock(&self.tokens).stopped
    }

    /// Blocks while more than `window` packets wait for an answer: the
    /// synchronization does not run ahead of the backup. `false` once
    /// replication has stopped or `timeout` passed without an answer.
    pub(crate) fn wait_window(&self, window: usize, timeout: Duration) -> bool {
        let mut t = lock(&self.tokens);
        let mut deadline = Instant::now() + timeout;
        let mut seen = t.pending.len();
        while !t.stopped && t.pending.len() > window {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            t = self
                .answered
                .wait_timeout(t, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            // Any answer restarts the wait.
            if t.pending.len() < seen {
                seen = t.pending.len();
                deadline = Instant::now() + timeout;
            }
        }
        !t.stopped
    }
}

/// `ReplicatedJournal`: a journal's appends, replicated.
pub(crate) struct JournalReplicator {
    pub(crate) journal: i8,
    pub(crate) replicator: Arc<Replicator>,
}

impl JournalReplicator {
    fn packet(&self, record: &Record) -> Packet {
        let journal_id = self.journal;
        let add = |operation, id: &i64, user_type: &u8, data: &Vec<u8>| -> Packet {
            ReplicationAddMessage {
                journal_id,
                operation,
                id: *id,
                journal_record_type: *user_type as i8,
                record_data: data.clone(),
            }
            .into()
        };
        let add_tx = |operation, tx: &i64, id: &i64, user_type: &u8, data: &Vec<u8>| -> Packet {
            ReplicationAddTXMessage {
                journal_id,
                operation,
                tx_id: *tx,
                id: *id,
                record_type: *user_type as i8,
                record_data: data.clone(),
            }
            .into()
        };
        match record {
            Record::Event {
                id,
                user_type,
                data,
            } => add(AddOperationType::Event, id, user_type, data),
            Record::Add {
                id,
                user_type,
                data,
            } => add(AddOperationType::Add, id, user_type, data),
            Record::Update {
                id,
                user_type,
                data,
            } => add(AddOperationType::Update, id, user_type, data),
            Record::AddTx {
                tx,
                id,
                user_type,
                data,
            } => add_tx(AddOperationType::Add, tx, id, user_type, data),
            Record::UpdateTx {
                tx,
                id,
                user_type,
                data,
            } => add_tx(AddOperationType::Update, tx, id, user_type, data),
            Record::Delete { id } => ReplicationDeleteMessage {
                journal_id,
                id: *id,
            }
            .into(),
            Record::DeleteTx { tx, id, data } => ReplicationDeleteTXMessage {
                journal_id,
                tx_id: *tx,
                id: *id,
                record_data: data.clone(),
            }
            .into(),
            Record::Prepare { tx, data, .. } => ReplicationPrepareMessage {
                journal_id,
                tx_id: *tx,
                record_data: data.clone(),
            }
            .into(),
            Record::Commit { tx, .. } => ReplicationCommitMessage {
                journal_id,
                rollback: false,
                tx_id: *tx,
            }
            .into(),
            Record::Rollback { tx } => ReplicationCommitMessage {
                journal_id,
                rollback: true,
                tx_id: *tx,
            }
            .into(),
        }
    }
}

impl saranga_journal::Replicator for JournalReplicator {
    fn replicate(&self, record: &Record, done: Option<Completion>) -> Option<Completion> {
        let packet = self.packet(record);
        match done {
            None => {
                self.replicator.send(packet, None);
                None
            }
            Some(done) => {
                let mut parts = join(done, 2);
                let local = parts.pop().expect("two parts");
                self.replicator.send(packet, parts.pop());
                Some(local)
            }
        }
    }

    fn barrier(&self, done: Completion) -> Completion {
        self.replicator.join_barrier(done)
    }
}

/// Where a store's page files and large messages are replicated, while a
/// backup is attached.
#[derive(Default)]
pub(crate) struct Slot(Mutex<Option<Arc<Replicator>>>);

impl Slot {
    pub(crate) fn get(&self) -> Option<Arc<Replicator>> {
        lock(&self.0).clone().filter(|r| !r.is_stopped())
    }

    pub(crate) fn set(&self, r: Option<Arc<Replicator>>) {
        *lock(&self.0) = r;
    }
}
