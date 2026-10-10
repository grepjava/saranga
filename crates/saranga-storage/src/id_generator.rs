//! `BatchingIDGenerator`: the storage manager's ID generator. Record IDs,
//! message IDs, transaction IDs and queue IDs all come from it.
//!
//! IDs are handed out from an in-memory counter. Only when the counter
//! reaches the end of the current batch is a checkpoint stored: an
//! `ID_COUNTER_RECORD` in the bindings journal whose body is the batch's
//! upper bound. On load the newest checkpoint says where to resume, and the
//! older checkpoint records are deleted. On a clean stop the current ID is
//! stored, so the next start resumes right after it.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, RwLock};

use saranga_buffer::Result;

use crate::codec::{IdCounterEncoding, RecordCodec};

/// `AbstractJournalStorageManager.CHECKPOINT_BATCH_SIZE`: the server stores
/// a checkpoint every `Integer.MAX_VALUE` IDs, so in practice only at the
/// first ID and at shutdown.
pub const CHECKPOINT_BATCH_SIZE: i64 = i32::MAX as i64;

/// Where the generator's records go: the bindings journal.
pub trait IdCounterStore: Send + Sync {
    /// `StorageManager.storeID`: append a synced add record with record ID
    /// `journal_id`, type `ID_COUNTER_RECORD` and body
    /// `IdCounterEncoding { id }` (see [`BatchingIdGenerator::encode_counter`]).
    /// Failures are the store's to log, as `batchingIdError` does.
    fn store_id(&self, journal_id: i64, id: i64);
    /// `StorageManager.deleteID`: delete the record `journal_id`
    /// (asynchronously).
    fn delete_id(&self, journal_id: i64);
    /// `StorageManager.isStarted`: when false, a checkpoint is not stored
    /// (IDs generated after shutdown, e.g. for notifications).
    fn is_started(&self) -> bool {
        true
    }
}

/// `idGeneratorStopped`: an ID was requested after `stop`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("AMQ229257: IDGenerator has been stopped")]
pub struct IdGeneratorStopped;

/// `BatchingIDGenerator`.
pub struct BatchingIdGenerator<S> {
    counter: AtomicI64,
    checkpoint_size: i64,
    /// The end of the current batch.
    next_id: AtomicI64,
    /// `saveCheckPoint` is synchronized.
    checkpoint: Mutex<()>,
    /// The read/write lock and the `started` flag it guards.
    started: RwLock<bool>,
    /// Checkpoint record IDs found by `load_state`, in load order.
    cleanup_records: Mutex<Option<Vec<i64>>>,
    store: S,
}

impl<S: IdCounterStore> BatchingIdGenerator<S> {
    /// `new BatchingIDGenerator(start, checkpointSize, storageManager)`.
    /// The server uses `new(0, CHECKPOINT_BATCH_SIZE, store)`.
    pub fn new(start: i64, checkpoint_size: i64, store: S) -> Self {
        Self {
            counter: AtomicI64::new(start),
            checkpoint_size,
            next_id: AtomicI64::new(start),
            checkpoint: Mutex::new(()),
            started: RwLock::new(true),
            cleanup_records: Mutex::new(None),
            store,
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// The body of an `ID_COUNTER_RECORD` (`createIDEncodingSupport`).
    pub fn encode_counter(id: i64) -> Vec<u8> {
        IdCounterEncoding { id }.encode()
    }

    /// `loadState`: an `ID_COUNTER_RECORD` read from the bindings journal.
    /// Call it for every such record in load order; the last one wins.
    pub fn load_state(&self, journal_id: i64, data: &[u8]) -> Result<()> {
        let encoding = IdCounterEncoding::decode(data)?;
        let mut started = self.started.write().unwrap_or_else(|e| e.into_inner());
        self.cleanup_records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(Vec::new)
            .push(journal_id);
        let next = encoding.id + 1;
        self.next_id.store(next, Ordering::SeqCst);
        self.counter.store(next, Ordering::SeqCst);
        *started = true;
        Ok(())
    }

    /// `cleanup`: after the bindings journal is loaded, deletes every
    /// checkpoint record but the last one loaded.
    pub fn cleanup(&self) {
        let records = self
            .cleanup_records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(records) = records
            && let Some((_, older)) = records.split_last()
        {
            for &id in older {
                self.store.delete_id(id);
            }
        }
    }

    /// `generateID`.
    pub fn generate_id(&self) -> std::result::Result<i64, IdGeneratorStopped> {
        let started = self.started.read().unwrap_or_else(|e| e.into_inner());
        if !*started {
            return Err(IdGeneratorStopped);
        }
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        if id >= self.next_id.load(Ordering::SeqCst) {
            self.save_checkpoint(id);
        }
        Ok(id)
    }

    /// `getCurrentID`: the next ID to be generated.
    pub fn current_id(&self) -> i64 {
        self.counter.load(Ordering::SeqCst)
    }

    /// `saveCheckPoint`: starts a new batch and stores its upper bound under
    /// a record ID taken from the counter itself.
    fn save_checkpoint(&self, id: i64) {
        let _guard = self.checkpoint.lock().unwrap_or_else(|e| e.into_inner());
        let next = self.next_id.load(Ordering::SeqCst);
        if id >= next {
            let next = next + self.checkpoint_size;
            self.next_id.store(next, Ordering::SeqCst);
            if self.store.is_started() {
                let journal_id = self.counter.fetch_add(1, Ordering::SeqCst);
                self.store.store_id(journal_id, next);
            }
        }
    }

    /// `persistCurrentID`: stores the current ID under a fresh record ID.
    pub fn persist_current_id(&self) {
        let record_id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.store.store_id(record_id, record_id);
    }

    /// `forceNextID`: continue from `next_id`, recording it.
    pub fn force_next_id(&self, next_id: i64) {
        let journal_id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.counter.store(next_id, Ordering::SeqCst);
        self.store.store_id(journal_id, next_id);
    }

    /// `stop`: persists the current ID; later `generate_id` calls fail.
    pub fn stop(&self) {
        let mut started = self.started.write().unwrap_or_else(|e| e.into_inner());
        self.persist_current_id();
        *started = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recorder {
        log: Mutex<Vec<String>>,
        stopped: std::sync::atomic::AtomicBool,
    }

    impl IdCounterStore for Recorder {
        fn store_id(&self, journal_id: i64, id: i64) {
            self.log
                .lock()
                .unwrap()
                .push(format!("store {journal_id}={id}"));
        }
        fn delete_id(&self, journal_id: i64) {
            self.log
                .lock()
                .unwrap()
                .push(format!("delete {journal_id}"));
        }
        fn is_started(&self) -> bool {
            !self.stopped.load(Ordering::SeqCst)
        }
    }

    fn log(g: &BatchingIdGenerator<Recorder>) -> Vec<String> {
        std::mem::take(&mut *g.store().log.lock().unwrap())
    }

    /// What the golden bindings journals show: a fresh server stores
    /// `recordID=1 IDCounterEncoding [id=2147483647]` for its first ID (0,
    /// used as a transaction ID), and on shutdown `recordID=N [id=N]`.
    #[test]
    fn fresh_server_matches_the_golden_bindings() {
        let g = BatchingIdGenerator::new(0, CHECKPOINT_BATCH_SIZE, Recorder::default());
        assert_eq!(g.generate_id(), Ok(0));
        assert_eq!(log(&g), ["store 1=2147483647"]);
        assert_eq!(g.generate_id(), Ok(2));
        for _ in 3..44 {
            g.generate_id().unwrap();
        }
        assert!(log(&g).is_empty());
        g.stop();
        assert_eq!(log(&g), ["store 45=45"]);
        assert_eq!(g.generate_id(), Err(IdGeneratorStopped));
    }

    #[test]
    fn reload_resumes_after_the_last_checkpoint_and_deletes_older_ones() {
        let g = BatchingIdGenerator::new(0, CHECKPOINT_BATCH_SIZE, Recorder::default());
        g.load_state(
            1,
            &BatchingIdGenerator::<Recorder>::encode_counter(i32::MAX as i64),
        )
        .unwrap();
        g.load_state(45, &45i64.to_be_bytes()).unwrap();
        g.load_state(80, &80i64.to_be_bytes()).unwrap();
        assert_eq!(g.current_id(), 81);
        g.cleanup();
        assert_eq!(log(&g), ["delete 1", "delete 45"]);
        // The batch starts at 81: the first ID stores a new checkpoint.
        assert_eq!(g.generate_id(), Ok(81));
        assert_eq!(
            log(&g),
            [format!("store 82={}", 81 + CHECKPOINT_BATCH_SIZE)]
        );
        assert_eq!(g.generate_id(), Ok(83));
        g.cleanup();
        assert!(log(&g).is_empty(), "cleanup runs once");
        assert!(g.load_state(1, &[0; 3]).is_err());
    }

    #[test]
    fn small_batches_checkpoint_at_every_batch_boundary() {
        let g = BatchingIdGenerator::new(0, 10, Recorder::default());
        let ids: Vec<i64> = (0..12).map(|_| g.generate_id().unwrap()).collect();
        assert_eq!(ids, [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13]);
        assert_eq!(log(&g), ["store 1=10", "store 11=20"]);
    }

    #[test]
    fn no_checkpoint_while_the_store_is_stopped() {
        let g = BatchingIdGenerator::new(5, 10, Recorder::default());
        g.store().stopped.store(true, Ordering::SeqCst);
        assert_eq!(g.generate_id(), Ok(5));
        assert_eq!(g.generate_id(), Ok(6));
        assert!(log(&g).is_empty());
    }

    #[test]
    fn force_next_id() {
        let g = BatchingIdGenerator::new(0, CHECKPOINT_BATCH_SIZE, Recorder::default());
        g.force_next_id(1000);
        assert_eq!(log(&g), ["store 1=1000"]);
        assert_eq!(g.current_id(), 1000);
    }
}
