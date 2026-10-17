//! The journal. Ported from `JournalImpl`, `JournalFilesRepository`,
//! `JournalFileImpl`, `JournalRecord`, `JournalTransaction` and `Reclaimer`.
//!
//! All state lives behind one mutex; file I/O for appends goes through the
//! flusher thread in [`crate::io`]. A file's accounting follows Artemis
//! exactly: every add, update and transactional record counts as a
//! "positive" in the file that holds it, every delete, commit and rollback
//! as a "negative" against the files it cancels, and a file is reclaimed
//! once all of its positives are cancelled by negatives in files that are
//! themselves reclaimable.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::control::{ControlFile, FILE_COMPACT_CONTROL};
use crate::format::{self, FileHeader, Record, SIZE_HEADER};
use crate::io::{self as fio, Completion, CriticalListener, DataFile, Writer, read_exact_at};
use crate::{JournalError, Result};

/// `JournalImpl.UPDATE_FACTOR`.
const UPDATE_FACTOR: f64 = 10.0;

/// Journal settings. The defaults are those of the Artemis message journal.
#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub directory: PathBuf,
    pub file_prefix: String,
    pub file_extension: String,
    pub file_size: usize,
    pub min_files: usize,
    /// Files kept for reuse; a negative value keeps all of them.
    pub pool_size: i32,
    /// Compaction runs only with more data files than this; 0 disables it.
    pub compact_min_files: usize,
    /// Compaction runs when live data is below this percentage of the data
    /// files' total size.
    pub compact_percentage: u32,
    pub user_version: i32,
    /// `journal-datasync`: `fdatasync` on synced appends.
    pub datasync: bool,
    /// `journal-buffer-size`; with the file size it bounds a record's size.
    /// 0 for no buffer (the bindings journal).
    pub buffer_size: usize,
    /// `setRemoveExtraFilesOnLoad`: delete empty files beyond the pool size
    /// on load instead of keeping them.
    pub remove_extra_files_on_load: bool,
    /// `journal-max-attic-files`.
    pub max_attic_files: usize,
    /// `setHistoryFolder`: keep a copy of each file the journal fills.
    pub retention: Option<Retention>,
}

/// Journal retention (`journal-retention-directory`): each file the journal
/// moves on from is copied to `directory` as
/// `<prefix>-<yyyyMMddHHmmss>-<file ID>.<extension>`, and copies older than
/// the period, or beyond the storage limit, are removed.
#[derive(Debug, Clone)]
pub struct Retention {
    pub directory: PathBuf,
    /// The storage limit in bytes; -1 for none.
    pub max_bytes: i64,
    /// Milliseconds; -1 for none.
    pub period_millis: i64,
    /// The local time zone's offset from UTC, in seconds, at an instant in
    /// milliseconds: the copies are named in local time, as Artemis's
    /// `GregorianCalendar` names them.
    pub local_offset: fn(i64) -> i64,
}

impl JournalConfig {
    /// The message journal (`activemq-data-N.amq`).
    pub fn messages(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            file_prefix: "activemq-data".into(),
            file_extension: "amq".into(),
            file_size: 10 * 1024 * 1024,
            min_files: 2,
            pool_size: -1,
            compact_min_files: 10,
            compact_percentage: 30,
            user_version: 0,
            datasync: true,
            buffer_size: 490 * 1024,
            remove_extra_files_on_load: true,
            max_attic_files: 10,
            retention: None,
        }
    }

    /// The bindings journal (`activemq-bindings-N.bindings`): 1 MiB files,
    /// no write buffer.
    pub fn bindings(directory: impl Into<PathBuf>) -> Self {
        Self {
            file_prefix: "activemq-bindings".into(),
            file_extension: "bindings".into(),
            file_size: 1024 * 1024,
            buffer_size: 0,
            ..Self::messages(directory)
        }
    }

    /// `getMaxRecordSize`: the largest record the journal takes.
    pub fn max_record_size(&self) -> usize {
        if self.buffer_size == 0 {
            self.file_size
        } else {
            self.file_size.min(self.buffer_size)
        }
    }
}

/// A record as loaded: `RecordInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordInfo {
    pub id: i64,
    pub user_record_type: u8,
    pub data: Vec<u8>,
    pub is_update: bool,
    pub replaceable_update: bool,
    pub compact_count: i16,
}

/// A transaction that was prepared and neither committed nor rolled back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTransactionInfo {
    pub id: i64,
    pub extra_data: Vec<u8>,
    pub records: Vec<RecordInfo>,
    pub records_to_delete: Vec<RecordInfo>,
}

/// `LoaderCallback`: receives the journal's content during [`Journal::load`].
pub trait LoaderCallback {
    fn add_record(&mut self, info: RecordInfo);
    fn update_record(&mut self, info: RecordInfo);
    fn delete_record(&mut self, id: i64);
    fn add_prepared_transaction(&mut self, tx: PreparedTransactionInfo);
    /// A transaction that was never completed; it has been rolled back.
    fn failed_transaction(
        &mut self,
        _tx: i64,
        _records: Vec<RecordInfo>,
        _to_delete: Vec<RecordInfo>,
    ) {
    }
}

/// `JournalLoadInformation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadInfo {
    pub number_of_records: usize,
    pub max_id: i64,
}

/// What [`Journal::load_all`] returns: the committed records in journal
/// order with deleted ones removed, as `load(List, List, ...)` does.
#[derive(Debug, Clone, Default)]
pub struct LoadResult {
    pub committed: Vec<RecordInfo>,
    pub prepared: Vec<PreparedTransactionInfo>,
    pub failed: Vec<i64>,
    pub max_id: i64,
}

/// A file of the journal, for tools and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalFileInfo {
    pub name: String,
    pub file_id: i64,
}

pub(crate) type FileKey = u64;

/// `JournalFileImpl`: a journal file and its reclaim accounting.
#[derive(Debug)]
pub(crate) struct JFile {
    pub(crate) id: i64,
    pub(crate) version: i32,
    pub(crate) name: String,
    handle: Option<Arc<DataFile>>,
    pos_count: i32,
    replaceable_count: i32,
    add_record: i32,
    live_size: i64,
    pos_reclaim: bool,
    neg_reclaim: bool,
    /// `setReclaimable`: false while a retention copy is pending.
    pub(crate) reclaimable: bool,
    neg_counts: HashMap<FileKey, i32>,
}

impl JFile {
    fn new(id: i64, version: i32, name: String) -> Self {
        Self {
            id,
            version,
            name,
            handle: None,
            pos_count: 0,
            replaceable_count: 0,
            add_record: 0,
            live_size: 0,
            pos_reclaim: false,
            neg_reclaim: false,
            reclaimable: true,
            neg_counts: HashMap::new(),
        }
    }

    pub(crate) fn record_id(&self) -> i32 {
        format::record_id_of(self.id)
    }

    fn can_reclaim(&self) -> bool {
        self.reclaimable && self.pos_reclaim && self.neg_reclaim
    }

    fn neg_count(&self, file: FileKey) -> i32 {
        self.neg_counts.get(&file).copied().unwrap_or(0)
    }
}

/// `JournalRecord`: where a live record and its updates are.
#[derive(Debug)]
pub(crate) struct RecordPos {
    add_file: FileKey,
    size: i64,
    /// (file, bytes, count), merged with the last entry when it is the same
    /// file, as `ObjIntIntArrayList.addToIntsIfMatch` does.
    updates: Vec<(FileKey, i64, i32)>,
}

/// `JournalTransaction`.
#[derive(Debug, Default)]
pub(crate) struct Tx {
    pos: Vec<TxUpdate>,
    neg: Vec<TxUpdate>,
    pending_files: Vec<FileKey>,
    last_file: Option<FileKey>,
    counter: i32,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TxUpdate {
    file: FileKey,
    id: i64,
    size: i64,
    replaceable: bool,
}

impl Tx {
    pub(crate) fn counter(&mut self, file: FileKey) -> i32 {
        if self.last_file != Some(file) {
            self.last_file = Some(file);
            self.counter = 0;
        }
        self.counter
    }

    fn inc_counter(&mut self, file: FileKey) {
        self.counter(file);
        self.counter += 1;
    }

    /// `setCompacting` / `clear`.
    pub(crate) fn clear(&mut self) {
        self.pos.clear();
        self.neg.clear();
        self.pending_files.clear();
        self.counter = 0;
        self.last_file = None;
    }

    /// `merge`.
    pub(crate) fn merge(&mut self, other: Tx) {
        self.pos.extend(other.pos);
        self.neg.extend(other.neg);
        for f in other.pending_files {
            if !self.pending_files.contains(&f) {
                self.pending_files.push(f);
            }
        }
    }
}

/// Files, records and transactions: everything the accounting touches.
#[derive(Debug, Default)]
pub(crate) struct Books {
    pub(crate) files: HashMap<FileKey, JFile>,
    next_key: FileKey,
    pub(crate) records: HashMap<i64, RecordPos>,
    pub(crate) transactions: HashMap<i64, Tx>,
    pub(crate) replaceable: HashSet<u8>,
}

impl Books {
    pub(crate) fn insert_file(&mut self, f: JFile) -> FileKey {
        self.next_key += 1;
        self.files.insert(self.next_key, f);
        self.next_key
    }

    pub(crate) fn file(&mut self, k: FileKey) -> &mut JFile {
        self.files.get_mut(&k).expect("journal file")
    }

    fn inc_neg(&mut self, on: FileKey, target: FileKey, delta: i32) {
        *self.file(on).neg_counts.entry(target).or_insert(0) += delta;
    }

    /// `new JournalRecord(file, size)`.
    pub(crate) fn add_record(&mut self, id: i64, file: FileKey, size: i64) {
        let f = self.file(file);
        f.pos_count += 1;
        f.live_size += size;
        f.add_record += 1;
        self.records.insert(
            id,
            RecordPos {
                add_file: file,
                size,
                updates: Vec::new(),
            },
        );
    }

    /// `JournalRecord.addUpdateFile`.
    pub(crate) fn add_update(&mut self, id: i64, file: FileKey, bytes: i64, replaceable: bool) {
        let Some(r) = self.records.get_mut(&id) else {
            return;
        };
        if bytes == 0 {
            return;
        }
        let merged = match r.updates.last_mut() {
            Some((f, b, c)) if *f == file => {
                *b += bytes;
                *c += 1;
                true
            }
            _ => {
                r.updates.push((file, bytes, 1));
                false
            }
        };
        let f = self.file(file);
        f.pos_count += 1;
        f.live_size += bytes;
        if !merged && replaceable {
            f.replaceable_count += 1;
        }
    }

    /// `records.remove(id).delete(file)`.
    pub(crate) fn delete_record(&mut self, id: i64, file: FileKey) -> bool {
        let Some(r) = self.records.remove(&id) else {
            return false;
        };
        self.inc_neg(file, r.add_file, 1);
        self.file(r.add_file).live_size -= r.size;
        for (uf, bytes, count) in r.updates {
            self.inc_neg(file, uf, count);
            self.file(uf).live_size -= bytes;
        }
        true
    }

    pub(crate) fn tx(&mut self, tx: i64) -> &mut Tx {
        self.transactions.entry(tx).or_default()
    }

    /// `JournalTransaction.addFile`.
    fn tx_add_file(&mut self, tx: i64, file: FileKey) {
        let t = self.tx(tx);
        if !t.pending_files.contains(&file) {
            t.pending_files.push(file);
            self.file(file).pos_count += 1;
        }
    }

    pub(crate) fn tx_add_positive(
        &mut self,
        tx: i64,
        file: FileKey,
        id: i64,
        size: i64,
        replaceable: bool,
    ) {
        self.tx(tx).inc_counter(file);
        self.tx_add_file(tx, file);
        self.tx(tx).pos.push(TxUpdate {
            file,
            id,
            size,
            replaceable,
        });
    }

    pub(crate) fn tx_add_negative(&mut self, tx: i64, file: FileKey, id: i64) {
        self.tx(tx).inc_counter(file);
        self.tx_add_file(tx, file);
        self.tx(tx).neg.push(TxUpdate {
            file,
            id,
            size: 0,
            replaceable: false,
        });
    }

    pub(crate) fn tx_prepare(&mut self, tx: i64, file: FileKey) {
        self.tx_add_file(tx, file);
    }

    /// `JournalTransaction.commit`, for a transaction already removed from
    /// the map.
    pub(crate) fn tx_commit(&mut self, t: Tx, file: FileKey) {
        for u in &t.pos {
            if self.records.contains_key(&u.id) {
                self.add_update(u.id, u.file, u.size, u.replaceable);
            } else {
                self.add_record(u.id, u.file, u.size);
            }
        }
        for u in &t.neg {
            self.delete_record(u.id, u.file);
        }
        for jf in t.pending_files {
            self.inc_neg(file, jf, 1);
        }
    }

    /// `JournalTransaction.rollback`.
    pub(crate) fn tx_rollback(&mut self, t: Tx, file: FileKey) {
        for jf in t.pending_files {
            self.inc_neg(file, jf, 1);
        }
    }

    /// `JournalTransaction.forget`.
    fn tx_forget(&mut self, t: Tx) {
        for jf in t.pending_files {
            self.file(jf).pos_count -= 1;
        }
    }

    /// `removeNegatives`: forget negatives pointing at a removed file.
    fn remove_negatives(&mut self, removed: FileKey, data_files: &VecDeque<FileKey>) {
        for k in data_files {
            if let Some(f) = self.files.get_mut(k) {
                f.neg_counts.remove(&removed);
            }
        }
    }
}

/// `Reclaimer.scan`.
fn reclaim_scan(books: &mut Books, files: &[FileKey]) {
    for i in 0..files.len() {
        let cur = files[i];
        if !books.files[&cur].neg_reclaim {
            let outstanding = (0..i).rev().any(|j| {
                let f = &books.files[&files[j]];
                !f.can_reclaim() && books.files[&cur].neg_count(files[j]) != 0
            });
            if outstanding {
                continue;
            }
            books.file(cur).neg_reclaim = true;
        }
        if !books.files[&cur].pos_reclaim {
            let pos = books.files[&cur].pos_count;
            let mut neg = 0;
            for f in &files[i..] {
                if neg >= pos {
                    break;
                }
                neg += books.files[f].neg_count(cur);
            }
            if neg >= pos {
                books.file(cur).pos_reclaim = true;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Started,
    Loaded,
    Failed,
}

pub(crate) struct Inner {
    pub(crate) cfg: JournalConfig,
    pub(crate) state: State,
    pub(crate) books: Books,
    pub(crate) data_files: VecDeque<FileKey>,
    free_files: VecDeque<FileKey>,
    opened_files: VecDeque<FileKey>,
    /// Opened files still being made, by the thread making each
    /// (`pushOpenedFile` on the files executor).
    creating: HashMap<FileKey, std::thread::JoinHandle<Result<()>>>,
    current: Option<FileKey>,
    position: u64,
    next_file_id: i64,
    auto_reclaim: bool,
    scratch: Vec<u8>,
    /// `historyPendingFiles`: files waiting for their retention copy.
    pub(crate) history_pending: Vec<FileKey>,
    /// The backup every append is replicated to (`ReplicatedJournal`).
    replicator: Option<Arc<dyn Replicator>>,
    /// The files sent to a backup are being read: none is reclaimed or
    /// compacted away (`replicationSyncPreserveOldFiles`).
    preserve: bool,
    /// Loading a backup's copy (`loadSyncOnly`): transactions not yet
    /// complete stay open, as the primary may still complete them.
    sync_only: bool,
}

/// Where a replicating journal sends what it appends: Artemis's
/// `ReplicatedJournal`, which writes locally and replicates each record.
pub trait Replicator: Send + Sync {
    /// Called for every record appended, in order, under the journal's
    /// lock. `done` is the caller's completion; what is returned runs when
    /// the local write is done, and `done` must run only once the backup
    /// has the record too.
    fn replicate(&self, record: &Record, done: Option<Completion>) -> Option<Completion>;

    /// As [`Replicator::replicate`] for a barrier: `done` waits for the
    /// backup to have everything replicated so far.
    fn barrier(&self, done: Completion) -> Completion;
}

/// An Artemis journal in one directory.
pub struct Journal {
    pub(crate) inner: Mutex<Inner>,
    pub(crate) writer: Writer,
}

impl Journal {
    /// Opens (without loading) the journal in `config.directory`, creating
    /// the directory if needed.
    pub fn open(config: JournalConfig) -> Result<Self> {
        if config.file_size < format::MIN_FILE_SIZE {
            return Err(JournalError::IllegalState(format!(
                "File size cannot be less than {} bytes",
                format::MIN_FILE_SIZE
            )));
        }
        if config.min_files < 2 {
            return Err(JournalError::IllegalState(
                "minFiles cannot be less than 2".into(),
            ));
        }
        if config.compact_percentage > 100 {
            return Err(JournalError::IllegalState(
                "Compact Percentage out of range".into(),
            ));
        }
        std::fs::create_dir_all(&config.directory).map_err(JournalError::io)?;
        let writer = Writer::new(&config.file_extension, config.datasync);
        Ok(Self {
            inner: Mutex::new(Inner {
                cfg: config,
                state: State::Started,
                books: Books::default(),
                data_files: VecDeque::new(),
                free_files: VecDeque::new(),
                opened_files: VecDeque::new(),
                creating: HashMap::new(),
                current: None,
                position: 0,
                next_file_id: 0,
                auto_reclaim: true,
                scratch: Vec::new(),
                history_pending: Vec::new(),
                replicator: None,
                preserve: false,
                sync_only: false,
            }),
            writer,
        })
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `replaceableRecord`: updates of this user record type only need their
    /// last value, so compaction keeps only the last one.
    pub fn replaceable_record(&self, user_type: u8) {
        self.lock().books.replaceable.insert(user_type);
    }

    pub fn set_auto_reclaim(&self, on: bool) {
        self.lock().auto_reclaim = on;
    }

    /// `load(LoaderCallback)`.
    pub fn load(&self, loader: &mut dyn LoaderCallback) -> Result<LoadInfo> {
        let mut inner = self.lock();
        inner.load(&self.writer, loader)
    }

    /// `loadSyncOnly`: a replicating backup's copy is loaded to go on
    /// appending what its primary replicates; nothing is reported, and
    /// transactions not yet complete stay open.
    pub fn load_sync_only(&self) -> Result<()> {
        struct Ignore;
        impl LoaderCallback for Ignore {
            fn add_record(&mut self, _: RecordInfo) {}
            fn update_record(&mut self, _: RecordInfo) {}
            fn delete_record(&mut self, _: i64) {}
            fn add_prepared_transaction(&mut self, _: PreparedTransactionInfo) {}
            fn failed_transaction(&mut self, _: i64, _: Vec<RecordInfo>, _: Vec<RecordInfo>) {}
        }
        let mut inner = self.lock();
        inner.sync_only = true;
        let r = inner.load(&self.writer, &mut Ignore);
        inner.sync_only = false;
        r.map(|_| ())
    }

    /// `load(committedRecords, preparedTransactions, failureCallback)`.
    pub fn load_all(&self) -> Result<LoadResult> {
        #[derive(Default)]
        struct Collect {
            out: LoadResult,
            deleted: HashSet<i64>,
        }
        impl LoaderCallback for Collect {
            fn add_record(&mut self, info: RecordInfo) {
                self.out.committed.push(info);
            }
            fn update_record(&mut self, info: RecordInfo) {
                self.out.committed.push(info);
            }
            fn delete_record(&mut self, id: i64) {
                self.deleted.insert(id);
            }
            fn add_prepared_transaction(&mut self, tx: PreparedTransactionInfo) {
                self.out.prepared.push(tx);
            }
            fn failed_transaction(&mut self, tx: i64, _: Vec<RecordInfo>, _: Vec<RecordInfo>) {
                self.out.failed.push(tx);
            }
        }
        let mut c = Collect::default();
        let info = self.load(&mut c)?;
        let deleted = c.deleted;
        c.out.committed.retain(|r| !deleted.contains(&r.id));
        c.out.max_id = info.max_id;
        Ok(c.out)
    }

    fn submit(
        &self,
        record: Record,
        sync: bool,
        completion: Option<Completion>,
        account: impl FnOnce(&mut Books, FileKey, i64),
    ) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        let size = record.encode_size();
        inner.check_record_size(size)?;
        let wait = sync && completion.is_none();
        let completion = inner.replicate(&record, completion);
        let (completion, rx) = completion_or_wait(completion, wait);
        let file = inner.append(&self.writer, record, None, sync, completion)?;
        account(&mut inner.books, file, size as i64);
        drop(inner);
        wait_for(rx)
    }

    /// `appendAddRecord`.
    /// How long the journal's write in progress has taken, and an ID of
    /// that write; `None` while it writes nothing (the critical analyzer's
    /// `TimedBuffer` path).
    pub fn writing(&self) -> Option<(std::time::Duration, u64)> {
        self.writer.busy()
    }

    pub fn append_add(
        &self,
        id: i64,
        user_type: u8,
        data: Vec<u8>,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<()> {
        self.submit(
            Record::Add {
                id,
                user_type,
                data,
            },
            sync,
            completion,
            |b, f, size| b.add_record(id, f, size),
        )
    }

    /// `appendAddEvent`: written but never tracked, so never deleted.
    pub fn append_add_event(
        &self,
        id: i64,
        user_type: u8,
        data: Vec<u8>,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<()> {
        self.submit(
            Record::Event {
                id,
                user_type,
                data,
            },
            sync,
            completion,
            |_, _, _| {},
        )
    }

    /// `tryAppendUpdateRecord`: `Ok(false)` (and the completion runs at
    /// once) when there is no such record.
    pub fn append_update(
        &self,
        id: i64,
        user_type: u8,
        data: Vec<u8>,
        sync: bool,
        replaceable_update: bool,
        completion: Option<Completion>,
    ) -> Result<bool> {
        self.submit_existing(
            id,
            Record::Update {
                id,
                user_type,
                data,
            },
            sync,
            completion,
            |b, f, size| b.add_update(id, f, size, replaceable_update),
        )
    }

    /// `tryAppendDeleteRecord`: `Ok(false)` when there is no such record.
    pub fn append_delete(
        &self,
        id: i64,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<bool> {
        self.submit_existing(id, Record::Delete { id }, sync, completion, |b, f, _| {
            b.delete_record(id, f);
        })
    }

    fn submit_existing(
        &self,
        id: i64,
        record: Record,
        sync: bool,
        completion: Option<Completion>,
        account: impl FnOnce(&mut Books, FileKey, i64),
    ) -> Result<bool> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        if !inner.books.records.contains_key(&id) {
            drop(inner);
            if let Some(c) = completion {
                c(Ok(()));
            }
            return Ok(false);
        }
        let size = record.encode_size();
        inner.check_record_size(size)?;
        let wait = sync && completion.is_none();
        let completion = inner.replicate(&record, completion);
        let (completion, rx) = completion_or_wait(completion, wait);
        let file = inner.append(&self.writer, record, None, sync, completion)?;
        account(&mut inner.books, file, size as i64);
        drop(inner);
        wait_for(rx).map(|_| true)
    }

    fn submit_tx(
        &self,
        record: Record,
        account: impl FnOnce(&mut Books, FileKey, i64),
    ) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        let tx = record.tx().expect("transactional record");
        if let Some(e) = &inner.books.tx(tx).error {
            return Err(JournalError::IllegalState(e.clone()));
        }
        let size = record.encode_size();
        inner.check_record_size(size)?;
        let completion = inner.replicate(&record, None);
        let file = inner.append(&self.writer, record, None, false, completion)?;
        account(&mut inner.books, file, size as i64);
        Ok(())
    }

    /// `appendAddRecordTransactional`.
    pub fn append_add_tx(&self, tx: i64, id: i64, user_type: u8, data: Vec<u8>) -> Result<()> {
        self.submit_tx(
            Record::AddTx {
                tx,
                id,
                user_type,
                data,
            },
            |b, f, size| b.tx_add_positive(tx, f, id, size, false),
        )
    }

    /// `appendUpdateRecordTransactional`.
    pub fn append_update_tx(&self, tx: i64, id: i64, user_type: u8, data: Vec<u8>) -> Result<()> {
        self.submit_tx(
            Record::UpdateTx {
                tx,
                id,
                user_type,
                data,
            },
            |b, f, size| b.tx_add_positive(tx, f, id, size, false),
        )
    }

    /// `appendDeleteRecordTransactional`; `data` may be empty.
    pub fn append_delete_tx(&self, tx: i64, id: i64, data: Vec<u8>) -> Result<()> {
        self.submit_tx(Record::DeleteTx { tx, id, data }, |b, f, _| {
            b.tx_add_negative(tx, f, id)
        })
    }

    /// `appendPrepareRecord`: `data` is the transaction's extra data.
    pub fn append_prepare(
        &self,
        tx: i64,
        data: Vec<u8>,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        if let Some(e) = &inner.books.tx(tx).error {
            return Err(JournalError::IllegalState(e.clone()));
        }
        let wait = sync && completion.is_none();
        let record = Record::Prepare {
            tx,
            num_records: 0,
            data,
        };
        let completion = inner.replicate(&record, completion);
        let (completion, rx) = completion_or_wait(completion, wait);
        let file = inner.append(&self.writer, record, Some(tx), sync, completion)?;
        inner.books.tx_prepare(tx, file);
        drop(inner);
        wait_for(rx)
    }

    /// `appendCommitRecord`.
    pub fn append_commit(&self, tx: i64, sync: bool, completion: Option<Completion>) -> Result<()> {
        self.complete(tx, true, sync, completion)
    }

    /// `appendRollbackRecord`.
    pub fn append_rollback(
        &self,
        tx: i64,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<()> {
        self.complete(tx, false, sync, completion)
    }

    fn complete(
        &self,
        tx: i64,
        commit: bool,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        let Some(t) = inner.books.transactions.get(&tx) else {
            return Err(JournalError::IllegalState(format!(
                "Cannot find tx with id {tx}"
            )));
        };
        if let Some(e) = &t.error {
            return Err(JournalError::IllegalState(e.clone()));
        }
        let wait = sync && completion.is_none();
        let record = if commit {
            Record::Commit { tx, num_records: 0 }
        } else {
            Record::Rollback { tx }
        };
        let completion = inner.replicate(&record, completion);
        let (completion, rx) = completion_or_wait(completion, wait);
        let file = inner.append(&self.writer, record, Some(tx), sync, completion)?;
        let t = inner.books.transactions.remove(&tx).unwrap_or_default();
        if commit {
            inner.books.tx_commit(t, file);
        } else {
            inner.books.tx_rollback(t, file);
        }
        drop(inner);
        wait_for(rx)
    }

    /// Runs `completion` once everything appended so far is on disk: an
    /// `OperationContext` with nothing of its own to wait for.
    pub fn on_written(&self, completion: Completion) -> Result<()> {
        let completion = match &self.lock().replicator {
            Some(r) => r.barrier(completion),
            None => completion,
        };
        self.writer.barrier(false, completion)
    }

    /// Starts replicating to a backup (`prepareJournalForCopy` and
    /// `replicationSyncPreserveOldFiles`): the current file is closed, and
    /// the data files written so far are returned for the backup to copy.
    /// `start` runs with the journal locked, before anything appended later
    /// is replicated; it tells the backup which files to expect. Nothing is
    /// reclaimed or compacted until [`Journal::sync_finished`], and the
    /// files are on disk when this returns.
    pub fn start_replication(
        &self,
        replicator: Arc<dyn Replicator>,
        start: impl FnOnce(&[JournalFileInfo]),
    ) -> Result<Vec<JournalFileInfo>> {
        let files = {
            let mut inner = self.lock();
            inner.check_loaded()?;
            inner.move_next_file(&self.writer, false)?;
            let files: Vec<JournalFileInfo> = inner
                .data_files
                .iter()
                .map(|k| {
                    let f = &inner.books.files[k];
                    JournalFileInfo {
                        name: f.name.clone(),
                        file_id: f.id,
                    }
                })
                .collect();
            inner.preserve = true;
            start(&files);
            inner.replicator = Some(replicator);
            files
        };
        self.writer.wait_idle()?;
        Ok(files)
    }

    /// `replicationSyncFinished`: the copied files may go again.
    pub fn sync_finished(&self) {
        self.lock().preserve = false;
    }

    /// `stopReplication`: appends are no longer replicated.
    pub fn stop_replication(&self) {
        let mut inner = self.lock();
        inner.replicator = None;
        inner.preserve = false;
    }

    /// The journal's directory.
    pub fn directory(&self) -> PathBuf {
        self.lock().cfg.directory.clone()
    }

    /// Blocks until every append so far is written and synced.
    pub fn sync(&self) -> Result<()> {
        let (c, rx) = completion_or_wait(None, true);
        self.writer.barrier(true, c.expect("waiter"))?;
        wait_for(rx)
    }

    /// Compacts the journal now (`scheduleCompactAndBlock`).
    pub fn compact(&self) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        if inner.preserve {
            return Ok(());
        }
        inner
            .compact(&self.writer)
            .map_err(|e| self.writer.critical(e, None))
    }

    /// Flushes and closes the journal (`stop`).
    pub fn stop(&self) -> Result<()> {
        let r = self.writer.wait_idle();
        let mut inner = self.lock();
        // A file still being made is finished, not left as a `.tmp`.
        let r = r.and(inner.finish_all_creating());
        let datasync = inner.cfg.datasync;
        for f in inner.books.files.values_mut() {
            if let Some(h) = f.handle.take()
                && datasync
            {
                let _ = fio::sync_data(&h.file, &h.path);
            }
        }
        inner.state = State::Started;
        r
    }

    /// Records currently live (`getNumberOfRecords`).
    pub fn number_of_records(&self) -> usize {
        self.lock().books.records.len()
    }

    /// Data files in order, the current file last.
    pub fn data_files(&self) -> Vec<JournalFileInfo> {
        let inner = self.lock();
        inner
            .data_files
            .iter()
            .chain(inner.current.iter())
            .map(|k| {
                let f = &inner.books.files[k];
                JournalFileInfo {
                    name: f.name.clone(),
                    file_id: f.id,
                }
            })
            .collect()
    }

    pub fn free_files_count(&self) -> usize {
        self.lock().free_files.len()
    }

    /// Writes nothing more to the current file: the next append starts a new
    /// one (`forceMoveNextFile`).
    pub fn force_move_next_file(&self) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        inner
            .move_next_file(&self.writer, false)
            .map_err(|e| self.writer.critical(e, None))
    }

    /// `forceBackup`: the current file moves to the history now.
    pub fn force_backup(&self) -> Result<()> {
        let mut inner = self.lock();
        inner.check_loaded()?;
        inner
            .move_next_file(&self.writer, true)
            .map_err(|e| self.writer.critical(e, None))
    }

    /// Runs reclaim now (`checkReclaimStatus`).
    pub fn check_reclaim_status(&self) -> Result<()> {
        let mut inner = self.lock();
        inner
            .check_reclaim_status(&self.writer)
            .map_err(|e| self.writer.critical(e, None))
    }

    /// `IOCriticalErrorListener`: told of the first I/O error writing the
    /// journal, after which it takes no more writes.
    pub fn set_critical_listener(&self, listener: Option<CriticalListener>) {
        self.writer.set_critical_listener(listener);
    }

    /// The I/O error that stopped the journal taking writes, if one has.
    pub fn failure(&self) -> Option<JournalError> {
        self.writer.failure()
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.writer.wait_idle();
    }
}

type Waiter = Option<std::sync::mpsc::Receiver<Result<()>>>;

fn completion_or_wait(completion: Option<Completion>, wait: bool) -> (Option<Completion>, Waiter) {
    if !wait {
        return (completion, None);
    }
    let (tx, rx) = std::sync::mpsc::channel();
    (
        Some(Box::new(move |r| {
            let _ = tx.send(r);
        })),
        Some(rx),
    )
}

fn wait_for(rx: Waiter) -> Result<()> {
    match rx {
        None => Ok(()),
        Some(rx) => rx
            .recv()
            .unwrap_or_else(|_| Err(JournalError::IllegalState("journal writer stopped".into()))),
    }
}

/// `getFileNameID`.
fn file_name_id(prefix: &str, name: &str) -> i64 {
    let dot = name.find('.').unwrap_or(name.len());
    name.get(prefix.len() + 1..dot)
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            let dash = name.rfind('-')?;
            name.get(dash + 1..dot)?.parse().ok()
        })
        .unwrap_or(0)
}

fn open_rw(path: &Path) -> Result<File> {
    fio::open(path, false).map_err(JournalError::io)
}

/// Load-time state of one transaction (`TransactionHolder`).
#[derive(Default)]
struct TxHolder {
    records: Vec<RecordInfo>,
    to_delete: Vec<RecordInfo>,
    prepared: bool,
    invalid: bool,
    extra_data: Vec<u8>,
}

impl Inner {
    pub(crate) fn path(&self, name: &str) -> PathBuf {
        self.cfg.directory.join(name)
    }

    /// Hands the record to the replicator, if there is one: the completion
    /// the local write runs.
    fn replicate(&self, record: &Record, completion: Option<Completion>) -> Option<Completion> {
        match &self.replicator {
            Some(r) => r.replicate(record, completion),
            None => completion,
        }
    }

    pub(crate) fn check_loaded(&self) -> Result<()> {
        match self.state {
            State::Loaded => Ok(()),
            State::Started => Err(JournalError::NotLoaded("STARTED")),
            State::Failed => Err(JournalError::NotLoaded("FAILED")),
        }
    }

    fn check_record_size(&self, size: usize) -> Result<()> {
        let max = self.cfg.max_record_size();
        if size > max.saturating_sub(2048) {
            tracing::warn!(
                "AMQ142033: Message of {size} bytes is bigger than the max record size of {max} bytes. You should try to move large application properties to the message body."
            );
            if size > max {
                return Err(JournalError::RecordTooLarge { size, max });
            }
        }
        Ok(())
    }

    /// `listFiles(extension)`: names ending with `.<extension>`.
    pub(crate) fn list_files(&self, extension: &str) -> Result<Vec<String>> {
        let suffix = format!(".{extension}");
        let mut out = Vec::new();
        for e in std::fs::read_dir(&self.cfg.directory).map_err(JournalError::io)? {
            let e = e.map_err(JournalError::io)?;
            if !e.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            if let Some(name) = e.file_name().to_str()
                && name.ends_with(&suffix)
            {
                out.push(name.to_owned());
            }
        }
        out.sort();
        Ok(out)
    }

    /// `checkControlFile`: finishes a compaction interrupted by a crash, then
    /// deletes leftover `.cmp` and `.tmp` files.
    fn check_control_file(&self) -> Result<()> {
        let path = self.path(FILE_COMPACT_CONTROL);
        if path.exists() {
            let bytes = std::fs::read(&path).map_err(JournalError::io)?;
            if let Some(ctl) = ControlFile::decode_file(&bytes)? {
                for f in &ctl.data_files {
                    let p = self.path(f);
                    if p.exists() {
                        std::fs::remove_file(p).map_err(JournalError::io)?;
                    }
                }
                for f in &ctl.new_files {
                    let p = self.path(f);
                    if p.exists() {
                        let to = f.rfind(".cmp").map_or(f.as_str(), |i| &f[..i]);
                        std::fs::rename(&p, self.path(to)).map_err(JournalError::io)?;
                    }
                }
                for (from, to) in &ctl.renames {
                    let p = self.path(from);
                    if p.exists() {
                        let _ = std::fs::remove_file(self.path(to));
                        std::fs::rename(p, self.path(to)).map_err(JournalError::io)?;
                    }
                }
            }
            std::fs::remove_file(&path).map_err(JournalError::io)?;
        }
        for ext in [".cmp", ".tmp"] {
            let ext = format!("{}{ext}", self.cfg.file_extension);
            let left = self.list_files(&ext)?;
            if !left.is_empty() {
                tracing::warn!(
                    "AMQ142022: Temporary files were left unattended after a crash on journal directory, deleting invalid files now"
                );
            }
            for f in left {
                tracing::warn!("AMQ142023: Deleting orphaned file {f}");
                let _ = std::fs::remove_file(self.path(&f));
            }
        }
        Ok(())
    }

    /// `orderFiles`.
    fn order_files(&mut self) -> Result<Vec<FileKey>> {
        let mut files = Vec::new();
        for name in self.list_files(&self.cfg.file_extension.clone())? {
            let path = self.path(&name);
            let len = std::fs::metadata(&path).map_err(JournalError::io)?.len();
            if len >= SIZE_HEADER as u64 {
                let f = File::open(&path).map_err(JournalError::io)?;
                let mut h = [0u8; SIZE_HEADER];
                read_exact_at(&f, &mut h, 0).map_err(JournalError::io)?;
                let header = FileHeader::decode(&h, self.cfg.user_version)?;
                files.push(JFile::new(header.file_id, header.journal_version, name));
            } else {
                tracing::warn!(
                    "AMQ142016: Ignoring journal file {name}: file is shorter then minimum header size. This file is being removed."
                );
                let _ = std::fs::remove_file(path);
            }
        }
        files.sort_by_key(|f| f.id);
        Ok(files
            .into_iter()
            .map(|f| self.books.insert_file(f))
            .collect())
    }

    fn load(&mut self, writer: &Writer, loader: &mut dyn LoaderCallback) -> Result<LoadInfo> {
        if self.state != State::Started {
            return Err(JournalError::IllegalState(
                "Journal must be in STARTED state to load".into(),
            ));
        }
        self.check_control_file()?;
        self.recover_history()?;
        let replaceable = std::mem::take(&mut self.books.replaceable);
        self.books = Books {
            replaceable,
            ..Books::default()
        };
        let _ = self.finish_all_creating();
        self.data_files.clear();
        self.free_files.clear();
        self.opened_files.clear();
        self.current = None;

        let ordered = self.order_files()?;
        // calculateNextfileID: the compactor reuses IDs under new names, so
        // both count.
        for k in &ordered {
            let f = &self.books.files[k];
            let id = file_name_id(&self.cfg.file_prefix, &f.name).max(f.id);
            self.next_file_id = self.next_file_id.max(id);
        }

        let mut holders: HashMap<i64, TxHolder> = HashMap::new();
        let mut order: Vec<i64> = Vec::new();
        let mut max_id: i64 = -1;
        let mut last_data_pos = SIZE_HEADER;

        for &key in &ordered {
            let (name, record_id, version) = {
                let f = &self.books.files[&key];
                (f.name.clone(), f.record_id(), f.version)
            };
            let bytes = std::fs::read(self.path(&name)).map_err(JournalError::io)?;
            let Some(scan) = format::scan(&bytes, record_id, version, false) else {
                continue;
            };
            drop(bytes);
            let mut has_data = scan.damaged;
            for sr in scan.records {
                let cc = sr.compact_count;
                let sr = Scanned {
                    record_kind: sr.record.marker(),
                    record: sr.record,
                };
                let info = |id, user_type, data, is_update: bool, books: &Books| RecordInfo {
                    id,
                    user_record_type: user_type,
                    data,
                    is_update,
                    replaceable_update: is_update && books.replaceable.contains(&user_type),
                    compact_count: cc,
                };
                match sr.record {
                    Record::Event { .. } => {}
                    Record::Add {
                        id,
                        user_type,
                        data,
                    } => {
                        max_id = max_id.max(id);
                        has_data = true;
                        let size = (data.len() + format::SIZE_ADD_RECORD + 1) as i64;
                        loader.add_record(info(id, user_type, data, false, &self.books));
                        self.books.add_record(id, key, size);
                    }
                    Record::Update {
                        id,
                        user_type,
                        data,
                    } => {
                        max_id = max_id.max(id);
                        has_data = true;
                        let size = (data.len() + format::SIZE_ADD_RECORD + 1) as i64;
                        let i = info(id, user_type, data, true, &self.books);
                        let repl = i.replaceable_update;
                        loader.update_record(i);
                        self.books.add_update(id, key, size, repl);
                    }
                    Record::Delete { id } => {
                        has_data = true;
                        loader.delete_record(id);
                        self.books.delete_record(id, key);
                    }
                    Record::AddTx {
                        tx,
                        id,
                        user_type,
                        data,
                    }
                    | Record::UpdateTx {
                        tx,
                        id,
                        user_type,
                        data,
                    } => {
                        // onReadUpdateRecordTX delegates to onReadAddRecordTX;
                        // only the record's update flag differs.
                        let is_update = matches!(sr.record_kind, format::UPDATE_RECORD_TX);
                        max_id = max_id.max(id);
                        has_data = true;
                        let size = (data.len() + format::SIZE_ADD_RECORD_TX + 1) as i64;
                        let i = info(id, user_type, data, is_update, &self.books);
                        let repl = i.replaceable_update;
                        holder(&mut holders, &mut order, tx).records.push(i);
                        self.books.tx_add_positive(tx, key, id, size, repl);
                    }
                    Record::DeleteTx { tx, id, data } => {
                        has_data = true;
                        holder(&mut holders, &mut order, tx)
                            .to_delete
                            .push(RecordInfo {
                                id,
                                user_record_type: 0,
                                data,
                                is_update: true,
                                replaceable_update: false,
                                compact_count: cc,
                            });
                        self.books.tx_add_negative(tx, key, id);
                    }
                    Record::Prepare {
                        tx,
                        num_records,
                        data,
                    } => {
                        has_data = true;
                        let h = holder(&mut holders, &mut order, tx);
                        h.prepared = true;
                        h.extra_data = data;
                        let healthy = self.books.tx(tx).counter(key) == num_records;
                        if healthy {
                            self.books.tx_prepare(tx, key);
                        } else {
                            tracing::warn!(
                                "AMQ142019: Prepared transaction {tx} was not considered completed, it will be ignored"
                            );
                            holders.get_mut(&tx).expect("holder").invalid = true;
                        }
                    }
                    Record::Commit { tx, num_records } => {
                        if let Some(h) = holders.remove(&tx) {
                            let Some(mut t) = self.books.transactions.remove(&tx) else {
                                return Err(JournalError::IllegalState(format!(
                                    "Cannot find tx {tx}"
                                )));
                            };
                            if t.counter(key) == num_records {
                                for r in h.records {
                                    if r.is_update {
                                        loader.update_record(r);
                                    } else {
                                        loader.add_record(r);
                                    }
                                }
                                for d in h.to_delete {
                                    loader.delete_record(d.id);
                                }
                                self.books.tx_commit(t, key);
                            } else {
                                tracing::warn!(
                                    "AMQ142020: Transaction {tx} is missing elements so the transaction is being ignored"
                                );
                                self.books.tx_forget(t);
                            }
                            has_data = true;
                        }
                    }
                    Record::Rollback { tx } => {
                        if holders.remove(&tx).is_some() {
                            let Some(t) = self.books.transactions.remove(&tx) else {
                                return Err(JournalError::IllegalState(format!(
                                    "Cannot find tx {tx}"
                                )));
                            };
                            self.books.tx_rollback(t, key);
                            has_data = true;
                        }
                    }
                }
            }
            if has_data {
                last_data_pos = scan.last_data_pos;
                self.data_files.push_back(key);
            } else {
                let check_delete = self.cfg.remove_extra_files_on_load;
                self.add_free_file(key, check_delete)?;
            }
        }

        self.set_up_current_file(last_data_pos as u64)?;
        self.state = State::Loaded;

        let mut seen = HashSet::new();
        for tx in order {
            if !seen.insert(tx) {
                continue;
            }
            let Some(h) = holders.remove(&tx) else {
                continue;
            };
            if self.sync_only && !h.invalid {
                continue;
            }
            if !h.prepared || h.invalid {
                tracing::warn!(
                    "AMQ142021: Uncommitted transaction with id {tx} found and discarded"
                );
                // A rollback record, or compaction would trip over the
                // unknown transaction.
                let file = self.append(writer, Record::Rollback { tx }, Some(tx), false, None)?;
                let t = self.books.transactions.remove(&tx).unwrap_or_default();
                self.books.tx_rollback(t, file);
                loader.failed_transaction(tx, h.records, h.to_delete);
            } else {
                for r in &h.records {
                    max_id = max_id.max(r.id);
                }
                loader.add_prepared_transaction(PreparedTransactionInfo {
                    id: tx,
                    extra_data: h.extra_data,
                    records: h.records,
                    records_to_delete: h.to_delete,
                });
            }
        }

        self.check_reclaim_status(writer)?;
        Ok(LoadInfo {
            number_of_records: self.books.records.len(),
            max_id,
        })
    }

    /// `setUpCurrentFile`.
    fn set_up_current_file(&mut self, last_data_pos: u64) -> Result<()> {
        self.ensure_min_files()?;
        if let Some(k) = self.data_files.pop_back() {
            self.open_handle(k)?;
            self.current = Some(k);
            self.position = last_data_pos;
        } else {
            let k = self.free_files.pop_front().expect("ensureMinFiles");
            self.open_handle(k)?;
            self.current = Some(k);
            self.position = SIZE_HEADER as u64;
        }
        self.push_opened_file()
    }

    fn open_handle(&mut self, k: FileKey) -> Result<Arc<DataFile>> {
        if let Some(h) = &self.books.files[&k].handle {
            return Ok(h.clone());
        }
        let path = self.path(&self.books.files[&k].name);
        let h = Arc::new(DataFile {
            file: open_rw(&path)?,
            path,
        });
        self.books.file(k).handle = Some(h.clone());
        Ok(h)
    }

    /// `ensureMinFiles`.
    fn ensure_min_files(&mut self) -> Result<()> {
        let have = self.data_files.len() + self.free_files.len();
        for _ in have..self.cfg.min_files {
            let k = self.create_file(false, None)?;
            self.free_files.push_back(k);
        }
        Ok(())
    }

    /// `createFile`: a new file named after a new file ID, written as
    /// `<name>.tmp`, filled with zeros, given its header and renamed.
    pub(crate) fn create_file(&mut self, keep_open: bool, cmp: Option<()>) -> Result<FileKey> {
        let (id, name, file) = self.new_file(cmp);
        file.write()?;
        let k = self
            .books
            .insert_file(JFile::new(id, format::FORMAT_VERSION, name));
        if keep_open {
            self.open_handle(k)?;
        }
        Ok(k)
    }

    /// The next file ID, the file's name, and what makes it.
    fn new_file(&mut self, cmp: Option<()>) -> (i64, String, NewFile) {
        self.next_file_id += 1;
        let id = self.next_file_id;
        let mut name = format!(
            "{}-{}.{}",
            self.cfg.file_prefix, id, self.cfg.file_extension
        );
        if cmp.is_some() {
            name.push_str(".cmp");
        }
        let file = NewFile {
            tmp: self.path(&format!("{name}.tmp")),
            path: self.path(&name),
            size: self.cfg.file_size,
            header: FileHeader::new(self.cfg.user_version, id).encode(),
            datasync: self.cfg.datasync,
        };
        (id, name, file)
    }

    /// Waits for an opened file still being made.
    fn finish_creating(&mut self, k: FileKey) -> Result<()> {
        match self.creating.remove(&k) {
            Some(h) => h.join().unwrap_or_else(|_| {
                Err(JournalError::IllegalState(
                    "making a journal file panicked".into(),
                ))
            }),
            None => Ok(()),
        }
    }

    /// Waits for every opened file still being made.
    pub(crate) fn finish_all_creating(&mut self) -> Result<()> {
        let keys: Vec<FileKey> = self.creating.keys().copied().collect();
        let mut first = Ok(());
        for k in keys {
            if let Err(e) = self.finish_creating(k) {
                first = first.and(Err(e));
            }
        }
        first
    }

    /// `takeFile`: a free file if there is one, a new file otherwise.
    fn take_file(&mut self) -> Result<FileKey> {
        match self.free_files.pop_front() {
            Some(k) => {
                self.open_handle(k)?;
                Ok(k)
            }
            None => self.create_file(true, None),
        }
    }

    /// `pushOpenedFile`: keeps the next file ready. A new one is made on a
    /// thread of its own, as Artemis's files executor makes it, while the
    /// current file fills: made here, it held every append behind its
    /// zeros and sync.
    fn push_opened_file(&mut self) -> Result<()> {
        let k = match self.free_files.pop_front() {
            Some(k) => {
                self.open_handle(k)?;
                k
            }
            None => {
                let (id, name, file) = self.new_file(None);
                let k = self
                    .books
                    .insert_file(JFile::new(id, format::FORMAT_VERSION, name));
                self.creating
                    .insert(k, std::thread::spawn(move || file.write()));
                k
            }
        };
        self.opened_files.push_back(k);
        Ok(())
    }

    /// `openFile`.
    fn open_file(&mut self) -> Result<FileKey> {
        let k = match self.opened_files.pop_front() {
            Some(k) => {
                if self.opened_files.is_empty() {
                    self.push_opened_file()?;
                }
                k
            }
            None => {
                self.push_opened_file()?;
                self.opened_files.pop_front().expect("pushed")
            }
        };
        self.finish_creating(k)?;
        self.open_handle(k)?;
        Ok(k)
    }

    /// `moveNextFile`.
    pub(crate) fn move_next_file(&mut self, writer: &Writer, schedule_reclaim: bool) -> Result<()> {
        let cur = self.current.expect("current file");
        // closeFile: queued writes keep their own handle.
        self.books.file(cur).handle = None;
        if !self.data_files.contains(&cur) {
            self.data_files.push_back(cur);
        }
        if self.cfg.retention.is_some() {
            self.pend_history(cur)?;
        }
        let next = self.open_file()?;
        self.current = Some(next);
        self.position = SIZE_HEADER as u64;
        if schedule_reclaim {
            self.process_backup(writer)?;
            self.check_reclaim_status(writer)?;
            self.check_compact(writer)?;
        }
        Ok(())
    }

    /// `switchFileIfNecessary`.
    fn switch_file_if_necessary(&mut self, writer: &Writer, size: usize) -> Result<()> {
        if size > self.cfg.file_size - SIZE_HEADER {
            return Err(JournalError::IllegalState(format!(
                "Record is too large to store {size} bytes"
            )));
        }
        if self.position + size as u64 > self.cfg.file_size as u64 {
            self.move_next_file(writer, true)?;
        }
        Ok(())
    }

    /// `appendRecord`: writes the record to the current file and returns it.
    /// For a prepare or commit, `tx` fills in the record count first.
    pub(crate) fn append(
        &mut self,
        writer: &Writer,
        mut record: Record,
        tx: Option<i64>,
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<FileKey> {
        if let Some(e) = writer.failure() {
            self.state = State::Failed;
            return Err(e);
        }
        let size = record.encode_size();
        self.switch_file_if_necessary(writer, size)
            .map_err(|e| writer.critical(e, None))?;
        let cur = self.current.expect("current file");
        if let Some(tx) = tx
            && matches!(record, Record::Prepare { .. } | Record::Commit { .. })
        {
            let n = self.books.tx(tx).counter(cur);
            record.set_num_records(n);
        }
        let handle = match self.open_handle(cur) {
            Ok(h) => h,
            Err(e) => {
                let path = self.path(&self.books.files[&cur].name);
                return Err(writer.critical(e, Some(&path)));
            }
        };
        let record_id = self.books.files[&cur].record_id();
        let mut buf = std::mem::take(&mut self.scratch);
        buf.clear();
        record.encode(record_id, 0, &mut buf);
        let r = writer.write(&handle, self.position, &buf, sync, completion);
        self.scratch = buf;
        r?;
        self.position += size as u64;
        Ok(cur)
    }

    /// `checkReclaimStatus`.
    pub(crate) fn check_reclaim_status(&mut self, writer: &Writer) -> Result<()> {
        if self.state != State::Loaded || !self.auto_reclaim || self.preserve {
            return Ok(());
        }
        let files: Vec<FileKey> = self.data_files.iter().copied().collect();
        reclaim_scan(&mut self.books, &files);
        let reclaim: Vec<FileKey> = files
            .into_iter()
            .filter(|k| self.books.files[k].can_reclaim())
            .collect();
        if reclaim.is_empty() {
            return Ok(());
        }
        // Their last writes may still be queued.
        writer.wait_idle()?;
        for k in reclaim {
            self.data_files.retain(|f| *f != k);
            self.books.remove_negatives(k, &self.data_files);
            self.add_free_file(k, true)?;
        }
        Ok(())
    }

    /// `addFreeFile(file, false, checkDelete)`: a file whose records are no
    /// longer needed is reinitialized under a new file ID for reuse, or
    /// deleted when the pool is full.
    pub(crate) fn add_free_file(&mut self, k: FileKey, check_delete: bool) -> Result<()> {
        let name = self.books.files[&k].name.clone();
        let path = self.path(&name);
        self.books.file(k).handle = None;
        let size = std::fs::metadata(&path).map_err(JournalError::io)?.len();
        if size != self.cfg.file_size as u64 {
            self.damaged_file(k, &path)?;
        } else if !check_delete
            || self.cfg.pool_size < 0
            || ((self.free_files.len() + self.data_files.len() + 1 + self.opened_files.len())
                as i64)
                < self.cfg.pool_size as i64
        {
            // reinitializeFile
            self.next_file_id += 1;
            let id = self.next_file_id;
            {
                let f = open_rw(&path)?;
                fio::write_at(
                    &f,
                    &path,
                    &FileHeader::new(self.cfg.user_version, id).encode(),
                    0,
                )
                .map_err(JournalError::io)?;
                if self.cfg.datasync {
                    fio::sync_data(&f, &path).map_err(JournalError::io)?;
                }
            }
            self.books.files.remove(&k);
            self.books.remove_negatives(k, &self.data_files);
            let nk = self
                .books
                .insert_file(JFile::new(id, format::FORMAT_VERSION, name));
            self.free_files.push_back(nk);
        } else {
            self.books.files.remove(&k);
            fio::remove_file(&path).map_err(JournalError::io)?;
        }
        Ok(())
    }

    /// `damagedFile`: moves a file of the wrong size to `attic/`.
    fn damaged_file(&mut self, k: FileKey, path: &Path) -> Result<()> {
        self.books.files.remove(&k);
        if !path.exists() {
            return Ok(());
        }
        let attic = self.cfg.directory.join("attic");
        std::fs::create_dir_all(&attic).map_err(JournalError::io)?;
        let count = std::fs::read_dir(&attic).map(|d| d.count()).unwrap_or(0);
        let name = path.file_name().expect("file name");
        if count < self.cfg.max_attic_files {
            tracing::warn!(
                "AMQ142044: Moving data file {} to attic",
                name.to_string_lossy()
            );
            fio::rename(path, &attic.join(name)).map_err(JournalError::io)?;
        } else {
            tracing::warn!("AMQ142045: Deleting {}", name.to_string_lossy());
            fio::remove_file(path).map_err(JournalError::io)?;
        }
        Ok(())
    }

    /// `needsCompact`.
    fn needs_compact(&self) -> bool {
        let files = &self.data_files;
        let (mut live, mut updates, mut adds) = (0i64, 0i64, 0i64);
        for k in files {
            let f = &self.books.files[k];
            live += f.live_size;
            updates += f.replaceable_count as i64;
            adds += f.add_record as i64;
        }
        if files.len() > self.cfg.compact_min_files && adds > 0 && updates > 0 {
            // Integer division, as in Java.
            if (updates / adds) as f64 > UPDATE_FACTOR {
                return true;
            }
        }
        let total = files.len() as i64 * self.cfg.file_size as i64;
        let margin = (total as f64 * (self.cfg.compact_percentage as f64 / 100.0)) as i64;
        live < margin && files.len() > self.cfg.compact_min_files
    }

    /// `checkCompact`.
    fn check_compact(&mut self, writer: &Writer) -> Result<()> {
        if self.cfg.compact_min_files == 0 || self.state != State::Loaded || self.preserve {
            return Ok(());
        }
        if self.needs_compact() {
            self.compact(writer)?;
        }
        Ok(())
    }

    // Used by the compactor.

    pub(crate) fn take_data_files_for_compaction(
        &mut self,
        writer: &Writer,
    ) -> Result<Vec<FileKey>> {
        self.move_next_file(writer, false)?;
        writer.wait_idle()?;
        let mut v: Vec<FileKey> = self.data_files.drain(..).collect();
        v.sort_by_key(|k| self.books.files[k].id);
        Ok(v)
    }

    pub(crate) fn file_path(&self, k: FileKey) -> PathBuf {
        self.path(&self.books.files[&k].name)
    }

    pub(crate) fn take_file_for_compaction(&mut self) -> Result<FileKey> {
        // openFileCMP: a fresh file renamed with `.cmp`.
        let k = self.take_file()?;
        self.books.file(k).handle = None;
        let name = self.books.files[&k].name.clone();
        let cmp = format!("{name}.cmp");
        fio::rename(&self.path(&name), &self.path(&cmp)).map_err(JournalError::io)?;
        self.books.file(k).name = cmp;
        Ok(k)
    }

    pub(crate) fn set_data_files_on_top(&mut self, files: &[FileKey]) {
        for k in files.iter().rev() {
            self.data_files.push_front(*k);
        }
    }

    pub(crate) fn rename_file(&mut self, k: FileKey, to: String) -> Result<()> {
        self.books.file(k).handle = None;
        let from = self.file_path(k);
        fio::rename(&from, &self.path(&to)).map_err(JournalError::io)?;
        self.books.file(k).name = to;
        Ok(())
    }

    pub(crate) fn set_auto_reclaim_internal(&mut self, on: bool) -> bool {
        std::mem::replace(&mut self.auto_reclaim, on)
    }
}

struct Scanned {
    record_kind: u8,
    record: Record,
}

fn holder<'a>(
    holders: &'a mut HashMap<i64, TxHolder>,
    order: &mut Vec<i64>,
    tx: i64,
) -> &'a mut TxHolder {
    holders.entry(tx).or_insert_with(|| {
        order.push(tx);
        TxHolder::default()
    })
}

/// `NIOSequentialFile.fill`: writes zeros so later appends never extend
/// the file.
/// A new file as `createFile` makes it: `<name>.tmp`, filled with zeros,
/// given its header, synced and renamed.
struct NewFile {
    tmp: PathBuf,
    path: PathBuf,
    size: usize,
    header: [u8; 16],
    datasync: bool,
}

impl NewFile {
    fn write(&self) -> Result<()> {
        {
            let f = fio::open(&self.tmp, true).map_err(JournalError::io)?;
            fill(&f, &self.tmp, self.size)?;
            fio::write_at(&f, &self.tmp, &self.header, 0).map_err(JournalError::io)?;
            if self.datasync {
                fio::sync_data(&f, &self.tmp).map_err(JournalError::io)?;
            }
        }
        fio::rename(&self.tmp, &self.path).map_err(JournalError::io)
    }
}

pub(crate) fn fill(f: &File, path: &Path, size: usize) -> Result<()> {
    let page = vec![0u8; 64 * 1024];
    let mut off = 0usize;
    while off < size {
        let n = page.len().min(size - off);
        fio::write_at(f, path, &page[..n], off as u64).map_err(JournalError::io)?;
        off += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_name_ids() {
        assert_eq!(file_name_id("activemq-data", "activemq-data-12.amq"), 12);
        assert_eq!(file_name_id("activemq-data", "activemq-data-3.amq.cmp"), 3);
        assert_eq!(file_name_id("other", "x-7.amq"), 7);
        assert_eq!(file_name_id("activemq-data", "garbage.amq"), 0);
    }
}
