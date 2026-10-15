//! Compaction. Ported from `JournalImpl.compact` and `JournalCompactor`.
//!
//! Live records of every data file are rewritten into new files that take
//! over the old files' IDs (so they still sort before the current file) but
//! are named after fresh file IDs with a `.cmp` extension. A control file
//! lists the old and new files; then the old files are freed, the new ones
//! lose their `.cmp` extension and the control file is deleted. A crash at
//! any point leaves either the old files or the new ones (see
//! `checkControlFile` in the loader).
//!
//! Unlike Artemis, appends wait while compaction runs: the compactor writes
//! straight into the journal's books instead of replaying the commands that
//! arrived meanwhile. The files it writes are the same.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::control::{ControlFile, FILE_COMPACT_CONTROL};
use crate::format::{self, FileHeader, Record};
use crate::io::{self as fio, Writer};
use crate::journal::{Books, Inner};
use crate::{JournalError, Result};

/// `COMPACT_SPLIT_LINE`: records compacted fewer times than this are
/// separated from older ones, once.
const COMPACT_SPLIT_LINE: i16 = 2;

type FileKey = u64;

struct Compactor<'a> {
    inner: &'a mut Inner,
    records_snapshot: HashSet<i64>,
    pending_transactions: HashSet<i64>,
    pending_writes_on_tx: HashMap<i64, Vec<(Record, i16, bool)>>,
    pending_updates: HashMap<u8, BTreeMap<i64, (Record, i16, bool)>>,
    split: bool,
    next_ordering_id: i64,
    current: Option<FileKey>,
    buf: Vec<u8>,
    new_data_files: Vec<FileKey>,
}

impl Inner {
    pub(crate) fn compact(&mut self, writer: &Writer) -> Result<()> {
        let previous_reclaim = self.set_auto_reclaim_internal(false);
        let r = self.compact_inner(writer);
        self.set_auto_reclaim_internal(previous_reclaim);
        r
    }

    fn compact_inner(&mut self, writer: &Writer) -> Result<()> {
        let files = self.take_data_files_for_compaction(writer)?;
        // The files compacted away are copied to the history first.
        self.process_backup(writer)?;
        if files.is_empty() {
            return Ok(());
        }
        let first_id = self.books.files[&files[0]].id;
        let records_snapshot: HashSet<i64> = self.books.records.keys().copied().collect();
        let mut pending_transactions = HashSet::new();
        for (id, tx) in self.books.transactions.iter_mut() {
            pending_transactions.insert(*id);
            // setCompacting: the compactor rebuilds what it needs.
            tx.clear();
        }
        // The compactor's transactions; the live ones come back after.
        let live_transactions = std::mem::take(&mut self.books.transactions);
        self.books.records.clear();

        let mut c = Compactor {
            inner: self,
            records_snapshot,
            pending_transactions,
            pending_writes_on_tx: HashMap::new(),
            pending_updates: HashMap::new(),
            split: false,
            next_ordering_id: first_id,
            current: None,
            buf: Vec::new(),
            new_data_files: Vec::new(),
        };
        let replaceable: Vec<u8> = c.inner.books.replaceable.iter().copied().collect();
        for t in replaceable {
            c.pending_updates.insert(t, BTreeMap::new());
        }
        for &k in &files {
            let f = &c.inner.books.files[&k];
            let (record_id, version) = (f.record_id(), f.version);
            let bytes = std::fs::read(c.inner.file_path(k)).map_err(JournalError::io)?;
            let Some(scan) = format::scan(&bytes, record_id, version, false) else {
                continue;
            };
            for sr in scan.records {
                c.on_read(sr.record, sr.compact_count)?;
            }
        }
        c.flush_updates()?;
        c.flush()?;
        let new_files = std::mem::take(&mut c.new_data_files);

        let control = ControlFile {
            data_files: files
                .iter()
                .map(|k| self.books.files[k].name.clone())
                .collect(),
            new_files: new_files
                .iter()
                .map(|k| self.books.files[k].name.clone())
                .collect(),
            renames: Vec::new(),
        };
        control.write(&self.cfg.directory)?;

        // Restore the structures: compacted files first, then merge the
        // compactor's view of each pending transaction into the live one.
        self.set_data_files_on_top(&new_files);
        let compacted = std::mem::replace(&mut self.books.transactions, live_transactions);
        for (id, t) in compacted {
            match self.books.transactions.get_mut(&id) {
                Some(live) => live.merge(t),
                None => tracing::warn!(
                    "AMQ142026: Compacting error: pending transaction {id} could not be merged back"
                ),
            }
        }

        // renameFiles
        for k in files {
            self.add_free_file(k, true)?;
        }
        for k in new_files {
            let name = self.books.files[&k].name.clone();
            let to = name.strip_suffix(".cmp").unwrap_or(&name).to_owned();
            self.rename_file(k, to)?;
        }
        std::fs::remove_file(self.cfg.directory.join(FILE_COMPACT_CONTROL))
            .map_err(JournalError::io)?;
        Ok(())
    }
}

impl Compactor<'_> {
    fn books(&mut self) -> &mut Books {
        &mut self.inner.books
    }

    fn contains_record(&self, id: i64) -> bool {
        self.records_snapshot.contains(&id)
    }

    fn on_read(&mut self, record: Record, cc: i16) -> Result<()> {
        let replaceable = |b: &Books, t: u8| b.replaceable.contains(&t);
        match record {
            Record::Event { .. } => {}
            Record::Add { id, .. } => {
                if self.contains_record(id) {
                    let size = record.encode_size();
                    self.check_size_and_split(size, cc)?;
                    let cur = self.write(&record, cc + 1)?;
                    self.books().add_record(id, cur, size as i64);
                }
            }
            Record::AddTx { tx, id, .. } => {
                if self.pending_transactions.contains(&tx) {
                    self.produce_add_tx(record, cc, false)?;
                } else if self.contains_record(id) {
                    self.pending_writes_on_tx
                        .entry(tx)
                        .or_default()
                        .push((record, cc, false));
                }
            }
            Record::UpdateTx {
                tx, id, user_type, ..
            } => {
                let repl = replaceable(&self.inner.books, user_type);
                if self.pending_transactions.contains(&tx) {
                    self.produce_add_tx(record, cc, repl)?;
                } else if self.contains_record(id) {
                    self.pending_writes_on_tx
                        .entry(tx)
                        .or_default()
                        .push((record, cc, repl));
                }
            }
            Record::Commit { tx, .. } => {
                if self.pending_transactions.contains(&tx) {
                    tracing::warn!(
                        "AMQ142024: Inconsistency during compacting: CommitRecord ID = {tx} for an already committed transaction during compacting"
                    );
                } else {
                    self.flush_tx(tx)?;
                    if let Some(t) = self.books().transactions.remove(&tx) {
                        let mut t = t;
                        let commit = Record::Commit { tx, num_records: 0 };
                        self.check_size(commit.encode_size())?;
                        let cur = self.current.expect("open file");
                        let n = t.counter(cur);
                        let mut commit = commit;
                        commit.set_num_records(n);
                        self.write(&commit, 0)?;
                        self.books().tx_commit(t, cur);
                    }
                }
            }
            Record::Delete { id } => {
                if self.books().records.contains_key(&id) {
                    tracing::warn!(
                        "AMQ142025: Inconsistency during compacting: Delete record being read on an existent record (id={id})"
                    );
                }
            }
            Record::DeleteTx { tx, id, .. } => {
                if self.pending_transactions.contains(&tx) {
                    self.check_size(record.encode_size())?;
                    let cur = self.write(&record, 0)?;
                    self.books().tx_add_negative(tx, cur, id);
                }
            }
            Record::Prepare { tx, data, .. } => {
                if self.pending_transactions.contains(&tx) {
                    self.flush_tx(tx)?;
                    let mut prepare = Record::Prepare {
                        tx,
                        num_records: 0,
                        data,
                    };
                    self.check_size(prepare.encode_size())?;
                    let cur = self.current.expect("open file");
                    let n = self.books().tx(tx).counter(cur);
                    prepare.set_num_records(n);
                    self.write(&prepare, 0)?;
                    self.books().tx_prepare(tx, cur);
                }
            }
            Record::Rollback { tx } => {
                if !self.pending_transactions.contains(&tx) {
                    match self.books().transactions.remove(&tx) {
                        Some(t) => {
                            self.flush_tx(tx)?;
                            let rollback = Record::Rollback { tx };
                            self.check_size(rollback.encode_size())?;
                            let cur = self.write(&rollback, 0)?;
                            self.books().tx_rollback(t, cur);
                        }
                        None => {
                            self.pending_writes_on_tx.remove(&tx);
                        }
                    }
                }
            }
            Record::Update { id, user_type, .. } => {
                if self.contains_record(id) {
                    let repl = replaceable(&self.inner.books, user_type);
                    match self.pending_updates.get_mut(&user_type) {
                        Some(latest) => {
                            latest.insert(id, (record, cc, repl));
                        }
                        None => self.produce_update(record, cc, repl)?,
                    }
                }
            }
        }
        Ok(())
    }

    fn produce_add_tx(&mut self, record: Record, cc: i16, replaceable: bool) -> Result<()> {
        let (tx, id) = match &record {
            Record::AddTx { tx, id, .. } | Record::UpdateTx { tx, id, .. } => (*tx, *id),
            _ => unreachable!("transactional add or update"),
        };
        let size = record.encode_size();
        self.check_size_and_split(size, cc)?;
        let cur = self.write(&record, cc + 1)?;
        self.books()
            .tx_add_positive(tx, cur, id, size as i64, replaceable);
        Ok(())
    }

    fn produce_update(&mut self, record: Record, cc: i16, replaceable: bool) -> Result<()> {
        let Record::Update { id, .. } = record else {
            unreachable!("update record")
        };
        let size = record.encode_size();
        self.check_size_and_split(size, cc)?;
        let cur = self.current.expect("open file");
        if self.books().records.contains_key(&id) {
            self.books().add_update(id, cur, size as i64, replaceable);
        } else {
            tracing::warn!("AMQ142028: Compacting with no add record for id {id}");
        }
        self.write(&record, cc + 1)?;
        Ok(())
    }

    fn flush_tx(&mut self, tx: i64) -> Result<()> {
        if let Some(writes) = self.pending_writes_on_tx.remove(&tx) {
            for (r, cc, repl) in writes {
                self.produce_add_tx(r, cc, repl)?;
            }
        }
        Ok(())
    }

    /// `flushUpdates`: the last update of each replaceable record.
    fn flush_updates(&mut self) -> Result<()> {
        let mut types: Vec<u8> = self.pending_updates.keys().copied().collect();
        types.sort_unstable();
        for t in types {
            let latest = std::mem::take(self.pending_updates.get_mut(&t).expect("type"));
            for (_, (r, cc, repl)) in latest {
                self.produce_update(r, cc, repl)?;
            }
        }
        Ok(())
    }

    fn check_size(&mut self, size: usize) -> Result<()> {
        self.check_size_and_split(size, -1)
    }

    /// `checkSizeAndCompactSplit`.
    fn check_size_and_split(&mut self, size: usize, cc: i16) -> Result<()> {
        if self.current.is_none() {
            if cc < COMPACT_SPLIT_LINE {
                self.split = true;
            }
            self.open_file()
        } else {
            if (0..COMPACT_SPLIT_LINE).contains(&cc) && !self.split {
                self.split = true;
                self.open_file()?;
            }
            if self.buf.len() + size > self.inner.cfg.file_size {
                self.open_file()?;
            }
            Ok(())
        }
    }

    /// `openFile`: starts the next compacted file.
    fn open_file(&mut self) -> Result<()> {
        self.flush()?;
        let k = self.inner.take_file_for_compaction()?;
        let id = self.next_ordering_id;
        self.next_ordering_id += 1;
        self.books().file(k).id = id;
        self.current = Some(k);
        self.buf.clear();
        self.buf
            .extend_from_slice(&FileHeader::new(self.inner.cfg.user_version, id).encode());
        Ok(())
    }

    fn write(&mut self, record: &Record, compact_count: i16) -> Result<FileKey> {
        let cur = self.current.expect("open file");
        let record_id = self.inner.books.files[&cur].record_id();
        record.encode(record_id, compact_count, &mut self.buf);
        Ok(cur)
    }

    /// Writes the whole file-sized buffer of the current compacted file.
    fn flush(&mut self) -> Result<()> {
        let Some(cur) = self.current.take() else {
            return Ok(());
        };
        self.buf.resize(self.inner.cfg.file_size, 0);
        let path = self.inner.file_path(cur);
        let f = fio::open(&path, false).map_err(JournalError::io)?;
        fio::write_at(&f, &path, &self.buf, 0).map_err(JournalError::io)?;
        fio::sync_all(&f, &path).map_err(JournalError::io)?;
        self.new_data_files.push(cur);
        Ok(())
    }
}
