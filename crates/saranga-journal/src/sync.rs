//! A replicating backup's journal while it synchronizes with its primary.
//! Ported from what `ReplicationEndpoint` does with `JournalImpl` during the
//! initial synchronization: `createFilesForBackupSync` reserves the
//! primary's data files under their own IDs, which are then copied byte for
//! byte, and `FileWrapperJournal` writes whatever the primary appends
//! meanwhile into new files numbered after them. Once the synchronization
//! is finished the directory is an ordinary journal: [`crate::Journal`]
//! loads it.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;

use crate::format::{FORMAT_VERSION, FileHeader, Record, SIZE_HEADER};
use crate::io::write_all_at;
use crate::journal::{JournalConfig, fill};
use crate::{JournalError, Result};

/// A transaction's record count in the file it last wrote to, as a
/// prepare or commit record states it (`JournalTransaction.getCounter`).
#[derive(Default)]
struct TxCounter {
    file: i64,
    count: i32,
}

/// The journal of a backup being synchronized.
pub struct SyncJournal {
    cfg: JournalConfig,
    /// The primary's files being copied, by file ID.
    copying: HashMap<i64, File>,
    /// The file appends go to, and where in it.
    current: Option<(i64, File, u64)>,
    next_file_id: i64,
    transactions: HashMap<i64, TxCounter>,
    scratch: Vec<u8>,
}

impl SyncJournal {
    /// `createFilesForBackupSync`: an empty file for each of the primary's
    /// data files, and the next file ID above them.
    pub fn start(cfg: JournalConfig, file_ids: &[i64]) -> Result<Self> {
        std::fs::create_dir_all(&cfg.directory).map_err(JournalError::io)?;
        let mut copying = HashMap::new();
        for id in file_ids {
            let path = cfg.directory.join(file_name(&cfg, *id));
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(JournalError::io)?;
            copying.insert(*id, f);
        }
        let next_file_id = file_ids.iter().copied().max().unwrap_or(0);
        Ok(Self {
            cfg,
            copying,
            current: None,
            next_file_id,
            transactions: HashMap::new(),
            scratch: Vec::new(),
        })
    }

    /// The next bytes of one of the primary's files.
    pub fn write_file(&mut self, file_id: i64, data: &[u8]) -> Result<()> {
        let f = self.copying.get_mut(&file_id).ok_or_else(|| {
            JournalError::IllegalState(format!("no journal file {file_id} is being copied"))
        })?;
        f.write_all(data).map_err(JournalError::io)
    }

    /// The end of one of the primary's files.
    pub fn close_file(&mut self, file_id: i64) -> Result<()> {
        if let Some(f) = self.copying.remove(&file_id)
            && self.cfg.datasync
        {
            f.sync_all().map_err(JournalError::io)?;
        }
        Ok(())
    }

    /// `FileWrapperJournal.writeRecord`: a record the primary appended
    /// meanwhile, written to the current file without any bookkeeping.
    pub fn append(&mut self, mut record: Record) -> Result<()> {
        let size = record.encode_size();
        if size > self.cfg.file_size - SIZE_HEADER {
            return Err(JournalError::IllegalState(format!(
                "Record is too large to store {size} bytes"
            )));
        }
        let full = match &self.current {
            None => true,
            Some((_, _, pos)) => pos + size as u64 > self.cfg.file_size as u64,
        };
        if full {
            self.next_file()?;
        }
        let (file_id, _, _) = self.current.as_ref().expect("a current file");
        let file_id = *file_id;
        match &record {
            Record::AddTx { tx, .. }
            | Record::UpdateTx { tx, .. }
            | Record::DeleteTx { tx, .. } => {
                let c = self.transactions.entry(*tx).or_default();
                if c.file != file_id {
                    *c = TxCounter {
                        file: file_id,
                        count: 0,
                    };
                }
                c.count += 1;
            }
            Record::Prepare { tx, .. } | Record::Commit { tx, .. } | Record::Rollback { tx } => {
                let n = match self.transactions.get(tx) {
                    Some(c) if c.file == file_id => c.count,
                    _ => 0,
                };
                if !matches!(record, Record::Prepare { .. }) {
                    self.transactions.remove(tx);
                }
                record.set_num_records(n);
            }
            _ => {}
        }
        let (_, f, pos) = self.current.as_mut().expect("a current file");
        self.scratch.clear();
        record.encode(crate::format::record_id_of(file_id), 0, &mut self.scratch);
        write_all_at(f, &self.scratch, *pos).map_err(JournalError::io)?;
        *pos += size as u64;
        Ok(())
    }

    fn next_file(&mut self) -> Result<()> {
        if let Some((_, f, _)) = self.current.take()
            && self.cfg.datasync
        {
            f.sync_data().map_err(JournalError::io)?;
        }
        self.next_file_id += 1;
        let id = self.next_file_id;
        let path = self.cfg.directory.join(file_name(&self.cfg, id));
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(JournalError::io)?;
        fill(&f, &path, self.cfg.file_size)?;
        let header = FileHeader {
            journal_version: FORMAT_VERSION,
            user_version: self.cfg.user_version,
            file_id: id,
        };
        write_all_at(&f, &header.encode(), 0).map_err(JournalError::io)?;
        self.current = Some((id, f, SIZE_HEADER as u64));
        Ok(())
    }

    /// The synchronization is over: every file is synced and closed.
    pub fn finish(mut self) -> Result<()> {
        for (_, f) in self.copying.drain() {
            f.sync_all().map_err(JournalError::io)?;
        }
        if let Some((_, f, _)) = self.current.take() {
            f.sync_all().map_err(JournalError::io)?;
        }
        Ok(())
    }
}

fn file_name(cfg: &JournalConfig, id: i64) -> String {
    format!("{}-{}.{}", cfg.file_prefix, id, cfg.file_extension)
}
