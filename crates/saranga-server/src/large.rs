//! Large messages (`LargeServerMessageImpl`, `LargeBody`): Core messages
//! whose body is streamed to `<large-messages-directory>/<id>.msg` (`.tmp`
//! for a non-durable message) instead of being held in memory. Without
//! persistence the body is kept in memory, as
//! `NullStorageLargeServerMessage` does.
//!
//! A body's file goes when the last holder of the body lets go of it:
//! every queue reference, delivery and transaction holds the message, so
//! that is when Artemis's reference count releases the message. Bodies are
//! never deleted once the store is stopping, and files left behind by a
//! crash are removed on the next start (`cleanupIncompleteFiles`,
//! `rebuildCounters`).

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

use saranga_core_protocol::packets::replication::{
    ReplicationLargeMessageBeginMessage, ReplicationLargeMessageEndMessage,
    ReplicationLargeMessageWriteMessage,
};

use crate::replication::Replicator;

/// `LargeServerMessageImpl.CHUNK_LM_SIZE`: the chunk size used when a
/// regular message is turned into a large one.
pub const CHUNK_SIZE: usize = 100 * 1024;

/// `LargeMessageExtension`.
const DURABLE: &str = "msg";
const TEMPORARY: &str = "tmp";

/// The large-messages directory (`largeMessagesFactory`).
#[derive(Debug)]
pub struct LargeMessageStore {
    /// `None` without persistence: bodies stay in memory.
    dir: Option<PathBuf>,
    /// `large-message-sync`.
    sync: bool,
    /// Cleared when the broker stops, so that bodies dropped during
    /// shutdown keep their files.
    live: AtomicBool,
    /// With journal retention, bodies are also recorded in the journal.
    history: std::sync::OnceLock<History>,
    /// The backup durable bodies are replicated to. Writes hold it shared;
    /// the start of a synchronization holds it alone, so that each body is
    /// either copied or replicated.
    replication: RwLock<Replication>,
    /// Bodies deleted while the backup copies them, deleted once it has
    /// (`largeMessagesToDelete`).
    deferred: Mutex<Vec<(i64, PathBuf)>>,
}

#[derive(Default)]
struct Replication {
    replicator: Option<Arc<Replicator>>,
    /// The bodies being copied to the backup.
    syncing: Option<HashSet<i64>>,
}

impl std::fmt::Debug for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Replication")
    }
}

impl Replication {
    fn replicator(&self) -> Option<&Arc<Replicator>> {
        self.replicator.as_ref().filter(|r| !r.is_stopped())
    }
}

/// `ReplicationLargeMessageEndMessage` for a body complete (`-1`, negated
/// by Java's constructor) or deleted.
fn end(message_id: i64, delete: bool) -> ReplicationLargeMessageEndMessage {
    ReplicationLargeMessageEndMessage {
        message_id,
        pending_record_id: Some(if delete { -1 } else { 1 }),
        is_delete: Some(delete),
    }
}

/// The storage a body's bytes are recorded in for journal retention.
struct History(Arc<crate::storage::Storage>);

impl std::fmt::Debug for History {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("History")
    }
}

impl LargeMessageStore {
    pub fn new(dir: PathBuf, sync: bool) -> Arc<Self> {
        Arc::new(Self {
            dir: Some(dir),
            sync,
            live: AtomicBool::new(true),
            history: std::sync::OnceLock::new(),
            replication: RwLock::default(),
            deferred: Mutex::default(),
        })
    }

    /// Records each body written in `storage`'s journal, when it keeps
    /// history (`addBytesToLargeMessage` with retention).
    pub fn set_history(&self, storage: Arc<crate::storage::Storage>) {
        if storage.is_history() {
            let _ = self.history.set(History(storage));
        }
    }

    /// Bodies held in memory (no persistence).
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            dir: None,
            sync: false,
            live: AtomicBool::new(true),
            history: std::sync::OnceLock::new(),
            replication: RwLock::default(),
            deferred: Mutex::default(),
        })
    }

    fn path(&self, id: i64, durable: bool) -> Option<PathBuf> {
        let ext = if durable { DURABLE } else { TEMPORARY };
        self.dir.as_ref().map(|d| d.join(format!("{id}.{ext}")))
    }

    /// Creates the directory, deletes the `.tmp` files of non-durable
    /// messages (`cleanupIncompleteFiles`) and returns the IDs of the `.msg`
    /// files found (`recoverLargeMessagesOnFolder`).
    pub fn start(&self) -> io::Result<HashSet<i64>> {
        let mut ids = HashSet::new();
        let Some(dir) = &self.dir else {
            return Ok(ids);
        };
        fs::create_dir_all(dir)?;
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let (Some(stem), Some(ext)) = (
                path.file_stem().and_then(|s| s.to_str()),
                path.extension().and_then(|s| s.to_str()),
            ) else {
                continue;
            };
            match ext {
                TEMPORARY => {
                    let _ = fs::remove_file(&path);
                }
                DURABLE => {
                    if let Ok(id) = stem.parse() {
                        ids.insert(id);
                    }
                }
                _ => {}
            }
        }
        Ok(ids)
    }

    /// Deletes the files of large messages nothing refers to any more.
    pub fn delete_files(&self, ids: impl IntoIterator<Item = i64>) {
        for id in ids {
            if let Some(path) = self.path(id, true) {
                tracing::debug!("Removing pending large message for file={}", path.display());
                self.remove(id, &path, |e| {
                    tracing::warn!(
                        "It was not possible to remove previously stored large message on folder::It will be retried on next startup: {e}"
                    )
                });
            }
        }
    }

    fn replication(&self) -> RwLockReadGuard<'_, Replication> {
        self.replication.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Deletes a body's file, and the backup's copy of a durable one; one
    /// the backup is still copying is deleted once it has.
    fn remove(&self, id: i64, path: &Path, on_error: impl FnOnce(io::Error)) {
        let durable = path.extension().is_some_and(|e| e == DURABLE);
        let replication = self.replication();
        if durable
            && replication.replicator().is_some()
            && replication
                .syncing
                .as_ref()
                .is_some_and(|s| s.contains(&id))
        {
            self.deferred
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((id, path.to_path_buf()));
            return;
        }
        if let Err(e) = fs::remove_file(path)
            && e.kind() != io::ErrorKind::NotFound
        {
            on_error(e);
        }
        if durable && let Some(r) = replication.replicator() {
            r.send(end(id, true), None);
        }
    }

    /// Starts replicating to a backup: `start` is given the durable bodies
    /// on disk, which the backup is to copy, and from then on every durable
    /// body written is replicated. Returns each body's file and the size
    /// copied.
    pub(crate) fn start_replication(
        &self,
        replicator: Arc<Replicator>,
        start: impl FnOnce(&[i64]),
    ) -> io::Result<Vec<(i64, PathBuf, u64)>> {
        let mut replication = self.replication.write().unwrap_or_else(|e| e.into_inner());
        let mut files = Vec::new();
        if let Some(dir) = &self.dir
            && dir.exists()
        {
            for entry in fs::read_dir(dir)? {
                let path = entry?.path();
                if path.extension().is_none_or(|e| e != DURABLE) {
                    continue;
                }
                let Some(id) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse().ok())
                else {
                    continue;
                };
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                files.push((id, path, size));
            }
        }
        files.sort();
        let ids: Vec<i64> = files.iter().map(|f| f.0).collect();
        start(&ids);
        replication.syncing = Some(ids.into_iter().collect());
        replication.replicator = Some(replicator);
        Ok(files)
    }

    /// The backup has its copies: what was deleted meanwhile goes now
    /// (`performCachedLargeMessageDeletes`).
    pub(crate) fn sync_finished(&self) {
        self.replication
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .syncing = None;
        self.delete_deferred();
    }

    /// Bodies are no longer replicated.
    pub(crate) fn stop_replication(&self) {
        *self.replication.write().unwrap_or_else(|e| e.into_inner()) = Replication::default();
        self.delete_deferred();
    }

    fn delete_deferred(&self) {
        let deferred =
            std::mem::take(&mut *self.deferred.lock().unwrap_or_else(|e| e.into_inner()));
        for (id, path) in deferred {
            self.remove(id, &path, |e| {
                tracing::warn!(
                    "AMQ222010: Critical IO Error, failed to delete large message file {}: {e}",
                    path.display()
                )
            });
        }
    }

    /// Stops deleting files: whatever is dropped from now on is kept.
    pub fn stop(&self) {
        self.live.store(false, Ordering::SeqCst);
    }

    fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }

    /// Starts a new body (`createCoreLargeMessage` and `LargeBody.createFile`).
    pub fn create(self: &Arc<Self>, id: i64, durable: bool) -> io::Result<LargeWriter> {
        let replication = self.replication();
        let file = match self.path(id, durable) {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)?;
                Some((file, path))
            }
            None => None,
        };
        let replicated = durable && file.is_some();
        if replicated && let Some(r) = replication.replicator() {
            r.send(ReplicationLargeMessageBeginMessage { message_id: id }, None);
        }
        Ok(LargeWriter {
            store: self.clone(),
            id,
            file,
            memory: Vec::new(),
            size: 0,
            finished: false,
            replicated,
        })
    }

    /// The body of a large message recovered from the journal. Its size is
    /// the file's; a missing file reads as empty.
    pub fn existing(self: &Arc<Self>, id: i64) -> Arc<LargeBody> {
        let path = self.path(id, true);
        let size = path
            .as_ref()
            .and_then(|p| fs::metadata(p).ok())
            .map_or(0, |m| m.len());
        Arc::new(LargeBody {
            store: self.clone(),
            id,
            size,
            backing: match path {
                Some(p) => Backing::File(p),
                None => Backing::Memory(Arc::from(Vec::new())),
            },
            keep: AtomicBool::new(false),
        })
    }
}

/// A body being received (`LargeBody.addBytes`).
pub struct LargeWriter {
    store: Arc<LargeMessageStore>,
    id: i64,
    file: Option<(File, PathBuf)>,
    memory: Vec<u8>,
    size: u64,
    finished: bool,
    /// A durable body on disk: what is written is replicated.
    replicated: bool,
}

impl LargeWriter {
    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Some(History(storage)) = self.store.history.get() {
            storage.large_body_history(self.id, bytes);
        }
        let replication = self.store.replication();
        match &mut self.file {
            Some((f, _)) => f.write_all(bytes)?,
            None => self.memory.extend_from_slice(bytes),
        }
        if self.replicated
            && let Some(r) = replication.replicator()
        {
            r.send(
                ReplicationLargeMessageWriteMessage {
                    message_id: self.id,
                    body: bytes.to_vec(),
                },
                None,
            );
        }
        self.size += bytes.len() as u64;
        Ok(())
    }

    /// `releaseResources`: syncs the file when `large-message-sync` is on
    /// and closes it.
    pub fn finish(mut self) -> io::Result<Arc<LargeBody>> {
        self.finished = true;
        let backing = match self.file.take() {
            Some((f, path)) => {
                if self.store.sync {
                    f.sync_all()?;
                }
                if self.replicated
                    && let Some(r) = self.store.replication().replicator()
                {
                    r.send(end(self.id, false), None);
                }
                Backing::File(path)
            }
            None => Backing::Memory(Arc::from(std::mem::take(&mut self.memory))),
        };
        Ok(Arc::new(LargeBody {
            store: self.store.clone(),
            id: self.id,
            size: self.size,
            backing,
            keep: AtomicBool::new(false),
        }))
    }
}

impl Drop for LargeWriter {
    /// An interrupted upload leaves nothing behind (`clearLargeMessage`).
    fn drop(&mut self) {
        if !self.finished
            && let Some((f, path)) = self.file.take()
            && self.store.is_live()
        {
            drop(f);
            self.store.remove(self.id, &path, |_| {});
        }
    }
}

#[derive(Debug)]
enum Backing {
    File(PathBuf),
    Memory(Arc<[u8]>),
}

/// A complete large-message body.
#[derive(Debug)]
pub struct LargeBody {
    store: Arc<LargeMessageStore>,
    id: i64,
    size: u64,
    backing: Backing,
    /// Set while the body is only in a page file: dropping it from memory
    /// must not delete it (`LargeBody.setPaged`).
    keep: AtomicBool,
}

impl LargeBody {
    /// The ID the file is named after.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// `getBodySize`.
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.backing {
            Backing::File(p) => Some(p),
            Backing::Memory(_) => None,
        }
    }

    /// Keeps the file when the body is dropped from memory.
    pub fn set_keep(&self, keep: bool) {
        self.keep.store(keep, Ordering::SeqCst);
    }

    pub fn reader(&self) -> io::Result<LargeReader> {
        Ok(match &self.backing {
            Backing::File(p) => LargeReader::File(File::open(p)?),
            Backing::Memory(m) => LargeReader::Memory(m.clone(), 0),
        })
    }

    /// The whole body (`getReadOnlyBodyBuffer`), for when a large message
    /// has to become a regular one.
    pub fn read_all(&self) -> io::Result<Vec<u8>> {
        match &self.backing {
            Backing::File(p) => fs::read(p),
            Backing::Memory(m) => Ok(m.to_vec()),
        }
    }

    /// `LargeBody.copyInto`: the body of a copy with its own ID.
    pub fn copy(&self, new_id: i64, durable: bool) -> io::Result<Arc<LargeBody>> {
        let backing = match (&self.backing, self.store.path(new_id, durable)) {
            (Backing::File(from), Some(to)) => {
                let replication = self.store.replication();
                fs::copy(from, &to)?;
                if self.store.sync {
                    // Syncing needs write access on Windows.
                    OpenOptions::new().write(true).open(&to)?.sync_all()?;
                }
                if durable && let Some(r) = replication.replicator() {
                    r.send(
                        ReplicationLargeMessageBeginMessage { message_id: new_id },
                        None,
                    );
                    let mut f = File::open(&to)?;
                    let mut chunk = vec![0; CHUNK_SIZE];
                    loop {
                        let n = f.read(&mut chunk)?;
                        if n == 0 {
                            break;
                        }
                        r.send(
                            ReplicationLargeMessageWriteMessage {
                                message_id: new_id,
                                body: chunk[..n].to_vec(),
                            },
                            None,
                        );
                    }
                    r.send(end(new_id, false), None);
                }
                Backing::File(to)
            }
            (Backing::Memory(m), _) => Backing::Memory(m.clone()),
            (Backing::File(from), None) => Backing::Memory(Arc::from(fs::read(from)?)),
        };
        Ok(Arc::new(LargeBody {
            store: self.store.clone(),
            id: new_id,
            size: self.size,
            backing,
            keep: AtomicBool::new(false),
        }))
    }
}

impl Drop for LargeBody {
    fn drop(&mut self) {
        if let Backing::File(p) = &self.backing
            && self.store.is_live()
            && !self.keep.load(Ordering::SeqCst)
        {
            self.store.remove(self.id, p, |e| {
                tracing::warn!(
                    "AMQ222010: Critical IO Error, failed to delete large message file {}: {e}",
                    p.display()
                )
            });
        }
    }
}

/// Reads a body in chunks (`LargeBodyReader`).
pub enum LargeReader {
    File(File),
    Memory(Arc<[u8]>, usize),
}

impl LargeReader {
    pub fn seek(&mut self, position: u64) -> io::Result<()> {
        match self {
            Self::File(f) => f.seek(SeekFrom::Start(position)).map(|_| ()),
            Self::Memory(_, at) => {
                *at = position as usize;
                Ok(())
            }
        }
    }

    /// Reads exactly `len` bytes.
    pub fn read_chunk(&mut self, len: usize) -> io::Result<Vec<u8>> {
        match self {
            Self::File(f) => {
                let mut buf = vec![0; len];
                f.read_exact(&mut buf)?;
                Ok(buf)
            }
            Self::Memory(m, at) => {
                let end = (*at + len).min(m.len());
                if end - *at < len {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let chunk = m[*at..end].to_vec();
                *at = end;
                Ok(chunk)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("saranga-large-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn write_read_copy_delete() {
        let d = dir("lifecycle");
        let store = LargeMessageStore::new(d.clone(), true);
        assert!(store.start().unwrap().is_empty());
        let mut w = store.create(5, true).unwrap();
        w.append(b"hello ").unwrap();
        w.append(b"world").unwrap();
        let body = w.finish().unwrap();
        assert_eq!(body.size(), 11);
        assert!(d.join("5.msg").exists());
        let mut r = body.reader().unwrap();
        r.seek(6).unwrap();
        assert_eq!(r.read_chunk(5).unwrap(), b"world");
        let copy = body.copy(9, false).unwrap();
        assert_eq!(copy.read_all().unwrap(), b"hello world");
        assert!(d.join("9.tmp").exists());
        drop(body);
        assert!(!d.join("5.msg").exists());
        // Files are kept once the store stops.
        store.stop();
        drop(copy);
        assert!(d.join("9.tmp").exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn start_cleans_and_lists() {
        let d = dir("start");
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("3.msg"), b"x").unwrap();
        fs::write(d.join("4.tmp"), b"x").unwrap();
        let store = LargeMessageStore::new(d.clone(), false);
        assert_eq!(store.start().unwrap(), HashSet::from([3]));
        assert!(!d.join("4.tmp").exists());
        store.delete_files([3]);
        assert!(!d.join("3.msg").exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn interrupted_upload_is_removed() {
        let d = dir("interrupted");
        let store = LargeMessageStore::new(d.clone(), false);
        let mut w = store.create(1, true).unwrap();
        w.append(b"partial").unwrap();
        drop(w);
        assert!(!d.join("1.msg").exists());
        let mem = LargeMessageStore::in_memory();
        let mut w = mem.create(1, true).unwrap();
        w.append(b"abc").unwrap();
        assert_eq!(w.finish().unwrap().read_all().unwrap(), b"abc");
        let _ = fs::remove_dir_all(&d);
    }
}
