//! File I/O for the journal: positional writes and the flusher thread that
//! gives group commit. Ported in spirit from `TimedBuffer` and
//! `NIOSequentialFile`.
//!
//! Appends are encoded under the journal lock into [`Batch`]es on a shared
//! queue. The flusher thread takes everything queued, writes it, issues one
//! `fdatasync` per touched file if any batch asked for a sync, and then runs
//! the completions. Appends that arrive while a write or sync is in flight
//! are batched into the next round, which is the group commit `TimedBuffer`
//! provides without its timer.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use saranga_fault::Op;

use crate::JournalError;

/// Runs once the bytes of an append are written (and synced, when the append
/// asked for it), with the I/O error otherwise. Completions run on the
/// flusher thread and must not block on the journal.
pub type Completion = Box<dyn FnOnce(Result<(), JournalError>) + Send>;

/// `IOCriticalErrorListener`: told of the journal's first I/O error, with
/// the file it happened on when there is one. After it the journal accepts
/// no more writes.
pub type CriticalListener = Arc<dyn Fn(&JournalError, Option<&Path>) + Send + Sync>;

/// An open journal file and its path.
#[derive(Debug)]
pub(crate) struct DataFile {
    pub(crate) file: File,
    pub(crate) path: PathBuf,
}

/// Opens `path` to read and write, creating (and truncating) it with
/// `create`.
pub(crate) fn open(path: &Path, create: bool) -> io::Result<File> {
    saranga_fault::check(Op::Open, path)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(create)
        .open(path)
}

/// [`write_all_at`] on the file at `path`.
pub(crate) fn write_at(file: &File, path: &Path, buf: &[u8], offset: u64) -> io::Result<()> {
    if let Err((n, e)) = saranga_fault::write(path, buf.len()) {
        write_all_at(file, &buf[..n], offset)?;
        return Err(e);
    }
    write_all_at(file, buf, offset)
}

/// `fdatasync` on the file at `path`.
pub(crate) fn sync_data(file: &File, path: &Path) -> io::Result<()> {
    saranga_fault::check(Op::Sync, path)?;
    file.sync_data()
}

/// `fsync` on the file at `path`.
pub(crate) fn sync_all(file: &File, path: &Path) -> io::Result<()> {
    saranga_fault::check(Op::Sync, path)?;
    file.sync_all()
}

pub(crate) fn rename(from: &Path, to: &Path) -> io::Result<()> {
    saranga_fault::check(Op::Rename, from)?;
    std::fs::rename(from, to)
}

pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    saranga_fault::check(Op::Remove, path)?;
    std::fs::remove_file(path)
}

pub(crate) fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let _ = &mut buf;
        let _ = &mut offset;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            let n = file.seek_write(buf, offset)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            buf = &buf[n..];
            offset += n as u64;
        }
        Ok(())
    }
}

pub(crate) fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let _ = &mut buf;
        let _ = &mut offset;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            let n = file.seek_read(buf, offset)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            buf = &mut buf[n..];
            offset += n as u64;
        }
        Ok(())
    }
}

/// Bytes for one file, contiguous from `offset`.
struct Batch {
    file: Arc<DataFile>,
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct Pending {
    batches: Vec<Batch>,
    sync: bool,
    completions: Vec<Completion>,
    /// Appends enqueued so far, and the number the flusher has finished.
    enqueued: u64,
    completed: u64,
    failed: Option<JournalError>,
    shutdown: bool,
    /// The flusher is waiting for work, and how many wait for it to be
    /// idle: a notification is a system call even with nobody waiting,
    /// so one is made only when someone is.
    flusher_waiting: bool,
    idle_waiters: usize,
}

struct Shared {
    pending: Mutex<Pending>,
    /// Signals the flusher that there is work, and waiters that work is done.
    work: Condvar,
    done: Condvar,
    datasync: bool,
    /// When the flusher started its current write, in nanoseconds since
    /// `epoch`; 0 while it waits for work. The critical analyzer reads it.
    busy_since: std::sync::atomic::AtomicU64,
    epoch: std::time::Instant,
    critical: Mutex<Option<CriticalListener>>,
}

impl Shared {
    /// Latches `e` as the journal's failure and, the first time, tells the
    /// critical listener (`onIOError`).
    fn fail(&self, e: &JournalError, file: Option<&Path>) {
        let first = {
            let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            let first = p.failed.is_none();
            p.failed.get_or_insert_with(|| e.clone());
            first
        };
        self.done.notify_all();
        if !first {
            return;
        }
        match file {
            Some(f) => tracing::warn!(
                "AMQ144011: Critical IO Exception happened: {e} on {}",
                f.display()
            ),
            None => tracing::warn!("AMQ144010: Critical IO Exception happened: {e}"),
        }
        let listener = self
            .critical
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(l) = listener {
            l(e, file);
        }
    }
}

/// The write queue and its flusher thread.
pub(crate) struct Writer {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

impl Writer {
    pub fn new(name: &str, datasync: bool) -> Self {
        let shared = Arc::new(Shared {
            pending: Mutex::default(),
            work: Condvar::new(),
            done: Condvar::new(),
            datasync,
            busy_since: std::sync::atomic::AtomicU64::new(0),
            epoch: std::time::Instant::now(),
            critical: Mutex::new(None),
        });
        let s = shared.clone();
        let thread = std::thread::Builder::new()
            .name(format!("journal-{name}"))
            .spawn(move || flusher(s))
            .expect("spawn journal flusher");
        Self {
            shared,
            thread: Some(thread),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Queues `bytes` at `offset` of `file`. Fails with the first I/O error
    /// the flusher hit: after that the journal accepts no more writes.
    pub fn write(
        &self,
        file: &Arc<DataFile>,
        offset: u64,
        bytes: &[u8],
        sync: bool,
        completion: Option<Completion>,
    ) -> Result<(), JournalError> {
        let mut p = self.lock();
        if let Some(e) = &p.failed {
            return Err(e.clone());
        }
        match p.batches.last_mut() {
            Some(b) if Arc::ptr_eq(&b.file, file) && b.offset + b.bytes.len() as u64 == offset => {
                b.bytes.extend_from_slice(bytes)
            }
            _ => p.batches.push(Batch {
                file: file.clone(),
                offset,
                bytes: bytes.to_vec(),
            }),
        }
        p.sync |= sync;
        if let Some(c) = completion {
            p.completions.push(c);
        }
        p.enqueued += 1;
        let wake = p.flusher_waiting;
        drop(p);
        if wake {
            self.shared.work.notify_one();
        }
        Ok(())
    }

    /// Runs `completion` once everything queued before it is written
    /// (and synced, with `sync`).
    pub fn barrier(&self, sync: bool, completion: Completion) -> Result<(), JournalError> {
        let mut p = self.lock();
        if let Some(e) = &p.failed {
            return Err(e.clone());
        }
        p.sync |= sync;
        p.completions.push(completion);
        p.enqueued += 1;
        let wake = p.flusher_waiting;
        drop(p);
        if wake {
            self.shared.work.notify_one();
        }
        Ok(())
    }

    /// Blocks until everything queued so far is written.
    pub fn wait_idle(&self) -> Result<(), JournalError> {
        let mut p = self.lock();
        let target = p.enqueued;
        while p.completed < target && p.failed.is_none() {
            p.idle_waiters += 1;
            p = self.shared.done.wait(p).unwrap_or_else(|e| e.into_inner());
            p.idle_waiters -= 1;
        }
        match &p.failed {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    pub fn failure(&self) -> Option<JournalError> {
        self.lock().failed.clone()
    }

    pub fn set_critical_listener(&self, listener: Option<CriticalListener>) {
        *self
            .shared
            .critical
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = listener;
    }

    /// An error of the journal's own I/O outside the flusher (a file
    /// created, opened, renamed or removed): an I/O error fails the journal
    /// as the flusher's do. Other errors pass through.
    pub fn critical(&self, e: JournalError, file: Option<&Path>) -> JournalError {
        if matches!(e, JournalError::Io(_)) {
            self.shared.fail(&e, file);
        }
        e
    }

    /// How long the write (and sync) in progress has taken, and when it
    /// started; `None` while the flusher is idle.
    pub fn busy(&self) -> Option<(std::time::Duration, u64)> {
        let since = self
            .shared
            .busy_since
            .load(std::sync::atomic::Ordering::Acquire);
        (since != 0).then(|| {
            let now = self.shared.epoch.elapsed().as_nanos() as u64 + 1;
            (
                std::time::Duration::from_nanos(now.saturating_sub(since)),
                since,
            )
        })
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.lock().shutdown = true;
        self.shared.work.notify_one();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn flusher(s: Arc<Shared>) {
    loop {
        let (batches, sync, completions, upto) = {
            let mut p = s.pending.lock().unwrap_or_else(|e| e.into_inner());
            while p.batches.is_empty() && p.completions.is_empty() && p.completed == p.enqueued {
                if p.shutdown {
                    return;
                }
                p.flusher_waiting = true;
                p = s.work.wait(p).unwrap_or_else(|e| e.into_inner());
                p.flusher_waiting = false;
            }
            (
                std::mem::take(&mut p.batches),
                std::mem::take(&mut p.sync),
                std::mem::take(&mut p.completions),
                p.enqueued,
            )
        };
        let started = s.epoch.elapsed().as_nanos() as u64 + 1;
        s.busy_since
            .store(started, std::sync::atomic::Ordering::Release);
        let result = write_batches(&batches, sync && s.datasync);
        s.busy_since.store(0, std::sync::atomic::Ordering::Release);
        drop(batches);
        if let Err((e, path)) = &result {
            s.fail(e, Some(path));
        }
        let mut p = s.pending.lock().unwrap_or_else(|e| e.into_inner());
        p.completed = upto;
        let outcome = match &p.failed {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        };
        let waiters = p.idle_waiters > 0;
        drop(p);
        if waiters {
            s.done.notify_all();
        }
        for c in completions {
            c(outcome.clone());
        }
    }
}

fn write_batches(batches: &[Batch], sync: bool) -> Result<(), (JournalError, PathBuf)> {
    fn failed(f: &DataFile) -> impl FnOnce(io::Error) -> (JournalError, PathBuf) + '_ {
        move |e| (JournalError::io(e), f.path.clone())
    }
    let mut touched: Vec<&Arc<DataFile>> = Vec::new();
    for b in batches {
        write_at(&b.file.file, &b.file.path, &b.bytes, b.offset).map_err(failed(&b.file))?;
        if !touched.iter().any(|f| Arc::ptr_eq(f, &b.file)) {
            touched.push(&b.file);
        }
    }
    if sync {
        for f in touched {
            sync_data(&f.file, &f.path).map_err(failed(f))?;
        }
    }
    Ok(())
}
