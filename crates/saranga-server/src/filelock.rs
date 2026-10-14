//! Whole-file exclusive locks, taken as `FileChannel.tryLock()` takes them so
//! that a Java broker and a Saranga broker exclude each other: an `fcntl`
//! record lock (`F_SETLK`, from 0 to the end and beyond) on Unix, and
//! `LockFileEx` over the whole file on Windows.
//!
//! A record lock belongs to the process, and closing any of its descriptors
//! for the file drops it; two locks taken in one process never exclude each
//! other. So the process keeps one handle per file, shared by everyone who
//! opens it, and refuses a second lock on it itself, as the JVM does
//! (`OverlappingFileLockException`).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

struct Shared {
    file: File,
    locked: Mutex<bool>,
}

type Open = Mutex<HashMap<PathBuf, Weak<Shared>>>;

fn open_files() -> &'static Open {
    static OPEN: OnceLock<Open> = OnceLock::new();
    OPEN.get_or_init(Default::default)
}

/// A file opened for reading and writing, whose handle the process shares.
#[derive(Clone)]
pub(crate) struct LockFile {
    shared: Arc<Shared>,
}

impl LockFile {
    /// Opens `path`, creating it empty if it does not exist.
    pub fn open(path: &Path) -> io::Result<Self> {
        if !path.exists() {
            // No lock can be held on a file that did not exist, so this
            // handle may close.
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
        }
        let key = std::fs::canonicalize(path)?;
        let mut open = open_files().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(shared) = open.get(&key).and_then(Weak::upgrade) {
            return Ok(Self { shared });
        }
        let file = OpenOptions::new().read(true).write(true).open(&key)?;
        let shared = Arc::new(Shared {
            file,
            locked: Mutex::new(false),
        });
        open.retain(|_, w| w.strong_count() > 0);
        open.insert(key, Arc::downgrade(&shared));
        Ok(Self { shared })
    }

    pub fn file(&self) -> &File {
        &self.shared.file
    }

    /// The lock, or `None` while another process, or this one, holds it.
    pub fn try_lock(&self) -> io::Result<Option<Held>> {
        let mut locked = self.shared.locked.lock().unwrap_or_else(|e| e.into_inner());
        if *locked || !sys::try_lock(&self.shared.file)? {
            return Ok(None);
        }
        *locked = true;
        Ok(Some(Held {
            shared: self.shared.clone(),
        }))
    }
}

/// A lock held; dropping it releases it.
pub(crate) struct Held {
    shared: Arc<Shared>,
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut locked = self.shared.locked.lock().unwrap_or_else(|e| e.into_inner());
        let _ = sys::unlock(&self.shared.file);
        *locked = false;
    }
}

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;

    fn set(file: &File, kind: libc::c_short) -> io::Result<bool> {
        // SAFETY: a zeroed `flock` is valid; the fields set describe the
        // whole file.
        let mut fl: libc::flock = unsafe { std::mem::zeroed() };
        fl.l_type = kind;
        fl.l_whence = libc::SEEK_SET as libc::c_short;
        fl.l_start = 0;
        fl.l_len = 0;
        // SAFETY: the descriptor is open for as long as `file` is.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &fl) } == -1 {
            let e = io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(false),
                _ => Err(e),
            };
        }
        Ok(true)
    }

    pub fn try_lock(file: &File) -> io::Result<bool> {
        set(file, libc::F_WRLCK as libc::c_short)
    }

    pub fn unlock(file: &File) -> io::Result<()> {
        set(file, libc::F_UNLCK as libc::c_short).map(|_| ())
    }
}

#[cfg(windows)]
mod sys {
    use std::fs::{File, TryLockError};
    use std::io;

    pub fn try_lock(file: &File) -> io::Result<bool> {
        match file.try_lock() {
            Ok(()) => Ok(true),
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(TryLockError::Error(e)) => Err(e),
        }
    }

    pub fn unlock(file: &File) -> io::Result<()> {
        file.unlock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_holder_in_the_process() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("serverlock.1");
        let a = LockFile::open(&p).unwrap();
        let b = LockFile::open(&p).unwrap();
        let held = a.try_lock().unwrap().expect("free");
        assert!(b.try_lock().unwrap().is_none());
        // Opening and dropping another handle keeps the lock.
        drop(LockFile::open(&p).unwrap());
        assert!(a.try_lock().unwrap().is_none());
        drop(held);
        assert!(b.try_lock().unwrap().is_some());
    }
}
