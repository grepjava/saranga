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
pub struct LockFile {
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
        let file = OpenOptions::new().read(true).write(true).open(&key)?;
        let mut open = open_files().lock().unwrap_or_else(|e| e.into_inner());
        // The handle opened before, unless the file it has was moved away
        // or replaced since.
        if let Some(shared) = open.get(&key).and_then(Weak::upgrade)
            && sys::same_file(&shared.file, &file)?
        {
            return Ok(Self { shared });
        }
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
pub struct Held {
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

    pub fn same_file(a: &File, b: &File) -> io::Result<bool> {
        use std::os::unix::fs::MetadataExt;
        let (a, b) = (a.metadata()?, b.metadata()?);
        Ok(a.dev() == b.dev() && a.ino() == b.ino())
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

    fn id(file: &File) -> io::Result<(u32, u32, u32)> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };
        // SAFETY: a zeroed BY_HANDLE_FILE_INFORMATION is valid, and the
        // handle is open for as long as `file` is.
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ))
    }

    pub fn same_file(a: &File, b: &File) -> io::Result<bool> {
        Ok(id(a)? == id(b)?)
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

    #[test]
    fn a_file_moved_away_is_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("server.lock");
        let a = LockFile::open(&p).unwrap();
        std::fs::create_dir(dir.path().join("old")).unwrap();
        std::fs::rename(&p, dir.path().join("old/server.lock")).unwrap();
        let b = LockFile::open(&p).unwrap();
        assert!(!Arc::ptr_eq(&a.shared, &b.shared));
        // Both lockable: they are different files.
        let _ha = a.try_lock().unwrap().expect("free");
        assert!(b.try_lock().unwrap().is_some());
    }
}
