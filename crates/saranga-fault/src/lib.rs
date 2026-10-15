//! Storage fault injection for tests.
//!
//! Every file operation the broker's stores make (the journals, page files,
//! large message files, the server lock and the node ID) asks [`check`] (or
//! [`write`], for writes) first. Without the `inject` feature both are
//! no-ops the compiler removes. With it, a test arms a [`Fault`] for the
//! files under a directory of its own, so tests running in parallel do not
//! see each other's faults, and the operation fails as a real disk would:
//! with `EIO`, or with `ENOSPC` for a full disk, after writing part of its
//! bytes when the fault tears the write.

use std::io;
use std::path::Path;
#[cfg(feature = "inject")]
use std::path::PathBuf;

/// A file operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// Creating a file, or opening one to write.
    Open,
    /// Writing bytes.
    Write,
    /// `fsync` or `fdatasync`.
    Sync,
    /// Renaming a file.
    Rename,
    /// Removing a file.
    Remove,
}

/// The error a fault fails with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// `EIO` (`ERROR_IO_DEVICE` on Windows).
    Io,
    /// `ENOSPC` (`ERROR_DISK_FULL` on Windows).
    DiskFull,
}

impl Error {
    pub fn to_io(self) -> io::Error {
        #[cfg(windows)]
        let code = match self {
            Error::Io => 1117,
            Error::DiskFull => 112,
        };
        #[cfg(not(windows))]
        let code = match self {
            Error::Io => 5,
            Error::DiskFull => 28,
        };
        io::Error::from_raw_os_error(code)
    }
}

/// Before `op` on `path`: the error an armed fault fails it with.
#[inline(always)]
pub fn check(op: Op, path: &Path) -> io::Result<()> {
    #[cfg(feature = "inject")]
    {
        if let Some(hit) = registry::hit(op, path) {
            return Err(hit.error.to_io());
        }
    }
    let _ = (op, path);
    Ok(())
}

/// Before writing `len` bytes to `path`: `Err((n, e))` when an armed fault
/// fails the write, after the first `n` bytes are written (0 unless the
/// fault tears writes).
#[inline(always)]
pub fn write(path: &Path, len: usize) -> Result<(), (usize, io::Error)> {
    #[cfg(feature = "inject")]
    {
        if let Some(hit) = registry::hit(Op::Write, path) {
            return Err((hit.torn.map_or(0, |t| t.min(len)), hit.error.to_io()));
        }
    }
    let _ = (path, len);
    Ok(())
}

/// A fault to arm: `op` on the files under a directory fails.
#[cfg(feature = "inject")]
#[derive(Clone, Debug)]
pub struct Fault {
    op: Op,
    under: PathBuf,
    name: Option<String>,
    after: u64,
    times: Option<u64>,
    error: Error,
    torn: Option<usize>,
}

#[cfg(feature = "inject")]
impl Fault {
    /// `op` on any file under `under` fails with [`Error::Io`], every time.
    pub fn new(op: Op, under: impl Into<PathBuf>) -> Self {
        Self {
            op,
            under: under.into(),
            name: None,
            after: 0,
            times: None,
            error: Error::Io,
            torn: None,
        }
    }

    /// Only files whose name contains `part`.
    pub fn named(mut self, part: impl Into<String>) -> Self {
        self.name = Some(part.into());
        self
    }

    /// The first `n` matching operations succeed.
    pub fn after(mut self, n: u64) -> Self {
        self.after = n;
        self
    }

    /// Fails `n` operations, then lets the rest succeed.
    pub fn times(mut self, n: u64) -> Self {
        self.times = Some(n);
        self
    }

    pub fn error(mut self, error: Error) -> Self {
        self.error = error;
        self
    }

    /// A failed write writes its first `n` bytes before failing.
    pub fn torn(mut self, n: usize) -> Self {
        self.torn = Some(n);
        self
    }

    /// Arms the fault until the returned guard is dropped.
    pub fn arm(self) -> Armed {
        Armed(registry::arm(self))
    }
}

/// An armed [`Fault`]; dropping it disarms the fault.
#[cfg(feature = "inject")]
pub struct Armed(std::sync::Arc<registry::Active>);

#[cfg(feature = "inject")]
impl Armed {
    /// How many operations the fault has failed.
    pub fn hits(&self) -> u64 {
        self.0.hits.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(feature = "inject")]
impl Drop for Armed {
    fn drop(&mut self) {
        registry::disarm(&self.0);
    }
}

#[cfg(feature = "inject")]
mod registry {
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::{Fault, Op};

    pub struct Active {
        fault: Fault,
        seen: AtomicU64,
        pub hits: AtomicU64,
    }

    static ARMED: Mutex<Vec<Arc<Active>>> = Mutex::new(Vec::new());
    static COUNT: AtomicUsize = AtomicUsize::new(0);

    fn armed() -> std::sync::MutexGuard<'static, Vec<Arc<Active>>> {
        ARMED.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn arm(fault: Fault) -> Arc<Active> {
        let a = Arc::new(Active {
            fault,
            seen: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        });
        let mut v = armed();
        v.push(a.clone());
        COUNT.store(v.len(), Ordering::Release);
        a
    }

    pub fn disarm(a: &Arc<Active>) {
        let mut v = armed();
        v.retain(|x| !Arc::ptr_eq(x, a));
        COUNT.store(v.len(), Ordering::Release);
    }

    pub fn hit(op: Op, path: &Path) -> Option<Fault> {
        if COUNT.load(Ordering::Acquire) == 0 {
            return None;
        }
        let v = armed();
        for a in v.iter() {
            let f = &a.fault;
            if f.op != op || !path.starts_with(&f.under) {
                continue;
            }
            if let Some(part) = &f.name
                && !path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().contains(part.as_str()))
            {
                continue;
            }
            if a.seen.fetch_add(1, Ordering::AcqRel) < f.after {
                continue;
            }
            let hits = a.hits.load(Ordering::Acquire);
            if f.times.is_some_and(|t| hits >= t) {
                continue;
            }
            a.hits.fetch_add(1, Ordering::AcqRel);
            return Some(f.clone());
        }
        None
    }
}

#[cfg(all(test, feature = "inject"))]
mod tests {
    use super::*;

    #[test]
    fn faults_match_their_directory_and_count() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let f = dir.path().join("data-1.amq");
        let armed = Fault::new(Op::Sync, dir.path())
            .named("data")
            .after(1)
            .times(2)
            .error(Error::DiskFull)
            .arm();
        assert!(check(Op::Sync, &f).is_ok());
        let e = check(Op::Sync, &f).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::StorageFull);
        assert!(check(Op::Sync, &other.path().join("data-1.amq")).is_ok());
        assert!(check(Op::Write, &f).is_ok());
        assert!(check(Op::Sync, &dir.path().join("x.amq")).is_ok());
        assert!(check(Op::Sync, &f).is_err());
        assert!(check(Op::Sync, &f).is_ok());
        assert_eq!(armed.hits(), 2);
        drop(armed);
        assert!(check(Op::Sync, &f).is_ok());
    }

    #[test]
    fn torn_writes() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("page");
        let _armed = Fault::new(Op::Write, dir.path()).torn(10).arm();
        assert_eq!(write(&f, 100).unwrap_err().0, 10);
        assert_eq!(write(&f, 4).unwrap_err().0, 4);
    }
}
