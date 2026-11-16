//! How full the journal's disk is (`FileStoreMonitor`).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use crate::paging::PagingManager;

/// The fraction of the file store holding `path` that is used, between 0
/// and 1 (`getDiskStoreUsage`), or `None` if it cannot be read.
pub fn usage(path: &Path) -> Option<f64> {
    let (total, free) = space(path).ok()?;
    if total == 0 {
        return None;
    }
    Some(1.0 - free as f64 / total as f64)
}

/// `FileStoreMonitor.calculateUsage`.
pub fn usage_of(usable: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    1.0 - usable as f64 / total as f64
}

/// `ByteUtil.getHumanReadableByteCount`: decimal magnitudes, one digit.
pub fn human_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0B".to_owned();
    }
    const SUFFIXES: [&str; 7] = ["E", "P", "T", "G", "M", "K", ""];
    let mut i = 0;
    while i < SUFFIXES.len() - 1 && 10f64.powi(18 - 3 * i as i32) > bytes as f64 {
        i += 1;
    }
    let magnitude = 10f64.powi(18 - 3 * i as i32);
    format!("{:.1}{}B", bytes as f64 / magnitude, SUFFIXES[i])
}

/// `FileStoreMonitorType` with its reference value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Limit {
    /// `max-disk-usage`, as a fraction.
    MaxUsage(f64),
    /// `min-disk-free`, in bytes.
    MinFree(u64),
}

/// The disk monitor: its limit and how often it looks.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonitorConfig {
    pub limit: Limit,
    /// `disk-scan-period`, in milliseconds.
    pub scan_period: u64,
}

impl MonitorConfig {
    /// `deployFileStoreMonitor`: `min-disk-free` wins over `max-disk-usage`;
    /// -1 for both means no monitor.
    pub fn new(max_disk_usage: i32, min_disk_free: i64, scan_period: i64) -> Option<Self> {
        let limit = if min_disk_free != -1 {
            if max_disk_usage != -1 {
                tracing::info!(
                    "AMQ224130: The min-disk-free value {min_disk_free} will override the max-disk-usage value {max_disk_usage} since both are set."
                );
            }
            Limit::MinFree(min_disk_free.max(0) as u64)
        } else if max_disk_usage != -1 {
            Limit::MaxUsage(f64::from(max_disk_usage) / 100.0)
        } else {
            return None;
        };
        // A scheduled component without a period never runs.
        (scan_period > 0).then_some(Self {
            limit,
            scan_period: scan_period as u64,
        })
    }
}

/// Starts `FileStoreMonitor` over the stores holding `dirs` (those that
/// exist now, as `addStore`), telling the paging manager every period. It
/// stops with the paging manager.
pub fn spawn_monitor(config: MonitorConfig, dirs: Vec<PathBuf>, paging: &Arc<PagingManager>) {
    let dirs: Vec<PathBuf> = dirs.into_iter().filter(|d| d.exists()).collect();
    let paging: Weak<PagingManager> = Arc::downgrade(paging);
    let period = std::time::Duration::from_millis(config.scan_period);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(period).await;
            let Some(paging) = paging.upgrade() else {
                break;
            };
            let (usable, total, ok, errors) = tick(&config.limit, &dirs);
            // An I/O error reading a store is a critical I/O error, as
            // Artemis's `tick` makes it, not room on the disk.
            for (e, dir) in errors {
                tracing::warn!(
                    "IO Error while calculating disk usage of {}: {e}",
                    dir.display()
                );
                paging.critical_io(&e, &dir);
            }
            paging.disk_tick(usable, total, ok, config.limit);
        }
    });
}

/// What a store could not be read for, and where.
type TickErrors = Vec<(std::io::Error, PathBuf)>;

/// `FileStoreMonitor.tick`: whether every store is within the limit, with
/// the space of the last one read, and the stores that could not be read.
/// A directory gone since the start is passed over: Artemis reads the file
/// store it found then, which is still there.
fn tick(limit: &Limit, dirs: &[PathBuf]) -> (u64, u64, bool, TickErrors) {
    let (mut usable, mut total, mut ok) = (0, 0, true);
    let mut errors = Vec::new();
    for dir in dirs {
        let (t, u) = match space(dir) {
            Ok(s) => s,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::Unsupported
                ) =>
            {
                continue;
            }
            Err(e) => {
                errors.push((e, dir.clone()));
                continue;
            }
        };
        (usable, total) = (u, t);
        ok = match *limit {
            Limit::MinFree(min) => usable > min,
            Limit::MaxUsage(max) => usage_of(usable, total) < max,
        };
        if !ok {
            break;
        }
    }
    (usable, total, ok, errors)
}

/// Total and usable bytes of the file store holding `path`.
#[cfg(unix)]
fn space(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c` is a valid NUL-terminated path and `st` is written by
    // statvfs before it is read.
    let st = unsafe {
        if libc::statvfs(c.as_ptr(), st.as_mut_ptr()) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        st.assume_init()
    };
    let frsize = st.f_frsize as u64;
    Ok((st.f_blocks as u64 * frsize, st.f_bavail as u64 * frsize))
}

/// Total and usable bytes of the file store holding `path`.
#[cfg(windows)]
fn space(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let dir = if path.exists() {
        path
    } else {
        path.parent()
            .filter(|p| p.exists())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?
    };
    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain(Some(0)).collect();
    let (mut available, mut total, mut free) = (0u64, 0u64, 0u64);
    // SAFETY: `wide` is NUL-terminated and the out-pointers are valid.
    let ok = unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut available, &mut total, &mut free) };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((total, available))
}

#[cfg(not(any(unix, windows)))]
fn space(_path: &Path) -> std::io::Result<(u64, u64)> {
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store that cannot be read is an error, not a store with room: the
    /// monitor makes it a critical I/O error. A directory gone is passed
    /// over.
    #[test]
    fn a_store_that_cannot_be_read_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        // Under a file: not a directory, on every platform.
        let (_, _, ok, errors) = tick(&Limit::MinFree(0), &[file.join("d")]);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(ok, "no verdict from a store not read");

        let (_, _, _, errors) = tick(&Limit::MinFree(0), &[dir.path().join("gone").join("d")]);
        assert!(errors.is_empty(), "{errors:?}");

        let (_, total, ok, errors) = tick(&Limit::MinFree(0), &[dir.path().to_owned()]);
        assert!(errors.is_empty() && ok && total > 0);
    }
}
