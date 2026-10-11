//! How full the journal's disk is (`FileStoreMonitor`).

use std::path::Path;

/// The fraction of the file store holding `path` that is used, between 0
/// and 1 (`getDiskStoreUsage`), or `None` if it cannot be read.
pub fn usage(path: &Path) -> Option<f64> {
    let (total, free) = space(path)?;
    if total == 0 {
        return None;
    }
    Some(1.0 - free as f64 / total as f64)
}

/// Total and usable bytes of the file store holding `path`.
#[cfg(unix)]
fn space(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c` is a valid NUL-terminated path and `st` is written by
    // statvfs before it is read.
    let st = unsafe {
        if libc::statvfs(c.as_ptr(), st.as_mut_ptr()) != 0 {
            return None;
        }
        st.assume_init()
    };
    let frsize = st.f_frsize as u64;
    Some((st.f_blocks as u64 * frsize, st.f_bavail as u64 * frsize))
}

/// Total and usable bytes of the file store holding `path`.
#[cfg(windows)]
fn space(path: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let dir = if path.exists() { path } else { path.parent()? };
    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain(Some(0)).collect();
    let (mut available, mut total, mut free) = (0u64, 0u64, 0u64);
    // SAFETY: `wide` is NUL-terminated and the out-pointers are valid.
    let ok = unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut available, &mut total, &mut free) };
    (ok != 0).then_some((total, available))
}

#[cfg(not(any(unix, windows)))]
fn space(_path: &Path) -> Option<(u64, u64)> {
    None
}
