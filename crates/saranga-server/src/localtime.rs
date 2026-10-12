//! The local time zone, as the JVM's default one: its offset from UTC, for
//! what Artemis prints or names in local time (log lines, journal
//! retention copies).

/// The local time zone's offset from UTC at an instant, in seconds.
pub fn offset(secs: i64) -> i64 {
    // The offset changes at most a few times a year: keep the last hour's.
    static CACHE: std::sync::Mutex<(i64, i64)> = std::sync::Mutex::new((i64::MIN, 0));
    let hour = secs.div_euclid(3600);
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if cache.0 != hour {
        *cache = (hour, os_local_offset(hour * 3600));
    }
    cache.1
}

#[cfg(unix)]
fn os_local_offset(secs: i64) -> i64 {
    let t = secs as libc::time_t;
    // SAFETY: localtime_r writes only to the tm it is given.
    unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&t, &mut tm).is_null() {
            return 0;
        }
        tm.tm_gmtoff as i64
    }
}

#[cfg(windows)]
fn os_local_offset(secs: i64) -> i64 {
    use windows_sys::Win32::Foundation::{FILETIME, SYSTEMTIME};
    use windows_sys::Win32::System::Time::{
        FileTimeToSystemTime, SystemTimeToFileTime, SystemTimeToTzSpecificLocalTime,
    };
    let ticks = (secs + 11_644_473_600) * 10_000_000;
    let utc = FILETIME {
        dwLowDateTime: ticks as u32,
        dwHighDateTime: (ticks >> 32) as u32,
    };
    // SAFETY: each call reads and writes only the structs it is given.
    unsafe {
        let mut st: SYSTEMTIME = std::mem::zeroed();
        let mut local: SYSTEMTIME = std::mem::zeroed();
        let mut out: FILETIME = std::mem::zeroed();
        if FileTimeToSystemTime(&utc, &mut st) == 0
            || SystemTimeToTzSpecificLocalTime(std::ptr::null(), &st, &mut local) == 0
            || SystemTimeToFileTime(&local, &mut out) == 0
        {
            return 0;
        }
        let local_ticks = (i64::from(out.dwHighDateTime) << 32) | i64::from(out.dwLowDateTime);
        (local_ticks - ticks) / 10_000_000
    }
}

#[cfg(not(any(unix, windows)))]
fn os_local_offset(_: i64) -> i64 {
    0
}

/// [`offset`] at an instant in milliseconds, as journal retention takes it.
pub fn offset_millis(millis: i64) -> i64 {
    offset(millis.div_euclid(1000))
}
