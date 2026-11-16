//! The local time zone, as the JVM's default one: its offset from UTC, for
//! what Artemis prints or names in local time (log lines, journal
//! retention copies).

/// The local time zone's offset from UTC at an instant, in seconds.
pub fn offset(secs: i64) -> i64 {
    // The offset changes at most a few times a year, always on a quarter
    // hour of UTC (zones such as Adelaide, St John's or Chatham change at
    // :30 or :45 past a UTC hour): keep the last quarter hour's.
    const QUARTER: i64 = 900;
    static CACHE: std::sync::Mutex<(i64, i64)> = std::sync::Mutex::new((i64::MIN, 0));
    let quarter = secs.div_euclid(QUARTER);
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if cache.0 != quarter {
        *cache = (
            quarter,
            os_local_offset(quarter.checked_mul(QUARTER).unwrap_or(secs)),
        );
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
    // An instant the FILETIME cannot hold has no local offset to give.
    let Some(ticks) = secs
        .checked_add(11_644_473_600)
        .and_then(|s| s.checked_mul(10_000_000))
    else {
        return 0;
    };
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

/// `new java.util.Date(millis).toString()` in the local time zone.
pub fn java_date(millis: i64) -> String {
    saranga_storage::java::date_at_offset(millis, offset_millis(millis))
}

#[cfg(test)]
mod tests {
    /// An instant outside what the OS can convert has no offset, rather
    /// than overflowing.
    #[test]
    fn extreme_instants_do_not_overflow() {
        assert_eq!(super::offset(i64::MAX), super::offset(i64::MAX));
        let _ = super::offset(i64::MIN);
        let _ = super::offset_millis(i64::MAX);
    }
}
