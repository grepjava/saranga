//! Journal retention (`JournalImpl.setHistoryFolder`, `processBackup`): a
//! file the journal moves on from is renamed `<name>.bkp` and kept from
//! reclaim until it is copied to the retention directory, then renamed back.

use std::fs::File;

use crate::format::{FileHeader, SIZE_HEADER};
use crate::io::{Writer, read_exact_at};
use crate::journal::{FileKey, Inner};
use crate::{JournalError, Result};

/// `JournalImpl.BKP`.
const BKP: &str = ".bkp";

impl Inner {
    /// `moveNextFile` with retention: the file just filled waits for its
    /// copy, out of reclaim's reach.
    pub(crate) fn pend_history(&mut self, k: FileKey) -> Result<()> {
        let name = self.books.files[&k].name.clone();
        if name.ends_with(BKP) {
            return Ok(());
        }
        let bkp = format!("{name}{BKP}");
        std::fs::rename(self.path(&name), self.path(&bkp)).map_err(JournalError::io)?;
        let f = self.books.file(k);
        f.name = bkp;
        f.reclaimable = false;
        self.history_pending.push(k);
        Ok(())
    }

    /// `processBackup`: copies the pending files, then `processBackupCleanup`.
    pub(crate) fn process_backup(&mut self, writer: &Writer) -> Result<()> {
        if self.cfg.retention.is_none() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.history_pending);
        if !pending.is_empty() {
            // Their last writes may still be queued.
            writer.wait_idle()?;
        }
        for k in pending {
            if self.books.files.contains_key(&k) {
                self.copy_to_history(k)?;
            }
        }
        self.history_cleanup();
        Ok(())
    }

    /// `copyFile`.
    fn copy_to_history(&mut self, k: FileKey) -> Result<()> {
        let Some(retention) = self.cfg.retention.clone() else {
            return Ok(());
        };
        let (name, id) = {
            let f = &self.books.files[&k];
            (f.name.clone(), f.id)
        };
        let Some(original) = name.strip_suffix(BKP).map(str::to_owned) else {
            return Ok(());
        };
        let to = retention
            .directory
            .join(self.history_file_name(id, now_millis()));
        tracing::debug!(
            "Copying journal retention from {} to {}",
            self.path(&name).display(),
            to.display()
        );
        std::fs::create_dir_all(&retention.directory).map_err(JournalError::io)?;
        std::fs::copy(self.path(&name), &to).map_err(JournalError::io)?;
        std::fs::rename(self.path(&name), self.path(&original)).map_err(JournalError::io)?;
        let f = self.books.file(k);
        f.name = original;
        f.reclaimable = true;
        Ok(())
    }

    /// `getHistoryFileName`.
    fn history_file_name(&self, file_id: i64, millis: i64) -> String {
        let offset = self
            .cfg
            .retention
            .as_ref()
            .map_or(0, |r| (r.local_offset)(millis));
        let c = Civil::of(millis.div_euclid(1000) + offset);
        format!(
            "{}-{:04}{:02}{:02}{:02}{:02}{:02}-{file_id}.{}",
            self.cfg.file_prefix,
            c.year,
            c.month,
            c.day,
            c.hour,
            c.minute,
            c.second,
            self.cfg.file_extension
        )
    }

    /// `getDatePortionMillis`.
    fn history_file_millis(&self, name: &str) -> Option<i64> {
        let offset = self.cfg.retention.as_ref()?.local_offset;
        history_file_millis(&self.cfg.file_prefix, name, offset)
    }

    /// `processBackupCleanup`: copies older than the period go, then the
    /// oldest beyond the storage limit's count of files.
    ///
    /// A storage limit is a limit however small: below one journal file it
    /// keeps no copy. Artemis divides it into a whole number of files and
    /// takes zero as no limit, so a limit smaller than a file lets the
    /// copies grow without bound.
    fn history_cleanup(&mut self) {
        let Some(retention) = self.cfg.retention.clone() else {
            return;
        };
        let max_files = (retention.max_bytes > 0)
            .then(|| (retention.max_bytes / (self.cfg.file_size as i64).max(1)) as usize);
        if max_files.is_none() && retention.period_millis <= 0 {
            return;
        }
        let suffix = format!(".{}", self.cfg.file_extension);
        let prefix = self.cfg.file_prefix.clone();
        let list = || -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(&retention.directory)
                .map(|d| {
                    d.filter_map(|e| e.ok()?.file_name().into_string().ok())
                        .filter(|n| n.ends_with(&suffix))
                        .collect()
                })
                .unwrap_or_default();
            // Oldest first: by the copy's date, then its file ID as a
            // number, so that file 10 copied in the same second as file 9
            // comes after it. Artemis sorts the names as text, putting 10
            // before 8 and 9.
            names.sort_by_cached_key(|n| history_order(&prefix, n));
            names
        };
        if retention.period_millis > 0 {
            let cut_off = now_millis() - retention.period_millis;
            for name in list() {
                match self.history_file_millis(&name) {
                    Some(t) if t < cut_off => {
                        tracing::debug!("File {name} is too old and should go");
                        let _ = std::fs::remove_file(retention.directory.join(&name));
                    }
                    _ => break,
                }
            }
        }
        if let Some(max_files) = max_files {
            let names = list();
            let excess = names.len().saturating_sub(max_files);
            for name in names.iter().take(excess) {
                tracing::debug!("Removing {name}");
                let _ = std::fs::remove_file(retention.directory.join(name));
            }
        }
    }

    /// `setHistoryFolder` at start: files left as `.bkp` by a broker that
    /// stopped before copying them are copied now, a partial copy of each
    /// removed first.
    pub(crate) fn recover_history(&mut self) -> Result<()> {
        let Some(retention) = self.cfg.retention.clone() else {
            return Ok(());
        };
        if retention.max_bytes > 0 && retention.max_bytes < self.cfg.file_size as i64 {
            tracing::warn!(
                "The journal retention storage-limit ({} bytes) is less than one journal file ({} bytes): no retention copy is kept",
                retention.max_bytes,
                self.cfg.file_size
            );
        }
        std::fs::create_dir_all(&retention.directory).map_err(JournalError::io)?;
        let pending = self.list_files(&format!("{}{BKP}", self.cfg.file_extension))?;
        for name in pending {
            let path = self.path(&name);
            let mut h = [0u8; SIZE_HEADER];
            let read = File::open(&path).and_then(|f| read_exact_at(&f, &mut h, 0));
            let Ok(()) = read else {
                continue;
            };
            let id = FileHeader::decode(&h, self.cfg.user_version)?.file_id;
            if let Ok(dir) = std::fs::read_dir(&retention.directory) {
                for e in dir.flatten() {
                    let n = e.file_name().to_string_lossy().into_owned();
                    if is_history_of(&self.cfg.file_prefix, &self.cfg.file_extension, id, &n) {
                        tracing::warn!(
                            "File {} was partially copied before, removing the file",
                            e.path().display()
                        );
                        let _ = std::fs::remove_file(e.path());
                    }
                }
            }
            let to = retention
                .directory
                .join(self.history_file_name(id, now_millis()));
            std::fs::copy(&path, &to).map_err(JournalError::io)?;
            let original = &name[..name.len() - BKP.len()];
            std::fs::rename(&path, self.path(original)).map_err(JournalError::io)?;
        }
        self.history_cleanup();
        Ok(())
    }
}

/// Where a retention copy (`<prefix>-<yyyyMMddHHmmss>-<id>.<ext>`) comes
/// in time: its date, then its file ID as a number. A name that is not a
/// copy's sorts by itself, after the copies.
fn history_order(prefix: &str, name: &str) -> (u8, String, i64, String) {
    let parsed = name
        .strip_prefix(prefix)
        .and_then(|r| r.strip_prefix('-'))
        .and_then(|r| r.split_once('.').map(|(stem, _)| stem))
        .and_then(|stem| stem.split_once('-'))
        .and_then(|(date, id)| Some((date.to_owned(), id.parse::<i64>().ok()?)));
    match parsed {
        Some((date, id)) => (0, date, id, String::new()),
        None => (1, String::new(), 0, name.to_owned()),
    }
}

/// `getDatePortionMillis`: the instant a retention copy's name
/// (`<prefix>-<yyyyMMddHHmmss>-<id>.<ext>`) gives, read in local time;
/// `None` for a name that is not a copy's.
/// Whether `name` is a history copy of journal file `id`. A copy is named
/// `<prefix>-<date>-<id>.<ext>`, so the ID is preceded by `-`; anchoring on it
/// keeps `1` from matching `11`, `21`, and so on (which would make recovering
/// file 1's backup delete the unrelated history of 11, 21, ...).
fn is_history_of(prefix: &str, extension: &str, id: i64, name: &str) -> bool {
    name.starts_with(prefix) && name.ends_with(&format!("-{id}.{extension}"))
}

pub fn history_file_millis(prefix: &str, name: &str, local_offset: fn(i64) -> i64) -> Option<i64> {
    let rest = name.get(prefix.len() + 1..)?;
    local_date_millis(&rest[..rest.find('-')?], local_offset)
}

/// A `yyyyMMddHHmmss` date in local time, as milliseconds since the epoch.
pub fn local_date_millis(date: &str, local_offset: fn(i64) -> i64) -> Option<i64> {
    if date.len() != 14 || !date.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = |a: usize, b: usize| date[a..b].parse::<i64>().ok();
    let local = Civil {
        year: n(0, 4)?,
        month: n(4, 6)?,
        day: n(6, 8)?,
        hour: n(8, 10)?,
        minute: n(10, 12)?,
        second: n(12, 14)?,
    }
    .seconds();
    Some((local - local_offset(local * 1000)) * 1000)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Calendar fields of an instant.
struct Civil {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
}

impl Civil {
    /// The fields of `secs` seconds since the epoch (Howard Hinnant's
    /// `civil_from_days`).
    fn of(secs: i64) -> Self {
        let days = secs.div_euclid(86_400);
        let tod = secs.rem_euclid(86_400);
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        Self {
            year: yoe + era * 400 + i64::from(month <= 2),
            month,
            day,
            hour: tod / 3600,
            minute: tod % 3600 / 60,
            second: tod % 60,
        }
    }

    /// Seconds since the epoch (`days_from_civil`).
    fn seconds(&self) -> i64 {
        let y = self.year - i64::from(self.month <= 2);
        let era = y.div_euclid(400);
        let yoe = y.rem_euclid(400);
        let mp = (self.month + 9) % 12;
        let doy = (153 * mp + 2) / 5 + self.day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era * 146_097 + doe - 719_468;
        days * 86_400 + self.hour * 3600 + self.minute * 60 + self.second
    }
}

#[cfg(test)]
mod tests {
    use super::{Civil, history_order, is_history_of};

    /// Copies sort oldest first: by date, then by file ID as a number, so
    /// file 10 copied in the same second as 8 and 9 comes after them.
    #[test]
    fn history_copies_sort_by_date_then_numeric_id() {
        let mut names: Vec<String> = [
            "activemq-data-20260101000001-1.amq",
            "activemq-data-20260101000000-10.amq",
            "activemq-data-20260101000000-9.amq",
            "activemq-data-20260101000000-8.amq",
        ]
        .map(str::to_owned)
        .to_vec();
        names.sort_by_cached_key(|n| history_order("activemq-data", n));
        assert_eq!(
            names,
            [
                "activemq-data-20260101000000-8.amq",
                "activemq-data-20260101000000-9.amq",
                "activemq-data-20260101000000-10.amq",
                "activemq-data-20260101000001-1.amq",
            ]
        );
    }

    /// Recovering a partially copied backup of file `id` must delete only
    /// that file's history copy, not the copy of a file whose ID merely ends
    /// in the same digits (`1` vs `11`, `21`).
    #[test]
    fn history_match_anchors_on_the_whole_file_id() {
        let name_of = |id: i64| format!("activemq-data-20260101000000-{id}.amq");
        assert!(is_history_of("activemq-data", "amq", 1, &name_of(1)));
        assert!(!is_history_of("activemq-data", "amq", 1, &name_of(11)));
        assert!(!is_history_of("activemq-data", "amq", 1, &name_of(21)));
        assert!(is_history_of("activemq-data", "amq", 11, &name_of(11)));
        assert!(!is_history_of("activemq-data", "amq", 2, &name_of(12)));
    }

    #[test]
    fn civil_round_trips() {
        for secs in [0, 951_782_400, 1_791_801_000, 4_102_444_799] {
            assert_eq!(Civil::of(secs).seconds(), secs);
        }
        let c = Civil::of(1_791_801_000);
        assert_eq!((c.year, c.month, c.day), (2026, 10, 12));
    }
}
