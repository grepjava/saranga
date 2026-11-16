//! A journal whose last file was written in format version 1 (before the
//! compact count and check size): what is appended after it loads again.

use saranga_journal::format::{FileHeader, Record};
use saranga_journal::{Journal, JournalConfig};

fn config(dir: &std::path::Path) -> JournalConfig {
    let mut c = JournalConfig::messages(dir);
    c.file_size = 64 * 1024;
    c.min_files = 2;
    c
}

fn ids(c: &JournalConfig) -> Vec<i64> {
    let j = Journal::open(c.clone()).unwrap();
    let mut ids: Vec<i64> = j
        .load_all()
        .unwrap()
        .committed
        .iter()
        .map(|r| r.id)
        .collect();
    ids.sort();
    j.stop().unwrap();
    ids
}

/// The version-1 file stays as it is read; a synchronous add appended after
/// loading it is written to a file of the current format, so the next load
/// finds it (appended to the version-1 file, it was read as version 1 and
/// lost).
#[test]
fn an_append_after_a_version_1_file_survives_a_reload() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path());
    std::fs::create_dir_all(&c.directory).unwrap();
    let mut file = FileHeader {
        journal_version: 1,
        user_version: 0,
        file_id: 1,
    }
    .encode()
    .to_vec();
    // A version-1 add record: no compact count byte.
    let mut rec = Vec::new();
    Record::Add {
        id: 1,
        user_type: 45,
        data: vec![1],
    }
    .encode(1, 0, &mut rec);
    rec.remove(5);
    let len = rec.len();
    rec[len - 4..].copy_from_slice(&(len as i32).to_be_bytes());
    file.extend(rec);
    file.resize(c.file_size, 0);
    std::fs::write(c.directory.join("activemq-data-1.amq"), file).unwrap();
    assert_eq!(ids(&c), vec![1]);

    let j = Journal::open(c.clone()).unwrap();
    j.load_all().unwrap();
    j.append_add(2, 45, vec![2], true, None).unwrap();
    j.stop().unwrap();
    drop(j);
    assert_eq!(ids(&c), vec![1, 2]);
}
