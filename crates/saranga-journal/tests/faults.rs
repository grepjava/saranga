//! The journal on a failing disk: the first I/O error fails the journal and
//! is reported once to the critical listener (`IOCriticalErrorListener`),
//! no write is accepted after it, and a reload keeps every record whose
//! append was answered.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use saranga_fault::{Error, Fault, Op};
use saranga_journal::{Journal, JournalConfig, JournalError, RecordInfo};

fn config(dir: &Path) -> JournalConfig {
    JournalConfig {
        file_size: 64 * 1024,
        min_files: 2,
        pool_size: 10,
        compact_min_files: 0,
        datasync: true,
        ..JournalConfig::messages(dir)
    }
}

type Reports = Arc<Mutex<Vec<(String, Option<PathBuf>)>>>;

fn open(cfg: &JournalConfig) -> (Journal, Reports) {
    let j = Journal::open(cfg.clone()).unwrap();
    j.load_all().unwrap();
    let reports: Reports = Arc::default();
    let r = reports.clone();
    j.set_critical_listener(Some(Arc::new(move |e, file| {
        r.lock()
            .unwrap()
            .push((e.to_string(), file.map(Path::to_path_buf)));
    })));
    (j, reports)
}

fn reload(cfg: &JournalConfig) -> Vec<i64> {
    let j = Journal::open(cfg.clone()).unwrap();
    let r = j.load_all().unwrap();
    j.stop().unwrap();
    ids(&r.committed)
}

fn ids(records: &[RecordInfo]) -> Vec<i64> {
    records
        .iter()
        .filter(|r| !r.is_update)
        .map(|r| r.id)
        .collect()
}

fn add(j: &Journal, id: i64) -> Result<(), JournalError> {
    j.append_add(id, 45, vec![id as u8; 100], true, None)
}

fn assert_failed(j: &Journal, reports: &Reports) {
    assert!(matches!(j.failure(), Some(JournalError::Io(_))));
    assert!(add(j, 1_000).is_err(), "a failed journal takes no writes");
    assert_eq!(reports.lock().unwrap().len(), 1, "reported once");
}

#[test]
fn a_failed_sync_fails_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let (j, reports) = open(&cfg);
    for id in 1..=5 {
        add(&j, id).unwrap();
    }
    let fault = Fault::new(Op::Sync, dir.path()).arm();
    assert!(add(&j, 6).is_err());
    assert_eq!(fault.hits(), 1);
    drop(fault);
    assert_failed(&j, &reports);
    let (message, file) = reports.lock().unwrap()[0].clone();
    assert!(message.starts_with("I/O error"), "{message}");
    assert!(
        file.unwrap()
            .extension()
            .is_some_and(|e| e == cfg.file_extension.as_str())
    );
    drop(j);

    let loaded = reload(&cfg);
    // The record whose sync failed was written: a reload may find it.
    assert_eq!(&loaded[..5], &[1, 2, 3, 4, 5]);
    assert!(loaded.len() <= 6);
}

#[test]
fn a_full_disk_fails_the_completion() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let (j, reports) = open(&cfg);
    add(&j, 1).unwrap();
    let fault = Fault::new(Op::Write, dir.path())
        .error(Error::DiskFull)
        .arm();
    let (tx, rx) = std::sync::mpsc::channel();
    j.append_add(
        2,
        45,
        vec![2; 100],
        true,
        Some(Box::new(move |r| tx.send(r).unwrap())),
    )
    .unwrap();
    match rx.recv().unwrap() {
        Err(JournalError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::StorageFull),
        other => panic!("{other:?}"),
    }
    drop(fault);
    assert_failed(&j, &reports);
    drop(j);
    assert_eq!(reload(&cfg), vec![1]);
}

#[test]
fn a_torn_write_is_dropped_on_reload() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let (j, reports) = open(&cfg);
    for id in 1..=10 {
        add(&j, id).unwrap();
    }
    let fault = Fault::new(Op::Write, dir.path()).torn(37).arm();
    assert!(add(&j, 11).is_err());
    drop(fault);
    assert_failed(&j, &reports);
    // A crash: the journal is dropped without being stopped.
    drop(j);

    assert_eq!(reload(&cfg), (1..=10).collect::<Vec<_>>());
    // The journal goes on from there.
    let (j, _) = open(&cfg);
    add(&j, 12).unwrap();
    j.stop().unwrap();
    drop(j);
    let mut want: Vec<i64> = (1..=10).collect();
    want.push(12);
    assert_eq!(reload(&cfg), want);
}

#[test]
fn a_file_that_cannot_be_made_fails_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let (j, reports) = open(&cfg);
    let fault = Fault::new(Op::Open, dir.path()).arm();
    let mut written = Vec::new();
    let mut id = 0;
    loop {
        id += 1;
        match add(&j, id) {
            Ok(()) => written.push(id),
            Err(_) => break,
        }
        assert!(id < 10_000, "the journal never needed a new file");
    }
    assert!(fault.hits() >= 1);
    drop(fault);
    assert_failed(&j, &reports);
    drop(j);
    let loaded = reload(&cfg);
    assert_eq!(&loaded[..written.len()], &written[..]);
}
