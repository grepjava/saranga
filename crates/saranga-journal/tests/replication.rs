//! A primary's journal replicated to a backup: the files written before
//! replication starts are copied as they are, what is appended afterwards
//! arrives as records, and the backup's directory loads to the same
//! records as the primary's.

use std::sync::{Arc, Mutex};

use saranga_journal::format::Record;
use saranga_journal::{Completion, Journal, JournalConfig, Replicator, SyncJournal};

#[derive(Default)]
struct Collect(Mutex<Vec<Record>>);

impl Replicator for Collect {
    fn replicate(&self, record: &Record, done: Option<Completion>) -> Option<Completion> {
        self.0.lock().unwrap().push(record.clone());
        if let Some(d) = done {
            d(Ok(()));
        }
        None
    }

    fn barrier(&self, done: Completion) -> Completion {
        done
    }
}

fn config(dir: &std::path::Path) -> JournalConfig {
    let mut c = JournalConfig::messages(dir);
    c.file_size = 64 * 1024;
    c.min_files = 2;
    c
}

fn committed(dir: &std::path::Path) -> Vec<(i64, Vec<u8>)> {
    let j = Journal::open(config(dir)).unwrap();
    let mut r: Vec<_> = j
        .load_all()
        .unwrap()
        .committed
        .into_iter()
        .map(|r| (r.id, r.data))
        .collect();
    r.sort();
    j.stop().unwrap();
    r
}

#[test]
fn a_backup_copy_loads_like_its_primary() {
    let primary = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let j = Journal::open(config(primary.path())).unwrap();
    j.load_all().unwrap();
    // Enough to fill a few files before replication starts.
    for id in 1..400 {
        j.append_add(id, 30, vec![id as u8; 300], false, None)
            .unwrap();
    }
    for id in 1..100 {
        j.append_delete(id, false, None).unwrap();
    }
    // A transaction open across the start of replication.
    j.append_add_tx(7, 1000, 31, b"before".to_vec()).unwrap();

    let collect = Arc::new(Collect::default());
    let mut announced = Vec::new();
    let files = j
        .start_replication(collect.clone(), |f| {
            announced = f.iter().map(|f| f.file_id).collect()
        })
        .unwrap();
    assert_eq!(
        files.iter().map(|f| f.file_id).collect::<Vec<_>>(),
        announced
    );
    assert!(files.len() > 1);

    // Replicated from here on.
    j.append_add_tx(7, 1001, 31, b"after".to_vec()).unwrap();
    j.append_commit(7, false, None).unwrap();
    for id in 400..500 {
        j.append_add(id, 30, vec![1; 300], false, None).unwrap();
    }
    j.append_update(450, 32, b"updated".to_vec(), false, false, None)
        .unwrap();
    j.append_delete(401, false, None).unwrap();
    j.sync().unwrap();

    let mut sync = SyncJournal::start(config(backup.path()), &announced).unwrap();
    for f in &files {
        let bytes = std::fs::read(primary.path().join(&f.name)).unwrap();
        for chunk in bytes.chunks(32 * 1024) {
            sync.write_file(f.file_id, chunk).unwrap();
        }
        sync.close_file(f.file_id).unwrap();
    }
    for r in collect.0.lock().unwrap().drain(..) {
        sync.append(r).unwrap();
    }
    sync.finish().unwrap();

    // The backup goes on appending once it has loaded its copy.
    let b = Journal::open(config(backup.path())).unwrap();
    b.load_sync_only().unwrap();
    b.append_delete(402, false, None).unwrap();
    b.stop().unwrap();
    j.append_delete(402, false, None).unwrap();
    j.stop().unwrap();
    drop(j);
    drop(b);

    let p = committed(primary.path());
    // The update of 450 is listed as well.
    assert_eq!(p.len(), 300 + 2 + 100 - 2 + 1);
    assert_eq!(committed(backup.path()), p);
}
