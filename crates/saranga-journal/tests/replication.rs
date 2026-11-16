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

/// As the broker's replicator: a caller's completion runs once the local
/// write is done and the backup answered; without one nothing waits for
/// the backup.
#[derive(Default)]
struct Withheld(Mutex<Vec<Completion>>);

impl Replicator for Withheld {
    fn replicate(&self, _record: &Record, done: Option<Completion>) -> Option<Completion> {
        let done = done?;
        let parts = Arc::new(Mutex::new((done, 0u8)));
        let both = |parts: Arc<Mutex<(Completion, u8)>>| -> Completion {
            Box::new(move |_| {
                let mut p = parts.lock().unwrap();
                p.1 += 1;
                if p.1 == 2 {
                    let (d, _) = std::mem::replace(&mut *p, (Box::new(|_| {}), 2));
                    d(Ok(()));
                }
            })
        };
        self.0.lock().unwrap().push(both(parts.clone()));
        Some(both(parts))
    }

    fn barrier(&self, done: Completion) -> Completion {
        done
    }
}

/// A synchronous append, update, delete, prepare, commit or rollback with
/// no completion of its own returns only once the backup has the record,
/// as Artemis's `ReplicationManager` lines up the operation context of
/// every replicated call.
#[test]
fn a_synchronous_append_waits_for_the_backup() {
    let dir = tempfile::tempdir().unwrap();
    let j = Arc::new(Journal::open(config(dir.path())).unwrap());
    j.load_all().unwrap();
    j.append_add(1, 30, b"one".to_vec(), true, None).unwrap();
    let backup = Arc::new(Withheld::default());
    j.start_replication(backup.clone(), |_| {}).unwrap();

    type Op = Box<dyn FnOnce(&Journal) + Send>;
    let ops: Vec<(&str, Op)> = vec![
        (
            "add",
            Box::new(|j| j.append_add(2, 30, b"two".to_vec(), true, None).unwrap()),
        ),
        (
            "update",
            Box::new(|j| {
                j.append_update(1, 30, b"1b".to_vec(), true, false, None)
                    .unwrap();
            }),
        ),
        (
            "delete",
            Box::new(|j| {
                j.append_delete(2, true, None).unwrap();
            }),
        ),
        (
            "prepare",
            Box::new(|j| {
                j.append_add_tx(9, 3, 30, b"three".to_vec()).unwrap();
                j.append_prepare(9, Vec::new(), true, None).unwrap();
            }),
        ),
        (
            "commit",
            Box::new(|j| j.append_commit(9, true, None).unwrap()),
        ),
        (
            "rollback",
            Box::new(|j| {
                j.append_add_tx(10, 4, 30, b"four".to_vec()).unwrap();
                j.append_rollback(10, true, None).unwrap();
            }),
        ),
    ];
    for (name, op) in ops {
        let (tx, rx) = std::sync::mpsc::channel();
        let journal = j.clone();
        let t = std::thread::spawn(move || {
            op(&journal);
            tx.send(()).unwrap();
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(300))
                .is_err(),
            "{name} returned before the backup answered"
        );
        for done in backup.0.lock().unwrap().drain(..) {
            done(Ok(()));
        }
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("{name} returned once the backup answered"));
        t.join().unwrap();
    }
    j.stop().unwrap();
}

/// A file that cannot be opened as replication starts is a critical I/O
/// error, as it is for an append: the journal takes no more writes.
#[test]
fn a_failed_file_switch_at_replication_start_is_critical() {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(config(dir.path())).unwrap();
    j.load_all().unwrap();
    j.append_add(1, 30, vec![1; 10], true, None).unwrap();
    // The file made at this switch, for the next, cannot be.
    let fault = saranga_fault::Fault::new(saranga_fault::Op::Open, dir.path()).arm();
    j.force_move_next_file().unwrap();
    let started = j.start_replication(Arc::new(Collect::default()), |_| {});
    drop(fault);
    assert!(started.is_err());
    assert!(j.failure().is_some(), "a critical I/O error");
    assert!(j.append_add(2, 30, vec![2; 10], true, None).is_err());
}
