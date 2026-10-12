//! Journal behaviour: what is written survives a reopen, transactions
//! resolve as Artemis resolves them, files are reclaimed and compacted.

use std::path::Path;

use saranga_journal::control::{ControlFile, FILE_COMPACT_CONTROL};
use saranga_journal::format::{self, FileHeader};
use saranga_journal::{Journal, JournalConfig, RecordInfo};

fn config(dir: &Path) -> JournalConfig {
    JournalConfig {
        file_size: 64 * 1024,
        min_files: 2,
        pool_size: 10,
        compact_min_files: 0,
        datasync: false,
        ..JournalConfig::messages(dir)
    }
}

fn open(cfg: &JournalConfig) -> Journal {
    let j = Journal::open(cfg.clone()).unwrap();
    j.load_all().unwrap();
    j
}

fn reload(cfg: &JournalConfig) -> saranga_journal::LoadResult {
    let j = Journal::open(cfg.clone()).unwrap();
    let r = j.load_all().unwrap();
    j.stop().unwrap();
    r
}

fn ids(records: &[RecordInfo]) -> Vec<i64> {
    records
        .iter()
        .filter(|r| !r.is_update)
        .map(|r| r.id)
        .collect()
}

#[test]
fn records_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    j.append_add(1, 45, b"one".to_vec(), false, None).unwrap();
    j.append_add(2, 45, b"two".to_vec(), true, None).unwrap();
    assert!(
        j.append_update(1, 34, b"upd".to_vec(), false, true, None)
            .unwrap()
    );
    assert!(j.append_delete(2, true, None).unwrap());
    assert!(!j.append_delete(99, true, None).unwrap());
    assert!(!j.append_update(99, 34, vec![], true, false, None).unwrap());
    j.stop().unwrap();
    drop(j);

    let r = reload(&cfg);
    assert_eq!(ids(&r.committed), vec![1]);
    let upd: Vec<_> = r.committed.iter().filter(|r| r.is_update).collect();
    assert_eq!(upd.len(), 1);
    assert_eq!(upd[0].data, b"upd");
    assert_eq!(r.max_id, 2);
}

#[test]
fn transactions_commit_rollback_prepare_and_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    j.append_add(1, 45, b"base".to_vec(), false, None).unwrap();
    // committed: adds 10, deletes 1
    j.append_add_tx(100, 10, 45, b"ten".to_vec()).unwrap();
    j.append_delete_tx(100, 1, vec![]).unwrap();
    j.append_commit(100, true, None).unwrap();
    // rolled back
    j.append_add_tx(101, 11, 45, b"eleven".to_vec()).unwrap();
    j.append_rollback(101, true, None).unwrap();
    // prepared, never resolved
    j.append_add_tx(102, 12, 45, b"twelve".to_vec()).unwrap();
    j.append_prepare(102, b"xid".to_vec(), true, None).unwrap();
    // never completed
    j.append_add_tx(103, 13, 45, b"thirteen".to_vec()).unwrap();
    j.sync().unwrap();
    assert!(j.append_commit(999, true, None).is_err());
    j.stop().unwrap();
    drop(j);

    let r = reload(&cfg);
    assert_eq!(ids(&r.committed), vec![10]);
    assert_eq!(r.prepared.len(), 1);
    assert_eq!(r.prepared[0].id, 102);
    assert_eq!(r.prepared[0].extra_data, b"xid");
    assert_eq!(ids(&r.prepared[0].records), vec![12]);
    assert_eq!(r.failed, vec![103]);
    assert_eq!(r.max_id, 13);

    // The loader rolled 103 back: a later load does not report it again.
    let r = reload(&cfg);
    assert!(r.failed.is_empty());
    assert_eq!(r.prepared.len(), 1);
}

#[test]
fn prepared_transaction_can_be_committed_after_reload() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    j.append_add_tx(7, 70, 45, b"x".to_vec()).unwrap();
    j.append_prepare(7, b"xid".to_vec(), true, None).unwrap();
    j.stop().unwrap();
    drop(j);

    let j = Journal::open(cfg.clone()).unwrap();
    let r = j.load_all().unwrap();
    assert_eq!(r.prepared.len(), 1);
    j.append_commit(7, true, None).unwrap();
    j.stop().unwrap();
    drop(j);

    let r = reload(&cfg);
    assert!(r.prepared.is_empty());
    assert_eq!(ids(&r.committed), vec![70]);
}

#[test]
fn files_roll_over_and_are_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    let body = vec![7u8; 1000];
    for id in 0..300 {
        j.append_add(id, 45, body.clone(), false, None).unwrap();
    }
    j.sync().unwrap();
    let before = j.data_files().len();
    assert!(before >= 4, "{before} files");
    for id in 0..290 {
        j.append_delete(id, false, None).unwrap();
    }
    // Reclaim runs when the journal moves to the next file.
    j.force_move_next_file().unwrap();
    j.append_add(1000, 45, body.clone(), true, None).unwrap();
    j.check_reclaim_status().unwrap();
    let after = j.data_files().len();
    assert!(after < before, "{before} -> {after}");
    j.stop().unwrap();
    drop(j);

    let r = reload(&cfg);
    let mut expect: Vec<i64> = (290..300).collect();
    expect.push(1000);
    assert_eq!(ids(&r.committed), expect);
}

#[test]
fn compaction_keeps_live_records_only() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    let body = vec![1u8; 1000];
    for id in 0..200 {
        j.append_add(id, 45, body.clone(), false, None).unwrap();
        j.append_update(id, 34, vec![id as u8], false, false, None)
            .unwrap();
    }
    for id in 0..195 {
        j.append_delete(id, false, None).unwrap();
    }
    // An open transaction spanning the compaction, committed after it.
    j.append_add_tx(500, 501, 45, b"tx".to_vec()).unwrap();
    j.set_auto_reclaim(false);
    let files_before = j.data_files().len();
    j.compact().unwrap();
    j.append_commit(500, true, None).unwrap();
    let files_after = j.data_files().len();
    assert!(
        files_after < files_before,
        "{files_before} -> {files_after}"
    );
    assert!(!dir.path().join(FILE_COMPACT_CONTROL).exists());
    j.stop().unwrap();
    drop(j);

    let r = reload(&cfg);
    let mut expect: Vec<i64> = (195..200).collect();
    expect.push(501);
    assert_eq!(ids(&r.committed), expect);
    let updates = r.committed.iter().filter(|r| r.is_update).count();
    assert_eq!(updates, 5);
    // Compacted records carry their compact count.
    assert!(
        r.committed
            .iter()
            .filter(|r| r.id < 200)
            .all(|r| r.compact_count == 1)
    );
    assert!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .all(|e| !e.file_name().to_string_lossy().ends_with(".cmp"))
    );
}

#[test]
fn replaceable_updates_keep_only_the_last_on_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = Journal::open(cfg.clone()).unwrap();
    j.replaceable_record(34);
    j.load_all().unwrap();
    j.append_add(1, 45, vec![0; 100], false, None).unwrap();
    for n in 0..20u8 {
        j.append_update(1, 34, vec![n], false, true, None).unwrap();
    }
    j.compact().unwrap();
    j.stop().unwrap();
    drop(j);
    let r = reload(&cfg);
    let upd: Vec<_> = r.committed.iter().filter(|r| r.is_update).collect();
    assert_eq!(upd.len(), 1);
    assert_eq!(upd[0].data, vec![19]);
}

#[test]
fn interrupted_compaction_is_finished_on_load() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    j.append_add(1, 45, b"old".to_vec(), true, None).unwrap();
    j.stop().unwrap();
    drop(j);

    // A compacted replacement for the data file, as the compactor leaves it
    // before renaming: same file ID, new name, `.cmp`, plus the control file.
    let data = open(&cfg);
    let files = data.data_files();
    data.stop().unwrap();
    drop(data);
    let old = &files[0];
    let mut cmp = FileHeader::new(0, old.file_id).encode().to_vec();
    format::Record::Add {
        id: 2,
        user_type: 45,
        data: b"compacted".to_vec(),
    }
    .encode(format::record_id_of(old.file_id), 1, &mut cmp);
    cmp.resize(cfg.file_size, 0);
    std::fs::write(dir.path().join("activemq-data-50.amq.cmp"), &cmp).unwrap();
    std::fs::write(dir.path().join("activemq-data-60.amq.tmp"), b"junk").unwrap();
    ControlFile {
        data_files: vec![old.name.clone()],
        new_files: vec!["activemq-data-50.amq.cmp".into()],
        renames: vec![],
    }
    .write(dir.path())
    .unwrap();

    let r = reload(&cfg);
    assert_eq!(ids(&r.committed), vec![2]);
    assert!(!dir.path().join(FILE_COMPACT_CONTROL).exists());
    assert!(dir.path().join("activemq-data-50.amq").exists());
    assert!(!dir.path().join("activemq-data-60.amq.tmp").exists());
}

#[test]
fn torn_tail_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    j.append_add(1, 45, vec![1; 50], true, None).unwrap();
    j.append_add(2, 45, vec![2; 50], true, None).unwrap();
    let name = j.data_files().last().unwrap().name.clone();
    j.stop().unwrap();
    drop(j);

    // Tear the second record: zero its check size.
    let path = dir.path().join(&name);
    let mut b = std::fs::read(&path).unwrap();
    let end = format::SIZE_HEADER + 2 * (format::SIZE_ADD_RECORD + 51);
    b[end - 4..end].fill(0);
    std::fs::write(&path, b).unwrap();

    let r = reload(&cfg);
    assert_eq!(ids(&r.committed), vec![1]);

    // Appends continue after the last valid record and stay readable.
    let j = open(&cfg);
    j.append_add(3, 45, vec![3; 10], true, None).unwrap();
    j.stop().unwrap();
    drop(j);
    assert_eq!(ids(&reload(&cfg).committed), vec![1, 3]);
}

#[test]
fn completions_run_after_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let j = open(&cfg);
    let (tx, rx) = std::sync::mpsc::channel();
    for id in 0..100 {
        let tx = tx.clone();
        j.append_add(
            id,
            45,
            vec![0; 10],
            true,
            Some(Box::new(move |r| tx.send((id, r.is_ok())).unwrap())),
        )
        .unwrap();
    }
    let got: Vec<_> = (0..100).map(|_| rx.recv().unwrap()).collect();
    assert!(got.iter().all(|(_, ok)| *ok));
    assert_eq!(
        got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        (0..100).collect::<Vec<_>>()
    );
}

#[test]
fn oversized_record_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = JournalConfig {
        buffer_size: 4096,
        ..config(dir.path())
    };
    let j = open(&cfg);
    let e = j.append_add(1, 45, vec![0; 5000], true, None).unwrap_err();
    assert!(e.to_string().starts_with("AMQ149005"), "{e}");
}

#[test]
fn retention_keeps_a_copy_of_each_filled_file() {
    let dir = tempfile::tempdir().unwrap();
    let history = dir.path().join("history");
    let cfg = JournalConfig {
        retention: Some(saranga_journal::Retention {
            directory: history.clone(),
            max_bytes: -1,
            period_millis: 86_400_000,
            local_offset: |_| 0,
        }),
        ..config(&dir.path().join("journal"))
    };
    let j = open(&cfg);
    // Each record a sixth of a file: about five files filled.
    for id in 1..=30 {
        j.append_add(id, 45, vec![7u8; 10 * 1024], false, None)
            .unwrap();
        j.append_delete(id, false, None).unwrap();
    }
    j.stop().unwrap();
    drop(j);

    let mut copies: Vec<String> = std::fs::read_dir(&history)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    copies.sort();
    assert!(copies.len() >= 3, "{copies:?}");
    for c in &copies {
        // activemq-data-yyyyMMddHHmmss-<id>.amq
        let rest = c.strip_prefix("activemq-data-").unwrap();
        let (date, id) = rest.strip_suffix(".amq").unwrap().split_once('-').unwrap();
        assert_eq!(date.len(), 14);
        id.parse::<i64>().unwrap();
        // A copy is a whole journal file.
        assert_eq!(
            std::fs::metadata(history.join(c)).unwrap().len(),
            cfg.file_size as u64
        );
    }
    // The last filled file may still wait as .bkp; a restart copies it.
    let journal = dir.path().join("journal");
    let pending = |d: &Path| {
        std::fs::read_dir(d)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".bkp")
            })
            .count()
    };
    let r = reload(&cfg);
    assert!(r.committed.is_empty());
    assert_eq!(pending(&journal), 0);

    // The copies load as a journal of their own, as `data print --journal
    // <history>` reads them.
    let copy_cfg = JournalConfig {
        retention: None,
        ..config(&history)
    };
    let j = Journal::open(copy_cfg).unwrap();
    let mut seen = 0;
    struct Count<'a>(&'a mut usize);
    impl saranga_journal::LoaderCallback for Count<'_> {
        fn add_record(&mut self, _: RecordInfo) {
            *self.0 += 1;
        }
        fn update_record(&mut self, _: RecordInfo) {}
        fn delete_record(&mut self, _: i64) {}
        fn add_prepared_transaction(&mut self, _: saranga_journal::PreparedTransactionInfo) {}
    }
    j.load(&mut Count(&mut seen)).unwrap();
    assert!(seen >= 15, "{seen}");
}

#[test]
fn retention_storage_limit_removes_the_oldest_copies() {
    let dir = tempfile::tempdir().unwrap();
    let history = dir.path().join("history");
    let cfg = JournalConfig {
        retention: Some(saranga_journal::Retention {
            directory: history.clone(),
            // Two files' worth.
            max_bytes: 2 * 64 * 1024,
            period_millis: -1,
            local_offset: |_| 0,
        }),
        ..config(&dir.path().join("journal"))
    };
    let j = open(&cfg);
    for id in 1..=60 {
        j.append_add(id, 45, vec![7u8; 10 * 1024], false, None)
            .unwrap();
        j.append_delete(id, false, None).unwrap();
    }
    j.stop().unwrap();
    assert_eq!(std::fs::read_dir(&history).unwrap().count(), 2);
}
