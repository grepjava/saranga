//! Writes a journal exercising every record type, file roll-over, reclaim
//! and compaction, or dumps a journal in the format of
//! `tests/interop/journal/JournalDump.java`, so the two implementations can be
//! diffed (`scripts/journal-interop.sh`).
//!
//!   cargo run -p saranga-journal --example sample -- write <dir>
//!   cargo run -p saranga-journal --example sample -- dump <dir>

use saranga_journal::{Journal, JournalConfig, RecordInfo};

const FILE_SIZE: usize = 64 * 1024;

fn config(dir: &str) -> JournalConfig {
    JournalConfig {
        file_size: FILE_SIZE,
        pool_size: 10,
        compact_min_files: 0,
        ..JournalConfig::messages(dir)
    }
}

fn write(dir: &str) {
    let j = Journal::open(config(dir)).unwrap();
    j.load_all().unwrap();
    let body = |id: i64, n: usize| {
        (0..n)
            .map(|i| (id as usize * 31 + i) as u8)
            .collect::<Vec<u8>>()
    };
    // Enough to roll over several files, most of it deleted again.
    for id in 1..=150 {
        j.append_add(id, 45, body(id, 900), false, None).unwrap();
        if id % 3 == 0 {
            j.append_update(id, 34, body(id, 8), false, false, None)
                .unwrap();
        }
    }
    for id in 1..=120 {
        j.append_delete(id, false, None).unwrap();
    }
    // Committed, rolled back and prepared transactions, one spanning the
    // compaction.
    j.append_add_tx(1000, 1001, 45, body(1001, 40)).unwrap();
    j.append_update_tx(1000, 130, 34, body(130, 4)).unwrap();
    j.append_delete_tx(1000, 121, vec![]).unwrap();
    j.append_commit(1000, false, None).unwrap();
    j.append_add_tx(2000, 2001, 45, body(2001, 40)).unwrap();
    j.append_rollback(2000, false, None).unwrap();
    j.append_add_tx(3000, 3001, 45, body(3001, 40)).unwrap();
    j.append_delete_tx(3000, 122, body(122, 3)).unwrap();
    j.compact().unwrap();
    j.append_prepare(3000, b"xid-3000".to_vec(), false, None)
        .unwrap();
    j.append_add_event(4000, 30, body(4000, 10), false, None)
        .unwrap();
    // Never completed: both loaders report it as failed.
    j.append_add_tx(5000, 5001, 45, body(5001, 10)).unwrap();
    j.append_add(6000, 45, body(6000, 2000), true, None)
        .unwrap();
    j.stop().unwrap();
}

fn record(r: &RecordInfo) -> String {
    let hex: String = r.data.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "R id={} type={} update={} cc={} data={hex}",
        r.id, r.user_record_type as i8, r.is_update, r.compact_count
    )
}

fn dump(dir: &str) {
    let j = Journal::open(config(dir)).unwrap();
    let r = j.load_all().unwrap();
    for rec in &r.committed {
        println!("{}", record(rec));
    }
    for p in &r.prepared {
        let hex: String = p.extra_data.iter().map(|b| format!("{b:02x}")).collect();
        println!("P tx={} extra={hex}", p.id);
        for rec in &p.records {
            println!("  {}", record(rec));
        }
        for rec in &p.records_to_delete {
            println!("  D id={}", rec.id);
        }
    }
    for tx in &r.failed {
        println!("F tx={tx}");
    }
    j.stop().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match (args.get(1).map(String::as_str), args.get(2)) {
        (Some("write"), Some(dir)) => write(dir),
        (Some("dump"), Some(dir)) => dump(dir),
        _ => eprintln!("usage: sample write|dump <dir>"),
    }
}
