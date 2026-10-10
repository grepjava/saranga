//! The journals Artemis 2.57.0 wrote in the golden corpus
//! (`tests/golden/*/data.tar.gz`): every record must re-encode to the bytes
//! it was read from, and every journal must load.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use saranga_journal::format::{self, FileHeader, SIZE_HEADER};
use saranga_journal::{Journal, JournalConfig};

fn golden_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden")
}

/// Extracts every scenario's `data/` into a temporary directory.
fn scenarios() -> Vec<(String, tempfile::TempDir)> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(golden_root()).unwrap().flatten() {
        let archive = e.path().join("data.tar.gz");
        if !archive.exists() {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let gz = flate2::read::GzDecoder::new(std::fs::File::open(&archive).unwrap());
        tar::Archive::new(gz).unpack(dir.path()).unwrap();
        out.push((e.file_name().to_string_lossy().into_owned(), dir));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(!out.is_empty(), "no golden data directories found");
    out
}

fn journals(data: &Path) -> [(JournalConfig, &'static str); 2] {
    [
        (JournalConfig::messages(data.join("journal")), ".amq"),
        (JournalConfig::bindings(data.join("bindings")), ".bindings"),
    ]
}

#[test]
fn golden_records_reencode_byte_for_byte() {
    let mut total = 0;
    for (scenario, dir) in scenarios() {
        for (cfg, ext) in journals(&dir.path().join("data")) {
            for e in std::fs::read_dir(&cfg.directory).unwrap().flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if !name.ends_with(ext) {
                    continue;
                }
                let bytes = std::fs::read(e.path()).unwrap();
                let header = FileHeader::decode(&bytes, 0).unwrap();
                assert_eq!(header.journal_version, format::FORMAT_VERSION);
                assert_eq!(&header.encode()[..], &bytes[..SIZE_HEADER]);
                let rid = format::record_id_of(header.file_id);
                let scan = format::scan(&bytes, rid, header.journal_version, false).unwrap();
                assert!(!scan.damaged, "{scenario}/{name} has damaged records");
                for r in &scan.records {
                    let mut enc = Vec::new();
                    r.record.encode(rid, r.compact_count, &mut enc);
                    assert_eq!(
                        &bytes[r.position..r.position + enc.len()],
                        &enc[..],
                        "{scenario}/{name} record at {}",
                        r.position
                    );
                }
                total += scan.records.len();
            }
        }
    }
    assert!(total > 0);
}

/// Loads every golden journal, reloads it, and checks the second load sees
/// the same records: loading must not lose or invent anything.
#[test]
fn golden_journals_load_and_reload() {
    for (scenario, dir) in scenarios() {
        for (cfg, _) in journals(&dir.path().join("data")) {
            let summary = |cfg: &JournalConfig| {
                let j = Journal::open(cfg.clone()).unwrap();
                let r = j.load_all().unwrap();
                j.stop().unwrap();
                let mut by_type: BTreeMap<u8, usize> = BTreeMap::new();
                for rec in &r.committed {
                    *by_type.entry(rec.user_record_type).or_default() += 1;
                }
                (r.committed, r.prepared, by_type, r.max_id)
            };
            let first = summary(&cfg);
            let second = summary(&cfg);
            assert_eq!(first.0, second.0, "{scenario} {:?}", cfg.directory);
            assert_eq!(first.1, second.1, "{scenario} {:?}", cfg.directory);
            eprintln!(
                "{scenario} {}: {:?} prepared={} max_id={}",
                cfg.file_extension,
                first.2,
                first.1.len(),
                first.3
            );
        }
    }
}

#[test]
fn core_basic_bindings_hold_the_golden_queues() {
    let (_, dir) = scenarios()
        .into_iter()
        .find(|(s, _)| s == "core-basic")
        .unwrap();
    let j = Journal::open(JournalConfig::bindings(dir.path().join("data/bindings"))).unwrap();
    let r = j.load_all().unwrap();
    // QUEUE_BINDING_RECORD (21) bodies start with the queue name.
    let names: Vec<String> = r
        .committed
        .iter()
        .filter(|rec| rec.user_record_type == 21)
        .map(|rec| {
            let len = i32::from_be_bytes(rec.data[0..4].try_into().unwrap()) as usize;
            rec.data[4..4 + len]
                .chunks(2)
                .map(|c| char::from_u32(u16::from_le_bytes([c[0], c[1]]) as u32).unwrap())
                .collect()
        })
        .collect();
    assert!(names.iter().any(|n| n == "golden.q1"), "{names:?}");
    assert!(names.iter().any(|n| n == "DLQ"), "{names:?}");
}
