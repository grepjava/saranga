//! Every user record Artemis 2.57.0 wrote in the golden corpus
//! (`tests/golden/*/data.tar.gz`, every scenario present) must decode to a
//! typed record, never `Unknown`, and re-encode to the identical bytes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use saranga_journal::format::{self, FileHeader, Record};
use saranga_storage::codec::{DeleteEncoding, Xid};
use saranga_storage::{JournalKind, RecordCodec, StorageRecord, decode_record, ids};

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

#[test]
fn golden_record_bodies_reencode_byte_for_byte() {
    let mut by_type: BTreeMap<&str, usize> = BTreeMap::new();
    for (scenario, dir) in scenarios() {
        for (kind, sub, ext) in [
            (JournalKind::Bindings, "bindings", ".bindings"),
            (JournalKind::Messages, "journal", ".amq"),
        ] {
            let path = dir.path().join("data").join(sub);
            for e in std::fs::read_dir(&path).unwrap().flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if !name.ends_with(ext) {
                    continue;
                }
                let bytes = std::fs::read(e.path()).unwrap();
                let header = FileHeader::decode(&bytes, 0).unwrap();
                let rid = format::record_id_of(header.file_id);
                let scan = format::scan(&bytes, rid, header.journal_version, false).unwrap();
                for r in scan.records {
                    let at = format!("{scenario}/{sub}/{name} @{}", r.position);
                    match r.record {
                        Record::Event {
                            user_type, data, ..
                        }
                        | Record::Add {
                            user_type, data, ..
                        }
                        | Record::Update {
                            user_type, data, ..
                        }
                        | Record::AddTx {
                            user_type, data, ..
                        }
                        | Record::UpdateTx {
                            user_type, data, ..
                        } => {
                            let rec = decode_record(kind, user_type, &data)
                                .unwrap_or_else(|e| panic!("{at}: type {user_type}: {e}"));
                            assert!(
                                !matches!(rec, StorageRecord::Unknown { .. }),
                                "{at}: unknown type {user_type}"
                            );
                            assert_eq!(rec.user_type(), user_type, "{at}");
                            assert_eq!(rec.encode(), data, "{at}: type {user_type}");
                            *by_type.entry(ids::name(user_type).unwrap()).or_default() += 1;
                        }
                        Record::DeleteTx { data, .. } if !data.is_empty() => {
                            let d = DeleteEncoding::decode(&data).unwrap();
                            assert_eq!(d.encode(), data, "{at}");
                            *by_type.entry("<delete tx body>").or_default() += 1;
                        }
                        Record::Prepare { data, .. } if !data.is_empty() => {
                            let xid = Xid::decode(&data).unwrap();
                            assert_eq!(xid.encode(), data, "{at}");
                            *by_type.entry("<prepare xid>").or_default() += 1;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    eprintln!("{by_type:#?}");
    assert!(by_type.len() > 5, "{by_type:?}");
}
