//! `saranga data print` against Java's `artemis data print`:
//! - every golden scenario that has a `data-print.txt` (made by
//!   `scripts/golden-print.sh`);
//! - `fixtures/every_record.data-print.txt`, Java's output for the data
//!   written by `support/every_record.rs` (one record of almost every type).
//!
//! Normalization, for things that are not data:
//! - The banner before the first `****` line (Saranga prints its own name).
//! - `Journal path: ...` lines (the tools ran on different paths).
//! - Java identity hash codes: `]@<n>` after a message and `XidImpl (<n>`.
//! - The `setting=` part of persisted address settings, which Saranga does
//!   not render yet.

use std::path::{Path, PathBuf};

use saranga_storage::describe::{PrintDataOptions, print_data};

#[path = "support/every_record.rs"]
mod every_record;

fn golden_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden")
}

fn normalize(text: &str) -> Vec<String> {
    let lines: Vec<String> = text
        .lines()
        .skip_while(|l| !l.starts_with("****"))
        .map(|l| {
            if l.starts_with("Journal path: ") {
                return "Journal path: <path>".to_owned();
            }
            if let Some(i) = l.find(" folder = ") {
                let folder = l[i + 10..].rsplit(['/', '\\']).next().unwrap_or("");
                return format!("{} folder = {folder}", &l[..i]);
            }
            let mut l = mask_after(&mask_after(l, "]@"), "XidImpl (");
            if l.contains("PersistedAddressSetting")
                && let Some(i) = l.find(", setting=")
            {
                l.truncate(i);
                l.push_str(", setting=<masked>");
            }
            l
        })
        .collect();
    sort_stores(lines)
}

/// The paging section lists stores in Java hash-map order, which for stores
/// sharing a bucket depends on the order Java listed the directories: the
/// stores are compared as a set.
fn sort_stores(lines: Vec<String>) -> Vec<String> {
    const STORE: &str = "####################################################################################################";
    let Some(start) = lines.iter().position(|l| l == STORE) else {
        return lines;
    };
    let mut out = lines[..start].to_vec();
    let mut stores: Vec<Vec<String>> = Vec::new();
    for l in &lines[start..] {
        if l == STORE {
            stores.push(Vec::new());
        }
        stores
            .last_mut()
            .expect("starts with a store")
            .push(l.clone());
    }
    stores.sort();
    out.extend(stores.into_iter().flatten());
    out
}

/// Replaces the digits following each `marker` with `<hash>`.
fn mask_after(line: &str, marker: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(i) = rest.find(marker) {
        let (head, tail) = rest.split_at(i + marker.len());
        out.push_str(head);
        let digits = tail
            .find(|c: char| !(c.is_ascii_digit() || c == '-'))
            .unwrap_or(tail.len());
        if digits > 0 {
            out.push_str("<hash>");
        }
        rest = &tail[digits..];
    }
    out.push_str(rest);
    out
}

/// Runs `data print` on `data` (holding `bindings/`, `journal/`, `paging/`)
/// and compares it with Java's output.
fn compare(name: &str, data: &Path, expected: &str) {
    let mut out = Vec::new();
    print_data(
        &mut out,
        &data.join("bindings"),
        &data.join("journal"),
        &data.join("paging"),
        &PrintDataOptions {
            print_pages: Some(saranga_paging::print::print_pages),
            ..PrintDataOptions::default()
        },
    )
    .unwrap();
    let actual = normalize(&String::from_utf8(out).unwrap());
    let expected = normalize(expected);
    for (n, (a, x)) in actual.iter().zip(&expected).enumerate() {
        assert_eq!(a, x, "{name}: line {} differs", n + 1);
    }
    assert_eq!(actual.len(), expected.len(), "{name}: line count");
}

#[test]
fn data_print_matches_artemis_on_the_golden_corpus() {
    let mut compared = Vec::new();
    for e in std::fs::read_dir(golden_root()).unwrap().flatten() {
        let expected = e.path().join("data-print.txt");
        let archive = e.path().join("data.tar.gz");
        if !expected.exists() || !archive.exists() {
            continue;
        }
        let scenario = e.file_name().to_string_lossy().into_owned();
        let dir = tempfile::tempdir().unwrap();
        let gz = flate2::read::GzDecoder::new(std::fs::File::open(&archive).unwrap());
        tar::Archive::new(gz).unpack(dir.path()).unwrap();
        compare(
            &scenario,
            &dir.path().join("data"),
            &std::fs::read_to_string(&expected).unwrap(),
        );
        compared.push(scenario);
    }
    eprintln!("compared: {compared:?}");
    assert!(!compared.is_empty(), "no data-print.txt found");
}

#[test]
fn data_print_matches_artemis_on_every_record_type() {
    let dir = tempfile::tempdir().unwrap();
    every_record::write(dir.path());
    let expected = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/every_record.data-print.txt"),
    )
    .unwrap();
    compare("every_record", dir.path(), &expected);
}

#[test]
fn masking() {
    assert_eq!(
        mask_after("a]@123, XidImpl (-5 bq", "]@"),
        "a]@<hash>, XidImpl (-5 bq"
    );
    assert_eq!(
        mask_after("XidImpl (-5 bq", "XidImpl ("),
        "XidImpl (<hash> bq"
    );
}
