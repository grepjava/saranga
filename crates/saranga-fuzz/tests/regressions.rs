//! Every input a fuzz target once failed on, kept in
//! `fuzz/regressions/<target>/`, must now go through its target. With
//! `SARANGA_FUZZ_DIR` set to a corpus (such as `fuzz/corpus`), every input
//! under it goes through instead.

use std::path::{Path, PathBuf};

fn run_dir(root: &Path) -> usize {
    let mut n = 0;
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)
        .unwrap_or_else(|e| panic!("{}: {e}", root.display()))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    for dir in dirs {
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        let (_, target) = saranga_fuzz::TARGETS
            .iter()
            .find(|(t, _)| *t == name)
            .unwrap_or_else(|| panic!("{}: no such target", dir.display()));
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.file_name().is_some_and(|f| f != "README.md"))
            .collect();
        files.sort();
        for file in files {
            let data = std::fs::read(&file).unwrap();
            let result = std::panic::catch_unwind(|| target(&data));
            assert!(result.is_ok(), "{} failed", file.display());
            n += 1;
        }
    }
    n
}

#[test]
fn regressions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/regressions");
    run_dir(&root);
}

#[test]
fn corpus() {
    if let Some(dir) = std::env::var_os("SARANGA_FUZZ_DIR") {
        let n = run_dir(Path::new(&dir));
        println!("{n} inputs");
    }
}

#[test]
fn every_target_has_a_fuzz_target_file() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/fuzz_targets");
    for (name, _) in saranga_fuzz::TARGETS {
        let file = dir.join(format!("{name}.rs"));
        assert!(file.exists(), "{} is missing", file.display());
    }
}
