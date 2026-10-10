//! Lists the records of every file in a journal directory, in file order.
//!
//!   cargo run -p saranga-journal --example scan -- <dir> <extension>

use saranga_journal::format::{self, FileHeader};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (dir, ext) = (&args[1], format!(".{}", args[2]));
    let mut files: Vec<(i64, String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(&ext))
        .map(|e| {
            let b = std::fs::read(e.path()).unwrap();
            let h = FileHeader::decode(&b, 0).unwrap();
            (h.file_id, e.file_name().to_string_lossy().into_owned(), b)
        })
        .collect();
    files.sort_by_key(|f| f.0);
    for (id, name, b) in files {
        let s = format::scan(&b, format::record_id_of(id), format::FORMAT_VERSION, false).unwrap();
        println!(
            "{name} id={id} records={} last={} damaged={}",
            s.records.len(),
            s.last_data_pos,
            s.damaged
        );
        for r in s.records {
            let line = format!("{:?}", r.record);
            println!(
                "  @{} cc={} {}",
                r.position,
                r.compact_count,
                &line[..line.len().min(90)]
            );
        }
    }
}
