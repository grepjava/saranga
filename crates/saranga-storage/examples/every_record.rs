//! Writes a data directory with one record of (almost) every user record
//! type (see `tests/support/every_record.rs`).
//!
//!   cargo run -p saranga-storage --example every_record -- <data-dir>
//!
//! `tests/fixtures/every_record.data-print.txt` is Java's output for it:
//! `artemis data print --journal <dir>/journal --bindings <dir>/bindings
//! --paging <dir>/paging --large-messages <dir>/large-messages`, run with
//! `JAVA_ARGS_APPEND="-Duser.timezone=UTC -Dstdout.encoding=UTF-8"`.

#[path = "../tests/support/every_record.rs"]
mod every_record;

fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("data dir"));
    every_record::write(&dir);
}
