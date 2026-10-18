//! The binary starts, unoptimised too: building the command line once took
//! more than the 1 MB main thread stack Windows gives (see build.rs).

use std::process::Command;

fn saranga(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_saranga"))
        .args(args)
        .env_remove("ARTEMIS_INSTANCE")
        .output()
        .expect("saranga runs")
}

#[test]
fn version() {
    let out = saranga(&["--version"]);
    assert!(out.status.success(), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("saranga "));
}

#[test]
fn a_command_s_help() {
    let out = saranga(&["perf", "client", "--help"]);
    assert!(out.status.success(), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stdout).contains("--message-count"));
}
