//! The main thread's stack on Windows is 1 MB, against 8 MB on Linux:
//! unoptimised, building the command line (clap's derived builders for
//! every command) takes more than 1 MB, so the binary asks for Linux's.

fn main() {
    let windows = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");
    match std::env::var("CARGO_CFG_TARGET_ENV").as_deref() {
        Ok("msvc") if windows => println!("cargo:rustc-link-arg-bins=/STACK:8388608"),
        Ok("gnu") if windows => println!("cargo:rustc-link-arg-bins=-Wl,--stack,8388608"),
        _ => {}
    }
}
