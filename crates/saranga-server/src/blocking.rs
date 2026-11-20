//! Work that may block (a whole file read, a large body copied and synced)
//! made where the broker cannot always leave the runtime.

/// Runs `f`, which may block, so that it does not hold up the other tasks
/// of a multi-thread runtime's worker (`block_in_place` hands them to
/// another worker first). Elsewhere (no runtime, a current-thread one such
/// as a remoting event loop, a blocking thread) it just runs.
pub(crate) fn off_the_workers<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// A whole file, read off the workers.
pub(crate) fn read_file(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    off_the_workers(|| {
        saranga_fault::check(saranga_fault::Op::Read, path)?;
        std::fs::read(path)
    })
}
