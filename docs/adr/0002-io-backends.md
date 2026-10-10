# ADR 0002: Journal and paging I/O backends

Status: accepted

## Context
Artemis offers ASYNCIO (libaio, Linux), NIO and MAPPED journal types, and batches writes
in a `TimedBuffer` so that one sync covers many appends. Saranga must produce byte-identical
files whichever backend is used, and must run on Windows.

## Decision
- A `JournalFile` trait with three implementations:
  - `uring` (Linux, io_uring, O_DIRECT, aligned buffers). This is the equivalent of ASYNCIO.
  - `buffered` (std::fs, positional writes, `sync_data`). This is the equivalent of NIO and is the default on Windows and macOS.
  - `mapped` (mmap, msync). This is the equivalent of MAPPED.
- A single journal writer task per journal. It does group commit: it collects appends until the buffer is
  full or the `journal-buffer-timeout` elapses, issues one write and one sync, then
  completes every waiting future.
- The file format, alignment and pre-allocation (`journal-file-size`, `journal-min-files`,
  `journal-pool-files`) follow Artemis exactly. Alignment is 512 bytes for AIO-equivalent
  and 1 byte for NIO, as in Artemis.
- `journal-type` in `broker.xml` selects the backend. When `ASYNCIO` is requested on a
  platform without io_uring, Saranga falls back to `buffered` with a warning, as Artemis does.

## Consequences
- The backends are interchangeable on the same data dir. Golden tests run against all three.
- The io_uring backend is behind a Cargo feature so Windows builds never compile it.
