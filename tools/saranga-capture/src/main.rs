//! Recording TCP proxy for building the golden corpus.
//!
//! `saranga-capture <listen-addr> <upstream-addr> <out-dir>`
//!
//! Every accepted connection N is forwarded to the upstream broker, and the raw
//! bytes are written to `<out-dir>/conn-N/c2s.bin` (client to server) and
//! `<out-dir>/conn-N/s2c.bin` (server to client), unmodified. A directory that
//! is there already (an earlier capture into the same `<out-dir>`) is never
//! written to: the connection takes the next number free.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let [_, listen, upstream, out] = args.as_slice() else {
        anyhow::bail!("usage: saranga-capture <listen-addr> <upstream-addr> <out-dir>");
    };
    let out = PathBuf::from(out);
    tokio::fs::create_dir_all(&out).await?;

    let listener = TcpListener::bind(listen).await.context("bind")?;
    eprintln!("capturing {listen} -> {upstream} into {}", out.display());
    let mut next = 0;
    loop {
        let (client, peer) = listener.accept().await?;
        let (n, dir) = claim_dir(&out, &mut next).context("making a connection directory")?;
        let upstream = upstream.clone();
        tokio::spawn(async move {
            if let Err(e) = proxy(client, &upstream, &dir).await {
                eprintln!("conn-{n} ({peer}): {e:#}");
            }
        });
    }
}

/// The first `conn-N` from `*next` on that is not there yet, made now (so no
/// other capture into `out` can take it too); `*next` goes past it.
fn claim_dir(out: &Path, next: &mut usize) -> std::io::Result<(usize, PathBuf)> {
    loop {
        let n = *next;
        *next += 1;
        let dir = out.join(format!("conn-{n}"));
        match std::fs::create_dir(&dir) {
            Ok(()) => return Ok((n, dir)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

/// A recording file, made new: one that is there is not truncated.
async fn record(path: PathBuf) -> std::io::Result<File> {
    File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .await
}

async fn proxy(client: TcpStream, upstream: &str, dir: &Path) -> Result<()> {
    let server = TcpStream::connect(upstream)
        .await
        .context("connect upstream")?;
    let (cr, cw) = client.into_split();
    let (sr, sw) = server.into_split();
    let c2s = record(dir.join("c2s.bin")).await?;
    let s2c = record(dir.join("s2c.bin")).await?;
    let (a, b) = tokio::join!(pump(cr, sw, c2s), pump(sr, cw, s2c));
    a.and(b)
}

/// Copy `from` to `to`, recording every byte, until EOF on `from`.
async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    mut log: File,
) -> Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = from.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        log.write_all(&buf[..n]).await?;
        to.write_all(&buf[..n]).await?;
    }
    log.flush().await?;
    let _ = to.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A capture into a directory an earlier one used takes the numbers that
    /// are free and leaves the earlier recordings as they were.
    #[tokio::test]
    async fn a_new_capture_leaves_earlier_recordings_alone() {
        let out = std::env::temp_dir().join(format!("saranga-capture-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        for n in [0, 1] {
            let dir = out.join(format!("conn-{n}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("c2s.bin"), b"earlier").unwrap();
        }
        let mut next = 0;
        let (n, dir) = claim_dir(&out, &mut next).unwrap();
        assert_eq!(n, 2);
        assert_eq!(dir, out.join("conn-2"));
        let (n, _) = claim_dir(&out, &mut next).unwrap();
        assert_eq!(n, 3);
        // A recording file that is there is refused, not truncated.
        assert!(record(out.join("conn-0").join("c2s.bin")).await.is_err());
        for n in [0, 1] {
            let earlier = std::fs::read(out.join(format!("conn-{n}")).join("c2s.bin")).unwrap();
            assert_eq!(earlier, b"earlier");
        }
        std::fs::remove_dir_all(&out).unwrap();
    }
}
