//! Recording TCP proxy for building the golden corpus.
//!
//! `saranga-capture <listen-addr> <upstream-addr> <out-dir>`
//!
//! Every accepted connection N is forwarded to the upstream broker, and the raw
//! bytes are written to `<out-dir>/conn-N/c2s.bin` (client to server) and
//! `<out-dir>/conn-N/s2c.bin` (server to client), unmodified.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static NEXT_CONN: AtomicUsize = AtomicUsize::new(0);

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
    loop {
        let (client, peer) = listener.accept().await?;
        let n = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
        let upstream = upstream.clone();
        let dir = out.join(format!("conn-{n}"));
        tokio::spawn(async move {
            if let Err(e) = proxy(client, &upstream, &dir).await {
                eprintln!("conn-{n} ({peer}): {e:#}");
            }
        });
    }
}

async fn proxy(client: TcpStream, upstream: &str, dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(dir).await?;
    let server = TcpStream::connect(upstream)
        .await
        .context("connect upstream")?;
    let (cr, cw) = client.into_split();
    let (sr, sw) = server.into_split();
    let c2s = File::create(dir.join("c2s.bin")).await?;
    let s2c = File::create(dir.join("s2c.bin")).await?;
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
