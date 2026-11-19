//! A WebSocket connection's reader and writer share its stream (through
//! `tokio::io::split`): the writer is woken when a read flushes the frames
//! it waits on, and frames held back go out on the writer's flush.

use std::time::Duration;

use saranga_transport::websocket::{EncoderType, WebSocketStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf};

fn masked_binary(payload: &[u8]) -> Vec<u8> {
    let mut f = vec![0x82, 0x80 | payload.len() as u8];
    let mask = [1u8, 2, 3, 4];
    f.extend_from_slice(&mask);
    f.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    f
}

/// Three 8 KiB frames, each with a four-byte header.
const SENT: usize = 3 * (8192 + 4);

fn reader(mut rd: ReadHalf<WebSocketStream<DuplexStream>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut b = [0u8; 1024];
        while let Ok(n) = rd.read(&mut b).await {
            if n == 0 {
                break;
            }
        }
    })
}

async fn drain(client: &mut DuplexStream) -> usize {
    let mut got = 0;
    let mut b = vec![0u8; 4096];
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while let Ok(n) = client.read(&mut b).await {
            if n == 0 {
                break;
            }
            got += n;
        }
    })
    .await;
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_writer_is_woken_after_a_read_flushes_for_it() {
    let (mut client, server) = tokio::io::duplex(1024);
    let ws = WebSocketStream::new(server, Vec::new(), 65536, EncoderType::Binary);
    let (rd, mut wr) = tokio::io::split(ws);
    let reader = reader(rd);
    let writer = tokio::spawn(async move {
        for i in 0..3u8 {
            wr.write_all(&vec![i; 8192]).await.unwrap();
        }
        wr.flush().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The client sends a frame: the reader runs, and flushes.
    client.write_all(&masked_binary(b"x")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let got = drain(&mut client).await;
    let done = tokio::time::timeout(Duration::from_secs(2), writer).await;
    reader.abort();
    assert!(done.is_ok(), "the writer was never woken");
    assert_eq!(got, SENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_held_back_go_out_on_the_flush() {
    let (mut client, server) = tokio::io::duplex(1024);
    let ws = WebSocketStream::new(server, Vec::new(), 65536, EncoderType::Binary);
    let (rd, mut wr) = tokio::io::split(ws);
    let reader = reader(rd);
    let writer = tokio::spawn(async move {
        for i in 0..3u8 {
            wr.write_all(&vec![i; 8192]).await.unwrap();
        }
        wr.flush().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let got = drain(&mut client).await;
    let done = tokio::time::timeout(Duration::from_secs(2), writer).await;
    reader.abort();
    assert!(done.is_ok(), "the writer's flush never ended");
    assert_eq!(got, SENT);
}
