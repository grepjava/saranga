//! Fills a queue with a large backlog, or drains and checks one: the
//! multi-GB paging soak (`scripts/paging-soak.sh`).
//!
//! ```text
//! page_soak <host:port> <queue> send <count> <size>
//! page_soak <host:port> <queue> receive <count> <size>
//! ```
//!
//! Messages are durable BYTES messages of `size` bytes, sent in transactions
//! of 100. Message `i` carries the long property `i` and a body derived from
//! `i`, so `receive` checks that every message arrives once, in order, intact.

use std::time::{Duration, Instant};

use saranga_client::message::bytes_message;
use saranga_client::*;

const BATCH: u64 = 100;

fn body(i: u64, size: usize) -> Vec<u8> {
    let seed = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..size)
        .map(|j| (seed >> ((j % 8) * 8)) as u8 ^ (j / 8) as u8)
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        eprintln!("usage: page_soak <host:port> <queue> send|receive <count> <size>");
        std::process::exit(2);
    }
    let (addr, queue, mode) = (&args[1], &args[2], args[3].as_str());
    let count: u64 = args[4].parse().expect("count");
    let size: usize = args[5].parse().expect("size");

    let conn = Connection::connect(addr, ConnectionOptions::default()).await?;
    let session = conn.create_session(SessionOptions::transacted()).await?;
    let start = Instant::now();
    let mut last_report = Instant::now();
    let report = |i: u64, last: &mut Instant| {
        if last.elapsed() >= Duration::from_secs(10) {
            *last = Instant::now();
            let secs = start.elapsed().as_secs_f64();
            println!(
                "{mode}: {i}/{count} ({:.0} MiB, {:.1} MiB/s)",
                (i as f64 * size as f64) / (1 << 20) as f64,
                (i as f64 * size as f64) / (1 << 20) as f64 / secs
            );
        }
    };

    match mode {
        "send" => {
            match session
                .create_queue(
                    QueueConfiguration::new(queue.as_str()).routing_type(RoutingType::Anycast),
                )
                .await
            {
                Err(e) if e.kind() == Some(ErrorType::QueueExists) => {}
                r => r?,
            }
            let producer = session.create_producer(Some(queue)).await?;
            for i in 0..count {
                let mut m = bytes_message(&body(i, size));
                m.put_property("i", PropertyValue::Long(i as i64));
                producer.send(m).await?;
                if (i + 1) % BATCH == 0 || i + 1 == count {
                    session.commit().await?;
                }
                report(i + 1, &mut last_report);
            }
        }
        "receive" => {
            let consumer = session.create_consumer(queue, None, false).await?;
            session.start().await?;
            for i in 0..count {
                let Some(d) = consumer.receive(Duration::from_secs(60)).await? else {
                    eprintln!("FAIL: message {i} did not arrive");
                    std::process::exit(1);
                };
                let got = d.message.property("i");
                if got != Some(&PropertyValue::Long(i as i64)) {
                    eprintln!("FAIL: expected message {i}, got {got:?}");
                    std::process::exit(1);
                }
                if d.message.body != body(i, size) {
                    eprintln!("FAIL: message {i} has a different body");
                    std::process::exit(1);
                }
                consumer.acknowledge(&d).await?;
                if (i + 1) % BATCH == 0 || i + 1 == count {
                    session.commit().await?;
                }
                report(i + 1, &mut last_report);
            }
            if let Some(d) = consumer.receive(Duration::from_secs(2)).await? {
                eprintln!("FAIL: an extra message {:?}", d.message.property("i"));
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("mode must be send or receive");
            std::process::exit(2);
        }
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{mode}: {count} messages, {:.0} MiB in {secs:.0} s",
        (count as f64 * size as f64) / (1 << 20) as f64
    );
    conn.close().await;
    Ok(())
}
