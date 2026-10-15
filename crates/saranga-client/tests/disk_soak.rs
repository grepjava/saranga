//! A soak on a small real file system, with no fault injected: the disk
//! fills for real. Ignored unless run by `scripts/disk-soak.sh`, which
//! mounts the file system and names it in `SARANGA_SOAK_DIR`; the soak
//! runs for `SARANGA_SOAK_SECONDS` (60 by default).
//!
//! Cycles alternate between two ways a disk fills:
//! - **Blocked:** with `max-disk-usage`, producers of small, paged and large
//!   messages are held back once the disk is past it, and a consumer then
//!   finds every message whose send was answered, once and in order.
//! - **Full:** without it, a filler file leaves a few megabytes free and
//!   the producers fill them: a store write fails with `ENOSPC`, which is a
//!   critical I/O error, the broker stops, and once the filler is gone a
//!   broker started on the same data has every message whose send was
//!   answered, once and in order.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use saranga_client::message;
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};

/// A send not answered in this long is held back by a full disk.
const BLOCKED: Duration = Duration::from_secs(3);
const WAIT: Duration = Duration::from_secs(30);

/// What each producer sends: its queue and the size of its messages.
const STREAMS: [(&str, usize); 3] = [
    ("orders", 1024),
    ("paged", 4 * 1024),
    ("orders", 150 * 1024),
];

fn config(dir: &Path, max_disk_usage: i32) -> BrokerConfig {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>1048576</journal-file-size>
             <journal-min-files>2</journal-min-files>
             <max-disk-usage>{max_disk_usage}</max-disk-usage>
             <disk-scan-period>10</disk-scan-period>
             <address-settings>
               <address-setting match="paged">
                 <max-size-bytes>524288</max-size-bytes>
                 <page-size-bytes>262144</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap()
}

async fn start(dir: &Path, max_disk_usage: i32) -> (Arc<Broker>, String) {
    let broker = Broker::start(config(dir, max_disk_usage)).await.unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let address = format!("127.0.0.1:{port}");
    broker
        .add_acceptor("artemis", &address, Acceptor::default())
        .await
        .unwrap();
    (broker, address)
}

/// What `saranga run` does to stop a broker.
async fn stop(broker: Arc<Broker>) {
    broker.pause_acceptors().await;
    broker.close_unestablished();
    for c in broker.registry().connections() {
        c.fail();
    }
    while !broker.registry().all_closed() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    broker.stop().unwrap();
    // Nothing keeps a stopped broker, or what holds its files open.
    let weak = Arc::downgrade(&broker);
    let post_office = Arc::downgrade(broker.post_office());
    let paging = Arc::downgrade(broker.post_office().paging());
    let storage = Arc::downgrade(broker.storage().unwrap());
    drop(broker);
    for _ in 0..500 {
        if weak.strong_count()
            + post_office.strong_count()
            + paging.strong_count()
            + storage.strong_count()
            == 0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "a stopped broker is still referenced: broker {}, post office {}, paging {}, storage {}",
        weak.strong_count(),
        post_office.strong_count(),
        paging.strong_count(),
        storage.strong_count()
    );
}

/// Files this process still has open that were deleted.
fn open_deleted() -> Vec<String> {
    let Ok(fds) = std::fs::read_dir("/proc/self/fd") else {
        return Vec::new();
    };
    fds.filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .map(|p| p.display().to_string())
        .filter(|p| p.ends_with("(deleted)"))
        .collect()
}

async fn session(address: &str, options: SessionOptions) -> (Connection, Session) {
    let conn = Connection::connect(address, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn.create_session(options).await.unwrap();
    (conn, session)
}

/// The numbers a producer's sends were answered for, those it cannot know
/// about (held back, or failed), and whether a send failed.
#[derive(Default)]
struct Sent {
    answered: Vec<i32>,
    unknown: Vec<i32>,
    failed: bool,
    /// The error a failed send was answered with.
    error: Option<String>,
}

/// Sends numbered messages of `size` bytes to `queue`, the numbers starting
/// at `from`, until one fails or is held back.
async fn produce(address: String, queue: &'static str, size: usize, from: i32) -> Sent {
    let (_conn, s) = session(&address, SessionOptions::default()).await;
    let producer = s.create_producer(Some(queue)).await.unwrap();
    let mut sent = Sent::default();
    for n in from.. {
        let mut m = message::bytes_message(&vec![(n % 251) as u8; size]);
        m.put_property("n", PropertyValue::Int(n));
        match tokio::time::timeout(BLOCKED, producer.send(m)).await {
            Ok(Ok(())) => sent.answered.push(n),
            Ok(Err(e)) => {
                sent.unknown.push(n);
                sent.failed = true;
                sent.error = Some(format!("{queue}, {size} bytes, message {n}: {e}"));
                break;
            }
            Err(_) => {
                sent.unknown.push(n);
                break;
            }
        }
    }
    sent
}

/// Runs every stream at once until each has stopped.
async fn produce_all(address: &str) -> Vec<Sent> {
    let tasks: Vec<_> = STREAMS
        .iter()
        .enumerate()
        .map(|(i, &(queue, size))| {
            tokio::spawn(produce(
                address.to_owned(),
                queue,
                size,
                i as i32 * 1_000_000,
            ))
        })
        .collect();
    let mut all = Vec::new();
    for t in tasks {
        all.push(t.await.unwrap());
    }
    all
}

/// Every message left on `queue`: its number, its body checked.
async fn drain(address: &str, queue: &str) -> Vec<i32> {
    let (_conn, s) = session(
        address,
        SessionOptions {
            ack_batch_size: 0,
            ..SessionOptions::default()
        },
    )
    .await;
    let consumer = s.create_consumer(queue, None, false).await.unwrap();
    s.start().await.unwrap();
    let mut got = Vec::new();
    while let Some(d) = consumer.receive(Duration::from_secs(2)).await.unwrap() {
        let n = match d.message.property("n") {
            Some(PropertyValue::Int(n)) => *n,
            other => panic!("no number: {other:?}"),
        };
        let stream = (n / 1_000_000) as usize;
        assert_eq!(d.message.body.len(), STREAMS[stream].1, "message {n}");
        assert!(
            d.message.body.iter().all(|b| *b == (n % 251) as u8),
            "message {n}'s body"
        );
        got.push(n);
        consumer.acknowledge(&d).await.unwrap();
    }
    got
}

/// Checks a queue against what was sent to it: every answered send, once,
/// in each stream's order, and nothing else but sends with no answer.
fn check(queue: &str, got: &[i32], sent: &[&Sent]) {
    let unique: BTreeSet<i32> = got.iter().copied().collect();
    assert_eq!(unique.len(), got.len(), "{queue}: duplicates");
    for s in sent {
        for n in &s.answered {
            assert!(unique.contains(n), "{queue}: message {n} was lost");
        }
    }
    for n in got {
        assert!(
            sent.iter()
                .any(|s| s.answered.contains(n) || s.unknown.contains(n)),
            "{queue}: message {n} was never sent"
        );
    }
    for stream in 0..STREAMS.len() as i32 {
        let ns: Vec<i32> = got
            .iter()
            .copied()
            .filter(|n| n / 1_000_000 == stream)
            .collect();
        assert!(ns.is_sorted(), "{queue}: stream {stream} out of order");
    }
}

fn check_all(address_got: &[(&str, Vec<i32>)], sent: &[Sent]) {
    for (queue, got) in address_got {
        let to: Vec<&Sent> = STREAMS
            .iter()
            .zip(sent)
            .filter(|((q, _), _)| q == queue)
            .map(|(_, s)| s)
            .collect();
        check(queue, got, &to);
    }
}

async fn create_queues(address: &str) {
    let (_conn, s) = session(address, SessionOptions::default()).await;
    for q in ["orders", "paged"] {
        s.create_queue(
            QueueConfiguration::new(q)
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await
        .unwrap();
    }
}

async fn drain_all(address: &str) -> Vec<(&'static str, Vec<i32>)> {
    let mut out = Vec::new();
    for q in ["orders", "paged"] {
        out.push((q, drain(address, q).await));
    }
    out
}

async fn blocked_cycle(dir: &Path) -> String {
    let (broker, address) = start(dir, 60).await;
    create_queues(&address).await;
    let sent = produce_all(&address).await;
    let errors: Vec<&String> = sent.iter().filter_map(|s| s.error.as_ref()).collect();
    assert!(
        errors.is_empty(),
        "a send failed where the disk monitor should have held it back: {errors:?}; critical: {}; disk full: {}; {}",
        broker.is_critical_io_failed(),
        broker.post_office().paging().is_disk_full(),
        usage(dir),
    );
    assert!(
        broker.post_office().paging().is_disk_full(),
        "the producers stopped with the disk not full"
    );
    assert!(!broker.is_critical_io_failed());
    let got = drain_all(&address).await;
    check_all(&got, &sent);
    stop(broker).await;
    let total: usize = got.iter().map(|(_, g)| g.len()).sum();
    format!("blocked at max-disk-usage, {total} messages")
}

/// The file system's usage, as `df` reports it.
fn usage(dir: &Path) -> String {
    std::process::Command::new("df")
        .arg("-k")
        .arg(dir)
        .output()
        .map(|o| {
            let out = String::from_utf8_lossy(&o.stdout).into_owned();
            out.lines().last().unwrap_or_default().to_owned()
        })
        .unwrap_or_default()
        + &format!(
            "; du: {}; open deleted: {:?}",
            std::process::Command::new("du")
                .arg("-sk")
                .arg(dir)
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
                .unwrap_or_default(),
            open_deleted()
                .iter()
                .map(|f| f.rsplit('/').next().unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        )
}

/// Fills the file system but for `leave` bytes.
fn fill(path: &Path, leave: u64) -> PathBuf {
    let filler = path.join("filler");
    let mut f = std::fs::File::create(&filler).unwrap();
    let chunk = vec![0u8; 1024 * 1024];
    let mut written = 0u64;
    while f.write_all(&chunk).is_ok() {
        written += chunk.len() as u64;
    }
    drop(f);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&filler)
        .unwrap();
    f.set_len(written.saturating_sub(leave)).unwrap();
    f.sync_all().unwrap();
    filler
}

async fn full_cycle(root: &Path, dir: &Path) -> String {
    let (broker, address) = start(dir, -1).await;
    create_queues(&address).await;
    let filler = fill(root, 8 * 1024 * 1024);
    let sent = produce_all(&address).await;
    assert!(
        sent.iter().any(|s| s.failed),
        "no send failed on a full disk"
    );
    tokio::time::timeout(WAIT, broker.shutdown_requested())
        .await
        .expect("a store failed but the broker did not stop");
    assert!(broker.is_critical_io_failed());
    stop(broker).await;
    std::fs::remove_file(filler).unwrap();

    let (broker, address) = start(dir, -1).await;
    let got = drain_all(&address).await;
    check_all(&got, &sent);
    stop(broker).await;
    let total: usize = got.iter().map(|(_, g)| g.len()).sum();
    format!("critical I/O error on a full disk, {total} messages after restart")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run by scripts/disk-soak.sh on a small file system"]
async fn disk_soak() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
    let root = PathBuf::from(std::env::var("SARANGA_SOAK_DIR").expect("SARANGA_SOAK_DIR"));
    let seconds: u64 = std::env::var("SARANGA_SOAK_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let until = Instant::now() + Duration::from_secs(seconds);
    let mut cycle = 0;
    while cycle < 2 || Instant::now() < until {
        cycle += 1;
        let dir = root.join(format!("cycle-{cycle}"));
        let started = Instant::now();
        let what = if cycle % 2 == 1 {
            blocked_cycle(&dir).await
        } else {
            full_cycle(&root, &dir).await
        };
        std::fs::remove_dir_all(&dir).unwrap();
        let open = open_deleted();
        assert!(open.is_empty(), "cycle {cycle} left files open: {open:?}");
        println!(
            "cycle {cycle}: {what}, {:.1}s; after: {}",
            started.elapsed().as_secs_f64(),
            usage(&root)
        );
    }
}
