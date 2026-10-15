//! The proxy in front of an echo server: each mode, the control port, a
//! schedule and the random chaos.

use std::sync::Arc;
use std::time::Duration;

use saranga_chaos::{
    Chaos, Command, Fault, Mode, RandomSpec, RouteSpec, Target, parse_duration, parse_range,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, sleep, timeout};

async fn echo() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (mut r, mut w) = stream.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    addr
}

async fn proxy() -> Arc<Chaos> {
    let target = echo().await;
    let spec = RouteSpec {
        name: "r".into(),
        listen: "127.0.0.1:0".into(),
        target,
    };
    Chaos::start(&[spec], 7).await.unwrap()
}

async fn connect(chaos: &Chaos) -> TcpStream {
    TcpStream::connect(chaos.route("r").unwrap().listen)
        .await
        .unwrap()
}

fn set(chaos: &Chaos, mode: Mode) {
    chaos
        .apply(&Command::Set(Target::Route("r".into()), mode))
        .unwrap();
}

/// Sends `text` and reads it back, within `wait`.
async fn round_trip(
    stream: &mut TcpStream,
    text: &[u8],
    wait: Duration,
) -> std::io::Result<Vec<u8>> {
    stream.write_all(text).await?;
    let mut buf = vec![0; text.len()];
    match timeout(wait, stream.read_exact(&mut buf)).await {
        Ok(r) => r.map(|_| buf),
        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
    }
}

/// True once the peer has closed or reset the connection.
async fn closed(stream: &mut TcpStream) -> bool {
    let mut buf = [0; 16];
    matches!(
        timeout(Duration::from_secs(5), stream.read(&mut buf)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test]
async fn forwards_both_ways_and_passes_the_end_of_stream() {
    let chaos = proxy().await;
    let mut stream = connect(&chaos).await;
    let big: Vec<u8> = (0..300_000u32).map(|i| i as u8).collect();
    assert_eq!(
        round_trip(&mut stream, &big, Duration::from_secs(5))
            .await
            .unwrap(),
        big
    );
    stream.shutdown().await.unwrap();
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
}

#[tokio::test]
async fn delay_holds_each_chunk_in_order() {
    let chaos = proxy().await;
    let mut stream = connect(&chaos).await;
    set(
        &chaos,
        Mode::Delay {
            ms: 150,
            jitter: 50,
        },
    );
    let start = Instant::now();
    for i in 0..20u8 {
        stream.write_all(&[i]).await.unwrap();
    }
    let mut buf = [0; 20];
    stream.read_exact(&mut buf).await.unwrap();
    // Each way at least 100ms.
    assert!(
        start.elapsed() >= Duration::from_millis(200),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(buf.to_vec(), (0..20).collect::<Vec<u8>>());
    set(&chaos, Mode::Heal);
    let start = Instant::now();
    round_trip(&mut stream, b"x", Duration::from_secs(1))
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(100));
}

#[tokio::test]
async fn a_blackhole_holds_everything_until_it_heals() {
    let chaos = proxy().await;
    let mut open = connect(&chaos).await;
    round_trip(&mut open, b"before", Duration::from_secs(1))
        .await
        .unwrap();
    set(&chaos, Mode::Blackhole);
    assert!(
        round_trip(&mut open, b"held", Duration::from_millis(500))
            .await
            .is_err()
    );
    // A new connection is accepted and held.
    let mut new = connect(&chaos).await;
    assert!(
        round_trip(&mut new, b"new", Duration::from_millis(300))
            .await
            .is_err()
    );
    set(&chaos, Mode::Heal);
    let mut buf = [0; 4];
    timeout(Duration::from_secs(2), open.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"held");
    let mut buf = [0; 3];
    timeout(Duration::from_secs(2), new.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"new");
}

#[tokio::test]
async fn a_cut_resets_open_connections_only() {
    let chaos = proxy().await;
    let mut first = connect(&chaos).await;
    round_trip(&mut first, b"a", Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        chaos
            .apply(&Command::Status)
            .unwrap()
            .contains("heal, 1 open")
    );
    chaos.apply(&Command::Cut(Target::All)).unwrap();
    assert!(closed(&mut first).await);
    let mut second = connect(&chaos).await;
    round_trip(&mut second, b"b", Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_cut_resets_held_connections() {
    let chaos = proxy().await;
    set(&chaos, Mode::Blackhole);
    let mut held = connect(&chaos).await;
    sleep(Duration::from_millis(100)).await;
    chaos
        .apply(&Command::Cut(Target::Route("r".into())))
        .unwrap();
    assert!(closed(&mut held).await);
}

#[tokio::test]
async fn refuse_resets_open_and_new_connections_until_healed() {
    let chaos = proxy().await;
    let mut open = connect(&chaos).await;
    round_trip(&mut open, b"a", Duration::from_secs(1))
        .await
        .unwrap();
    set(&chaos, Mode::Refuse);
    assert!(closed(&mut open).await);
    let mut new = connect(&chaos).await;
    assert!(closed(&mut new).await);
    set(&chaos, Mode::Heal);
    let mut healed = connect(&chaos).await;
    round_trip(&mut healed, b"b", Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn drop_resets_connections_at_random() {
    let chaos = proxy().await;
    set(&chaos, Mode::Drop(1.0));
    let mut new = connect(&chaos).await;
    assert!(closed(&mut new).await);
    set(&chaos, Mode::Heal);
    let mut open = connect(&chaos).await;
    round_trip(&mut open, b"a", Duration::from_secs(1))
        .await
        .unwrap();
    set(&chaos, Mode::Drop(1.0));
    let _ = open.write_all(b"b").await;
    assert!(closed(&mut open).await);
    // With no chance, nothing is dropped.
    set(&chaos, Mode::Drop(0.0));
    let mut kept = connect(&chaos).await;
    for _ in 0..50 {
        round_trip(&mut kept, b"c", Duration::from_secs(1))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn the_control_port_applies_commands() {
    let chaos = proxy().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(chaos.clone().control(listener));
    let stream = TcpStream::connect(addr).await.unwrap();
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    w.write_all(b"delay r 20 5\nstatus\nnonsense\ncut nowhere\nheal all\n")
        .await
        .unwrap();
    let mut reply = Vec::new();
    for _ in 0..6 {
        reply.push(lines.next_line().await.unwrap().unwrap());
    }
    assert_eq!(reply[0], "ok");
    assert!(
        reply[1].starts_with("r 127.0.0.1:") && reply[1].ends_with(": delay 20ms ±5ms, 0 open"),
        "{}",
        reply[1]
    );
    assert_eq!(reply[2], "random off");
    assert_eq!(reply[3], "ok");
    assert_eq!(reply[4], "error: unknown command: nonsense");
    assert_eq!(reply[5], "error: no route named nowhere");
    assert_eq!(lines.next_line().await.unwrap().unwrap(), "ok");
    assert!(chaos.apply(&Command::Status).unwrap().contains(": heal, "));
}

#[tokio::test]
async fn a_schedule_applies_each_command_at_its_time() {
    let chaos = proxy().await;
    assert!(chaos.schedule("1s cut nowhere").is_err());
    assert!(chaos.schedule("soon heal r").is_err());
    chaos
        .schedule("# a partition\n200ms blackhole r\n600ms heal r  # healed\n")
        .unwrap();
    let mut stream = connect(&chaos).await;
    round_trip(&mut stream, b"a", Duration::from_secs(1))
        .await
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    assert!(
        round_trip(&mut stream, b"b", Duration::from_millis(150))
            .await
            .is_err()
    );
    sleep(Duration::from_millis(400)).await;
    let mut buf = [0; 1];
    timeout(Duration::from_secs(1), stream.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"b");
}

#[tokio::test]
async fn random_chaos_cuts_until_switched_off() {
    let chaos = proxy().await;
    let spec = RandomSpec {
        faults: vec![Fault::Cut],
        every: (Duration::from_millis(50), Duration::from_millis(100)),
        hold: (Duration::ZERO, Duration::ZERO),
        max_delay: 0,
        drop: 0.0,
        routes: vec!["r".into()],
    };
    chaos.random(spec).unwrap();
    let mut stream = connect(&chaos).await;
    assert!(closed(&mut stream).await);
    chaos.apply(&Command::Random(false)).unwrap();
    sleep(Duration::from_millis(150)).await;
    let mut kept = connect(&chaos).await;
    for _ in 0..10 {
        round_trip(&mut kept, b"a", Duration::from_secs(1))
            .await
            .unwrap();
        sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test]
async fn random_chaos_heals_each_fault_it_holds() {
    let chaos = proxy().await;
    let spec = RandomSpec {
        faults: vec![Fault::Blackhole, Fault::Refuse, Fault::Delay, Fault::Drop],
        every: (Duration::from_millis(10), Duration::from_millis(20)),
        hold: (Duration::from_millis(10), Duration::from_millis(30)),
        max_delay: 50,
        drop: 0.5,
        routes: Vec::new(),
    };
    chaos.random(spec).unwrap();
    sleep(Duration::from_millis(500)).await;
    chaos.apply(&Command::Random(false)).unwrap();
    sleep(Duration::from_millis(50)).await;
    let status = chaos.apply(&Command::Status).unwrap();
    assert!(
        status.contains(": heal, ") && status.contains("random off"),
        "{status}"
    );
    let mut stream = connect(&chaos).await;
    round_trip(&mut stream, b"a", Duration::from_secs(1))
        .await
        .unwrap();
}

#[test]
fn commands_times_and_routes_parse() {
    let route = |s: &str| Target::Route(s.into());
    assert_eq!(
        "heal all".parse::<Command>().unwrap(),
        Command::Set(Target::All, Mode::Heal)
    );
    assert_eq!(
        "delay a 100".parse::<Command>().unwrap(),
        Command::Set(route("a"), Mode::Delay { ms: 100, jitter: 0 })
    );
    assert_eq!(
        " delay  a 100 20 ".parse::<Command>().unwrap(),
        Command::Set(
            route("a"),
            Mode::Delay {
                ms: 100,
                jitter: 20
            }
        )
    );
    assert_eq!(
        "drop a 0.25".parse::<Command>().unwrap(),
        Command::Set(route("a"), Mode::Drop(0.25))
    );
    assert_eq!(
        "cut b".parse::<Command>().unwrap(),
        Command::Cut(route("b"))
    );
    assert_eq!(
        "random off".parse::<Command>().unwrap(),
        Command::Random(false)
    );
    for bad in [
        "",
        "heal",
        "heal a b",
        "delay a",
        "delay a x",
        "drop a 2",
        "random",
        "status x",
        "jump a",
    ] {
        assert!(bad.parse::<Command>().is_err(), "{bad:?}");
    }
    assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
    assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
    assert!(parse_duration("5").is_err());
    assert_eq!(
        parse_range("2s..6s").unwrap(),
        (Duration::from_secs(2), Duration::from_secs(6))
    );
    assert!(parse_range("6s..2s").is_err());
    let spec: RouteSpec = "b=127.0.0.1:1,localhost:2".parse().unwrap();
    assert_eq!(
        (
            spec.name.as_str(),
            spec.listen.as_str(),
            spec.target.as_str()
        ),
        ("b", "127.0.0.1:1", "localhost:2")
    );
    assert!("all=a,b".parse::<RouteSpec>().is_err());
    assert!("b=a".parse::<RouteSpec>().is_err());
}
