//! Load.java's work without a JVM: one producer and one consumer on one
//! queue over STOMP 1.2, MQTT 3.1.1 (QoS 0 or 1) or Core (saranga-client,
//! sending BYTES messages as the Artemis perf client's JMS session does),
//! 1 KiB bodies, for SECONDS of which the first WARMUP are not measured, at
//! RATE messages a second (0: unbounded), at most `-inflight` (default
//! 1000) sent and not yet received. It prints what Load.java prints, the
//! RESULT line bench.sh reads included, so either client can be given the
//! same run.
//!
//!   load [-inflight N] [-persistent] [-spin] stomp|mqtt0|mqtt1|core host port seconds warmup rate
//!
//! `-spin` has the consumer poll its socket rather than sleep in `read`, so
//! the client's own wakeup is not in the latency, and the producer spin for
//! the last 50 microseconds before a send is due.

mod core;

use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

/// A side of the run: the consumer or the producer, on its own thread.
type Side = Box<dyn FnOnce() -> io::Result<()> + Send>;

pub(crate) const SIZE: usize = 1024;
const MAX_LAT: usize = 1 << 22;

pub(crate) static SENT: AtomicU64 = AtomicU64::new(0);
static RECEIVED: AtomicU64 = AtomicU64::new(0);
static STOP: AtomicBool = AtomicBool::new(false);
static MEASURING: AtomicBool = AtomicBool::new(false);
static NLAT: AtomicUsize = AtomicUsize::new(0);
static EPOCH: OnceLock<Instant> = OnceLock::new();
static INFLIGHT: OnceLock<Semaphore> = OnceLock::new();
static LAT: OnceLock<Box<[AtomicU64]>> = OnceLock::new();

pub(crate) struct Opts {
    pub(crate) persistent: bool,
    pub(crate) spin: bool,
    pub(crate) inflight: usize,
}

fn now() -> u64 {
    EPOCH.get().unwrap().elapsed().as_nanos() as u64
}

pub(crate) fn stop() -> bool {
    STOP.load(Ordering::Relaxed)
}

/// A counting semaphore.
struct Semaphore {
    n: Mutex<usize>,
    cv: Condvar,
}

impl Semaphore {
    fn new(n: usize) -> Self {
        Self {
            n: Mutex::new(n),
            cv: Condvar::new(),
        }
    }
    fn try_acquire(&self) -> bool {
        let mut n = self.n.lock().unwrap();
        if *n > 0 {
            *n -= 1;
            true
        } else {
            false
        }
    }
    fn acquire(&self) {
        let mut n = self.n.lock().unwrap();
        while *n == 0 {
            n = self
                .cv
                .wait_timeout(n, Duration::from_millis(100))
                .unwrap()
                .0;
            if stop() {
                return;
            }
        }
        *n -= 1;
    }
    fn release(&self) {
        *self.n.lock().unwrap() += 1;
        self.cv.notify_one();
    }
}

pub(crate) fn stamp(b: &mut [u8]) {
    b[..8].copy_from_slice(&now().to_be_bytes());
}

pub(crate) fn record(b: &[u8]) {
    let t = u64::from_be_bytes(b[..8].try_into().unwrap());
    let d = now().saturating_sub(t);
    RECEIVED.fetch_add(1, Ordering::Relaxed);
    INFLIGHT.get().unwrap().release();
    if MEASURING.load(Ordering::Relaxed) {
        let i = NLAT.fetch_add(1, Ordering::Relaxed);
        if i < MAX_LAT {
            LAT.get().unwrap()[i].store(d, Ordering::Relaxed);
        }
    }
}

/// Paces to the rate, as Load.java's Pacer: more than 10 ms behind it, it
/// starts over from now rather than catching up in a burst.
pub(crate) struct Pacer {
    rate: u64,
    start: u64,
    count: u64,
    spin: bool,
}

impl Pacer {
    pub(crate) fn new(rate: u64, spin: bool) -> Self {
        Self {
            rate,
            start: now(),
            count: 0,
            spin,
        }
    }
    /// `flush` is called before waiting for room in the window.
    pub(crate) fn next(&mut self, flush: &mut dyn FnMut() -> io::Result<()>) -> io::Result<()> {
        let inflight = INFLIGHT.get().unwrap();
        if !inflight.try_acquire() {
            flush()?;
            inflight.acquire();
        }
        self.count += 1;
        if self.rate == 0 {
            return Ok(());
        }
        let due = self.start + self.count * 1_000_000_000 / self.rate;
        let t = now();
        if due > t {
            if self.spin {
                if due - t > 50_000 {
                    thread::sleep(Duration::from_nanos(due - t - 50_000));
                }
                while now() < due {
                    std::hint::spin_loop();
                }
            } else {
                thread::sleep(Duration::from_nanos(due - t));
            }
        } else if t - due > 10_000_000 {
            self.start = now();
            self.count = 0;
        }
        Ok(())
    }
}

fn connect(host: &str, port: u16) -> io::Result<TcpStream> {
    let s = TcpStream::connect((host, port))?;
    s.set_nodelay(true)?;
    Ok(s)
}

/// A socket reader that, with `spin`, polls rather than sleeps in `read`.
struct SpinRead {
    s: TcpStream,
    spin: bool,
}

impl Read for SpinRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.spin {
            return self.s.read(buf);
        }
        loop {
            match self.s.read(buf) {
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    if stop() {
                        return Err(ErrorKind::Interrupted.into());
                    }
                    std::hint::spin_loop();
                }
                r => return r,
            }
        }
    }
}

fn reader(s: &TcpStream, spin: bool) -> io::Result<BufReader<SpinRead>> {
    Ok(BufReader::with_capacity(
        65536,
        SpinRead {
            s: s.try_clone()?,
            spin,
        },
    ))
}

impl SpinRead {
    /// Once connected and subscribed: polling from then on.
    fn start_spinning(r: &mut BufReader<SpinRead>) -> io::Result<()> {
        if r.get_ref().spin {
            r.get_ref().s.set_nonblocking(true)?;
        }
        Ok(())
    }
}

/// A consumer only receives, and a broker closes a connection it has had
/// nothing from within its TTL, so it pings as a real client does.
fn keep_alive(out: &'static Mutex<TcpStream>, ping: &'static [u8], every: Duration) {
    thread::spawn(move || {
        while !stop() {
            thread::sleep(every);
            let _ = out.lock().unwrap().write_all(ping);
        }
    });
}

// ---- STOMP 1.2: receipts every 100 sends, at most inflight / 100 outstanding.

struct Frame {
    command: String,
    content_length: Option<usize>,
    headers: Vec<(String, String)>,
}

fn line(r: &mut impl BufRead) -> io::Result<String> {
    let mut v = Vec::new();
    if r.read_until(b'\n', &mut v)? == 0 {
        return Err(ErrorKind::UnexpectedEof.into());
    }
    v.pop();
    if v.last() == Some(&b'\r') {
        v.pop();
    }
    Ok(String::from_utf8_lossy(&v).into_owned())
}

fn read_frame(r: &mut impl BufRead, body: &mut Vec<u8>) -> io::Result<Frame> {
    let mut command = line(r)?;
    while command.is_empty() {
        command = line(r)?; // heart-beats
    }
    let mut f = Frame {
        command,
        content_length: None,
        headers: Vec::new(),
    };
    loop {
        let h = line(r)?;
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k == "content-length" && f.content_length.is_none() {
                f.content_length = v.parse().ok();
            }
            f.headers.push((k.into(), v.into()));
        }
    }
    body.clear();
    match f.content_length {
        Some(n) => {
            body.resize(n, 0);
            r.read_exact(body)?;
            let mut nul = [0];
            r.read_exact(&mut nul)?;
            if nul[0] != 0 {
                return Err(io::Error::other("no NUL"));
            }
        }
        None => {
            r.read_until(0, body)?;
            body.pop();
        }
    }
    if f.command == "ERROR" {
        return Err(io::Error::other(format!("STOMP ERROR {:?}", f.headers)));
    }
    Ok(f)
}

fn stomp_connect(s: &mut TcpStream, r: &mut impl BufRead) -> io::Result<()> {
    s.write_all(b"CONNECT\naccept-version:1.2\nhost:localhost\nlogin:admin\npasscode:admin\n\n\0")?;
    let f = read_frame(r, &mut Vec::new())?;
    if f.command != "CONNECTED" {
        return Err(io::Error::other(format!("STOMP: {}", f.command)));
    }
    Ok(())
}

fn stomp_consumer(host: &str, port: u16, o: &Opts) -> io::Result<()> {
    let mut s = connect(host, port)?;
    let mut r = reader(&s, o.spin)?;
    stomp_connect(&mut s, &mut r)?;
    s.write_all(
        b"SUBSCRIBE\nid:1\ndestination:BENCH.STOMP\nsubscription-type:ANYCAST\nack:auto\n\n\0",
    )?;
    let out: &'static Mutex<TcpStream> = Box::leak(Box::new(Mutex::new(s)));
    keep_alive(out, b"\n", Duration::from_secs(10));
    SpinRead::start_spinning(&mut r)?;
    let mut body = Vec::with_capacity(SIZE);
    while !stop() {
        let f = read_frame(&mut r, &mut body)?;
        if f.command == "MESSAGE" {
            record(&body);
        }
    }
    Ok(())
}

fn stomp_producer(host: &str, port: u16, rate: u64, o: &Opts) -> io::Result<()> {
    let mut s = connect(host, port)?;
    let mut r = reader(&s, false)?;
    stomp_connect(&mut s, &mut r)?;
    let window: &'static Semaphore = Box::leak(Box::new(Semaphore::new(o.inflight / 100)));
    thread::spawn(move || {
        let mut body = Vec::new();
        while !stop() {
            match read_frame(&mut r, &mut body) {
                Ok(f) if f.command == "RECEIPT" => window.release(),
                Ok(_) => {}
                Err(e) => fail(e),
            }
        }
    });
    let head = format!(
        "SEND\ndestination:BENCH.STOMP\ndestination-type:ANYCAST\n{}content-length:{SIZE}\n",
        if o.persistent {
            "persistent:true\n"
        } else {
            ""
        }
    );
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut body = vec![b'x'; SIZE];
    let mut pacer = Pacer::new(rate, o.spin);
    let mut i: u64 = 0;
    while !stop() {
        i += 1;
        let receipt = i.is_multiple_of(100);
        if receipt {
            window.acquire();
        }
        buf.extend_from_slice(head.as_bytes());
        if receipt {
            buf.extend_from_slice(format!("receipt:{i}\n").as_bytes());
        }
        buf.push(b'\n');
        stamp(&mut body);
        buf.extend_from_slice(&body);
        buf.push(0);
        if rate > 0 || receipt || buf.len() >= 60 * 1024 {
            s.write_all(&buf)?;
            buf.clear();
        }
        SENT.fetch_add(1, Ordering::Relaxed);
        pacer.next(&mut || {
            s.write_all(&buf)?;
            buf.clear();
            Ok(())
        })?;
    }
    Ok(())
}

// ---- MQTT 3.1.1: QoS 1 sends at most 100 unacknowledged.

fn put_len(b: &mut Vec<u8>, mut n: usize) {
    loop {
        let d = (n % 128) as u8;
        n /= 128;
        b.push(if n > 0 { d | 128 } else { d });
        if n == 0 {
            break;
        }
    }
}

fn put_str(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as u16).to_be_bytes());
    b.extend_from_slice(s.as_bytes());
}

fn packet(t: u8, rest: &[u8]) -> Vec<u8> {
    let mut b = vec![t];
    put_len(&mut b, rest.len());
    b.extend_from_slice(rest);
    b
}

/// A packet's type byte, its remaining bytes into `rest`.
fn read_packet(r: &mut impl Read, rest: &mut Vec<u8>) -> io::Result<u8> {
    let mut b = [0u8];
    r.read_exact(&mut b)?;
    let t = b[0];
    let (mut n, mut mul) = (0usize, 1usize);
    loop {
        r.read_exact(&mut b)?;
        n += (b[0] & 127) as usize * mul;
        mul *= 128;
        if b[0] & 128 == 0 {
            break;
        }
    }
    rest.resize(n, 0);
    r.read_exact(rest)?;
    Ok(t)
}

fn mqtt_connect(s: &mut TcpStream, r: &mut impl Read, id: &str) -> io::Result<()> {
    let mut b = Vec::new();
    put_str(&mut b, "MQTT");
    b.extend_from_slice(&[4, 0x80 | 0x40 | 0x02, 0, 60]);
    put_str(&mut b, id);
    put_str(&mut b, "admin");
    put_str(&mut b, "admin");
    s.write_all(&packet(0x10, &b))?;
    let mut rest = Vec::new();
    let t = read_packet(r, &mut rest)?;
    if t != 0x20 || rest.get(1) != Some(&0) {
        return Err(io::Error::other(format!("CONNACK {t} {rest:?}")));
    }
    Ok(())
}

fn mqtt_consumer(host: &str, port: u16, qos: u8, o: &Opts) -> io::Result<()> {
    let mut s = connect(host, port)?;
    let mut r = reader(&s, o.spin)?;
    mqtt_connect(&mut s, &mut r, "bench-consumer")?;
    let mut b = vec![0, 1];
    put_str(&mut b, "bench/mqtt");
    b.push(qos);
    s.write_all(&packet(0x82, &b))?;
    let out: &'static Mutex<TcpStream> = Box::leak(Box::new(Mutex::new(s)));
    keep_alive(out, &[0xc0, 0], Duration::from_secs(30));
    SpinRead::start_spinning(&mut r)?;
    let mut acks: Vec<u8> = Vec::with_capacity(4096);
    let mut rest = Vec::with_capacity(SIZE + 64);
    while !stop() {
        let t = read_packet(&mut r, &mut rest)?;
        if t & 0xf0 != 0x30 {
            continue;
        }
        let tlen = u16::from_be_bytes([rest[0], rest[1]]) as usize;
        let mut off = 2 + tlen;
        if (t >> 1) & 3 > 0 {
            acks.extend_from_slice(&[0x40, 2, rest[off], rest[off + 1]]);
            off += 2;
            // Acknowledgements go out once nothing more is buffered.
            if r.buffer().is_empty() {
                out.lock().unwrap().write_all(&acks)?;
                acks.clear();
            }
        }
        record(&rest[off..]);
    }
    Ok(())
}

fn mqtt_producer(host: &str, port: u16, qos: u8, rate: u64, o: &Opts) -> io::Result<()> {
    let mut s = connect(host, port)?;
    let mut r = reader(&s, false)?;
    mqtt_connect(&mut s, &mut r, "bench-producer")?;
    let window: &'static Semaphore = Box::leak(Box::new(Semaphore::new(100)));
    thread::spawn(move || {
        let mut rest = Vec::new();
        while !stop() {
            match read_packet(&mut r, &mut rest) {
                Ok(0x40) => window.release(),
                Ok(_) => {}
                Err(e) => fail(e),
            }
        }
    });
    let topic = b"bench/mqtt";
    let mut body = vec![b'x'; SIZE];
    let rest_len = 2 + topic.len() + if qos > 0 { 2 } else { 0 } + SIZE;
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut pacer = Pacer::new(rate, o.spin);
    let mut id: u16 = 0;
    let mut i: u64 = 0;
    while !stop() {
        i += 1;
        if qos > 0 {
            if !window.try_acquire() {
                s.write_all(&buf)?;
                buf.clear();
                window.acquire();
            }
            id = id % 65535 + 1;
        }
        buf.push(0x30 | (qos << 1));
        put_len(&mut buf, rest_len);
        buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic);
        if qos > 0 {
            buf.extend_from_slice(&id.to_be_bytes());
        }
        stamp(&mut body);
        buf.extend_from_slice(&body);
        if rate > 0 || i.is_multiple_of(100) || buf.len() >= 60 * 1024 {
            s.write_all(&buf)?;
            buf.clear();
        }
        SENT.fetch_add(1, Ordering::Relaxed);
        pacer.next(&mut || {
            s.write_all(&buf)?;
            buf.clear();
            Ok(())
        })?;
    }
    Ok(())
}

fn fail(e: io::Error) -> ! {
    if stop() {
        // Sockets close under the threads at the end.
        thread::park();
        std::process::exit(0);
    }
    eprintln!("{e}");
    std::process::exit(2);
}

fn main() {
    let mut o = Opts {
        persistent: false,
        spin: false,
        inflight: 1000,
    };
    let mut args: Vec<String> = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "-persistent" => o.persistent = true,
            "-spin" => o.spin = true,
            "-inflight" => {
                o.inflight = it.next().and_then(|n| n.parse().ok()).expect("-inflight N")
            }
            _ => args.push(a),
        }
    }
    if args.len() != 6 {
        eprintln!(
            "load [-inflight N] [-persistent] [-spin] stomp|mqtt0|mqtt1|core host port seconds warmup rate"
        );
        std::process::exit(2);
    }
    let proto = args[0].clone();
    let host = args[1].clone();
    let port: u16 = args[2].parse().expect("port");
    let seconds: u64 = args[3].parse().expect("seconds");
    let warmup: u64 = args[4].parse().expect("warmup");
    let rate: u64 = args[5].parse().expect("rate");
    EPOCH.set(Instant::now()).unwrap();
    INFLIGHT.set(Semaphore::new(o.inflight)).ok().unwrap();
    LAT.set((0..MAX_LAT).map(|_| AtomicU64::new(0)).collect())
        .ok()
        .unwrap();
    let o: &'static Opts = Box::leak(Box::new(o));
    let (consumer, producer): (Side, Side) = match proto.as_str() {
        "stomp" => {
            let (h1, h2) = (host.clone(), host.clone());
            (
                Box::new(move || stomp_consumer(&h1, port, o)),
                Box::new(move || stomp_producer(&h2, port, rate, o)),
            )
        }
        "mqtt0" | "mqtt1" => {
            let qos = if proto == "mqtt1" { 1 } else { 0 };
            let (h1, h2) = (host.clone(), host.clone());
            (
                Box::new(move || mqtt_consumer(&h1, port, qos, o)),
                Box::new(move || mqtt_producer(&h2, port, qos, rate, o)),
            )
        }
        "core" => {
            let (h1, h2) = (host.clone(), host.clone());
            (
                Box::new(move || core::consumer(&h1, port)),
                Box::new(move || core::producer(&h2, port, rate, o)),
            )
        }
        _ => {
            eprintln!("unknown protocol {proto}");
            std::process::exit(2);
        }
    };
    thread::spawn(move || consumer().unwrap_or_else(|e| fail(e)));
    thread::sleep(Duration::from_millis(500));
    thread::spawn(move || producer().unwrap_or_else(|e| fail(e)));
    let (mut s0, mut r0, mut sum_s, mut sum_r, mut window0, mut n) = (0, 0, 0, 0, 0, 0);
    let t0 = Instant::now();
    for i in 1..=seconds {
        // Ticks on the second from the start, not drifting with the work.
        let due = t0 + Duration::from_secs(i);
        thread::sleep(due.saturating_duration_since(Instant::now()));
        let (s, r) = (
            SENT.load(Ordering::Relaxed),
            RECEIVED.load(Ordering::Relaxed),
        );
        if i == warmup {
            MEASURING.store(true, Ordering::Relaxed);
        }
        if i == warmup + 3 {
            window0 = r;
        }
        if i > warmup {
            sum_s += s - s0;
            sum_r += r - r0;
            n += 1;
        }
        s0 = s;
        r0 = r;
    }
    STOP.store(true, Ordering::Relaxed);
    let k = NLAT.load(Ordering::Relaxed).min(MAX_LAT);
    let mut l: Vec<u64> = LAT.get().unwrap()[..k]
        .iter()
        .map(|x| x.load(Ordering::Relaxed))
        .collect();
    l.sort_unstable();
    let recv = sum_r.checked_div(n).unwrap_or(0);
    let (p50, p99) = if k > 0 {
        (l[k / 2] / 1000, l[(k as f64 * 0.99) as usize] / 1000)
    } else {
        (0, 0)
    };
    eprintln!(
        "totals: sent {} received {}",
        SENT.load(Ordering::Relaxed),
        RECEIVED.load(Ordering::Relaxed)
    );
    println!(
        "{proto}: sent {} msg/s, received {recv} msg/s, p50 {p50} us, p99 {p99} us",
        sum_s.checked_div(n).unwrap_or(0)
    );
    println!(
        "RESULT {} {recv} {p50} {p99} {}",
        if recv > 0 { "success" } else { "fail" },
        RECEIVED.load(Ordering::Relaxed) - window0
    );
    std::process::exit(0);
}
