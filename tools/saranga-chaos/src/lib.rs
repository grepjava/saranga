//! A TCP fault proxy for the chaos soaks.
//!
//! Each route listens on an address and forwards every connection it accepts
//! to a target. A route is in one mode at a time:
//!
//! - `heal`: bytes pass as they come.
//! - `delay MS [JITTER]`: each chunk is held MS milliseconds, give or take up
//!   to JITTER, and chunks keep their order.
//! - `blackhole`: nothing passes either way and nothing is closed, as in a
//!   partition. New connections are accepted and held, not connected. When
//!   the route heals what was held goes on, unless a peer gave up first.
//! - `refuse`: open connections are reset, and new ones are reset as soon as
//!   they are accepted.
//! - `drop P`: a new connection is reset with probability P, and each chunk
//!   resets the connection carrying it with probability P.
//!
//! `cut` resets every open connection of a route once and leaves its mode.
//! Commands come from the control port, a schedule or the random chaos, which
//! applies a fault to a route chosen at random, holds it and heals the route.
//! Every command applied is logged with the seconds since the start.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, sleep_until};

/// A route's mode.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    Heal,
    Delay { ms: u64, jitter: u64 },
    Blackhole,
    Refuse,
    Drop(f64),
}

/// The route or routes a command names.
#[derive(Clone, Debug, PartialEq)]
pub enum Target {
    All,
    Route(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Set(Target, Mode),
    Cut(Target),
    Random(bool),
    Status,
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(line: &str) -> Result<Command> {
        let words: Vec<&str> = line.split_whitespace().collect();
        let target = |i: usize| -> Result<Target> {
            match words.get(i) {
                None => bail!("{}: no route given", words[0]),
                Some(&"all") => Ok(Target::All),
                Some(name) => Ok(Target::Route(name.to_string())),
            }
        };
        let number = |i: usize| -> Result<u64> {
            let word = words
                .get(i)
                .ok_or_else(|| anyhow!("{}: a number is missing", words[0]))?;
            word.parse()
                .with_context(|| format!("{}: not a number: {word}", words[0]))
        };
        let (command, arity) = match words.first().copied() {
            None => bail!("no command"),
            Some("heal") => (Command::Set(target(1)?, Mode::Heal), 2..=2),
            Some("delay") => {
                let ms = number(2)?;
                let jitter = if words.len() > 3 { number(3)? } else { 0 };
                (Command::Set(target(1)?, Mode::Delay { ms, jitter }), 3..=4)
            }
            Some("blackhole") => (Command::Set(target(1)?, Mode::Blackhole), 2..=2),
            Some("refuse") => (Command::Set(target(1)?, Mode::Refuse), 2..=2),
            Some("drop") => {
                let word = words
                    .get(2)
                    .ok_or_else(|| anyhow!("drop: no probability given"))?;
                let p: f64 = word
                    .parse()
                    .with_context(|| format!("drop: not a probability: {word}"))?;
                if !(0.0..=1.0).contains(&p) {
                    bail!("drop: not a probability: {word}");
                }
                (Command::Set(target(1)?, Mode::Drop(p)), 3..=3)
            }
            Some("cut") => (Command::Cut(target(1)?), 2..=2),
            Some("random") => match words.get(1).copied() {
                Some("on") => (Command::Random(true), 2..=2),
                Some("off") => (Command::Random(false), 2..=2),
                _ => bail!("random: on or off"),
            },
            Some("status") => (Command::Status, 1..=1),
            Some(other) => bail!("unknown command: {other}"),
        };
        if !arity.contains(&words.len()) {
            bail!("{}: wrong number of arguments", words[0]);
        }
        Ok(command)
    }
}

/// A time such as `500ms`, `2s` or `1.5s`.
pub fn parse_duration(text: &str) -> Result<Duration> {
    let (number, scale) = if let Some(ms) = text.strip_suffix("ms") {
        (ms, 0.001)
    } else if let Some(s) = text.strip_suffix('s') {
        (s, 1.0)
    } else {
        bail!("not a time (500ms, 2s): {text}");
    };
    let value: f64 = number
        .parse()
        .with_context(|| format!("not a time: {text}"))?;
    if value < 0.0 {
        bail!("not a time: {text}");
    }
    Ok(Duration::from_secs_f64(value * scale))
}

/// A range of times such as `2s..6s`.
pub fn parse_range(text: &str) -> Result<(Duration, Duration)> {
    let (low, high) = text
        .split_once("..")
        .ok_or_else(|| anyhow!("not a range (2s..6s): {text}"))?;
    let (low, high) = (parse_duration(low)?, parse_duration(high)?);
    if low > high {
        bail!("not a range: {text}");
    }
    Ok((low, high))
}

/// A route as given on the command line: `name=listen,target`.
#[derive(Clone, Debug)]
pub struct RouteSpec {
    pub name: String,
    pub listen: String,
    pub target: String,
}

impl FromStr for RouteSpec {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<RouteSpec> {
        let (name, addresses) = text
            .split_once('=')
            .ok_or_else(|| anyhow!("not a route (name=listen,target): {text}"))?;
        let (listen, target) = addresses
            .split_once(',')
            .ok_or_else(|| anyhow!("not a route (name=listen,target): {text}"))?;
        if name.is_empty() || name == "all" {
            bail!("not a route name: {name:?}");
        }
        Ok(RouteSpec {
            name: name.into(),
            listen: listen.into(),
            target: target.into(),
        })
    }
}

/// The faults the random chaos chooses among.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fault {
    Cut,
    Delay,
    Blackhole,
    Refuse,
    Drop,
}

impl FromStr for Fault {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Fault> {
        Ok(match text {
            "cut" => Fault::Cut,
            "delay" => Fault::Delay,
            "blackhole" => Fault::Blackhole,
            "refuse" => Fault::Refuse,
            "drop" => Fault::Drop,
            _ => bail!("unknown fault: {text}"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct RandomSpec {
    pub faults: Vec<Fault>,
    /// The time between one fault and the next.
    pub every: (Duration, Duration),
    /// How long a fault other than a cut lasts.
    pub hold: (Duration, Duration),
    /// The longest delay.
    pub max_delay: u64,
    /// A drop's probability.
    pub drop: f64,
    /// The routes faults are applied to; all when empty.
    pub routes: Vec<String>,
}

#[derive(Clone, Copy)]
struct State {
    mode: Mode,
    /// Changed by a cut or a refuse: connections opened before close.
    epoch: u64,
}

pub struct Route {
    pub name: String,
    pub listen: SocketAddr,
    pub target: String,
    state: watch::Sender<State>,
    open: AtomicUsize,
}

pub struct Chaos {
    routes: Vec<Arc<Route>>,
    start: Instant,
    rng: Mutex<u64>,
    random: watch::Sender<bool>,
}

impl Chaos {
    /// Binds every route and starts forwarding.
    pub async fn start(specs: &[RouteSpec], seed: u64) -> Result<Arc<Chaos>> {
        let mut routes = Vec::new();
        let mut listeners = Vec::new();
        for spec in specs {
            if routes.iter().any(|r: &Arc<Route>| r.name == spec.name) {
                bail!("route {} given twice", spec.name);
            }
            let listener = TcpListener::bind(&spec.listen)
                .await
                .with_context(|| format!("route {}: bind {}", spec.name, spec.listen))?;
            routes.push(Arc::new(Route {
                name: spec.name.clone(),
                listen: listener.local_addr()?,
                target: spec.target.clone(),
                state: watch::channel(State {
                    mode: Mode::Heal,
                    epoch: 0,
                })
                .0,
                open: AtomicUsize::new(0),
            }));
            listeners.push(listener);
        }
        let chaos = Arc::new(Chaos {
            routes,
            start: Instant::now(),
            rng: Mutex::new(seed),
            random: watch::channel(false).0,
        });
        for (route, listener) in chaos.routes.iter().zip(listeners) {
            tokio::spawn(chaos.clone().accept(route.clone(), listener));
        }
        Ok(chaos)
    }

    pub fn routes(&self) -> &[Arc<Route>] {
        &self.routes
    }

    pub fn route(&self, name: &str) -> Option<&Arc<Route>> {
        self.routes.iter().find(|r| r.name == name)
    }

    fn log(&self, what: &str) {
        println!("{:9.3} {what}", self.start.elapsed().as_secs_f64());
    }

    /// The next number of a splitmix64 sequence.
    fn next(&self) -> u64 {
        let mut state = self.rng.lock().unwrap();
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number from 0 to n - 1.
    fn below(&self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    fn chance(&self, p: f64) -> bool {
        let unit = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
        p > 0.0 && unit < p
    }

    fn between(&self, (low, high): (Duration, Duration)) -> Duration {
        let span = (high - low).as_millis() as u64;
        low + Duration::from_millis(self.below(span + 1))
    }

    fn targets(&self, target: &Target) -> Result<Vec<&Arc<Route>>> {
        match target {
            Target::All => Ok(self.routes.iter().collect()),
            Target::Route(name) => Ok(vec![
                self.route(name)
                    .ok_or_else(|| anyhow!("no route named {name}"))?,
            ]),
        }
    }

    /// Applies a command, returning what to answer.
    pub fn apply(&self, command: &Command) -> Result<String> {
        match command {
            Command::Status => {
                let mut lines = String::new();
                for route in &self.routes {
                    let state = *route.state.borrow();
                    lines.push_str(&format!(
                        "{} {} -> {}: {}, {} open\n",
                        route.name,
                        route.listen,
                        route.target,
                        describe(state.mode),
                        route.open.load(Ordering::Relaxed)
                    ));
                }
                lines.push_str(&format!(
                    "random {}\n",
                    if *self.random.borrow() { "on" } else { "off" }
                ));
                Ok(lines)
            }
            Command::Random(on) => {
                self.random.send_replace(*on);
                self.log(&format!("random {}", if *on { "on" } else { "off" }));
                if !on {
                    self.apply(&Command::Set(Target::All, Mode::Heal))?;
                }
                Ok(String::new())
            }
            Command::Cut(target) => {
                for route in self.targets(target)? {
                    let open = route.open.load(Ordering::Relaxed);
                    route.state.send_modify(|s| s.epoch += 1);
                    self.log(&format!("{}: cut ({open} open)", route.name));
                }
                Ok(String::new())
            }
            Command::Set(target, mode) => {
                for route in self.targets(target)? {
                    let before = route.state.borrow().mode;
                    if before == *mode {
                        continue;
                    }
                    route.state.send_modify(|s| {
                        s.mode = *mode;
                        if *mode == Mode::Refuse {
                            s.epoch += 1;
                        }
                    });
                    self.log(&format!("{}: {}", route.name, describe(*mode)));
                }
                Ok(String::new())
            }
        }
    }

    /// Accepts commands on `addr`, a line each, answering each with what it
    /// returns and `ok`, or with `error: ...`.
    pub async fn control(self: Arc<Self>, listener: TcpListener) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                sleep(Duration::from_millis(100)).await;
                continue;
            };
            let chaos = self.clone();
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut lines = BufReader::new(read).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let reply = match line.parse::<Command>().and_then(|c| chaos.apply(&c)) {
                        Ok(text) => format!("{text}ok\n"),
                        Err(e) => format!("error: {e:#}\n"),
                    };
                    if write.write_all(reply.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    }

    /// Applies a schedule's commands, each at its time from the start: a
    /// line is `TIME COMMAND`, as `2s cut bridge`; `#` starts a comment.
    pub fn schedule(self: &Arc<Self>, text: &str) -> Result<()> {
        let mut entries = Vec::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap().trim();
            if line.is_empty() {
                continue;
            }
            let (time, command) = line
                .split_once(char::is_whitespace)
                .ok_or_else(|| anyhow!("line {}: TIME COMMAND", n + 1))?;
            let time = parse_duration(time).with_context(|| format!("line {}", n + 1))?;
            let command: Command = command.parse().with_context(|| format!("line {}", n + 1))?;
            if let Command::Set(target, _) | Command::Cut(target) = &command {
                self.targets(target)
                    .with_context(|| format!("line {}", n + 1))?;
            }
            entries.push((time, command));
        }
        let chaos = self.clone();
        tokio::spawn(async move {
            for (time, command) in entries {
                sleep_until(chaos.start + time).await;
                if let Err(e) = chaos.apply(&command) {
                    chaos.log(&format!("schedule: {e:#}"));
                }
            }
        });
        Ok(())
    }

    /// Starts the random chaos, on until `random off`.
    pub fn random(self: &Arc<Self>, spec: RandomSpec) -> Result<()> {
        if spec.faults.is_empty() {
            bail!("random: no faults given");
        }
        let routes: Vec<Arc<Route>> = if spec.routes.is_empty() {
            self.routes.clone()
        } else {
            spec.routes
                .iter()
                .map(|n| {
                    self.route(n)
                        .cloned()
                        .ok_or_else(|| anyhow!("no route named {n}"))
                })
                .collect::<Result<_>>()?
        };
        if routes.is_empty() {
            bail!("random: no routes");
        }
        self.random.send_replace(true);
        let chaos = self.clone();
        tokio::spawn(async move {
            let mut on = chaos.random.subscribe();
            loop {
                if on.wait_for(|on| *on).await.is_err() {
                    return;
                }
                tokio::select! {
                    _ = sleep(chaos.between(spec.every)) => {}
                    _ = on.wait_for(|on| !*on) => continue,
                }
                let route = &routes[chaos.below(routes.len() as u64) as usize];
                let target = Target::Route(route.name.clone());
                let fault = spec.faults[chaos.below(spec.faults.len() as u64) as usize];
                let mode = match fault {
                    Fault::Cut => {
                        let _ = chaos.apply(&Command::Cut(target));
                        continue;
                    }
                    Fault::Delay => {
                        let ms = 1 + chaos.below(spec.max_delay.max(1));
                        Mode::Delay { ms, jitter: ms / 2 }
                    }
                    Fault::Blackhole => Mode::Blackhole,
                    Fault::Refuse => Mode::Refuse,
                    Fault::Drop => Mode::Drop(spec.drop),
                };
                let _ = chaos.apply(&Command::Set(target.clone(), mode));
                tokio::select! {
                    _ = sleep(chaos.between(spec.hold)) => {
                        let _ = chaos.apply(&Command::Set(target, Mode::Heal));
                    }
                    // random off has healed every route.
                    _ = on.wait_for(|on| !*on) => {}
                }
            }
        });
        Ok(())
    }

    async fn accept(self: Arc<Self>, route: Arc<Route>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((client, _)) => {
                    let chaos = self.clone();
                    let route = route.clone();
                    tokio::spawn(async move { chaos.connection(&route, client).await });
                }
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }
    }

    async fn connection(&self, route: &Route, client: TcpStream) {
        let mut state = route.state.subscribe();
        let State { mode, epoch } = *state.borrow();
        match mode {
            Mode::Refuse => return reset(client),
            Mode::Drop(p) if self.chance(p) => return reset(client),
            _ => {}
        }
        // Held, not connected, while the route is a black hole.
        let open = state
            .wait_for(|s| s.epoch != epoch || s.mode != Mode::Blackhole)
            .await
            .map(|s| s.epoch == epoch)
            .unwrap_or(false);
        if !open {
            return reset(client);
        }
        let Ok(server) = TcpStream::connect(&route.target).await else {
            return reset(client);
        };
        let _ = client.set_nodelay(true);
        let _ = server.set_nodelay(true);
        route.open.fetch_add(1, Ordering::Relaxed);
        let (mut cr, mut cw) = client.into_split();
        let (mut sr, mut sw) = server.into_split();
        let failed = tokio::select! {
            r = async {
                tokio::try_join!(
                    self.pump(route, &mut cr, &mut sw, epoch),
                    self.pump(route, &mut sr, &mut cw, epoch),
                )
            } => r.is_err(),
            _ = state.wait_for(|s| s.epoch != epoch) => true,
        };
        route.open.fetch_sub(1, Ordering::Relaxed);
        if failed {
            if let Ok(client) = cr.reunite(cw) {
                reset(client);
            }
            if let Ok(server) = sr.reunite(sw) {
                reset(server);
            }
        }
    }

    /// Forwards one direction until its end of stream, which it passes on;
    /// fails when either side fails or a drop resets the connection.
    async fn pump(
        &self,
        route: &Route,
        from: &mut OwnedReadHalf,
        to: &mut OwnedWriteHalf,
        epoch: u64,
    ) -> Result<(), ()> {
        let (tx, mut rx) = mpsc::channel::<(Instant, Vec<u8>)>(64);
        let read = async move {
            let state = route.state.subscribe();
            let mut buf = vec![0u8; 64 * 1024];
            let mut last = Instant::now();
            loop {
                let n = match from.read(&mut buf).await {
                    Ok(0) => {
                        // The writer passes the end of stream on.
                        drop(tx);
                        return Ok(());
                    }
                    Ok(n) => n,
                    Err(_) => return Err(()),
                };
                let mode = state.borrow().mode;
                let mut at = Instant::now();
                match mode {
                    Mode::Drop(p) if self.chance(p) => {
                        self.log(&format!("{}: dropped a connection", route.name));
                        return Err(());
                    }
                    Mode::Delay { ms, jitter } => {
                        let spread = self.below(2 * jitter + 1) as i64 - jitter as i64;
                        at += Duration::from_millis((ms as i64 + spread).max(0) as u64);
                    }
                    _ => {}
                }
                at = at.max(last);
                last = at;
                if tx.send((at, buf[..n].to_vec())).await.is_err() {
                    return Err(());
                }
            }
        };
        let write = async {
            let mut state = route.state.subscribe();
            while let Some((at, bytes)) = rx.recv().await {
                sleep_until(at).await;
                let _ = state
                    .wait_for(|s| s.mode != Mode::Blackhole || s.epoch != epoch)
                    .await;
                to.write_all(&bytes).await.map_err(|_| ())?;
            }
            to.shutdown().await.map_err(|_| ())
        };
        tokio::try_join!(read, write).map(|_| ())
    }
}

fn describe(mode: Mode) -> String {
    match mode {
        Mode::Heal => "heal".into(),
        Mode::Delay { ms, jitter: 0 } => format!("delay {ms}ms"),
        Mode::Delay { ms, jitter } => format!("delay {ms}ms ±{jitter}ms"),
        Mode::Blackhole => "blackhole".into(),
        Mode::Refuse => "refuse".into(),
        Mode::Drop(p) => format!("drop {p}"),
    }
}

/// Closes a connection with a reset, as a failed network leaves it.
fn reset(stream: TcpStream) {
    let _ = stream.set_zero_linger();
    drop(stream);
}
