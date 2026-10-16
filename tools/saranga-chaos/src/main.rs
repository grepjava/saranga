//! The chaos proxy: see the library for what each mode does.
//!
//! ```text
//! saranga-chaos [OPTIONS] ROUTE...
//!   ROUTE                name=listen,target, as bridge=127.0.0.1:61700,127.0.0.1:61716
//!   --control ADDR       accept commands on ADDR, a line each
//!   --schedule FILE      apply FILE's commands, each at its time (TIME COMMAND)
//!   --random FAULTS      apply faults from the list at random: cut, delay,
//!                        blackhole, refuse, drop (as cut,delay,blackhole)
//!   --routes NAMES       the routes random faults go to (default all)
//!   --every MIN..MAX     the time between random faults (default 2s..6s)
//!   --hold MIN..MAX      how long a random fault lasts (default 1s..4s)
//!   --max-delay MS       the longest random delay (default 300)
//!   --drop P             a random drop's probability (default 0.02)
//!   --seed N             the random seed (default from the clock)
//!
//! saranga-chaos ctl ADDR COMMAND...
//!   sends one command to a running proxy and prints its answer; exits 1 on
//!   an error. Commands: heal ROUTE, delay ROUTE MS [JITTER], blackhole
//!   ROUTE, refuse ROUTE, drop ROUTE P, cut ROUTE, random on|off, status
//!   (ROUTE may be all).
//!
//! saranga-chaos check HISTORY
//!   checks a history of queue operations (see the history module) and
//!   prints what it counted and found; exits 1 unless it is valid.
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use saranga_chaos::{Chaos, RandomSpec, RouteSpec, history, parse_range};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("ctl") {
        return ctl(&args[1..]).await;
    }
    if args.first().map(String::as_str) == Some("check") {
        return check(&args[1..]);
    }
    let mut routes = Vec::new();
    let mut control = None;
    let mut schedule = None;
    let mut random = None;
    let mut spec = RandomSpec {
        faults: Vec::new(),
        every: (Duration::from_secs(2), Duration::from_secs(6)),
        hold: (Duration::from_secs(1), Duration::from_secs(4)),
        max_delay: 300,
        drop: 0.02,
        routes: Vec::new(),
    };
    let mut seed = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or_else(|| anyhow!("{arg}: no value given"));
        match arg.as_str() {
            "--control" => control = Some(value()?),
            "--schedule" => schedule = Some(value()?),
            "--random" => {
                random = Some(
                    value()?
                        .split(',')
                        .map(str::parse)
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            "--routes" => spec.routes = value()?.split(',').map(String::from).collect(),
            "--every" => spec.every = parse_range(&value()?)?,
            "--hold" => spec.hold = parse_range(&value()?)?,
            "--max-delay" => spec.max_delay = value()?.parse().context("--max-delay")?,
            "--drop" => spec.drop = value()?.parse().context("--drop")?,
            "--seed" => seed = value()?.parse().context("--seed")?,
            _ if arg.starts_with("--") => bail!("unknown option {arg}"),
            _ => routes.push(arg.parse::<RouteSpec>()?),
        }
    }
    if routes.is_empty() {
        bail!("usage: saranga-chaos [OPTIONS] name=listen,target...");
    }
    let chaos = Chaos::start(&routes, seed).await?;
    for route in chaos.routes() {
        println!("route {} {} -> {}", route.name, route.listen, route.target);
    }
    if let Some(file) = schedule {
        let text = std::fs::read_to_string(&file).with_context(|| format!("read {file}"))?;
        chaos.schedule(&text)?;
    }
    if let Some(faults) = random {
        spec.faults = faults;
        println!("seed {seed}");
        chaos.random(spec)?;
    }
    match control {
        Some(addr) => {
            let listener = TcpListener::bind(&addr)
                .await
                .with_context(|| format!("bind {addr}"))?;
            println!("control {}", listener.local_addr()?);
            chaos.control(listener).await;
        }
        None => std::future::pending::<()>().await,
    }
    Ok(())
}

fn check(args: &[String]) -> Result<()> {
    let [file] = args else {
        bail!("usage: saranga-chaos check HISTORY");
    };
    let text = std::fs::read_to_string(file).with_context(|| format!("read {file}"))?;
    let report = history::check(&history::parse(&text)?);
    println!("{report}");
    if !report.valid() {
        std::process::exit(1);
    }
    Ok(())
}

async fn ctl(args: &[String]) -> Result<()> {
    let [addr, command @ ..] = args else {
        bail!("usage: saranga-chaos ctl ADDR COMMAND...");
    };
    if command.is_empty() {
        bail!("usage: saranga-chaos ctl ADDR COMMAND...");
    }
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect {addr}"))?;
    stream
        .write_all(format!("{}\n", command.join(" ")).as_bytes())
        .await?;
    stream.shutdown().await?;
    let mut reply = String::new();
    stream.read_to_string(&mut reply).await?;
    print!("{reply}");
    if !reply.ends_with("ok\n") {
        std::process::exit(1);
    }
    Ok(())
}
