//! `check cluster` (`ClusterCheck`, `ClusterNodeVerifier`): every node of
//! the topology the broker gives is asked for its own, which must be the
//! same, and for its time, which must be within `--variance` of the
//! others'.

use std::time::Instant;

use saranga_server::management::json::Json;

use super::super::management::{SimpleFailure, simple_call, uncaught};
use super::ClusterArgs;
use crate::client::ConnectionArgs;

/// `TopologyItem`: a node, its primary's and backup's URLs.
#[derive(Debug, Clone)]
struct Item {
    node_id: String,
    primary: Option<String>,
    backup: Option<String>,
    primary_time: i64,
}

impl Item {
    /// `TopologyItem.equals`.
    fn same(&self, other: &Item) -> bool {
        self.node_id == other.node_id
            && self.primary == other.primary
            && self.backup == other.backup
    }
}

impl std::fmt::Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopologyItem{{nodeID='{}', primary='{}', backup='{}'}}",
            self.node_id,
            or_null(&self.primary),
            or_null(&self.backup)
        )
    }
}

fn or_null(s: &Option<String>) -> &str {
    s.as_deref().unwrap_or("null")
}

const STARS: usize = 127;

pub async fn run(mut args: ClusterArgs) -> anyhow::Result<()> {
    args.conn.resolve(false)?;
    // `createConnectionFactory().close()`.
    args.conn.try_connect().await?;
    // `verify`'s result is the command's: a failed verification is printed,
    // the command does not fail.
    verify(&args.conn, args.variance).await.map_err(uncaught)?;
    Ok(())
}

/// `getMessage` of what a management call threw.
fn message(e: &anyhow::Error) -> String {
    if let Some(f) = e.downcast_ref::<SimpleFailure>() {
        return f.to_string();
    }
    let text = format!("{e:#}");
    text.strip_prefix("Failed to create session factory; nested exception is ")
        .map(str::to_owned)
        .unwrap_or(text)
}

/// The first line `printStackTrace` prints: the exception and its message.
fn exception_line(e: &anyhow::Error) -> String {
    if e.downcast_ref::<SimpleFailure>().is_some() {
        format!("java.lang.Exception: {}", message(e))
    } else {
        message(e)
    }
}

/// The connection options with another URL (`new SimpleManagement(uri,
/// user, password)`).
fn at(conn: &ConnectionArgs, url: &str) -> ConnectionArgs {
    let mut c = conn.clone();
    c.url = Some(url.to_owned());
    c
}

async fn topology(conn: &ConnectionArgs) -> anyhow::Result<Vec<Json>> {
    let text = simple_call(conn, "broker", "listNetworkTopology", Vec::new()).await?;
    match Json::parse(&text) {
        Ok(Json::Array(a)) => Ok(a),
        _ => anyhow::bail!("jakarta.json.JsonException: not a JSON array: {text}"),
    }
}

/// `navigateTopology` into `parseTopology`'s map: by node ID, in the order
/// first seen, a later entry replacing an earlier one.
fn parse(topology: &[Json]) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    for node in topology {
        let url = |key: &str| match node.get(key) {
            Some(Json::String(s)) => Some(format!("tcp://{s}")),
            _ => None,
        };
        let item = Item {
            node_id: match node.get("nodeID") {
                Some(Json::String(s)) => s.clone(),
                _ => "null".to_owned(),
            },
            primary: url("primary"),
            backup: url("backup"),
            primary_time: 0,
        };
        match items.iter_mut().find(|i| i.node_id == item.node_id) {
            Some(slot) => *slot = item,
            None => items.push(item),
        }
    }
    items
}

/// `printTopology`.
fn print_topology(prefix: &str, topology: &[Json]) {
    println!(
        "{prefix}{:<40} | {:<25} | {:<25}",
        "nodeID", "live", "backup"
    );
    for item in parse_each(topology) {
        println!(
            "{prefix}{:<40} | {:<25} | {:<25}",
            item.node_id,
            or_null(&item.primary),
            or_null(&item.backup)
        );
    }
}

/// Each node of a topology, duplicates included.
fn parse_each(topology: &[Json]) -> Vec<Item> {
    topology
        .iter()
        .flat_map(|n| parse(std::slice::from_ref(n)))
        .collect()
}

/// `fetchTopologyTime`: each primary's time, 0 where it cannot be had.
async fn fetch_times(conn: &ConnectionArgs, items: &mut [Item]) -> Vec<i64> {
    let mut times = Vec::new();
    for item in items.iter_mut() {
        let Some(primary) = item.primary.clone() else {
            continue;
        };
        let at = at(conn, &primary);
        match simple_call(&at, "broker", "getCurrentTimeMillis", Vec::new()).await {
            Ok(t) => {
                item.primary_time = t.parse().unwrap_or(0);
                times.push(item.primary_time);
            }
            Err(e) => {
                eprintln!(
                    "Cannot fetch liveTime for nodeID={}, url={primary} -> {}",
                    item.node_id,
                    message(&e)
                );
                item.primary_time = 0;
            }
        }
    }
    times
}

/// `ClusterNodeVerifier.verify`.
async fn verify(conn: &ConnectionArgs, allowed_variance: i64) -> anyhow::Result<bool> {
    simple_call(conn, "broker", "getNodeID", Vec::new()).await?;
    let main_json = topology(conn).await?;
    let mut main = parse(&main_json);
    let mut result = true;
    // Fetched once to learn whether the times can be had, then again.
    fetch_times(conn, &mut main).await;

    // verifyTime
    println!("{}", "*".repeat(STARS));
    let times = fetch_times(conn, &mut main).await;
    println!(
        "{:<40} | {:<25} | {:<19} | {:<25}",
        "nodeID", "primary", "primary local time", "backup"
    );
    let initial = Instant::now();
    for item in &main {
        let date = if item.primary_time == 0 {
            String::new()
        } else {
            saranga_storage::java::simple_date_at_offset(
                item.primary_time,
                saranga_server::localtime::offset_millis(item.primary_time),
            )
        };
        println!(
            "{:<40} | {:<25} | {:<19} | {:<25}",
            item.node_id,
            or_null(&item.primary),
            date,
            or_null(&item.backup)
        );
    }
    // How long the times took is added to the allowed variance.
    let latency = initial.elapsed().as_millis() as i64;
    let variance = match (times.iter().min(), times.iter().max()) {
        (Some(min), Some(max)) => max - min,
        _ => 0,
    };
    let allowed = allowed_variance + latency;
    if variance < allowed {
        println!("Time variance in the cluster is {variance} milliseconds");
    } else {
        println!(
            "WARNING: Time variance in the cluster is greater than {allowed} milliseconds: {variance}. Please verify your server's NTP configuration."
        );
        result = false;
    }
    println!("{}", "*".repeat(STARS));

    for item in &main {
        println!(
            "--> Verifying Topology for NodeID {}, primary = {}, backup = {}",
            item.node_id,
            or_null(&item.primary),
            or_null(&item.backup)
        );
        let Some(primary) = &item.primary else {
            continue;
        };
        println!("   verification on primary {primary}");
        match sub_verify(conn, primary, &main).await {
            Ok(true) => println!("   ok!"),
            Ok(false) => result = false,
            Err(e) => {
                println!("{}", exception_line(&e));
                result = false;
            }
        }
    }
    Ok(result)
}

/// `subVerify` and `compareTopology`: the node's own topology, compared
/// with the main one.
async fn sub_verify(conn: &ConnectionArgs, uri: &str, main: &[Item]) -> anyhow::Result<bool> {
    let json = topology(&at(conn, uri)).await?;
    let other = parse(&json);
    let mismatch = if main.len() != other.len() {
        Some(format!(
            "main topology size {}!= compareTopology size {}",
            main.len(),
            other.len()
        ))
    } else {
        let mut found = None;
        for m in main {
            match other.iter().find(|o| o.node_id == m.node_id) {
                // `item.equals(...)` on a node the other topology lacks.
                None => anyhow::bail!(
                    "java.lang.NullPointerException: Cannot invoke \"org.apache.activemq.artemis.cli.commands.check.ClusterNodeVerifier$TopologyItem.equals(Object)\" because \"item\" is null"
                ),
                Some(o) if !o.same(m) => {
                    found = Some(format!("Topology mistmatch on {o}"));
                    break;
                }
                Some(_) => {}
            }
        }
        found
    };
    match mismatch {
        Some(text) => {
            println!("{text}");
            println!("    Topology detailing for {uri}");
            print_topology("    ", &json);
            Ok(false)
        }
        None => Ok(true),
    }
}
