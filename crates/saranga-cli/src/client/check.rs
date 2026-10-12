//! `check node|queue` (`CheckGroup`): checks run against the broker, each
//! reported as `Checking that <assertion> ... success`, then a summary.

use std::time::{Duration, Instant};

use anyhow::bail;
use saranga_server::management::json::Json;

use super::jms::{Destination, JmsConnection};
use super::management::Requestor;
use super::{CliError, ConnectionArgs};

#[derive(clap::Subcommand, Debug)]
pub enum CheckCommand {
    /// Check a node.
    Node(NodeArgs),
    /// Check a queue.
    Queue(QueueArgs),
}

/// `CheckAbstract`'s options.
#[derive(clap::Args, Debug)]
pub struct CheckArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Name of the target to check.
    #[arg(long)]
    name: Option<String>,
    /// Time to wait for the check to complete (in milliseconds).
    #[arg(long, default_value_t = 30000)]
    timeout: u64,
    /// Continue with the rest of the checks even if a particular module
    /// check fails.
    #[arg(long = "fail-at-end")]
    fail_at_end: bool,
}

#[derive(clap::Args, Debug)]
pub struct NodeArgs {
    #[command(flatten)]
    check: CheckArgs,
    /// Check that the node is started. This check is executed by default if
    /// there are no other checks.
    #[arg(long)]
    up: bool,
    /// Disk usage percentage to check or -1 to use the max-disk-usage.
    #[arg(long = "diskUsage", allow_negative_numbers = true)]
    disk_usage: Option<i32>,
    /// Memory usage percentage to check.
    #[arg(long = "memoryUsage", allow_negative_numbers = true)]
    memory_usage: Option<i32>,
    /// Check that the node has a connected live.
    #[arg(long)]
    live: bool,
    /// Check that the node has a connected live.
    #[arg(long)]
    primary: bool,
    /// Check that the node has a connected backup.
    #[arg(long)]
    backup: bool,
    /// Number of peers to check.
    #[arg(long, allow_negative_numbers = true)]
    peers: Option<i32>,
}

#[derive(clap::Args, Debug)]
pub struct QueueArgs {
    #[command(flatten)]
    check: CheckArgs,
    /// Check that the queue exists and is not paused. This check is
    /// executed by default if there are no other checks.
    #[arg(long)]
    up: bool,
    /// Number of the messages to browse or -1 to browse at most 1 message
    /// only if it is immediately available (succeeding also if no messages
    /// are available).
    #[arg(long, allow_negative_numbers = true)]
    browse: Option<i32>,
    /// Number of the messages to consume or -1 to consume at most 1 message
    /// only if it is immediately available (succeeding also if no messages
    /// are available).
    #[arg(long, allow_negative_numbers = true)]
    consume: Option<i32>,
    /// Number of the messages to produce.
    #[arg(long, allow_negative_numbers = true)]
    produce: Option<i32>,
}

/// A check's outcome: `failure` (`CheckException`) or `error`.
enum Outcome {
    Failure(String),
    Error(String),
}

type CheckResult = Result<(), Outcome>;

fn failure(m: impl Into<String>) -> Outcome {
    Outcome::Failure(m.into())
}

impl From<anyhow::Error> for Outcome {
    fn from(e: anyhow::Error) -> Self {
        Outcome::Error(format!("{e:#}"))
    }
}

/// `CheckContext`: the connection, the management requestor and what was
/// learned about the node.
struct Context {
    conn: ConnectionArgs,
    requestor: Requestor,
    node_id: Option<String>,
    topology: Option<Vec<Json>>,
}

impl Context {
    async fn attribute(&self, resource: &str, attribute: &str) -> anyhow::Result<Json> {
        let reply = self.requestor.attribute(resource, attribute).await?;
        if !reply.succeeded {
            bail!(
                "Failed to get {resource}.{attribute}. Reason: {}",
                reply.string()
            );
        }
        Ok(reply.result.unwrap_or(Json::Null))
    }

    async fn node_id(&mut self) -> anyhow::Result<String> {
        if self.node_id.is_none() {
            let id = self.attribute("broker", "NodeID").await?;
            self.node_id = Some(id.as_str().unwrap_or("null").to_owned());
        }
        Ok(self.node_id.clone().unwrap_or_default())
    }

    /// `listNetworkTopology`, invoked with no parameters at all.
    async fn topology(&mut self) -> anyhow::Result<Vec<Json>> {
        if self.topology.is_none() {
            let reply = self
                .requestor
                .invoke_raw("broker", "listNetworkTopology", None)
                .await?;
            if !reply.succeeded {
                bail!(
                    "Failed to invoke broker.listNetworkTopology. Reason: {}",
                    reply.string()
                );
            }
            let nodes = match Json::parse(&reply.string()) {
                Ok(Json::Array(nodes)) => nodes,
                _ => Vec::new(),
            };
            self.topology = Some(nodes);
        }
        Ok(self.topology.clone().unwrap_or_default())
    }

    async fn node(&mut self, id: &str) -> anyhow::Result<Option<Json>> {
        Ok(self
            .topology()
            .await?
            .into_iter()
            .find(|n| n.get("nodeID").and_then(Json::as_str) == Some(id)))
    }
}

/// A check: its assertion and what it runs.
enum Task {
    NodeUp,
    NodePrimary,
    NodeBackup,
    NodePeers(i32),
    DiskUsage(i32),
    MemoryUsage(i32),
    QueueUp,
    Produce(i32),
    Browse(i32),
    Consume(i32),
}

pub async fn run(cmd: CheckCommand) -> anyhow::Result<()> {
    let (check, kind, tasks) = match cmd {
        CheckCommand::Node(mut args) => {
            args.check.conn.resolve(false)?;
            let tasks = node_tasks(&args)?;
            (args.check, "NodeCheck", tasks)
        }
        CheckCommand::Queue(mut args) => {
            args.check.conn.resolve(false)?;
            if args.check.name.is_none() {
                args.check.name = args.check.conn.common.input(
                    "--name",
                    "Name is a mandatory property for Queue check",
                    None,
                );
            }
            let tasks = queue_tasks(&args).map_err(|e| check_failed("QueueCheck", &e))?;
            (args.check, "QueueCheck", tasks)
        }
    };
    let timeout = Duration::from_millis(check.timeout);
    match tokio::time::timeout(timeout, run_tasks(&check, kind, &tasks)).await {
        Ok(r) => r,
        Err(_) => bail!(CliError(format!("{kind} failed. Reason: timeout"))),
    }
}

/// An argument error, which Artemis meets in the check's thread and
/// reports as the failed check's cause.
fn check_failed(kind: &str, e: &anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(CliError(format!(
        "{kind} failed. Reason: java.lang.IllegalArgumentException: {e}"
    )))
}

fn node_tasks(args: &NodeArgs) -> anyhow::Result<Vec<(String, Task)>> {
    let invalid = |m: String| check_failed("NodeCheck", &anyhow::anyhow!(m));
    let mut tasks = Vec::new();
    if args.primary || args.live {
        tasks.push(("the node has a primary".to_owned(), Task::NodePrimary));
    }
    if args.backup {
        tasks.push(("the node has a backup".to_owned(), Task::NodeBackup));
    }
    if let Some(peers) = args.peers {
        if peers > 0 {
            tasks.push((format!("there are {peers} peers"), Task::NodePeers(peers)));
        } else {
            return Err(invalid(format!("Invalid peers number to check: {peers}")));
        }
    }
    if let Some(disk) = args.disk_usage {
        if disk == -1 {
            tasks.push((
                "the disk usage is less then the max-disk-usage".to_owned(),
                Task::DiskUsage(-1),
            ));
        } else if disk > 0 && disk < 100 {
            tasks.push((
                format!("the disk usage is less then {disk}"),
                Task::DiskUsage(disk),
            ));
        } else {
            return Err(invalid(format!("Invalid disk usage percentage: {disk}")));
        }
    }
    if let Some(memory) = args.memory_usage {
        if memory > 0 && memory < 100 {
            tasks.push((
                format!("the memory usage is less then {memory}"),
                Task::MemoryUsage(memory),
            ));
        } else {
            return Err(invalid(format!(
                "Invalid memory usage percentage: {memory}"
            )));
        }
    }
    if args.up || tasks.is_empty() {
        tasks.insert(0, ("the node is started".to_owned(), Task::NodeUp));
    }
    Ok(tasks)
}

fn queue_tasks(args: &QueueArgs) -> anyhow::Result<Vec<(String, Task)>> {
    let Some(name) = args.check.name.as_deref() else {
        bail!("The name of the queue is required");
    };
    let mut tasks = Vec::new();
    if let Some(n) = args.produce {
        if n > 0 {
            tasks.push((
                format!("a producer can send {n} messages to the queue {name}"),
                Task::Produce(n),
            ));
        } else {
            bail!("Invalid  number of messages to produce: {n}");
        }
    }
    if let Some(n) = args.browse {
        if n == -1 {
            tasks.push((
                format!("a consumer can browse the queue {name}"),
                Task::Browse(n),
            ));
        } else if n > 0 {
            tasks.push((
                format!("a consumer can browse {n} messages from the queue {name}"),
                Task::Browse(n),
            ));
        } else {
            bail!("Invalid  number of messages to browse: {n}");
        }
    }
    if let Some(n) = args.consume {
        if n == -1 {
            tasks.push((
                format!("a consumer can consume the queue {name}"),
                Task::Consume(n),
            ));
        } else if n > 0 {
            tasks.push((
                format!("a consumer can consume {n} messages from the queue {name}"),
                Task::Consume(n),
            ));
        } else {
            bail!("Invalid  number of messages to consume: {n}");
        }
    }
    if args.up || tasks.is_empty() {
        tasks.insert(
            0,
            (
                format!("the queue {name} exists and is not paused"),
                Task::QueueUp,
            ),
        );
    }
    Ok(tasks)
}

async fn run_tasks(check: &CheckArgs, kind: &str, tasks: &[(String, Task)]) -> anyhow::Result<()> {
    let conn = check.conn.connect().await?;
    let requestor = Requestor::open(&conn).await?;
    let mut context = Context {
        conn: check.conn.clone(),
        requestor,
        node_id: None,
        topology: None,
    };
    println!("Running {kind}");
    let started = Instant::now();
    let (mut success, mut failed, mut errors) = (0, 0, 0);
    let mut stop = None;
    for (assertion, task) in tasks {
        print!("Checking that {assertion} ... ");
        let outcome = run_task(&mut context, check.name.as_deref().unwrap_or(""), task).await;
        let reason = match outcome {
            Ok(()) => {
                success += 1;
                println!("success");
                continue;
            }
            Err(Outcome::Failure(m)) => {
                failed += 1;
                format!("failure: {m}")
            }
            Err(Outcome::Error(m)) => {
                errors += 1;
                format!("error: {m}")
            }
        };
        println!("{reason}");
        if !check.fail_at_end {
            stop = Some(reason);
            break;
        }
    }
    let skipped = tasks.len() - failed - errors - success;
    println!(
        "Checks run: {}, Failures: {failed}, Errors: {errors}, Skipped: {skipped}, Time elapsed: {:.3} sec - {kind}",
        tasks.len(),
        started.elapsed().as_millis() as f32 / 1000.0
    );
    context.requestor.close().await;
    conn.close().await;
    if let Some(reason) = stop {
        bail!(CliError(format!("{kind} failed. Reason: {reason}")));
    }
    if success < tasks.len() {
        bail!(CliError(format!(
            "{kind} failed. Reason: checks not successful"
        )));
    }
    Ok(())
}

async fn run_task(ctx: &mut Context, name: &str, task: &Task) -> CheckResult {
    match task {
        Task::NodeUp => {
            if ctx.attribute("broker", "Started").await?.as_bool() != Some(true) {
                return Err(failure("The node isn't started."));
            }
        }
        Task::NodePrimary | Task::NodeBackup => {
            let id = if name.is_empty() {
                ctx.node_id().await?
            } else {
                name.to_owned()
            };
            let key = if matches!(task, Task::NodePrimary) {
                "primary"
            } else {
                "backup"
            };
            let found = ctx.node(&id).await?;
            if found.as_ref().and_then(|n| n.get(key)).is_none() {
                let what = if key == "primary" {
                    "primary"
                } else {
                    "backup"
                };
                return Err(failure(format!("No {what} found for the node {id}")));
            }
        }
        Task::NodePeers(peers) => {
            let count: i32 = ctx
                .topology()
                .await?
                .iter()
                .map(|n| {
                    i32::from(n.get("primary").is_some()) + i32::from(n.get("backup").is_some())
                })
                .sum();
            if count < *peers {
                return Err(failure(format!("Insufficient peers: {peers}")));
            }
        }
        Task::DiskUsage(threshold) => {
            let max = if *threshold == -1 {
                ctx.attribute("broker", "MaxDiskUsage")
                    .await?
                    .as_i64()
                    .unwrap_or(0) as i32
            } else {
                *threshold
            };
            let usage = match ctx.attribute("broker", "DiskStoreUsage").await? {
                Json::Double(d) => d,
                Json::Long(n) => n as f64,
                _ => 0.0,
            };
            resource_usage("DiskStoreUsage", (usage * 100.0) as i32, max)?;
        }
        Task::MemoryUsage(threshold) => {
            let usage = ctx
                .attribute("broker", "AddressMemoryUsagePercentage")
                .await?
                .as_i64()
                .unwrap_or(0) as i32;
            resource_usage("MemoryUsage", usage, *threshold)?;
        }
        Task::QueueUp => {
            if ctx
                .attribute(&format!("queue.{name}"), "Paused")
                .await?
                .as_bool()
                == Some(true)
            {
                return Err(failure("The queue is paused."));
            }
        }
        Task::Produce(n) => {
            let conn = ctx.conn.connect().await?;
            let r = produce(&conn, name, *n).await;
            conn.close().await;
            r?;
        }
        Task::Browse(n) => {
            let conn = ctx.conn.connect().await?;
            let r = browse(&conn, name, *n).await;
            conn.close().await;
            r?;
        }
        Task::Consume(n) => {
            let conn = ctx.conn.connect().await?;
            let r = consume(&conn, name, *n).await;
            conn.close().await;
            r?;
        }
    }
    Ok(())
}

fn resource_usage(resource: &str, usage: i32, threshold: i32) -> CheckResult {
    if usage > threshold {
        return Err(failure(format!(
            "The {resource} {usage} is less than {threshold}"
        )));
    }
    Ok(())
}

async fn produce(conn: &JmsConnection, queue: &str, n: i32) -> CheckResult {
    let session = conn.session(false).await?;
    let dest = Destination::Queue(queue.to_owned());
    let producer = session.producer(&dest).await?;
    for _ in 0..n {
        let m = saranga_client::message::text_message("CHECK_MESSAGE");
        session.send(&producer, &dest, m, true, 0).await?;
    }
    session.close().await;
    Ok(())
}

async fn browse(conn: &JmsConnection, queue: &str, n: i32) -> CheckResult {
    let session = conn.session(false).await?;
    session.start().await?;
    let browser = session.browser(queue, None).await?;
    if n == -1 {
        browser
            .receive_immediate()
            .await
            .map_err(anyhow::Error::from)?;
    } else {
        for count in 0..n {
            if browser
                .receive_immediate()
                .await
                .map_err(anyhow::Error::from)?
                .is_none()
            {
                return Err(failure(format!("Insufficient messages to browse: {count}")));
            }
        }
    }
    session.close().await;
    Ok(())
}

async fn consume(conn: &JmsConnection, queue: &str, n: i32) -> CheckResult {
    let session = conn.session(false).await?;
    session.start().await?;
    let dest = Destination::Queue(queue.to_owned());
    let consumer = session.consumer(&dest, None, None).await?;
    if n == -1 {
        session.receive(&consumer, Some(Duration::ZERO)).await?;
    } else {
        for count in 0..n {
            if session.receive(&consumer, None).await?.is_none() {
                return Err(failure(format!(
                    "Insufficient messages to consume: {count}"
                )));
            }
        }
    }
    session.close().await;
    Ok(())
}

/// `new Date().toString()`, in the local time zone:
/// `Sun Oct 11 22:00:00 GMT+08:00 2026`.
pub fn java_date_now() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    saranga_server::localtime::java_date(millis)
}
