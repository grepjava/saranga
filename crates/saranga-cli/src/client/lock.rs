//! `lock list|start|stop` (`LockGroup`): the broker's lock coordinators,
//! through `SimpleManagement`.

use anyhow::bail;
use saranga_server::management::json::Json;

use super::management::{simple_call, uncaught};
use super::queue::TableOut;
use super::{CliError, ConnectionArgs, UsageError};

#[derive(clap::Subcommand, Debug)]
pub enum LockCommand {
    /// List the lock coordinators and their status on the server.
    List(ListArgs),
    /// Start a lock coordinator.
    Start(NameArgs),
    /// Stop a lock coordinator.
    Stop(NameArgs),
}

#[derive(clap::Args, Debug)]
pub struct ListArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Monitor locks continuously by repeating the command at the specified
    /// interval in milliseconds. Use -1 to disable (default).
    #[arg(long, default_value_t = -1, allow_negative_numbers = true)]
    sleep: i64,
}

#[derive(clap::Args, Debug)]
pub struct NameArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Name of the lock coordinator to start.
    lock_name: Option<String>,
}

pub async fn run(command: LockCommand) -> anyhow::Result<()> {
    match command {
        LockCommand::List(args) => list(args).await,
        LockCommand::Start(args) => manage(args, "startLockCoordinator").await,
        LockCommand::Stop(args) => manage(args, "stopLockCoordinator").await,
    }
}

/// `LockManagementAbstract`: the operation on the named coordinator.
async fn manage(mut args: NameArgs, operation: &str) -> anyhow::Result<()> {
    args.conn.resolve(false)?;
    let name = args.lock_name.map_or(Json::Null, Json::from);
    simple_call(&args.conn, "broker", operation, vec![name])
        .await
        .map(|_| ())
        .map_err(uncaught)
}

/// `LockList`: the lock coordinators, again every `--sleep` milliseconds.
async fn list(mut args: ListArgs) -> anyhow::Result<()> {
    args.conn.resolve(false)?;
    loop {
        println!(
            "{}>> Lock list results for {}",
            super::check::java_date_now(),
            super::instance_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_else(|| "null".to_owned())
        );
        let text = simple_call(
            &args.conn,
            "broker",
            "listLockCoordinatorsAsJSON",
            Vec::new(),
        )
        .await
        .map_err(uncaught)?;
        let locks = match Json::parse(&text) {
            Ok(Json::Array(a)) => a,
            Ok(other) => bail!(CliError(format!(
                "jakarta.json.JsonException: not a JSON array: {other}"
            ))),
            Err(e) => bail!(CliError(format!("jakarta.json.JsonException: {e}"))),
        };
        print_stats(&locks);
        if args.sleep <= 0 {
            return Ok(());
        }
        println!("Waiting {} before another lock list iteration", args.sleep);
        let Ok(millis) = u64::try_from(args.sleep) else {
            bail!(UsageError("timeout value is negative".to_owned()));
        };
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    }
}

/// `printStats`: a table of the coordinators' name, ID and status.
fn print_stats(locks: &[Json]) {
    if locks.is_empty() {
        println!("\u{1b}[33mNo Lock Coordinators configured\u{1b}[0m");
        println!();
        return;
    }
    let fields = ["NAME", "ID", "STATUS"];
    let keys = ["name", "lockId", "status"];
    let width = |s: &str| s.encode_utf16().count();
    let cell = |lock: &Json, key: &str| match lock.get(key) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    };
    let mut sizes: Vec<usize> = fields.iter().map(|f| f.len()).collect();
    for lock in locks {
        for (i, key) in keys.iter().enumerate() {
            sizes[i] = sizes[i].max(width(&cell(lock, key)));
        }
    }
    let table = TableOut::new("|", 2, sizes);
    let centre = [false; 3];
    let titles: Vec<Vec<String>> = fields
        .iter()
        .map(|f| f.split('_').map(str::to_owned).collect())
        .collect();
    table.print_lines(&titles, Some(&centre));
    for lock in locks {
        let columns: Vec<String> = keys.iter().map(|k| cell(lock, k)).collect();
        table.print(&columns, Some(&centre));
    }
    println!();
}
