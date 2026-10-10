//! The `saranga` command (the counterpart of `artemis`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;

mod data;

use clap::{Parser, Subcommand};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "saranga",
    version,
    about = "A message broker compatible with Apache ActiveMQ Artemis"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the broker in the foreground.
    Run {
        /// A broker.xml; its acceptors that speak CORE are opened. Its
        /// relative data directories are under the broker instance, the
        /// directory above the file's (`<instance>/etc/broker.xml`). Without
        /// one the broker keeps everything in memory.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Listen here instead of on the configured acceptors.
        #[arg(long)]
        bind: Option<SocketAddr>,
    },
    /// Tools on a broker's data directory.
    #[command(subcommand)]
    Data(data::DataCommand),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let logs = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    );
    if matches!(cli.command, Command::Data(_)) {
        // The data tools print their report on stdout.
        logs.with_writer(std::io::stderr).init();
    } else {
        logs.init();
    }
    match cli.command {
        Command::Run { config, bind } => run(config, bind).await,
        Command::Data(cmd) => data::run(cmd),
    }
}

/// `<instance>/etc/broker.xml` -> `<instance>`.
fn instance_dir(config: &Path) -> PathBuf {
    let etc = config
        .canonicalize()
        .unwrap_or_else(|_| config.to_path_buf())
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    etc.parent().map(Path::to_path_buf).unwrap_or(etc)
}

/// How the broker was asked to stop.
enum Stop {
    Clean,
    Halt,
}

/// `Run.addShutdownHook`: `STOP_ME` next to the configuration stops the
/// broker, `KILL_ME` halts it at once (the files are removed on start).
async fn watch_stop_files(dir: PathBuf) -> Stop {
    let stop = dir.join("STOP_ME");
    let kill = dir.join("KILL_ME");
    let _ = std::fs::remove_file(&stop);
    let _ = std::fs::remove_file(&kill);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if kill.exists() {
            let _ = std::fs::remove_file(&kill);
            return Stop::Halt;
        }
        if stop.exists() {
            let _ = std::fs::remove_file(&stop);
            return Stop::Clean;
        }
    }
}

async fn run(config: Option<PathBuf>, bind: Option<SocketAddr>) -> anyhow::Result<()> {
    let filters = Arc::new(SelectorFilters);
    let (broker_config, mut binds) = match &config {
        Some(path) => {
            let file = saranga_config::BrokerConfig::from_file(path)
                .with_context(|| format!("reading {}", path.display()))?;
            for w in &file.warnings {
                tracing::warn!("{}: {w}", path.display());
            }
            let binds = file
                .acceptors
                .iter()
                .filter(|a| {
                    let p = a.protocols();
                    p.is_empty() || p.iter().any(|p| p.eq_ignore_ascii_case("CORE"))
                })
                .map(|a| {
                    let host = a.host.as_deref().unwrap_or("localhost");
                    let port = a.port.unwrap_or(61616);
                    (a.name.clone().unwrap_or_default(), format!("{host}:{port}"))
                })
                .collect();
            (
                BrokerConfig::from_file_config(&file, filters, &instance_dir(path))?,
                binds,
            )
        }
        None => (BrokerConfig::new(filters), Vec::new()),
    };
    if let Some(bind) = bind {
        binds = vec![("core".to_owned(), bind.to_string())];
    } else if binds.is_empty() {
        binds = vec![("artemis".to_owned(), "0.0.0.0:61616".to_owned())];
    }
    let broker = Broker::start(broker_config).await?;
    let mut servers = tokio::task::JoinSet::new();
    for (name, address) in binds {
        let listener = TcpListener::bind(&address)
            .await
            .with_context(|| format!("acceptor {name} on {address}"))?;
        // Artemis's log IDs, so log-based tooling keeps working (ADR 0003).
        tracing::info!("AMQ221020: Started Core Acceptor at {address} for protocols [CORE]");
        servers.spawn(broker.clone().serve(listener));
    }
    tracing::info!("AMQ221007: Server is now active");
    let stop_dir = config
        .as_deref()
        .and_then(|c| c.canonicalize().ok())
        .and_then(|c| c.parent().map(Path::to_path_buf));
    let stop_files = async {
        match stop_dir {
            Some(dir) => watch_stop_files(dir).await,
            None => std::future::pending().await,
        }
    };
    let stop = tokio::select! {
        Some(result) = servers.join_next() => {
            result??;
            Stop::Clean
        }
        _ = tokio::signal::ctrl_c() => Stop::Clean,
        s = stop_files => s,
    };
    match stop {
        Stop::Halt => {
            eprintln!("Halting by user request");
            std::process::exit(0);
        }
        Stop::Clean => {
            servers.abort_all();
            broker.stop()?;
            tracing::info!("AMQ221002: Saranga Message Broker is stopped");
        }
    }
    Ok(())
}
