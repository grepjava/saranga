//! The `saranga` command (the counterpart of `artemis`).

use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use saranga_server::postoffice::NoFilters;
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
        /// Where the Core acceptor listens.
        #[arg(long, default_value = "0.0.0.0:61616")]
        bind: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().command {
        Command::Run { bind } => run(bind).await,
    }
}

async fn run(bind: SocketAddr) -> anyhow::Result<()> {
    let broker = Broker::new(BrokerConfig::new(Arc::new(NoFilters)))?;
    let listener = TcpListener::bind(bind).await?;
    // Artemis's log IDs, so log-based tooling keeps working (ADR 0003).
    tracing::info!("AMQ221020: Started Core Acceptor at {bind} for protocols [CORE]");
    tracing::info!("AMQ221007: Server is now active");
    tokio::select! {
        result = broker.serve(listener) => result?,
        _ = tokio::signal::ctrl_c() => tracing::info!("AMQ221002: Saranga Message Broker is stopped"),
    }
    Ok(())
}
