//! The `saranga` command (the counterpart of `artemis`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;

mod data;
mod instance;

use clap::{Parser, Subcommand};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use saranga_transport::{TlsAcceptor, TlsSettings};
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
        /// directory above the file's (`<instance>/etc/broker.xml`), and
        /// `login.config` and `bootstrap.xml` next to it apply. Without a
        /// configuration the broker keeps everything in memory and has no
        /// security.
        #[arg(long, conflicts_with = "etc")]
        config: Option<PathBuf>,
        /// An Artemis instance's etc directory, used as `artemis run` uses
        /// it: `bootstrap.xml` names `broker.xml` and the security domain.
        #[arg(long)]
        etc: Option<PathBuf>,
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
        Command::Run { config, etc, bind } => {
            let instance = match (config, etc) {
                (Some(c), _) => Some(instance::Instance::from_broker_xml(&c)?),
                (None, Some(e)) => Some(instance::Instance::from_etc(&e)?),
                (None, None) => None,
            };
            run(instance, bind).await
        }
        Command::Data(cmd) => data::run(cmd),
    }
}

/// An acceptor to open.
struct Bind {
    name: String,
    address: String,
    acceptor: Acceptor,
}

/// The configured acceptors that speak Core, with their TLS settings
/// (`NettyAcceptor`) and security domain.
fn core_acceptors(file: &saranga_config::BrokerConfig) -> anyhow::Result<Vec<Bind>> {
    let mut out = Vec::new();
    for a in &file.acceptors {
        let protocols = a.protocols();
        if !(protocols.is_empty() || protocols.iter().any(|p| p.eq_ignore_ascii_case("CORE"))) {
            continue;
        }
        let name = a.name.clone().unwrap_or_default();
        let host = a.host.as_deref().unwrap_or("localhost");
        let port = a.port.unwrap_or(61616);
        // `ConfigurationHelper.getPasswordProperty`: `mask-password` and
        // `password-codec` from the broker, unless the URI sets its own.
        let use_mask = a
            .param("activemq.usemaskedpassword")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .or(file.mask_password);
        let codec = a
            .param("activemq.passwordcodec")
            .unwrap_or(&file.password_codec);
        let tls = TlsSettings::from_params(
            |k| a.param(k).map(str::to_owned),
            |v| {
                saranga_config::mask::resolve_mask(use_mask, v, Some(codec))
                    .map_err(|e| e.to_string())
            },
        )
        .and_then(|s| s.map(TlsAcceptor::new).transpose())
        .with_context(|| format!("acceptor {name}"))?;
        out.push(Bind {
            name,
            address: format!("{host}:{port}"),
            acceptor: Acceptor {
                tls,
                security_domain: a.param("securityDomain").map(str::to_owned),
                host: host.to_owned(),
                port,
                core_connection_security: a
                    .param("coreConnectionSecurityEnabled")
                    .is_none_or(|v| v.eq_ignore_ascii_case("true")),
                core_initial_max_frame_size: size_param(a, "coreInitialMaxFrameSize")?
                    .unwrap_or(Acceptor::CORE_INITIAL_MAX_FRAME_SIZE),
                core_max_frame_size: size_param(a, "coreMaxFrameSize")?
                    .unwrap_or(Acceptor::CORE_MAX_FRAME_SIZE),
            },
        });
    }
    Ok(out)
}

/// A frame size acceptor parameter, in bytes.
fn size_param(a: &saranga_config::AcceptorConfig, name: &str) -> anyhow::Result<Option<usize>> {
    a.param(name)
        .map(|v| {
            v.parse::<usize>()
                .with_context(|| format!("acceptor parameter {name}={v:?}"))
        })
        .transpose()
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

async fn run(instance: Option<instance::Instance>, bind: Option<SocketAddr>) -> anyhow::Result<()> {
    let filters = Arc::new(SelectorFilters);
    let (broker_config, mut binds, reload_period) = match &instance {
        Some(instance) => {
            let file = instance.broker_config()?;
            let binds = core_acceptors(&file)?;
            let mut config = BrokerConfig::from_file_config(&file, filters, &instance.dir)?;
            config.security = instance.security(&file)?;
            let period = file.configuration_file_refresh_period.max(1) as u64;
            (config, binds, std::time::Duration::from_millis(period))
        }
        None => (
            BrokerConfig::new(filters),
            Vec::new(),
            std::time::Duration::from_secs(5),
        ),
    };
    if let Some(bind) = bind {
        // The first Core acceptor's settings (TLS, security domain), at
        // this address instead.
        let first = binds.into_iter().next().unwrap_or_else(|| Bind {
            name: "core".to_owned(),
            address: String::new(),
            acceptor: Acceptor::default(),
        });
        binds = vec![Bind {
            address: bind.to_string(),
            ..first
        }];
    } else if binds.is_empty() {
        binds = vec![Bind {
            name: "artemis".to_owned(),
            address: "0.0.0.0:61616".to_owned(),
            acceptor: Acceptor::default(),
        }];
    }
    let broker = Broker::start(broker_config).await?;
    let mut servers = tokio::task::JoinSet::new();
    for Bind {
        name,
        address,
        acceptor,
    } in binds
    {
        let listener = TcpListener::bind(&address)
            .await
            .with_context(|| format!("acceptor {name} on {address}"))?;
        if let Some(tls) = &acceptor.tls {
            tls.spawn_reloader(reload_period);
        }
        // Artemis's log IDs, so log-based tooling keeps working (ADR 0003).
        tracing::info!("AMQ221020: Started Core Acceptor at {address} for protocols [CORE]");
        servers.spawn(broker.clone().serve_acceptor(listener, acceptor));
    }
    tracing::info!("AMQ221007: Server is now active");
    let stop_dir = instance
        .as_ref()
        .and_then(|i| i.broker_xml.parent().map(Path::to_path_buf));
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
