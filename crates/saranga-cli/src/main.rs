//! The `saranga` command (the counterpart of `artemis`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;

mod client;
mod completion;
mod create;
mod data;
mod instance;
mod logging;
mod perf;
mod upgrade;
mod web;

use clap::{CommandFactory, Parser, Subcommand};
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
        /// Broker properties applied over broker.xml, as `artemis run
        /// --properties`: comma-separated files, or directories ending in
        /// `/`. The default is `broker.properties` next to broker.xml, if
        /// there is one.
        #[arg(long)]
        properties: Option<String>,
        /// A system property, as `java -D`: `${name}` in the configuration
        /// files resolves to it, and `brokerconfig.`-prefixed ones are
        /// broker properties. Repeatable.
        #[arg(short = 'D', value_name = "NAME=VALUE", value_parser = parse_system_property)]
        system_properties: Vec<(String, String)>,
        /// This will allow the server to kill itself. Useful for tests
        /// (e.g. failover tests).
        #[arg(long)]
        allow_kill: bool,
    },
    /// Tools on a broker's data directory.
    #[command(subcommand)]
    Data(data::DataCommand),
    /// Create a new broker instance, as `artemis create`: its bin/artemis
    /// runs this saranga.
    Create(Box<create::CreateArgs>),
    /// Update a broker instance to this saranga, keeping all the data and
    /// broker.xml, as `artemis upgrade`. Warning: backup your instance
    /// before using this command and compare the files.
    Upgrade(Box<upgrade::UpgradeArgs>),
    /// Generates the auto complete script file to be used in bash or zsh.
    /// Usage: source <(./artemis completion)
    Completion {
        /// the script used to start artemis. (default ./artemis)
        #[arg(long = "start-script", default_value = "./artemis")]
        start_script: String,
    },
    /// Create a queue, or delete, update, purge or print stats of queues.
    #[command(subcommand)]
    Queue(client::queue::QueueCommand),
    /// Create, delete, update or show addresses.
    #[command(subcommand)]
    Address(client::address::AddressCommand),
    /// Add, list, reset or remove users of the broker's security manager.
    #[command(subcommand)]
    User(client::user::UserCommand),
    /// Send message(s) to a broker.
    Producer(client::messages::ProducerArgs),
    /// Consume messages from a queue.
    Consumer(client::messages::ConsumerArgs),
    /// Browse messages on a queue.
    Browser(client::messages::BrowseArgs),
    /// Move messages from one JMS destination towards another JMS
    /// destination.
    Transfer(client::messages::TransferArgs),
    /// Check a node or a queue.
    #[command(subcommand)]
    Check(client::check::CheckCommand),
    /// Stop the broker of an instance, as `artemis stop`: STOP_ME next to
    /// its broker.xml.
    Stop {
        /// Override the broker configuration from the bootstrap.xml.
        #[arg(long)]
        broker: Option<PathBuf>,
        /// Broker Configuration URI. Default:
        /// xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml.
        configuration: Option<String>,
    },
    /// Use 'help perf' for sub commands list.
    #[command(subcommand)]
    Perf(perf::PerfCommand),
    /// Calculate the journal-buffer-timeout to use with the current data
    /// folder.
    PerfJournal(perf::journal::PerfJournalArgs),
    /// Print version information.
    Version,
    /// Information on current folder and instance.
    Pwd,
    /// Kill a broker started with --allow-kill, as `artemis kill`: KILL_ME
    /// next to its broker.xml.
    Kill {
        /// Override the broker configuration from the bootstrap.xml.
        #[arg(long)]
        broker: Option<PathBuf>,
        /// Broker Configuration URI. Default:
        /// xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml.
        configuration: Option<String>,
    },
    /// Masks a password for the configuration files, as `artemis mask`:
    /// write the result as ENC(<result>).
    Mask {
        /// The password to be masked.
        password: String,
        /// A one-way hash (for users files) instead of reversible masking.
        #[arg(long)]
        hash: bool,
        /// The Blowfish key to mask with, instead of the default (or
        /// ARTEMIS_DEFAULT_SENSITIVE_STRING_CODEC_KEY).
        #[arg(long)]
        key: Option<String>,
        /// Use the <password-codec> of the broker.xml given with --config.
        #[arg(long, requires = "config")]
        password_codec: bool,
        /// The broker.xml whose password codec --password-codec uses.
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let client = matches!(
        cli.command,
        Command::Queue(_)
            | Command::Address(_)
            | Command::User(_)
            | Command::Producer(_)
            | Command::Consumer(_)
            | Command::Browser(_)
            | Command::Transfer(_)
            | Command::Check(_)
            | Command::Perf(_)
            | Command::PerfJournal(_)
            | Command::Stop { .. }
            | Command::Kill { .. }
            | Command::Version
            | Command::Pwd
            | Command::Create(_)
            | Command::Upgrade(_)
            | Command::Completion { .. }
    );
    if matches!(cli.command, Command::Run { .. }) {
        // The broker logs as Artemis does, once its log4j2.properties is read.
        logging::init_broker();
    } else {
        let logs = tracing_subscriber::fmt().with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(if client { "warn" } else { "info" })),
        );
        if client || matches!(cli.command, Command::Data(_)) {
            // These commands print their report on stdout.
            logs.with_writer(std::io::stderr).init();
        } else {
            logs.init();
        }
    }
    if client {
        return match run_client(cli.command).await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                report_client_error(&e);
                std::process::ExitCode::FAILURE
            }
        };
    }
    match run_command(cli.command).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// The commands against a running broker.
async fn run_client(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Queue(c) => client::queue::run(c).await,
        Command::Address(c) => client::address::run(c).await,
        Command::User(c) => client::user::run(c).await,
        Command::Producer(a) => client::messages::producer(a).await,
        Command::Consumer(a) => client::messages::consumer(a).await,
        Command::Browser(a) => client::messages::browser(a).await,
        Command::Transfer(a) => client::messages::transfer(a).await,
        Command::Check(c) => client::check::run(c).await,
        Command::Perf(c) => perf::run(c).await,
        Command::PerfJournal(a) => perf::journal::run(&a),
        Command::Stop {
            broker,
            configuration,
        } => control_file(broker, configuration, "STOP_ME"),
        Command::Kill {
            broker,
            configuration,
        } => control_file(broker, configuration, "KILL_ME"),
        Command::Create(a) => create::run(*a),
        Command::Upgrade(a) => upgrade::run(&a),
        Command::Completion { start_script } => {
            print!("{}", completion::script(&Cli::command(), &start_script));
            Ok(())
        }
        Command::Version => {
            // PrintVersion
            println!(
                "Saranga {VERSION} (Apache Artemis {})",
                saranga_server::management::VERSION
            );
            println!("Saranga home: {}", broker_home());
            println!("Saranga instance: {}", broker_instance());
            Ok(())
        }
        Command::Pwd => {
            let line = "*".repeat(127);
            println!();
            println!("{line}");
            println!("* Artemis instance:: {}", broker_instance());
            println!("* Home:: {}", broker_home());
            println!(
                "* etc:: {}",
                client::instance_etc()
                    .map(|e| e.display().to_string().replace('\\', "/"))
                    .unwrap_or_else(|| format!("{}/etc", broker_instance()))
            );
            let here = std::env::current_dir()
                .and_then(|d| d.canonicalize())
                .map(|d| dunce_path(&d))
                .unwrap_or_default();
            println!("* Current dir:{here}");
            println!("{line}");
            println!();
            Ok(())
        }
        _ => unreachable!("not a client command"),
    }
}

/// `getBrokerHome`: `ARTEMIS_HOME`, as the scripts set it, else the
/// directory above saranga's.
fn broker_home() -> String {
    std::env::var("ARTEMIS_HOME")
        .ok()
        .or_else(|| {
            let exe = std::env::current_exe().ok()?;
            Some(dunce_path(exe.parent()?.parent()?))
        })
        .unwrap_or_else(|| "null".to_owned())
        .replace('\\', "/")
}

/// `getBrokerInstance`: `ARTEMIS_INSTANCE`, with forward slashes, or
/// `null`.
fn broker_instance() -> String {
    client::instance_dir().map_or_else(
        || "null".to_owned(),
        |d| d.display().to_string().replace('\\', "/"),
    )
}

/// A path without Windows' `\\?\` verbatim prefix.
fn dunce_path(p: &Path) -> String {
    let s = p.display().to_string();
    s.strip_prefix(r"\\?\").map(str::to_owned).unwrap_or(s)
}

/// Reports a failed command as `Artemis.execute` does: a `CLIException`'s
/// message, a `RuntimeException`'s with the usage hint, a failed
/// connection as the CLI's retry prompt starts.
fn report_client_error(e: &anyhow::Error) {
    if let Some(u) = e.downcast_ref::<client::UsageError>() {
        eprintln!("{u}");
        println!();
        println!("Usage: saranga help [<args>...]");
        println!("use 'help <command>' for more information");
        println!("      [<args>...]");
    } else if let Some(c) = e.downcast_ref::<client::CliError>() {
        eprintln!("{c}");
    } else if let Some(c) = e.downcast_ref::<client::jms::ConnectFailed>() {
        eprintln!("Connection failed::{c}");
    } else if let Some(c) = e.downcast_ref::<saranga_client::Error>() {
        eprintln!("{}", client::jms::message(c));
    } else {
        eprintln!("{e:#}");
    }
}

/// `artemis stop` (`STOP_ME`) and `artemis kill` (`KILL_ME`): the file in
/// the directory of the instance's broker.xml, which the running broker
/// looks for.
fn control_file(
    broker: Option<PathBuf>,
    configuration: Option<String>,
    file: &str,
) -> anyhow::Result<()> {
    let broker_xml = match broker {
        Some(b) => PathBuf::from(b.to_string_lossy().trim_start_matches("file:")),
        None => {
            let etc = match configuration {
                Some(c) => {
                    let bootstrap = PathBuf::from(c.trim_start_matches("xml:"));
                    bootstrap.parent().map(Path::to_path_buf).unwrap_or_default()
                }
                None => client::instance_etc().context(
                    "stop needs a broker instance: run it from the instance's bin/ (ARTEMIS_INSTANCE)",
                )?,
            };
            instance::Instance::from_etc(&etc, instance::Options::default())?.broker_xml
        }
    };
    let dir = broker_xml
        .parent()
        .context("the broker configuration has no directory")?;
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(file))
        .with_context(|| format!("creating {}", dir.join(file).display()))?;
    Ok(())
}

async fn run_command(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Run {
            config,
            etc,
            bind,
            properties,
            system_properties,
            allow_kill,
        } => {
            let options = instance::Options {
                system: system_properties,
                broker_properties: properties,
            };
            // Without either, the instance whose bin/artemis runs this
            // (ARTEMIS_INSTANCE), if any.
            let etc = etc.or_else(client::instance_etc);
            let instance = match (config, etc) {
                (Some(c), _) => Some(instance::Instance::from_broker_xml(&c, options)?),
                (None, Some(e)) => Some(instance::Instance::from_etc(&e, options)?),
                (None, None) => None,
            };
            if let Some(instance) = &instance {
                logging::configure(
                    logging::config_path(
                        &instance.etc,
                        instance.lookup("log4j2.configurationFile"),
                    ),
                    instance.system_lookup(),
                );
            }
            // Run is a LockAbstract: the data tools refuse an instance whose
            // broker runs.
            let _lock = match &instance {
                Some(i) => Some(data::dirs::CliLock::hold(&i.dir)?),
                None => None,
            };
            run(instance, bind, allow_kill).await
        }
        Command::Data(cmd) => data::run(cmd).await,
        Command::Mask {
            password,
            hash,
            key,
            password_codec,
            config,
        } => mask(
            &password,
            hash,
            key,
            password_codec.then_some(config).flatten(),
        ),
        _ => unreachable!("a client command"),
    }
}

/// The protocols besides Core that `saranga run` serves.
const SERVED_PROTOCOLS: [&str; 5] = ["CORE", "STOMP", "MQTT", "AMQP", "OPENWIRE"];

/// The protocols besides Core that `saranga run` serves, as configured.
fn protocol_managers(
    file: Option<&saranga_config::BrokerConfig>,
    binds: &[Bind],
) -> Vec<Arc<dyn saranga_server::protocol::ProtocolManager>> {
    let mut mqtt = saranga_protocol_mqtt::MqttConfig::default();
    if let Some(f) = file {
        mqtt.session_scan_interval = f.mqtt_session_scan_interval;
        mqtt.subscription_persistence_enabled = f.mqtt_subscription_persistence_enabled;
        mqtt.internal_naming_prefix = f.internal_naming_prefix.clone();
        mqtt.management_address = f.management_address.clone();
        mqtt.management_notification_address = f.management_notification_address.clone();
    }
    // An acceptor that lists none of the protocols serves them all.
    mqtt.served = binds.iter().any(|b| match &b.acceptor.protocols {
        None => true,
        Some(listed) => {
            listed.iter().any(|p| p == "MQTT")
                || !listed
                    .iter()
                    .any(|p| SERVED_PROTOCOLS.contains(&p.as_str()))
        }
    });
    let amqp = saranga_protocol_amqp::AmqpConfig {
        use_core_subscription_naming: file.is_some_and(|f| f.amqp_use_core_subscription_naming),
    };
    vec![
        Arc::new(saranga_protocol_stomp::StompProtocol),
        Arc::new(saranga_protocol_mqtt::MqttProtocol::new(mqtt)),
        Arc::new(saranga_protocol_amqp::AmqpProtocol::new(amqp)),
        Arc::new(saranga_protocol_openwire::OpenWireProtocol::new()),
    ]
}

/// An acceptor to open.
struct Bind {
    name: String,
    address: String,
    acceptor: Acceptor,
}

/// Every protocol Artemis has, by the name acceptors list it under.
const ARTEMIS_PROTOCOLS: [&str; 6] = ["CORE", "AMQP", "STOMP", "HORNETQ", "MQTT", "OPENWIRE"];

/// The configured acceptors, with their TLS settings (`NettyAcceptor`),
/// security domain and protocols. `served` names the protocols Saranga
/// has.
fn acceptors(file: &saranga_config::BrokerConfig, served: &[&str]) -> anyhow::Result<Vec<Bind>> {
    let mut out = Vec::new();
    for a in &file.acceptors {
        let name = a.name.clone().unwrap_or_default();
        let listed = a.protocols();
        for p in &listed {
            if !ARTEMIS_PROTOCOLS.contains(&p.as_str()) {
                tracing::warn!(
                    "AMQ222203: Classpath lacks a protocol-manager for protocol {p}, Protocol being ignored on acceptor {}",
                    a.uri
                );
            }
        }
        // No list (or none Artemis knows) means every protocol.
        let known: Vec<&str> = listed
            .iter()
            .map(String::as_str)
            .filter(|p| ARTEMIS_PROTOCOLS.contains(p))
            .collect();
        let wanted: Vec<&str> = if known.is_empty() {
            ARTEMIS_PROTOCOLS.to_vec()
        } else {
            known
        };
        let missing: Vec<&str> = wanted
            .iter()
            .copied()
            .filter(|p| !served.contains(p))
            .collect();
        if missing.len() == wanted.len() {
            tracing::warn!(
                "acceptor {name} was not opened: Saranga does not serve {} yet",
                missing.join(", ")
            );
            continue;
        }
        if !missing.is_empty() {
            tracing::warn!(
                "acceptor {name} does not serve {}: Saranga does not serve them yet",
                missing.join(", ")
            );
        }
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
        let handshake_timeout = match a.param("handshake-timeout") {
            None => Some(saranga_server::protocol::DEFAULT_HANDSHAKE_TIMEOUT),
            Some(v) => {
                let secs: i64 = v
                    .parse()
                    .with_context(|| format!("acceptor {name}: handshake-timeout={v:?}"))?;
                (secs > 0).then(|| std::time::Duration::from_secs(secs as u64))
            }
        };
        out.push(Bind {
            name: name.clone(),
            address: format!("{host}:{port}"),
            acceptor: Acceptor {
                name,
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
                prefixes: prefixes(a),
                protocols: (!listed.is_empty()).then_some(listed.clone()),
                handshake_timeout,
                params: a
                    .params
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
                    .collect(),
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
/// broker, `KILL_ME` halts it at once with `--allow-kill` (the files are
/// removed on start).
async fn watch_stop_files(dir: PathBuf, allow_kill: bool) -> Stop {
    let stop = dir.join("STOP_ME");
    let kill = dir.join("KILL_ME");
    let _ = std::fs::remove_file(&stop);
    let _ = std::fs::remove_file(&kill);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if allow_kill && kill.exists() {
            let _ = std::fs::remove_file(&kill);
            return Stop::Halt;
        }
        if stop.exists() {
            let _ = std::fs::remove_file(&stop);
            return Stop::Clean;
        }
    }
}

/// `configuration-file-refresh-period`: every period, if `broker.xml` or
/// the broker properties changed, they are read again and the broker
/// reloads what can change while it runs (`ReloadManagerImpl`).
fn spawn_config_reloader(
    instance: Arc<instance::Instance>,
    broker: Arc<Broker>,
    period: std::time::Duration,
) {
    tokio::spawn(async move {
        let mut last = instance.config_fingerprint();
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            let now = instance.config_fingerprint();
            if now == last {
                continue;
            }
            last = now;
            let reload = instance.broker_config().and_then(|c| {
                saranga_server::reload::Reload::from_file_config(&c).map_err(anyhow::Error::from)
            });
            match reload {
                Ok(reload) => broker.reload(reload),
                Err(e) => tracing::warn!("configuration not reloaded: {e:#}"),
            }
        }
    });
}

/// `anycastPrefix` and `multicastPrefix`, each a comma-separated list
/// (`CoreProtocolManager.setAnycastPrefix`).
fn prefixes(a: &saranga_config::AcceptorConfig) -> Vec<(String, saranga_server::RoutingType)> {
    let mut out = Vec::new();
    for (key, t) in [
        ("anycastPrefix", saranga_server::RoutingType::Anycast),
        ("multicastPrefix", saranga_server::RoutingType::Multicast),
    ] {
        if let Some(list) = a.param(key) {
            out.extend(list.split(',').map(|p| (p.to_owned(), t)));
        }
    }
    out
}

/// Binds the acceptor's host the way Netty does: to the one address
/// `InetAddress.getByName` gives, which is the first IPv4 address when the
/// name has one (`localhost` is 127.0.0.1, not `::1`).
async fn bind_acceptor(address: &str) -> std::io::Result<TcpListener> {
    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host(address).await?.collect();
    addrs.sort_by_key(|a| !a.is_ipv4());
    match addrs.first() {
        Some(a) => TcpListener::bind(a).await,
        None => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("{address} resolves to no address"),
        )),
    }
}

/// `artemis mask` (`Mask.execute`).
fn mask(
    password: &str,
    hash: bool,
    key: Option<String>,
    config: Option<PathBuf>,
) -> anyhow::Result<()> {
    use saranga_config::mask::Codec;
    let codec = match config {
        Some(path) => {
            let instance = instance::Instance::from_broker_xml(&path, Default::default())?;
            let file = instance.broker_config()?;
            Codec::from_desc(Some(&file.password_codec))?
        }
        None => {
            let mut params = Vec::new();
            if hash {
                params.push(("algorithm".to_owned(), "one-way".to_owned()));
            }
            if let Some(key) = key {
                if hash {
                    println!("Option --key ignored in case of hashing");
                } else {
                    params.push(("key".to_owned(), key));
                }
            }
            Codec::init(&params, |n| std::env::var(n).ok())?
        }
    };
    println!("result: {}", codec.encode(password)?);
    Ok(())
}

/// SIGTERM, as `artemis-service stop` and service managers send it: a
/// clean stop, as the JVM's shutdown hook makes it for Artemis.
async fn terminated() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
            return;
        }
    }
    std::future::pending::<()>().await
}

/// `-D name=value`; a bare `-D name` is the empty string, as in Java.
fn parse_system_property(s: &str) -> Result<(String, String), String> {
    let (k, v) = s.split_once('=').unwrap_or((s, ""));
    if k.is_empty() {
        return Err("expected NAME=VALUE".to_owned());
    }
    Ok((k.to_owned(), v.to_owned()))
}

/// Saranga's version, in the startup and shutdown messages.
const VERSION: &str = env!("CARGO_PKG_VERSION");

async fn run(
    instance: Option<instance::Instance>,
    bind: Option<SocketAddr>,
    allow_kill: bool,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    tracing::info!("AMQ101000: Starting Saranga Server version {VERSION}");
    let filters = Arc::new(SelectorFilters);
    let served = SERVED_PROTOCOLS;
    let mut file_config = None;
    let (mut broker_config, mut binds, reload_period) = match &instance {
        Some(instance) => {
            let file = instance.broker_config()?;
            tracing::info!(
                "AMQ221000: Primary message broker is starting with configuration Broker Configuration (clustered=false,journalDirectory={},bindingsDirectory={},largeMessagesDirectory={},pagingDirectory={})",
                file.journal_directory,
                file.bindings_directory,
                file.large_messages_directory,
                file.paging_directory
            );
            if file.persistence_enabled {
                // Buffered file I/O on every platform: Artemis's NIO.
                tracing::info!("AMQ221012: Using NIO Journal");
            }
            let binds = acceptors(&file, &served)?;
            let mut config = BrokerConfig::from_file_config(&file, filters, &instance.dir)?;
            let security = instance.security(&file)?;
            config.security = security.store;
            config.users = security.users;
            config.bootstrap_users = security.bootstrap_users;
            config.management.file_path = Some(instance.broker_xml.clone());
            let period = file.configuration_file_refresh_period.max(1) as u64;
            file_config = Some(file);
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
    broker_config.protocols = protocol_managers(file_config.as_ref(), &binds);
    tracing::info!(
        "AMQ221043: Protocol module found: [saranga-server]. Adding protocol support for: CORE"
    );
    for p in &broker_config.protocols {
        tracing::info!(
            "AMQ221043: Protocol module found: [saranga-protocol-{}]. Adding protocol support for: {}",
            p.name().to_ascii_lowercase(),
            p.name()
        );
    }
    let broker = Broker::start(broker_config).await?;
    let mut servers = tokio::task::JoinSet::new();
    for Bind {
        name,
        address,
        acceptor,
    } in binds
    {
        let listener = bind_acceptor(&address)
            .await
            .with_context(|| format!("acceptor {name} on {address}"))?;
        if let Some(tls) = &acceptor.tls {
            tls.spawn_reloader(reload_period);
        }
        // Artemis's log IDs, so log-based tooling keeps working (ADR 0003).
        let names: Vec<&str> = broker
            .protocols_for(&acceptor)
            .iter()
            .map(|p| p.name())
            .collect();
        tracing::info!(
            "AMQ221020: Started NIO Acceptor at {address} for protocols [{}]",
            names.join(",")
        );
        servers.spawn(broker.clone().serve_acceptor(listener, acceptor));
    }
    let mut web_started = false;
    if let (Some(instance), Some(file)) = (&instance, &file_config)
        && let Some(web) = instance.bootstrap.as_ref().and_then(|b| b.web.clone())
    {
        web::start(instance, file, &web, &broker).await?;
        web_started = true;
    }
    tracing::info!("AMQ221007: Server is now active");
    tracing::info!(
        "AMQ221001: Saranga Message Broker version {VERSION} [{}, nodeID={}] ",
        broker.management().config().name,
        broker.node_id()
    );
    let instance = instance.map(Arc::new);
    if let Some(instance) = &instance {
        spawn_config_reloader(instance.clone(), broker.clone(), reload_period);
        // reloadConfigurationFile through management.
        let loader = instance.clone();
        broker.set_config_loader(Arc::new(move || {
            loader
                .broker_config()
                .and_then(|c| {
                    saranga_server::reload::Reload::from_file_config(&c)
                        .map_err(anyhow::Error::from)
                })
                .map_err(|e| format!("{e:#}"))
        }));
    }
    let stop_dir = instance
        .as_ref()
        .and_then(|i| i.broker_xml.parent().map(Path::to_path_buf));
    let stop_files = async {
        match stop_dir {
            Some(dir) => watch_stop_files(dir, allow_kill).await,
            None => std::future::pending().await,
        }
    };
    let stop = tokio::select! {
        Some(result) = servers.join_next() => {
            result??;
            Stop::Clean
        }
        _ = tokio::signal::ctrl_c() => Stop::Clean,
        _ = terminated() => Stop::Clean,
        // The critical analyzer's SHUTDOWN policy.
        _ = broker.shutdown_requested() => Stop::Clean,
        s = stop_files => s,
    };
    match stop {
        Stop::Halt => {
            eprintln!("Halting by user request");
            std::process::exit(0);
        }
        Stop::Clean => {
            if web_started {
                // Its tasks end with the runtime.
                tracing::info!("AMQ241005: Stopping embedded web server");
                tracing::info!("AMQ241006: Stopped embedded web server");
            }
            servers.abort_all();
            broker.stop()?;
            tracing::info!(
                "AMQ221002: Saranga Message Broker version {VERSION} [{}] stopped, uptime {}",
                broker.node_id(),
                saranga_server::management::print_duration(started.elapsed().as_millis() as i64)
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use clap::CommandFactory;

    /// Duplicate argument IDs (a command's `--user` against the
    /// connection's) and the like.
    #[test]
    fn cli_is_well_formed() {
        super::Cli::command().debug_assert();
    }
}
