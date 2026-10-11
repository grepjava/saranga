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
    },
    /// Tools on a broker's data directory.
    #[command(subcommand)]
    Data(data::DataCommand),
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
        Command::Run {
            config,
            etc,
            bind,
            properties,
            system_properties,
        } => {
            let options = instance::Options {
                system: system_properties,
                broker_properties: properties,
            };
            let instance = match (config, etc) {
                (Some(c), _) => Some(instance::Instance::from_broker_xml(&c, options)?),
                (None, Some(e)) => Some(instance::Instance::from_etc(&e, options)?),
                (None, None) => None,
            };
            run(instance, bind).await
        }
        Command::Data(cmd) => data::run(cmd),
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
    }
}

/// The protocols besides Core that `saranga run` serves.
const SERVED_PROTOCOLS: [&str; 4] = ["CORE", "STOMP", "MQTT", "AMQP"];

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

/// `-D name=value`; a bare `-D name` is the empty string, as in Java.
fn parse_system_property(s: &str) -> Result<(String, String), String> {
    let (k, v) = s.split_once('=').unwrap_or((s, ""));
    if k.is_empty() {
        return Err("expected NAME=VALUE".to_owned());
    }
    Ok((k.to_owned(), v.to_owned()))
}

async fn run(instance: Option<instance::Instance>, bind: Option<SocketAddr>) -> anyhow::Result<()> {
    let filters = Arc::new(SelectorFilters);
    let served = SERVED_PROTOCOLS;
    let mut file_config = None;
    let (mut broker_config, mut binds, reload_period) = match &instance {
        Some(instance) => {
            let file = instance.broker_config()?;
            let binds = acceptors(&file, &served)?;
            let mut config = BrokerConfig::from_file_config(&file, filters, &instance.dir)?;
            let security = instance.security(&file)?;
            config.security = security.store;
            config.users = security.users;
            config.bootstrap_users = security.bootstrap_users;
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
    tracing::info!("AMQ221007: Server is now active");
    let instance = instance.map(Arc::new);
    if let Some(instance) = &instance {
        spawn_config_reloader(instance.clone(), broker.clone(), reload_period);
    }
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
