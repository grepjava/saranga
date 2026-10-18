//! The `saranga` command (the counterpart of `artemis`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;

// mimalloc packs the many small allocations a queued message makes (its
// properties, strings and references) tighter than the system allocator,
// and faster. Off (--no-default-features) for heap profilers that track
// the system allocator.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod activation;
mod client;
mod completion;
mod create;
mod data;
mod help;
mod instance;
mod logging;
mod network;
mod perf;
mod shell;
mod upgrade;
mod web;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use saranga_server::node::NodeManager;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use saranga_transport::{TlsAcceptor, TlsSettings};

#[derive(Parser)]
#[command(
    name = "saranga",
    version,
    about = "A message broker compatible with Apache ActiveMQ Artemis",
    disable_help_subcommand = true
)]
struct Cli {
    /// Without a command, the shell.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the broker in the foreground.
    Run {
        /// A broker.xml; its acceptors are opened. Its
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
        /// Override the broker configuration from the bootstrap.xml.
        #[arg(long, conflicts_with = "config")]
        broker: Option<String>,
        /// Broker Configuration URI. Default:
        /// xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml.
        #[arg(conflicts_with = "config")]
        configuration: Option<String>,
        /// The data folders, which `artemis run` takes and does not use:
        /// the broker's are broker.xml's.
        #[command(flatten)]
        folders: data::dirs::FolderArgs,
        /// Print additional information.
        #[arg(long)]
        verbose: bool,
    },
    /// Tools on a broker's data directory.
    #[command(subcommand)]
    Data(data::DataCommand),
    /// The activation sequences of replication with a lock manager.
    #[command(subcommand)]
    Activation(activation::ActivationCommand),
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
        /// Print additional information.
        #[arg(long)]
        verbose: bool,
    },
    /// Use 'help perf' for sub commands list.
    #[command(subcommand)]
    Perf(perf::PerfCommand),
    /// Calculate the journal-buffer-timeout to use with the current data
    /// folder.
    PerfJournal(perf::journal::PerfJournalArgs),
    /// Print version information.
    Version {
        /// Print additional information.
        #[arg(long)]
        verbose: bool,
    },
    /// Information on current folder and instance.
    Pwd {
        /// Print additional information.
        #[arg(long)]
        verbose: bool,
    },
    /// Kill a broker started with --allow-kill, as `artemis kill`: KILL_ME
    /// next to its broker.xml.
    Kill {
        /// Override the broker configuration from the bootstrap.xml.
        #[arg(long)]
        broker: Option<PathBuf>,
        /// Broker Configuration URI. Default:
        /// xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml.
        configuration: Option<String>,
        /// Print additional information.
        #[arg(long)]
        verbose: bool,
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
        /// Use the <password-codec> of the instance's broker.xml, or of the
        /// one given with --config.
        #[arg(long)]
        password_codec: bool,
        /// The broker.xml whose password codec --password-codec uses, in
        /// place of the instance's.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Print additional information.
        #[arg(long)]
        verbose: bool,
    },
    /// A shell running the CLI's commands, as `artemis shell`. `saranga`
    /// without a command starts it too.
    Shell(shell::ShellArgs),
    /// Connect to the broker validating credentials for commands.
    #[command(hide = true)]
    Connect(client::ConnectionArgs),
    /// Clear previously typed user credentials.
    #[command(hide = true)]
    Disconnect(client::ConnectionArgs),
    /// Print the help of the given command, as `artemis help` prints it.
    Help {
        /// The command and subcommand.
        args: Vec<String>,
    },
}

/// The command line (`Artemis.buildCommand`). In the shell, `connect` and
/// `disconnect` are offered and `run` and `shell` are not; outside it, the
/// reverse.
fn command_line(in_shell: bool) -> clap::Command {
    let (shown, hidden) = if in_shell {
        (["connect", "disconnect"], ["run", "shell"])
    } else {
        (["run", "shell"], ["connect", "disconnect"])
    };
    let mut command = Cli::command();
    for name in shown {
        command = command.mut_subcommand(name, |c| c.hide(false));
    }
    for name in hidden {
        command = command.mut_subcommand(name, |c| c.hide(true));
    }
    any_path(command)
}

/// A file option takes any value, the empty one too, as picocli's `File`
/// converter does (`new File("")`); clap's refuses an empty path.
fn any_path(mut command: clap::Command) -> clap::Command {
    #[derive(Clone)]
    struct AnyPath;
    impl clap::builder::TypedValueParser for AnyPath {
        type Value = std::path::PathBuf;
        fn parse_ref(
            &self,
            _: &clap::Command,
            _: Option<&clap::Arg>,
            value: &std::ffi::OsStr,
        ) -> Result<Self::Value, clap::Error> {
            Ok(std::path::PathBuf::from(value))
        }
    }
    let path = clap::builder::ValueParser::path_buf().type_id();
    let ids: Vec<clap::Id> = command
        .get_arguments()
        .filter(|a| a.get_value_parser().type_id() == path)
        .map(|a| a.get_id().clone())
        .collect();
    for id in ids {
        command = command.mut_arg(id, |a| a.value_parser(AnyPath));
    }
    let subs: Vec<String> = command
        .get_subcommands()
        .map(|s| s.get_name().to_owned())
        .collect();
    for name in subs {
        command = command.mut_subcommand(name, any_path);
    }
    command
}

/// `data exp`'s `log-interval` and `undefined-prefix`, which Artemis's
/// declares without dashes (`log-interval=<logInterval>`), as
/// `--log-interval` and `--undefined-prefix`.
pub fn artemis_spellings<T>(mut args: Vec<T>) -> Vec<T>
where
    T: From<String> + AsRef<std::ffi::OsStr>,
{
    let words: Vec<Option<String>> = args
        .iter()
        .map(|a| a.as_ref().to_str().map(str::to_owned))
        .collect();
    let Some(exp) = words
        .windows(2)
        .position(|w| w[0].as_deref() == Some("data") && w[1].as_deref() == Some("exp"))
    else {
        return args;
    };
    for (i, word) in words.iter().enumerate().skip(exp + 2) {
        let Some(word) = word else { continue };
        for name in ["log-interval", "undefined-prefix"] {
            if word == name || word.starts_with(&format!("{name}=")) {
                args[i] = T::from(format!("--{word}"));
            }
        }
    }
    args
}

/// What picocli makes of a command line (`help::parse`), as
/// `Artemis.execute` and the shell act on it.
pub(crate) enum Parsed {
    /// The command line in the form saranga's clap command line reads.
    Args(Vec<String>),
    /// A mistake, `ParameterException`'s message.
    Mistake(help::parse::ParseError),
    /// A group alone (`queue`), whose action prints its help: the group's
    /// words.
    Group(Vec<String>),
    /// Not for the parser: words that are not text, or saranga's own
    /// `--help` and `--version`, which clap answers.
    Clap,
}

/// The command line `words` (without the program's name), parsed as
/// picocli parses Artemis's.
pub(crate) fn parse_artemis(
    words: &[std::ffi::OsString],
    command: &clap::Command,
    in_shell: bool,
) -> Parsed {
    let Some(words) = words
        .iter()
        .map(|w| w.to_str().map(str::to_owned))
        .collect::<Option<Vec<String>>>()
    else {
        return Parsed::Clap;
    };
    // The shell takes them as Artemis's does: as no option of its commands.
    let own = |w: &String| matches!(w.as_str(), "-h" | "--help" | "-V" | "--version");
    if !in_shell && words.iter().take_while(|w| *w != "--").any(own) {
        return Parsed::Clap;
    }
    match help::parse::parse(&words, in_shell) {
        Err(e) => Parsed::Mistake(e),
        Ok(p) if p.is_group() => Parsed::Group(p.path),
        Ok(p) => Parsed::Args(artemis_spellings(p.clap_args("saranga", command))),
    }
}

/// The help mode of where saranga runs: in an instance or from the home.
pub(crate) fn help_mode() -> help::Mode {
    if client::instance_dir().is_some() {
        help::Mode::Instance
    } else {
        help::Mode::Home
    }
}

/// A `ParameterException`, as `Artemis.execute` reports a
/// `RuntimeException`: its message, then `help help`.
fn report_mistake(e: &help::parse::ParseError) {
    eprintln!("{e}");
    println!();
    print!(
        "{}",
        help::help("saranga", &["help".to_owned()], help::Mode::Home, false)
    );
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let mut command_line = command_line(false);
    let raw: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let args: Vec<std::ffi::OsString> = match parse_artemis(&raw[1..], &command_line, false) {
        Parsed::Args(args) => args.into_iter().map(Into::into).collect(),
        Parsed::Mistake(e) => {
            report_mistake(&e);
            return std::process::ExitCode::FAILURE;
        }
        Parsed::Group(words) => {
            print!("{}", help::help("saranga", &words, help_mode(), false));
            return std::process::ExitCode::SUCCESS;
        }
        Parsed::Clap => artemis_spellings(raw),
    };
    let cli = Cli::from_arg_matches(&command_line.clone().get_matches_from(args))
        .unwrap_or_else(|e| e.format(&mut command_line).exit());
    let command = match cli.command {
        Some(Command::Connect(_)) | Some(Command::Disconnect(_)) => {
            let name = if matches!(cli.command, Some(Command::Connect(_))) {
                "connect"
            } else {
                "disconnect"
            };
            command_line
                .error(
                    clap::error::ErrorKind::InvalidSubcommand,
                    format!("unrecognized subcommand '{name}'"),
                )
                .exit()
        }
        Some(c) => c,
        // `Artemis.run`: the shell, by default.
        None => {
            logging::init_utility(client::instance_etc().as_deref());
            shell::run(true, None).await;
            return std::process::ExitCode::SUCCESS;
        }
    };
    if matches!(command, Command::Run { .. }) {
        // The broker logs as Artemis does, once its log4j2.properties is read.
        logging::init_broker();
    } else {
        // bin/artemis runs every other command with artemis-utility.profile.
        logging::init_utility(client::instance_etc().as_deref());
    }
    if let Command::Shell(args) = command {
        shell::shell(args).await;
        return std::process::ExitCode::SUCCESS;
    }
    if execute(command).await {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}

/// Runs a command and reports its failure as `Artemis.execute` does; false
/// when it failed.
async fn execute(command: Command) -> bool {
    // `run` adds its own -D options.
    if !matches!(command, Command::Run { .. }) {
        apply_discovery_switch(&java_args_system_properties());
    }
    if let Command::Help { args } = &command {
        print!(
            "{}",
            help::help("saranga", args, help_mode(), shell::in_shell())
        );
        return true;
    }
    let client = matches!(
        command,
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
            | Command::Version { .. }
            | Command::Pwd { .. }
            | Command::Create(_)
            | Command::Upgrade(_)
            | Command::Completion { .. }
            | Command::Connect(_)
            | Command::Disconnect(_)
    );
    if client {
        return match run_client(command).await {
            Ok(()) => true,
            Err(e) => {
                report_client_error(&e);
                false
            }
        };
    }
    match run_command(command).await {
        Ok(()) => true,
        Err(e)
            if e.downcast_ref::<client::UsageError>().is_some()
                || e.downcast_ref::<client::CliError>().is_some()
                || e.downcast_ref::<client::ConfigurationError>().is_some() =>
        {
            report_client_error(&e);
            false
        }
        Err(e) => {
            eprintln!("Error: {e:?}");
            false
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
            ..
        } => control_file(broker, configuration, "STOP_ME"),
        Command::Kill {
            broker,
            configuration,
            ..
        } => control_file(broker, configuration, "KILL_ME"),
        Command::Create(a) => create::run(*a),
        Command::Upgrade(a) => upgrade::run(&a),
        Command::Connect(a) => shell::connect(a).await,
        Command::Disconnect(a) => shell::disconnect(a),
        Command::Completion { start_script } => {
            print!("{}", completion::script(&start_script));
            Ok(())
        }
        Command::Version { .. } => {
            // PrintVersion
            println!(
                "Saranga {VERSION} (Apache Artemis {})",
                saranga_server::management::VERSION
            );
            println!("Saranga home: {}", broker_home());
            println!("Saranga instance: {}", broker_instance());
            Ok(())
        }
        Command::Pwd { .. } => {
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
    create::saranga_home()
        .map(|h| dunce_path(&h))
        .unwrap_or_else(|_| "null".to_owned())
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
        print!(
            "{}",
            help::help(
                "saranga",
                &["help".to_owned()],
                help::Mode::Home,
                shell::in_shell()
            )
        );
    } else if let Some(c) = e.downcast_ref::<client::CliError>() {
        eprintln!("{c}");
    } else if let Some(c) = e.downcast_ref::<client::ConfigurationError>() {
        eprintln!("{c}");
        println!();
        println!(
            "Configuration should be specified as 'scheme:location'. Default configuration is 'xml:${{ARTEMIS_INSTANCE}}/etc/bootstrap.xml'"
        );
    } else if let Some(c) = e.downcast_ref::<client::jms::ConnectFailed>() {
        eprintln!("Connection failed::{c}");
    } else if let Some(c) = e.downcast_ref::<saranga_client::Error>() {
        eprintln!("{}", client::jms::message(c));
    } else {
        eprintln!("{e:#}");
    }
}

/// `--broker`: the broker.xml, a `file:` URI or a path.
fn broker_path(broker: &str) -> PathBuf {
    saranga_config::file_uri_path(broker)
        .unwrap_or_else(|| PathBuf::from(broker.trim_start_matches("file:")))
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

/// `ServerLocatorConfig.isDiscoveryEnabled`: the `artemis.discovery.enabled`
/// system property, when given, over `ARTEMIS_DISCOVERY_ENABLED`.
fn apply_discovery_switch(system: &[(String, String)]) {
    use saranga_client::discovery::{DISCOVERY_ENABLED_PROPERTY, set_discovery_enabled};
    if let Some((_, v)) = system
        .iter()
        .rev()
        .find(|(k, _)| k == DISCOVERY_ENABLED_PROPERTY)
    {
        // `Boolean.parseBoolean`.
        set_discovery_enabled(v.eq_ignore_ascii_case("true"));
    }
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
            broker,
            configuration,
            ..
        } => {
            let mut system = java_args_system_properties();
            system.extend(system_properties);
            apply_discovery_switch(&system);
            let options = instance::Options {
                system,
                broker_properties: properties,
            };
            // Without either, the instance whose bin/artemis runs this
            // (ARTEMIS_INSTANCE), if any.
            let etc = etc.or_else(client::instance_etc);
            let bootstrap = configuration
                .filter(|c| !c.is_empty())
                .map(|c| instance::bootstrap_path(&c))
                .transpose()?;
            let mut instance = match (config, etc, bootstrap) {
                (Some(c), _, _) => Some(instance::Instance::from_broker_xml(&c, options)?),
                (None, etc, Some(b)) => {
                    // Outside an instance, the bootstrap file's directory.
                    let etc = etc
                        .or_else(|| b.parent().map(Path::to_path_buf))
                        .unwrap_or_default();
                    Some(instance::Instance::from_bootstrap(&etc, &b, options)?)
                }
                (None, Some(e), None) => Some(instance::Instance::from_etc(&e, options)?),
                (None, None, None) => None,
            };
            if let (Some(i), Some(b)) = (&mut instance, broker) {
                i.broker_xml = broker_path(&b);
            }
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
        Command::Activation(cmd) => activation::run(cmd).await,
        Command::Mask {
            password,
            hash,
            key,
            password_codec,
            config,
            ..
        } => mask(&password, hash, key, password_codec, config),
        _ => unreachable!("a client command"),
    }
}

/// The protocols besides Core that `saranga run` serves.
const SERVED_PROTOCOLS: [&str; 6] = ["CORE", "HORNETQ", "STOMP", "MQTT", "AMQP", "OPENWIRE"];

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
        broker_connections: file
            .map(|f| f.broker_connections.clone())
            .unwrap_or_default(),
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
    current: Arc<saranga_server::Current>,
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
                Ok(reload) => current.get().reload(reload).await,
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

/// `artemis mask` (`Mask.execute`).
/// `Mask.execute`: with `--password-codec`, the codec of the instance's
/// broker configuration (or of saranga's `--config`), else the default
/// codec with `--hash` and `--key`.
fn mask(
    password: &str,
    hash: bool,
    key: Option<String>,
    password_codec: bool,
    config: Option<PathBuf>,
) -> anyhow::Result<()> {
    use saranga_config::mask::Codec;
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
    let codec = if password_codec {
        let instance = match (config, client::instance_etc()) {
            (Some(path), _) => instance::Instance::from_broker_xml(&path, Default::default())?,
            (None, Some(etc)) => instance::Instance::from_etc(&etc, Default::default())?,
            (None, None) => {
                // `getBrokerConfiguration` outside an instance reads
                // null/etc/broker.xml, which is not there.
                let path = std::env::current_dir()
                    .unwrap_or_default()
                    .join("null")
                    .join("etc")
                    .join("broker.xml");
                anyhow::bail!(
                    "java.io.FileNotFoundException: {} ({})",
                    path.display(),
                    if cfg!(windows) {
                        "The system cannot find the path specified"
                    } else {
                        "No such file or directory"
                    }
                );
            }
        };
        let file = instance.broker_config()?;
        Codec::from_desc(Some(&file.password_codec))?
    } else {
        Codec::init(&params, |n| std::env::var(n).ok())?
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

/// The `-D` options of `JAVA_ARGS` and `JAVA_ARGS_APPEND`, which an
/// instance's bin/artemis exports from its profile, in the order Artemis's
/// passes them to Java, so that the command line's come after them and win.
/// Outside an instance (no `ARTEMIS_INSTANCE`), none.
fn java_args_system_properties() -> Vec<(String, String)> {
    if std::env::var_os("ARTEMIS_INSTANCE").is_none() {
        return Vec::new();
    }
    ["JAVA_ARGS", "JAVA_ARGS_APPEND"]
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .flat_map(|args| {
            args.split_whitespace()
                .filter_map(|a| {
                    let d = a.trim_matches('"').strip_prefix("-D")?;
                    parse_system_property(d).ok()
                })
                .collect::<Vec<_>>()
        })
        .collect()
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

/// A broker `run` started, with what stopping it needs.
struct Started {
    broker: Arc<Broker>,
    at: std::time::Instant,
    file: Option<saranga_config::BrokerConfig>,
    reload_period: std::time::Duration,
    /// A shared-store backup that took over from its primary.
    backup: bool,
    /// Where `server.lock` is, with persistence.
    lock_dir: Option<PathBuf>,
}

/// `ActiveMQServerImpl.internalStart` as `run` does it: the configuration
/// read, the broker started and its acceptors bound. `as_backup`: a
/// replicating broker that handed back to its restarted primary starts
/// again as its backup, whatever its configured role.
async fn boot(
    instance: Option<&instance::Instance>,
    bind: Option<SocketAddr>,
    as_backup: bool,
) -> anyhow::Result<Started> {
    let at = std::time::Instant::now();
    *BOOT_STARTED.lock().unwrap_or_else(|e| e.into_inner()) = Some(at);
    let filters = Arc::new(SelectorFilters);
    let served = SERVED_PROTOCOLS;
    let mut file_config = None;
    let (mut broker_config, mut binds, reload_period) = match instance {
        Some(instance) => {
            let file = instance.broker_config()?;
            tracing::info!(
                "AMQ221000: {} message broker is starting with configuration Broker Configuration (clustered={},journalDirectory={},bindingsDirectory={},largeMessagesDirectory={},pagingDirectory={})",
                if file.ha_policy.is_backup() || as_backup {
                    "Backup"
                } else {
                    "Primary"
                },
                !file.cluster_connections.is_empty(),
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
    tracing::info!(
        "AMQ221043: Protocol module found: [saranga-server]. Adding protocol support for: HORNETQ"
    );
    for p in &broker_config.protocols {
        tracing::info!(
            "AMQ221043: Protocol module found: [saranga-protocol-{}]. Adding protocol support for: {}",
            p.name().to_ascii_lowercase(),
            p.name()
        );
    }
    let lock_dir = broker_config.storage.as_ref().map(|storage| {
        broker_config
            .ha
            .lock_directory
            .clone()
            .unwrap_or_else(|| storage.messages.directory.clone())
    });
    // Replication with a lock manager: the manager decides which broker is
    // active on the node's data.
    if let (Some(storage), Some(dir), Some(file)) =
        (&broker_config.storage, &lock_dir, file_config.as_ref())
        && let Some(policy) = file.ha_policy.replication().filter(|r| r.manager.is_some())
    {
        let m = policy.manager.as_ref().expect("a manager");
        let manager = saranga_ha::lockmanager::LockManager::new(&m.class_name, &m.properties)
            .map_err(|e| anyhow::anyhow!("AMQ224000: Failure in initialisation: {e}"))?;
        let Some(cluster) = broker_config.cluster().cloned() else {
            anyhow::bail!("a replicating broker needs a cluster-connection to find its peer");
        };
        let primary = !file.ha_policy.is_backup();
        let activated = saranga_server::replication::pluggable::activate(
            saranga_server::replication::pluggable::Activation {
                name: broker_config.management.name.clone(),
                manager,
                lock_dir: dir.clone(),
                primary,
                as_backup,
                coordination_id: policy.coordination_id.clone(),
                backup: saranga_server::replication::backup::BackupConfig {
                    name: broker_config.management.name.clone(),
                    policy: policy.clone(),
                    cluster,
                    messages: storage.messages.clone(),
                    bindings: storage.bindings.clone(),
                    paging: storage.paging.clone(),
                    large_messages: storage.large_messages.clone(),
                    fail_back: false,
                    pluggable: None,
                },
                version: VERSION.to_owned(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("AMQ224000: Failure in initialisation: {e}"))?;
        if activated.from_backup {
            tracing::info!(
                "AMQ221037: Saranga::name={} to become 'active'",
                broker_config.management.name
            );
        }
        broker_config.ha.coordination = Some(activated.coordination);
        // A backup is active with its own `allow-failback`; a primary
        // never hands back.
        broker_config.ha.policy = saranga_config::HaPolicy::ReplicationPrimary(policy.clone());
    }
    // A replicating backup copies its primary until the primary fails, then
    // starts as a replicating primary on its copy, with the primary's node ID.
    if broker_config.ha.coordination.is_none()
        && let (Some(storage), Some(dir), Some(file)) =
            (&broker_config.storage, &lock_dir, file_config.as_ref())
        && let Some((backup_policy, fail_back, active_policy)) =
            replica_role(&file.ha_policy, as_backup, dir, broker_config.cluster()).await?
    {
        let Some(cluster) = broker_config.cluster().cloned() else {
            anyhow::bail!("a replicating backup needs a cluster-connection to find its primary");
        };
        // The node manager's start: `server.lock` is there before the data
        // is moved away.
        let d = dir.clone();
        tokio::task::spawn_blocking(move || saranga_server::node::NodeManager::open(&d, None))
            .await?
            .context("the backup's server.lock")?;
        let node_id = saranga_server::replication::backup::replicate(
            saranga_server::replication::backup::BackupConfig {
                name: broker_config.management.name.clone(),
                policy: backup_policy,
                cluster,
                messages: storage.messages.clone(),
                bindings: storage.bindings.clone(),
                paging: storage.paging.clone(),
                large_messages: storage.large_messages.clone(),
                fail_back,
                pluggable: None,
            },
            VERSION,
        )
        .await?;
        let d = dir.clone();
        let id = node_id.clone();
        tokio::task::spawn_blocking(move || saranga_server::node::write_node_id(&d, &id))
            .await?
            .context("writing the primary's node ID to server.lock")?;
        tracing::info!(
            "AMQ221037: Saranga::name={} to become 'active'",
            broker_config.management.name
        );
        broker_config.ha.policy = saranga_config::HaPolicy::ReplicationPrimary(active_policy);
    }
    let backup = match (&lock_dir, &file_config) {
        (Some(dir), Some(file))
            if matches!(
                file.ha_policy,
                saranga_config::HaPolicy::SharedStoreBackup { .. }
            ) =>
        {
            Some(
                await_primary(
                    dir.clone(),
                    broker_config.ha.lock_acquisition_timeout,
                    broker_config.cluster().cloned(),
                )
                .await?,
            )
        }
        _ => None,
    };
    let broker = Broker::start_activated(broker_config, backup.clone()).await?;
    for Bind {
        name,
        address,
        acceptor,
    } in binds
    {
        if let Some(tls) = &acceptor.tls {
            tls.spawn_reloader(reload_period);
        }
        if let Err(e) = broker
            .add_acceptor(&name, &address, acceptor)
            .await
            .with_context(|| format!("acceptor {name} on {address}"))
        {
            // What did start stops again, so that a later start can bind.
            let _ = stop_broker(&broker, None).await;
            return Err(e);
        }
    }
    if let Some(n) = &backup {
        n.release_backup();
    }
    Ok(Started {
        broker,
        at,
        file: file_config,
        reload_period,
        backup: backup.is_some(),
        lock_dir,
    })
}

/// Whether this start replicates another broker's data before it is
/// active: the policy it replicates with, whether it asks for failback,
/// and the policy it is then active with. A configured backup
/// (`ReplicaPolicy`) is active with `check-for-active-server` off and its
/// own `allow-failback`. A primary is a backup when it starts again after
/// handing back, or, with `check-for-active-server`, when a broker of the
/// cluster is active with its node ID (`isNodeIdUsed`); it asks that broker
/// to hand back, and is active again with its own policy.
async fn replica_role(
    policy: &saranga_config::HaPolicy,
    as_backup: bool,
    dir: &Path,
    cluster: Option<&saranga_server::cluster::ClusterConfig>,
) -> anyhow::Result<
    Option<(
        saranga_config::Replication,
        bool,
        saranga_config::Replication,
    )>,
> {
    use saranga_config::HaPolicy;
    let primary = match policy {
        HaPolicy::ReplicationBackup(p) => {
            let active = saranga_config::Replication {
                check_for_active_server: false,
                ..p.clone()
            };
            return Ok(Some((p.clone(), false, active)));
        }
        HaPolicy::ReplicationPrimary(p) => p,
        _ => return Ok(None),
    };
    // `ReplicatedPolicy.getReplicaPolicy`.
    let replica = saranga_config::Replication {
        allow_failback: false,
        restart_backup: true,
        ..primary.clone()
    };
    if as_backup {
        return Ok(Some((replica, false, primary.clone())));
    }
    let Some(cluster) = cluster.filter(|_| primary.check_for_active_server) else {
        return Ok(None);
    };
    let d = dir.to_path_buf();
    let node_id = tokio::task::spawn_blocking(move || {
        saranga_server::node::NodeManager::open(&d, None).map(|n| n.node_id())
    })
    .await?
    .context("the primary's server.lock")?;
    if saranga_server::replication::backup::is_node_id_used(cluster, &node_id).await {
        Ok(Some((replica, true, primary.clone())))
    } else {
        Ok(None)
    }
}

/// `SharedStoreBackupActivation.run` up to the activation: the backup lock,
/// then the primary's, once the primary has failed. Meanwhile the backup
/// announces itself through the cluster connection, if there is one
/// (`BackupManager`), so the primary's clients know where to fail over.
async fn await_primary(
    dir: PathBuf,
    timeout: Option<std::time::Duration>,
    cluster: Option<saranga_server::cluster::ClusterConfig>,
) -> anyhow::Result<Arc<NodeManager>> {
    let n = tokio::task::spawn_blocking(move || {
        let n = NodeManager::open(&dir, timeout)?;
        n.start_backup()?;
        std::io::Result::Ok(Arc::new(n))
    })
    .await?
    .context("the backup's wait for its primary")?;
    let announcer =
        cluster.map(|c| saranga_server::cluster::BackupAnnouncer::start(c, n.node_id()));
    tracing::info!(
        "AMQ221109: Saranga Backup Server version {VERSION} [{}] started; waiting for primary to fail before activating",
        n.node_id()
    );
    let n2 = n.clone();
    let waited = tokio::task::spawn_blocking(move || n2.await_primary()).await;
    // `BackupManager.activated`: the connectors are no longer needed.
    if let Some(a) = announcer {
        a.close().await;
    }
    waited?.context("the backup's wait for its primary")?;
    Ok(n)
}

/// After failing back: the primary is waited for to be active
/// (`awaitActiveStatus`) before this broker waits as its backup again.
async fn await_active_primary(dir: PathBuf) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        let n = NodeManager::open(&dir, None)?;
        n.await_active_status()
    })
    .await?
    .context("waiting for the primary to become active")
}

/// The broker is active.
fn announce(instance: Option<&Arc<instance::Instance>>, started: &Started) {
    let broker = &started.broker;
    if started.backup {
        tracing::info!("AMQ221010: Backup Server is now active");
        if let Some(saranga_config::HaPolicy::SharedStoreBackup {
            allow_failback: true,
            ..
        }) = started.file.as_ref().map(|f| &f.ha_policy)
        {
            broker.start_failback_checker();
        }
    } else {
        tracing::info!("AMQ221007: Server is now active");
    }
    tracing::info!(
        "AMQ221001: Saranga Message Broker version {VERSION} [{}, nodeID={}] ",
        broker.management().config().name,
        broker.node_id()
    );
    if let Some(instance) = instance {
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
}

/// `ActiveMQServerImpl.stop`: the broker's services, then no new
/// connections, the clients given `graceful-shutdown-timeout` when graceful
/// shutdown is enabled, the rest closed, then the broker stopped.
async fn stop_broker(
    broker: &Arc<Broker>,
    file: Option<&saranga_config::BrokerConfig>,
) -> anyhow::Result<()> {
    // The grouping handler, federation, cluster manager and protocol
    // services go first, while the connections are still open.
    broker.stop_services();
    // RemotingService.pauseAcceptors: no new connections.
    broker.pause_acceptors().await;
    if let Some(timeout) = file
        .filter(|c| c.graceful_shutdown_enabled)
        .map(|c| c.graceful_shutdown_timeout)
    {
        wait_for_clients(broker, timeout).await;
    }
    close_connections(broker).await;
    broker.stop()?;
    // `close`: the lock manager stopped, its lock with it.
    if let Some(c) = broker.coordination() {
        c.manager.stop().await;
    }
    Ok(())
}

/// `reloadNetworkHealthCheck`: the network check of the configuration, if
/// it has one, checked now and then every period. The receiver holds
/// whether the network is healthy.
async fn network_monitor(
    file: saranga_config::BrokerConfig,
) -> Option<tokio::sync::watch::Receiver<bool>> {
    let (check, healthy) = tokio::task::spawn_blocking(move || {
        network::NetworkCheck::from_config(&file).map(|c| {
            let healthy = c.check();
            (Arc::new(c), healthy)
        })
    })
    .await
    .ok()
    .flatten()?;
    let (tx, rx) = tokio::sync::watch::channel(healthy);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(check.period()).await;
            let c = check.clone();
            let Ok(healthy) = tokio::task::spawn_blocking(move || c.check()).await else {
                break;
            };
            if tx.send(healthy).is_err() {
                break;
            }
        }
    });
    Some(rx)
}

async fn run(
    instance: Option<instance::Instance>,
    bind: Option<SocketAddr>,
    allow_kill: bool,
) -> anyhow::Result<()> {
    tracing::info!("AMQ101000: Starting Saranga Server version {VERSION}");
    let instance = instance.map(Arc::new);
    let stop_dir = instance
        .as_ref()
        .and_then(|i| i.broker_xml.parent().map(Path::to_path_buf));
    let stop_files = async {
        match stop_dir {
            Some(dir) => watch_stop_files(dir, allow_kill).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(stop_files);
    // A backup waits here for its primary to fail, and a primary for the
    // lock another broker holds; a stop ends the wait.
    let started = tokio::select! {
        s = boot(instance.as_deref(), bind, false) => s?,
        _ = tokio::signal::ctrl_c() => {
            stopped_waiting();
            return Ok(());
        }
        _ = terminated() => {
            stopped_waiting();
            return Ok(());
        }
        s = &mut stop_files => {
            if matches!(s, Stop::Halt) {
                saranga_server::node::interrupt_waits();
                eprintln!("Halting by user request");
                std::process::exit(0);
            }
            stopped_waiting();
            return Ok(());
        }
    };
    let current = saranga_server::Current::new(started.broker.clone());
    let mut web_started = false;
    if let (Some(instance), Some(file)) = (&instance, &started.file)
        && let Some(mut web) = instance.bootstrap.as_ref().and_then(|b| b.web.clone())
    {
        for w in web.apply_system(&instance.system) {
            tracing::warn!("{w}");
        }
        web::start(instance, file, &web, &current).await?;
        web_started = true;
    }
    announce(instance.as_ref(), &started);
    if let Some(instance) = &instance {
        spawn_config_reloader(instance.clone(), current.clone(), started.reload_period);
    }
    let mut network = match &started.file {
        Some(file) => network_monitor(file.clone()).await,
        None => None,
    };
    let name = started.broker.management().config().name.clone();
    let mut running = Some(started);
    let stop = loop {
        let is_running = running.is_some();
        let broker = current.get();
        // The network check stops a running broker while the network is
        // unhealthy, and starts the one it stopped once it is healthy.
        let network_changed = async {
            let changed = match network.as_mut() {
                Some(rx) => rx.wait_for(|h| *h != is_running).await.is_ok(),
                None => false,
            };
            if !changed {
                std::future::pending::<()>().await;
            }
        };
        let failback = tokio::select! {
            _ = tokio::signal::ctrl_c() => break Stop::Clean,
            _ = terminated() => break Stop::Clean,
            // The critical analyzer's SHUTDOWN policy.
            _ = broker.shutdown_requested(), if is_running => break Stop::Clean,
            s = &mut stop_files => break s,
            _ = network_changed => false,
            _ = broker.failback_requested(), if is_running => true,
        };
        if failback {
            // The FailbackChecker's restart: stopped as if failed, so the
            // primary takes the lock; once it is active, a backup again.
            // A replicating broker hands back to the primary it replicates
            // to, and replicates it again at once.
            let Some(s) = running.take() else { continue };
            stop_broker(&s.broker, s.file.as_ref()).await?;
            stopped(&s);
            let replicating = s
                .file
                .as_ref()
                .is_some_and(|f| f.ha_policy.replication().is_some());
            let restart = replicating
                || matches!(
                    s.file.as_ref().map(|f| &f.ha_policy),
                    Some(saranga_config::HaPolicy::SharedStoreBackup {
                        restart_backup: true,
                        ..
                    })
                );
            let Some(dir) = s.lock_dir.clone() else {
                continue;
            };
            if let Some(c) = s.broker.coordination() {
                c.await_failback().await;
            }
            if replicating {
                tracing::info!(
                    "AMQ221039: Restarting as replicating backup server after primary restart"
                );
            }
            let rebooted = tokio::select! {
                r = async {
                    if !replicating {
                        await_active_primary(dir).await?;
                    }
                    if !restart {
                        return Ok(None);
                    }
                    boot(instance.as_deref(), bind, replicating).await.map(Some)
                } => r?,
                _ = tokio::signal::ctrl_c() => {
                    stopped_waiting();
                    break Stop::Clean;
                }
                _ = terminated() => {
                    stopped_waiting();
                    break Stop::Clean;
                }
                s = &mut stop_files => {
                    if matches!(s, Stop::Halt) {
                        saranga_server::node::interrupt_waits();
                    } else {
                        stopped_waiting();
                    }
                    break s;
                }
            };
            match rebooted {
                Some(s) => {
                    current.set(s.broker.clone());
                    announce(instance.as_ref(), &s);
                    running = Some(s);
                }
                None => break Stop::Clean,
            }
            continue;
        }
        match running.take() {
            Some(s) => {
                tracing::warn!(
                    "AMQ201001: Network is unhealthy, stopping service ActiveMQServerImpl::name={name}"
                );
                stop_broker(&s.broker, s.file.as_ref()).await?;
                stopped(&s);
            }
            None => {
                tracing::info!(
                    "AMQ201000: Network is healthy, starting service ActiveMQServerImpl::name={name}"
                );
                match boot(instance.as_deref(), bind, false).await {
                    Ok(s) => {
                        current.set(s.broker.clone());
                        announce(instance.as_ref(), &s);
                        running = Some(s);
                    }
                    Err(e) => {
                        // Not started again until the next run, as
                        // NetworkHealthCheck forgets it stopped it.
                        tracing::warn!(
                            "AMQ202004: Error starting component ActiveMQServerImpl::name={name} : {e:#}"
                        );
                        network = None;
                    }
                }
            }
        }
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
            if let Some(s) = running {
                stop_broker(&s.broker, s.file.as_ref()).await?;
                stopped(&s);
                // A critical I/O error (AMQ222010): stopped as Artemis
                // stops, and the process ends as failed.
                if s.broker.is_critical_io_failed() {
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}

/// When the latest start began (`ActiveMQServerImpl.startDate`).
static BOOT_STARTED: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

/// AMQ221002 for a broker stopped while it waits for a lock: Artemis's
/// server has started, its activation waiting, so it reports stopping too.
fn stopped_waiting() {
    saranga_server::node::interrupt_waits();
    let at = *BOOT_STARTED.lock().unwrap_or_else(|e| e.into_inner());
    tracing::info!(
        "AMQ221002: Saranga Message Broker version {VERSION} [{}] stopped, uptime {}",
        saranga_server::node::last_node_id().unwrap_or_else(|| "null".to_owned()),
        at.map_or_else(
            || "not started".to_owned(),
            |at| saranga_server::management::print_duration(at.elapsed().as_millis() as i64)
        )
    );
}

/// AMQ221002, with the uptime of this start.
fn stopped(s: &Started) {
    tracing::info!(
        "AMQ221002: Saranga Message Broker version {VERSION} [{}] stopped, uptime {}",
        s.broker.node_id(),
        saranga_server::management::print_duration(s.at.elapsed().as_millis() as i64)
    );
}

/// `RemotingServiceImpl.stop`: the connections left are closed before the
/// rest of the broker stops.
async fn close_connections(broker: &saranga_server::Broker) {
    broker.close_unestablished();
    // `freeze`: Core clients are told the node is going. The backup's
    // connection stays for the broker's stop to end replication.
    for c in broker.registry().connections() {
        if !broker.is_backup_connection(&c) {
            c.disconnect();
        }
    }
    let closed = async {
        loop {
            let registry = broker.registry();
            let backups = registry
                .connections()
                .iter()
                .filter(|c| broker.is_backup_connection(c))
                .count();
            if registry.registered() <= backups {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), closed).await;
}

/// The graceful shutdown of `ActiveMQServerImpl.stop`: the clients
/// connected are given `timeout` milliseconds (-1: for ever) to disconnect.
async fn wait_for_clients(broker: &saranga_server::Broker, timeout: i64) {
    let disconnected = async {
        while !broker.registry().connections().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    if timeout < 0 {
        disconnected.await;
    } else {
        let limit = std::time::Duration::from_millis(timeout as u64);
        let _ = tokio::time::timeout(limit, disconnected).await;
    }
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

    /// `data exp log-interval=0`, as Artemis's declares it.
    #[test]
    fn dashless_export_options() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            super::artemis_spellings(args(&[
                "saranga",
                "data",
                "exp",
                "log-interval=0",
                "undefined-prefix",
                "X"
            ])),
            args(&[
                "saranga",
                "data",
                "exp",
                "--log-interval=0",
                "--undefined-prefix",
                "X"
            ])
        );
        let other = args(&["saranga", "data", "print", "log-interval=0"]);
        assert_eq!(super::artemis_spellings(other.clone()), other);
    }

    /// `artemis run`'s configuration URI, refused as `BrokerFactory`
    /// refuses it.
    #[test]
    fn bootstrap_uri() {
        let error = |uri: &str| {
            super::instance::bootstrap_path(uri)
                .unwrap_err()
                .to_string()
        };
        assert_eq!(
            error("../etc/bootstrap.xml"),
            "Invalid configuration URI, no scheme specified: ../etc/bootstrap.xml"
        );
        assert_eq!(
            error("foo:../etc/bootstrap.xml"),
            "Invalid configuration URI, can't find configuration scheme: foo"
        );
        assert_eq!(
            error("xml:../etc/missing.xml"),
            "Invalid configuration URI, can't find file: missing.xml"
        );
        let manifest = concat!("xml:", env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        assert!(super::instance::bootstrap_path(manifest).is_ok());
    }
}
