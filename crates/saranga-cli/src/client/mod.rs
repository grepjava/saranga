//! The commands that work against a running broker, as the Artemis CLI's
//! (`artemis queue`, `address`, `user`, `producer`, `consumer`, `browser`,
//! `transfer` and `check`): the same options, the same management
//! operations and messages, and the same output.
//!
//! The broker is reached over Core with `saranga-client`. Where the Artemis
//! CLI goes through the Core JMS client, [`jms`] does what that client does
//! on the wire.

use std::io::BufRead;
use std::path::PathBuf;

use anyhow::{Context, bail};
use saranga_client::{Connection, ConnectionOptions, Connector};
use saranga_transport::TlsConnector;

pub mod address;
pub mod check;
pub mod jms;
pub mod lock;
pub mod management;
pub mod messages;
pub mod queue;
pub mod user;

/// `ActionAbstract.DEFAULT_BROKER_URL`.
pub const DEFAULT_BROKER_URL: &str = "tcp://localhost:61616";
/// `ActionAbstract.DEFAULT_BROKER_ACCEPTOR`.
pub const DEFAULT_BROKER_ACCEPTOR: &str = "artemis";

/// The options every command has (`ActionAbstract`, `InputAbstract`).
#[derive(clap::Args, Clone, Debug, Default)]
pub struct Common {
    /// Print additional information.
    #[arg(long)]
    pub verbose: bool,
    /// Disable all the inputs, and make a best guess for any required
    /// input.
    #[arg(long)]
    pub silent: bool,
}

impl Common {
    /// `isSilentInput`: no prompts with `--silent`. The `artemis` script
    /// enables input, with a terminal or without.
    pub fn is_silent_input(&self) -> bool {
        self.silent
    }

    /// `InputAbstract.input`: the default with `--silent`, else a
    /// non-empty line, asked for as `InputReader.inputLoop` asks. Standard
    /// input ending fails as `Scanner.nextLine` does (`No line found`).
    pub fn input(
        &self,
        property: &str,
        prompt: &str,
        silent_default: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        if self.is_silent_input() {
            return Ok(silent_default.map(str::to_owned));
        }
        println!();
        loop {
            println!("{property}:");
            println!("{prompt}");
            let mut line = String::new();
            if std::io::stdin().lock().read_line(&mut line).unwrap_or(0) == 0 {
                bail!(UsageError("No line found".to_owned()));
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if !line.is_empty() {
                return Ok(Some(line.trim().to_owned()));
            }
        }
    }

    /// `InputAbstract.inputBoolean`.
    pub fn input_boolean(
        &self,
        property: &str,
        prompt: &str,
        silent_default: bool,
    ) -> anyhow::Result<bool> {
        if self.is_silent_input() {
            return Ok(silent_default);
        }
        loop {
            let value = self
                .input(
                    property,
                    &format!("{prompt}, valid values are Y, N, True, False"),
                    Some(&silent_default.to_string()),
                )?
                .unwrap_or_default();
            match value.trim().to_uppercase().as_str() {
                "TRUE" | "Y" => return Ok(true),
                "FALSE" | "N" => return Ok(false),
                _ => {}
            }
        }
    }

    /// `InputAbstract.inputPassword`: mandatory, and an error with
    /// `--silent`.
    pub fn input_password(&self, property: &str, prompt: &str) -> anyhow::Result<String> {
        if self.is_silent_input() {
            bail!(UsageError(format!(
                "Must specify {property} when using --silent option"
            )));
        }
        println!();
        loop {
            println!("{property}: is mandatory with this configuration:");
            println!("{prompt}");
            let Some(line) = read_password() else {
                println!("Invalid Entry!");
                bail!("no input");
            };
            let line = line.trim();
            if line.is_empty() {
                println!("Invalid Entry!");
            } else {
                return Ok(line.to_owned());
            }
        }
    }
}

/// A line from the terminal, not echoed (`Console.readPassword`).
fn read_password() -> Option<String> {
    let _echo = EchoOff::new();
    let mut line = String::new();
    let n = std::io::stdin().lock().read_line(&mut line).ok()?;
    println!();
    (n > 0).then(|| line.trim_end_matches(['\r', '\n']).to_owned())
}

/// Terminal echo off until dropped.
struct EchoOff {
    #[cfg(unix)]
    saved: Option<libc::termios>,
    #[cfg(windows)]
    saved: Option<u32>,
}

impl EchoOff {
    #[cfg(unix)]
    fn new() -> Self {
        // SAFETY: tcgetattr and tcsetattr on stdin with a termios this
        // function owns.
        unsafe {
            let mut t: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(libc::STDIN_FILENO, &mut t) != 0 {
                return Self { saved: None };
            }
            let saved = t;
            t.c_lflag &= !libc::ECHO;
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t);
            Self { saved: Some(saved) }
        }
    }

    #[cfg(windows)]
    fn new() -> Self {
        use windows_sys::Win32::System::Console::{
            ENABLE_ECHO_INPUT, GetConsoleMode, GetStdHandle, STD_INPUT_HANDLE, SetConsoleMode,
        };
        // SAFETY: console mode calls on this process's stdin handle.
        unsafe {
            let handle = GetStdHandle(STD_INPUT_HANDLE);
            let mut mode = 0u32;
            if GetConsoleMode(handle, &mut mode) == 0 {
                return Self { saved: None };
            }
            SetConsoleMode(handle, mode & !ENABLE_ECHO_INPUT);
            Self { saved: Some(mode) }
        }
    }

    #[cfg(not(any(unix, windows)))]
    fn new() -> Self {
        Self {}
    }
}

impl Drop for EchoOff {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(t) = &self.saved {
            // SAFETY: restores the attributes read in `new`.
            unsafe {
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, t);
            }
        }
        #[cfg(windows)]
        if let Some(mode) = self.saved {
            use windows_sys::Win32::System::Console::{
                GetStdHandle, STD_INPUT_HANDLE, SetConsoleMode,
            };
            // SAFETY: restores the mode read in `new`.
            unsafe {
                SetConsoleMode(GetStdHandle(STD_INPUT_HANDLE), mode);
            }
        }
    }
}

/// An error in the command's arguments, reported as Artemis reports a
/// `RuntimeException`: the message, then the usage hint.
#[derive(Debug)]
pub struct UsageError(pub String);

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UsageError {}

/// A `CLIException`: its message alone, and exit status 1.
#[derive(Debug)]
pub struct CliError(pub String);

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CliError {}

/// A `ConfigurationException`: its message, then how a configuration URI
/// is written, and exit status 1.
#[derive(Debug)]
pub struct ConfigurationError(pub String);

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigurationError {}

/// The connection options (`ConnectionConfigurationAbtract`,
/// `ConnectionAbstract`).
#[derive(clap::Args, Clone, Debug, Default)]
pub struct ConnectionArgs {
    #[command(flatten)]
    pub common: Common,
    /// Connection URL. Default: build URL from the 'artemis' acceptor
    /// defined in the broker.xml or tcp://localhost:61616 if the acceptor
    /// cannot be parsed.
    #[arg(long)]
    pub url: Option<String>,
    /// Name used to find the default connection URL on the acceptor list.
    #[arg(long)]
    pub acceptor: Option<String>,
    /// User used to connect.
    #[arg(long)]
    pub user: Option<String>,
    /// Password used to connect.
    #[arg(long)]
    pub password: Option<String>,
    /// ClientID set on the connection.
    #[arg(long = "clientID")]
    pub client_id: Option<String>,
    /// Protocol used. Valid values are CORE (AMQP is not supported by
    /// saranga's CLI).
    #[arg(long, default_value = "CORE")]
    pub protocol: String,
}

impl ConnectionArgs {
    /// `ConnectionConfigurationAbtract.execute`: the URL, from the
    /// instance's acceptor without `--url`, printed unless `quiet`.
    pub fn resolve(&mut self, quiet: bool) -> anyhow::Result<()> {
        crate::shell::recover(self);
        if !self.protocol.eq_ignore_ascii_case("core") {
            bail!(UsageError(format!(
                "protocol {} not supported",
                self.protocol.to_uppercase()
            )));
        }
        if self.url.is_none() {
            self.url = Some(instance_url(self.acceptor.as_deref()));
        }
        if !quiet {
            println!("Connection brokerURL = {}", self.url());
        }
        Ok(())
    }

    pub fn url(&self) -> &str {
        self.url.as_deref().unwrap_or(DEFAULT_BROKER_URL)
    }

    /// A Core connection with this user and password, or the shell's.
    pub async fn connect(&self) -> anyhow::Result<jms::JmsConnection> {
        let mut args = self.clone();
        crate::shell::recover(&mut args);
        let conn = jms::JmsConnection::connect(
            args.url(),
            args.user.clone(),
            args.password.clone(),
            args.client_id.clone(),
        )
        .await?;
        crate::shell::save_connection(args.url(), args.user.as_deref(), args.password.as_deref());
        Ok(conn)
    }

    /// `createCoreConnectionFactory`'s `tryConnect`: a connection made and
    /// closed at once. When it fails, `Connection failed::<reason>`, then
    /// (unless the broker refused the user) a URL for the retry, and the
    /// user and password when there are none, are asked for; the retry's
    /// failure is ignored, and the command goes on with its own URL and
    /// the user and password given.
    pub async fn try_connect(&mut self) -> anyhow::Result<()> {
        crate::shell::recover(self);
        let failed = match self.connect().await {
            Ok(conn) => {
                conn.close().await;
                return Ok(());
            }
            Err(e) => match e.downcast::<jms::ConnectFailed>() {
                Ok(f) => f,
                // The transport did not connect (`ActiveMQNotConnectedException`).
                Err(_) => jms::ConnectFailed {
                    reason:
                        "AMQ219007: Cannot connect to server(s). Tried with all available servers."
                            .to_owned(),
                    security: false,
                },
            },
        };
        eprintln!("Connection failed::{failed}");
        let mut retry = self.clone();
        if !failed.security {
            let url = self.common.input(
                "--url",
                "Type in the connection URL for a retry (e.g. tcp://localhost:61616)",
                Some(self.url()),
            )?;
            retry.url = url.or(retry.url);
        }
        if self.user.is_none() {
            self.user = self
                .common
                .input("--user", "Type the username for a retry", None)?;
        }
        if !self.common.is_silent_input() && self.password.is_none() {
            self.password = Some(
                self.common
                    .input_password("--password", "Type the password for a retry")?,
            );
        }
        retry.user = self.user.clone();
        retry.password = self.password.clone();
        if let Ok(conn) = retry.connect().await {
            conn.close().await;
        }
        Ok(())
    }

    /// `doManagement`'s connection and session: a session factory and one
    /// session, each with this user and password (after the factory's
    /// `tryConnect`).
    pub async fn core_session(&self) -> anyhow::Result<(Connection, saranga_client::Session)> {
        let mut args = self.clone();
        args.try_connect().await?;
        args.simple_session().await
    }

    /// `SimpleManagement.open`'s session factory and session, with this
    /// user and password or the shell's, and no `tryConnect`.
    pub async fn simple_session(&self) -> anyhow::Result<(Connection, saranga_client::Session)> {
        let mut args = self.clone();
        crate::shell::recover(&mut args);
        let conn = open(args.url(), args.user.as_deref(), args.password.as_deref()).await?;
        let options = saranga_client::SessionOptions {
            username: args.user.clone(),
            password: args.password.clone(),
            ..saranga_client::SessionOptions::default()
        };
        match conn.create_session(options).await {
            Ok(session) => Ok((conn, session)),
            Err(e) => {
                conn.close().await;
                Err(e.into())
            }
        }
    }
}

/// The broker instance the command belongs to: `ARTEMIS_INSTANCE`, as the
/// instance's `bin/artemis` (or `bin/saranga`) script sets it.
pub fn instance_dir() -> Option<PathBuf> {
    env_path("ARTEMIS_INSTANCE")
}

/// A path from the environment, without the quotes a Windows script's
/// `set NAME="path"` leaves on it.
fn env_path(name: &str) -> Option<PathBuf> {
    let v = std::env::var_os(name)?;
    let s = v.to_string_lossy();
    Some(
        match s.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            Some(inner) => PathBuf::from(inner),
            None => PathBuf::from(v),
        },
    )
}

/// The instance's `etc/`: `ARTEMIS_INSTANCE_ETC`, else `<instance>/etc`.
pub fn instance_etc() -> Option<PathBuf> {
    env_path("ARTEMIS_INSTANCE_ETC").or_else(|| instance_dir().map(|d| d.join("etc")))
}

/// `getBrokerURLInstance`: the URL of the instance's acceptor called
/// `name` (default `artemis`), else the default URL.
pub fn instance_url(name: Option<&str>) -> String {
    let name = name.unwrap_or(DEFAULT_BROKER_ACCEPTOR);
    let Some(etc) = instance_etc() else {
        return DEFAULT_BROKER_URL.to_owned();
    };
    let found = (|| -> anyhow::Result<Option<String>> {
        let instance =
            crate::instance::Instance::without_bootstrap(&etc, crate::instance::Options::default());
        // getBrokerConfiguration deploys only the core configuration.
        let config =
            instance.broker_config_validated(saranga_config::Validation::Deploy { jms: false })?;
        // `findTransportURI`: the acceptor of that name, else the connector.
        let Some(a) = config
            .acceptors
            .iter()
            .chain(&config.connectors)
            .find(|a| a.name.as_deref() == Some(name))
        else {
            return Ok(None);
        };
        let scheme = a.param("scheme").unwrap_or("tcp");
        let mut host = a.host.clone().unwrap_or_else(|| "localhost".to_owned());
        let bare = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        // `InetAddress.getByName(host).isAnyLocalAddress()`.
        let any_local = match bare.parse::<std::net::IpAddr>() {
            Ok(ip) => ip.is_unspecified(),
            Err(_) => std::net::ToSocketAddrs::to_socket_addrs(&(bare.as_str(), 0))
                .map_err(|e| anyhow::anyhow!("{bare}: {e}"))?
                .next()
                .is_some_and(|s| s.ip().is_unspecified()),
        };
        if any_local {
            host = "localhost".to_owned();
        } else if bare.contains(':') {
            // `new URI(scheme, null, host, port, ...)` brackets an IPv6 host.
            host = format!("[{bare}]");
        }
        Ok(Some(format!(
            "{scheme}://{host}:{}",
            a.port.unwrap_or(61616)
        )))
    })();
    match found {
        Ok(Some(url)) => url,
        Ok(None) => DEFAULT_BROKER_URL.to_owned(),
        Err(e) => {
            print!("Can not get the broker url instance: {e:#}");
            DEFAULT_BROKER_URL.to_owned()
        }
    }
}

/// A URL's servers and its `ServerLocator` parameters.
type ParsedUrl = (Vec<Connector>, Vec<(String, String)>);

/// Every server of a connection URL, `tcp://host:port?params` or
/// `(tcp://a:1,tcp://b:2)?params`, `amqp://` taken as `tcp://`, with the
/// parameters for the `ServerLocator`: those after the list, or the single
/// server's. A server's own parameters and then those after the list
/// give its TLS.
pub fn parse_url_list(url: &str) -> anyhow::Result<ParsedUrl> {
    let url = url.trim();
    let (list, locator_query) = match url.strip_prefix('(') {
        Some(rest) => {
            let (list, after) = rest
                .split_once(')')
                .with_context(|| format!("invalid URL {url}"))?;
            (list, after.strip_prefix('?').unwrap_or(""))
        }
        None => (url, url.split_once('?').map_or("", |(_, q)| q)),
    };
    let mut servers = Vec::new();
    for item in list.split(',') {
        let (scheme, rest) = item
            .split_once("://")
            .with_context(|| format!("invalid URL {url}"))?;
        if !matches!(scheme, "tcp" | "amqp") {
            bail!("schema {scheme} not found");
        }
        let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
        let authority = authority.trim_end_matches('/');
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') || authority.starts_with('[') => (
                h.to_owned(),
                p.parse()
                    .with_context(|| format!("invalid port in {url}"))?,
            ),
            _ => (authority.to_owned(), 61616),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let host = if host.is_empty() { "localhost" } else { host };
        let own = params(query)?;
        let shared = params(locator_query)?;
        let tls = TlsConnector::from_uri_params(|k| {
            own.iter()
                .chain(&shared)
                .find(|(name, _)| name == k)
                .map(|(_, v)| v.clone())
        });
        servers.push(Connector::new(host, port).with_tls(tls.map(std::sync::Arc::new)));
    }
    Ok((servers, params(locator_query)?))
}

/// A URL's parameters as `URISupport.parseQuery` reads them: split on `&`
/// and `;`, each name and value URL-decoded.
fn params(query: &str) -> anyhow::Result<Vec<(String, String)>> {
    Ok(saranga_config::query_params(query)
        .map_err(|e| anyhow::anyhow!(e))?
        .into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect())
}

/// The `ServerLocator` settings of a URL's parameters, as
/// `BeanSupport.setData` sets them: `ha`, `reconnectAttempts`,
/// `failoverAttempts`, `initialConnectAttempts`, `retryInterval`,
/// `retryIntervalMultiplier`, `maxRetryInterval`, `callTimeout`,
/// `callFailoverTimeout`, `clientFailureCheckPeriod` and `connectionTTL`.
/// Others are ignored.
pub fn locator_options(params: &[(String, String)], options: &mut ConnectionOptions) {
    use std::time::Duration;
    let millis = |v: &str| v.parse::<i64>().ok();
    let optional = |v: &str| millis(v).map(|m| (m >= 0).then(|| Duration::from_millis(m as u64)));
    for (k, v) in params {
        match k.as_str() {
            "ha" => options.ha = v.eq_ignore_ascii_case("true"),
            "reconnectAttempts" => options.reconnect_attempts = v.parse().unwrap_or(0),
            "failoverAttempts" => options.failover_attempts = v.parse().unwrap_or(0),
            "initialConnectAttempts" => options.initial_connect_attempts = v.parse().unwrap_or(1),
            "retryInterval" => {
                if let Some(m) = millis(v) {
                    options.retry_interval = Duration::from_millis(m.max(0) as u64);
                }
            }
            "retryIntervalMultiplier" => {
                if let Ok(m) = v.parse() {
                    options.retry_interval_multiplier = m;
                }
            }
            "maxRetryInterval" => {
                if let Some(m) = millis(v) {
                    options.max_retry_interval = Duration::from_millis(m.max(0) as u64);
                }
            }
            "callTimeout" => {
                if let Some(m) = millis(v) {
                    options.call_timeout = Duration::from_millis(m.max(0) as u64);
                }
            }
            "callFailoverTimeout" => {
                if let Some(t) = optional(v) {
                    options.call_failover_timeout = t;
                }
            }
            "clientFailureCheckPeriod" => {
                if let Some(t) = optional(v) {
                    options.client_failure_check_period = t;
                }
            }
            "connectionTTL" => {
                if let Some(t) = optional(v) {
                    options.connection_ttl = t;
                }
            }
            _ => {}
        }
    }
}

/// A `udp://group:port?params` URL's discovery group
/// (`UDPServerLocatorSchema`): `localBindAddress`, `localBindPort`,
/// `refreshTimeout` and `discoveryInitialWaitTimeout` among its parameters.
pub fn discovery_url(
    url: &str,
) -> anyhow::Result<Option<saranga_client::discovery::DiscoveryConfig>> {
    use saranga_client::discovery::{DiscoveryConfig, EndpointConfig};
    use std::time::Duration;
    let Some(rest) = url.trim().strip_prefix("udp://") else {
        return Ok(None);
    };
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    let authority = authority.trim_end_matches('/');
    let (host, port) = authority
        .rsplit_once(':')
        .with_context(|| format!("invalid URL {url}: no group port"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("invalid port in {url}"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let mut config = DiscoveryConfig::new(EndpointConfig::new(host, port));
    for (k, v) in &params(query)? {
        let millis = || {
            v.parse::<i64>()
                .ok()
                .map(|m| Duration::from_millis(m.max(0) as u64))
        };
        match k.as_str() {
            "localBindAddress" => config.endpoint.local_bind_address = Some(v.to_owned()),
            "localBindPort" => {
                if let Ok(p) = v.parse() {
                    config.endpoint.local_bind_port = p;
                }
            }
            "refreshTimeout" => {
                if let Some(t) = millis() {
                    config.refresh_timeout = t;
                }
            }
            "discoveryInitialWaitTimeout" => {
                if let Some(t) = millis() {
                    config.initial_wait_timeout = t;
                }
            }
            _ => {}
        }
    }
    Ok(Some(config))
}

/// Opens a Core connection to a URL, authenticated as `user` if given
/// (`createSessionFactory(user, password)`): to the first of its servers
/// that answers, reconnecting and failing over as its parameters say.
pub async fn open(
    url: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> anyhow::Result<Connection> {
    let mut options = ConnectionOptions {
        user: user.map(str::to_owned),
        password: password.map(str::to_owned),
        ..ConnectionOptions::default()
    };
    if let Some(config) = discovery_url(url)? {
        let (_, query) = url.split_once('?').unwrap_or((url, ""));
        let params = params(query)?;
        locator_options(&params, &mut options);
        return Connection::connect_discovery(config, options)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to create session factory; nested exception is {e}")
            });
    }
    let (servers, params) = parse_url_list(url)?;
    locator_options(&params, &mut options);
    Connection::connect_to(servers, options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create session factory; nested exception is {e}"))
}

#[cfg(test)]
mod tests {
    /// The first server of a URL.
    fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
        let c = super::parse_url_list(url)?.0.remove(0);
        Ok((c.host, c.port))
    }

    /// The TLS settings of each server of a URL.
    fn tls(url: &str) -> Vec<Option<saranga_transport::ConnectorTlsSettings>> {
        super::parse_url_list(url)
            .unwrap()
            .0
            .into_iter()
            .map(|c| c.tls.map(|t| t.settings().unwrap().clone()))
            .collect()
    }

    #[test]
    fn urls() {
        assert_eq!(
            parse_url("tcp://localhost:61616").unwrap(),
            ("localhost".into(), 61616)
        );
        assert_eq!(parse_url("tcp://h:1?x=y").unwrap(), ("h".into(), 1));
        assert_eq!(
            parse_url("(tcp://a:1,tcp://b:2)?ha=true").unwrap(),
            ("a".into(), 1)
        );
        assert_eq!(parse_url("amqp://h:5672").unwrap(), ("h".into(), 5672));
        assert_eq!(parse_url("tcp://[::1]:7").unwrap(), ("::1".into(), 7));
        assert!(parse_url("vm://0").is_err());
    }

    #[test]
    fn tls_urls() {
        assert_eq!(tls("tcp://h:1"), [None]);
        let one = tls(
            "tcp://h:1?sslEnabled=true&trustStorePath=t.jks&trustStorePassword=ENC(3a34fd21b82bf2a822fa49a8d8fa115d)&verifyHost=false",
        );
        let s = one[0].as_ref().unwrap();
        assert_eq!(s.trust_store_path.as_deref(), Some("t.jks"));
        assert_eq!(s.trust_store_password.as_deref(), Some("password"));
        assert!(!s.verify_host);
        // The list's parameters apply to each server, its own first.
        let list = tls("(tcp://a:1,tcp://b:2?trustAll=false)?sslEnabled=true&trustAll=true");
        assert!(list[0].as_ref().unwrap().trust_all);
        assert!(!list[1].as_ref().unwrap().trust_all);
        assert!(list[1].as_ref().unwrap().verify_host);
    }

    #[test]
    fn url_lists_and_locator_parameters() {
        use saranga_client::Connector;
        let (servers, params) =
            super::parse_url_list("(tcp://a:1,tcp://b:2?x=y)?ha=true&reconnectAttempts=-1")
                .unwrap();
        assert_eq!(servers, [Connector::new("a", 1), Connector::new("b", 2)]);
        let mut options = saranga_client::ConnectionOptions::default();
        super::locator_options(&params, &mut options);
        assert!(options.ha);
        assert_eq!(options.reconnect_attempts, -1);

        let (servers, params) =
            super::parse_url_list("tcp://h:1?retryInterval=500&callFailoverTimeout=-1").unwrap();
        assert_eq!(servers, [Connector::new("h", 1)]);
        let mut options = saranga_client::ConnectionOptions::default();
        super::locator_options(&params, &mut options);
        assert_eq!(
            options.retry_interval,
            std::time::Duration::from_millis(500)
        );
        assert_eq!(options.call_failover_timeout, None);
    }
}
