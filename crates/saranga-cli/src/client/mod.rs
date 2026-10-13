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
use saranga_client::{Connection, ConnectionOptions};

pub mod address;
pub mod check;
pub mod jms;
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
    /// `isSilentInput`: no prompts with `--silent` or without a terminal.
    pub fn is_silent_input(&self) -> bool {
        self.silent || !std::io::IsTerminal::is_terminal(&std::io::stdin())
    }

    /// `InputAbstract.input`: the default without input, else a non-empty
    /// line, asked for as `InputReader.inputLoop` asks.
    pub fn input(
        &self,
        property: &str,
        prompt: &str,
        silent_default: Option<&str>,
    ) -> Option<String> {
        if self.is_silent_input() {
            return silent_default.map(str::to_owned);
        }
        println!();
        loop {
            println!("{property}:");
            println!("{prompt}");
            let mut line = String::new();
            if std::io::stdin().lock().read_line(&mut line).unwrap_or(0) == 0 {
                return silent_default.map(str::to_owned);
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if !line.is_empty() {
                return Some(line.trim().to_owned());
            }
        }
    }

    /// `InputAbstract.inputBoolean`.
    pub fn input_boolean(&self, property: &str, prompt: &str, silent_default: bool) -> bool {
        if self.is_silent_input() {
            return silent_default;
        }
        loop {
            let value = self
                .input(
                    property,
                    &format!("{prompt}, valid values are Y, N, True, False"),
                    Some(&silent_default.to_string()),
                )
                .unwrap_or_default();
            match value.trim().to_uppercase().as_str() {
                "TRUE" | "Y" => return true,
                "FALSE" | "N" => return false,
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

    /// `tryConnect`, as making a connection factory does: a connection
    /// made and closed at once.
    pub async fn try_connect(&self) -> anyhow::Result<()> {
        self.connect().await?.close().await;
        Ok(())
    }

    /// `doManagement`'s connection and session: a session factory and one
    /// session, each with this user and password (after the factory's
    /// `tryConnect`).
    pub async fn core_session(&self) -> anyhow::Result<(Connection, saranga_client::Session)> {
        self.try_connect().await?;
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
            crate::instance::Instance::from_etc(&etc, crate::instance::Options::default())?;
        let config = instance.broker_config()?;
        Ok(config
            .acceptors
            .iter()
            .find(|a| a.name.as_deref() == Some(name))
            .map(|a| {
                let scheme = a.param("scheme").unwrap_or("tcp");
                let mut host = a.host.clone().unwrap_or_else(|| "localhost".to_owned());
                if host == "0.0.0.0" || host == "::" || host == "[::]" {
                    host = "localhost".to_owned();
                }
                format!("{scheme}://{host}:{}", a.port.unwrap_or(61616))
            }))
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

/// The host and port of a connection URL: `tcp://host:port?params`, the
/// first of `(tcp://a:1,tcp://b:2)?ha=true`, and `amqp://` taken as
/// `tcp://`.
pub fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
    let first = url
        .trim()
        .trim_start_matches('(')
        .split([',', ')'])
        .next()
        .unwrap_or_default();
    let (scheme, rest) = first
        .split_once("://")
        .with_context(|| format!("invalid URL {url}"))?;
    if !matches!(scheme, "tcp" | "amqp") {
        bail!("schema {scheme} not found");
    }
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    let full_query = url.rsplit_once('?').map(|(_, q)| q).unwrap_or(query);
    for param in full_query.split(['&', ';']) {
        let (k, v) = param.split_once('=').unwrap_or((param, ""));
        if k == "sslEnabled" && v.eq_ignore_ascii_case("true") {
            bail!("saranga's CLI does not support TLS connections (sslEnabled) yet");
        }
    }
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
    Ok((
        if host.is_empty() {
            "localhost".to_owned()
        } else {
            host.to_owned()
        },
        port,
    ))
}

/// Opens a Core connection to a URL, authenticated as `user` if given
/// (`createSessionFactory(user, password)`).
pub async fn open(
    url: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> anyhow::Result<Connection> {
    let (host, port) = parse_url(url)?;
    let options = ConnectionOptions {
        user: user.map(str::to_owned),
        password: password.map(str::to_owned),
        ..ConnectionOptions::default()
    };
    Connection::connect((host.as_str(), port), options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create session factory; nested exception is {e}"))
}

#[cfg(test)]
mod tests {
    use super::parse_url;

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
        assert!(parse_url("tcp://h:1?sslEnabled=true").is_err());
        assert!(parse_url("vm://0").is_err());
    }
}
