//! `saranga shell`, and `saranga` alone: the CLI's commands typed at a
//! prompt and run one after another in the same process, as `artemis shell`
//! (`Shell`) runs them. In the shell, `connect` checks a URL, user and
//! password and keeps them for the commands that follow, and `disconnect`
//! forgets them (`Connect`, `Disconnect`).
//!
//! Differences from Artemis's shell, which reads lines with JLine:
//! - There is no line editing, completion or recall of earlier lines. The
//!   history file is still written as JLine writes it, so an Artemis shell
//!   on the same instance recalls the lines.
//! - Without a terminal the shell reads the lines of standard input at its
//!   prompts, as Artemis's does through JLine's dumb terminal. Artemis reads
//!   the history answer with a `Scanner` that buffers the input after it, so
//!   the lines that follow the answer are lost there; here they run.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::client::ConnectionArgs;

/// `Terminal`'s colours.
const WARNING: &str = "\x1b[31m";
const INPUT: &str = "\x1b[32m";
const INFO: &str = "\x1b[33m";
const CLEAR: &str = "\x1b[0m";

/// `Shell.DEFAULT_HISTORY_FILE`, in the instance's `etc`.
const DEFAULT_HISTORY_FILE: &str = "history-file";
/// What the history file holds once history was refused.
const NO_HISTORY: &str = "NO_HISTORY";

/// `IN_SHELL`.
static IN_SHELL: AtomicBool = AtomicBool::new(false);
/// `CONNECTED`: `connect` succeeded, so `<CTRL-D>` disconnects first.
static CONNECTED: AtomicBool = AtomicBool::new(false);
/// The prompt, once a connection or `disconnect` set it.
static PROMPT: Mutex<Option<String>> = Mutex::new(None);
/// `CONNECTION_INFORMATION`: the URL, user and password of the first
/// connection made in the shell.
static CONNECTION: Mutex<Option<Saved>> = Mutex::new(None);
/// Waiting for a line at the prompt.
static AT_PROMPT: AtomicBool = AtomicBool::new(false);
/// `<CTRL-C>` was pressed since the last line was read.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

struct Saved {
    url: String,
    user: Option<String>,
    password: Option<String>,
}

/// `shell`'s options.
#[derive(clap::Args, Clone, Debug, Default)]
pub struct ShellArgs {
    /// It will be used for an initial connection if set.
    #[arg(long)]
    pub url: Option<String>,
    /// It will be used for an initial connection if set.
    #[arg(long)]
    pub user: Option<String>,
    /// It will be used for an initial connection if set.
    #[arg(long)]
    pub password: Option<String>,
    /// File where shell history is being stored.
    #[arg(long)]
    pub history: Option<PathBuf>,
}

/// `Shell.inShell`.
pub fn in_shell() -> bool {
    IN_SHELL.load(Ordering::Relaxed)
}

/// `recoverConnectionInformation`: what the shell's connection has and the
/// command was not given.
pub fn recover(args: &mut ConnectionArgs) {
    if let Some(saved) = CONNECTION.lock().unwrap().as_ref() {
        if args.user.is_none() {
            args.user = saved.user.clone();
        }
        if args.password.is_none() {
            args.password = saved.password.clone();
        }
        if args.url.is_none() {
            args.url = Some(saved.url.clone());
        }
    }
}

/// `saveConnectionInfo`: in the shell, the first connection made is kept
/// for the commands that follow, and shown in the prompt.
pub fn save_connection(url: &str, user: Option<&str>, password: Option<&str>) {
    if !in_shell() {
        return;
    }
    let mut saved = CONNECTION.lock().unwrap();
    if saved.is_some() {
        return;
    }
    *saved = Some(Saved {
        url: url.to_owned(),
        user: user.map(str::to_owned),
        password: password.map(str::to_owned),
    });
    println!(
        "CLI connected to broker {url}, user:{}",
        user.unwrap_or("null")
    );
    set_prompt(&match user {
        Some(u) => format!("{u}@{url}"),
        None => url.to_owned(),
    });
}

/// `connect`: `createConnectionFactory().close()`, whose `tryConnect`
/// checks the connection (asking for another URL, user and password when
/// it fails, the retry's failure ignored) and keeps it for the shell's
/// commands.
pub async fn connect(mut args: ConnectionArgs) -> anyhow::Result<()> {
    args.resolve(false)?;
    *CONNECTION.lock().unwrap() = None;
    match crate::client::jmsapi::Conn::try_connect(&mut args).await {
        Ok(()) => {
            println!("Connection Successful!");
            if in_shell() {
                CONNECTED.store(true, Ordering::Relaxed);
            }
        }
        Err(e) => {
            crate::report_client_error(&e);
            println!("Connection Failure!");
        }
    }
    Ok(())
}

/// `disconnect`: forgets the shell's connection.
pub fn disconnect(mut args: ConnectionArgs) -> anyhow::Result<()> {
    args.resolve(false)?;
    *CONNECTION.lock().unwrap() = None;
    println!("Connection information cleared!");
    if in_shell() {
        *PROMPT.lock().unwrap() = None;
        CONNECTED.store(false, Ordering::Relaxed);
    }
    Ok(())
}

/// The banner, whose last line names the prompt (`getNameFromBanner`).
fn banner() -> String {
    format!(" Saranga {}\n\n\n", env!("CARGO_PKG_VERSION"))
}

fn print_banner() {
    print!("{INFO}{}{CLEAR}", banner());
}

fn set_prompt(name: &str) {
    *PROMPT.lock().unwrap() = Some(format!("{INPUT}{name} > {CLEAR}"));
}

fn prompt() -> String {
    PROMPT.lock().unwrap().clone().unwrap_or_else(|| {
        let banner = banner();
        let name = banner
            .lines()
            .rfind(|l| !l.trim().is_empty())
            .unwrap_or_default();
        format!("{INPUT}{} > {CLEAR}", name.trim())
    })
}

/// `saranga shell` (`Shell.run`): the banner, the connection its options
/// describe, then the shell.
pub async fn shell(args: ShellArgs) {
    IN_SHELL.store(true, Ordering::Relaxed);
    print_banner();
    if args.url.is_some() || args.user.is_some() || args.password.is_some() {
        let conn = ConnectionArgs {
            url: args.url,
            user: args.user,
            password: args.password,
            protocol: "CORE".to_owned(),
            ..ConnectionArgs::default()
        };
        if let Err(e) = connect(conn).await {
            crate::report_client_error(&e);
        }
    }
    run(false, args.history).await;
}

/// `Shell.runShell`: lines from the terminal until `exit` or `<CTRL-D>`,
/// or from standard input until it ends.
pub async fn run(print_banner_first: bool, history: Option<PathBuf>) {
    IN_SHELL.store(true, Ordering::Relaxed);
    let instance = crate::client::instance_dir();
    let default_file = absolute(
        &instance
            .clone()
            .unwrap_or_else(|| PathBuf::from("null"))
            .join("etc")
            .join(DEFAULT_HISTORY_FILE),
    );
    let history = history
        .or_else(|| std::env::var_os("ARTEMIS_SHELL_HISTORY").map(PathBuf::from))
        .or_else(|| inquire_history(&default_file));
    let mut history = history.map(History::open);

    if print_banner_first {
        print_banner();
    }
    match &history {
        None => println!(
            "{WARNING}Shell history disabled as recorded in {}{CLEAR}",
            default_file.display()
        ),
        Some(h) => println!(
            "{WARNING}Shell history being saved at {}{CLEAR}",
            absolute(&h.file).display()
        ),
    }
    println!();
    println!("For a list of commands, type {WARNING}help{CLEAR}:");
    println!("Type {WARNING}exit{CLEAR} or press {WARNING}<CTRL-D>{CLEAR} to leave the session:");

    // <CTRL-C> stops the command running, or the line being typed.
    let interrupt = Arc::new(tokio::sync::Notify::new());
    let watcher = tokio::spawn({
        let interrupt = interrupt.clone();
        async move {
            while tokio::signal::ctrl_c().await.is_ok() {
                println!();
                println!("....Interrupted by the user.");
                INTERRUPTED.store(true, Ordering::Relaxed);
                if AT_PROMPT.load(Ordering::Relaxed) {
                    print!("{}", prompt());
                    let _ = std::io::stdout().flush();
                }
                interrupt.notify_waiters();
            }
        }
    });

    loop {
        print!("{}", prompt());
        let _ = std::io::stdout().flush();
        INTERRUPTED.store(false, Ordering::Relaxed);
        AT_PROMPT.store(true, Ordering::Relaxed);
        let line = tokio::task::block_in_place(read_line);
        AT_PROMPT.store(false, Ordering::Relaxed);
        let line = match line {
            Some(line) => line,
            None => {
                // A console read that <CTRL-C> cut short ends as input
                // does; the signal arrives just after.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if INTERRUPTED.swap(false, Ordering::Relaxed) {
                    continue;
                }
                if !end_of_input().await {
                    continue;
                }
                break;
            }
        };
        let words = words(&line);
        if words.len() == 1 && words[0] == "exit" {
            if !end_of_input().await {
                continue;
            }
            break;
        }
        if let Some(h) = &mut history {
            h.add(&line);
        }
        // JLine's registry runs nothing for an empty line or a comment.
        if words.is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        tokio::select! {
            _ = execute(words) => {}
            _ = interrupt.notified() => {}
        }
    }
    watcher.abort();
    IN_SHELL.store(false, Ordering::Relaxed);
}

/// `EndOfFileException`: when connected, `disconnect` and go on; else the
/// shell ends (true).
async fn end_of_input() -> bool {
    if CONNECTED.load(Ordering::Relaxed) {
        execute(vec!["disconnect".to_owned()]).await;
        return false;
    }
    true
}

/// One command line, parsed as the shell's command line and run: as
/// JLine's registry runs it, `Unknown command` for a first word that names
/// none, and as picocli's `execute` does, a mistake's message, then the
/// suggestions for an unmatched word or else the usage of the command it
/// was made in, on the error stream.
async fn execute(words: Vec<String>) {
    use clap::FromArgMatches;
    if !crate::help::parse::shell_command(&words[0]) {
        println!("Unknown command: {}", words[0]);
        return;
    }
    let mut command = crate::command_line(true);
    let os: Vec<std::ffi::OsString> = words.iter().map(Into::into).collect();
    let args = match crate::parse_artemis(&os, &command, true) {
        crate::Parsed::Args(args) => args,
        crate::Parsed::Mistake(e) => {
            eprintln!("{e}");
            match e.suggestions() {
                Some(s) => eprintln!("{s}"),
                None => eprint!(
                    "{}",
                    crate::help::help("saranga", &e.path, crate::help_mode(), true)
                ),
            }
            return;
        }
        crate::Parsed::Group(words) => {
            print!(
                "{}",
                crate::help::help("saranga", &words, crate::help_mode(), true)
            );
            return;
        }
        crate::Parsed::Clap => {
            crate::artemis_spellings(std::iter::once("saranga".to_owned()).chain(words).collect())
        }
    };
    let matches = match command.clone().try_get_matches_from(args) {
        Ok(m) => m,
        Err(e) => {
            let _ = e.print();
            return;
        }
    };
    let cli = match crate::Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.format(&mut command).print();
            return;
        }
    };
    match cli.command {
        None => {}
        Some(c @ (crate::Command::Run { .. } | crate::Command::Shell(_))) => {
            let name = if matches!(c, crate::Command::Run { .. }) {
                "run"
            } else {
                "shell"
            };
            let _ = command
                .error(
                    clap::error::ErrorKind::InvalidSubcommand,
                    format!("unrecognized subcommand '{name}'"),
                )
                .print();
        }
        Some(c) => {
            crate::execute(c).await;
        }
    }
}

/// A line from the terminal, without its end; `None` at its end.
fn read_line() -> Option<String> {
    let mut line = String::new();
    match std::io::stdin().lock().read_line(&mut line) {
        Ok(0) | Err(_) => None,
        Ok(_) => Some(line.trim_end_matches(['\r', '\n']).to_owned()),
    }
}

/// JLine's `DefaultParser`: words separated by blanks, quoted with `'` or
/// `"`, and `\` escaping the next character.
fn words(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut quote = None;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (_, '\\') => {
                if let Some(next) = chars.next() {
                    word.push(next);
                }
                in_word = true;
            }
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => word.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                in_word = true;
            }
            (None, c) if c.is_whitespace() => {
                if in_word {
                    out.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            (None, c) => {
                word.push(c);
                in_word = true;
            }
        }
    }
    if in_word {
        out.push(word);
    }
    out
}

/// `inquiryDefaultHistory`: the instance's history file, asking once
/// whether to keep one; `None` once refused, or outside an instance.
fn inquire_history(file: &Path) -> Option<PathBuf> {
    if !file.exists() && std::fs::File::create(file).is_err() {
        return None;
    }
    let empty = std::fs::metadata(file).ok()?.len() == 0;
    if empty {
        println!();
        let answer = loop {
            println!("Allow shell history? (Y/N)");
            let line = read_line()?;
            let line = line.to_uppercase();
            if line == "Y" || line == "N" {
                break line;
            }
        };
        if answer == "N" {
            let end = if cfg!(windows) { "\r\n" } else { "\n" };
            std::fs::write(file, format!("{NO_HISTORY}{end}")).ok()?;
        }
        owner_only(file);
    }
    let text = std::fs::read_to_string(file).ok()?;
    let first = text.lines().next().map(|l| l.trim_end_matches('\r'));
    (first != Some(NO_HISTORY)).then(|| file.to_path_buf())
}

/// The history file, as JLine's `DefaultHistory` keeps it: one
/// `<epoch millis>:<line>` per line.
struct History {
    file: PathBuf,
    last: Option<String>,
}

impl History {
    fn open(file: PathBuf) -> Self {
        let last = std::fs::read_to_string(&file).ok().and_then(|text| {
            text.lines()
                .rfind(|l| !l.is_empty())
                .and_then(|l| l.split_once(':'))
                .map(|(_, line)| line.to_owned())
        });
        Self { file, last }
    }

    /// `DefaultHistory.add` and `save`: trailing blanks go, and lines
    /// starting with a blank or repeating the one before are not kept.
    fn add(&mut self, line: &str) {
        if line.is_empty() || line.starts_with(char::is_whitespace) {
            return;
        }
        let line = escape(line.trim_end());
        if self.last.as_deref() == Some(line.as_str()) {
            return;
        }
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default();
        let written = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)
            .and_then(|mut f| writeln!(f, "{millis}:{line}"));
        match written {
            Ok(()) => owner_only(&self.file),
            Err(e) => eprintln!("Error saving history of shell : {e}"),
        }
        self.last = Some(line);
    }
}

/// The escapes of JLine's history file.
fn escape(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    for c in line.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out
}

/// `setHistoryFilePermissions`: readable and writable by its owner only.
fn owner_only(file: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600));
    }
    #[cfg(not(unix))]
    let _ = file;
}

/// `File.getAbsolutePath`.
fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_split_as_jline_splits_them() {
        assert_eq!(words("  queue stat  "), ["queue", "stat"]);
        assert_eq!(
            words(r#"producer --message "a b" --url 'tcp://h:1'"#),
            ["producer", "--message", "a b", "--url", "tcp://h:1"]
        );
        assert_eq!(words(r"a\ b c\\d"), ["a b", r"c\d"]);
        assert_eq!(words(r#"x """#), ["x", ""]);
        assert!(words("   ").is_empty());
    }

    #[test]
    fn history_is_written_as_jline_writes_it() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("history-file");
        let mut h = History::open(file.clone());
        for line in [
            "queue stat",
            "queue stat",
            " hidden",
            "",
            r"a\b",
            "x\ny",
            "i  j ",
            "queue stat",
        ] {
            h.add(line);
        }
        let text = std::fs::read_to_string(&file).unwrap();
        let lines: Vec<&str> = text.lines().map(|l| l.split_once(':').unwrap().1).collect();
        assert_eq!(
            lines,
            ["queue stat", r"a\\b", r"x\ny", "i  j", "queue stat"]
        );
        // A shell opened later does not repeat the last line either.
        let mut again = History::open(file.clone());
        again.add("queue stat");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), text);
    }

    #[test]
    fn refused_history_is_remembered() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("history-file");
        std::fs::write(&file, "NO_HISTORY\r\n").unwrap();
        assert_eq!(inquire_history(&file), None);
        std::fs::write(&file, "1:queue stat\n").unwrap();
        assert_eq!(inquire_history(&file), Some(file.clone()));
        assert_eq!(inquire_history(&dir.path().join("none").join("x")), None);
    }

    #[test]
    fn the_prompt_names_saranga() {
        assert_eq!(
            prompt(),
            format!("{INPUT}Saranga {} > {CLEAR}", env!("CARGO_PKG_VERSION"))
        );
    }
}
