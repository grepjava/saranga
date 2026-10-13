//! Where the data tools find a broker's data (`DataAbstract`,
//! `Configurable.readConfiguration`) and the lock they hold on an instance
//! (`LockAbstract`).

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;

/// `--bindings`, `--journal`, `--paging`, `--large-messages`, `--broker`,
/// the configuration URI and `--verbose`.
#[derive(Args, Clone, Default)]
pub struct DataDirArgs {
    /// The folder used for bindings. Default: read from broker.xml.
    #[arg(long)]
    pub bindings: Option<PathBuf>,
    /// The folder used for normal messages. Default: read from broker.xml.
    #[arg(long)]
    pub journal: Option<PathBuf>,
    /// The folder used for paged messages. Default: read from broker.xml.
    #[arg(long)]
    pub paging: Option<PathBuf>,
    /// The folder used for large-messages. Default: read from broker.xml.
    #[arg(long)]
    pub large_messages: Option<PathBuf>,
    /// Override the broker configuration from the bootstrap.xml.
    #[arg(long)]
    pub broker: Option<String>,
    /// Broker Configuration URI. Default:
    /// xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml.
    pub configuration: Option<String>,
    /// Print additional information.
    #[arg(long)]
    pub verbose: bool,
}

/// `DBOption`'s `--f` and `--output`, of `print`, `exp` and `recover`.
#[derive(Args, Clone, Default)]
pub struct DbArgs {
    /// This will allow certain tools like print-data to be performed
    /// ignoring any running servers. WARNING: Changing data concurrently
    /// with a running broker may damage your data. Be careful with this
    /// option.
    #[arg(long = "f")]
    pub ignore_lock: bool,
    /// Output name for the file.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

impl DbArgs {
    /// The CLI lock (`OptionalLocking`): none taken with `--f`.
    pub fn lock(&self) -> anyhow::Result<CliLock> {
        let instance = crate::client::instance_dir();
        match instance {
            Some(_) if self.ignore_lock => Ok(CliLock(None)),
            _ => CliLock::take(instance.as_deref()),
        }
    }

    /// Where the tool prints: `--output`, created once the lock is held,
    /// else standard output.
    pub fn out(&self) -> anyhow::Result<Box<dyn std::io::Write>> {
        Ok(match &self.output {
            Some(p) => {
                Box::new(File::create(p).with_context(|| format!("creating {}", p.display()))?)
            }
            None => Box::new(std::io::stdout()),
        })
    }
}

/// The data directories, and the journal settings a tool reading them needs.
pub struct DataDirs {
    pub bindings: PathBuf,
    pub journal: PathBuf,
    pub paging: PathBuf,
    pub large_messages: PathBuf,
    /// `getJournalRetentionLocation` and its settings, from broker.xml.
    pub retention: Option<saranga_journal::Retention>,
    /// broker.xml, where there is one.
    pub config: Option<saranga_config::BrokerConfig>,
    /// The broker instance, if the tool runs in one.
    pub instance: Option<PathBuf>,
}

impl DataDirArgs {
    /// The directories given, else those of the instance's broker.xml,
    /// else `./data/*` outside an instance. Each must exist
    /// (`DBOption.getParameterConfiguration`).
    pub fn resolve(&self) -> anyhow::Result<DataDirs> {
        let mut dirs = self.unchecked()?;
        // The directories from the configuration are made absolute
        // (`getAbsolutePath`), the ones given are not.
        let cwd = std::env::current_dir().unwrap_or_default();
        for (dir, given) in [
            (&mut dirs.bindings, &self.bindings),
            (&mut dirs.journal, &self.journal),
            (&mut dirs.paging, &self.paging),
            (&mut dirs.large_messages, &self.large_messages),
        ] {
            if given.is_none() && dir.is_relative() {
                *dir = cwd.join(&*dir);
            }
            check_exists(dir)?;
        }
        Ok(dirs)
    }

    /// As [`DataDirArgs::resolve`], whether they exist or not.
    pub fn unchecked(&self) -> anyhow::Result<DataDirs> {
        let instance = crate::client::instance_dir();
        let configured = self.configured(instance.as_deref())?;
        let mut retention = None;
        let (config, defaults) = match configured {
            Some((config, dir)) => {
                let s = saranga_server::config::storage_config(&config, &dir);
                let d = [
                    s.bindings.directory.clone(),
                    s.messages.directory.clone(),
                    s.paging.clone(),
                    s.large_messages.clone(),
                ];
                retention = s.messages.retention.clone();
                (Some(config), d)
            }
            None => {
                // `./data` under the broker instance `.`.
                let data = Path::new(".").join(".").join("data");
                (
                    None,
                    [
                        data.join("bindings"),
                        data.join("journal"),
                        data.join("paging"),
                        data.join("largemessages"),
                    ],
                )
            }
        };
        let [b, j, p, l] = defaults;
        let dirs = DataDirs {
            bindings: self.bindings.clone().unwrap_or(b),
            journal: self.journal.clone().unwrap_or(j),
            paging: self.paging.clone().unwrap_or(p),
            large_messages: self.large_messages.clone().unwrap_or(l),
            retention,
            config,
            instance,
        };
        Ok(dirs)
    }

    /// broker.xml (`--broker`, else the instance's) and the directory its
    /// relative data directories are under.
    fn configured(
        &self,
        instance: Option<&Path>,
    ) -> anyhow::Result<Option<(saranga_config::BrokerConfig, PathBuf)>> {
        let options = crate::instance::Options::default();
        let configuration = self.configuration.as_deref().filter(|c| !c.is_empty());
        let inst = match (&self.broker, instance) {
            (None, _) if configuration.is_some() => {
                let bootstrap = crate::instance::bootstrap_path(configuration.unwrap_or_default())
                    .map_err(|e| crate::client::ConfigurationError(e.to_string()))?;
                let etc = crate::client::instance_etc()
                    .or_else(|| bootstrap.parent().map(Path::to_path_buf))
                    .unwrap_or_default();
                crate::instance::Instance::from_bootstrap(&etc, &bootstrap, options)?
            }
            (Some(b), _) => {
                let path = b
                    .trim_start_matches("xml:")
                    .trim_start_matches("file:")
                    .trim_start_matches("//");
                crate::instance::Instance::from_broker_xml(Path::new(path), options)?
            }
            (None, Some(_)) => match crate::client::instance_etc() {
                Some(etc) => crate::instance::Instance::from_etc(&etc, options)?,
                None => return Ok(None),
            },
            (None, None) => return Ok(None),
        };
        let config = inst.broker_config()?;
        Ok(Some((config, inst.dir)))
    }
}

/// `DataAbstract.checkIfDirectoryExists`.
fn check_exists(dir: &Path) -> anyhow::Result<()> {
    if !dir.exists() {
        anyhow::bail!(
            "Could not find folder: {}, please pass --bindings, --journal and --paging as arguments",
            dir.display()
        );
    }
    Ok(())
}

/// `LockAbstract`: in an instance, `<instance>/lock/cli.lock`, held while
/// the tool runs; outside one, a warning.
pub struct CliLock(#[allow(dead_code)] Option<File>);

impl CliLock {
    pub fn take(instance: Option<&Path>) -> anyhow::Result<Self> {
        let Some(instance) = instance else {
            eprintln!(
                "Warning: You are running a data tool outside of any broker instance. Modifying data on a running server might break the server's data"
            );
            eprintln!();
            return Ok(Self(None));
        };
        Self::hold(instance)
    }

    /// The lock of an instance (`lockCLI`).
    pub fn hold(instance: &Path) -> anyhow::Result<Self> {
        let place = instance.join("lock");
        std::fs::create_dir_all(&place).with_context(|| format!("creating {}", place.display()))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(place.join("cli.lock"))
            .with_context(|| format!("opening {}", place.join("cli.lock").display()))?;
        if file.try_lock().is_err() {
            return Err(crate::client::CliError(format!(
                "Error: There is another process using the server at {}. Cannot start the process!",
                place.display()
            ))
            .into());
        }
        Ok(Self(Some(file)))
    }
}

/// The message of the `FileNotFoundException` Java throws opening a file:
/// the path, then the system's reason in parentheses.
pub fn file_not_found(path: &Path, e: &std::io::Error) -> String {
    let reason = e.to_string();
    let reason = reason.split(" (os error").next().unwrap_or(&reason);
    format!("{} ({})", path.display(), reason.trim_end_matches('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_lock_is_a_cli_exception() {
        let dir = tempfile::tempdir().unwrap();
        let _held = CliLock::hold(dir.path()).unwrap();
        let e = CliLock::hold(dir.path()).err().unwrap();
        let e = e.downcast_ref::<crate::client::CliError>().unwrap();
        assert!(e.0.starts_with("Error: There is another process using the server at "));
    }

    #[test]
    fn file_not_found_is_as_java_reports_it() {
        let e = std::io::Error::from_raw_os_error(2);
        let reason = e.to_string();
        let reason = reason.split(" (os error").next().unwrap();
        assert_eq!(
            file_not_found(Path::new("nosuch"), &e),
            format!("nosuch ({})", reason.trim_end_matches('.'))
        );
        assert!(!file_not_found(Path::new("x"), &e).contains("os error"));
    }

    #[test]
    fn the_parameter_directories_include_large_messages() {
        let dir = tempfile::tempdir().unwrap();
        let [b, j, p] = ["b", "j", "p"].map(|d| {
            std::fs::create_dir(dir.path().join(d)).unwrap();
            Some(dir.path().join(d))
        });
        let mut args = DataDirArgs {
            bindings: b,
            journal: j,
            paging: p,
            large_messages: Some(dir.path().join("l")),
            broker: None,
            ..DataDirArgs::default()
        };
        let e = args.resolve().err().unwrap().to_string();
        assert!(e.starts_with("Could not find folder: "), "{e}");
        assert!(
            e.contains(&dir.path().join("l").display().to_string()),
            "{e}"
        );
        args.large_messages = args.bindings.clone();
        assert!(args.resolve().is_ok());
    }
}
