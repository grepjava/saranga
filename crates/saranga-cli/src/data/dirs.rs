//! Where the data tools find a broker's data (`DataAbstract`,
//! `Configurable.readConfiguration`) and the lock they hold on an instance
//! (`LockAbstract`).

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;

/// `--bindings`, `--journal`, `--paging`, `--large-messages` and `--broker`.
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
}

/// The data directories, and the journal settings a tool reading them needs.
pub struct DataDirs {
    pub bindings: PathBuf,
    pub journal: PathBuf,
    pub paging: PathBuf,
    pub large_messages: PathBuf,
    /// broker.xml, where there is one.
    pub config: Option<saranga_config::BrokerConfig>,
    /// The broker instance, if the tool runs in one.
    pub instance: Option<PathBuf>,
}

impl DataDirArgs {
    /// The directories given, else those of the instance's broker.xml,
    /// else `./data/*` outside an instance. Each must exist.
    pub fn resolve(&self) -> anyhow::Result<DataDirs> {
        let dirs = self.unchecked()?;
        for dir in [&dirs.bindings, &dirs.journal, &dirs.paging] {
            check_exists(dir)?;
        }
        Ok(dirs)
    }

    /// As [`DataDirArgs::resolve`], whether they exist or not.
    pub fn unchecked(&self) -> anyhow::Result<DataDirs> {
        let instance = crate::client::instance_dir();
        let configured = self.configured(instance.as_deref())?;
        let (config, defaults) = match configured {
            Some((config, dir)) => {
                let s = saranga_server::config::storage_config(&config, &dir);
                let d = [
                    s.bindings.directory.clone(),
                    s.messages.directory.clone(),
                    s.paging.clone(),
                    s.large_messages.clone(),
                ];
                (Some(config), d)
            }
            None => {
                let data = Path::new("./data");
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
        let inst = match (&self.broker, instance) {
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
pub fn check_exists(dir: &Path) -> anyhow::Result<()> {
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
            anyhow::bail!(
                "Error: There is another process using the server at {}. Cannot start the process!",
                place.display()
            );
        }
        Ok(Self(Some(file)))
    }
}
