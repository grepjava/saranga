//! `artemis properties` (`ExportProperties`): the broker's configuration
//! written as broker properties, as `ConfigurationImpl.exportAsProperties`
//! writes it.

use std::path::{Path, PathBuf};

use anyhow::Context;
use saranga_config::ConfigError;
use saranga_config::configuration::{
    Configuration, ExportContext, ExportError, export_as_properties, read_configuration,
};

use crate::data::dirs::java_file_path;

/// `Configurable.readConfiguration` then `exportAsProperties(output)`.
pub fn run(
    broker: Option<String>,
    configuration: Option<String>,
    output: PathBuf,
) -> anyhow::Result<()> {
    println!("Exporting configuration as {}", java_file_path(&output));
    let (config, broker_instance) = match crate::client::instance_dir() {
        // Outside an instance the defaults, the data under `./data`.
        None => {
            let config = Configuration {
                bindings_directory: "./data/bindings".to_owned(),
                journal_directory: "./data/journal".to_owned(),
                large_messages_directory: "./data/largemessages".to_owned(),
                paging_directory: "./data/paging".to_owned(),
                ..Configuration::default()
            };
            (config, ".".to_owned())
        }
        Some(dir) => (read(broker, configuration)?, java_file(&dir)),
    };
    let ctx = ExportContext {
        max_memory: saranga_server::jvm::max_memory(),
        broker_instance,
        uuid: time_uuid,
        identity: identity_string,
    };
    // The `FileWriter` is opened before the export runs, and what was
    // written before an exception is flushed as it closes.
    let (text, failed) = export_as_properties(&config, &ctx);
    std::fs::write(&output, text)
        .map_err(|e| anyhow::anyhow!(crate::data::dirs::file_not_found(&output, &e)))?;
    match failed {
        None => Ok(()),
        Some(ExportError::Runtime(m)) => Err(crate::client::UsageError(m).into()),
        // `Artemis.execute` prints a `NullPointerException`'s stack trace.
        Some(ExportError::NullPointer(m)) => Err(crate::client::CliError(m).into()),
    }
}

/// `ActiveMQServerControl.exportConfigAsProperties` of a running broker:
/// its configuration read again from its file, written as
/// `broker_config_as_properties.txt` in `java.io.tmpdir`, which `artemis
/// run` sets to the instance's `tmp`.
pub fn export_running(instance: &crate::instance::Instance) -> anyhow::Result<PathBuf> {
    let config = read_instance(instance, None)?;
    let ctx = ExportContext {
        max_memory: saranga_server::jvm::max_memory(),
        broker_instance: java_file(&instance.dir),
        uuid: time_uuid,
        identity: identity_string,
    };
    let tmp = instance
        .lookup("java.io.tmpdir")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("java.io.tmpdir system property must be set"))?;
    let output = tmp.join(CONFIG_AS_PROPERTIES_FILE);
    let (text, failed) = export_as_properties(&config, &ctx);
    std::fs::write(&output, text)
        .map_err(|e| anyhow::anyhow!(crate::data::dirs::file_not_found(&output, &e)))?;
    match failed {
        None => Ok(output),
        Some(ExportError::Runtime(m) | ExportError::NullPointer(m)) => Err(anyhow::anyhow!(m)),
    }
}

/// `ActiveMQServerControlImpl.CONFIG_AS_PROPERTIES_FILE`.
pub const CONFIG_AS_PROPERTIES_FILE: &str = "broker_config_as_properties.txt";

/// `UUIDGenerator.generateStringUUID`: a time-based UUID.
fn time_uuid() -> String {
    saranga_client::uuid::to_string(&saranga_client::uuid::time_based())
}

/// `FileDeploymentManager.readConfiguration` of the broker configuration
/// `getBrokerDTO` names: `--broker` (a `file:` URI, `file:` added when
/// missing), else bootstrap.xml's `<server configuration>`.
fn read(broker: Option<String>, configuration: Option<String>) -> anyhow::Result<Configuration> {
    let options = crate::instance::Options::default();
    let configuration = configuration.filter(|c| !c.is_empty());
    let etc = crate::client::instance_etc().unwrap_or_default();
    let instance = match &configuration {
        Some(c) => {
            let bootstrap = crate::instance::bootstrap_path(c)
                .map_err(|e| crate::client::ConfigurationError(e.to_string()))?;
            crate::instance::Instance::from_bootstrap(&etc, &bootstrap, options)?
        }
        None => crate::instance::Instance::from_etc(&etc, options)?,
    };
    read_instance(&instance, broker)
}

/// [`read`] of an instance: `broker`, else its configuration's URL.
fn read_instance(
    instance: &crate::instance::Instance,
    broker: Option<String>,
) -> anyhow::Result<Configuration> {
    let url = match broker {
        Some(b) if b.starts_with("file:") => b,
        Some(b) => format!("file:{b}"),
        None => instance
            .bootstrap
            .as_ref()
            .and_then(|b| b.server_configuration.clone())
            .unwrap_or_else(|| {
                format!(
                    "file:{}",
                    instance.broker_xml.display().to_string().replace('\\', "/")
                )
            }),
    };
    let path = saranga_config::file_uri_path(&url)
        .unwrap_or_else(|| PathBuf::from(url.trim_start_matches("file:")));
    let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    // `XMLUtil.urlToElement`: the bytes as UTF-8.
    let xml = String::from_utf8_lossy(&bytes);
    let config = read_configuration(
        &xml,
        |n| instance.lookup(n),
        saranga_config::Validation::Deploy { jms: true },
        Some(&url),
    )
    .map_err(|e| thrown(e, &path))?;
    Ok(config)
}

/// What reading the configuration threw, as `Artemis.execute` reports it:
/// a `RuntimeException` by its message and the usage (the schema's
/// `IllegalStateException` is `Invalid configuration`, its error logged as
/// it is found), a `NullPointerException` by itself, a checked exception
/// as Java's `main` lets it out.
fn thrown(e: ConfigError, path: &Path) -> anyhow::Error {
    match e {
        ConfigError::Decode(d) => d.into(),
        ConfigError::Invalid(_) => {
            crate::client::UsageError("Invalid configuration".to_owned()).into()
        }
        ConfigError::Runtime(m) => crate::client::UsageError(m).into(),
        ConfigError::NullPointer(m) => crate::client::CliError(m).into(),
        ConfigError::Checked(m) => {
            crate::client::CliError(format!("Exception in thread \"main\" {m}")).into()
        }
        e => anyhow::Error::new(e).context(format!("reading {}", path.display())),
    }
}

/// `new File(path).toString()`: the platform's separators, repeated ones
/// collapsed and a trailing one dropped.
fn java_file(path: &Path) -> String {
    let text = path.display().to_string();
    let sep = if cfg!(windows) { '\\' } else { '/' };
    let text = if cfg!(windows) {
        text.replace('/', "\\")
    } else {
        text
    };
    let mut out = String::with_capacity(text.len());
    for (i, c) in text.chars().enumerate() {
        // A UNC path keeps its leading pair.
        if c == sep && out.ends_with(sep) && !(cfg!(windows) && i == 1) {
            continue;
        }
        out.push(c);
    }
    let root = out.len() == 1 || (cfg!(windows) && out.len() == 3 && out.ends_with(":\\"));
    if out.ends_with(sep) && !root {
        out.pop();
    }
    out
}

/// `Object.toString` of a plugin without its own: the class, `@` and an
/// identity hash, which Java makes up as it runs.
fn identity_string(class: &str) -> String {
    let mut b = [0u8; 4];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b).expect("random bytes");
    format!("{class}@{:x}", u32::from_be_bytes(b) & 0x7fff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_strings_are_as_java_prints_them() {
        if cfg!(windows) {
            assert_eq!(java_file(Path::new("D:/tmp//x/")), "D:\\tmp\\x");
            assert_eq!(java_file(Path::new("C:\\")), "C:\\");
        } else {
            assert_eq!(java_file(Path::new("/tmp//x/")), "/tmp/x");
            assert_eq!(java_file(Path::new("/")), "/");
        }
    }
}
