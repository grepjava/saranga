//! A broker instance's `etc/` directory, read the way `artemis run` reads
//! it: `bootstrap.xml` names the security manager and `broker.xml`;
//! `login.config` holds the JAAS domains; `${...}` placeholders resolve
//! against the system properties given with `-D`, then those the `artemis`
//! script defines, then the environment. Broker properties (`--properties`,
//! else `etc/broker.properties`, then `-D brokerconfig.*`) apply over
//! `broker.xml`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use saranga_config::{
    BootstrapConfig, BootstrapSecurity, BrokerConfig, ConfigError, DecodeError, Validation,
    broker_properties,
};
use saranga_security::jaas::ModuleFactory;
use saranga_security::{
    BasicSecurityManager, JaasSecurityManager, LoginConfig, SecurityManager, SecurityStore,
    SecurityStoreConfig,
};
use saranga_server::users::Users;

/// What `XmlUtil.decode` threw, as a command that does not catch it ends:
/// a `RuntimeException` with its message and the usage
/// (`Artemis.execute`), another as Java's `main` reports an uncaught
/// exception. Other errors are left as they are.
pub fn uncaught(e: anyhow::Error) -> anyhow::Error {
    match e.downcast::<DecodeError>() {
        Ok(d) if d.runtime => {
            crate::client::UsageError(d.message.clone().unwrap_or_default()).into()
        }
        Ok(d) => crate::client::CliError(format!("Exception in thread \"main\" {d}")).into(),
        Err(e) => e,
    }
}

/// What `XmlUtil.decode` threw, as `Run` ends: its stack trace printed.
pub fn run_failure(e: anyhow::Error) -> anyhow::Error {
    match e.downcast::<DecodeError>() {
        Ok(d) => crate::client::CliError(d.to_string()).into(),
        Err(e) => e,
    }
}

/// `Artemis.verifyManagementDTO`, before every command: `management.xml`
/// in `etc` decoded, what fails printed and the command run all the same.
pub fn verify_management_xml(etc: &Path, system: Vec<(String, String)>) {
    let path = etc.join("management.xml");
    let Ok(xml) = std::fs::read(&path) else {
        return;
    };
    let instance = Instance::without_bootstrap(
        etc,
        Options {
            system,
            broker_properties: None,
        },
    );
    if let Err(e) = saranga_config::decode_dto(saranga_config::Dto::ManagementContext, &xml, &|n| {
        instance.lookup(n)
    }) {
        eprintln!("{e}");
    }
}

/// `ActiveMQBasicSecurityManager`'s class name in `bootstrap.xml`.
const BASIC_SECURITY_MANAGER: &str =
    "org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager";

/// What `bootstrap.xml` and `login.config` make of security.
#[derive(Default)]
pub struct Security {
    /// `None` with security off.
    pub store: Option<Arc<SecurityStore>>,
    /// The basic security manager's users, for the broker to load.
    pub users: Option<Arc<Users>>,
    pub bootstrap_users: Vec<(String, String, Vec<String>)>,
}

/// What `saranga run` adds to the instance's files.
#[derive(Debug, Default)]
pub struct Options {
    /// `-D name=value`: system properties, in order.
    pub system: Vec<(String, String)>,
    /// `--properties`: broker properties sources.
    pub broker_properties: Option<String>,
}

pub struct Instance {
    /// The instance directory: relative data directories are under it.
    pub dir: PathBuf,
    pub etc: PathBuf,
    pub broker_xml: PathBuf,
    pub bootstrap: Option<BootstrapConfig>,
    properties: HashMap<&'static str, String>,
    /// System properties: the profile's JAVA_ARGS, then the command line's.
    pub system: Vec<(String, String)>,
    /// `--properties`: broker properties sources.
    broker_properties: Option<String>,
    /// Whether `etc/broker.properties` existed when first looked for: `run`
    /// decides it once, as it starts.
    etc_properties: std::sync::OnceLock<bool>,
}

impl Instance {
    /// An instance from its `etc/` directory: `bootstrap.xml` if there is
    /// one, else `broker.xml` next to it.
    pub fn from_etc(etc: &Path, options: Options) -> anyhow::Result<Self> {
        let bootstrap_path = absolute(etc).join("bootstrap.xml");
        Self::from_bootstrap(etc, &bootstrap_path, options)
    }

    /// An instance from its `etc/` directory and the bootstrap file given
    /// as `artemis run`'s configuration (`xml:<path>`), which names
    /// `broker.xml`; without the file, `broker.xml` in `etc/`.
    pub fn from_bootstrap(
        etc: &Path,
        bootstrap_path: &Path,
        options: Options,
    ) -> anyhow::Result<Self> {
        let etc = absolute(etc);
        let mut instance = Self::new(etc.clone(), etc.join("broker.xml"), options);
        if bootstrap_path.exists() {
            let bootstrap_path = &absolute(bootstrap_path);
            let bootstrap = instance.read_bootstrap(bootstrap_path)?;
            if let Some(configured) = bootstrap.configuration_path() {
                if configured.exists() {
                    instance.broker_xml = configured;
                } else {
                    // `artemis create` writes the instance's absolute path;
                    // an instance that moved still has broker.xml here.
                    tracing::warn!(
                        "{}: {} does not exist; using {}",
                        bootstrap_path.display(),
                        configured.display(),
                        instance.broker_xml.display()
                    );
                }
            }
            instance.bootstrap = Some(bootstrap);
        }
        Ok(instance)
    }

    /// An instance around a `broker.xml`: its directory is `etc/`, whose
    /// `bootstrap.xml` (if any) still chooses the security manager.
    pub fn from_broker_xml(path: &Path, options: Options) -> anyhow::Result<Self> {
        let path = absolute(path);
        let etc = path.parent().map(Path::to_path_buf).unwrap_or_default();
        let mut instance = Self::new(etc.clone(), path, options);
        let bootstrap_path = etc.join("bootstrap.xml");
        if bootstrap_path.exists() {
            instance.bootstrap = Some(instance.read_bootstrap(&bootstrap_path)?);
        }
        Ok(instance)
    }

    /// An instance from its `etc/` directory with its `broker.xml`, which
    /// is read as Artemis's `getBrokerConfiguration` reads it, without
    /// `bootstrap.xml`.
    pub fn without_bootstrap(etc: &Path, options: Options) -> Self {
        let etc = absolute(etc);
        Self::new(etc.clone(), etc.join("broker.xml"), options)
    }

    fn new(etc: PathBuf, broker_xml: PathBuf, options: Options) -> Self {
        // `<instance>/etc` -> `<instance>`.
        let dir = etc
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| etc.clone());
        let slash = |p: &Path| p.display().to_string().replace('\\', "/");
        let mut properties = HashMap::new();
        properties.insert("artemis.instance", slash(&dir));
        // bin/artemis passes the profile's ARTEMIS_HOME on.
        properties.insert(
            "artemis.home",
            crate::create::saranga_home()
                .map(|h| slash(&h))
                .unwrap_or_else(|_| slash(&dir)),
        );
        properties.insert("artemis.instance.etc", slash(&etc));
        properties.insert("artemis.URI.instance", uri_path(&dir));
        properties.insert("data.dir", slash(&dir.join("data")));
        properties.insert(
            "java.security.auth.login.config",
            slash(&etc.join("login.config")),
        );
        Self {
            dir,
            etc,
            broker_xml,
            bootstrap: None,
            properties,
            system: options.system,
            broker_properties: options.broker_properties,
            etc_properties: std::sync::OnceLock::new(),
        }
    }

    fn system_property(&self, name: &str) -> Option<String> {
        self.system
            .iter()
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }

    /// The system properties: those `artemis run` defines, then those
    /// given with `-D`, the later of a name winning.
    pub fn system_properties(&self) -> Vec<(String, String)> {
        let mut defined: Vec<(String, String)> = self
            .properties
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect();
        defined.sort();
        for (k, v) in &self.system {
            match defined.iter_mut().find(|(n, _)| n == k) {
                Some(p) => p.1 = v.clone(),
                None => defined.push((k.clone(), v.clone())),
            }
        }
        defined
    }

    /// `${sys:name}` in log4j's configuration: the system properties given
    /// with `-D`, then those `artemis run` defines.
    pub fn system_lookup(&self) -> crate::logging::Lookup {
        let system = self.system.clone();
        let properties = self.properties.clone();
        Arc::new(move |name| {
            system
                .iter()
                .rev()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .or_else(|| properties.get(name).cloned())
        })
    }

    /// A system property as `artemis run` defines it, then an environment
    /// variable (`XMLUtil.replaceSystemPropsInString`).
    pub fn lookup(&self, name: &str) -> Option<String> {
        self.system_property(name)
            .or_else(|| self.properties.get(name).cloned())
            .or_else(|| std::env::var(name).ok())
    }

    /// `bootstrap.xml` decoded as `Configurable.getBrokerDTO` decodes it;
    /// what `XmlUtil.decode` throws is a `DecodeError` alone, for the
    /// command to report as Artemis's does ([`uncaught`], [`run_failure`]).
    fn read_bootstrap(&self, path: &Path) -> anyhow::Result<BootstrapConfig> {
        let xml = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let mut bootstrap = match BootstrapConfig::parse_bytes(&xml, |n| self.lookup(n)) {
            Ok(b) => b,
            Err(ConfigError::Decode(e)) => return Err(e.into()),
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!("reading {}", path.display())));
            }
        };
        bootstrap.apply_system_security(&self.system);
        for w in &bootstrap.warnings {
            tracing::warn!("{}: {w}", path.display());
        }
        Ok(bootstrap)
    }

    /// `etc/management.xml` decoded as `Run.getManagementDTO` decodes it.
    /// Without the file, `ManagementFactory`'s `ConfigurationException`
    /// when `required`.
    pub fn read_management_xml(&self, required: bool) -> anyhow::Result<()> {
        let path = self.etc.join("management.xml");
        if !path.exists() {
            if required {
                anyhow::bail!(crate::client::CliError(
                    "org.apache.activemq.artemis.cli.ConfigurationException: Invalid configuration URI, can't find file: management.xml".to_owned()
                ));
            }
            return Ok(());
        }
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let xml =
            saranga_config::decode_dto(saranga_config::Dto::ManagementContext, &bytes, &|n| {
                self.lookup(n)
            })?;
        // `ManagementFactory.create` on the deprecated `<whitelist>`.
        let (whitelist, allowlist) = saranga_config::management_lists(&xml);
        if whitelist {
            tracing::warn!(
                target: "org.apache.activemq.artemis.core.server",
                "AMQ224110: Configuration 'whitelist' is deprecated, please use the 'allowlist' configuration"
            );
            if allowlist {
                tracing::warn!(
                    target: "org.apache.activemq.artemis.core.server",
                    "AMQ224111: Both 'whitelist' and 'allowlist' detected. Configuration 'whitelist' is deprecated, please use only the 'allowlist' configuration"
                );
            }
        }
        Ok(())
    }

    /// `broker.xml` read and validated as the broker deploys it, its
    /// warnings left for [`broker_config`](Self::broker_config).
    pub fn read_broker_xml(&self) -> anyhow::Result<()> {
        let xml = self.broker_xml_text()?;
        BrokerConfig::parse_with(&xml, |n| self.lookup(n)).map_err(|e| self.broker_xml_error(e))?;
        Ok(())
    }

    /// `broker.xml` as `XMLUtil.streamToElement` reads it: through an
    /// `InputStreamReader` in UTF-8, which puts U+FFFD for what is not.
    fn broker_xml_text(&self) -> anyhow::Result<String> {
        let path = &self.broker_xml;
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// What reading `broker.xml` failed with: what the parser threw as it
    /// threw it, anything else with the file named.
    fn broker_xml_error(&self, e: saranga_config::ConfigError) -> anyhow::Error {
        match e {
            saranga_config::ConfigError::Decode(d) => d.into(),
            e => anyhow::Error::new(e).context(format!("reading {}", self.broker_xml.display())),
        }
    }

    /// The broker configuration as the broker deploys it: `<core>` and
    /// `<jms>` validated against their schemas.
    pub fn broker_config(&self) -> anyhow::Result<BrokerConfig> {
        self.broker_config_validated(Validation::Deploy { jms: true })
    }

    /// The broker configuration, `broker.xml` validated as `validation`
    /// says: deployed by the broker or a tool (`FileDeploymentManager`), or
    /// read again by a reload, which only logs what is not valid.
    pub fn broker_config_validated(&self, validation: Validation) -> anyhow::Result<BrokerConfig> {
        let path = &self.broker_xml;
        let xml = self.broker_xml_text()?;
        let mut config = BrokerConfig::parse_validated(&xml, |n| self.lookup(n), validation)
            .map_err(|e| self.broker_xml_error(e))?;
        for w in &config.warnings {
            tracing::warn!("{}: {w}", path.display());
        }
        let lookup = |n: &str| self.lookup(n);
        let mut warnings = Vec::new();
        if let Some(sources) = self.broker_properties_sources() {
            warnings.extend(
                broker_properties::apply_sources(&mut config, &sources, &lookup)
                    .with_context(|| format!("reading broker properties {sources}"))?,
            );
        }
        warnings.extend(broker_properties::apply_system(
            &mut config,
            &self.system,
            &lookup,
        ));
        for w in &warnings {
            tracing::warn!("{w}");
        }
        config.warnings.extend(warnings);
        Ok(config)
    }

    /// What a change to reloads the configuration: `broker.xml` and the
    /// broker properties sources, a directory among them watched as itself
    /// (`ActiveMQServerImpl`'s reload callbacks), each once.
    pub fn watched(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.broker_xml.clone()];
        if let Some(sources) = self.broker_properties_sources() {
            for source in sources.split(',') {
                let path = PathBuf::from(source.split_once('?').map_or(source, |(p, _)| p));
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        }
        paths
    }

    /// `Configuration.resolvePropertiesSources`: the `broker.properties`
    /// system property, else `--properties`, else `etc/broker.properties`
    /// if it exists (`Run.execute`).
    fn broker_properties_sources(&self) -> Option<String> {
        self.system_property("broker.properties")
            .or_else(|| self.broker_properties.clone())
            .or_else(|| {
                let default = self.etc.join("broker.properties");
                self.etc_properties
                    .get_or_init(|| default.exists())
                    .then(|| default.display().to_string().replace('\\', "/"))
            })
    }

    /// The security of this instance. As in Artemis, the security manager
    /// exists with security disabled too: management adds users and
    /// security settings through it, though no check is made.
    pub fn security(&self, config: &BrokerConfig) -> anyhow::Result<Security> {
        let enabled = config.security_enabled;
        let (domain, certificate_domain) = match self
            .bootstrap
            .as_ref()
            .and_then(|b| b.security.clone())
        {
            None => (JaasSecurityManager::DEFAULT_DOMAIN.to_owned(), None),
            Some(BootstrapSecurity::Jaas {
                domain,
                certificate_domain,
            }) => (domain, certificate_domain),
            Some(BootstrapSecurity::Manager {
                class_name,
                properties,
            }) if class_name == BASIC_SECURITY_MANAGER => {
                return self.basic_security(config, &properties);
            }
            Some(BootstrapSecurity::Manager { class_name, .. }) => {
                bail!(
                    "security manager {class_name} is not supported; use <jaas-security> or ActiveMQBasicSecurityManager"
                )
            }
        };
        let path = self.etc.join("login.config");
        let login = if path.exists() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            match LoginConfig::parse(&text, |n| self.lookup(n)) {
                Ok(l) => l,
                Err(e) if !enabled => {
                    tracing::debug!("{}: {e}", path.display());
                    LoginConfig::default()
                }
                Err(e) => {
                    return Err(anyhow::Error::from(e))
                        .with_context(|| format!("reading {}", path.display()));
                }
            }
        } else {
            if enabled && config.jaas_configs.is_empty() {
                tracing::warn!(
                    "security is enabled but {} does not exist: every login will fail",
                    path.display()
                );
            }
            LoginConfig::default()
        };
        // `ConfigurationImpl.getAppConfigurationEntry`: a realm from broker
        // properties is used over the one in login.config.
        let realms = saranga_security::manager::realms(&config.jaas_configs);
        let in_use = login
            .entries
            .iter()
            .filter(|(n, _)| !realms.iter().any(|(r, _)| r == n))
            .chain(realms.iter());
        for (name, modules) in in_use.filter(|_| enabled) {
            for m in modules {
                if !ModuleFactory::supports(&m.class_name) {
                    tracing::warn!(
                        "{}: login module {} in {name} is not supported; logins that need it fail",
                        path.display(),
                        m.class_name
                    );
                }
            }
        }
        for d in std::iter::once(&domain).chain(certificate_domain.as_ref()) {
            if enabled
                && (path.exists() || !config.jaas_configs.is_empty())
                && login.entry(d).is_none()
                && !realms.iter().any(|(r, _)| r == d)
            {
                tracing::warn!(
                    "{}: no entry for the security domain {d}: logins through it fail",
                    path.display()
                );
            }
        }
        let manager = JaasSecurityManager::new(login, Some(&self.etc), domain, certificate_domain);
        manager.set_jaas_configs(realms);
        Ok(Security {
            store: Some(self.store(config, Arc::new(manager))?),
            ..Security::default()
        })
    }

    fn store(
        &self,
        config: &BrokerConfig,
        manager: Arc<dyn SecurityManager>,
    ) -> anyhow::Result<Arc<SecurityStore>> {
        Ok(Arc::new(SecurityStore::new(
            SecurityStoreConfig::from_broker(config),
            config.security_repository()?,
            manager,
        )))
    }

    /// `ActiveMQBasicSecurityManager.init`: users live in the bindings
    /// journal; the bootstrap credentials (`bootstrapUser`,
    /// `bootstrapPassword` and `bootstrapRole`, or `bootstrapUserFile` and
    /// `bootstrapRoleFile`) are added or updated at startup.
    fn basic_security(
        &self,
        config: &BrokerConfig,
        properties: &[(String, String)],
    ) -> anyhow::Result<Security> {
        let get = |k: &str| {
            properties
                .iter()
                .rev()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.clone())
        };
        let mut bootstrap_users = Vec::new();
        if let (Some(users), Some(roles)) = (get("bootstrapUserFile"), get("bootstrapRoleFile")) {
            // ClassloadingUtil.loadProperties: from the classpath, which
            // holds etc/.
            let read = |name: &str| -> anyhow::Result<Vec<(String, String)>> {
                let path = self.etc.join(name);
                let bytes =
                    std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
                Ok(saranga_security::properties::parse(&bytes))
            };
            let roles = read(&roles)?;
            for (user, password) in read(&users)? {
                let user_roles = roles
                    .iter()
                    .filter(|(_, members)| members.split(',').any(|m| m == user))
                    .map(|(role, _)| role.clone())
                    .collect();
                bootstrap_users.push((user, password, user_roles));
            }
        } else if let (Some(user), Some(password), Some(role)) = (
            get("bootstrapUser"),
            get("bootstrapPassword"),
            get("bootstrapRole"),
        ) {
            bootstrap_users.push((user, password, vec![role]));
        } else {
            tracing::warn!(
                "AMQ222299: No bootstrap credentials found. User management may not function."
            );
        }
        if !config.persistence_enabled {
            tracing::warn!(
                "the basic security manager keeps users in the journal, and persistence is off: every login will fail"
            );
        }
        let users = Users::new();
        let manager = BasicSecurityManager::new(users.clone());
        Ok(Security {
            store: Some(self.store(config, Arc::new(manager))?),
            users: Some(users),
            bootstrap_users,
        })
    }
}

fn absolute(p: &Path) -> PathBuf {
    let p = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
    // `canonicalize` on Windows gives `\\?\C:\...`, which Java never shows.
    match p.to_str().and_then(|s| s.strip_prefix(r"\\?\")) {
        Some(stripped) => PathBuf::from(stripped),
        None => p,
    }
}

/// The configuration URI of `artemis run` and the data tools, as `BrokerFactory` takes it: only
/// the `xml:` scheme, naming a bootstrap file (relative to the current
/// directory), which must exist.
pub fn bootstrap_path(uri: &str) -> anyhow::Result<PathBuf> {
    // `java.net.URI`'s scheme: a letter, then letters, digits, `+`, `-`
    // and `.`, before the first `:`.
    let scheme = uri.split_once(':').map(|(s, _)| s).filter(|s| {
        s.starts_with(|c: char| c.is_ascii_alphabetic())
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    });
    let Some(scheme) = scheme else {
        bail!("Invalid configuration URI, no scheme specified: {uri}");
    };
    if scheme != "xml" {
        bail!("Invalid configuration URI, can't find configuration scheme: {scheme}");
    }
    let path = PathBuf::from(&uri[scheme.len() + 1..]);
    if !path.exists() {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        bail!("Invalid configuration URI, can't find file: {name}");
    }
    Ok(path)
}

/// `artemis.URI.instance`: the path as a URI path, `/C:/x%20y` on Windows.
fn uri_path(p: &Path) -> String {
    let s = p.display().to_string().replace('\\', "/");
    let s = if s.starts_with('/') {
        s
    } else {
        format!("/{s}")
    };
    s.replace(' ', "%20")
}
