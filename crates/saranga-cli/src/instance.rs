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
use saranga_config::{BootstrapConfig, BootstrapSecurity, BrokerConfig, broker_properties};
use saranga_security::jaas::{ControlFlag, ModuleEntry, ModuleFactory};
use saranga_security::{JaasSecurityManager, LoginConfig, SecurityStore, SecurityStoreConfig};

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
    /// System properties given on the command line, in order.
    system: Vec<(String, String)>,
    /// `--properties`: broker properties sources.
    broker_properties: Option<String>,
}

impl Instance {
    /// An instance from its `etc/` directory: `bootstrap.xml` if there is
    /// one, else `broker.xml` next to it.
    pub fn from_etc(etc: &Path, options: Options) -> anyhow::Result<Self> {
        let etc = absolute(etc);
        let mut instance = Self::new(etc.clone(), etc.join("broker.xml"), options);
        let bootstrap_path = etc.join("bootstrap.xml");
        if bootstrap_path.exists() {
            let bootstrap = instance.read_bootstrap(&bootstrap_path)?;
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

    fn new(etc: PathBuf, broker_xml: PathBuf, options: Options) -> Self {
        // `<instance>/etc` -> `<instance>`.
        let dir = etc
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| etc.clone());
        let slash = |p: &Path| p.display().to_string().replace('\\', "/");
        let mut properties = HashMap::new();
        properties.insert("artemis.instance", slash(&dir));
        properties.insert(
            "artemis.home",
            std::env::var("ARTEMIS_HOME").unwrap_or_else(|_| slash(&dir)),
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
        }
    }

    fn system_property(&self, name: &str) -> Option<String> {
        self.system
            .iter()
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }

    /// A system property as `artemis run` defines it, then an environment
    /// variable (`XMLUtil.replaceSystemPropsInString`).
    pub fn lookup(&self, name: &str) -> Option<String> {
        self.system_property(name)
            .or_else(|| self.properties.get(name).cloned())
            .or_else(|| std::env::var(name).ok())
    }

    fn read_bootstrap(&self, path: &Path) -> anyhow::Result<BootstrapConfig> {
        let xml =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let bootstrap = BootstrapConfig::parse(&xml, |n| self.lookup(n))
            .with_context(|| format!("reading {}", path.display()))?;
        for w in &bootstrap.warnings {
            tracing::warn!("{}: {w}", path.display());
        }
        Ok(bootstrap)
    }

    pub fn broker_config(&self) -> anyhow::Result<BrokerConfig> {
        let path = &self.broker_xml;
        let xml =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut config = BrokerConfig::parse_with(&xml, |n| self.lookup(n))
            .with_context(|| format!("reading {}", path.display()))?;
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

    /// `Configuration.resolvePropertiesSources`: the `broker.properties`
    /// system property, else `--properties`, else `etc/broker.properties`
    /// if it exists (`Run.execute`).
    fn broker_properties_sources(&self) -> Option<String> {
        self.system_property("broker.properties")
            .or_else(|| self.broker_properties.clone())
            .or_else(|| {
                let default = self.etc.join("broker.properties");
                default
                    .exists()
                    .then(|| default.display().to_string().replace('\\', "/"))
            })
    }

    /// The security store for this instance, `None` with security off.
    pub fn security(&self, config: &BrokerConfig) -> anyhow::Result<Option<Arc<SecurityStore>>> {
        if !config.security_enabled {
            return Ok(None);
        }
        let (domain, certificate_domain) =
            match self.bootstrap.as_ref().and_then(|b| b.security.clone()) {
                None => (JaasSecurityManager::DEFAULT_DOMAIN.to_owned(), None),
                Some(BootstrapSecurity::Jaas {
                    domain,
                    certificate_domain,
                }) => (domain, certificate_domain),
                Some(BootstrapSecurity::Manager { class_name, .. }) => {
                    bail!("security manager {class_name} is not supported; use <jaas-security>")
                }
            };
        let path = self.etc.join("login.config");
        let mut login = if path.exists() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            LoginConfig::parse(&text, |n| self.lookup(n))
                .with_context(|| format!("reading {}", path.display()))?
        } else {
            if config.jaas_configs.is_empty() {
                tracing::warn!(
                    "security is enabled but {} does not exist: every login will fail",
                    path.display()
                );
            }
            LoginConfig::default()
        };
        // `ConfigurationImpl.getAppConfigurationEntry`: a realm from broker
        // properties replaces the one in login.config.
        for (realm, modules) in &config.jaas_configs {
            let entries = modules
                .iter()
                .map(|m| ModuleEntry {
                    class_name: m.login_module_class.clone(),
                    flag: if m.control_flag.is_empty() {
                        ControlFlag::Required
                    } else {
                        ControlFlag::parse(&m.control_flag).unwrap_or(ControlFlag::Required)
                    },
                    options: m.params.clone(),
                })
                .collect();
            login.entries.retain(|(n, _)| n != realm);
            login.entries.push((realm.clone(), entries));
        }
        for (name, modules) in &login.entries {
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
            if (path.exists() || !config.jaas_configs.is_empty()) && login.entry(d).is_none() {
                tracing::warn!(
                    "{}: no entry for the security domain {d}: logins through it fail",
                    path.display()
                );
            }
        }
        let manager = JaasSecurityManager::new(login, Some(&self.etc), domain, certificate_domain);
        let store = SecurityStore::new(
            SecurityStoreConfig::from_broker(config),
            config.security_repository()?,
            Arc::new(manager),
        );
        Ok(Some(Arc::new(store)))
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
