//! Security managers: who a client is, and whether its roles allow a
//! check. Ported from `ActiveMQSecurityManager5`,
//! `ActiveMQJAASSecurityManager` and `SecurityManagerUtil`.

use std::path::Path;
use std::sync::Arc;

use saranga_config::{CheckType, Role};

use crate::jaas::{self, Callbacks, LoginConfig, ModuleFactory, PeerCertificate, Subject};

/// What the security store knows about a client connection.
#[derive(Debug, Clone, Default)]
pub struct ConnectionInfo {
    /// As Artemis prints it: `/127.0.0.1:51234`.
    pub remote_address: String,
    /// The TLS client certificate chain, if any.
    pub certificates: Option<Arc<[PeerCertificate]>>,
    /// The acceptor's `securityDomain`, which overrides the broker's.
    pub security_domain: Option<String>,
    /// The connection's ID, for the audit log; `None` for a web login.
    pub connection_id: Option<String>,
}

impl ConnectionInfo {
    /// `CertificateUtil.getCertSubjectDN`.
    pub fn cert_subject_dn(&self) -> &str {
        self.certificates
            .as_deref()
            .and_then(|c| c.first())
            .map(|c| c.subject_dn_legacy.as_str())
            .unwrap_or(CERT_SUBJECT_DN_UNAVAILABLE)
    }
}

/// `CertificateUtil.CERT_SUBJECT_DN_UNAVAILABLE`.
pub const CERT_SUBJECT_DN_UNAVAILABLE: &str = "unavailable";

/// The login failed in a way that must not be cached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoCache(pub String);

/// `ActiveMQSecurityManager5`.
pub trait SecurityManager: Send + Sync {
    /// The subject for these credentials, `Ok(None)` if they are refused.
    fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        connection: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache>;

    /// Does `subject` hold a role that `roles` grant `check` to?
    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, address: &str)
    -> bool;

    fn user_from_subject(&self, subject: &Subject) -> Option<String> {
        subject.user().map(str::to_owned)
    }

    /// The JAAS manager, for the management operations that edit the
    /// files of its `PropertiesLoginModule`.
    fn as_jaas(&self) -> Option<&JaasSecurityManager> {
        None
    }
}

/// `SecurityManagerUtil.authorize`: the subject has a role principal named
/// like one of the roles granted the check.
///
/// Artemis turns a granted role named `*` into a principal that claims to
/// equal anything, but looks the subject's roles up in a `HashSet` of
/// those, where `RolePrincipal.equals` decides; so `*` matches no role
/// there, and none here.
pub fn authorize_roles(subject: &Subject, roles: &[Role], check: CheckType) -> bool {
    subject
        .roles()
        .any(|held| roles.iter().any(|r| r.allows(check) && r.name == held))
}

/// `ActiveMQJAASSecurityManager`: logs in through a JAAS configuration
/// entry (the domain), or through the certificate domain when the client
/// presented a certificate and one is configured.
pub struct JaasSecurityManager {
    config: LoginConfig,
    modules: ModuleFactory,
    domain: String,
    certificate_domain: Option<String>,
    overrides: std::sync::RwLock<Overrides>,
}

/// Login configurations by realm, as broker properties give them
/// (`jaasConfigs`).
pub type Realms = Vec<(String, Vec<jaas::ModuleEntry>)>;

/// `JaasAppConfiguration.asAppConfigurationEntry` for each realm of
/// `jaasConfigs`. A module without a control flag is required.
pub fn realms(configs: &[(String, Vec<saranga_config::JaasModuleConfig>)]) -> Realms {
    configs
        .iter()
        .map(|(realm, modules)| {
            let entries = modules
                .iter()
                .map(|m| jaas::ModuleEntry {
                    class_name: m.login_module_class.clone(),
                    flag: if m.control_flag.is_empty() {
                        jaas::ControlFlag::Required
                    } else {
                        jaas::ControlFlag::parse(&m.control_flag)
                            .unwrap_or(jaas::ControlFlag::Required)
                    },
                    options: m.params.clone(),
                })
                .collect();
            (realm.clone(), entries)
        })
        .collect()
}

/// The JAAS configurations installed over `login.config`.
///
/// `ConfigurationImpl.parseProperties` installs the configuration it fills,
/// when it has `jaasConfigs`, as the JVM's JAAS configuration, falling back
/// to the one installed before it (`initJaasConfigOverride`). The broker's
/// own configuration is installed so at startup and gets each reload's
/// `jaasConfigs` (`setJaasConfigs`); each reload that has some installs the
/// configuration it read, which keeps them. A realm is looked up from the
/// last installed back to `login.config`, so a realm a later reload drops
/// is still found in an earlier reload's configuration, as in Artemis.
#[derive(Default)]
struct Overrides {
    /// The broker configuration's `jaasConfigs`.
    live: Realms,
    /// Oldest first.
    installed: Vec<Installed>,
}

enum Installed {
    /// The broker's configuration, whose realms are `live`.
    Live,
    /// A reload's configuration, with the realms it read.
    Read(Realms),
}

impl Overrides {
    fn entry(&self, realm: &str) -> Option<Vec<jaas::ModuleEntry>> {
        self.installed.iter().rev().find_map(|i| {
            let realms = match i {
                Installed::Live => &self.live,
                Installed::Read(r) => r,
            };
            realms
                .iter()
                .find(|(n, _)| n == realm)
                .map(|(_, e)| e.clone())
        })
    }
}

impl JaasSecurityManager {
    /// `artemis run`'s default domain (`<jaas-security domain>`).
    pub const DEFAULT_DOMAIN: &str = "activemq";

    /// `base_dir` is where relative files named in the configuration live:
    /// the directory of `login.config`.
    pub fn new(
        config: LoginConfig,
        base_dir: Option<&Path>,
        domain: impl Into<String>,
        certificate_domain: Option<String>,
    ) -> Self {
        Self {
            config,
            modules: ModuleFactory::new(base_dir.map(Path::to_path_buf)),
            domain: domain.into(),
            certificate_domain,
            overrides: std::sync::RwLock::default(),
        }
    }

    /// `login.config`, without the realms of `jaasConfigs`.
    pub fn config(&self) -> &LoginConfig {
        &self.config
    }

    fn overrides(&self) -> std::sync::RwLockReadGuard<'_, Overrides> {
        self.overrides.read().unwrap_or_else(|e| e.into_inner())
    }

    /// The startup configuration's `jaasConfigs`: installed over
    /// `login.config` when there are some.
    pub fn set_jaas_configs(&self, realms: Realms) {
        let mut o = self.overrides.write().unwrap_or_else(|e| e.into_inner());
        if !realms.is_empty() {
            o.installed.push(Installed::Live);
        }
        o.live = realms;
    }

    /// A reload's `jaasConfigs`: the configuration it read is installed
    /// when it has some, then the broker's configuration takes them.
    pub fn reload_jaas_configs(&self, realms: Realms) {
        let mut o = self.overrides.write().unwrap_or_else(|e| e.into_inner());
        if !realms.is_empty() {
            o.installed.push(Installed::Read(realms.clone()));
        }
        o.live = realms;
    }

    /// `Configuration.getConfiguration().getAppConfigurationEntry`: the
    /// realm's login modules, from `jaasConfigs` or `login.config`.
    pub fn entry(&self, realm: &str) -> Option<Vec<jaas::ModuleEntry>> {
        self.overrides()
            .entry(realm)
            .or_else(|| self.config.entry(realm).map(<[_]>::to_vec))
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn certificate_domain(&self) -> Option<&str> {
        self.certificate_domain.as_deref()
    }
}

impl SecurityManager for JaasSecurityManager {
    fn as_jaas(&self) -> Option<&JaasSecurityManager> {
        Some(self)
    }

    fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        connection: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        let domain = match (&connection.security_domain, &self.certificate_domain) {
            (Some(d), _) => d.as_str(),
            (None, Some(cert_domain))
                if !cert_domain.is_empty() && connection.certificates.is_some() =>
            {
                cert_domain.as_str()
            }
            _ => self.domain.as_str(),
        };
        let Some(entries) = self.entry(domain) else {
            tracing::debug!("No LoginModules configured for {domain}");
            return Ok(None);
        };
        let callbacks = Callbacks {
            user,
            password,
            certificates: connection.certificates.as_deref(),
            remote_address: Some(&connection.remote_address),
        };
        match jaas::login(&entries, &self.modules, &callbacks) {
            Ok(subject) => Ok(Some(subject)),
            Err(e) if e.no_cache => Err(NoCache(e.message)),
            Err(e) => {
                tracing::debug!("login failed for {user:?}: {e}");
                Ok(None)
            }
        }
    }

    fn authorize(
        &self,
        subject: &Subject,
        roles: &[Role],
        check: CheckType,
        _address: &str,
    ) -> bool {
        authorize_roles(subject, roles, check)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn realm(name: &str, class: &str) -> (String, Vec<jaas::ModuleEntry>) {
        (
            name.to_owned(),
            vec![jaas::ModuleEntry {
                class_name: class.to_owned(),
                flag: jaas::ControlFlag::Required,
                options: Vec::new(),
            }],
        )
    }

    fn class(m: &JaasSecurityManager, realm: &str) -> Option<String> {
        m.entry(realm).map(|e| e[0].class_name.clone())
    }

    fn manager() -> JaasSecurityManager {
        let file = LoginConfig::parse("activemq { FileModule required; };", |_| None).unwrap();
        JaasSecurityManager::new(file, None, "activemq", None)
    }

    /// `initJaasConfigOverride`: the startup configuration is installed
    /// over login.config and takes each reload's realms; a reload with
    /// realms installs its own configuration, which keeps them, so a realm
    /// dropped later is still found there.
    #[test]
    fn jaas_configs_override_login_config_as_artemis_installs_them() {
        let m = manager();
        m.set_jaas_configs(vec![realm("activemq", "Startup")]);
        assert_eq!(class(&m, "activemq").as_deref(), Some("Startup"));

        m.reload_jaas_configs(vec![realm("activemq", "First")]);
        assert_eq!(class(&m, "activemq").as_deref(), Some("First"));

        m.reload_jaas_configs(vec![realm("other", "Second")]);
        assert_eq!(class(&m, "other").as_deref(), Some("Second"));
        assert_eq!(class(&m, "activemq").as_deref(), Some("First"));

        m.reload_jaas_configs(Vec::new());
        assert_eq!(class(&m, "other").as_deref(), Some("Second"));
    }

    /// Without realms at startup, login.config serves until a reload
    /// brings some, and keeps serving the realms none names.
    #[test]
    fn login_config_serves_until_a_reload_brings_realms() {
        let m = manager();
        m.set_jaas_configs(Vec::new());
        assert_eq!(class(&m, "activemq").as_deref(), Some("FileModule"));
        m.reload_jaas_configs(Vec::new());
        assert_eq!(class(&m, "activemq").as_deref(), Some("FileModule"));
        m.reload_jaas_configs(vec![realm("activemq", "Guest")]);
        assert_eq!(class(&m, "activemq").as_deref(), Some("Guest"));
        assert_eq!(class(&m, "missing"), None);
        m.reload_jaas_configs(Vec::new());
        assert_eq!(class(&m, "activemq").as_deref(), Some("Guest"));
    }
}
