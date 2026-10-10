//! JAAS: login configurations, login modules and the login sequence.
//!
//! [`login`] is `javax.security.auth.login.LoginContext.login`: every
//! module of the entry logs in, honouring the control flags, then every
//! module commits, adding its principals to the subject. As in the JDK,
//! commit is called on each module whether or not its own login
//! succeeded, which is how a `PropertiesLoginModule` placed after a
//! certificate module adds roles for the user the certificate identified.

mod certificate;
pub mod config;
mod guest;
mod properties_module;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

pub use config::{ControlFlag, LoginConfig, ModuleEntry, ParseError};

/// The package of Artemis's own login modules.
pub const ARTEMIS_JAAS: &str = "org.apache.activemq.artemis.spi.core.security.jaas";

/// A principal of an authenticated subject: Artemis's `UserPrincipal` and
/// `RolePrincipal`, or another class some module adds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Principal {
    User(String),
    Role(String),
    Other { class: String, name: String },
}

/// `javax.security.auth.Subject`: a set of principals, kept in the order
/// they were added (the JDK's `SecureSet` is a list too).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subject {
    principals: Vec<Principal>,
}

impl Subject {
    pub fn add(&mut self, principal: Principal) {
        if !self.principals.contains(&principal) {
            self.principals.push(principal);
        }
    }

    pub fn principals(&self) -> &[Principal] {
        &self.principals
    }

    pub fn users(&self) -> impl Iterator<Item = &str> {
        self.principals.iter().filter_map(|p| match p {
            Principal::User(u) => Some(u.as_str()),
            _ => None,
        })
    }

    pub fn roles(&self) -> impl Iterator<Item = &str> {
        self.principals.iter().filter_map(|p| match p {
            Principal::Role(r) => Some(r.as_str()),
            _ => None,
        })
    }

    /// `SecurityManagerUtil.getUserFromSubject`: the first user principal.
    pub fn user(&self) -> Option<&str> {
        self.users().next()
    }
}

/// A peer certificate of a TLS connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerCertificate {
    /// DER encoding.
    pub der: Vec<u8>,
    /// `X500Principal.getName()`: the subject DN in RFC 2253 form.
    pub subject_dn: String,
    /// `X509Certificate.getSubjectDN().getName()`, the form Artemis uses in
    /// messages and the certificate login modules match.
    pub subject_dn_legacy: String,
    /// The certificate's subject alternative name URIs.
    pub san_uris: Vec<String>,
}

/// What the callback handler (`JaasCallbackHandler`) offers modules.
#[derive(Debug, Clone, Copy, Default)]
pub struct Callbacks<'a> {
    pub user: Option<&'a str>,
    pub password: Option<&'a str>,
    /// The client's certificate chain, client certificate first.
    pub certificates: Option<&'a [PeerCertificate]>,
    pub remote_address: Option<&'a str>,
}

/// `LoginException`. `no_cache` is Artemis's `NoCacheLoginException`: the
/// failure must not be cached (the LDAP server was unreachable, say).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct LoginError {
    pub message: String,
    pub no_cache: bool,
}

impl LoginError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            no_cache: false,
        }
    }

    pub fn no_cache(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            no_cache: true,
        }
    }
}

/// `javax.security.auth.spi.LoginModule`, after `initialize`.
pub trait LoginModule: Send {
    /// `Ok(false)` means the module is to be ignored.
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError>;
    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError>;
}

/// Creates login modules by class name. Holds what modules share across
/// logins: the directory relative file names resolve against and the
/// properties files already read.
pub struct ModuleFactory {
    base_dir: Option<PathBuf>,
    properties: PropertiesCache,
}

impl ModuleFactory {
    /// `base_dir` is the directory of the login configuration file, which
    /// Artemis's modules resolve their files against unless a module has a
    /// `baseDir` option.
    pub fn new(base_dir: Option<PathBuf>) -> Self {
        Self {
            base_dir,
            properties: PropertiesCache::default(),
        }
    }

    /// `PropertiesLoader.FileNameKey`: `baseDir` option, else the login
    /// configuration's directory.
    pub(crate) fn file(&self, entry: &ModuleEntry, name: &str) -> PathBuf {
        let base = entry
            .option("baseDir")
            .map(PathBuf::from)
            .or_else(|| self.base_dir.clone());
        match base {
            Some(dir) => dir.join(name),
            None => PathBuf::from(name),
        }
    }

    pub(crate) fn properties(&self, path: &Path, reload: bool) -> Arc<LoadedProperties> {
        self.properties.obtain(path, reload)
    }

    pub fn create(&self, entry: &ModuleEntry) -> Result<Box<dyn LoginModule>, LoginError> {
        let class = entry.class_name.as_str();
        let simple = class
            .strip_prefix(ARTEMIS_JAAS)
            .and_then(|s| s.strip_prefix('.'));
        match simple {
            Some("PropertiesLoginModule") => Ok(Box::new(
                properties_module::PropertiesLoginModule::new(self, entry),
            )),
            Some("GuestLoginModule") => Ok(Box::new(guest::GuestLoginModule::new(entry))),
            Some("TextFileCertificateLoginModule") => Ok(Box::new(
                certificate::TextFileCertificateLoginModule::new(self, entry),
            )),
            Some("ExternalCertificateLoginModule") => Ok(Box::new(
                certificate::ExternalCertificateLoginModule::new(entry),
            )),
            _ => Err(LoginError::new(format!(
                "unable to instantiate LoginModule {class}: not supported by Saranga"
            ))),
        }
    }

    /// Is this a module class [`create`](Self::create) knows?
    pub fn supports(class: &str) -> bool {
        matches!(
            class
                .strip_prefix(ARTEMIS_JAAS)
                .and_then(|s| s.strip_prefix('.')),
            Some(
                "PropertiesLoginModule"
                    | "GuestLoginModule"
                    | "TextFileCertificateLoginModule"
                    | "ExternalCertificateLoginModule"
            )
        )
    }
}

/// `LoginContext.login` over the modules of one entry.
pub fn login(
    entries: &[ModuleEntry],
    factory: &ModuleFactory,
    callbacks: &Callbacks<'_>,
) -> Result<Subject, LoginError> {
    let mut modules: Vec<Option<Box<dyn LoginModule>>> = entries.iter().map(|_| None).collect();
    let mut subject = Subject::default();
    invoke(entries, &mut modules, factory, |m| m.login(callbacks))?;
    invoke(entries, &mut modules, factory, |m| m.commit(&mut subject))?;
    Ok(subject)
}

/// `LoginContext.invoke` for the login or commit phase.
fn invoke(
    entries: &[ModuleEntry],
    modules: &mut [Option<Box<dyn LoginModule>>],
    factory: &ModuleFactory,
    mut call: impl FnMut(&mut dyn LoginModule) -> Result<bool, LoginError>,
) -> Result<(), LoginError> {
    let mut first_error: Option<LoginError> = None;
    let mut first_required_error: Option<LoginError> = None;
    let mut success = false;
    for (entry, slot) in entries.iter().zip(modules.iter_mut()) {
        let result = match slot {
            Some(m) => call(m.as_mut()),
            None => match factory.create(entry) {
                Ok(m) => call(slot.insert(m).as_mut()),
                Err(e) => Err(e),
            },
        };
        match result {
            Ok(true) => {
                if entry.flag == ControlFlag::Sufficient && first_required_error.is_none() {
                    return Ok(());
                }
                success = true;
            }
            Ok(false) => {}
            Err(e) => match entry.flag {
                ControlFlag::Requisite => return Err(first_required_error.unwrap_or(e)),
                ControlFlag::Required => {
                    first_required_error.get_or_insert(e);
                }
                ControlFlag::Sufficient | ControlFlag::Optional => {
                    first_error.get_or_insert(e);
                }
            },
        }
    }
    if let Some(e) = first_required_error {
        Err(e)
    } else if !success {
        Err(first_error.unwrap_or_else(|| LoginError::new("Login Failure: all modules ignored")))
    } else {
        Ok(())
    }
}

/// A properties file as last read (`ReloadableProperties`).
#[derive(Debug, Default)]
pub(crate) struct LoadedProperties {
    pub entries: Vec<(String, String)>,
}

impl LoadedProperties {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// `looksLikeRegexp`: `/.../`.
    pub fn is_regexp(value: &str) -> bool {
        value.len() > 2 && value.starts_with('/') && value.ends_with('/')
    }

    /// `invertedPropertiesValuesMap`: each comma-separated (untrimmed) value
    /// mapped to the keys that list it.
    pub fn inverted_values(&self) -> HashMap<&str, Vec<&str>> {
        let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
        for (key, value) in &self.entries {
            for item in saranga_config_split(value) {
                let keys = out.entry(item).or_default();
                if !keys.contains(&key.as_str()) {
                    keys.push(key);
                }
            }
        }
        out
    }
}

/// `String.split(",")`: trailing empty strings dropped.
fn saranga_config_split(s: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = s.split(',').collect();
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    if parts.iter().all(|p| p.is_empty()) && !s.is_empty() {
        parts.clear();
    }
    parts
}

struct CacheEntry {
    reload: bool,
    loaded_at: Option<SystemTime>,
    props: Arc<LoadedProperties>,
}

/// `PropertiesLoader.staticCache`: files by path, read once, and read again
/// on use if the module asked for `reload` and the file changed.
#[derive(Default)]
struct PropertiesCache {
    files: Mutex<HashMap<PathBuf, CacheEntry>>,
}

impl PropertiesCache {
    fn obtain(&self, path: &Path, reload: bool) -> Arc<LoadedProperties> {
        let mut files = self.files.lock().unwrap_or_else(|e| e.into_inner());
        let entry = files
            .entry(path.to_path_buf())
            .or_insert_with(|| CacheEntry {
                reload,
                loaded_at: None,
                props: Arc::default(),
            });
        let stale = match entry.loaded_at {
            None => true,
            // `hasModificationAfter`: modified less than a second before the
            // last read, or after it.
            Some(at) => {
                entry.reload
                    && std::fs::metadata(path)
                        .and_then(|m| m.modified())
                        .is_ok_and(|m| m + Duration::from_secs(1) > at)
            }
        };
        if stale {
            let now = SystemTime::now();
            let entries = match std::fs::read(path) {
                Ok(bytes) => crate::properties::parse(&bytes),
                Err(e) => {
                    tracing::error!(
                        "AMQ224085: Failed to load property PropsFile={}, reason: {e}",
                        path.display()
                    );
                    Vec::new()
                }
            };
            entry.props = Arc::new(LoadedProperties { entries });
            entry.loaded_at = Some(now);
        }
        entry.props.clone()
    }
}

#[cfg(test)]
mod tests;
