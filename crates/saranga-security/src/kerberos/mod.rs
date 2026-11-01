//! Kerberos, through the system's Kerberos library loaded when first used:
//! MIT Kerberos (1.11 or later) on Linux, FreeBSD and macOS (Homebrew's or
//! MacPorts'), and MIT Kerberos for Windows on Windows when it is
//! installed, else Windows's own SSPI. Nothing is linked at build time.
//!
//! This is what Java's Kerberos gives Artemis: the credentials
//! `Krb5LoginModule` obtains (a TGT from a password, a keytab or a ticket
//! cache, and an acceptor's keytab), and GSS-API security contexts over
//! them, for AMQP's GSSAPI mechanism and LDAP's GSSAPI binds.

mod mit;
#[cfg(windows)]
mod sspi;

use std::fmt;
use std::sync::{Arc, OnceLock};

/// A failure of the Kerberos library, or none to use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// A client's credentials, held for as long as the subject that has them
/// (a TGT in a memory ticket cache, or the ticket cache named).
#[derive(Clone)]
pub struct Initiator(pub(crate) Arc<dyn InitiatorCredential>);

impl fmt::Debug for Initiator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Initiator({})", self.0.describe())
    }
}

impl PartialEq for Initiator {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Initiator {}

pub(crate) trait InitiatorCredential: Send + Sync {
    fn describe(&self) -> String;
    fn as_any(&self) -> &dyn std::any::Any;
}

/// What an acceptor accepts with: a keytab's keys, for one principal or
/// (unbound, `principal=*`) for any it has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acceptor {
    Keytab {
        /// The keytab, or the library's default.
        keytab: Option<String>,
        /// The principal, or none for any in the keytab.
        principal: Option<String>,
    },
    /// Keys made from a password (`storeKey` without a keytab, as an
    /// acceptor). The library cannot accept with them.
    PasswordKeys { principal: String },
}

/// A GSS-API security context, as an acceptor or an initiator.
pub trait SecurityContext: Send {
    /// Takes the peer's token; gives the token to send back, if any.
    fn step(&mut self, token: &[u8]) -> Result<Option<Vec<u8>>>;
    fn is_established(&self) -> bool;
    /// The initiator's name (`getSrcName`), once established.
    fn source_name(&self) -> Result<String>;
    /// `wrap` with QOP 0, integrity only.
    fn wrap(&mut self, data: &[u8]) -> Result<Vec<u8>>;
    /// `unwrap`, refusing a token out of sequence, old or replayed.
    fn unwrap(&mut self, data: &[u8]) -> Result<Vec<u8>>;
}

/// A Kerberos library.
pub(crate) trait Backend: Send + Sync {
    fn name(&self) -> &str;
    /// A principal's full name (`PrincipalName`): the default realm added
    /// to one without a realm.
    fn canonical_name(&self, name: &str) -> Result<String>;
    /// A TGT from the KDC for a password.
    fn initiate_with_password(&self, principal: &str, password: &str) -> Result<Initiator>;
    /// Whether the keytab (or the default) has keys for the principal.
    fn keytab_has_keys(&self, keytab: Option<&str>, principal: &str) -> bool;
    /// A TGT from the KDC for the keytab's keys.
    fn initiate_with_keytab(&self, keytab: Option<&str>, principal: &str) -> Result<Initiator>;
    /// The TGT of a ticket cache (the default one unless named), if it is
    /// for `principal` (any when none) and still current, renewed first
    /// when `renew` and past half its life. Gives its client's name.
    fn ticket_cache(
        &self,
        principal: Option<&str>,
        cache: Option<&str>,
        renew: bool,
    ) -> Result<Option<(String, Initiator)>>;
    fn accept(&self, acceptor: &Acceptor) -> Result<Box<dyn SecurityContext>>;
    /// A context to `service` (`ldap@host`, host-based), asking for mutual
    /// authentication when `mutual`.
    fn initiate(
        &self,
        initiator: &Initiator,
        service: &str,
        mutual: bool,
    ) -> Result<Box<dyn SecurityContext>>;
}

/// The Kerberos library, loaded on first use.
pub(crate) fn backend() -> Result<&'static dyn Backend> {
    static BACKEND: OnceLock<std::result::Result<Box<dyn Backend>, Error>> = OnceLock::new();
    BACKEND
        .get_or_init(|| {
            let mit = mit::Mit::load();
            #[cfg(windows)]
            let mit = mit.or_else(|e| {
                tracing::debug!("{e}; using SSPI");
                sspi::Sspi::load()
            });
            let backend = mit?;
            tracing::debug!("Kerberos through {}", backend.name());
            Ok(backend)
        })
        .as_ref()
        .map(|b| b.as_ref())
        .map_err(Clone::clone)
}

/// Points the Kerberos library at the `krb5.conf` the
/// `java.security.krb5.conf` system property names, as Java reads it
/// (MIT reads `KRB5_CONFIG`). `java.security.krb5.realm` and
/// `java.security.krb5.kdc`, which stand for a configuration file in Java,
/// are not read: a warning says so. Called when the system properties are
/// set, before anything uses Kerberos.
pub fn apply_system_properties() {
    if let Some(conf) = saranga_config::system::property("java.security.krb5.conf") {
        // SAFETY: called once at startup, before the Kerberos library is
        // loaded and before the broker's threads read the environment.
        unsafe { std::env::set_var("KRB5_CONFIG", conf) };
    }
    for p in ["java.security.krb5.realm", "java.security.krb5.kdc"] {
        if saranga_config::system::property(p).is_some() {
            tracing::warn!("{p} is not supported: name the realm and its KDC in krb5.conf");
        }
    }
}

/// A context accepting with the keys a login obtained.
pub fn accept(acceptor: &Acceptor) -> Result<Box<dyn SecurityContext>> {
    backend()?.accept(acceptor)
}

/// A context to `service` (`ldap@host`) with the TGT a login obtained.
pub fn initiate(
    initiator: &Initiator,
    service: &str,
    mutual: bool,
) -> Result<Box<dyn SecurityContext>> {
    backend()?.initiate(initiator, service, mutual)
}

/// The service a host-based name like `ldap@host` stands for, as
/// `service/host` (the form SSPI takes).
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn service_principal(service: &str) -> String {
    match service.split_once('@') {
        Some((s, h)) => format!("{s}/{h}"),
        None => service.to_owned(),
    }
}

/// RFC 4752's security layers, as `GssKrb5Base` names them.
pub mod layer {
    pub const NO_PROTECTION: u8 = 1;
    pub const INTEGRITY_ONLY_PROTECTION: u8 = 2;
    pub const PRIVACY_PROTECTION: u8 = 4;
}
