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
        }
    }

    pub fn config(&self) -> &LoginConfig {
        &self.config
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
        let Some(entries) = self.config.entry(domain) else {
            tracing::debug!("No LoginModules configured for {domain}");
            return Ok(None);
        };
        let callbacks = Callbacks {
            user,
            password,
            certificates: connection.certificates.as_deref(),
            remote_address: Some(&connection.remote_address),
        };
        match jaas::login(entries, &self.modules, &callbacks) {
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
