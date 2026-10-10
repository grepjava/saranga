//! Login by TLS client certificate.
//!
//! - `TextFileCertificateLoginModule`: a users file maps user names to
//!   certificate subjects (`user=CN=client, O=acme`, or a `/regex/`), a
//!   roles file maps roles to users.
//! - `ExternalCertificateLoginModule`: the subject itself is the user name;
//!   roles may come from subject alternative name URIs.
//!
//! Both match `X509Certificate.getSubjectDN().getName()`, the form with a
//! space after each comma.

use std::sync::Arc;

use super::{
    Callbacks, LoadedProperties, LoginError, LoginModule, ModuleEntry, ModuleFactory, Principal,
    Subject,
};

pub const USER_FILE_PROP_NAME: &str = "org.apache.activemq.jaas.textfiledn.user";
pub const ROLE_FILE_PROP_NAME: &str = "org.apache.activemq.jaas.textfiledn.role";
pub const SAN_URI_ROLE_PREFIX_PROP: &str = "sanUriRolePrefix";

pub(crate) struct TextFileCertificateLoginModule {
    users: Arc<LoadedProperties>,
    roles: Arc<LoadedProperties>,
    user: Option<String>,
}

impl TextFileCertificateLoginModule {
    pub(crate) fn new(factory: &ModuleFactory, entry: &ModuleEntry) -> Self {
        let reload = entry.bool_option("reload");
        let file = |prop: &str| factory.file(entry, entry.option(prop).unwrap_or(""));
        if entry.bool_option("normalise") {
            tracing::warn!(
                "TextFileCertificateLoginModule: normalise is not supported; subjects are matched as written"
            );
        }
        Self {
            users: factory.properties(&file(USER_FILE_PROP_NAME), reload),
            roles: factory.properties(&file(ROLE_FILE_PROP_NAME), reload),
            user: None,
        }
    }

    /// `getUserNameForCertificates`: an exact subject, else the first
    /// `/regex/` that matches all of it.
    fn user_for(&self, dn: &str) -> Option<String> {
        let exact = self
            .users
            .entries
            .iter()
            .rev()
            .find(|(_, v)| !LoadedProperties::is_regexp(v) && v == dn);
        if let Some((user, _)) = exact {
            return Some(user.clone());
        }
        self.users.entries.iter().find_map(|(user, v)| {
            if !LoadedProperties::is_regexp(v) {
                return None;
            }
            let pattern = format!("^(?:{})$", &v[1..v.len() - 1]);
            match regex::Regex::new(&pattern) {
                Ok(re) if re.is_match(dn) => Some(user.clone()),
                Ok(_) => None,
                Err(e) => {
                    tracing::warn!("Ignoring invalid regexp: {v}: {e}");
                    None
                }
            }
        })
    }
}

impl LoginModule for TextFileCertificateLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        let Some(certs) = callbacks.certificates else {
            return Err(LoginError::new(
                "Client certificates not found. Cannot authenticate.",
            ));
        };
        let dn = certs
            .first()
            .map(|c| c.subject_dn_legacy.as_str())
            .unwrap_or_default();
        match self.user_for(dn) {
            Some(user) => {
                self.user = Some(user);
                Ok(true)
            }
            None => Err(LoginError::new(format!(
                "No user for client certificate: {dn}"
            ))),
        }
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let Some(user) = self.user.take() else {
            // `new UserPrincipal(null)` throws.
            return Err(LoginError::new("name cannot be null"));
        };
        subject.add(Principal::User(user.clone()));
        if let Some(roles) = self.roles.inverted_values().get(user.as_str()) {
            for role in roles {
                subject.add(Principal::Role((*role).to_owned()));
            }
        }
        Ok(true)
    }
}

pub(crate) struct ExternalCertificateLoginModule {
    san_uri_role_prefix: Option<String>,
    user: Option<String>,
    roles: Vec<String>,
}

impl ExternalCertificateLoginModule {
    pub(crate) fn new(entry: &ModuleEntry) -> Self {
        Self {
            san_uri_role_prefix: entry.option(SAN_URI_ROLE_PREFIX_PROP).map(str::to_owned),
            user: None,
            roles: Vec::new(),
        }
    }
}

impl LoginModule for ExternalCertificateLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        let first = callbacks.certificates.and_then(|c| c.first());
        self.user = first.map(|c| c.subject_dn_legacy.clone());
        if let (Some(cert), Some(prefix)) = (first, &self.san_uri_role_prefix) {
            self.roles = cert
                .san_uris
                .iter()
                .filter_map(|uri| uri.strip_prefix(prefix.as_str()))
                .map(str::to_owned)
                .collect();
        }
        Ok(self.user.is_some())
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        if let Some(user) = self.user.take() {
            subject.add(Principal::User(user));
            for role in self.roles.drain(..) {
                subject.add(Principal::Role(role));
            }
        }
        Ok(true)
    }
}
