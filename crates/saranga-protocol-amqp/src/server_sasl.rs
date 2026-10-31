//! The broker's SASL mechanisms (`ServerSASLFactory`, `ServerSASL` and
//! `SASLResult`): each takes what the client sends, may answer with a
//! challenge, and has a result once it is done.

use std::sync::Arc;

use saranga_security::jaas::Callbacks;
use saranga_security::scram::{self, Scram};
use saranga_security::{ConnectionInfo, Principal};
use saranga_server::Broker;

use crate::scram::{ScramServer, ServerState, ascii_text};

/// `SASLResult`.
#[derive(Debug, Clone, Default)]
pub(crate) struct SaslResult {
    pub success: bool,
    pub user: Option<String>,
    /// PLAIN's password (`PlainSASLResult.getPassword`).
    pub password: Option<String>,
    /// The principals of the identity established (`getSubject`), which
    /// later logins on the connection see.
    pub principals: Option<Vec<Principal>>,
}

impl SaslResult {
    fn failed() -> Self {
        Self::default()
    }
}

/// What a mechanism works with: the broker, the connection, and the
/// acceptor's `saslLoginConfigScope`.
pub(crate) struct Env {
    pub broker: Arc<Broker>,
    pub info: Arc<ConnectionInfo>,
    pub login_config_scope: String,
}

/// A mechanism chosen by the client (`ServerSASL`).
pub(crate) enum ServerSasl {
    Anonymous,
    Plain(Option<SaslResult>),
    External {
        /// The TLS peer's principal (`X500Principal.getName()`).
        principal: String,
        result: Option<SaslResult>,
    },
    Scram(Box<JaasScram>),
}

impl ServerSasl {
    /// `MechanismFinder.getFactory(mechanism).create(...)`: `None` where
    /// there is no factory or it makes nothing (EXTERNAL without a TLS
    /// peer certificate).
    pub(crate) fn create(mechanism: &str, info: &ConnectionInfo) -> Option<Self> {
        match mechanism {
            "ANONYMOUS" => Some(Self::Anonymous),
            "PLAIN" => Some(Self::Plain(None)),
            "EXTERNAL" => {
                let cert = info.certificates.as_deref().and_then(|c| c.first());
                if cert.is_none() {
                    tracing::debug!("SASL EXTERNAL mechanism requires a TLS peer principal");
                }
                cert.map(|c| Self::External {
                    principal: c.subject_dn.clone(),
                    result: None,
                })
            }
            m => Scram::from_name(m).map(|s| Self::Scram(Box::new(JaasScram::new(s)))),
        }
    }

    /// `processSASL`: what the client sent (`None` when a `sasl-init` has
    /// no initial response), and the challenge to answer with, if any.
    pub(crate) async fn process(&mut self, env: &Env, bytes: Option<Vec<u8>>) -> Option<Vec<u8>> {
        match self {
            Self::Anonymous => None,
            Self::Plain(result) => {
                *result = Some(plain(env, bytes.as_deref()).await);
                None
            }
            Self::External { principal, result } => {
                if let Some(bytes) = bytes {
                    *result = Some(if bytes.is_empty() {
                        SaslResult {
                            success: true,
                            user: Some(principal.clone()),
                            ..SaslResult::default()
                        }
                    } else {
                        // No other identity is accepted.
                        SaslResult::failed()
                    });
                }
                Some(Vec::new())
            }
            Self::Scram(s) => s.process(env, bytes).await,
        }
    }

    /// `result`: `None` while the exchange goes on.
    pub(crate) fn result(&self) -> Option<SaslResult> {
        match self {
            Self::Anonymous => Some(SaslResult {
                success: true,
                ..SaslResult::default()
            }),
            Self::Plain(r) | Self::External { result: r, .. } => r.clone(),
            Self::Scram(s) => s.result(),
        }
    }
}

/// `PlainSASL`: the user and password of `[authzid] NUL user NUL password`
/// (`String.split`, so trailing empty parts go), validated when security is
/// enabled.
async fn plain(env: &Env, data: Option<&[u8]>) -> SaslResult {
    // Artemis fails here with a NullPointerException.
    let Some(data) = data else {
        return SaslResult::failed();
    };
    let text = String::from_utf8_lossy(data).into_owned();
    let mut parts: Vec<&str> = text.split('\0').collect();
    while parts.last() == Some(&"") {
        parts.pop();
    }
    let (user, password) = match parts.as_slice() {
        [u, p] | [_, u, p] => (Some((*u).to_owned()), Some((*p).to_owned())),
        _ => (None, None),
    };
    let success = match env.broker.security().cloned() {
        None => true,
        Some(store) => {
            let info = env.info.clone();
            let (u, p) = (user.clone(), password.clone());
            tokio::task::spawn_blocking(move || {
                store.authenticate(u.as_deref(), p.as_deref(), &info).is_ok()
            })
            .await
            .unwrap_or(false)
        }
    };
    SaslResult {
        success,
        user,
        password,
        principals: None,
    }
}

/// `JAASSCRAMServerSASL`: SCRAM-SHA-256 or SCRAM-SHA-512, the user's
/// SCRAM data from a login through `saslLoginConfigScope`, whose subject's
/// principals become the connection's.
pub(crate) struct JaasScram {
    scram: Scram,
    server: ScramServer,
    /// The user and the login's principals, once the client's first
    /// message is answered (`SCRAMSASLResult`).
    identity: Option<(String, Vec<Principal>)>,
    failed: bool,
}

impl JaasScram {
    fn new(scram: Scram) -> Self {
        Self {
            scram,
            server: ScramServer::new(scram, scram::random_uuid()),
            identity: None,
            failed: false,
        }
    }

    fn fail(&mut self, e: impl std::fmt::Display) {
        self.failed = true;
        tracing::warn!("SASL-SCRAM Authentication failed: {e}");
    }

    async fn process(&mut self, env: &Env, bytes: Option<Vec<u8>>) -> Option<Vec<u8>> {
        // Artemis fails here with a NullPointerException.
        let Some(bytes) = bytes else {
            self.failed = true;
            return None;
        };
        let message = ascii_text(&bytes);
        match self.server.state() {
            ServerState::Initial => {
                let user = match self.server.handle_client_first_message(&message) {
                    Ok(user) => user,
                    Err(e) => {
                        self.fail(e);
                        return None;
                    }
                };
                match self.user_data(env, &user).await {
                    Ok((data, principals)) => {
                        self.identity = Some((user, principals));
                        Some(self.server.prepare_first_message(data).into_bytes())
                    }
                    Err(e) => {
                        self.fail(e);
                        None
                    }
                }
            }
            ServerState::PreparedFirst => match self.server.prepare_final_message(&message) {
                Ok(last) => Some(last.into_bytes()),
                Err(e) => {
                    self.fail(e);
                    None
                }
            },
            _ => {
                self.failed = true;
                None
            }
        }
    }

    /// `aquireUserData`: a login through the scope, whose handler gives the
    /// user name and the mechanism, for the SCRAM data its subject holds.
    async fn user_data(
        &self,
        env: &Env,
        user: &str,
    ) -> Result<(scram::UserData, Vec<Principal>), String> {
        let scope = env.login_config_scope.clone();
        let Some(jaas) = env.broker.login_config().cloned() else {
            return Err(format!("No LoginModules configured for {scope}"));
        };
        let (user, mechanism) = (user.to_owned(), self.scram);
        tokio::task::spawn_blocking(move || {
            let callbacks = Callbacks {
                user: Some(&user),
                scram: Some(mechanism),
                ..Callbacks::default()
            };
            let subject = jaas.login(&scope, &callbacks).map_err(|e| e.message)?;
            match subject.scram_user_data() {
                Some(data) => Ok((data.clone(), subject.principals().to_vec())),
                None => Err(format!(
                    "can't aquire user data through configured login config scope ({scope})"
                )),
            }
        })
        .await
        .map_err(|e| e.to_string())?
    }

    fn result(&self) -> Option<SaslResult> {
        if self.failed {
            return Some(SaslResult::failed());
        }
        let (user, principals) = self.identity.as_ref()?;
        self.server.is_ended().then(|| SaslResult {
            success: self.server.is_successful(),
            user: Some(user.clone()),
            password: None,
            principals: Some(principals.clone()),
        })
    }
}
