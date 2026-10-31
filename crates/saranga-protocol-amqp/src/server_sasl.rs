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
    /// OAUTHBEARER (RFC 7628) or XOAUTH2: a bearer token, validated as the
    /// password of the user it names (`ServerSASLToken`).
    Token(TokenKind, Option<SaslResult>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenKind {
    OAuthBearer,
    XOAuth2,
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
            "OAUTHBEARER" => Some(Self::Token(TokenKind::OAuthBearer, None)),
            "XOAUTH2" => Some(Self::Token(TokenKind::XOAuth2, None)),
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
            Self::Token(kind, result) => {
                let parsed = match kind {
                    TokenKind::OAuthBearer => oauth_bearer(bytes.as_deref()),
                    TokenKind::XOAuth2 => xoauth2(bytes.as_deref()),
                };
                let r = match parsed {
                    Some((user, token)) => {
                        let success = authenticate(env, user.clone(), Some(token.clone())).await;
                        SaslResult {
                            success,
                            user,
                            password: Some(token),
                            principals: None,
                        }
                    }
                    None => SaslResult::failed(),
                };
                *result = Some(r);
                // Artemis answers a failure with an error (RFC 7628's JSON
                // status), but a failed outcome carries no additional data,
                // so it never reaches the client.
                None
            }
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
            Self::Token(_, r) => r.clone(),
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
    let success = authenticate(env, user.clone(), password.clone()).await;
    SaslResult {
        success,
        user,
        password,
        principals: None,
    }
}

/// `securityStore.authenticate`, when security is enabled: a mechanism's
/// user and password (or bearer token, which `JwtCallback` gets as the
/// password).
async fn authenticate(env: &Env, user: Option<String>, password: Option<String>) -> bool {
    match env.broker.security().cloned() {
        None => true,
        Some(store) => {
            let info = env.info.clone();
            tokio::task::spawn_blocking(move || {
                store
                    .authenticate(user.as_deref(), password.as_deref(), &info)
                    .is_ok()
            })
            .await
            .unwrap_or(false)
        }
    }
}

/// `OAuthBearerSASL`: `n,a=<user>,\x01host=<host>\x01port=<port>\x01auth=Bearer
/// <token>\x01\x01` (RFC 7628 3.1), the user from the GS2 header's `a=`,
/// which may be missing. `None` where it is not one.
fn oauth_bearer(bytes: Option<&[u8]>) -> Option<(Option<String>, String)> {
    let bytes = bytes.filter(|b| b.len() >= 2 && b.ends_with(&[1, 1]))?;
    let data = String::from_utf8_lossy(bytes);
    let segments = java_split(&data, '\u{1}');
    if segments.len() < 2 {
        return None;
    }
    let user = segments[0]
        .split(',')
        .filter_map(|s| s.strip_prefix("a="))
        .next_back()
        .map(str::to_owned);
    let token = segments[1..]
        .iter()
        .filter_map(|s| s.strip_prefix("auth=Bearer "))
        .next_back()?;
    Some((user, token.to_owned()))
}

/// `XOAuth2SASL`: `user=<user>\x01auth=Bearer <token>\x01\x01`, scanned as
/// Artemis scans it.
fn xoauth2(bytes: Option<&[u8]>) -> Option<(Option<String>, String)> {
    let bytes = bytes.filter(|b| !b.is_empty())?;
    let text = |from: usize, to: usize| String::from_utf8_lossy(&bytes[from..to]).into_owned();
    let (mut from, mut to) = (0, 0);
    while to < bytes.len() && bytes[to] != 1 {
        to += 1;
    }
    let mut user_part = None;
    if to - from >= 5 {
        user_part = Some(text(from, to));
        to += 1;
        from = to;
    }
    while to < bytes.len() && bytes[to] != 1 {
        to += 1;
    }
    let token_part = (to - from >= 12).then(|| text(from, to));
    let (user, token) = (user_part?, token_part?);
    // Two 0x01 after the token, and nothing else.
    if to != bytes.len() - 2 {
        return None;
    }
    let user = user.strip_prefix("user=")?;
    let token = token.strip_prefix("auth=Bearer ")?;
    Some((Some(user.to_owned()), token.to_owned()))
}

/// `String.split` on one character: trailing empty parts dropped.
fn java_split(s: &str, c: char) -> Vec<&str> {
    let mut parts: Vec<&str> = s.split(c).collect();
    while parts.last() == Some(&"") {
        parts.pop();
    }
    parts
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_tokens_are_read_as_artemis_reads_them() {
        let ob = |s: &str| oauth_bearer(Some(s.as_bytes()));
        assert_eq!(
            ob("n,a=tok,\u{1}host=h\u{1}port=5672\u{1}auth=Bearer abc\u{1}\u{1}"),
            Some((Some("tok".into()), "abc".into()))
        );
        assert_eq!(
            ob("n,,\u{1}auth=Bearer abc\u{1}\u{1}"),
            Some((None, "abc".into()))
        );
        assert_eq!(ob("n,a=tok,\u{1}auth=Bearer abc\u{1}"), None);
        assert_eq!(ob("n,a=tok,\u{1}host=h\u{1}\u{1}"), None);
        assert_eq!(oauth_bearer(None), None);

        let x = |s: &str| xoauth2(Some(s.as_bytes()));
        assert_eq!(
            x("user=tok\u{1}auth=Bearer abc\u{1}\u{1}"),
            Some((Some("tok".into()), "abc".into()))
        );
        assert_eq!(x("user=tok\u{1}auth=Bearer abc\u{1}"), None);
        assert_eq!(x("user=tok\u{1}auth=Bearer abc\u{1}\u{1}x"), None);
        assert_eq!(x("usr=tok\u{1}auth=Bearer abc\u{1}\u{1}"), None);
        assert_eq!(x("u\u{1}auth=Bearer abc\u{1}\u{1}"), None);
    }
}
