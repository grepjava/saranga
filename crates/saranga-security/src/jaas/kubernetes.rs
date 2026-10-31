//! `KubernetesLoginModule`: the password, a bearer token, reviewed by the
//! Kubernetes API server (`KubernetesClientImpl`): a `TokenReview` posted to
//! `https://{KUBERNETES_SERVICE_HOST}:{KUBERNETES_SERVICE_PORT}/apis/authentication.k8s.io/v1/tokenreviews`
//! with the broker's own service account token (`KUBERNETES_TOKEN_PATH`),
//! over TLS trusting the cluster's CA (`KUBERNETES_CA_PATH`). Each setting
//! is a system property or else an environment variable. A token the
//! server does not authenticate leaves the module ignored; one it does
//! gives the user it names, its groups as roles (unless
//! `ignoreTokenReviewRoles`) and the roles the
//! `org.apache.activemq.jaas.kubernetes.role` file maps the user to.

use std::sync::{Arc, Mutex};

use saranga_buffer::json::Json;

use super::http::{self, HttpClient};
use super::{Callbacks, LoadedProperties, LoginError, LoginModule, ModuleEntry, ModuleFactory};
use super::{Principal, Subject};

pub const K8S_ROLE_FILE_PROP_NAME: &str = "org.apache.activemq.jaas.kubernetes.role";

const KUBERNETES_HOST: &str = "KUBERNETES_SERVICE_HOST";
const KUBERNETES_PORT: &str = "KUBERNETES_SERVICE_PORT";
const KUBERNETES_TOKEN_PATH: &str = "KUBERNETES_TOKEN_PATH";
const KUBERNETES_CA_PATH: &str = "KUBERNETES_CA_PATH";
const DEFAULT_KUBERNETES_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const DEFAULT_KUBERNETES_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// `KubernetesClientImpl.getParam`: the system property, else the
/// environment variable.
fn param(name: &str) -> Option<String> {
    saranga_config::system::property(name).or_else(|| std::env::var(name).ok())
}

/// The status of a token review (`TokenReview`).
#[derive(Debug, Default)]
struct TokenReview {
    authenticated: bool,
    user: Option<User>,
}

#[derive(Debug)]
struct User {
    username: Option<String>,
    groups: Vec<String>,
}

impl TokenReview {
    /// `TokenReview.fromJsonString`: the response's `status`; members of
    /// the wrong type fail as `JsonObject`'s getters fail.
    fn from_json(text: &str) -> Result<Self, LoginError> {
        let json = Json::parse(text).map_err(LoginError::new)?;
        let Json::Object(_) = json else {
            return Err(LoginError::new("TokenReview response is not a JSON object"));
        };
        let Some(status) = object(&json, "status")? else {
            return Ok(Self::default());
        };
        // `getBoolean(name, false)`: false unless a boolean.
        let authenticated = status.get("authenticated").and_then(Json::as_bool) == Some(true);
        let user = match object(status, "user")? {
            None => None,
            Some(user) => {
                let groups = strings(user, "groups")?;
                if let Some(extra) = object(user, "extra")? {
                    strings(extra, "authentication.kubernetes.io/pod-name")?;
                    strings(extra, "authentication.kubernetes.io/pod-uid")?;
                }
                Some(User {
                    // `getString(name, null)`: null unless a string.
                    username: user
                        .get("username")
                        .and_then(Json::as_str)
                        .map(str::to_owned),
                    groups,
                })
            }
        };
        strings(status, "audiences")?;
        Ok(Self {
            authenticated,
            user,
        })
    }

    fn username(&self) -> Option<&str> {
        self.user.as_ref().and_then(|u| u.username.as_deref())
    }
}

/// `getJsonObject`: absent or null is none, another type fails.
fn object<'a>(json: &'a Json, key: &str) -> Result<Option<&'a Json>, LoginError> {
    match json.get(key) {
        None | Some(Json::Null) => Ok(None),
        Some(o @ Json::Object(_)) => Ok(Some(o)),
        Some(_) => Err(LoginError::new(format!("{key} is not a JSON object"))),
    }
}

/// `getJsonArray(key).getValuesAs(JsonString::getString)`: absent is
/// empty, anything but an array of strings fails.
fn strings(json: &Json, key: &str) -> Result<Vec<String>, LoginError> {
    match json.get(key) {
        None | Some(Json::Null) => Ok(Vec::new()),
        Some(Json::Array(items)) => items
            .iter()
            .map(|i| {
                i.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| LoginError::new(format!("{key} holds a value not a string")))
            })
            .collect(),
        Some(_) => Err(LoginError::new(format!("{key} is not a JSON array"))),
    }
}

/// `KubernetesClientImpl.httpClient`: built on first use, for every module,
/// and again on the next login while it cannot be.
static CLIENT: Mutex<Option<Arc<HttpClient>>> = Mutex::new(None);

fn client() -> Option<Arc<HttpClient>> {
    let mut client = CLIENT.lock().unwrap_or_else(|e| e.into_inner());
    if client.is_none() {
        match tls_config() {
            Ok(tls) => *client = Some(Arc::new(HttpClient::untimed(Arc::new(tls)))),
            Err(e) => tracing::error!("Unable to build a valid SSLContext or HttpClient: {e}"),
        }
    }
    client.clone()
}

/// `buildSSLContext`: `TLS`, trusting the CA file. Without the file the
/// context is left uninitialized, which the client cannot be built on.
fn tls_config() -> Result<rustls::ClientConfig, String> {
    let ca = param(KUBERNETES_CA_PATH).unwrap_or_else(|| DEFAULT_KUBERNETES_CA_PATH.to_owned());
    if !std::path::Path::new(&ca).exists() {
        tracing::debug!("Kubernetes CA certificate not found at: {ca}. Truststore not configured");
        return Err("SSLContext is not initialized".into());
    }
    http::tls_config(Some("TLS"), Some(&ca))
}

/// `KubernetesClientImpl`: where the API server is and the token to call
/// it with, read when the module is created.
struct KubernetesClient {
    host: Option<String>,
    port: Option<String>,
    token_path: String,
}

impl KubernetesClient {
    fn new() -> Self {
        let client = Self {
            host: param(KUBERNETES_HOST),
            port: param(KUBERNETES_PORT),
            token_path: param(KUBERNETES_TOKEN_PATH)
                .unwrap_or_else(|| DEFAULT_KUBERNETES_TOKEN_PATH.to_owned()),
        };
        tracing::debug!("using apiUri {}", client.api_uri());
        client
    }

    fn api_uri(&self) -> String {
        format!(
            "https://{}:{}/apis/authentication.k8s.io/v1/tokenreviews",
            self.host.as_deref().unwrap_or("null"),
            self.port.as_deref().unwrap_or("null")
        )
    }

    /// The URI, unless `HttpRequest` refuses it: `java.net.URI` takes an
    /// authority it cannot read as host and port (`null:null`, an IPv6
    /// address without brackets) as one with no host.
    fn checked_uri(&self) -> Result<String, LoginError> {
        let host = self.host.as_deref().unwrap_or("null");
        let port = self.port.as_deref().unwrap_or("null");
        let host_ok = !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_');
        let port_ok = port.chars().all(|c| c.is_ascii_digit());
        if host_ok && port_ok {
            // An empty port is the scheme's.
            Ok(if port.is_empty() {
                format!("https://{host}/apis/authentication.k8s.io/v1/tokenreviews")
            } else {
                self.api_uri()
            })
        } else {
            Err(LoginError::new(format!(
                "unsupported URI {}",
                self.api_uri()
            )))
        }
    }

    /// `getTokenReview`: unauthenticated when the broker's token cannot be
    /// read, the client cannot be built, the request fails or the server
    /// answers other than 201 Created.
    fn token_review(&self, token: &str) -> Result<TokenReview, LoginError> {
        tracing::debug!(
            "Loading client authentication token from {}",
            self.token_path
        );
        let auth_token = match std::fs::read(&self.token_path) {
            Ok(bytes) => read_token(&String::from_utf8_lossy(&bytes)),
            Err(e) => {
                tracing::error!(
                    "Cannot retrieve Service Account Authentication Token from {}: {e}",
                    self.token_path
                );
                return Ok(TokenReview::default());
            }
        };
        tracing::debug!(
            "Loaded client authentication token from {}",
            self.token_path
        );
        let request = Json::object()
            .with("apiVersion", "authentication.k8s.io/v1")
            .with("kind", "TokenReview")
            .with("spec", Json::object().with("token", token))
            .to_string();
        let Some(client) = client() else {
            return Ok(TokenReview::default());
        };
        let uri = self.checked_uri()?;
        tracing::debug!("Submit TokenReview request to Kubernetes API");
        let authorization = format!("Bearer {auth_token}");
        let headers = [
            ("Authorization", authorization.as_str()),
            ("Accept", "application/json; charset=utf-8"),
        ];
        match client.post(&uri, &headers, request.as_bytes()) {
            Ok(response) if response.status == 201 => {
                tracing::debug!("Received valid TokenReview response");
                TokenReview::from_json(&String::from_utf8_lossy(&response.body))
            }
            Ok(response) => {
                tracing::error!(
                    "Unable to retrieve a valid TokenReview. Received StatusCode: {}. Body: {}",
                    response.status,
                    String::from_utf8_lossy(&response.body)
                );
                Ok(TokenReview::default())
            }
            Err(e) => {
                tracing::error!("Unable to request ReviewToken: {e}");
                Ok(TokenReview::default())
            }
        }
    }
}

/// `readFile`: the lines that are neither blank nor comments, joined.
fn read_token(text: &str) -> String {
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .collect()
}

pub(crate) struct KubernetesLoginModule {
    client: KubernetesClient,
    roles: Option<Arc<LoadedProperties>>,
    ignore_token_review_roles: bool,
    debug: bool,
    review: TokenReview,
}

impl KubernetesLoginModule {
    pub(crate) fn new(factory: &ModuleFactory, entry: &ModuleEntry) -> Self {
        let debug = entry.bool_option("debug");
        // The role mapping file is optional.
        let roles = entry.option(K8S_ROLE_FILE_PROP_NAME).map(|name| {
            factory.properties(&factory.file(entry, name), entry.bool_option("reload"))
        });
        Self {
            client: KubernetesClient::new(),
            roles,
            ignore_token_review_roles: entry.bool_option("ignoreTokenReviewRoles"),
            debug,
            review: TokenReview::default(),
        }
    }
}

impl LoginModule for KubernetesLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        callbacks.jaas("PasswordCallback")?;
        let Some(token) = callbacks.password else {
            return Err(LoginError::new("Bearer token is null"));
        };
        if token.is_empty() {
            return Err(LoginError::new("Bearer token is empty"));
        }
        self.review = self.client.token_review(token)?;
        if self.debug {
            tracing::debug!(
                "login authenticated={} user={:?}",
                self.review.authenticated,
                self.review.username()
            );
        }
        Ok(self.review.authenticated)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let review = std::mem::take(&mut self.review);
        if !review.authenticated {
            return Ok(false);
        }
        // `UserPrincipal` refuses a null name.
        let Some(user) = review.user else {
            return Err(LoginError::new("name cannot be null"));
        };
        let Some(username) = user.username else {
            return Err(LoginError::new("name cannot be null"));
        };
        let mut principals = vec![Principal::User(username.clone())];
        if !self.ignore_token_review_roles {
            principals.extend(user.groups.into_iter().map(Principal::Role));
        }
        if let Some(roles) = &self.roles
            && let Some(mapped) = roles.inverted_values().get(username.as_str())
        {
            principals.extend(mapped.iter().map(|r| Principal::Role((*r).to_owned())));
        }
        if self.debug {
            tracing::debug!("commit, result: true, principals: {principals:?}");
        }
        for p in principals {
            subject.add(p);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_reviews_are_read_as_artemis_reads_them() {
        let r = TokenReview::from_json(
            r#"{"kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:ns:app","uid":"1","groups":["g1","g2"],"extra":{"authentication.kubernetes.io/pod-name":["p"]}},"audiences":["a"]}}"#,
        )
        .unwrap();
        assert!(r.authenticated);
        assert_eq!(r.username(), Some("system:serviceaccount:ns:app"));
        assert_eq!(r.user.as_ref().unwrap().groups, ["g1", "g2"]);
        let r = TokenReview::from_json(r#"{"status":{"authenticated":"true"}}"#).unwrap();
        assert!(!r.authenticated && r.user.is_none());
        assert!(!TokenReview::from_json("{}").unwrap().authenticated);
        assert!(TokenReview::from_json(r#"{"status":{"user":{"groups":[1]}}}"#).is_err());
        assert!(TokenReview::from_json(r#"{"status":[]}"#).is_err());
        assert!(TokenReview::from_json("not json").is_err());
    }

    #[test]
    fn the_token_file_drops_blank_and_comment_lines() {
        assert_eq!(read_token("# c\n\nabc\n  \ndef\r\n"), "abcdef");
    }

    #[test]
    fn uris_java_cannot_take_are_refused() {
        let client = |host: Option<&str>, port: Option<&str>| KubernetesClient {
            host: host.map(str::to_owned),
            port: port.map(str::to_owned),
            token_path: String::new(),
        };
        assert_eq!(
            client(Some("k8s.local"), Some("6443"))
                .checked_uri()
                .unwrap(),
            "https://k8s.local:6443/apis/authentication.k8s.io/v1/tokenreviews"
        );
        assert!(client(None, None).checked_uri().is_err());
        assert!(client(Some("fd00::1"), Some("443")).checked_uri().is_err());
        assert!(client(Some("h"), Some("x")).checked_uri().is_err());
    }
}
