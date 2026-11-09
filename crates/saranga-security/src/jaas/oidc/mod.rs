//! `OIDCLoginModule`: a JWT, the password of `JwtCallback`, validated
//! against the keys of an OpenID Connect provider (`provider`, whose
//! `/.well-known/openid-configuration` names its issuer and `jwks_uri`),
//! its claims checked, and its identities and roles taken from claim paths
//! (`identityPaths`, `rolesPaths`, `roleMapping`). With `requireOAuth2MTLS`,
//! or a `cnf.x5t#S256` claim (or `cnf.x5t#256`), the client's TLS
//! certificate must be the one the token names (RFC 8705).
//!
//! As in Artemis, each provider's metadata and keys are kept for every
//! module that names it (`SharedOIDCMetadataAccess`): metadata that failed
//! in a way that may pass is fetched again after `metadataRetrySeconds`,
//! keys after `cacheKeysSeconds`.

pub(crate) mod jose;

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use regex::Regex;
use saranga_buffer::json::Json;

use self::jose::{Claims, ClaimsRules, JoseError, Jwk, Jwt};
use super::{ARTEMIS_JAAS, Callbacks, LoginError, LoginModule, ModuleEntry, Principal, Subject};

/// The options and their defaults (`OIDCSupport.ConfigKey`).
mod key {
    pub const DEBUG: (&str, Option<&str>) = ("debug", Some("false"));
    pub const USER_CLASS: (&str, Option<&str>) = ("userPrincipalClass", None);
    pub const ROLE_CLASS: (&str, Option<&str>) = ("rolePrincipalClass", None);
    pub const PROVIDER_URL: (&str, Option<&str>) = ("provider", None);
    pub const CACHE_KEYS_TIME_SECONDS: (&str, Option<&str>) = ("cacheKeysSeconds", Some("3600"));
    pub const METADATA_RETRY_TIME_SECONDS: (&str, Option<&str>) =
        ("metadataRetrySeconds", Some("30"));
    pub const TLS_VERSION: (&str, Option<&str>) = ("tlsVersion", Some("TLSv1.3"));
    pub const CA_CERTIFICATE: (&str, Option<&str>) = ("caCertificate", None);
    pub const HTTP_TIMEOUT_MILLISECONDS: (&str, Option<&str>) = ("httpTimeout", Some("5000"));
    pub const ALLOW_PLAIN_JWT: (&str, Option<&str>) = ("allowPlainJWT", Some("false"));
    pub const MAX_CLOCK_SKEW_SECONDS: (&str, Option<&str>) = ("maxClockSkew", Some("60"));
    pub const AUDIENCE: (&str, Option<&str>) = ("audience", None);
    pub const REQUIRED_CLAIMS: (&str, Option<&str>) =
        ("requiredClaims", Some("aud, iss, sub, azp, exp"));
    pub const IDENTITY_PATHS: (&str, Option<&str>) = ("identityPaths", Some("sub"));
    pub const ROLES_PATHS: (&str, Option<&str>) = ("rolesPaths", None);
    pub const ROLE_MAPPING: (&str, Option<&str>) = ("roleMapping", None);
    pub const REQUIRE_OAUTH_MTLS: (&str, Option<&str>) = ("requireOAuth2MTLS", Some("false"));
}

type Key = (&'static str, Option<&'static str>);

fn string_option(entry: &ModuleEntry, key: Key) -> Option<&str> {
    entry.option(key.0).or(key.1)
}

fn bool_option(entry: &ModuleEntry, key: Key) -> bool {
    string_option(entry, key).is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// `Integer.parseInt`, which fails `initialize`.
fn int_option(entry: &ModuleEntry, key: Key) -> Result<i32, LoginError> {
    let v = string_option(entry, key).unwrap_or_default();
    v.parse()
        .map_err(|_| LoginError::new(format!("For input string: \"{v}\"")))
}

/// `split("\\s*,\\s*")`: whitespace around commas goes, at either end it
/// stays; blank values are dropped.
fn string_array_option(entry: &ModuleEntry, key: Key) -> Option<Vec<String>> {
    static COMMA: OnceLock<Regex> = OnceLock::new();
    let comma = COMMA.get_or_init(|| Regex::new(r"\s*,\s*").expect("pattern"));
    let v = string_option(entry, key)?;
    Some(
        comma
            .split(v)
            .filter(|s| !s.trim().is_empty())
            .map(str::to_owned)
            .collect(),
    )
}

/// `mappingOption`: `jwt-role=local-role` pairs, trimmed.
fn mapping_option(entry: &ModuleEntry, key: Key) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for value in string_array_option(entry, key).unwrap_or_default() {
        if let Some((k, v)) = value.split_once('=') {
            let (k, v) = (k.trim(), v.trim());
            if !k.is_empty() && !v.is_empty() {
                out.insert(k.to_owned(), v.to_owned());
            }
        }
    }
    out
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// How a principal of a configured class is made: Artemis's own classes are
/// the user and role principals; another class gives a principal of that
/// class, which nothing in the broker counts as a user or role.
fn principal(class: Option<&str>, role: bool, name: String) -> Principal {
    let artemis = |simple: &str| format!("{ARTEMIS_JAAS}.{simple}");
    match class {
        None => {
            if role {
                Principal::Role(name)
            } else {
                Principal::User(name)
            }
        }
        Some(c) if c == artemis("UserPrincipal") => Principal::User(name),
        Some(c) if c == artemis("RolePrincipal") => Principal::Role(name),
        Some(c) => Principal::Other {
            class: c.to_owned(),
            name,
        },
    }
}

// ------------------------------------------------------------ metadata

/// `OIDCMetadata`: a provider's metadata as last fetched, and its keys.
struct Metadata {
    timestamp: i64,
    recoverable: bool,
    valid: bool,
    last_keys_check: i64,
    jwks_uri: Option<String>,
    keys: Arc<Vec<Jwk>>,
}

impl Metadata {
    fn failed(recoverable: bool, why: &str) -> Self {
        tracing::debug!("OIDC metadata: {why}");
        Self {
            timestamp: now_millis(),
            recoverable,
            valid: false,
            last_keys_check: 0,
            jwks_uri: None,
            keys: Arc::default(),
        }
    }

    /// `new OIDCMetadata(expectedIssuer, json)`: valid where its issuer is
    /// the provider's URL.
    fn from_json(expected_issuer: &str, json: &Json) -> Self {
        let mut m = Self::failed(true, "");
        m.jwks_uri = json
            .get("jwks_uri")
            .and_then(Json::as_str)
            .map(str::to_owned);
        match json.get("issuer").and_then(Json::as_str) {
            None => {
                tracing::debug!("OIDC Metadata issuer is missing");
                m.recoverable = false;
            }
            Some(i) if i != expected_issuer => {
                tracing::debug!("OIDC Metadata issuer mismatch");
                m.recoverable = false;
            }
            Some(_) => m.valid = true,
        }
        m
    }

    fn should_fetch_again(&self, retry_seconds: i32) -> bool {
        !self.valid
            && self.recoverable
            && now_millis() - self.timestamp >= i64::from(retry_seconds) * 1000
    }

    fn should_refresh_public_keys(&self, cache_seconds: i32) -> bool {
        self.valid && now_millis() - self.last_keys_check > i64::from(cache_seconds) * 1000
    }

    /// `configureKeys`.
    fn configure_keys(&mut self, json: &str) {
        self.last_keys_check = now_millis();
        self.keys = Arc::new(match jose::parse_jwk_set(json) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(
                    "Failed to parse JWK JSON structure. No keys will be available for JWT signature validation: {e}"
                );
                Vec::new()
            }
        });
    }
}

/// `SharedOIDCMetadataAccess.cache` and `SharedHttpClientAccess.cache`.
#[derive(Default)]
struct Shared {
    metadata: HashMap<String, Metadata>,
    clients: HashMap<String, Arc<super::http::HttpClient>>,
}

fn shared() -> &'static Mutex<Shared> {
    static SHARED: OnceLock<Mutex<Shared>> = OnceLock::new();
    SHARED.get_or_init(Mutex::default)
}

/// What a module's options say of its provider.
struct Provider {
    url: String,
    cache_keys_seconds: i32,
    metadata_retry_seconds: i32,
    http_timeout: i32,
    tls_version: Option<String>,
    ca_certificate: Option<String>,
    debug: bool,
}

impl Provider {
    /// `getMetadata`: the provider's keys, fetching the metadata where there
    /// is none or it should be fetched again, and the keys where they are
    /// due.
    fn keys(&self) -> Arc<Vec<Jwk>> {
        let mut shared = shared().lock().unwrap_or_else(|e| e.into_inner());
        let client = shared
            .clients
            .entry(self.url.clone())
            .or_insert_with(|| {
                if self.debug {
                    tracing::debug!(
                        "Created new HTTP Client for accessing OIDC provider at {}",
                        self.url
                    );
                }
                Arc::new(super::http::HttpClient::new(
                    self.http_timeout,
                    self.tls_version.as_deref(),
                    self.ca_certificate.as_deref(),
                ))
            })
            .clone();
        let fetch = shared
            .metadata
            .get(&self.url)
            .is_none_or(|m| m.should_fetch_again(self.metadata_retry_seconds));
        if fetch {
            let m = self.fetch_metadata(&client);
            shared.metadata.insert(self.url.clone(), m);
        }
        let m = shared.metadata.get_mut(&self.url).expect("just put");
        if m.should_refresh_public_keys(self.cache_keys_seconds)
            && let Some(uri) = m.jwks_uri.clone()
        {
            let json = self.fetch_jwk_set(&client, &uri);
            m.configure_keys(&json);
        }
        m.keys.clone()
    }

    /// `fetchOIDCMetadata`.
    fn fetch_metadata(&self, client: &super::http::HttpClient) -> Metadata {
        let uri = format!("{}/.well-known/openid-configuration", self.url);
        if self.debug {
            tracing::debug!("Fetching OIDC Metadata from {uri}");
        }
        match client.get(&uri) {
            Ok(r) if r.status == 200 => {
                let text = String::from_utf8_lossy(&r.body);
                match Json::parse(&text) {
                    Ok(j @ Json::Object(_)) => Metadata::from_json(&self.url, &j),
                    _ => Metadata::failed(false, "OIDC metadata invalid - JSON error"),
                }
            }
            Ok(r) => {
                tracing::warn!(
                    "OIDC Metadata cannot be retrieved: HTTP status code {}",
                    r.status
                );
                let recoverable = r.status < 400 || r.status >= 500;
                Metadata::failed(
                    recoverable,
                    &format!("HTTP {} error when fetching OIDC metadata", r.status),
                )
            }
            Err(e) => Metadata::failed(true, &e),
        }
    }

    /// `fetchJwkSet`: the body, or `{}` (no keys) with a warning.
    fn fetch_jwk_set(&self, client: &super::http::HttpClient, uri: &str) -> String {
        if self.debug {
            tracing::debug!("Fetching JWK set from {uri}");
        }
        let why = match client.get(uri) {
            Ok(r) if r.status == 200 => return String::from_utf8_lossy(&r.body).into_owned(),
            Ok(r) => format!(
                "HTTP {} error when fetching public keys from {uri}",
                r.status
            ),
            Err(e) => e,
        };
        tracing::warn!("Error retrieving keys from {uri}: {why}");
        "{}".into()
    }
}

// ------------------------------------------------------------ the module

pub(crate) struct OidcLoginModule {
    debug: bool,
    user_class: Option<String>,
    role_class: Option<String>,
    require_mtls: bool,
    allow_plain: bool,
    rules: ClaimsRules,
    identity_paths: Option<Vec<String>>,
    roles_paths: Option<Vec<String>>,
    role_mapping: HashMap<String, String>,
    keys: Arc<Vec<Jwk>>,
    /// The claims of the token `login` validated, for `commit`.
    claims: Option<Claims>,
}

impl OidcLoginModule {
    /// `initialize`: the options, and the provider's keys (`OIDCSupport.initialize`).
    pub(crate) fn new(entry: &ModuleEntry) -> Result<Self, LoginError> {
        let debug = bool_option(entry, key::DEBUG);
        if debug {
            tracing::debug!("OIDCLoginModule initialized with debug information");
        }
        let Some(url) = string_option(entry, key::PROVIDER_URL) else {
            return Err(LoginError::new("Missing OpenID Connect provider URL"));
        };
        // Artemis keeps only the last character of a URL ending in a slash
        // and loops for ever: here the slashes go.
        let url = url.trim_end_matches('/').to_owned();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(LoginError::new(format!(
                "OpenID Connect provider URL is invalid: {url}"
            )));
        }
        let provider = Provider {
            url,
            cache_keys_seconds: int_option(entry, key::CACHE_KEYS_TIME_SECONDS)?,
            metadata_retry_seconds: int_option(entry, key::METADATA_RETRY_TIME_SECONDS)?,
            http_timeout: int_option(entry, key::HTTP_TIMEOUT_MILLISECONDS)?,
            tls_version: string_option(entry, key::TLS_VERSION).map(str::to_owned),
            ca_certificate: string_option(entry, key::CA_CERTIFICATE).map(str::to_owned),
            debug,
        };
        let audience = string_array_option(entry, key::AUDIENCE).map(|a| a.into_iter().collect());
        let required: BTreeSet<String> = string_array_option(entry, key::REQUIRED_CLAIMS)
            .unwrap_or_default()
            .into_iter()
            .collect();
        let max_clock_skew_seconds = i64::from(int_option(entry, key::MAX_CLOCK_SKEW_SECONDS)?);
        Ok(Self {
            debug,
            user_class: string_option(entry, key::USER_CLASS).map(str::to_owned),
            role_class: string_option(entry, key::ROLE_CLASS).map(str::to_owned),
            require_mtls: bool_option(entry, key::REQUIRE_OAUTH_MTLS),
            allow_plain: bool_option(entry, key::ALLOW_PLAIN_JWT),
            rules: ClaimsRules {
                issuer: provider.url.clone(),
                audience,
                required,
                max_clock_skew_seconds,
            },
            identity_paths: string_array_option(entry, key::IDENTITY_PATHS),
            roles_paths: string_array_option(entry, key::ROLES_PATHS),
            role_mapping: mapping_option(entry, key::ROLE_MAPPING),
            keys: provider.keys(),
            claims: None,
        })
    }

    fn error(&self, message: String) -> LoginError {
        if self.debug {
            tracing::error!("{message}");
        }
        LoginError::new(message)
    }
}

impl LoginModule for OidcLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        callbacks
            .jaas("CertificateCallback")
            .map_err(|e| LoginError::new(format!("Can't obtain the JWT token: {}", e.message)))?;
        // JwtCallback: the password.
        let Some(token) = callbacks.password else {
            return Ok(false);
        };
        let (jwt, claims) = match Jwt::parse(token).and_then(|j| j.claims().map(|c| (j, c))) {
            Ok(v) => v,
            Err(JoseError::Parse(e) | JoseError::Rejected(e)) => {
                return Err(self.error(format!("JWT parsing error: {e}")));
            }
        };
        if let Err(JoseError::Parse(e) | JoseError::Rejected(e)) = jose::process(
            &jwt,
            claims.as_ref(),
            &self.keys,
            &self.rules,
            self.allow_plain,
            now_millis(),
        ) {
            let summary = jose::summary(claims.as_ref())
                .map(|s| format!(" ({s})"))
                .unwrap_or_default();
            return Err(self.error(format!("JWT processing error: {e}{summary}")));
        }
        let Some(claims) = claims else {
            return Err(self.error("JWT processing error: no claims".into()));
        };
        let thumbprints = jose::thumbprints(&claims);
        if self.require_mtls || !thumbprints.is_empty() {
            let first = callbacks.certificates.and_then(|c| c.first());
            let matching = first.is_some_and(|cert| jose::bound_to(&thumbprints, &cert.der));
            if !matching {
                let message = match (first, thumbprints.is_empty()) {
                    (None, true) => "OAuth2 mTLS failed - no certificates found in transport layer".to_owned(),
                    (None, false) => {
                        "OAuth2 mTLS failed - cnf/x5t#256 present, but no certificates found in transport layer"
                            .to_owned()
                    }
                    (Some(_), _) => {
                        let summary = jose::summary(Some(&claims)).map(|s| format!(" ({s})")).unwrap_or_default();
                        format!(
                            "OAuth2 mTLS failed - certificate from the transport layer doesn't match cnf/x5t#256 thumbprint{summary}"
                        )
                    }
                };
                return Err(self.error(message));
            }
        }
        if self.debug {
            tracing::debug!(
                "JAAS login successful for JWT token with {}",
                jose::summary(Some(&claims)).unwrap_or_default()
            );
        }
        self.claims = Some(claims);
        Ok(true)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let Some(claims) = self.claims.take() else {
            return Ok(false);
        };
        let names = |paths: &Option<Vec<String>>, what: &str| -> Result<Vec<String>, LoginError> {
            let mut out: Vec<String> = Vec::new();
            for path in paths.iter().flatten() {
                let found = jose::strings_at(&claims, path).ok_or_else(|| {
                    LoginError::new(format!(
                        "Can't determine user {what} from JWT using \"{path}\" path"
                    ))
                })?;
                for n in found {
                    if !out.contains(&n) {
                        out.push(n);
                    }
                }
            }
            Ok(out)
        };
        let users = names(&self.identity_paths, "identity")?;
        let roles: Vec<String> = {
            let mut out: Vec<String> = Vec::new();
            for r in names(&self.roles_paths, "role")? {
                let mapped = self.role_mapping.get(&r).cloned().unwrap_or(r);
                if !out.contains(&mapped) {
                    out.push(mapped);
                }
            }
            out
        };
        if self.debug {
            tracing::debug!("Found identities: {}", users.join(", "));
            if self.roles_paths.is_some() {
                tracing::debug!("Found roles: {}", roles.join(", "));
            }
        }
        for u in users {
            subject.add(principal(self.user_class.as_deref(), false, u));
        }
        for r in roles {
            subject.add(principal(self.role_class.as_deref(), true, r));
        }
        Ok(true)
    }
}
