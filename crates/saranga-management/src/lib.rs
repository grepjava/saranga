//! The broker's embedded web server, from `bootstrap.xml`'s `<web>`. On each
//! `<binding>`:
//!
//! - the REST API at `/api` (see [`rest`]), always;
//! - the Prometheus metrics where the metrics plugin's app is
//!   (`<app url="metrics" war="metrics.war"/>`);
//! - where the web console's app is (`<app url="console"
//!   war="console.war"/>`), Artemis's own console (see [`console`]) and the
//!   Jolokia compatibility layer under it, at `/console/jolokia`, for the
//!   console and tools that speak Jolokia. As in Artemis, Jolokia needs a
//!   console session or a user of the broker's JAAS domain holding one of
//!   the console roles (`HAWTIO_ROLES`), and every attribute and operation
//!   is checked against `management.xml`'s `<authorisation>`. Without that
//!   app neither is served.
//! - Saranga's own console at `/saranga` (its pages are embedded at build
//!   time), with its API at `/saranga/api/v1` (see [`api`]), unless
//!   `saranga.console.enabled` is `false`.

mod access;
pub mod api;
mod api_schema;
mod catalog;
pub mod console;
mod exceptions;
mod jolokia;
mod live;
mod mbean;
pub mod metrics;
mod pages;
mod platform;
mod rbac;
pub mod rest;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use saranga_server::Current;
use saranga_server::management::json::Json;
use saranga_transport::TlsAcceptor;
use tokio::net::TcpListener;

pub use access::AccessList;
pub use rbac::{Guard, RBAC_BUILDER, Rbac};

/// One `<binding>`, ready to serve.
pub struct Binding {
    pub name: String,
    /// `http://host:port` or `https://host:port`, as configured.
    pub uri: String,
    pub host: String,
    pub port: u16,
    pub tls: Option<Arc<TlsAcceptor>>,
    /// `(url, war)` of each `<app>`.
    pub apps: Vec<(String, String)>,
}

/// What the web server serves, beyond its bindings.
pub struct Options {
    /// The JMX domain (`<jmx-domain>`).
    pub domain: String,
    /// The broker's name, the `broker` key of its MBeans.
    pub broker_name: String,
    /// `management.xml`'s rules, or the RBAC guard.
    pub access: Guard,
    /// The roles that may use the API (`HAWTIO_ROLES`); `*` is anyone.
    pub console_roles: Vec<String>,
    /// `jolokia-access.xml`'s CORS rules.
    pub cors: Cors,
    pub root_redirect: Option<String>,
    /// The acceptors, by name, with their parameters (`AcceptorControl`).
    pub acceptors: Vec<(String, Vec<(String, String)>)>,
    /// Where the apps' `.war` directories are looked for: the instance's
    /// `<web path>`, then the home's.
    pub web_dirs: Vec<PathBuf>,
    /// `hawtio.sessionTimeout`, in seconds: how long an idle console
    /// session lasts.
    pub session_timeout: i64,
    /// `hawtio.http.strictTransportSecurity`, the console's
    /// `Strict-Transport-Security` header.
    pub strict_transport_security: Option<String>,
    /// The system properties `java.lang:type=Runtime` reports: those
    /// `artemis run` defines, then those given with `-D`.
    pub system_properties: Vec<(String, String)>,
    /// Whether Saranga's own console is served (`saranga.console.enabled`,
    /// true by default).
    pub saranga_console: bool,
}

/// `jolokia-access.xml`'s `<cors>`: allowed origins, and whether a
/// request's `Origin` is checked on the server side.
#[derive(Debug, Clone, Default)]
pub struct Cors {
    pub allow_origins: Vec<String>,
    pub strict: bool,
}

impl Cors {
    /// Reads `<restrict><cors>`; no file allows any origin.
    pub fn parse(xml: &str) -> Self {
        let Ok(doc) = roxmltree::Document::parse(xml) else {
            return Self::default();
        };
        let mut cors = Self::default();
        for n in doc.descendants().filter(|n| n.is_element()) {
            match n.tag_name().name() {
                "allow-origin" => {
                    cors.allow_origins
                        .push(n.text().unwrap_or("").trim().to_owned());
                }
                "strict-checking" => cors.strict = true,
                _ => {}
            }
        }
        cors
    }

    /// `CorsChecker.check` of a request's Origin or Referer: with
    /// origins listed, a request without either is refused.
    fn allows_checked(&self, origin: Option<&str>) -> bool {
        match origin {
            Some(o) => self.allows(o),
            None => self.allow_origins.is_empty(),
        }
    }

    fn allows(&self, origin: &str) -> bool {
        self.allow_origins.is_empty()
            || self
                .allow_origins
                .iter()
                .any(|p| mbean::glob(p, origin) || p == "*")
    }
}

struct AppState {
    agent: jolokia::Agent,
    broker: Arc<Current>,
    console_roles: Vec<String>,
    cors: Cors,
    /// Jolokia's path, `/console/jolokia`.
    base: String,
    console: console::Console,
    strict_transport_security: Option<HeaderValue>,
}

/// `ContentSecurityPolicyFilter`'s policy, without Keycloak or OIDC.
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; \
    style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data:; \
    connect-src 'self'; frame-src 'self'; manifest-src 'self'; media-src 'self'; \
    object-src 'self'; worker-src 'self' blob:; frame-ancestors 'none'; \
    script-src-elem 'self'; style-src-elem 'self' 'unsafe-inline'; form-action 'self'";

impl AppState {
    /// The headers `ResponseHeadersFilter` puts on every answer: with
    /// `no_cache`, `CacheHeadersFilter`'s too, unless the answer has its
    /// own.
    fn secure(&self, mut r: Response, no_cache: bool) -> Response {
        let h = r.headers_mut();
        if no_cache && !h.contains_key(header::CACHE_CONTROL) {
            h.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static(console::NO_CACHE),
            );
        }
        h.insert(
            "Content-Security-Policy",
            HeaderValue::from_static(CONTENT_SECURITY_POLICY),
        );
        if let Some(v) = &self.strict_transport_security {
            h.insert("Strict-Transport-Security", v.clone());
        }
        secure(r)
    }

    fn forbidden(&self) -> Response {
        self.secure(console::forbidden("NONE"), true)
    }
}

#[derive(Clone, Copy)]
struct Peer(SocketAddr);

/// Starts serving every binding; the tasks end with the runtime.
pub async fn start(
    broker: Arc<Current>,
    bindings: Vec<Binding>,
    options: Options,
) -> std::io::Result<()> {
    let mut agent_id = None;
    let process = Arc::new(platform::Process::new(options.system_properties.clone()));
    // The console's history and notifications, one for every binding.
    let live = options
        .saranga_console
        .then(|| live::Live::start(broker.clone()));
    for (index, b) in bindings.into_iter().enumerate() {
        // Java resolves a host name to its IPv4 address first.
        let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host((b.host.as_str(), b.port))
            .await?
            .collect();
        addrs.sort_by_key(|a| !a.is_ipv4());
        let addr = addrs
            .first()
            .copied()
            .ok_or_else(|| std::io::Error::other(format!("cannot resolve {}", b.host)))?;
        let listener = TcpListener::bind(addr).await?;
        let agent_id = agent_id
            .get_or_insert_with(|| {
                format!(
                    "{}-{}-{:08x}-servlet",
                    addr.ip(),
                    std::process::id(),
                    rand_u32()
                )
            })
            .clone();
        tracing::info!("AMQ241001: HTTP Server started at {}", b.uri);
        tracing::info!("Saranga REST API available at {}/api", b.uri);
        let mut router = rest::router(broker.clone());
        // `saranga create` writes Artemis's redirect to its console; without
        // Artemis's console.war, `/` goes to Saranga's own.
        let to_missing_console = options.root_redirect.as_deref().is_some_and(|r| {
            b.apps.iter().any(|(url, war)| {
                url.trim_matches('/') == r.trim_matches('/')
                    && war.starts_with("console")
                    && console::find_war(&options.web_dirs, war).is_none()
            })
        });
        let root = if to_missing_console && options.saranga_console {
            Some(format!("{}/", api::BASE))
        } else {
            options
                .root_redirect
                .as_deref()
                .map(|r| format!("/{}", r.trim_start_matches('/')))
        };
        if let Some(location) = root {
            router =
                router.route(
                    "/",
                    get(move || {
                        let location = location.clone();
                        async move {
                            (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
                        }
                    }),
                );
        }
        for (url, war) in &b.apps {
            let url = url.trim_matches('/');
            if war.starts_with("console") {
                let base = format!("/{url}/jolokia");
                tracing::info!("AMQ241002: Jolokia REST API available at {}{base}", b.uri);
                let dir = console::find_war(&options.web_dirs, war);
                match &dir {
                    Some(_) => {
                        tracing::info!("AMQ241004: Artemis Console available at {}/{url}", b.uri)
                    }
                    None => tracing::warn!(
                        "The console's pages are not served: no {war} in {}",
                        options
                            .web_dirs
                            .iter()
                            .map(|d| d.display().to_string())
                            .collect::<Vec<_>>()
                            .join(" or ")
                    ),
                }
                let state = Arc::new(AppState {
                    agent: jolokia::Agent {
                        broker: broker.clone(),
                        domain: options.domain.clone(),
                        broker_name: options.broker_name.clone(),
                        access: options.access.clone(),
                        agent_id: agent_id.clone(),
                        acceptors: options.acceptors.clone(),
                        process: process.clone(),
                    },
                    broker: broker.clone(),
                    console_roles: options.console_roles.clone(),
                    cors: options.cors.clone(),
                    base,
                    console: console::Console::new(
                        format!("/{url}"),
                        dir,
                        format!("JSESSIONID_{index}_{}", addr.port()),
                        b.tls.is_some(),
                        options.session_timeout,
                    ),
                    strict_transport_security: options
                        .strict_transport_security
                        .as_deref()
                        .and_then(|v| HeaderValue::from_str(v).ok()),
                });
                let app: Router = Router::new()
                    .route(&format!("/{url}"), any(console::handle))
                    .route(&format!("/{url}/"), any(console::handle))
                    .route(&format!("/{url}/{{*rest}}"), any(console::handle))
                    .with_state(state);
                router = router.merge(app);
            } else if war.starts_with("metrics") {
                let broker = broker.clone();
                let handler = move || {
                    let broker = broker.clone();
                    async move {
                        (
                            [(header::CONTENT_TYPE, metrics::CONTENT_TYPE)],
                            metrics::scrape(&broker.get()).await,
                        )
                            .into_response()
                    }
                };
                router = router
                    .route(&format!("/{url}"), get(handler.clone()))
                    .route(&format!("/{url}/"), get(handler));
            } else {
                tracing::warn!(
                    "the web app {war} at /{url} is not served: Saranga runs no JVM and serves only the console and the metrics app"
                );
            }
        }
        if let Some(live) = &live {
            tracing::info!("Saranga console available at {}{}/", b.uri, api::BASE);
            tracing::info!("Saranga console API available at {}{}", b.uri, api::API);
            router = router.merge(pages::router(Arc::new(pages::PagesState::new(
                options
                    .strict_transport_security
                    .as_deref()
                    .and_then(|v| HeaderValue::from_str(v).ok()),
            ))));
            let state = Arc::new(api::ApiState {
                agent: jolokia::Agent {
                    broker: broker.clone(),
                    domain: options.domain.clone(),
                    broker_name: options.broker_name.clone(),
                    access: options.access.clone(),
                    agent_id: agent_id.clone(),
                    acceptors: options.acceptors.clone(),
                    process: process.clone(),
                },
                broker: broker.clone(),
                console_roles: options.console_roles.clone(),
                sessions: console::Console::new(
                    api::BASE.to_owned(),
                    None,
                    format!("SARANGA_SESSION_{index}_{}", addr.port()),
                    b.tls.is_some(),
                    options.session_timeout,
                ),
                live: live.clone(),
            });
            router = router.merge(api::router(state));
        }
        tokio::spawn(serve(listener, b.tls.clone(), router));
    }
    Ok(())
}

async fn serve(listener: TcpListener, tls: Option<Arc<TlsAcceptor>>, router: Router) {
    use hyper_util::rt::TokioIo;
    use hyper_util::service::TowerToHyperService;
    use tower::ServiceExt;
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let router = router.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let service = router.map_request(move |mut r: axum::http::Request<_>| {
                r.extensions_mut().insert(Peer(peer));
                r
            });
            let service = TowerToHyperService::new(service);
            let http = hyper::server::conn::http1::Builder::new();
            let result = match tls {
                Some(tls) => match tls.accept(stream).await {
                    Ok(s) => http.serve_connection(TokioIo::new(s), service).await,
                    Err(_) => return,
                },
                None => http.serve_connection(TokioIo::new(stream), service).await,
            };
            if let Err(e) = result {
                tracing::debug!("web connection from {peer}: {e}");
            }
        });
    }
}

/// The headers hawtio's filters put on every answer, which the REST API
/// gives too.
fn secure(mut r: Response) -> Response {
    let h = r.headers_mut();
    h.insert("X-Frame-Options", HeaderValue::from_static("DENY"));
    h.insert("X-XSS-Protection", HeaderValue::from_static("1"));
    h.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    h.insert("Referrer-Policy", HeaderValue::from_static("strict-origin"));
    r
}

/// The roles and subject of the caller, as hawtio's `AuthenticationFilter`
/// finds them: the console session's, unless an `Authorization: Basic`
/// header names another user (which ends the session); else the header's
/// user, when the broker's security manager validates it and the user
/// holds a console role. Artemis's console does not audit this login (its
/// `AuthenticationFilter` audits only the console's own login page).
fn caller(
    state: &AppState,
    headers: &HeaderMap,
    peer: SocketAddr,
    session: Option<(String, console::Session)>,
) -> Result<(Vec<String>, Arc<saranga_security::jaas::Subject>), Box<Response>> {
    let basic = console::basic(headers);
    if let Some((id, s)) = session {
        match &basic {
            Some((user, _)) if *user != s.user => state.console.invalidate(&id),
            _ => return Ok((s.roles, s.subject)),
        }
    }
    let (user, password) = basic.unzip();
    match console::authenticate(state, user.as_deref(), password.as_deref(), peer) {
        console::Auth::Authorized { roles, subject, .. } => Ok((roles, subject)),
        console::Auth::NotAuthorized | console::Auth::NoCredentials => {
            Err(Box::new(state.forbidden()))
        }
        console::Auth::Throttled(wait) => {
            Err(Box::new(state.secure(console::too_many(wait), true)))
        }
    }
}

/// The Jolokia agent at `/console/jolokia`.
async fn jolokia(
    state: &AppState,
    peer: SocketAddr,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
    session: Option<(String, console::Session)>,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().ok())
        .map(str::to_owned);
    let cors = |mut r: Response| {
        if let Some(o) = &origin
            && state.cors.allows(o)
            && let Ok(v) = HeaderValue::from_str(o)
        {
            r.headers_mut()
                .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
            r.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        state.secure(r, true)
    };
    // `handleCorsPreflightRequest`: only an allowed origin gets headers.
    if method == Method::OPTIONS {
        let mut r = StatusCode::OK.into_response();
        if origin.as_deref().is_some_and(|o| state.cors.allows(o)) {
            if let Some(h) = headers.get(header::ACCESS_CONTROL_REQUEST_HEADERS) {
                r.headers_mut()
                    .insert(header::ACCESS_CONTROL_ALLOW_HEADERS, h.clone());
            }
            r.headers_mut().insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static("31536000"),
            );
        }
        return cors(r);
    }
    let (roles, subject) = match caller(state, headers, peer, session) {
        Ok(c) => c,
        Err(r) => return *r,
    };
    // `<strict-checking/>`: the Origin, or else the Referer, must be
    // allowed (`HttpRequestHandler.checkAccess`); Jolokia answers a
    // refusal as an error.
    let checked = origin.clone().or_else(|| {
        headers
            .get(header::REFERER)
            .and_then(|o| o.to_str().ok())
            .map(|r| r.replace(['\r', '\n'], ""))
    });
    if state.cors.strict && !state.cors.allows_checked(checked.as_deref()) {
        let shown = checked.as_deref().unwrap_or("null");
        let mut error = Json::object();
        error.put("error_type", "java.lang.Exception");
        error.put(
            "error",
            format!("java.lang.Exception : Origin {shown} is not allowed to call this agent"),
        );
        error.put("status", 403i64);
        return cors(json_response(&error));
    }
    let caller = jolokia::Caller {
        roles,
        subject: Some(subject),
        remote: Some(format!("{}:{}", peer.ip(), peer.port())),
    };
    let scheme = if headers.get("x-forwarded-proto").is_some() {
        "https"
    } else {
        "http"
    };
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost");
    let url = format!("{scheme}://{host}{}", state.base);
    let canonical = jolokia::query_canonical(uri.query());
    let answer = if method == Method::POST {
        state
            .agent
            .post(&caller, &String::from_utf8_lossy(&body), &url, canonical)
            .await
    } else {
        let raw = uri.path().strip_prefix(state.base.as_str()).unwrap_or("");
        let path = percent_decode(raw);
        state.agent.get(&caller, &path, &url, canonical).await
    };
    match answer {
        jolokia::Answer::Json(j) => cors(json_response(&j)),
        jolokia::Answer::BadRequest(m) => cors(
            (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("400 (Bad Request)\n\n{m}\n"),
            )
                .into_response(),
        ),
    }
}

/// A Jolokia answer: always 200, the status in the JSON.
fn json_response(j: &Json) -> Response {
    // Jolokia's `Expires` is an hour ago.
    let expires = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json".to_owned()),
            (header::CACHE_CONTROL, "no-cache".to_owned()),
            (header::EXPIRES, httpdate::fmt_http_date(expires)),
        ],
        j.to_string(),
    )
        .into_response()
}

/// `%XX` escapes decoded; invalid ones kept as they are.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(hex) = std::str::from_utf8(&b[i + 1..i + 3])
            && let Ok(v) = u8::from_str_radix(hex, 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let value = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    };
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for &c in s {
        acc = (acc << 6) | value(c)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn rand_u32() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish() as u32
}

/// The guard of an instance's MBeans: `management.xml`'s `<authorisation>`;
/// without one, `rbac` when `builder` (`javax.management.builder.initial`)
/// names `ArtemisRbacMBeanServerBuilder`, else the default list. Artemis
/// sets the property to its own builder for an `<authorisation>`, over the
/// one given. `filter` gives the values of the file's attributes.
pub fn guard(
    etc: &Path,
    builder: Option<&str>,
    rbac: Rbac,
    filter: &dyn Fn(&str) -> String,
) -> Guard {
    let xml = std::fs::read_to_string(etc.join("management.xml")).ok();
    let authorisation = xml.as_deref().is_some_and(access::has_authorisation);
    if !authorisation && builder == Some(RBAC_BUILDER) {
        return Guard::Rbac(rbac);
    }
    Guard::Acl(match xml {
        Some(xml) => AccessList::parse_with(&xml, filter).unwrap_or_else(|e| {
            tracing::warn!("management.xml: {e}; using the default access list");
            AccessList::default_list()
        }),
        None => AccessList::default_list(),
    })
}

/// `jolokia-access.xml` from an instance's etc.
pub fn cors(etc: &Path) -> Cors {
    std::fs::read_to_string(etc.join("jolokia-access.xml"))
        .map(|x| Cors::parse(&x))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{base64_decode, percent_decode};

    #[test]
    fn decoding() {
        assert_eq!(
            base64_decode("YXJ0ZW1pczphcnRlbWlz").unwrap(),
            b"artemis:artemis"
        );
        assert_eq!(base64_decode("YQ==").unwrap(), b"a");
        assert_eq!(percent_decode("broker=%220.0.0.0%22"), "broker=\"0.0.0.0\"");
        assert_eq!(percent_decode("%"), "%");
    }
}
