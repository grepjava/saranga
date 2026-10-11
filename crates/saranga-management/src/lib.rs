//! The broker's embedded web server, from `bootstrap.xml`'s `<web>`. On each
//! `<binding>`:
//!
//! - the REST API at `/api` (see [`rest`]), always;
//! - the Prometheus metrics where the metrics plugin's app is
//!   (`<app url="metrics" war="metrics.war"/>`);
//! - the Jolokia compatibility layer where the web console's app is
//!   (`<app url="console" war="console.war"/>`, at `/console/jolokia`), for
//!   the Artemis console and tools that speak Jolokia. As in Artemis, it
//!   needs a user of the broker's JAAS domain holding one of the console
//!   roles (`HAWTIO_ROLES`), and every attribute and operation is checked
//!   against `management.xml`'s `<authorisation>`. Without that app it is
//!   not served.

mod access;
mod catalog;
mod jolokia;
mod mbean;
pub mod metrics;
pub mod rest;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use saranga_server::Broker;
use saranga_transport::TlsAcceptor;
use tokio::net::TcpListener;

pub use access::AccessList;

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
    /// `management.xml`'s rules.
    pub access: AccessList,
    /// The roles that may use the API (`HAWTIO_ROLES`); `*` is anyone.
    pub console_roles: Vec<String>,
    /// `jolokia-access.xml`'s CORS rules.
    pub cors: Cors,
    pub root_redirect: Option<String>,
    /// The acceptors, by name, with their parameters (`AcceptorControl`).
    pub acceptors: Vec<(String, Vec<(String, String)>)>,
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
    broker: Arc<Broker>,
    console_roles: Vec<String>,
    cors: Cors,
    base: String,
}

#[derive(Clone, Copy)]
struct Peer(SocketAddr);

/// Starts serving every binding; the tasks end with the runtime.
pub async fn start(
    broker: Arc<Broker>,
    bindings: Vec<Binding>,
    options: Options,
) -> std::io::Result<()> {
    let mut agent_id = None;
    for b in bindings {
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
        if let Some(redirect) = &options.root_redirect {
            let location = format!("/{}", redirect.trim_start_matches('/'));
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
                let state = Arc::new(AppState {
                    agent: jolokia::Agent {
                        broker: broker.clone(),
                        domain: options.domain.clone(),
                        broker_name: options.broker_name.clone(),
                        access: options.access.clone(),
                        agent_id: agent_id.clone(),
                        acceptors: options.acceptors.clone(),
                    },
                    broker: broker.clone(),
                    console_roles: options.console_roles.clone(),
                    cors: options.cors.clone(),
                    base: base.clone(),
                });
                let api: Router = Router::new()
                    .route(&base, any(jolokia_handler))
                    .route(&format!("{base}/"), any(jolokia_handler))
                    .route(&format!("{base}/{{*rest}}"), any(jolokia_handler))
                    .with_state(state);
                router = router.merge(api);
            } else if war.starts_with("metrics") {
                let broker = broker.clone();
                let handler = move || {
                    let broker = broker.clone();
                    async move {
                        (
                            [(header::CONTENT_TYPE, metrics::CONTENT_TYPE)],
                            metrics::scrape(&broker).await,
                        )
                            .into_response()
                    }
                };
                router = router
                    .route(&format!("/{url}"), get(handler.clone()))
                    .route(&format!("/{url}/"), get(handler));
            }
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

/// The headers the console's filters put on every answer.
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

fn forbidden() -> Response {
    secure(
        (
            StatusCode::FORBIDDEN,
            [
                (
                    header::CACHE_CONTROL,
                    "max-age=0, no-cache, must-revalidate, proxy-revalidate, private",
                ),
                (
                    header::HeaderName::from_static("hawtio-forbidden-reason"),
                    "NONE",
                ),
            ],
        )
            .into_response(),
    )
}

/// The roles of the user the `Authorization: Basic` header names, when the
/// broker's security manager validates it.
fn login(state: &AppState, headers: &HeaderMap, peer: SocketAddr) -> Option<Vec<String>> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = value
        .strip_prefix("Basic ")
        .or_else(|| value.strip_prefix("basic "))?;
    let decoded = String::from_utf8(base64_decode(encoded.trim())?).ok()?;
    let (user, password) = decoded.split_once(':')?;
    let store = state.broker.security()?;
    let connection = saranga_security::ConnectionInfo {
        remote_address: format!("/{peer}"),
        certificates: None,
        security_domain: None,
    };
    let subject = store
        .manager()
        .authenticate(Some(user), Some(password), &connection)
        .ok()??;
    let roles: Vec<String> = subject.roles().map(str::to_owned).collect();
    let allowed = state
        .console_roles
        .iter()
        .any(|r| r == "*" || roles.iter().any(|u| u == r));
    allowed.then_some(roles)
}

async fn jolokia_handler(
    State(state): State<Arc<AppState>>,
    Extension(Peer(peer)): Extension<Peer>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().ok())
        .map(str::to_owned);
    if let Some(o) = &origin
        && state.cors.strict
        && !state.cors.allows(o)
    {
        return secure(
            (
                StatusCode::FORBIDDEN,
                format!("Origin {o} is not allowed to call this agent"),
            )
                .into_response(),
        );
    }
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
        secure(r)
    };
    if method == Method::OPTIONS {
        let mut r = StatusCode::OK.into_response();
        r.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST"),
        );
        if let Some(h) = headers.get(header::ACCESS_CONTROL_REQUEST_HEADERS) {
            r.headers_mut()
                .insert(header::ACCESS_CONTROL_ALLOW_HEADERS, h.clone());
        }
        return cors(r);
    }
    let Some(roles) = login(&state, &headers, peer) else {
        return forbidden();
    };
    let caller = jolokia::Caller { roles };
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
    let answer = if method == Method::POST {
        state
            .agent
            .post(&caller, &String::from_utf8_lossy(&body), &url)
            .await
    } else {
        let raw = uri.path().strip_prefix(state.base.as_str()).unwrap_or("");
        let path = percent_decode(raw);
        state.agent.get(&caller, &path, &url).await
    };
    match answer {
        jolokia::Answer::Json(j) => cors(
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/json"),
                    (header::CACHE_CONTROL, "no-cache"),
                ],
                j.to_string(),
            )
                .into_response(),
        ),
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

/// `management.xml`'s rules from an instance's etc, or the default list.
pub fn access_list(etc: &Path) -> AccessList {
    match std::fs::read_to_string(etc.join("management.xml")) {
        Ok(xml) => AccessList::parse(&xml).unwrap_or_else(|e| {
            tracing::warn!("management.xml: {e}; using the default access list");
            AccessList::default_list()
        }),
        Err(_) => AccessList::default_list(),
    }
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
