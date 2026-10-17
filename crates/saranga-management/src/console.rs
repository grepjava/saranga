//! Artemis's web console (`web/console.war`: hawtio 4.7.0 with the Artemis
//! plugin), served as the `.war`'s filters and servlets serve it:
//!
//! - `SessionExpiryFilter`: `/refresh`, and a session idle for longer than
//!   its timeout (`hawtio.sessionTimeout`, 1800 s) ends with a 403. Jolokia
//!   requests do not keep a session alive.
//! - `ClientRouteRedirectFilter`: the console's own routes (`/artemis/...`,
//!   `/login`) answer `index.html`, and send a user without a session to
//!   `/login`, unless the request carries a valid `Authorization: Basic`.
//! - `BaseTagHrefFilter`: `index.html`'s `<base href>` follows the app's
//!   URL.
//! - The servlets: `/auth/login` (a JSON form login that starts a session,
//!   throttled as `AuthenticationThrottler` throttles), `/auth/logout`,
//!   `/auth/config/*`, `/user`, `/keycloak/*`, `/plugin` and
//!   `/proxy/enabled`, with Artemis's settings (no Keycloak, no OIDC, the
//!   proxy disabled).
//! - The `.war`'s files, from the instance's `web` directory, else the
//!   home's.
//!
//! Directories are not listed, as Jetty lists them.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use saranga_security::audit;
use saranga_security::jaas::{ARTEMIS_JAAS, Principal, Subject};
use saranga_server::management::json::Json;

use crate::{AppState, Peer};

/// `AuthenticationConfiguration.UNSECURED_PATHS`: the paths that are not
/// client routes.
const UNSECURED: &[&str] = &[
    "/index.html",
    "/favicon.ico",
    "/hawtconfig.json",
    "/robots.txt",
    "/json.worker.js",
    "/editor.worker.js",
    "/css",
    "/fonts",
    "/img",
    "/js",
    "/static",
    "/login",
    "/auth/login",
    "/auth/logout",
    "/auth/config",
    "/user",
    "/keycloak",
    "/plugin",
    "/preset-connections",
    "/jolokia",
    "/proxy",
];

/// `CacheHeadersFilter`'s answer for what the browser must not cache.
pub(crate) const NO_CACHE: &str = "max-age=0, no-cache, must-revalidate, proxy-revalidate, private";

/// `AuthSessionHelpers.DEFAULT_SESSION_TIMEOUT`, in seconds.
pub const DEFAULT_SESSION_TIMEOUT: i64 = 1800;

/// A logged-in console session.
#[derive(Clone)]
pub(crate) struct Session {
    pub user: String,
    pub roles: Vec<String>,
    pub subject: Arc<Subject>,
    /// `LastAccess`: when the session was last used other than by Jolokia.
    last_access: Option<u64>,
    /// When Jetty last saw the session, by any request.
    accessed: Instant,
}

/// `AuthenticationThrottler.Attempt`: failed logins of one user.
struct Attempt {
    timestamp: u64,
    count: u32,
}

impl Attempt {
    /// Seconds until the next login may be tried; not blocked at 0 or less.
    fn retry_after(&self, now: u64) -> i64 {
        // The first three failures are free; then 1, 2, 4... seconds, at
        // most a day.
        let backoff = if self.count < 3 {
            self.timestamp
        } else {
            let secs = 2u64.saturating_pow(self.count - 3).min(24 * 60 * 60);
            self.timestamp + secs * 1000
        };
        (backoff as i64 - now as i64) / 1000
    }
}

/// The console of one `<app>`.
pub(crate) struct Console {
    /// The context path, `/console`.
    pub base: String,
    /// The `.war` directory, when one was found.
    dir: Option<PathBuf>,
    /// `JSESSIONID_<binding index>_<port>`, as `WebServerComponent` names
    /// it.
    cookie: String,
    /// Whether the binding is https (the cookie is then `Secure`).
    https: bool,
    /// `hawtio.sessionTimeout`, in seconds; negative never expires.
    timeout: i64,
    sessions: Mutex<HashMap<String, Session>>,
    attempts: Mutex<HashMap<String, Attempt>>,
}

/// What `Authenticator.authenticate` concluded.
pub(crate) enum Auth {
    Authorized {
        user: String,
        roles: Vec<String>,
        subject: Arc<Subject>,
    },
    NotAuthorized,
    NoCredentials,
    Throttled(i64),
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Console {
    pub fn new(
        base: String,
        dir: Option<PathBuf>,
        cookie: String,
        https: bool,
        timeout: i64,
    ) -> Self {
        Self {
            base,
            dir,
            cookie,
            https,
            timeout: if timeout == 0 {
                DEFAULT_SESSION_TIMEOUT
            } else {
                timeout
            },
            sessions: Mutex::new(HashMap::new()),
            attempts: Mutex::new(HashMap::new()),
        }
    }

    fn expired(&self, s: &Session) -> bool {
        self.timeout >= 0 && s.accessed.elapsed().as_secs() > self.timeout as u64
    }

    /// The request's session, as Jetty finds it from the cookie: one idle
    /// past its timeout is gone.
    pub fn session(&self, headers: &HeaderMap) -> Option<(String, Session)> {
        let id = cookie(headers, &self.cookie)?;
        let mut sessions = self.sessions.lock().unwrap();
        let s = sessions.get_mut(&id)?;
        if self.expired(s) {
            sessions.remove(&id);
            return None;
        }
        s.accessed = Instant::now();
        Some((id, s.clone()))
    }

    pub fn invalidate(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
    }

    fn touch(&self, id: &str, now: u64) {
        if let Some(s) = self.sessions.lock().unwrap().get_mut(id) {
            s.last_access = Some(now);
        }
    }

    /// `AuthSessionHelpers.setup` on a new session: its id.
    fn start(&self, user: String, roles: Vec<String>, subject: Arc<Subject>) -> String {
        let id = format!("node0{:032x}.node0", rand::random::<u128>());
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, s| !self.expired(s));
        sessions.insert(
            id.clone(),
            Session {
                user,
                roles,
                subject,
                last_access: None,
                accessed: Instant::now(),
            },
        );
        id
    }

    /// The `Set-Cookie` of a new session, with Jetty's `Expires` beside it.
    fn set_cookie(&self, r: &mut Response, id: &str) {
        let secure = if self.https { "; Secure" } else { "" };
        let value = format!(
            "{}={id}; Path={}; HttpOnly{secure}; SameSite=Strict",
            self.cookie, self.base
        );
        if let Ok(v) = HeaderValue::from_str(&value) {
            r.headers_mut().append(header::SET_COOKIE, v);
        }
        r.headers_mut().insert(
            header::EXPIRES,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"),
        );
    }

    /// `AuthenticationThrottler.attempt`: seconds to wait, when blocked.
    fn blocked(&self, user: &str) -> Option<i64> {
        let mut attempts = self.attempts.lock().unwrap();
        let now = now_ms();
        attempts.retain(|_, a| now.saturating_sub(a.timestamp) < 24 * 60 * 60 * 1000);
        let wait = attempts.get(user)?.retry_after(now);
        (wait > 0).then_some(wait)
    }

    fn failed(&self, user: &str) {
        if user == "public" {
            return;
        }
        let mut attempts = self.attempts.lock().unwrap();
        let a = attempts.entry(user.to_owned()).or_insert(Attempt {
            timestamp: 0,
            count: 0,
        });
        a.timestamp = now_ms();
        a.count += 1;
    }

    fn succeeded(&self, user: &str) {
        self.attempts.lock().unwrap().remove(user);
    }
}

/// The named cookie's value.
fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|c| c.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

/// The user and password of an `Authorization: Basic` header
/// (`Authenticator.extractAuthHeader`).
pub(crate) fn basic(headers: &HeaderMap) -> Option<(String, String)> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let mut parts = value.trim().split(' ');
    let (kind, info) = (parts.next()?, parts.next()?);
    if parts.next().is_some() || !kind.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = String::from_utf8(crate::base64_decode(info)?).ok()?;
    let (user, password) = decoded.split_once(':')?;
    Some((user.to_owned(), password.to_owned()))
}

/// `Authenticator.authenticate`: the broker's JAAS domain checks the user,
/// who must hold one of the console roles (`hawtio.roles`); repeated
/// failures are throttled.
pub(crate) fn authenticate(
    state: &AppState,
    user: Option<&str>,
    password: Option<&str>,
    peer: SocketAddr,
) -> Auth {
    let (Some(user), Some(password)) = (user, password) else {
        return Auth::NoCredentials;
    };
    if user.trim().is_empty() || password.trim().is_empty() || user == "public" {
        return Auth::NoCredentials;
    }
    let console = &state.console;
    if let Some(wait) = console.blocked(user) {
        return Auth::Throttled(wait);
    }
    let broker = state.broker.get();
    let subject = broker.security().and_then(|store| {
        let connection = saranga_security::ConnectionInfo {
            remote_address: format!("/{peer}"),
            certificates: None,
            security_domain: None,
            connection_id: None,
        };
        match store
            .manager()
            .authenticate(Some(user), Some(password), &connection)
        {
            Ok(Some(subject)) => Some(Arc::new(subject)),
            _ => None,
        }
    });
    let authorized = subject.and_then(|subject| {
        let roles: Vec<String> = subject.roles().map(str::to_owned).collect();
        state
            .console_roles
            .iter()
            .any(|r| r == "*" || roles.iter().any(|u| u == r))
            .then_some((roles, subject))
    });
    match authorized {
        Some((roles, subject)) => {
            console.succeeded(user);
            Auth::Authorized {
                user: user.to_owned(),
                roles,
                subject,
            }
        }
        None => {
            console.failed(user);
            Auth::NotAuthorized
        }
    }
}

/// `ServletHelpers.cleanPath`: no repeated or trailing slashes.
fn clean(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        if c == '/' && out.ends_with('/') {
            continue;
        }
        out.push(c);
    }
    if out.len() > 1 {
        while out.ends_with('/') {
            out.pop();
        }
    }
    out
}

fn redirect(location: &str) -> Response {
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, location.to_owned()),
            (header::CONTENT_LENGTH, "0".to_owned()),
        ],
    )
        .into_response()
}

fn json(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn status(code: StatusCode) -> Response {
    (code, [(header::CONTENT_LENGTH, "0")]).into_response()
}

/// `ServletHelpers.doForbidden`.
pub(crate) fn forbidden(reason: &'static str) -> Response {
    (
        StatusCode::FORBIDDEN,
        [
            (
                header::HeaderName::from_static("hawtio-forbidden-reason"),
                reason,
            ),
            (header::CONTENT_LENGTH, "0"),
        ],
    )
        .into_response()
}

/// `ServletHelpers.doTooManyRequests`.
pub(crate) fn too_many(wait: i64) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [
            (header::RETRY_AFTER, wait.to_string()),
            (header::CONTENT_LENGTH, "0".to_owned()),
        ],
    )
        .into_response()
}

/// Jetty's error page.
fn error_page(code: StatusCode, uri: &str, message: &str, servlet: &str) -> Response {
    let escape = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    };
    let n = code.as_u16();
    let (uri, message) = (escape(uri), escape(message));
    let body = format!(
        "<html>\n<head>\n<meta http-equiv=\"Content-Type\" content=\"text/html;charset=ISO-8859-1\"/>\n\
         <title>Error {n} {message}</title>\n</head>\n<body><h2>HTTP ERROR {n} {message}</h2>\n<table>\n\
         <tr><th>URI:</th><td>{uri}</td></tr>\n<tr><th>STATUS:</th><td>{n}</td></tr>\n\
         <tr><th>MESSAGE:</th><td>{message}</td></tr>\n<tr><th>SERVLET:</th><td>{servlet}</td></tr>\n\
         </table>\n\n</body>\n</html>\n"
    );
    (
        code,
        [
            (header::CACHE_CONTROL, "must-revalidate,no-cache,no-store"),
            (header::CONTENT_TYPE, "text/html;charset=iso-8859-1"),
        ],
        body,
    )
        .into_response()
}

/// Jetty's (and `web.xml`'s) media type of a file.
fn content_type(path: &Path) -> &'static str {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "html" | "htm" => "text/html;charset=utf-8",
        "js" | "mjs" => "text/javascript",
        "css" => "text/css",
        "json" | "map" => "application/json",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "application/font-woff",
        "ttf" => "font/ttf",
        "txt" => "text/plain",
        "xml" => "application/xml",
        _ => "application/octet-stream",
    }
}

/// `BaseTagHrefFilter.replaceHrefs`: the `<base href>` of `index.html`, and
/// every `src` and `href` starting with it, moved to `href`.
fn base_href(html: &str, href: &str) -> String {
    let Some(start) = html.find("<base ") else {
        return html.to_owned();
    };
    let tag_end = html[start..].find('>').map_or(html.len(), |e| start + e);
    let tag = &html[start + 6..tag_end];
    let old = tag.find("href").and_then(|i| {
        let rest = tag[i + 4..].trim_start_matches(|c: char| c.is_whitespace() || c == '=');
        match rest.chars().next()? {
            q @ ('"' | '\'') => rest[1..].split(q).next(),
            _ => rest.split(|c: char| c.is_whitespace()).next(),
        }
    });
    let old = old.unwrap_or("/hawtio");
    let mut out = html.to_owned();
    for attr in ["src=", "href="] {
        for q in ["\"", "'", ""] {
            out = out.replace(&format!("{attr}{q}{old}"), &format!("{attr}{q}{href}"));
        }
    }
    out
}

/// A file of the `.war` under `rel` (`/static/js/main.js`), percent-decoded;
/// none for a path that leaves the directory.
fn resolve(dir: &Path, rel: &str) -> Option<PathBuf> {
    let decoded = crate::percent_decode(rel);
    let mut path = dir.to_path_buf();
    for segment in decoded.split('/').filter(|s| !s.is_empty()) {
        if segment == "." || segment == ".." || segment.contains(['\\', ':', '\0']) {
            return None;
        }
        path.push(segment);
    }
    Some(path)
}

fn http_date(t: SystemTime) -> String {
    httpdate::fmt_http_date(t)
}

/// Serves a file of the `.war`; `index.html` with its `<base href>` set.
fn serve_file(console: &Console, rel: &str, uri: &str, headers: &HeaderMap) -> Response {
    let not_found = || error_page(StatusCode::NOT_FOUND, uri, "Not Found", "default");
    let Some(dir) = &console.dir else {
        return not_found();
    };
    let Some(path) = resolve(dir, rel) else {
        return not_found();
    };
    let Ok(meta) = std::fs::metadata(&path) else {
        return not_found();
    };
    if !meta.is_file() {
        return not_found();
    }
    let modified = meta.modified().ok().map(http_date);
    let index = rel == "/" || rel.ends_with("/index.html");
    if !index
        && let Some(m) = &modified
        && headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|v| v.to_str().ok())
            == Some(m.as_str())
    {
        return StatusCode::NOT_MODIFIED.into_response();
    }
    let Ok(bytes) = std::fs::read(&path) else {
        return not_found();
    };
    let body = if index {
        let href = format!("{}/", console.base);
        if console.base == "/hawtio" {
            bytes
        } else {
            base_href(&String::from_utf8_lossy(&bytes), &href).into_bytes()
        }
    } else {
        bytes
    };
    let mut r = ([(header::CONTENT_TYPE, content_type(&path))], body).into_response();
    if let Some(m) = modified.and_then(|m| HeaderValue::from_str(&m).ok()) {
        r.headers_mut().insert(header::LAST_MODIFIED, m);
    }
    r
}

/// `index.html`, as the filters forward a client route to it.
fn index(console: &Console, uri: &str, headers: &HeaderMap) -> Response {
    serve_file(console, "/index.html", uri, headers)
}

/// `CacheHeadersFilter.cacheInBrowser`: whether the path asked for, not
/// what answers it, is a file of the `.war` other than `index.html`.
fn cacheable(console: &Console, rel: &str) -> bool {
    if rel == "/" || rel.ends_with("/index.html") {
        return false;
    }
    let decoded = crate::percent_decode(rel);
    console
        .dir
        .as_ref()
        .and_then(|d| resolve(d, &decoded))
        .is_some_and(|p| p.exists())
}

/// The path with its `.` and `..` segments resolved, as Jetty resolves
/// them before routing; none when it leaves the root.
fn remove_dots(path: &str) -> Option<String> {
    let mut out: Vec<&str> = Vec::new();
    let segments: Vec<&str> = path.split('/').skip(1).collect();
    let last = segments.len().saturating_sub(1);
    for (i, s) in segments.iter().enumerate() {
        match *s {
            "." => {
                if i == last {
                    out.push("");
                }
            }
            ".." => {
                out.pop()?;
                if i == last {
                    out.push("");
                }
            }
            s => out.push(s),
        }
    }
    Some(format!("/{}", out.join("/")))
}

/// The answer to a request under the console's context path.
pub(crate) async fn handle(
    State(state): State<Arc<AppState>>,
    Extension(Peer(peer)): Extension<Peer>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let console = &state.console;
    let Some(path) = remove_dots(uri.path()) else {
        return status(StatusCode::BAD_REQUEST);
    };
    let path = path.as_str();
    let Some(rel) = path
        .strip_prefix(console.base.as_str())
        .filter(|r| r.is_empty() || r.starts_with('/'))
    else {
        return status(StatusCode::NOT_FOUND);
    };
    // Jetty's context: `/console` is `/console/`.
    if rel.is_empty() {
        let location = match uri.query() {
            Some(q) => format!("{}/?{q}", console.base),
            None => format!("{}/", console.base),
        };
        return (
            StatusCode::MOVED_PERMANENTLY,
            [
                (header::LOCATION, location),
                (header::CONTENT_LENGTH, "0".to_owned()),
            ],
        )
            .into_response();
    }

    // SessionExpiryFilter.
    let relative = rel.trim_start_matches('/');
    let first = relative.split('/').next().unwrap_or("");
    let mut session = console.session(&headers);
    let now = now_ms();
    if relative.is_empty() {
        if let Some((id, _)) = &session {
            console.touch(id, now);
        }
    } else if session.is_none() || console.timeout < 0 || relative == "auth/logout" {
        if first == "refresh" {
            return refresh_ok();
        }
    } else if let Some((id, s)) = &session {
        if let Some(last) = s.last_access
            && now.saturating_sub(last) / 1000 > console.timeout as u64
        {
            tracing::info!("Expiring session due to inactivity");
            console.invalidate(id);
            return forbidden("NONE");
        }
        if first == "refresh" {
            console.touch(id, now);
            return refresh_ok();
        }
        if !(matches!(first, "jolokia" | "proxy") && s.last_access.is_some()) {
            console.touch(id, now);
        }
    }

    // The rest passes `ResponseHeadersFilter`.
    if first == "jolokia" && (rel == "/jolokia" || rel.starts_with("/jolokia/")) {
        return crate::jolokia(&state, peer, method, &uri, &headers, body, session).await;
    }
    let response = route(
        &state,
        peer,
        &method,
        &uri,
        path,
        rel,
        &headers,
        &body,
        &mut session,
    );
    state.secure(response, !cacheable(console, rel))
}

fn refresh_ok() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/plain;charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        "ok",
    )
        .into_response()
}

/// The filters after the headers, and the servlets.
#[allow(clippy::too_many_arguments)]
fn route(
    state: &AppState,
    peer: SocketAddr,
    method: &Method,
    uri: &Uri,
    path: &str,
    rel: &str,
    headers: &HeaderMap,
    body: &Bytes,
    session: &mut Option<(String, Session)>,
) -> Response {
    let console = &state.console;
    let base = &console.base;

    // AuthenticationFilter on /proxy/*: the proxy is disabled.
    if rel == "/proxy" || rel.starts_with("/proxy/") {
        if rel == "/proxy/enabled" {
            return json("false\n".to_owned());
        }
        return match session {
            None => forbidden("SESSION_EXPIRED"),
            Some(_) => status(StatusCode::NOT_FOUND),
        };
    }

    // ClientRouteRedirectFilter.
    let full = clean(path);
    let route = if full == *base {
        "/".to_owned()
    } else {
        full[base.len().min(full.len())..].to_owned()
    };
    let login_page = route.starts_with("/login");
    let secured = !UNSECURED.iter().any(|p| route.starts_with(p));
    if login_page || secured {
        let target = format!("{base}/");
        let login = format!("{base}/login");
        if session.is_some() {
            if login_page {
                return redirect(&target);
            }
            return index(console, path, headers);
        }
        if login_page {
            return index(console, path, headers);
        }
        // Pre-emptive authentication from an Authorization header.
        let (user, password) = basic(headers).unzip();
        return match authenticate(state, user.as_deref(), password.as_deref(), peer) {
            Auth::Authorized {
                user,
                roles,
                subject,
            } => {
                tracing::info!("Logging in user: {user}");
                let id = console.start(user, roles, subject);
                let mut r = index(console, path, headers);
                console.set_cookie(&mut r, &id);
                r
            }
            Auth::NotAuthorized => redirect(&format!("{login}#noauth")),
            Auth::NoCredentials | Auth::Throttled(_) => redirect(&login),
        };
    }

    // The servlets.
    let servlet = |prefix: &str| -> Option<String> {
        if rel == prefix {
            Some(String::new())
        } else {
            rel.strip_prefix(prefix)
                .filter(|r| r.starts_with('/'))
                .map(|r| r.trim().trim_start_matches('/').to_owned())
        }
    };
    let get = method == Method::GET || method == Method::HEAD;
    if rel == "/auth/login" {
        return login(state, peer, method, body, session);
    }
    if rel == "/auth/logout" {
        if !get {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        if let Some((id, s)) = session.take() {
            tracing::info!("Logging out existing user: {}", s.user);
            console.invalidate(&id);
        }
        return redirect(&format!("{base}/login"));
    }
    if let Some(op) = servlet("/auth/config") {
        if !get {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        let op = if op.is_empty() {
            "login".to_owned()
        } else {
            op
        };
        return match op.as_str() {
            "oidc" => json("{}\n".to_owned()),
            "session-timeout" => {
                let now = now_ms() as i64;
                let t = query(uri, "t")
                    .and_then(|t| t.parse::<i64>().ok())
                    .unwrap_or(now);
                json(format!(
                    "{{\"req\":{t},\"now\":{now},\"timeout\":{}}}\n",
                    console.timeout
                ))
            }
            "login" => json(format!(
                "[{{\"method\":\"form\",\"logoutUrl\":\"{base}/auth/logout\",\
                     \"name\":\"User credentials\",\"userField\":\"username\",\
                     \"passwordField\":\"password\",\"type\":\"json\",\
                     \"url\":\"{base}/auth/login\"}}]\n"
            )),
            _ => status(StatusCode::NOT_FOUND),
        };
    }
    if servlet("/user").is_some() {
        if !get {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        return match session {
            Some((_, s)) => json(format!("{}\n", Json::String(s.user.clone()))),
            None => forbidden("NONE"),
        };
    }
    if let Some(op) = servlet("/keycloak") {
        if !get {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        let op = if op.is_empty() {
            "client-config".to_owned()
        } else {
            op
        };
        return match op.as_str() {
            "enabled" => json("false\n".to_owned()),
            "client-config" => error_page(
                StatusCode::NOT_FOUND,
                path,
                "Keycloak client configuration not found",
                "keycloak",
            ),
            "validate-subject-matches" => {
                match query(uri, "keycloakUser").filter(|u| !u.trim().is_empty()) {
                    None => status(StatusCode::BAD_REQUEST),
                    Some(u) => match session.take() {
                        Some((id, s)) if s.user != u => {
                            console.invalidate(&id);
                            json("false\n".to_owned())
                        }
                        kept => {
                            *session = kept;
                            json("true\n".to_owned())
                        }
                    },
                }
            }
            _ => status(StatusCode::NOT_FOUND),
        };
    }
    if servlet("/plugin").is_some() {
        if !get {
            return status(StatusCode::METHOD_NOT_ALLOWED);
        }
        return json("[]".to_owned());
    }
    serve_file(console, rel, path, headers)
}

/// A query parameter, decoded.
fn query(uri: &Uri, name: &str) -> Option<String> {
    uri.query()?
        .split('&')
        .filter_map(|p| p.split_once('=').or(Some((p, ""))))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| crate::percent_decode(&v.replace('+', " ")))
}

/// `LoginServlet`, with Artemis's audit of it (its `AuthenticationFilter`
/// on `/auth/login/*`).
fn login(
    state: &AppState,
    peer: SocketAddr,
    method: &Method,
    body: &Bytes,
    session: &mut Option<(String, Session)>,
) -> Response {
    let console = &state.console;
    if method == Method::GET || method == Method::HEAD {
        return redirect(&format!("{}/login", console.base));
    }
    if method != Method::POST {
        return status(StatusCode::METHOD_NOT_ALLOWED);
    }
    if let Some((id, s)) = session.take() {
        tracing::info!("Logging out existing user: {}", s.user);
        console.invalidate(&id);
    }
    let remote = format!("{}:{}", peer.ip(), peer.port());
    let failed = |code: u16| {
        if audit::resource_enabled() {
            audit::resource(audit::fill(
                "AMQ601716: User {} failed authentication on connection {}, reason: {}",
                &[
                    &audit::caller(None, Some(&remote)),
                    "null",
                    &code.to_string(),
                ],
            ));
        }
    };
    // `ServletHelpers.readObject` fails on anything but a JSON object.
    let form = match Json::parse(&String::from_utf8_lossy(body)) {
        Ok(form @ Json::Object(_)) => form,
        Ok(_) | Err(_) => {
            failed(500);
            return error_page(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{}/auth/login", console.base),
                "java.io.IOException: the login is not a JSON object",
                "login",
            );
        }
    };
    let field = |k: &str| form.get(k).and_then(Json::as_str).map(str::to_owned);
    let (user, password) = (field("username"), field("password"));
    match authenticate(state, user.as_deref(), password.as_deref(), peer) {
        Auth::Authorized {
            user,
            roles,
            subject,
        } => {
            tracing::info!("Logging in user: {user}");
            let principals: Vec<Json> = subject
                .principals()
                .iter()
                .map(|p| {
                    let (class, name) = match p {
                        Principal::User(u) => (format!("{ARTEMIS_JAAS}.UserPrincipal"), u.clone()),
                        Principal::Role(r) => (format!("{ARTEMIS_JAAS}.RolePrincipal"), r.clone()),
                        Principal::Other { class, name } => (class.clone(), name.clone()),
                    };
                    Json::object().with("name", name).with("type", class)
                })
                .collect();
            let answer = Json::object()
                .with("credentials", Json::Array(Vec::new()))
                .with("principals", Json::Array(principals));
            if audit::resource_enabled() {
                audit::resource(audit::fill(
                    "AMQ601715: User {} successfully authenticated on connection {}",
                    &[&audit::caller(Some(&subject), Some(&remote)), "null"],
                ));
            }
            let id = console.start(user, roles, subject);
            let mut r = json(answer.to_string());
            console.set_cookie(&mut r, &id);
            r
        }
        Auth::NotAuthorized | Auth::NoCredentials => {
            failed(403);
            forbidden("NONE")
        }
        Auth::Throttled(wait) => {
            failed(429);
            too_many(wait)
        }
    }
}

/// The `.war` of an app: in the first of `dirs` (the instance's web
/// directory, then the home's) that has it, as `WebServerComponent` looks.
pub fn find_war(dirs: &[PathBuf], war: &str) -> Option<PathBuf> {
    dirs.iter().map(|d| d.join(war)).find(|p| p.is_dir())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_tags() {
        let html = r#"<head><base href="/console/"/><link href="/console/a.css"><script src="static/x.js"></script>"#;
        assert_eq!(
            base_href(html, "/broker/"),
            r#"<head><base href="/broker/"/><link href="/broker/a.css"><script src="static/x.js"></script>"#
        );
        assert_eq!(base_href("<p>", "/x/"), "<p>");
        assert_eq!(
            base_href("<base href='/hawtio/'><a href='/hawtio/x'>", "/c/"),
            "<base href='/c/'><a href='/c/x'>"
        );
    }

    #[test]
    fn paths() {
        assert_eq!(clean("/console//artemis/"), "/console/artemis");
        assert_eq!(clean("/"), "/");
        let dir = Path::new("/w");
        assert!(resolve(dir, "/static/../../etc/passwd").is_none());
        assert!(resolve(dir, "/static/%2e%2e/x").is_none());
        assert!(resolve(dir, "/a\\b").is_none());
        assert_eq!(
            resolve(dir, "/img/a.png"),
            Some(dir.join("img").join("a.png"))
        );
    }

    #[test]
    fn dot_segments() {
        assert_eq!(
            remove_dots("/console/static/../index.html").as_deref(),
            Some("/console/index.html")
        );
        assert_eq!(
            remove_dots("/console/./a/.").as_deref(),
            Some("/console/a/")
        );
        assert_eq!(remove_dots("/console/a/..").as_deref(), Some("/console/"));
        assert_eq!(remove_dots("/console/../../x"), None);
        assert_eq!(remove_dots("/").as_deref(), Some("/"));
    }

    #[test]
    fn throttling() {
        let a = |count| Attempt {
            timestamp: 10_000,
            count,
        };
        // Three failures are free; the fourth waits one second, less the
        // time since.
        assert!(a(3).retry_after(10_001) <= 0);
        assert_eq!(a(4).retry_after(10_001), 1);
        assert_eq!(a(6).retry_after(10_000), 8);
        assert_eq!(a(60).retry_after(10_000), 24 * 60 * 60);
    }

    #[test]
    fn cookies() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_static("a=1; JSESSIONID_0_8161=node0x.node0; b=2"),
        );
        assert_eq!(
            cookie(&h, "JSESSIONID_0_8161").as_deref(),
            Some("node0x.node0")
        );
        assert_eq!(cookie(&h, "JSESSIONID_1_8161"), None);
    }
}
