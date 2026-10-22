//! The API of Saranga's own console, at `/saranga/api/v1` on every binding.
//!
//! It is the console's view of the same MBeans Jolokia serves: every call
//! goes through the Jolokia agent's guard (`management.xml`'s
//! `<authorisation>`, or the `mops.` RBAC guard), parameter conversion and
//! audit, so a user may do here exactly what Artemis's console lets them
//! do. The login is the console's: the broker's JAAS domain, a console role
//! (`HAWTIO_ROLES`), and the same throttling of failed logins.
//!
//! - `POST   /session` `{"user", "password"}` logs in; `DELETE /session`
//!   logs out. A request may instead carry `Authorization: Basic`.
//! - `GET    /me`: the user, their roles, and which broker operations the
//!   guard lets them call. `POST /me/can` `{"checks": [{"resource",
//!   "method"}]}` asks about others.
//! - `GET    /views`: the fields each listing filters and sorts by.
//! - `GET    /broker`: the broker's status attributes the user may read.
//! - `GET    /{connections|sessions|consumers|producers|addresses|queues}`
//!   `?filter=field:OPERATION:value&sort=field&order=asc|desc&page=1&size=100`:
//!   one page of the listing, `{"data", "count", "page", "size"}`.
//! - `DELETE /connections/{id}`, `/sessions/{connection}/{id}`,
//!   `/consumers/{session}/{id}`: closes one.
//! - `GET    /openapi.json`: this API, as OpenAPI 3.1.
//!
//! A request that changes something must carry `X-Saranga-Console: 1`, which
//! a cross-site form cannot send. Errors are `{"error", "status"}`.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use saranga_security::audit;
use saranga_server::Current;
use saranga_server::management::json::Json;
use saranga_server::management::view_fields;

use crate::console::{self, Auth, Console};
use crate::jolokia::{Agent, Caller, Failure};
use crate::mbean::ObjectName;
use crate::rbac::Guard;
use crate::{Peer, percent_decode};

/// Where the console is served.
pub const BASE: &str = "/saranga";
/// Where its API is.
pub const API: &str = "/saranga/api/v1";
/// The header every request that changes something must carry.
const MARK: &str = "x-saranga-console";

/// The listings, in the order the console shows them.
pub const LISTINGS: [&str; 6] = [
    "connections",
    "sessions",
    "consumers",
    "producers",
    "addresses",
    "queues",
];

/// The broker operations `GET /me` reports on: what the console's pages
/// offer.
const BROKER_ACTIONS: &[&str] = &[
    "listConnections",
    "listSessions",
    "listConsumers",
    "listProducers",
    "listAddresses",
    "listQueues",
    "closeConnectionWithID",
    "closeSessionWithID",
    "closeConsumerWithID",
    "createAddress",
    "deleteAddress",
    "createQueue",
    "destroyQueue",
];

/// The attributes `GET /broker` reads, those the broker's control has.
const STATUS: &[&str] = &[
    "Name",
    "Version",
    "NodeID",
    "Uptime",
    "UptimeMillis",
    "Started",
    "Active",
    "Backup",
    "ReplicaSync",
    "SharedStore",
    "HAPolicy",
    "Clustered",
    "PersistenceEnabled",
    "SecurityEnabled",
    "JournalType",
    "AddressMemoryUsage",
    "AddressMemoryUsagePercentage",
    "GlobalMaxSize",
    "DiskStoreUsage",
    "MaxDiskUsage",
    "ConnectionCount",
    "TotalConnectionCount",
    "TotalMessageCount",
    "TotalMessagesAdded",
    "TotalMessagesAcknowledged",
    "TotalConsumerCount",
];

/// The largest page a listing serves.
const MAX_PAGE_SIZE: i64 = 10_000;
/// The most checks one `POST /me/can` answers.
const MAX_CHECKS: usize = 1_000;

pub(crate) struct ApiState {
    pub agent: Agent,
    pub broker: Arc<Current>,
    pub console_roles: Vec<String>,
    pub sessions: Console,
}

pub(crate) fn router(state: Arc<ApiState>) -> Router {
    let api = Router::new()
        .route("/openapi.json", get(openapi))
        .route("/session", post(login).delete(logout))
        .route("/me", get(me))
        .route("/me/can", post(can))
        .route("/views", get(views))
        .route("/broker", get(broker))
        .route("/{kind}", get(listing))
        .route("/connections/{id}", delete(close_connection))
        .route("/sessions/{connection}/{id}", delete(close_session))
        .route("/consumers/{session}/{id}", delete(close_consumer))
        .fallback(no_such_api)
        .with_state(state);
    Router::new().nest(API, api)
}

fn reply(status: StatusCode, body: &Json) -> Response {
    crate::secure(
        (
            status,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, console::NO_CACHE),
            ],
            body.to_string(),
        )
            .into_response(),
    )
}

fn ok(body: &Json) -> Response {
    reply(StatusCode::OK, body)
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    reply(
        status,
        &Json::object()
            .with("error", message.into())
            .with("status", status.as_u16() as i64),
    )
}

/// A failed call: the guard's refusals are 403, what is not there 404, bad
/// parameters 400, and an operation that failed 400 with its message.
fn failure(f: Failure) -> Response {
    let status = match f.status {
        403 => StatusCode::FORBIDDEN,
        404 => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_REQUEST,
    };
    error(status, f.message)
}

async fn no_such_api() -> Response {
    error(StatusCode::NOT_FOUND, "no such API")
}

/// A logged-in user.
struct User {
    name: String,
    caller: Caller,
}

/// The user a request runs as: its session's, unless an `Authorization:
/// Basic` header names another user, as for Jolokia.
fn user(state: &ApiState, headers: &HeaderMap, peer: SocketAddr) -> Result<User, Box<Response>> {
    let remote = Some(format!("{}:{}", peer.ip(), peer.port()));
    let basic = console::basic(headers);
    if let Some((id, s)) = state.sessions.session(headers) {
        match &basic {
            Some((name, _)) if *name != s.user => state.sessions.invalidate(&id),
            _ => {
                return Ok(User {
                    name: s.user,
                    caller: Caller {
                        roles: s.roles,
                        subject: Some(s.subject),
                        remote,
                    },
                });
            }
        }
    }
    let Some((name, password)) = basic else {
        return Err(Box::new(error(StatusCode::UNAUTHORIZED, "not logged in")));
    };
    match authenticate(state, &name, &password, peer) {
        Auth::Authorized {
            user,
            roles,
            subject,
        } => Ok(User {
            name: user,
            caller: Caller {
                roles,
                subject: Some(subject),
                remote,
            },
        }),
        Auth::Throttled(wait) => Err(Box::new(throttled(wait))),
        Auth::NotAuthorized | Auth::NoCredentials => Err(Box::new(error(
            StatusCode::UNAUTHORIZED,
            "wrong user or password",
        ))),
    }
}

fn authenticate(state: &ApiState, user: &str, password: &str, peer: SocketAddr) -> Auth {
    console::login_user(
        &state.sessions,
        &state.broker,
        &state.console_roles,
        Some(user),
        Some(password),
        peer,
    )
}

fn throttled(wait: i64) -> Response {
    let mut r = error(
        StatusCode::TOO_MANY_REQUESTS,
        format!("too many failed logins: try again in {wait} s"),
    );
    if let Ok(v) = HeaderValue::from_str(&wait.to_string()) {
        r.headers_mut().insert(header::RETRY_AFTER, v);
    }
    r
}

/// A request that changes something must say the console sent it.
fn marked(headers: &HeaderMap) -> Result<(), Box<Response>> {
    match headers.get(MARK).and_then(|v| v.to_str().ok()) {
        Some("1") => Ok(()),
        _ => Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "a request that changes something needs the header X-Saranga-Console: 1",
        ))),
    }
}

/// The broker's MBean.
fn broker_bean(state: &ApiState) -> Result<ObjectName, Box<Response>> {
    state
        .agent
        .bean_of("broker")
        .map(|(n, _)| n)
        .ok_or_else(|| {
            Box::new(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "the broker is not running",
            ))
        })
}

/// An operation on the broker's MBean.
async fn on_broker(
    state: &ApiState,
    user: &User,
    operation: &str,
    arguments: Vec<Json>,
) -> Result<Json, Box<Response>> {
    let name = broker_bean(state)?;
    state
        .agent
        .call(&user.caller, name, operation, arguments)
        .await
        .map_err(|f| Box::new(failure(f)))
}

async fn login(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(r) = marked(&headers) {
        return *r;
    }
    if let Some((id, _)) = state.sessions.session(&headers) {
        state.sessions.invalidate(&id);
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
    let form = Json::parse(&String::from_utf8_lossy(&body)).unwrap_or(Json::Null);
    let field = |k: &str| form.get(k).and_then(Json::as_str).unwrap_or("").to_owned();
    let (name, password) = (field("user"), field("password"));
    match authenticate(&state, &name, &password, peer) {
        Auth::Authorized {
            user,
            roles,
            subject,
        } => {
            tracing::info!("Saranga console: logging in user {user}");
            if audit::resource_enabled() {
                audit::resource(audit::fill(
                    "AMQ601715: User {} successfully authenticated on connection {}",
                    &[&audit::caller(Some(&subject), Some(&remote)), "null"],
                ));
            }
            let caller = Caller {
                roles: roles.clone(),
                subject: Some(subject.clone()),
                remote: Some(remote.clone()),
            };
            let body = me_json(&state, &user, &caller);
            let id = state.sessions.start(user, roles, subject);
            let mut r = ok(&body);
            state.sessions.set_cookie(&mut r, &id);
            r
        }
        Auth::Throttled(wait) => {
            failed(429);
            throttled(wait)
        }
        Auth::NotAuthorized | Auth::NoCredentials => {
            failed(401);
            error(StatusCode::UNAUTHORIZED, "wrong user or password")
        }
    }
}

async fn logout(State(state): State<Arc<ApiState>>, headers: HeaderMap) -> Response {
    if let Err(r) = marked(&headers) {
        return *r;
    }
    if let Some((id, s)) = state.sessions.session(&headers) {
        tracing::info!("Saranga console: logging out user {}", s.user);
        state.sessions.invalidate(&id);
    }
    let mut r = crate::secure(StatusCode::NO_CONTENT.into_response());
    state.sessions.clear_cookie(&mut r);
    r
}

/// The user, their roles, the guard, and the broker operations it lets
/// them call.
fn me_json(state: &ApiState, user: &str, caller: &Caller) -> Json {
    let mut actions = Json::object();
    let name = state.agent.bean_of("broker").map(|(n, _)| n);
    for a in BROKER_ACTIONS {
        let allowed = name.as_ref().is_some_and(|n| state.agent.may(caller, n, a));
        actions.put(a, allowed);
    }
    let guard = match &state.agent.access {
        Guard::Acl(_) => "management.xml",
        Guard::Rbac(_) => "rbac",
    };
    Json::object()
        .with("user", user)
        .with(
            "roles",
            Json::Array(
                caller
                    .roles
                    .iter()
                    .map(|r| Json::from(r.as_str()))
                    .collect(),
            ),
        )
        .with("guard", guard)
        .with("broker", actions)
}

async fn me(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    headers: HeaderMap,
) -> Response {
    match user(&state, &headers, peer) {
        Ok(u) => ok(&me_json(&state, &u.name, &u.caller)),
        Err(r) => *r,
    }
}

/// Whether the user may call each method on each resource: what the
/// console asks before it offers an action.
async fn can(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let Ok(request) = Json::parse(&String::from_utf8_lossy(&body)) else {
        return error(StatusCode::BAD_REQUEST, "the body must be JSON");
    };
    let Some(Json::Array(checks)) = request.get("checks") else {
        return error(StatusCode::BAD_REQUEST, "the body needs a checks array");
    };
    if checks.len() > MAX_CHECKS {
        return error(
            StatusCode::BAD_REQUEST,
            format!("at most {MAX_CHECKS} checks at a time"),
        );
    }
    let results: Vec<Json> = checks
        .iter()
        .map(|c| {
            let s = |k: &str| c.get(k).and_then(Json::as_str);
            let allowed = match (s("resource"), s("method")) {
                (Some(r), Some(m)) => state
                    .agent
                    .bean_of(r)
                    .is_some_and(|(n, _)| state.agent.may(&u.caller, &n, m)),
                _ => false,
            };
            Json::from(allowed)
        })
        .collect();
    ok(&Json::object().with("results", Json::Array(results)))
}

/// The fields of one listing.
fn view_json(kind: &str) -> Option<Json> {
    let (fields, default_sort) = view_fields(kind)?;
    let fields = fields
        .iter()
        .map(|(constant, name)| {
            Json::object()
                .with("name", *name)
                .with("constant", *constant)
        })
        .collect();
    Some(
        Json::object()
            .with("fields", Json::Array(fields))
            .with("defaultSort", default_sort),
    )
}

async fn views(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = user(&state, &headers, peer) {
        return *r;
    }
    let mut out = Json::object();
    for kind in LISTINGS {
        if let Some(v) = view_json(kind) {
            out.put(kind, v);
        }
    }
    ok(&out)
}

async fn broker(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let Some((name, bean)) = state.agent.bean_of("broker") else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "the broker is not running");
    };
    let control = bean.control();
    let mut attributes = Json::object();
    let mut denied = Vec::new();
    for a in STATUS.iter().filter(|a| control.attr(a).is_some()) {
        match state.agent.read_attribute(&u.caller, &name, &bean, a).await {
            Ok(v) => attributes.put(a, v),
            Err(f) if f.status == 403 => denied.push(Json::from(*a)),
            // Not readable on this broker: left out.
            Err(_) => {}
        }
    }
    ok(&Json::object()
        .with("attributes", attributes)
        .with("denied", Json::Array(denied)))
}

/// A listing's query: its filters, sort and page.
#[derive(Debug, PartialEq)]
struct ListQuery {
    filters: Vec<(String, String, String)>,
    sort: Option<(String, bool)>,
    page: i64,
    size: i64,
}

/// `filter=field:OPERATION:value` (repeated), `sort`, `order`, `page` (from
/// 1) and `size`.
fn list_query(query: Option<&str>) -> Result<ListQuery, String> {
    let mut q = ListQuery {
        filters: Vec::new(),
        sort: None,
        page: 1,
        size: 100,
    };
    let mut sort = None;
    let mut descending = false;
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = percent_decode(&v.replace('+', " "));
        match k {
            "filter" => {
                let mut parts = v.splitn(3, ':');
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(f), Some(op), Some(value)) if !f.is_empty() && !op.is_empty() => q
                        .filters
                        .push((f.to_owned(), op.to_uppercase(), value.to_owned())),
                    _ => return Err(format!("a filter is field:OPERATION:value, not {v}")),
                }
            }
            "sort" => sort = Some(v),
            "order" => match v.to_ascii_lowercase().as_str() {
                "asc" => descending = false,
                "desc" => descending = true,
                _ => return Err(format!("order is asc or desc, not {v}")),
            },
            "page" | "size" => {
                let n: i64 = v
                    .parse()
                    .ok()
                    .filter(|n| *n >= 1)
                    .ok_or_else(|| format!("{k} is a number from 1, not {v}"))?;
                if k == "page" {
                    q.page = n;
                } else if n > MAX_PAGE_SIZE {
                    return Err(format!("size is at most {MAX_PAGE_SIZE}"));
                } else {
                    q.size = n;
                }
            }
            _ => return Err(format!("unknown parameter {k}")),
        }
    }
    q.sort = sort.filter(|s| !s.is_empty()).map(|s| (s, descending));
    Ok(q)
}

/// The options the views take: `searchFilters`, `sortField` and
/// `sortOrder`.
fn view_options(q: &ListQuery) -> String {
    let filters = q
        .filters
        .iter()
        .map(|(f, op, v)| {
            Json::object()
                .with("field", f.as_str())
                .with("operation", op.as_str())
                .with("value", v.as_str())
        })
        .collect();
    let mut o = Json::object().with("searchFilters", Json::Array(filters));
    if let Some((field, descending)) = &q.sort {
        o.put("sortField", field.as_str());
        o.put("sortOrder", if *descending { "desc" } else { "asc" });
    }
    o.to_string()
}

fn list_operation(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "connections" => "listConnections(java.lang.String,int,int)",
        "sessions" => "listSessions(java.lang.String,int,int)",
        "consumers" => "listConsumers(java.lang.String,int,int)",
        "producers" => "listProducers(java.lang.String,int,int)",
        "addresses" => "listAddresses(java.lang.String,int,int)",
        "queues" => "listQueues(java.lang.String,int,int)",
        _ => return None,
    })
}

async fn listing(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path(kind): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let Some(operation) = list_operation(&kind) else {
        return no_such_api().await;
    };
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let q = match list_query(uri.query()) {
        Ok(q) => q,
        Err(e) => return error(StatusCode::BAD_REQUEST, e),
    };
    let arguments = vec![
        Json::from(view_options(&q)),
        Json::Long(q.page),
        Json::Long(q.size),
    ];
    let answer = match on_broker(&state, &u, operation, arguments).await {
        Ok(a) => a,
        Err(r) => return *r,
    };
    // The views answer JSON text.
    let mut page = match &answer {
        Json::String(text) => match Json::parse(text) {
            Ok(j) => j,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e),
        },
        other => other.clone(),
    };
    page.put("page", q.page);
    page.put("size", q.size);
    ok(&page)
}

async fn close(
    state: &ApiState,
    headers: &HeaderMap,
    peer: SocketAddr,
    operation: &str,
    arguments: Vec<Json>,
) -> Response {
    if let Err(r) = marked(headers) {
        return *r;
    }
    let u = match user(state, headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    match on_broker(state, &u, operation, arguments).await {
        // `closeConnectionWithID` and the others answer whether it was
        // there to close.
        Ok(Json::Bool(false)) => error(StatusCode::NOT_FOUND, "nothing to close by that ID"),
        Ok(_) => crate::secure(StatusCode::NO_CONTENT.into_response()),
        Err(r) => *r,
    }
}

async fn close_connection(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    close(
        &state,
        &headers,
        peer,
        "closeConnectionWithID(java.lang.String)",
        vec![Json::from(id)],
    )
    .await
}

async fn close_session(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path((connection, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    close(
        &state,
        &headers,
        peer,
        "closeSessionWithID(java.lang.String,java.lang.String)",
        vec![Json::from(connection), Json::from(id)],
    )
    .await
}

async fn close_consumer(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path((session, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    close(
        &state,
        &headers,
        peer,
        "closeConsumerWithID(java.lang.String,java.lang.String)",
        vec![Json::from(session), Json::from(id)],
    )
    .await
}

async fn openapi() -> Response {
    ok(&crate::api_schema::document())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_queries() {
        assert_eq!(
            list_query(None).unwrap(),
            ListQuery {
                filters: vec![],
                sort: None,
                page: 1,
                size: 100
            }
        );
        let q = list_query(Some(
            "filter=name:contains:a%3Ab&filter=consumerCount:GREATER_THAN:0&sort=name&order=desc&page=2&size=10",
        ))
        .unwrap();
        assert_eq!(
            q.filters,
            vec![
                ("name".into(), "CONTAINS".into(), "a:b".into()),
                ("consumerCount".into(), "GREATER_THAN".into(), "0".into())
            ]
        );
        assert_eq!(q.sort, Some(("name".into(), true)));
        assert_eq!((q.page, q.size), (2, 10));
        assert_eq!(
            view_options(&q),
            r#"{"searchFilters":[{"field":"name","operation":"CONTAINS","value":"a:b"},{"field":"consumerCount","operation":"GREATER_THAN","value":"0"}],"sortField":"name","sortOrder":"desc"}"#
        );
        assert!(list_query(Some("page=0")).is_err());
        assert!(list_query(Some("size=10001")).is_err());
        assert!(list_query(Some("filter=name")).is_err());
        assert!(list_query(Some("order=up")).is_err());
        assert!(list_query(Some("bogus=1")).is_err());
    }

    #[test]
    fn every_listing_has_a_view() {
        for kind in LISTINGS {
            assert!(view_json(kind).is_some(), "{kind}");
            assert!(list_operation(kind).is_some(), "{kind}");
        }
    }

    #[test]
    fn status_attributes_exist() {
        for a in STATUS {
            assert!(crate::catalog::BROKER.attr(a).is_some(), "{a}");
        }
        for a in BROKER_ACTIONS {
            assert!(!crate::catalog::BROKER.ops_named(a).is_empty(), "{a}");
        }
    }
}
