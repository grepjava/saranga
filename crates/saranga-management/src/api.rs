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
//! - `GET    /resources`: the management resources (MBeans) the user may
//!   see. `GET /resources/{resource}` describes one, with its attributes'
//!   values and its operations, each with whether the guard allows it;
//!   `GET /resources/{resource}/attributes?name=A&name=B` reads some.
//! - `PUT    /resources/{resource}/attributes/{name}` `{"value"}` writes an
//!   attribute; `POST /resources/{resource}/operations` `{"operation",
//!   "arguments"}` calls an operation, by signature when it is overloaded.
//! - `GET    /events`: the live data, as server-sent events; `PUT
//!   /events/{stream}` sets what a stream watches. `GET /history`: the last
//!   hour of some resources' counters. `GET /notifications`: the last
//!   management notifications. See [`crate::live`].
//! - `GET    /openapi.json`: this API, as OpenAPI 3.1.
//!
//! A request that changes something must carry `X-Saranga-Console: 1`, which
//! a cross-site form cannot send. Errors are `{"error", "status"}`.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, FromRequestParts, Path, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post, put};
use saranga_security::audit;
use saranga_server::Current;
use saranga_server::management::json::Json;
use saranga_server::management::view_fields;
use tower::ServiceExt;

use crate::console::{self, Auth, Console};
use crate::jolokia::{Agent, Caller, Failure};
use crate::live::{self, Live};
use crate::mbean::{Bean, ObjectName};
use crate::peers::{self, Credentials, Peers, Request as PeerRequest, Scope};
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
    "listNetworkTopology",
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
    /// The history and notifications every binding shares.
    pub live: Arc<Live>,
    /// The links to the other nodes, which every binding shares.
    pub peers: Arc<Peers>,
}

pub(crate) fn router(state: Arc<ApiState>) -> Router {
    // `/nodes/{node}/...` is the API again, for another node: the same
    // routes, with the node the request is for.
    let scoped = Router::new().nest(API, routes().with_state(state.clone()));
    let api = routes()
        .route("/nodes", get(nodes))
        .route(
            "/nodes/{node}/{*rest}",
            any(move |p, s, r| for_node(scoped.clone(), p, s, r)),
        )
        .with_state(state);
    Router::new().nest(API, api)
}

fn routes() -> Router<Arc<ApiState>> {
    Router::new()
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
        .route("/resources", get(resources))
        .route("/resources/{resource}", get(resource))
        .route("/resources/{resource}/attributes", get(attributes))
        .route("/resources/{resource}/attributes/{name}", put(write))
        .route("/resources/{resource}/operations", post(invoke))
        .route("/events", get(live::events))
        .route("/events/{stream}", put(live::watch))
        .route("/history", get(live::history))
        .route("/notifications", get(live::notifications))
        .fallback(no_such_api)
}

/// `/nodes/{node}/{rest}`: the request for `/{rest}`, for that node.
async fn for_node(
    scoped: Router,
    Path((node, rest)): Path<(String, String)>,
    State(state): State<Arc<ApiState>>,
    request: Request,
) -> Response {
    let scope = Scope::parse(&node, state.broker.get().node_id());
    let (parts, body) = request.into_parts();
    let path = match parts.uri.query() {
        Some(q) => format!("{API}/{rest}?{q}"),
        None => format!("{API}/{rest}"),
    };
    let Ok(uri) = path.parse::<Uri>() else {
        return no_such_api().await;
    };
    // A new request, so the routes inside see their own path parameters
    // only: the client's address and the node go with it.
    let mut inner = Request::new(body);
    *inner.method_mut() = parts.method;
    *inner.uri_mut() = uri;
    *inner.version_mut() = parts.version;
    *inner.headers_mut() = parts.headers;
    if let Some(p) = parts.extensions.get::<Peer>() {
        inner.extensions_mut().insert(*p);
    }
    inner.extensions_mut().insert(scope);
    match scoped.oneshot(inner).await {
        Ok(r) => r,
        Err(e) => match e {},
    }
}

/// The nodes of the cluster this broker knows of: itself, then the others
/// its cluster connections' topology has.
async fn nodes(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = user(&state, &headers, peer) {
        return *r;
    }
    let broker = state.broker.get();
    let endpoint = |tc: &Option<_>| match tc.as_ref().and_then(peers::endpoint) {
        Some(e) => Json::from(e),
        None => Json::Null,
    };
    let data = peers::members(&broker)
        .iter()
        .map(|m| {
            Json::object()
                .with("nodeID", m.node_id.as_str())
                .with("local", m.local)
                .with("primary", endpoint(&m.primary))
                .with("backup", endpoint(&m.backup))
                .with(
                    "backupGroup",
                    m.backup_group.as_deref().map_or(Json::Null, Json::from),
                )
                .with(
                    "clusters",
                    Json::Array(m.clusters.iter().map(|c| Json::from(c.as_str())).collect()),
                )
        })
        .collect();
    ok(&Json::object()
        .with("local", broker.node_id())
        .with("data", Json::Array(data)))
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

pub(crate) fn ok(body: &Json) -> Response {
    reply(StatusCode::OK, body)
}

pub(crate) fn error(status: StatusCode, message: impl Into<String>) -> Response {
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
    Problem::from(f).response()
}

/// Why a request failed, for an answer or for a view of the live stream.
pub(crate) struct Problem {
    pub status: StatusCode,
    pub message: String,
    /// The node as a whole could not be asked (not there, not reachable,
    /// or refusing the user), so no more is asked of it.
    pub whole: bool,
}

impl Problem {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            whole: false,
        }
    }

    /// A failure of the node as a whole.
    pub fn of_node(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            whole: true,
            ..Self::new(status, message)
        }
    }

    pub fn response(self) -> Response {
        error(self.status, self.message)
    }

    /// As a view of the live stream gives it.
    pub fn json(&self) -> Json {
        Json::object()
            .with("error", self.message.as_str())
            .with("status", self.status.as_u16() as i64)
    }
}

impl From<Failure> for Problem {
    fn from(f: Failure) -> Self {
        let status = match f.status {
            403 => StatusCode::FORBIDDEN,
            404 => StatusCode::NOT_FOUND,
            _ => StatusCode::BAD_REQUEST,
        };
        Self::new(status, f.message)
    }
}

async fn no_such_api() -> Response {
    error(StatusCode::NOT_FOUND, "no such API")
}

/// A logged-in user.
#[derive(Clone)]
pub(crate) struct User {
    pub name: String,
    pub caller: Caller,
    /// What the other nodes are asked as.
    pub password: Option<String>,
}

impl User {
    pub fn credentials(&self) -> Credentials {
        Credentials {
            user: self.name.clone(),
            password: self.password.clone(),
        }
    }
}

/// The user a request runs as: its session's, unless an `Authorization:
/// Basic` header names another user, as for Jolokia.
pub(crate) fn user(
    state: &ApiState,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Result<User, Box<Response>> {
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
                    password: s.password,
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
            password: Some(password),
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
pub(crate) fn marked(headers: &HeaderMap) -> Result<(), Box<Response>> {
    match headers.get(MARK).and_then(|v| v.to_str().ok()) {
        Some("1") => Ok(()),
        _ => Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "a request that changes something needs the header X-Saranga-Console: 1",
        ))),
    }
}

/// The node a request is for, which `/nodes/{node}/...` sets; this broker
/// otherwise.
pub(crate) struct Scoped(pub Scope);

impl<S: Send + Sync> FromRequestParts<S> for Scoped {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(
            parts
                .extensions
                .get::<Scope>()
                .cloned()
                .unwrap_or(Scope::Local),
        ))
    }
}

/// Where a request goes: this broker's Jolokia agent, or another node by
/// management messages.
#[derive(Clone, Debug)]
pub(crate) enum Target {
    Local,
    Peer(String),
}

impl Target {
    /// One node's target; every node's only for the listings.
    pub fn of(scope: &Scope) -> Result<Self, Problem> {
        match scope {
            Scope::Local => Ok(Self::Local),
            Scope::Node(n) => Ok(Self::Peer(n.clone())),
            Scope::All => Err(Problem::new(
                StatusCode::BAD_REQUEST,
                "only the listings span every node",
            )),
        }
    }
}

/// An operation's name without its signature: a management message names
/// the operation, and the node picks the one its arguments fit.
fn bare(operation: &str) -> &str {
    operation.split_once('(').map_or(operation, |(n, _)| n)
}

/// An operation on another node's resource, as the user.
async fn peer_call(
    state: &ApiState,
    u: &User,
    node: &str,
    resource: &str,
    operation: &str,
    arguments: Vec<Json>,
) -> Result<Json, Problem> {
    state
        .peers
        .request(
            &state.broker.get(),
            node,
            &u.credentials(),
            PeerRequest::Invoke {
                resource,
                operation: bare(operation),
                arguments,
            },
        )
        .await
}

/// An attribute of another node's resource, as the user.
pub(crate) async fn peer_attribute(
    state: &ApiState,
    u: &User,
    node: &str,
    resource: &str,
    name: &str,
) -> Result<Json, Problem> {
    state
        .peers
        .request(
            &state.broker.get(),
            node,
            &u.credentials(),
            PeerRequest::Attribute { resource, name },
        )
        .await
}

/// An operation on a resource of the target.
pub(crate) async fn call_on(
    state: &ApiState,
    u: &User,
    t: &Target,
    resource: &str,
    operation: &str,
    arguments: Vec<Json>,
) -> Result<Json, Problem> {
    match t {
        Target::Local => {
            let Some((name, _)) = state.agent.bean_of(resource) else {
                return Err(if resource == "broker" {
                    Problem::new(StatusCode::SERVICE_UNAVAILABLE, "the broker is not running")
                } else {
                    Problem::new(
                        StatusCode::NOT_FOUND,
                        format!("no such resource: {resource}"),
                    )
                });
            };
            Ok(state
                .agent
                .call(&u.caller, name, operation, arguments)
                .await?)
        }
        Target::Peer(node) => peer_call(state, u, node, resource, operation, arguments).await,
    }
}

/// An operation on the target's broker answering JSON text, parsed.
pub(crate) async fn broker_json_on(
    state: &ApiState,
    u: &User,
    t: &Target,
    operation: &str,
    arguments: Vec<Json>,
) -> Result<Json, Problem> {
    match call_on(state, u, t, "broker", operation, arguments).await? {
        Json::String(text) => {
            Json::parse(&text).map_err(|e| Problem::new(StatusCode::INTERNAL_SERVER_ERROR, e))
        }
        other => Ok(other),
    }
}

/// Whether a failure says the node itself could not be asked, rather than
/// that it refused or failed what it was asked.
fn unreachable(p: &Problem) -> bool {
    p.whole || p.status.is_server_error()
}

/// The resource of a management resource name (`ResourceNames`), for the
/// shape of another node's: its attributes and operations are the same.
fn bean_named(resource: &str) -> Option<Bean> {
    if resource == "broker" {
        return Some(Bean::Broker);
    }
    let (kind, name) = resource.split_once('.')?;
    let name = name.to_owned();
    Some(match kind {
        "address" => Bean::Address(name),
        "queue" => Bean::Queue(name),
        "divert" => Bean::Divert(name),
        "acceptor" => Bean::Acceptor(name, Vec::new()),
        "broadcastgroup" => Bean::BroadcastGroup(name),
        "bridge" => Bean::Bridge(name),
        "brokerconnection" => Bean::BrokerConnection(name),
        "clusterconnection" => Bean::ClusterConnection(name),
        _ => return None,
    })
}

/// Another node's resource, as `GET /resources` lists one: management
/// messages do not give its object name.
fn peer_resource_json(bean: &Bean) -> Json {
    Json::object()
        .with("resource", bean.resource())
        .with("type", resource_type(bean))
        .with("objectName", "")
        .with("className", bean.class_name())
}

/// A resource of a kind, by its name.
type NamedBean = fn(String) -> Bean;

/// The resources of another node: its broker, and those the broker's
/// attributes name.
async fn peer_resources(state: &ApiState, u: &User, node: &str) -> Result<Vec<Json>, Problem> {
    let mut out = vec![peer_resource_json(&Bean::Broker)];
    let kinds: [(&str, NamedBean); 5] = [
        ("AddressNames", Bean::Address),
        ("QueueNames", Bean::Queue),
        ("DivertNames", Bean::Divert),
        ("BridgeNames", Bean::Bridge),
        ("ClusterConnectionNames", Bean::ClusterConnection),
    ];
    for (attr, bean) in kinds {
        let names = match peer_attribute(state, u, node, "broker", attr).await {
            Ok(Json::Array(names)) => names,
            Ok(_) => Vec::new(),
            Err(p) if unreachable(&p) => return Err(p),
            Err(_) => continue,
        };
        let mut names: Vec<String> = names
            .iter()
            .filter_map(|n| n.as_str().map(str::to_owned))
            .collect();
        names.sort();
        out.extend(names.into_iter().map(|n| peer_resource_json(&bean(n))));
    }
    if let Ok(Json::Array(connections)) = broker_json_on(
        state,
        u,
        &Target::Peer(node.to_owned()),
        "listBrokerConnections",
        Vec::new(),
    )
    .await
    {
        out.extend(
            connections
                .iter()
                .filter_map(|c| c.get("name").and_then(Json::as_str))
                .map(|n| peer_resource_json(&Bean::BrokerConnection(n.to_owned()))),
        );
    }
    Ok(out)
}

/// One attribute's value on another node, or why it could not be read.
pub(crate) async fn peer_attribute_value(
    state: &ApiState,
    u: &User,
    node: &str,
    resource: &str,
    attr: &str,
) -> Result<Json, Problem> {
    match peer_attribute(state, u, node, resource, attr).await {
        Ok(v) => Ok(Json::object().with("value", v)),
        Err(p) if unreachable(&p) => Err(p),
        Err(p) => Ok(Json::object()
            .with("error", p.message)
            .with("denied", p.status == StatusCode::FORBIDDEN)),
    }
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
            let id = state
                .sessions
                .start(user, roles, subject, Some(password.clone()));
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
        state.peers.forget(&s.user).await;
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

/// On another node the guard is that node's: it answers each request, so
/// what the user may do there is not known beforehand (`null`).
fn me_peer_json(u: &User) -> Json {
    let mut actions = Json::object();
    for a in BROKER_ACTIONS {
        actions.put(a, Json::Null);
    }
    Json::object()
        .with("user", u.name.as_str())
        .with(
            "roles",
            Json::Array(
                u.caller
                    .roles
                    .iter()
                    .map(|r| Json::from(r.as_str()))
                    .collect(),
            ),
        )
        .with("guard", "node")
        .with("broker", actions)
}

async fn me(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    match Target::of(&scope) {
        Ok(Target::Local) => ok(&me_json(&state, &u.name, &u.caller)),
        Ok(Target::Peer(_)) => ok(&me_peer_json(&u)),
        Err(p) => p.response(),
    }
}

/// Whether the user may call each method on each resource: what the
/// console asks before it offers an action.
async fn can(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let local = match Target::of(&scope) {
        Ok(t) => matches!(t, Target::Local),
        Err(p) => return p.response(),
    };
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
            if !local {
                return Json::Null;
            }
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
    Scoped(scope): Scoped,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let t = match Target::of(&scope) {
        Ok(t) => t,
        Err(p) => return p.response(),
    };
    match status_on(&state, &u, &t).await {
        Ok(s) => ok(&s),
        Err(p) => p.response(),
    }
}

/// The broker's status attributes the user may read, and those they may
/// not.
pub(crate) async fn status(state: &ApiState, u: &User) -> Result<Json, Problem> {
    let Some((name, bean)) = state.agent.bean_of("broker") else {
        return Err(Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the broker is not running",
        ));
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
    Ok(Json::object()
        .with("attributes", attributes)
        .with("denied", Json::Array(denied)))
}

/// The target's status attributes the user may read, and those they may
/// not.
pub(crate) async fn status_on(state: &ApiState, u: &User, t: &Target) -> Result<Json, Problem> {
    let Target::Peer(node) = t else {
        return status(state, u).await;
    };
    let mut attributes = Json::object();
    let mut denied = Vec::new();
    for a in STATUS {
        match peer_attribute(state, u, node, "broker", a).await {
            Ok(v) => attributes.put(a, v),
            Err(p) if unreachable(&p) => return Err(p),
            Err(p) if p.status == StatusCode::FORBIDDEN => denied.push(Json::from(*a)),
            Err(_) => {}
        }
    }
    Ok(Json::object()
        .with("attributes", attributes)
        .with("denied", Json::Array(denied)))
}

/// A listing's query: its filters, sort and page.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListQuery {
    filters: Vec<(String, String, String)>,
    sort: Option<(String, bool)>,
    pub page: i64,
    pub size: i64,
}

/// `filter=field:OPERATION:value` (repeated), `sort`, `order`, `page` (from
/// 1) and `size`.
pub(crate) fn list_query(query: Option<&str>) -> Result<ListQuery, String> {
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

pub(crate) fn list_operation(kind: &str) -> Option<&'static str> {
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
    Scoped(scope): Scoped,
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
    let page = match scope {
        Scope::All => Ok(list_all(&state, &u, operation, &q).await),
        scope => match Target::of(&scope) {
            Ok(t) => list_on(&state, &u, &t, operation, &q).await,
            Err(p) => Err(p),
        },
    };
    match page {
        Ok(page) => ok(&page),
        Err(p) => p.response(),
    }
}

/// One page of a listing on the target, by its operation.
pub(crate) async fn list_on(
    state: &ApiState,
    u: &User,
    t: &Target,
    operation: &str,
    q: &ListQuery,
) -> Result<Json, Problem> {
    let arguments = vec![
        Json::from(view_options(q)),
        Json::Long(q.page),
        Json::Long(q.size),
    ];
    // The views answer JSON text.
    let mut page = broker_json_on(state, u, t, operation, arguments).await?;
    page.put("page", q.page);
    page.put("size", q.size);
    Ok(page)
}

/// One page of a listing across every node: each node's first rows, as far
/// as this page reaches, merged, each row with its node, sorted again and
/// paged. A node that cannot answer is named in `errors`.
pub(crate) async fn list_all(state: &ApiState, u: &User, operation: &str, q: &ListQuery) -> Json {
    let broker = state.broker.get();
    let reach = q.page.saturating_mul(q.size).min(MAX_PAGE_SIZE);
    let first = ListQuery {
        page: 1,
        size: reach,
        ..q.clone()
    };
    let targets: Vec<(String, Target)> = peers::members(&broker)
        .into_iter()
        .filter(|m| m.local || m.primary.is_some())
        .map(|m| {
            let t = if m.local {
                Target::Local
            } else {
                Target::Peer(m.node_id.clone())
            };
            (m.node_id, t)
        })
        .collect();
    let mut rows = Vec::new();
    let mut count = 0;
    let mut errors = Vec::new();
    for (node, t) in &targets {
        match list_on(state, u, t, operation, &first).await {
            Ok(page) => {
                count += page.get("count").and_then(Json::as_i64).unwrap_or(0);
                if let Some(Json::Array(data)) = page.get("data") {
                    rows.extend(data.iter().cloned().map(|mut r| {
                        r.put("node", node.as_str());
                        r
                    }));
                }
            }
            Err(p) => errors.push(p.json().with("node", node.as_str())),
        }
    }
    if let Some((field, descending)) = &q.sort {
        rows.sort_by(|a, b| {
            let o = compare(a.get(field), b.get(field));
            if *descending { o.reverse() } else { o }
        });
    }
    let start = ((q.page - 1).saturating_mul(q.size)).min(rows.len() as i64) as usize;
    let data: Vec<Json> = rows.into_iter().skip(start).take(q.size as usize).collect();
    Json::object()
        .with("data", Json::Array(data))
        .with("count", count)
        .with("page", q.page)
        .with("size", q.size)
        .with("errors", Json::Array(errors))
}

/// Two rows' values of a field, as the views compare them: numbers as
/// numbers, though the views write them as text, and the rest as text.
fn compare(a: Option<&Json>, b: Option<&Json>) -> std::cmp::Ordering {
    let number = |v: Option<&Json>| match v? {
        Json::Long(n) => Some(*n as f64),
        Json::Double(d) => Some(*d),
        Json::String(s) => s.parse::<f64>().ok(),
        _ => None,
    };
    if let (Some(x), Some(y)) = (number(a), number(b)) {
        return x.total_cmp(&y);
    }
    let text = |v: Option<&Json>| match v {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    };
    text(a).cmp(&text(b))
}

async fn close(
    state: &ApiState,
    headers: &HeaderMap,
    peer: SocketAddr,
    scope: Scope,
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
    let t = match Target::of(&scope) {
        Ok(t) => t,
        Err(p) => return p.response(),
    };
    match call_on(state, &u, &t, "broker", operation, arguments).await {
        // `closeConnectionWithID` and the others answer whether it was
        // there to close.
        Ok(Json::Bool(false)) => error(StatusCode::NOT_FOUND, "nothing to close by that ID"),
        Ok(_) => crate::secure(StatusCode::NO_CONTENT.into_response()),
        Err(p) => p.response(),
    }
}

async fn close_connection(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    close(
        &state,
        &headers,
        peer,
        scope,
        "closeConnectionWithID(java.lang.String)",
        vec![Json::from(id)],
    )
    .await
}

async fn close_session(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path((connection, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    close(
        &state,
        &headers,
        peer,
        scope,
        "closeSessionWithID(java.lang.String,java.lang.String)",
        vec![Json::from(connection), Json::from(id)],
    )
    .await
}

async fn close_consumer(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path((session, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    close(
        &state,
        &headers,
        peer,
        scope,
        "closeConsumerWithID(java.lang.String,java.lang.String)",
        vec![Json::from(session), Json::from(id)],
    )
    .await
}

/// The kind of a resource, as its name begins.
fn resource_type(bean: &Bean) -> &'static str {
    match bean {
        Bean::Broker => "broker",
        Bean::Address(_) => "address",
        Bean::Queue(_) => "queue",
        Bean::Divert(_) => "divert",
        Bean::Acceptor(..) => "acceptor",
        Bean::BroadcastGroup(_) => "broadcastgroup",
        Bean::Bridge(_) => "bridge",
        Bean::BrokerConnection(_) => "brokerconnection",
        Bean::ClusterConnection(_) => "clusterconnection",
        Bean::HawtioSecurity => "hawtio",
        Bean::Platform(_) => "platform",
    }
}

fn resource_json(name: &ObjectName, bean: &Bean) -> Json {
    Json::object()
        .with("resource", bean.resource())
        .with("type", resource_type(bean))
        .with("objectName", name.canonical())
        .with("className", bean.class_name())
}

/// `MBeanOperationInfo`'s impact.
fn impact(i: u8) -> &'static str {
    match i {
        0 => "INFO",
        1 => "ACTION",
        2 => "ACTION_INFO",
        _ => "UNKNOWN",
    }
}

/// A resource the user may see.
fn visible_resource(
    state: &ApiState,
    user: &User,
    resource: &str,
) -> Result<(ObjectName, Bean), Box<Response>> {
    state
        .agent
        .visible(&user.caller)
        .map_err(|f| Box::new(failure(f)))?
        .into_iter()
        .find(|(_, b)| b.resource() == resource)
        .ok_or_else(|| {
            Box::new(error(
                StatusCode::NOT_FOUND,
                format!("no such resource: {resource}"),
            ))
        })
}

/// One attribute's value, or why it could not be read.
pub(crate) async fn attribute_value(
    state: &ApiState,
    user: &User,
    name: &ObjectName,
    bean: &Bean,
    attr: &str,
) -> Json {
    match state
        .agent
        .read_attribute(&user.caller, name, bean, attr)
        .await
    {
        Ok(v) => Json::object().with("value", v),
        Err(f) => Json::object()
            .with("error", f.message)
            .with("denied", f.status == 403),
    }
}

async fn resources(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    match Target::of(&scope) {
        Ok(Target::Local) => {}
        Ok(Target::Peer(node)) => {
            return match peer_resources(&state, &u, &node).await {
                Ok(data) => ok(&Json::object().with("data", Json::Array(data))),
                Err(p) => p.response(),
            };
        }
        Err(p) => return p.response(),
    }
    match state.agent.visible(&u.caller) {
        Ok(beans) => ok(&Json::object().with(
            "data",
            Json::Array(beans.iter().map(|(n, b)| resource_json(n, b)).collect()),
        )),
        Err(f) => failure(f),
    }
}

/// A resource: its attributes with their values, and its operations, each
/// with whether the guard lets the user call it.
async fn resource(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path(resource): Path<String>,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    match Target::of(&scope) {
        Ok(Target::Local) => {}
        Ok(Target::Peer(node)) => {
            return match peer_resource(&state, &u, &node, &resource).await {
                Ok(j) => ok(&j),
                Err(p) => p.response(),
            };
        }
        Err(p) => return p.response(),
    }
    let (name, bean) = match visible_resource(&state, &u, &resource) {
        Ok(b) => b,
        Err(r) => return *r,
    };
    state.agent.audit_info(&u.caller, &bean);
    let control = bean.control();
    let mut attributes = Vec::with_capacity(control.attrs.len());
    for a in control.attrs {
        let mut j = Json::object()
            .with("name", a.name)
            .with("type", a.type_name)
            .with("description", a.desc)
            .with("readable", a.readable)
            .with(
                "writable",
                a.writable && state.agent.may(&u.caller, &name, &format!("set{}", a.name)),
            );
        if a.readable {
            let v = attribute_value(&state, &u, &name, &bean, a.name).await;
            if let Json::Object(members) = v {
                for (k, v) in members {
                    j.put(&k, v);
                }
            }
        }
        attributes.push(j);
    }
    let operations = control
        .ops
        .iter()
        .map(|o| {
            let params = o
                .params
                .iter()
                .map(|p| {
                    Json::object()
                        .with("name", p.name)
                        .with("type", p.type_name)
                        .with("description", p.desc)
                })
                .collect();
            Json::object()
                .with("name", o.name)
                .with("signature", format!("{}{}", o.name, o.signature()))
                .with("description", o.desc)
                .with("returns", o.ret)
                .with("impact", impact(o.impact))
                .with("params", Json::Array(params))
                .with("allowed", state.agent.may(&u.caller, &name, o.name))
        })
        .collect();
    ok(&resource_json(&name, &bean)
        .with("attributes", Json::Array(attributes))
        .with("operations", Json::Array(operations)))
}

/// Another node's resource: its attributes with their values, and its
/// operations, which that node allows or refuses when called (`allowed` is
/// `null`).
async fn peer_resource(
    state: &ApiState,
    u: &User,
    node: &str,
    resource: &str,
) -> Result<Json, Problem> {
    let bean = bean_named(resource).ok_or_else(|| {
        Problem::new(
            StatusCode::NOT_FOUND,
            format!("no such resource: {resource}"),
        )
    })?;
    let control = bean.control();
    let mut attributes = Vec::with_capacity(control.attrs.len());
    for a in control.attrs {
        let mut j = Json::object()
            .with("name", a.name)
            .with("type", a.type_name)
            .with("description", a.desc)
            .with("readable", a.readable)
            .with("writable", a.writable);
        if a.readable {
            let v = peer_attribute_value(state, u, node, resource, a.name).await?;
            // The broker answers what it does not have as failed: a
            // resource that is not there has no attributes at all.
            if let Some(Json::String(e)) = v.get("error")
                && (e.contains("AMQ229071") || e.contains("not found"))
                && attributes.is_empty()
            {
                return Err(Problem::new(
                    StatusCode::NOT_FOUND,
                    format!("no such resource: {resource}"),
                ));
            }
            if let Json::Object(members) = v {
                for (k, v) in members {
                    j.put(&k, v);
                }
            }
        }
        attributes.push(j);
    }
    let operations = control
        .ops
        .iter()
        .map(|o| {
            let params = o
                .params
                .iter()
                .map(|p| {
                    Json::object()
                        .with("name", p.name)
                        .with("type", p.type_name)
                        .with("description", p.desc)
                })
                .collect();
            Json::object()
                .with("name", o.name)
                .with("signature", format!("{}{}", o.name, o.signature()))
                .with("description", o.desc)
                .with("returns", o.ret)
                .with("impact", impact(o.impact))
                .with("params", Json::Array(params))
                .with("allowed", Json::Null)
        })
        .collect();
    Ok(peer_resource_json(&bean)
        .with("attributes", Json::Array(attributes))
        .with("operations", Json::Array(operations)))
}

/// Some attributes' values: `name=A&name=B`.
async fn attributes(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path(resource): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let t = match Target::of(&scope) {
        Ok(t) => t,
        Err(p) => return p.response(),
    };
    let mut wanted = Vec::new();
    for pair in uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
    {
        match pair.split_once('=') {
            Some(("name", v)) => wanted.push(percent_decode(&v.replace('+', " "))),
            _ => return error(StatusCode::BAD_REQUEST, format!("unknown parameter {pair}")),
        }
    }
    if wanted.len() > MAX_CHECKS {
        return error(
            StatusCode::BAD_REQUEST,
            format!("at most {MAX_CHECKS} attributes at a time"),
        );
    }
    let mut out = Json::object();
    match t {
        Target::Local => {
            let (name, bean) = match visible_resource(&state, &u, &resource) {
                Ok(b) => b,
                Err(r) => return *r,
            };
            for a in &wanted {
                out.put(a, attribute_value(&state, &u, &name, &bean, a).await);
            }
        }
        Target::Peer(node) => {
            for a in &wanted {
                match peer_attribute_value(&state, &u, &node, &resource, a).await {
                    Ok(v) => out.put(a, v),
                    Err(p) => return p.response(),
                }
            }
        }
    }
    ok(&Json::object().with("attributes", out))
}

/// The JSON body of a request that changes something, from a marked request
/// of a logged-in user.
fn change(
    state: &ApiState,
    headers: &HeaderMap,
    peer: SocketAddr,
    body: &Bytes,
) -> Result<(User, Json), Box<Response>> {
    marked(headers)?;
    let u = user(state, headers, peer)?;
    let body = Json::parse(&String::from_utf8_lossy(body))
        .map_err(|_| Box::new(error(StatusCode::BAD_REQUEST, "the body must be JSON")))?;
    Ok((u, body))
}

async fn write(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path((resource, attr)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (u, body) = match change(&state, &headers, peer, &body) {
        Ok(c) => c,
        Err(r) => return *r,
    };
    let Some(value) = body.get("value").cloned() else {
        return error(StatusCode::BAD_REQUEST, "the body needs a value");
    };
    match Target::of(&scope) {
        Ok(Target::Local) => {}
        Ok(Target::Peer(node)) => {
            // A management message cannot write an attribute: its setter
            // is called, as an operation.
            let previous = peer_attribute(&state, &u, &node, &resource, &attr)
                .await
                .unwrap_or(Json::Null);
            let setter = format!("set{attr}");
            return match peer_call(&state, &u, &node, &resource, &setter, vec![value]).await {
                Ok(_) => ok(&Json::object().with("previous", previous)),
                Err(p) => p.response(),
            };
        }
        Err(p) => return p.response(),
    }
    let (name, _) = match visible_resource(&state, &u, &resource) {
        Ok(b) => b,
        Err(r) => return *r,
    };
    match state
        .agent
        .write_attribute(&u.caller, name, &attr, value)
        .await
    {
        Ok(previous) => ok(&Json::object().with("previous", previous)),
        Err(f) => failure(f),
    }
}

async fn invoke(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    Path(resource): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (u, body) = match change(&state, &headers, peer, &body) {
        Ok(c) => c,
        Err(r) => return *r,
    };
    let Some(operation) = body.get("operation").and_then(Json::as_str) else {
        return error(StatusCode::BAD_REQUEST, "the body needs an operation");
    };
    let arguments = match body.get("arguments") {
        None => Vec::new(),
        Some(Json::Array(a)) => a.clone(),
        Some(_) => return error(StatusCode::BAD_REQUEST, "arguments is an array"),
    };
    match Target::of(&scope) {
        Ok(Target::Local) => {}
        Ok(Target::Peer(node)) => {
            return match peer_call(&state, &u, &node, &resource, operation, arguments).await {
                Ok(result) => ok(&Json::object().with("result", result)),
                Err(p) => p.response(),
            };
        }
        Err(p) => return p.response(),
    }
    let (name, _) = match visible_resource(&state, &u, &resource) {
        Ok(b) => b,
        Err(r) => return *r,
    };
    match state
        .agent
        .call(&u.caller, name, operation, arguments)
        .await
    {
        Ok(result) => ok(&Json::object().with("result", result)),
        Err(f) => failure(f),
    }
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
