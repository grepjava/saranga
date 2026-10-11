//! The REST API, at `/api` on every binding: the management resources'
//! attributes and operations, and queue browsing, as plain JSON.
//!
//! It is management messages over HTTP: the same resources, operations,
//! parameter conversion and security. With broker security on, the user
//! `Authorization: Basic` names needs the `manage` permission on the
//! management address and, with `management-message-rbac`, the view or edit
//! permission of each method; with it off, no login is asked for.
//!
//! - `GET  /api/broker/attributes/{attribute}`
//! - `GET  /api/{addresses|queues|diverts}/{name}/attributes/{attribute}`
//! - `POST /api/broker/operations/{operation}`
//! - `POST /api/{addresses|queues|diverts}/{name}/operations/{operation}`
//! - `GET  /api/queues/{name}/messages[?filter=..][&page=..&pageSize=..]`
//!
//! An operation's body is a JSON array of its parameters (empty for none).
//! A value comes back as the JSON it is; a failure as `{"error": ...}` with
//! 401 (no or wrong login), 403 (not permitted), 404 (no such resource,
//! attribute or operation) or 400 (the operation failed).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use saranga_config::CheckType;
use saranga_security::{ConnectionInfo, SecurityAuth};
use saranga_server::Broker;
use saranga_server::audit::Who;
use saranga_server::management::Caller;
use saranga_server::management::json::Json;

use crate::Peer;

/// The API's routes.
pub(crate) fn router(broker: Arc<Broker>) -> Router {
    Router::new()
        .route("/api/{kind}/{a}/{b}", get(get3).post(post3))
        .route("/api/{kind}/{name}/{part}/{member}", get(get4).post(post4))
        .with_state(broker)
}

fn reply(status: StatusCode, body: Json) -> Response {
    super::secure(
        (
            status,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            body.to_string(),
        )
            .into_response(),
    )
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    reply(status, Json::object().with("error", message.into()))
}

fn not_found(what: impl Into<String>) -> Response {
    error(StatusCode::NOT_FOUND, what)
}

/// The management name of `/api/{kind}/{name}`.
fn resource(kind: &str, name: &str) -> Option<String> {
    let prefix = match kind {
        "addresses" => "address",
        "queues" => "queue",
        "diverts" => "divert",
        _ => return None,
    };
    Some(format!("{prefix}.{name}"))
}

/// The session a request runs as: `None` without broker security.
fn login(
    broker: &Broker,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Result<Option<SecurityAuth>, Box<Response>> {
    let Some(store) = broker.security().filter(|s| s.is_enabled()) else {
        return Ok(None);
    };
    let unauthorized = || {
        let mut r = error(StatusCode::UNAUTHORIZED, "a user and password are needed");
        r.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Basic realm=\"saranga\""),
        );
        r
    };
    let (user, password) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Basic ")
                .or_else(|| v.strip_prefix("basic "))
        })
        .and_then(|e| super::base64_decode(e.trim()))
        .and_then(|d| String::from_utf8(d).ok())
        .and_then(|d| d.split_once(':').map(|(u, p)| (u.to_owned(), p.to_owned())))
        .ok_or_else(|| Box::new(unauthorized()))?;
    let connection = Arc::new(ConnectionInfo {
        remote_address: format!("/{peer}"),
        certificates: None,
        security_domain: None,
        connection_id: None,
    });
    let authenticated = store
        .authenticate(Some(&user), Some(&password), &connection)
        .map_err(|_| Box::new(unauthorized()))?;
    let auth = SecurityAuth {
        username: Some(user),
        password: Some(password),
        connection,
        subject: authenticated.subject,
    };
    store
        .check(
            broker.management().address(),
            None,
            CheckType::Manage,
            &auth,
        )
        .map_err(|e| Box::new(error(StatusCode::FORBIDDEN, e.0)))?;
    Ok(Some(auth))
}

/// Who a request is: its session for the permission checks, and for the
/// audit log its subject and address.
fn caller<'a>(broker: &'a Broker, auth: &'a Option<SecurityAuth>, remote: &'a str) -> Caller<'a> {
    Caller {
        security: broker.security().map(|s| &**s).zip(auth.as_ref()),
        who: Some(Who {
            subject: auth.as_ref().and_then(|a| a.subject.as_deref()),
            remote: Some(remote),
        }),
    }
}

/// The caller of a call whose permissions are checked already.
fn checked<'a>(broker: &'a Broker, auth: &'a Option<SecurityAuth>, remote: &'a str) -> Caller<'a> {
    Caller {
        security: None,
        ..caller(broker, auth, remote)
    }
}

async fn read(
    broker: &Arc<Broker>,
    auth: &Option<SecurityAuth>,
    remote: &str,
    resource: &str,
    attribute: &str,
) -> Response {
    let m = broker.management();
    if !m.exists(broker, resource) {
        return not_found(format!("no resource {resource}"));
    }
    let Ok(value) = m
        .attribute(broker, resource, attribute, checked(broker, auth, remote))
        .await
    else {
        return not_found(format!("no attribute {attribute} on {resource}"));
    };
    let mut chars = attribute.chars();
    let upper: String = chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default();
    let getter = if matches!(value, Json::Bool(_)) {
        "is"
    } else {
        "get"
    };
    if let Err(e) = m.permits(
        caller(broker, auth, remote),
        resource,
        &format!("{getter}{upper}"),
    ) {
        return error(StatusCode::FORBIDDEN, e);
    }
    reply(StatusCode::OK, value)
}

async fn call(
    broker: &Arc<Broker>,
    auth: &Option<SecurityAuth>,
    remote: &str,
    resource: &str,
    operation: &str,
    body: &[u8],
) -> Response {
    let text = String::from_utf8_lossy(body);
    let params = if text.trim().is_empty() {
        Vec::new()
    } else {
        match Json::parse(&text) {
            Ok(Json::Array(params)) => params,
            _ => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "the body must be a JSON array of the operation's parameters",
                );
            }
        }
    };
    let m = broker.management();
    if !m.exists(broker, resource) {
        return not_found(format!("no resource {resource}"));
    }
    if let Err(e) = m.permits(caller(broker, auth, remote), resource, operation) {
        return error(StatusCode::FORBIDDEN, e);
    }
    match m
        .invoke(
            broker,
            resource,
            operation,
            &params,
            checked(broker, auth, remote),
        )
        .await
    {
        Ok(value) => reply(StatusCode::OK, value),
        Err(e) if e.starts_with("AMQ229069") => not_found(e),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

async fn browse(
    broker: &Arc<Broker>,
    auth: &Option<SecurityAuth>,
    remote: &str,
    queue: &str,
    query: &HashMap<String, String>,
) -> Response {
    let resource = format!("queue.{queue}");
    let m = broker.management();
    if !m.exists(broker, &resource) {
        return not_found(format!("no resource {resource}"));
    }
    let number = |k: &str| {
        query
            .get(k)
            .map(|v| v.parse::<i32>().map_err(|_| k.to_owned()))
    };
    let page = match (number("page"), number("pageSize")) {
        (None, None) => None,
        (Some(Ok(p)), Some(Ok(s))) if p >= 1 && s >= 1 => Some((p, s)),
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "page and pageSize go together, as numbers from 1",
            );
        }
    };
    if let Err(e) = m.permits(caller(broker, auth, remote), &resource, "browse") {
        return error(StatusCode::FORBIDDEN, e);
    }
    match m
        .browse(
            broker,
            queue,
            page,
            query.get("filter").cloned(),
            checked(broker, auth, remote),
        )
        .await
    {
        Ok(value) => reply(StatusCode::OK, value),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

async fn get3(
    State(broker): State<Arc<Broker>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path((kind, a, b)): Path<(String, String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let auth = match login(&broker, &headers, peer) {
        Ok(a) => a,
        Err(r) => return *r,
    };
    let remote = peer.to_string();
    match (kind.as_str(), a.as_str(), b.as_str()) {
        ("broker", "attributes", attribute) => {
            read(&broker, &auth, &remote, "broker", attribute).await
        }
        ("queues", queue, "messages") => browse(&broker, &auth, &remote, queue, &query).await,
        _ => not_found("no such API"),
    }
}

async fn post3(
    State(broker): State<Arc<Broker>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path((kind, a, b)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let auth = match login(&broker, &headers, peer) {
        Ok(a) => a,
        Err(r) => return *r,
    };
    let remote = peer.to_string();
    match (kind.as_str(), a.as_str()) {
        ("broker", "operations") => call(&broker, &auth, &remote, "broker", &b, &body).await,
        _ => not_found("no such API"),
    }
}

async fn get4(
    State(broker): State<Arc<Broker>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path((kind, name, part, member)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth = match login(&broker, &headers, peer) {
        Ok(a) => a,
        Err(r) => return *r,
    };
    let remote = peer.to_string();
    match (resource(&kind, &name), part.as_str()) {
        (Some(r), "attributes") => read(&broker, &auth, &remote, &r, &member).await,
        _ => not_found("no such API"),
    }
}

async fn post4(
    State(broker): State<Arc<Broker>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path((kind, name, part, member)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let auth = match login(&broker, &headers, peer) {
        Ok(a) => a,
        Err(r) => return *r,
    };
    let remote = peer.to_string();
    match (resource(&kind, &name), part.as_str()) {
        (Some(r), "operations") => call(&broker, &auth, &remote, &r, &member, &body).await,
        _ => not_found("no such API"),
    }
}
