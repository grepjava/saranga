//! What the console's administration pages read that no management
//! operation gives: every address-settings and security-settings match with
//! its values, which match supplies each of an address's settings, and the
//! configuration `exportConfigAsProperties` wrote.
//!
//! These are this broker's alone, read from its repositories as the
//! operations that change them (`addAddressSettings`, `addSecuritySettings`)
//! write them; another node answers its own operations, so for one these
//! answer 400 and the console asks it `getAddressSettingsAsJSON` and
//! `getRolesAsJSON`. Each is allowed to whoever the guard lets call the
//! operation that reads the same thing.

use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use saranga_server::management::json::Json;
use saranga_server::management::{role_json, settings_json};

use crate::api::{ApiState, Problem, Scoped, Target, User, call_on, error, marked, ok, user};
use crate::peers::Request as PeerRequest;
use crate::{Peer, percent_decode};

/// The operation whose permission reads the address settings.
const ADDRESS_SETTINGS: &str = "getAddressSettingsAsJSON";
/// The operation whose permission reads the security settings.
const ROLES: &str = "getRolesAsJSON";
/// The operation that writes the exported configuration.
const EXPORT: &str = "exportConfigAsProperties";

/// The user, for a request this broker answers itself, allowed `operation`
/// on the broker.
fn local_user(
    state: &ApiState,
    headers: &HeaderMap,
    peer: std::net::SocketAddr,
    scope: &crate::peers::Scope,
    operation: &str,
) -> Result<User, Box<Response>> {
    let u = user(state, headers, peer)?;
    match Target::of(scope) {
        Ok(Target::Local) => {}
        Ok(Target::Peer(_)) => {
            return Err(Box::new(error(
                StatusCode::BAD_REQUEST,
                "this is listed for the broker you are logged in to; ask another node its own operations",
            )));
        }
        Err(p) => return Err(Box::new(p.response())),
    }
    let Some((name, _)) = state.agent.bean_of("broker") else {
        return Err(Box::new(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the broker is not running",
        )));
    };
    if !state.agent.may(&u.caller, &name, operation) {
        return Err(Box::new(error(
            StatusCode::FORBIDDEN,
            format!("{} may not call {operation}", u.name),
        )));
    }
    Ok(u)
}

/// The `address` of a query.
fn address_of(uri: &Uri) -> Option<String> {
    uri.query()?
        .split('&')
        .find_map(|kv| kv.strip_prefix("address="))
        .map(percent_decode)
}

/// `GET /address-settings`: every match with its settings as configured
/// (`toJSON`'s names, nothing defaulted) and whether management added it,
/// and every setting's name and type.
/// With `?address=`, also the matches that apply to that address, most
/// specific first, the settings they merge into (what
/// `getAddressSettingsAsJSON` answers) and the match each value comes from.
pub(crate) async fn address_settings(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = local_user(&state, &headers, peer, &scope, ADDRESS_SETTINGS) {
        return *r;
    }
    let broker = state.broker.get();
    let po = broker.post_office();
    let matches: Vec<Json> = po
        .settings_entries()
        .into_iter()
        .map(|(m, s, added)| {
            Json::object()
                .with("match", m)
                .with("settings", settings_json(Some(s)))
                .with("added", added)
        })
        .collect();
    let names: Vec<Json> = saranga_config::AddressSettings::kinds()
        .into_iter()
        .map(|(name, kind, choices)| {
            let mut o = Json::object().with("name", name).with("type", kind);
            if !choices.is_empty() {
                o.put(
                    "choices",
                    Json::Array(choices.iter().map(|c| Json::from(*c)).collect()),
                );
            }
            o
        })
        .collect();
    let mut out = Json::object()
        .with("matches", Json::Array(matches))
        .with("names", Json::Array(names));
    if let Some(address) = address_of(&uri) {
        let applying = po.settings_matching(&address);
        let mut from = Json::object();
        for (m, s) in &applying {
            for (name, _) in s.named_values() {
                if from.get(&name).is_none() {
                    from.put(&name, m.as_str());
                }
            }
        }
        out.put("address", address.as_str());
        out.put(
            "applying",
            Json::Array(
                applying
                    .iter()
                    .map(|(m, _)| Json::from(m.as_str()))
                    .collect(),
            ),
        );
        out.put("effective", settings_json(po.settings_configured(&address)));
        out.put("from", from);
    }
    ok(&out)
}

/// `GET /security-settings`: every match with its roles, as
/// `getRolesAsJSON` writes them; with `?address=`, also the match whose
/// roles apply to it.
pub(crate) async fn security_settings(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = local_user(&state, &headers, peer, &scope, ROLES) {
        return *r;
    }
    let broker = state.broker.get();
    let Some(store) = broker.security_store() else {
        return ok(&Json::object()
            .with("enabled", false)
            .with("matches", Json::Array(Vec::new())));
    };
    let matches: Vec<Json> = store
        .entries()
        .into_iter()
        .map(|(m, roles)| {
            Json::object()
                .with("match", m)
                .with("roles", Json::Array(roles.iter().map(role_json).collect()))
        })
        .collect();
    let mut out = Json::object()
        .with("enabled", store.is_enabled())
        .with("matches", Json::Array(matches));
    if let Some(address) = address_of(&uri) {
        out.put("address", address.as_str());
        out.put(
            "applying",
            store.matching(&address).map_or(Json::Null, Json::from),
        );
    }
    ok(&out)
}

/// `POST /configuration/export`: `exportConfigAsProperties`, as the user
/// through the guard and the audit, then the file it wrote. Another node
/// writes its own file, which only it can read: the answer then has no
/// text.
pub(crate) async fn export(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = marked(&headers) {
        return *r;
    }
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let t = match Target::of(&scope) {
        Ok(t) => t,
        Err(p) => return p.response(),
    };
    if let Err(p) = call_on(&state, &u, &t, "broker", EXPORT, Vec::new()).await {
        return p.response();
    }
    if let Target::Peer(_) = t {
        return ok(&Json::object().with("text", Json::Null));
    }
    match exported(&state) {
        Ok((file, text)) => ok(&Json::object().with("file", file).with("text", text)),
        Err(p) => p.response(),
    }
}

/// The file the last export wrote, and its text.
fn exported(state: &ApiState) -> Result<(String, String), Problem> {
    let file = state.broker.get().exported_config().ok_or_else(|| {
        Problem::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "no configuration was exported",
        )
    })?;
    let text = std::fs::read_to_string(&file)
        .map_err(|e| Problem::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((file.display().to_string(), text))
}

/// The most messages one export gives.
const MAX_EXPORT: usize = 10_000;
/// The most messages one import sends.
const MAX_IMPORT: usize = 10_000;

/// The query's `filter` and `max`.
fn export_query(uri: &Uri) -> (Option<String>, usize) {
    let mut filter = None;
    let mut max = MAX_EXPORT;
    for kv in uri.query().unwrap_or("").split('&') {
        if let Some(f) = kv.strip_prefix("filter=") {
            filter = Some(percent_decode(f)).filter(|f| !f.trim().is_empty());
        } else if let Some(m) = kv.strip_prefix("max=") {
            max = m
                .parse::<usize>()
                .unwrap_or(MAX_EXPORT)
                .clamp(1, MAX_EXPORT);
        }
    }
    (filter, max)
}

/// The user, allowed `operation` on a queue of this broker, and the queue's
/// address.
fn on_local_queue(
    state: &ApiState,
    u: &User,
    queue: &str,
    operation: &str,
) -> Result<String, Box<Response>> {
    let resource = format!("queue.{queue}");
    let Some((name, _)) = state.agent.bean_of(&resource) else {
        return Err(Box::new(error(
            StatusCode::NOT_FOUND,
            format!("no such resource: {resource}"),
        )));
    };
    if !state.agent.may(&u.caller, &name, operation) {
        return Err(Box::new(error(
            StatusCode::FORBIDDEN,
            format!("{} may not call {operation} on {resource}", u.name),
        )));
    }
    let broker = state.broker.get();
    let q = broker.post_office().queue(queue).ok_or_else(|| {
        Box::new(error(
            StatusCode::NOT_FOUND,
            format!("no such resource: {resource}"),
        ))
    })?;
    Ok(q.config().address.to_string())
}

/// `GET /queues/{name}/export?filter=&max=`: up to `max` (10,000 at most)
/// of the messages `filter` selects, whole, as
/// `saranga_server::management::export` writes them. On this broker it is
/// allowed to whoever may `browse` the queue; another node's are browsed on
/// the user's link, which that node's `browse` permission allows.
pub(crate) async fn export_messages(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    axum::extract::Path(queue): axum::extract::Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let (filter, max) = export_query(&uri);
    match Target::of(&scope) {
        Err(p) => p.response(),
        Ok(Target::Local) => {
            if let Err(r) = on_local_queue(&state, &u, &queue, "browse") {
                return *r;
            }
            let broker = state.broker.get();
            match saranga_server::management::export_messages(&broker, &queue, filter, max).await {
                Ok(messages) => ok(&Json::Array(messages)),
                Err(e) => error(StatusCode::BAD_REQUEST, e),
            }
        }
        Ok(Target::Peer(node)) => {
            let request = PeerRequest::Export {
                queue: &queue,
                filter: filter.as_deref(),
                max,
            };
            match state
                .peers
                .request(&state.broker.get(), &node, &u.credentials(), request)
                .await
            {
                Ok(messages) => ok(&messages),
                Err(p) => p.response(),
            }
        }
    }
}

/// `POST /queues/{name}/import`: the messages of the body (an array, as the
/// export writes them, 10,000 at most) sent to the queue, in order, as the
/// user, so the address's `send` permission applies; on this broker the
/// guard must also let the user call the queue's `sendMessage`. Answers how
/// many were sent, and why the next one was not.
pub(crate) async fn import_messages(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Scoped(scope): Scoped,
    axum::extract::Path(queue): axum::extract::Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Err(r) = marked(&headers) {
        return *r;
    }
    let u = match user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let Ok(Json::Array(items)) = Json::parse(&String::from_utf8_lossy(&body)) else {
        return error(
            StatusCode::BAD_REQUEST,
            "the body must be a JSON array of messages",
        );
    };
    if items.len() > MAX_IMPORT {
        return error(
            StatusCode::BAD_REQUEST,
            format!("at most {MAX_IMPORT} messages at a time"),
        );
    }
    let mut messages = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        match saranga_server::management::export::message_from_json(item) {
            Ok(m) => messages.push(m),
            Err(e) => return error(StatusCode::BAD_REQUEST, format!("message {}: {e}", i + 1)),
        }
    }
    let t = match Target::of(&scope) {
        Ok(t) => t,
        Err(p) => return p.response(),
    };
    let address = match &t {
        Target::Local => match on_local_queue(&state, &u, &queue, "sendMessage") {
            Ok(a) => a,
            Err(r) => return *r,
        },
        Target::Peer(node) => {
            match crate::api::peer_attribute(&state, &u, node, &format!("queue.{queue}"), "Address")
                .await
            {
                Ok(Json::String(a)) => a,
                Ok(_) => return error(StatusCode::BAD_GATEWAY, "the queue has no address"),
                Err(p) => return p.response(),
            }
        }
    };
    // Only this queue of the address gets them.
    let fqqn = format!("{address}::{queue}");
    match t {
        Target::Local => {
            let broker = state.broker.get();
            let (sent, failed) = saranga_server::management::as_subject(
                u.caller.subject.clone(),
                saranga_server::management::import_messages(&broker, &fqqn, messages),
            )
            .await;
            match failed {
                Some(e) if sent == 0 => error(StatusCode::BAD_REQUEST, e),
                Some(e) => ok(&Json::object().with("sent", sent as i64).with("error", e)),
                None => ok(&Json::object().with("sent", sent as i64)),
            }
        }
        Target::Peer(node) => {
            let request = PeerRequest::Import {
                address: &fqqn,
                messages,
            };
            match state
                .peers
                .request(&state.broker.get(), &node, &u.credentials(), request)
                .await
            {
                Ok(r) => ok(&r),
                Err(p) => p.response(),
            }
        }
    }
}
