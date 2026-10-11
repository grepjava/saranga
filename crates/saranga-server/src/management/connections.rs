//! `ActiveMQServerControl`'s connections, sessions, consumers and
//! producers: counting, listing and closing them.

use std::sync::Arc;

use super::Outcome;
use super::args::{Call, Ty::*};
use super::json::Json;
use super::views;
use crate::Broker;

/// The broker attributes these give.
pub(super) fn attribute(broker: &Broker, name: &str) -> Option<Json> {
    let r = broker.registry();
    Some(match name {
        "ConnectionCount" => Json::from(r.connections().len() as i32),
        "TotalConnectionCount" => Json::from(r.total_connections()),
        "SessionCount" => Json::from(r.sessions().len() as i32),
        "TotalSessionCount" => Json::from(r.total_sessions()),
        _ => return None,
    })
}

/// `buildSessionJSON`.
fn session_json(broker: &Broker, s: &crate::registry::SessionEntry) -> Json {
    let mut o = Json::object()
        .with("sessionID", s.name.clone())
        .with("creationTime", s.created)
        .with("consumerCount", s.consumers().len() as i32);
    if let Some(u) = &s.validated_user {
        o.put("validatedUser", u.clone());
        o.put("principal", u.clone());
    }
    if let Some(m) = broker.session_metadata_all(&s.name) {
        o.put(
            "metadata",
            Json::Object(m.into_iter().map(|(k, v)| (k, Json::from(v))).collect()),
        );
    }
    o
}

pub(super) async fn invoke(broker: &Arc<Broker>, call: &Call<'_>) -> Option<Outcome> {
    let r = broker.registry();
    if call.is("listConnectionIDs", &[]).is_some() {
        let ids: Vec<String> = r.connections().iter().map(|c| c.id().to_owned()).collect();
        return Some(Ok(Json::from(ids)));
    }
    if call.is("listRemoteAddresses", &[]).is_some() {
        let all: Vec<String> = r
            .connections()
            .iter()
            .map(|c| c.remote_address().to_owned())
            .collect();
        return Some(Ok(Json::from(all)));
    }
    if let Some(a) = call.is("listRemoteAddresses", &[S]) {
        let ip = a.s_or_empty(0);
        let all: Vec<String> = r
            .connections()
            .iter()
            .map(|c| c.remote_address().to_owned())
            .filter(|addr| addr.contains(&ip))
            .collect();
        return Some(Ok(Json::from(all)));
    }
    if let Some(a) = call.is("listSessions", &[S]) {
        let names: Vec<String> = r
            .connection(&a.s_or_empty(0))
            .map(|c| c.sessions().iter().map(|s| s.name.clone()).collect())
            .unwrap_or_default();
        return Some(Ok(Json::from(names)));
    }
    if call.is("listConnectionsAsJSON", &[]).is_some() {
        let all = r
            .connections()
            .iter()
            .map(|c| {
                Json::object()
                    .with("connectionID", c.id())
                    .with("clientAddress", c.remote_address())
                    .with("creationTime", c.created())
                    .with("implementation", c.implementation())
                    .with("sessionCount", c.sessions().len() as i32)
            })
            .collect();
        return Some(Ok(Json::from(Json::Array(all).to_string())));
    }
    if let Some(a) = call.is("listSessionsAsJSON", &[S]) {
        let all = r
            .connection(&a.s_or_empty(0))
            .map(|c| c.sessions())
            .unwrap_or_default()
            .iter()
            .map(|s| session_json(broker, s))
            .collect();
        return Some(Ok(Json::from(Json::Array(all).to_string())));
    }
    if call.is("listAllSessionsAsJSON", &[]).is_some() {
        let all = r
            .sessions()
            .iter()
            .map(|s| session_json(broker, s))
            .collect();
        return Some(Ok(Json::from(Json::Array(all).to_string())));
    }
    if let Some(a) = call.is("listConsumersAsJSON", &[S]) {
        let id = a.s_or_empty(0);
        let mine: Vec<_> = views::all_consumers(broker)
            .into_iter()
            .filter(|(c, _, _)| c.id() == id)
            .collect();
        return Some(Ok(Json::from(
            views::consumers_json(&mine).await.to_string(),
        )));
    }
    if call.is("listAllConsumersAsJSON", &[]).is_some() {
        let all = views::all_consumers(broker);
        return Some(Ok(Json::from(
            views::consumers_json(&all).await.to_string(),
        )));
    }
    if call.is("listProducersInfoAsJSON", &[]).is_some() {
        return Some(Ok(Json::from(views::producers_info(broker).to_string())));
    }
    if let Some(a) = call.is("listConnections", &[S, I, I]) {
        return Some(views::list_connections(broker, a.s(0).as_deref(), a.i(1), a.i(2)).await);
    }
    if let Some(a) = call.is("listSessions", &[S, I, I]) {
        return Some(views::list_sessions(broker, a.s(0).as_deref(), a.i(1), a.i(2)).await);
    }
    if let Some(a) = call.is("listConsumers", &[S, I, I]) {
        return Some(views::list_consumers(broker, a.s(0).as_deref(), a.i(1), a.i(2)).await);
    }
    if let Some(a) = call.is("listProducers", &[S, I, I]) {
        return Some(views::list_producers(broker, a.s(0).as_deref(), a.i(1), a.i(2)).await);
    }

    // Closing.
    if let Some(a) = call.is("closeConnectionsForAddress", &[S]) {
        let ip = a.s_or_empty(0);
        let mut closed = false;
        for c in r.connections() {
            if c.remote_address().contains(&ip) {
                c.fail();
                closed = true;
            }
        }
        return Some(Ok(Json::from(closed)));
    }
    if let Some(a) = call.is("closeConsumerConnectionsForAddress", &[S]) {
        // The connections of the consumers of every queue whose address
        // matches (`getMatchingBindings`).
        let address = a.s_or_empty(0);
        let address = crate::postoffice::split_fqqn(&address).0.to_owned();
        let wildcard = *broker.post_office().wildcard();
        let matching = |queue_address: &str| wildcard.matches(queue_address, &address);
        let mut closed = false;
        for (conn, _, consumer) in views::all_consumers(broker) {
            if matching(&consumer.queue.config().address) && !conn.is_failed() {
                conn.fail();
                closed = true;
            }
        }
        return Some(Ok(Json::from(closed)));
    }
    if let Some(a) = call.is("closeConnectionsForUser", &[S]) {
        let user = a.s_or_empty(0);
        let mut closed = false;
        for c in r.connections() {
            if c.sessions()
                .iter()
                .any(|s| s.username.as_deref() == Some(user.as_str()))
            {
                c.fail();
                closed = true;
            }
        }
        return Some(Ok(Json::from(closed)));
    }
    if let Some(a) = call.is("closeConnectionWithID", &[S]) {
        return Some(Ok(Json::from(match r.connection(&a.s_or_empty(0)) {
            Some(c) => {
                c.fail();
                true
            }
            None => false,
        })));
    }
    for force in [false, true] {
        let types: &[_] = if force { &[S, S, B] } else { &[S, S] };
        if let Some(a) = call.is("closeSessionWithID", types) {
            let session = r
                .connection(&a.s_or_empty(0))
                .and_then(|c| c.sessions().into_iter().find(|s| s.name == a.s_or_empty(1)));
            return Some(Ok(Json::from(match session {
                Some(s) => {
                    for c in s.consumers() {
                        c.disconnect();
                    }
                    s.close(force && a.b(2))
                }
                None => false,
            })));
        }
    }
    if let Some(a) = call.is("closeConsumerWithID", &[S, S]) {
        let id = a.s_or_empty(1);
        let Ok(id) = id.parse::<i64>() else {
            return Some(Err(format!("For input string: \"{id}\"")));
        };
        let consumer = r.session(&a.s_or_empty(0)).and_then(|s| {
            s.consumers()
                .into_iter()
                .find(|c| c.sequential_id as i64 == id)
        });
        return Some(Ok(Json::from(match consumer {
            Some(c) => {
                c.disconnect();
                true
            }
            None => false,
        })));
    }
    None
}
