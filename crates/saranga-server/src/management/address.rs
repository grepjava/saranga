//! `AddressControlImpl`: an address's attributes and operations.

use std::sync::Arc;

use super::Outcome;
use super::args::{Call, Ty::*};
use super::json::Json;
use crate::Broker;
use crate::RoutingType;
use crate::postoffice::RoutingTypes;
use crate::queue::{AckReason, Selection, SelectionAction};

/// The routing types in `EnumSet` order: MULTICAST, then ANYCAST.
pub(super) fn routing_type_names(t: RoutingTypes) -> Vec<&'static str> {
    [RoutingType::Multicast, RoutingType::Anycast]
        .into_iter()
        .filter(|r| t.contains(*r))
        .map(RoutingType::name)
        .collect()
}

pub(super) async fn attribute(
    broker: &Arc<Broker>,
    name: &str,
    attribute: &str,
) -> Result<Option<Json>, String> {
    let po = broker.post_office();
    let Some(st) = po.address_status(name) else {
        return Err(format!("AMQ229203: Address Does Not Exist: {name}"));
    };
    let store = po.paging().existing_store(name);
    let local_queues =
        || -> Vec<String> { st.queues.iter().map(|q| q.name().to_string()).collect() };
    Ok(Some(match attribute {
        "Address" => Json::from(&*st.info.name),
        "Id" => Json::from(st.info.id),
        "RoutingTypes" => Json::from(routing_type_names(st.info.routing_types)),
        "RoutingTypesAsJSON" => {
            Json::from(Json::from(routing_type_names(st.info.routing_types)).to_string())
        }
        "QueueNames" | "AllQueueNames" => Json::from(local_queues()),
        "RemoteQueueNames" => Json::Array(Vec::new()),
        "BindingNames" => Json::from(
            st.queues
                .iter()
                .map(|q| q.name().to_string())
                .chain(st.diverts.iter().map(|d| d.name.to_string()))
                .collect::<Vec<_>>(),
        ),
        "QueueCount" => Json::from(st.queues.len() as i64),
        "MessageCount" | "NumberOfMessages" => {
            Json::from(st.queues.iter().map(|q| q.message_count()).sum::<i64>())
        }
        "RoutedMessageCount" => Json::from(st.routed),
        "UnRoutedMessageCount" => Json::from(st.unrouted),
        "NumberOfBytesPerPage" => Json::from(store.as_ref().map_or(0, |s| s.page_size_bytes())),
        "AddressSize" => Json::from(store.as_ref().map_or(0, |s| s.address_size())),
        "NumberOfPages" => Json::from(
            store
                .as_ref()
                .filter(|s| s.is_paging())
                .map_or(0, |s| s.number_of_pages()),
        ),
        "Paging" => Json::from(store.as_ref().is_some_and(|s| s.is_paging())),
        "AddressLimitPercent" => {
            Json::from(store.as_ref().map_or(0, |s| s.address_limit_percent()))
        }
        "MaxPageReadBytes" => Json::from(
            store
                .as_ref()
                .map_or(0, |s| s.address_settings().max_read_page_bytes as i32),
        ),
        "MaxPageReadMessages" => Json::from(
            store
                .as_ref()
                .map_or(0, |s| s.address_settings().max_read_page_messages as i32),
        ),
        "PrefetchPageBytes" => Json::from(
            store
                .as_ref()
                .map_or(0, |s| s.address_settings().prefetch_page_bytes as i32),
        ),
        "PrefetchPageMessages" => Json::from(
            store
                .as_ref()
                .map_or(0, |s| s.address_settings().prefetch_page_messages as i32),
        ),
        "BlockedViaManagement" => Json::from(
            store
                .as_ref()
                .is_some_and(|s| s.is_blocked_via_management()),
        ),
        "Paused" => Json::from(st.paused),
        "Temporary" => Json::from(st.info.temporary),
        "AutoCreated" => Json::from(st.info.auto_created),
        "Internal" => Json::from(st.info.internal),
        "RetroactiveResource" => Json::from(
            st.info
                .name
                .starts_with(&*super::queue::internal_prefix(broker)),
        ),
        "CurrentDuplicateIdCacheSize" => Json::from(if po.duplicate_cache_exists(name) {
            crate::duplicate::lock(&po.duplicate_cache(name)).len() as i64
        } else {
            0
        }),
        "Roles" => Json::Array(Vec::new()),
        "RolesAsJSON" => Json::from("[]"),
        _ => return Ok(None),
    }))
}

pub(super) async fn invoke(broker: &Arc<Broker>, name: &str, call: &Call<'_>) -> Option<Outcome> {
    let po = broker.post_office();
    if call.is("pause", &[]).is_some() || call.is("pause", &[B]).is_some() {
        let persist = call.is("pause", &[B]).is_some_and(|a| a.b(0));
        return Some(
            po.pause_address(name, persist)
                .map(|_| Json::Null)
                .map_err(|e| e.message),
        );
    }
    if call.is("resume", &[]).is_some() {
        return Some(
            po.resume_address(name)
                .map(|_| Json::Null)
                .map_err(|e| e.message),
        );
    }
    if call.is("block", &[]).is_some() {
        return Some(Ok(Json::from(match po.paging().existing_store(name) {
            Some(s) => {
                s.block();
                true
            }
            None => false,
        })));
    }
    if call.is("unblock", &[]).is_some() {
        if let Some(s) = po.paging().existing_store(name) {
            s.unblock();
        }
        return Some(Ok(Json::Null));
    }
    if call.is("clearDuplicateIdCache", &[]).is_some() {
        if !po.duplicate_cache_exists(name) {
            return Some(Ok(Json::from(false)));
        }
        let ids = po.ids().clone();
        crate::duplicate::lock(&po.duplicate_cache(name)).clear(|| ids.next());
        return Some(Ok(Json::from(true)));
    }
    if call.is("purge", &[]).is_some() {
        let mut total = 0i64;
        for q in po
            .address_status(name)
            .map(|s| s.queues)
            .unwrap_or_default()
        {
            total += q
                .act(
                    Selection::default(),
                    SelectionAction::Remove(AckReason::Killed),
                )
                .await as i64;
        }
        return Some(Ok(Json::from(total)));
    }
    if call.is("schedulePageCleanup", &[]).is_some() {
        if let Some(s) = po.paging().existing_store(name) {
            s.cleanup();
        }
        return Some(Ok(Json::Null));
    }
    for (types, create_id) in [
        (&[M, I, S, B, S, S][..], false),
        (&[M, I, S, B, S, S, B][..], true),
    ] {
        if let Some(a) = call.is("sendMessage", types) {
            let create = create_id && a.b(6);
            let headers = a.string_map(0).unwrap_or_default();
            return Some(
                super::messages::send(
                    broker,
                    name,
                    &headers,
                    a.i(1),
                    a.s(2),
                    a.b(3),
                    a.s(4),
                    a.s(5),
                    create,
                )
                .await
                .map(Json::from),
            );
        }
    }
    None
}
