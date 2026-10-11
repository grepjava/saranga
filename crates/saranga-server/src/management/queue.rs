//! `QueueControlImpl`: a queue's attributes and operations.

use std::sync::Arc;

use super::Outcome;
use super::args::{Call, Ty::*};
use super::json::Json;
use super::messages;
use crate::Broker;
use crate::queue::{AckReason, Queue, Selection, SelectionAction};

/// `QueueControlImpl.FLUSH_LIMIT`.
const FLUSH_LIMIT: i32 = 500;

pub(super) async fn attribute(
    broker: &Arc<Broker>,
    q: &Queue,
    name: &str,
) -> Result<Option<Json>, String> {
    let c = q.config();
    let settings = q.settings();
    Ok(Some(match name {
        "Name" => Json::from(&*c.name),
        "Address" => Json::from(&*c.address),
        "Filter" => Json::from(q.filter_string()),
        "Durable" => Json::from(c.durable),
        "User" => Json::from(c.user.clone()),
        "RoutingType" => Json::from(c.routing_type.name()),
        "Temporary" => Json::from(c.temporary),
        "RetroactiveResource" => Json::from(c.name.starts_with(&*internal_prefix(broker))),
        "MessageCount" => Json::from(q.message_count()),
        "PersistentSize" => {
            let s = q.stats().await;
            let paged = q.page_subscription().map_or(0, |s| s.persistent_size());
            Json::from(s.pending_size + s.scheduled_size + s.delivering_size + paged)
        }
        "DurableMessageCount" => {
            if !q.is_durable() {
                Json::from(0i64)
            } else {
                let s = q.stats().await;
                let paged = q.page_subscription().map_or(0, |s| s.message_count());
                Json::from(
                    s.durable_pending_count
                        + s.durable_scheduled_count
                        + i64::from(s.durable_delivering_count)
                        + paged,
                )
            }
        }
        "DurablePersistentSize" => {
            if !q.is_durable() {
                Json::from(0i64)
            } else {
                let s = q.stats().await;
                let paged = q.page_subscription().map_or(0, |s| s.persistent_size());
                Json::from(
                    s.durable_pending_size
                        + s.durable_scheduled_size
                        + s.durable_delivering_size
                        + paged,
                )
            }
        }
        "ConsumerCount" => Json::from(q.consumer_count()),
        "DeliveringCount" => Json::from(q.stats().await.delivering_count),
        "DeliveringSize" => Json::from(q.stats().await.delivering_size),
        "DurableDeliveringCount" => Json::from(q.stats().await.durable_delivering_count),
        "DurableDeliveringSize" => Json::from(q.stats().await.durable_delivering_size),
        "MessagesAdded" => Json::from(q.messages_added()),
        "MessagesAcknowledged" => Json::from(q.messages_acknowledged()),
        "AcknowledgeAttempts" => Json::from(q.messages_acknowledged()),
        "MessagesExpired" => Json::from(q.messages_expired()),
        "MessagesKilled" => Json::from(q.messages_killed()),
        "ID" => Json::from(q.id()),
        "ScheduledCount" => Json::from(q.stats().await.scheduled_count),
        "ScheduledSize" => Json::from(q.stats().await.scheduled_size),
        "DurableScheduledCount" => Json::from(q.stats().await.durable_scheduled_count),
        "DurableScheduledSize" => Json::from(q.stats().await.durable_scheduled_size),
        "DeadLetterAddress" => Json::from(settings.dead_letter_address.clone()),
        "ExpiryAddress" => Json::from(settings.expiry_address.clone()),
        "MaxConsumers" => Json::from(c.max_consumers),
        "PurgeOnNoConsumers" => Json::from(c.purge_on_no_consumers),
        "Enabled" => Json::from(c.enabled),
        "ConfigurationManaged" => Json::from(c.configuration_managed),
        "Exclusive" => Json::from(c.exclusive),
        "LastValue" => Json::from(c.last_value || c.last_value_key.is_some()),
        "LastValueKey" => Json::from(c.last_value_key.clone()),
        "ConsumersBeforeDispatch" => Json::from(c.consumers_before_dispatch),
        "DelayBeforeDispatch" => Json::from(c.delay_before_dispatch),
        "InternalQueue" => Json::from(c.internal),
        "Paused" => Json::from(q.is_paused()),
        "PersistedPause" => Json::from(q.is_persisted_pause()),
        "GroupCount" => Json::from(q.stats().await.group_count),
        "GroupRebalance" => Json::from(c.group_rebalance),
        "GroupRebalancePauseDispatch" => Json::from(c.group_rebalance_pause_dispatch),
        "GroupBuckets" => Json::from(c.group_buckets),
        "GroupFirstKey" => Json::from(c.group_first_key.clone()),
        "RingSize" => Json::from(c.ring_size),
        "AutoDelete" => Json::from(c.auto_delete),
        "PreparedTransactionMessageCount" => {
            Json::from(broker.resource_manager().prepared_acks_on(q.id()) as i32)
        }
        "FirstMessageAsJSON" => {
            let limit = settings.management_message_attribute_size_limit;
            let first = q.peek_first().await;
            let m = first.map_or(Json::Null, |r| messages::to_map(&r, limit));
            Json::from(Json::Array(vec![m]).to_string())
        }
        "FirstMessageTimestamp" => Json::from(first_timestamp(q).await),
        "FirstMessageAge" => Json::from(
            first_timestamp(q)
                .await
                .map(|t| crate::message::now_millis() - t),
        ),
        _ => return Ok(None),
    }))
}

async fn first_timestamp(q: &Queue) -> Option<i64> {
    q.peek_first().await.map(|r| r.message.core().timestamp)
}

pub(super) fn internal_prefix(broker: &Broker) -> String {
    broker.management().config().file.as_ref().map_or_else(
        || "$.artemis.internal.".to_owned(),
        |f| f.internal_naming_prefix.clone(),
    )
}

fn filter(
    broker: &Broker,
    text: Option<String>,
) -> Result<Option<crate::queue::FilterRef>, String> {
    let text = text.filter(|t| !t.trim().is_empty());
    broker
        .post_office()
        .filters()
        .create(text.as_deref())
        .map_err(|e| e.message)
}

/// The queue `name` a message goes to, for the move and copy operations.
fn target(broker: &Broker, name: &str) -> Result<Queue, String> {
    broker
        .post_office()
        .queue(name)
        .ok_or_else(|| format!("AMQ229049: No queue found for {name}"))
}

fn priority(p: i32) -> Result<u8, String> {
    if (0..=9).contains(&p) {
        Ok(p as u8)
    } else {
        Err(format!(
            "AMQ229048: invalid new Priority value: {p}. It must be between 0 and 9 (both included)"
        ))
    }
}

pub(super) async fn invoke(broker: &Arc<Broker>, q: &Queue, call: &Call<'_>) -> Option<Outcome> {
    let limit = q.settings().management_message_attribute_size_limit;
    let browse_limit = q.settings().management_browse_page_size;
    macro_rules! try_op {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            }
        };
    }
    let count = |n: usize| Some(Ok(Json::from(n as i32)));
    if call.is("removeMessage", &[L]).is_some() {
        let a = call.is("removeMessage", &[L])?;
        let n = q
            .act(
                Selection::id(a.l(0)),
                SelectionAction::Remove(AckReason::Normal),
            )
            .await;
        return Some(Ok(Json::from(n == 1)));
    }
    if let Some(a) = call.is("removeMessages", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        return count(remove(q, f, FLUSH_LIMIT).await);
    }
    if let Some(a) = call.is("removeMessages", &[I, S]) {
        let f = try_op!(filter(broker, a.s(1)));
        return count(remove(q, f, a.i(0)).await);
    }
    if call.is("removeAllMessages", &[]).is_some() {
        return count(remove(q, None, FLUSH_LIMIT).await);
    }
    if let Some(a) = call.is("expireMessage", &[L]) {
        let n = q.act(Selection::id(a.l(0)), SelectionAction::Expire).await;
        return Some(Ok(Json::from(n == 1)));
    }
    if let Some(a) = call.is("expireMessages", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        return count(q.act(Selection::filter(f), SelectionAction::Expire).await);
    }
    if let Some(a) = call.is("retryMessage", &[L]) {
        let n = q.act(Selection::id(a.l(0)), SelectionAction::Retry).await;
        return Some(Ok(Json::from(n > 0)));
    }
    if call.is("retryMessages", &[]).is_some() {
        return count(q.act(Selection::default(), SelectionAction::Retry).await);
    }
    if let Some(a) = call.is("retryMessages", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        return count(q.act(Selection::filter(f), SelectionAction::Retry).await);
    }
    let move_one = |id: i64, other: Option<String>, reject: bool| async move {
        let other = other.unwrap_or_default();
        let t = target(broker, &other)?;
        let address = t.config().address.to_string();
        let n = q
            .act(
                Selection::id(id),
                SelectionAction::Move {
                    address,
                    target: Some(t),
                    reject_duplicates: reject,
                },
            )
            .await;
        Ok(Json::from(n == 1))
    };
    if let Some(a) = call.is("moveMessage", &[L, S]) {
        return Some(move_one(a.l(0), a.s(1), false).await);
    }
    if let Some(a) = call.is("moveMessage", &[L, S, B]) {
        return Some(move_one(a.l(0), a.s(1), a.b(2)).await);
    }
    let move_many = |filter_text: Option<String>,
                     other: Option<String>,
                     reject: bool,
                     message_count: i32| async move {
        let other = other.unwrap_or_default();
        if **q.name() == *other {
            return Err("Cannot move messages onto itself".to_owned());
        }
        let f = filter(broker, filter_text)?;
        let t = target(broker, &other)?;
        let address = t.config().address.to_string();
        let sel = Selection {
            filter: f,
            message_id: None,
            max: (message_count > 0).then_some(message_count as usize),
        };
        let n = q
            .act(
                sel,
                SelectionAction::Move {
                    address,
                    target: Some(t),
                    reject_duplicates: reject,
                },
            )
            .await;
        Ok(Json::from(n as i32))
    };
    if let Some(a) = call.is("moveMessages", &[S, S]) {
        return Some(move_many(a.s(0), a.s(1), false, -1).await);
    }
    if let Some(a) = call.is("moveMessages", &[S, S, B]) {
        return Some(move_many(a.s(0), a.s(1), a.b(2), -1).await);
    }
    if let Some(a) = call.is("moveMessages", &[I, S, S, B]) {
        return Some(move_many(a.s(1), a.s(2), a.b(3), -1).await);
    }
    if let Some(a) = call.is("moveMessages", &[I, S, S, B, I]) {
        return Some(move_many(a.s(1), a.s(2), a.b(3), a.i(4)).await);
    }
    if let Some(a) = call.is("copyMessage", &[L, S]) {
        let t = try_op!(target(broker, &a.s_or_empty(1)));
        let address = t.config().address.to_string();
        let n = q
            .act(
                Selection::id(a.l(0)),
                SelectionAction::Copy { address, target: t },
            )
            .await;
        return Some(Ok(Json::from(n == 1)));
    }
    if let Some(a) = call.is("sendMessagesToDeadLetterAddress", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        return count(
            q.act(Selection::filter(f), SelectionAction::SendToDeadLetter)
                .await,
        );
    }
    if let Some(a) = call.is("sendMessageToDeadLetterAddress", &[L]) {
        let n = q
            .act(Selection::id(a.l(0)), SelectionAction::SendToDeadLetter)
            .await;
        return Some(Ok(Json::from(n == 1)));
    }
    if let Some(a) = call.is("changeMessagesPriority", &[S, I]) {
        let p = try_op!(priority(a.i(1)));
        let f = try_op!(filter(broker, a.s(0)));
        return count(
            q.act(Selection::filter(f), SelectionAction::ChangePriority(p))
                .await,
        );
    }
    if let Some(a) = call.is("changeMessagePriority", &[L, I]) {
        let p = try_op!(priority(a.i(1)));
        let n = q
            .act(Selection::id(a.l(0)), SelectionAction::ChangePriority(p))
            .await;
        return Some(Ok(Json::from(n == 1)));
    }
    if call.is("countMessages", &[]).is_some() {
        return Some(Ok(Json::from(q.message_count())));
    }
    if let Some(a) = call.is("countMessages", &[S]) {
        let groups = try_op!(count_messages(broker, q, a.s(0), None).await);
        return Some(Ok(Json::from(
            groups
                .into_iter()
                .find(|(k, _)| k.is_none())
                .map_or(0, |(_, n)| n),
        )));
    }
    if let Some(a) = call.is("countMessages", &[S, S]) {
        let groups = try_op!(count_messages(broker, q, a.s(0), a.s(1)).await);
        return Some(Ok(Json::from(group_json(groups).to_string())));
    }
    if let Some(a) = call.is("countDeliveringMessages", &[S]) {
        let groups = try_op!(count_delivering(broker, q, a.s(0), None).await);
        return Some(Ok(Json::from(
            groups
                .into_iter()
                .find(|(k, _)| k.is_none())
                .map_or(0, |(_, n)| n),
        )));
    }
    if let Some(a) = call.is("countDeliveringMessages", &[S, S]) {
        let groups = try_op!(count_delivering(broker, q, a.s(0), a.s(1)).await);
        return Some(Ok(Json::from(group_json(groups).to_string())));
    }
    if let Some(a) = call.is("listMessages", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        return Some(Ok(list_messages(q, f, limit, browse_limit).await));
    }
    if let Some(a) = call.is("listMessagesAsJSON", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        return Some(Ok(Json::from(
            list_messages(q, f, limit, browse_limit).await.to_string(),
        )));
    }
    if call.is("listScheduledMessages", &[]).is_some() {
        return Some(Ok(scheduled(q, limit).await));
    }
    if call.is("listScheduledMessagesAsJSON", &[]).is_some() {
        return Some(Ok(Json::from(scheduled(q, limit).await.to_string())));
    }
    if call.is("listDeliveringMessages", &[]).is_some() {
        return Some(Ok(delivering(q, limit).await));
    }
    if call.is("listDeliveringMessagesAsJSON", &[]).is_some() {
        return Some(Ok(Json::from(delivering(q, limit).await.to_string())));
    }
    if call.is("peekFirstMessageAsJSON", &[]).is_some() {
        let first = q.peek_first().await;
        return Some(Ok(Json::from(first.map_or("null".to_owned(), |r| {
            messages::to_map(&r, -1).to_string()
        }))));
    }
    if call.is("peekFirstScheduledMessageAsJSON", &[]).is_some() {
        let first = q.peek_first_scheduled().await;
        return Some(Ok(Json::from(first.map_or("null".to_owned(), |r| {
            messages::to_map(&r, -1).to_string()
        }))));
    }
    if call.is("pause", &[]).is_some() {
        q.pause(false);
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("pause", &[B]) {
        q.pause(a.b(0));
        return Some(Ok(Json::Null));
    }
    if call.is("resume", &[]).is_some() {
        q.resume();
        return Some(Ok(Json::Null));
    }
    if call.is("enable", &[]).is_some() || call.is("disable", &[]).is_some() {
        let mut spec = spec_of(q);
        spec.enabled = Some(call.op == "enable");
        return Some(
            broker
                .post_office()
                .update_queue(spec)
                .map(|_| Json::Null)
                .map_err(|e| e.message),
        );
    }
    if call.is("resetAllGroups", &[]).is_some() {
        q.reset_groups(None).await;
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("resetGroup", &[S]) {
        q.reset_groups(Some(a.s_or_empty(0))).await;
        return Some(Ok(Json::Null));
    }
    if call.is("listGroupsAsJSON", &[]).is_some() {
        let groups = q.groups().await;
        let items: Vec<Json> = groups
            .into_iter()
            .map(|(g, c)| {
                Json::object()
                    .with("groupID", g)
                    .with("consumerID", c as i64)
                    .with("browseOnly", false)
            })
            .collect();
        return Some(Ok(Json::from(Json::Array(items).to_string())));
    }
    if call.is("listConsumersAsJSON", &[]).is_some() {
        let consumers = q.consumers().await;
        let items: Vec<Json> = consumers
            .into_iter()
            .map(|c| {
                Json::object()
                    .with("consumerID", c.client_id)
                    .with("sequentialID", c.id as i64)
                    .with("browseOnly", c.browse_only)
                    .with("messagesInTransit", c.delivering)
                    .with("messagesInTransitSize", c.delivering_size)
            })
            .collect();
        return Some(Ok(Json::from(Json::Array(items).to_string())));
    }
    if call.is("resetMessagesAdded", &[]).is_some() {
        q.reset_messages_added();
        return Some(Ok(Json::Null));
    }
    if call.is("resetMessagesAcknowledged", &[]).is_some() {
        q.reset_messages_acknowledged();
        return Some(Ok(Json::Null));
    }
    if call.is("resetMessagesExpired", &[]).is_some() {
        q.reset_messages_expired();
        return Some(Ok(Json::Null));
    }
    if call.is("resetMessagesKilled", &[]).is_some() {
        q.reset_messages_killed();
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("deliverScheduledMessages", &[S]) {
        let f = try_op!(filter(broker, a.s(0)));
        q.act(Selection::filter(f), SelectionAction::DeliverScheduled)
            .await;
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("deliverScheduledMessage", &[L]) {
        q.act(Selection::id(a.l(0)), SelectionAction::DeliverScheduled)
            .await;
        return Some(Ok(Json::Null));
    }
    if call.is("flushExecutor", &[]).is_some() {
        q.stats().await;
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("sendMessage", &[M, I, S, B, S, S]) {
        return Some(send(broker, q, &a, false).await);
    }
    if let Some(a) = call.is("sendMessage", &[M, I, S, B, S, S, B]) {
        let create_id = a.b(6);
        return Some(send(broker, q, &a, create_id).await);
    }
    None
}

/// `deleteMatchingReferences(flushLimit, filter)`: acknowledged as killed.
async fn remove(q: &Queue, f: Option<crate::queue::FilterRef>, _flush_limit: i32) -> usize {
    q.act(
        Selection::filter(f),
        SelectionAction::Remove(AckReason::Killed),
    )
    .await
}

/// The queue's configuration as a `QueueConfiguration` for `updateQueue`.
pub(super) fn spec_of(q: &Queue) -> crate::postoffice::QueueSpec {
    let c = q.config();
    let mut spec = crate::postoffice::QueueSpec::new(c.name.clone(), c.address.clone());
    spec.routing_type = Some(c.routing_type);
    spec.filter_string = q.filter_string();
    spec.durable = c.durable;
    spec.max_consumers = Some(c.max_consumers);
    spec.purge_on_no_consumers = Some(c.purge_on_no_consumers);
    spec.exclusive = Some(c.exclusive);
    spec.group_rebalance = Some(c.group_rebalance);
    spec.group_rebalance_pause_dispatch = Some(c.group_rebalance_pause_dispatch);
    spec.group_buckets = Some(c.group_buckets);
    spec.group_first_key = c.group_first_key.clone();
    spec.non_destructive = Some(c.non_destructive);
    spec.consumers_before_dispatch = Some(c.consumers_before_dispatch);
    spec.delay_before_dispatch = Some(c.delay_before_dispatch);
    spec.ring_size = Some(c.ring_size);
    spec.enabled = Some(c.enabled);
    spec.user = c.user.clone();
    spec.configuration_managed = c.configuration_managed;
    spec
}

/// `internalCountMessages`: how many queued messages the filter accepts,
/// keyed by the value of `group_by` (`None` without one).
async fn count_messages(
    broker: &Broker,
    q: &Queue,
    filter_text: Option<String>,
    group_by: Option<String>,
) -> Result<Vec<(Option<String>, i64)>, String> {
    let f = filter(broker, filter_text)?;
    if f.is_none() && group_by.is_none() {
        return Ok(vec![(None, q.message_count())]);
    }
    let refs = q.browse().await;
    Ok(group(refs.iter(), f.as_ref(), group_by.as_deref()))
}

async fn count_delivering(
    broker: &Broker,
    q: &Queue,
    filter_text: Option<String>,
    group_by: Option<String>,
) -> Result<Vec<(Option<String>, i64)>, String> {
    let f = filter(broker, filter_text)?;
    if f.is_none() && group_by.is_none() {
        return Ok(vec![(None, i64::from(q.stats().await.delivering_count))]);
    }
    let delivering = q.delivering_messages().await;
    Ok(group(
        delivering.iter().flat_map(|(_, refs)| refs.iter()),
        f.as_ref(),
        group_by.as_deref(),
    ))
}

fn group<'a>(
    refs: impl Iterator<Item = &'a crate::message::MessageRef>,
    f: Option<&crate::queue::FilterRef>,
    group_by: Option<&str>,
) -> Vec<(Option<String>, i64)> {
    let mut out: Vec<(Option<String>, i64)> = Vec::new();
    for r in refs {
        if f.is_some_and(|f| !f.matches(&*r.message)) {
            continue;
        }
        let key = group_by.map(|g| messages::filter_value_string(&r.message, g));
        let key = key.flatten();
        match out.iter_mut().find(|(k, _)| *k == key) {
            Some((_, n)) => *n += 1,
            None => out.push((key, 1)),
        }
    }
    out
}

/// `JsonUtil.toJsonObject` of the counts: the `null` key as "null".
fn group_json(groups: Vec<(Option<String>, i64)>) -> Json {
    let mut o = Json::object();
    for (k, n) in groups {
        o.put(k.as_deref().unwrap_or("null"), n);
    }
    o
}

/// `listMessages`: the first `management-browse-page-size` queued
/// messages, those the filter accepts as maps.
async fn list_messages(
    q: &Queue,
    f: Option<crate::queue::FilterRef>,
    limit: i32,
    browse_limit: i32,
) -> Json {
    let refs = q.browse().await;
    Json::Array(
        refs.iter()
            .take(browse_limit.max(0) as usize)
            .filter(|r| f.as_ref().is_none_or(|f| f.matches(&*r.message)))
            .map(|r| messages::to_map(r, limit))
            .collect(),
    )
}

async fn scheduled(q: &Queue, limit: i32) -> Json {
    Json::Array(
        q.scheduled_messages()
            .await
            .iter()
            .map(|r| messages::to_map(r, limit))
            .collect(),
    )
}

/// `listDeliveringMessages`: each consumer's delivering messages, keyed by
/// the consumer's `toManagementString`.
async fn delivering(q: &Queue, limit: i32) -> Json {
    let mut o = Json::object();
    for (c, refs) in q.delivering_messages().await {
        o.put(
            &format!("ServerConsumerImpl [id={}]", c.client_id),
            Json::Array(refs.iter().map(|r| messages::to_map(r, limit)).collect()),
        );
    }
    o
}

/// `sendMessage(headers, type, body, durable, user, password[,
/// createMessageId])`: a message built from the arguments goes to this
/// queue, as the user given.
async fn send(
    broker: &Arc<Broker>,
    q: &Queue,
    a: &super::args::Args,
    create_message_id: bool,
) -> Outcome {
    let headers = a.string_map(0).unwrap_or_default();
    let fqqn = format!("{}::{}", q.config().address, q.name());
    messages::send(
        broker,
        &fqqn,
        &headers,
        a.i(1),
        a.s(2),
        a.b(3),
        a.s(4),
        a.s(5),
        create_message_id,
    )
    .await
    .map(Json::from)
}
