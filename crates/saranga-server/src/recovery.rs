//! Binding records and restart. Ported from the binding encodings'
//! constructors (`PersistentQueueBindingEncoding(QueueConfiguration)`,
//! `PersistentAddressBindingEncoding`), and from the journal loading of
//! `ActiveMQServerImpl.loadJournals` with `PostOfficeJournalLoader`.

use std::collections::HashMap;
use std::sync::Arc;

use saranga_buffer::SimpleString;
use saranga_storage::RecordCodec;
use saranga_storage::codec::{PersistentAddressBindingEncoding, PersistentQueueBindingEncoding};

use crate::large::LargeMessageStore;
use crate::message::{self, MessageRef, ServerMessage};
use crate::postoffice::{AddressInfo, PostOffice, RoutingTypes};
use crate::queue::{Queue, QueueConfig};
use crate::storage::{RecoveredMessages, Storage, StoreResult, ids};
use crate::transaction::{ResourceManager, Transaction, TxState};
use crate::{BrokerError, Result, RoutingType};

/// Duplicate IDs with their journal records.
type DuplicateIds = Vec<(Vec<u8>, i64)>;

fn ss(s: &str) -> SimpleString {
    SimpleString::from(s)
}

fn text(s: &SimpleString) -> String {
    s.to_string_lossy()
}

/// The `QUEUE_BINDING_RECORD` body for a queue, every field written as
/// Artemis 2.57 writes it.
pub fn queue_binding(c: &QueueConfig) -> Vec<u8> {
    PersistentQueueBindingEncoding {
        name: ss(&c.name),
        address: ss(&c.address),
        filter_string: c.filter_string.as_deref().map(ss),
        metadata: Some(PersistentQueueBindingEncoding::metadata_for(
            c.user.as_deref(),
        )),
        auto_created: c.auto_created,
        consumers: Some((
            c.max_consumers,
            c.purge_on_no_consumers,
            c.routing_type.to_byte() as u8,
        )),
        exclusive: Some(c.exclusive),
        last_value: Some(c.last_value),
        configuration_managed: Some(c.configuration_managed),
        consumers_before_dispatch: Some(c.consumers_before_dispatch),
        delay_before_dispatch: Some(c.delay_before_dispatch),
        last_value_key: Some(c.last_value_key.as_deref().map(ss)),
        non_destructive: Some(c.non_destructive),
        group_rebalance: Some(c.group_rebalance),
        group_buckets: Some(c.group_buckets),
        auto_delete: Some(c.auto_delete),
        auto_delete_delay: Some(c.auto_delete_delay),
        auto_delete_message_count: Some(c.auto_delete_message_count),
        group_first_key: Some(c.group_first_key.as_deref().map(ss)),
        ring_size: Some(c.ring_size),
        enabled: Some(c.enabled),
        group_rebalance_pause_dispatch: Some(c.group_rebalance_pause_dispatch),
        internal: Some(false),
    }
    .encode()
}

/// A queue's configuration from its binding record. Fields an older broker
/// did not write take the defaults `PersistentQueueBindingEncoding.decode`
/// gives them.
pub fn queue_config(b: &PersistentQueueBindingEncoding) -> QueueConfig {
    let (max_consumers, purge_on_no_consumers, routing_type) =
        b.consumers.unwrap_or((-1, false, 0));
    QueueConfig {
        name: text(&b.name).into(),
        address: text(&b.address).into(),
        routing_type: RoutingType::from_byte(routing_type as i8).unwrap_or(RoutingType::Multicast),
        filter_string: b
            .filter_string
            .as_ref()
            .map(text)
            .filter(|f| !f.trim().is_empty()),
        durable: true,
        temporary: false,
        auto_created: b.auto_created,
        max_consumers,
        purge_on_no_consumers,
        exclusive: b.exclusive.unwrap_or(false),
        last_value: b.last_value.unwrap_or(false),
        last_value_key: b.last_value_key.clone().flatten().as_ref().map(text),
        non_destructive: b.non_destructive.unwrap_or(false),
        consumers_before_dispatch: b.consumers_before_dispatch.unwrap_or(0),
        delay_before_dispatch: b.delay_before_dispatch.unwrap_or(-1),
        group_rebalance: b.group_rebalance.unwrap_or(false),
        group_rebalance_pause_dispatch: b.group_rebalance_pause_dispatch.unwrap_or(false),
        group_buckets: b.group_buckets.unwrap_or(-1),
        group_first_key: b.group_first_key.clone().flatten().as_ref().map(text),
        auto_delete: b.auto_delete.unwrap_or(false),
        auto_delete_delay: b.auto_delete_delay.unwrap_or(0),
        auto_delete_message_count: b.auto_delete_message_count.unwrap_or(0),
        ring_size: b.ring_size.unwrap_or(-1),
        enabled: b.enabled.unwrap_or(true),
        configuration_managed: b.configuration_managed.unwrap_or(false),
        user: b.user(),
    }
}

/// The `ADDRESS_BINDING_RECORD` body: routing types in `EnumSet` order.
pub fn address_binding(info: &AddressInfo) -> Vec<u8> {
    let mut routing_types = Vec::new();
    if info.routing_types.multicast {
        routing_types.push(RoutingType::Multicast.to_byte() as u8);
    }
    if info.routing_types.anycast {
        routing_types.push(RoutingType::Anycast.to_byte() as u8);
    }
    PersistentAddressBindingEncoding {
        name: ss(&info.name),
        routing_types,
        auto_created: info.auto_created,
        internal: Some(false),
    }
    .encode()
}

fn address_info(id: i64, b: &PersistentAddressBindingEncoding) -> AddressInfo {
    let mut routing_types = RoutingTypes::default();
    for &t in &b.routing_types {
        if let Some(t) = RoutingType::from_byte(t as i8) {
            routing_types.add(t);
        }
    }
    AddressInfo {
        id,
        name: text(&b.name).into(),
        routing_types,
        auto_created: b.auto_created,
        temporary: false,
    }
}

fn corrupt(what: &str, e: impl std::fmt::Display) -> BrokerError {
    BrokerError::internal(format!("{what}: {e}"))
}

/// `loadBindingJournal` with `initAddresses` and `initQueues`: addresses
/// first, then their queues, each with its persisted ID.
pub fn load_bindings(storage: &Storage, post_office: &PostOffice) -> Result<()> {
    let records = storage
        .load_bindings()
        .map_err(|e| corrupt("loading the bindings journal", e))?;
    let mut queues = Vec::new();
    for r in records {
        match r.user_record_type {
            ids::ADDRESS_BINDING_RECORD => {
                let b = PersistentAddressBindingEncoding::decode(&r.data)
                    .map_err(|e| corrupt("address binding", e))?;
                post_office.reload_address(address_info(r.id, &b));
            }
            ids::QUEUE_BINDING_RECORD => {
                let b = PersistentQueueBindingEncoding::decode(&r.data)
                    .map_err(|e| corrupt("queue binding", e))?;
                queues.push((r.id, queue_config(&b)));
            }
            other => tracing::debug!("bindings record type {other} is not used yet"),
        }
    }
    for (id, config) in queues {
        if let Err(e) = post_office.reload_queue(id, config) {
            tracing::warn!("{e}");
        }
    }
    Ok(())
}

/// `loadMessageJournal` with `handleAddMessage` and the prepared
/// transactions: references go back to their queues in journal order with
/// their delivery counts; prepared XA branches go to the resource manager
/// holding their sends and acknowledgements.
///
/// `stored` holds the large-message files found before loading
/// (`recoverLargeMessagesOnFolder`); the IDs of large messages in the
/// journal leave it, and what remains is for the caller to delete once the
/// pages have been looked at.
pub async fn load_messages(
    storage: &Storage,
    post_office: &PostOffice,
    resource_manager: &ResourceManager,
    large_store: &Arc<LargeMessageStore>,
    stored: &mut std::collections::HashSet<i64>,
) -> Result<()> {
    let RecoveredMessages {
        messages,
        large,
        queues,
        prepared,
        duplicates,
        cursor_acks,
        page_completes,
        page_txs,
    } = storage
        .load_messages()
        .map_err(|e| corrupt("loading the message journal", e))?;
    let by_id: HashMap<i64, Queue> = post_office
        .queues()
        .into_iter()
        .map(|q| (q.id(), q))
        .collect();
    let messages: HashMap<i64, Arc<ServerMessage>> = messages
        .into_iter()
        .map(|(id, m)| {
            if !large.contains(&id) {
                return (id, ServerMessage::new(m));
            }
            stored.remove(&m.message_id);
            let body = large_store.existing(m.message_id);
            // parseLargeMessage: a copy written by an older broker may
            // still share its original's file.
            let body = match (body.path(), message::orig_message_id(&m)) {
                (Some(p), Some(orig)) if !p.exists() => {
                    let original = large_store.existing(orig);
                    original.set_keep(true);
                    original.copy(m.message_id, true).unwrap_or(body)
                }
                _ => body,
            };
            (id, ServerMessage::new_large(m, body))
        })
        .collect();
    let paging = post_office.paging();
    paging.load_records(cursor_acks, page_completes, page_txs);
    // The duplicate-ID caches, per address in journal order.
    let mut by_address: Vec<(String, DuplicateIds)> = Vec::new();
    for d in duplicates {
        match by_address.iter_mut().find(|(a, _)| *a == d.address) {
            Some((_, ids)) => ids.push((d.id, d.record)),
            None => by_address.push((d.address, vec![(d.id, d.record)])),
        }
    }
    for (address, ids) in by_address {
        let cache = post_office.duplicate_cache(&address);
        let mut c = crate::duplicate::lock(&cache);
        if c.is_persistent() {
            c.load(ids, || post_office.ids().next())
                .map_err(|e| corrupt("duplicate IDs", e))?;
        }
    }
    let now = message::now_millis();
    for (queue_id, refs) in queues {
        let Some(queue) = by_id.get(&queue_id) else {
            if !refs.is_empty() {
                tracing::warn!("AMQ222033: Cannot find queue {queue_id} to load messages into");
            }
            continue;
        };
        for r in refs {
            let Some(m) = messages.get(&r.message_id) else {
                continue;
            };
            m.durable_up();
            // A journaled delivery time already past is dropped; without
            // one the message's own property counts (`PostOfficeJournalLoader`
            // then `reload`).
            let scheduled_time = match r.scheduled_time {
                0 => message::scheduled_delivery_time(m.core()),
                t if t <= now => 0,
                t => t,
            };
            // setDeliveryCount also sets the persisted count.
            queue.add(MessageRef {
                message: m.clone(),
                delivery_count: r.delivery_count,
                persisted_count: r.delivery_count,
                scheduled_time,
                paged: None,
            });
        }
    }
    for p in prepared {
        let mut tx = Transaction::new(p.id, Some(p.xid.clone()));
        tx.persistent = true;
        for (queue_id, message_id) in p.sends {
            let (Some(q), Some(m)) = (by_id.get(&queue_id), messages.get(&message_id)) else {
                tracing::warn!(
                    "AMQ222034: Cannot find queue {queue_id} for a message in a prepared transaction"
                );
                continue;
            };
            m.durable_up();
            let mut r = MessageRef::new(m.clone());
            r.scheduled_time = message::scheduled_delivery_time(m.core());
            tx.sends.push((q.clone(), r));
        }
        for (queue_id, message_id) in p.acks {
            let Some(q) = by_id.get(&queue_id) else {
                tracing::warn!(
                    "AMQ222035: Cannot find queue {queue_id} for an acknowledgement in a prepared transaction"
                );
                continue;
            };
            match q.remove_reference(message_id).await {
                Some(r) => tx.acks.push((q.clone(), r)),
                None => tracing::warn!("AMQ222036: Unable to remove reference {message_id}"),
            }
        }
        for d in p.duplicates {
            tx.duplicates.push(crate::duplicate::PendingId {
                cache: post_office.duplicate_cache(&d.address),
                id: d.id,
                record: d.record,
            });
        }
        // Paged messages the transaction sent wait for it; those it
        // acknowledged are not delivered meanwhile.
        for pt in &p.page_txs {
            tx.paging.page_txs.push(paging.reload_prepared(pt));
        }
        for a in &p.cursor_acks {
            let (Some(q), Some(sub)) = (by_id.get(&a.queue_id), paging.subscription(a.queue_id))
            else {
                tracing::warn!(
                    "AMQ222035: Cannot find queue {} for an acknowledgement in a prepared transaction",
                    a.queue_id
                );
                continue;
            };
            sub.reload_prepared_ack(a.page, a.message_nr);
            if let Some(pr) = sub.paged_ref(a.page, a.message_nr) {
                sub.read_in(pr.position.size);
                let mut r = MessageRef::new(pr.message);
                r.paged = Some(crate::message::PagePosition {
                    ack_record: a.record,
                    ..pr.position
                });
                tx.acks.push((q.clone(), r));
            }
        }
        tx.paging.updates = p.page_tx_updates.clone();
        tx.paging.recovered(paging.clone());
        tx.state = TxState::Prepared;
        resource_manager.put(p.xid, Arc::new(std::sync::Mutex::new(tx)));
    }
    Ok(())
}

/// Persists a new address (`addAddressInfo` → `addAddressBinding`).
pub fn store_address(storage: &Storage, info: &AddressInfo) -> StoreResult<()> {
    storage.add_binding(info.id, ids::ADDRESS_BINDING_RECORD, address_binding(info))
}
