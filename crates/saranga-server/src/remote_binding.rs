//! Remote queue bindings: a queue of another broker in the cluster, bound
//! here to its address so that messages routed to it go to the
//! store-and-forward queue of the cluster bridge that reaches its broker,
//! each carrying the remote queue's ID for that bridge
//! (`_AMQ_ROUTE_TO<bridge>`).
//!
//! Ported from `RemoteQueueBindingImpl`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};

use saranga_config::MessageLoadBalancing;

use crate::Name;
use crate::queue::{FilterRef, Queue};

/// `Message.HDR_ROUTE_TO_IDS`.
pub const HDR_ROUTE_TO_IDS: &str = "_AMQ_ROUTE_TO";

/// `BindingType.REMOTE_QUEUE`.
pub const BINDING_TYPE_REMOTE_QUEUE: i32 = 1;

/// The consumers the other broker reported (`consumerCount`, `filters`
/// and `filterCounts`).
#[derive(Default)]
struct Consumers {
    count: i32,
    /// Each distinct filter with the consumers that have it.
    filters: Vec<(String, FilterRef, i32)>,
}

/// `RemoteQueueBindingImpl`.
pub struct RemoteBinding {
    id: i64,
    address: Name,
    /// `uniqueName`, which is also its cluster name: the queue's name and
    /// its broker's node ID.
    unique_name: Name,
    routing_name: Name,
    remote_queue_id: i64,
    filter: RwLock<Option<FilterRef>>,
    sf_queue: Queue,
    /// `_AMQ_ROUTE_TO` and the bridge's name.
    ids_header: String,
    distance: i32,
    load_balancing: MessageLoadBalancing,
    consumers: Mutex<Consumers>,
    connected: AtomicBool,
}

impl RemoteBinding {
    #[allow(clippy::too_many_arguments)] // RemoteQueueBindingImpl's constructor
    pub fn new(
        id: i64,
        address: &str,
        unique_name: &str,
        routing_name: &str,
        remote_queue_id: i64,
        filter: Option<FilterRef>,
        sf_queue: Queue,
        bridge_name: &str,
        distance: i32,
        load_balancing: MessageLoadBalancing,
    ) -> Self {
        Self {
            id,
            address: address.into(),
            unique_name: unique_name.into(),
            routing_name: routing_name.into(),
            remote_queue_id,
            filter: RwLock::new(filter),
            sf_queue,
            ids_header: format!("{HDR_ROUTE_TO_IDS}{bridge_name}"),
            distance,
            load_balancing,
            consumers: Mutex::default(),
            connected: AtomicBool::new(true),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn address(&self) -> &Name {
        &self.address
    }

    /// `getUniqueName` and `getClusterName`.
    pub fn unique_name(&self) -> &Name {
        &self.unique_name
    }

    pub fn routing_name(&self) -> &Name {
        &self.routing_name
    }

    pub fn remote_queue_id(&self) -> i64 {
        self.remote_queue_id
    }

    pub fn filter(&self) -> Option<FilterRef> {
        self.filter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `setFilter`: a `BINDING_UPDATED` from the other broker.
    pub fn set_filter(&self, filter: Option<FilterRef>) {
        *self.filter.write().unwrap_or_else(|e| e.into_inner()) = filter;
    }

    /// The store-and-forward queue messages for it go to.
    pub fn sf_queue(&self) -> &Queue {
        &self.sf_queue
    }

    /// The property its routing adds the remote queue ID to.
    pub fn ids_header(&self) -> &str {
        &self.ids_header
    }

    pub fn distance(&self) -> i32 {
        self.distance
    }

    pub fn load_balancing(&self) -> MessageLoadBalancing {
        self.load_balancing
    }

    fn consumers(&self) -> std::sync::MutexGuard<'_, Consumers> {
        self.consumers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `isHighAcceptPriority`: the other broker has a consumer that would
    /// take the message, and the cluster balances on demand.
    pub fn is_high_accept_priority(&self, message: &dyn saranga_filter::Filterable) -> bool {
        let c = self.consumers();
        if c.count <= 0 || self.load_balancing == MessageLoadBalancing::Off {
            return false;
        }
        c.filters.is_empty() || c.filters.iter().any(|(_, f, _)| f.matches(message))
    }

    /// `addConsumer`: one more consumer there, with its filter.
    pub fn add_consumer(&self, filter: Option<(String, FilterRef)>) {
        let mut c = self.consumers();
        if let Some((text, f)) = filter {
            match c.filters.iter_mut().find(|(t, _, _)| *t == text) {
                Some(entry) => entry.2 += 1,
                None => c.filters.push((text, f, 1)),
            }
        }
        c.count += 1;
    }

    /// `removeConsumer`.
    pub fn remove_consumer(&self, filter: Option<&str>) {
        let mut c = self.consumers();
        if let Some(text) = filter.filter(|t| !t.is_empty())
            && let Some(i) = c.filters.iter().position(|(t, _, _)| t == text)
        {
            c.filters[i].2 -= 1;
            if c.filters[i].2 == 0 {
                c.filters.remove(i);
            }
        }
        c.count = (c.count - 1).max(0);
    }

    /// `reset`: the other broker is about to report its consumers again.
    pub fn reset(&self) {
        let mut c = self.consumers();
        c.count = 0;
        c.filters.clear();
    }

    /// `consumerCount`.
    pub fn consumer_count(&self) -> i32 {
        self.consumers().count
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// `disconnect`: the bridge lost its connection; the binding stays
    /// until the other broker reports its queues again.
    pub fn disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    pub fn connect(&self) {
        self.connected.store(true, Ordering::SeqCst);
    }

    /// `addRouteContextToMessage`: the remote queue's ID, ahead of those
    /// the property already holds.
    pub fn add_route_context(&self, ids: Option<&[u8]>) -> Vec<u8> {
        let mut out = self.remote_queue_id.to_be_bytes().to_vec();
        out.extend_from_slice(ids.unwrap_or_default());
        out
    }

    /// `toManagementString`.
    pub fn management_string(&self) -> String {
        format!(
            "RemoteQueueBindingImpl [address={}, storeAndForwardQueue={}, remoteQueueID={}]",
            self.address,
            self.sf_queue.name(),
            self.remote_queue_id
        )
    }
}

impl std::fmt::Debug for RemoteBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RemoteQueueBindingImpl({})[address={}, uniqueName={}, remoteQueueID={}]",
            if self.is_connected() {
                "connected"
            } else {
                "disconnected"
            },
            self.address,
            self.unique_name,
            self.remote_queue_id
        )
    }
}

/// `UUID.stripTrailingUUID`: a cluster name without the node ID ending it.
pub fn strip_trailing_uuid(name: &str) -> &str {
    const LEN: usize = 36;
    if name.len() < LEN || !name.is_char_boundary(name.len() - LEN) {
        return name;
    }
    let (head, tail) = name.split_at(name.len() - LEN);
    let is_uuid = tail.char_indices().all(|(i, c)| match i {
        8 | 13 | 18 | 23 => c == '-',
        _ => c.is_ascii_hexdigit(),
    });
    if is_uuid { head } else { name }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_node_id_is_stripped() {
        assert_eq!(
            strip_trailing_uuid("orders1f7f5b5e-6c0c-11ee-8b45-a4bb6d4f2a10"),
            "orders"
        );
        assert_eq!(strip_trailing_uuid("orders"), "orders");
    }
}
