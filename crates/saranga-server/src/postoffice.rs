//! Addresses, queue bindings and routing. Ported from `PostOfficeImpl`,
//! `SimpleAddressManager` / `WildcardAddressManager`, `BindingsImpl`, and
//! the address and queue management of `ActiveMQServerImpl` and
//! `ServerSessionImpl` (`createQueue`, `checkAutoCreate`, the queries).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

use crate::error::{BrokerError, Result};
use crate::message::{MessageRef, routing_type};
use crate::queue::{FilterRef, Queue, QueueConfig};
use crate::settings::{AddressSettings, SettingsRepository};
use crate::wildcard::WildcardConfig;
use crate::{IdGenerator, Name, RoutingType};

/// `CompositeAddress.SEPARATOR`: `address::queue` is a fully qualified
/// queue name (FQQN).
pub const FQQN_SEPARATOR: &str = "::";

/// Splits an FQQN into its address and queue (`CompositeAddress`).
pub fn split_fqqn(name: &str) -> (&str, Option<&str>) {
    match name.split_once(FQQN_SEPARATOR) {
        Some((address, queue)) => (address, Some(queue)),
        None => (name, None),
    }
}

/// Who routing auto-creates addresses and queues for: the sending
/// session's security checks (`ServerSessionImpl.createAddress` and
/// `createQueue` check `CREATE_ADDRESS` and `CREATE_DURABLE_QUEUE`) and the
/// user its queues are created for.
#[derive(Clone, Copy, Default)]
pub struct Creator<'a> {
    pub security: Option<(
        &'a saranga_security::SecurityStore,
        &'a saranga_security::SecurityAuth,
    )>,
    pub user: Option<&'a str>,
}

impl Creator<'_> {
    pub fn check(
        &self,
        address: &str,
        queue: Option<&str>,
        check: saranga_config::CheckType,
    ) -> Result<()> {
        match self.security {
            Some((store, auth)) => store
                .check(address, queue, check, auth)
                .map_err(BrokerError::security),
            None => Ok(()),
        }
    }
}

/// `CompositeAddress.extractQueueName`.
pub fn queue_name_of(name: &str) -> &str {
    split_fqqn(name).1.unwrap_or(name)
}

/// Parses filter strings (`FilterImpl.createFilter`).
pub trait FilterFactory: Send + Sync {
    /// `None` or a blank string means no filter.
    fn create(&self, text: Option<&str>) -> Result<Option<FilterRef>>;
}

/// A filter factory that accepts only "no filter", for brokers built
/// without the selector engine.
pub struct NoFilters;

impl FilterFactory for NoFilters {
    fn create(&self, text: Option<&str>) -> Result<Option<FilterRef>> {
        match text.map(str::trim) {
            None | Some("") => Ok(None),
            Some(t) => Err(BrokerError::invalid_filter(t)),
        }
    }
}

/// The routing types an address supports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoutingTypes {
    pub multicast: bool,
    pub anycast: bool,
}

impl RoutingTypes {
    pub fn of(routing_type: RoutingType) -> Self {
        let mut t = Self::default();
        t.add(routing_type);
        t
    }

    pub fn contains(self, routing_type: RoutingType) -> bool {
        match routing_type {
            RoutingType::Multicast => self.multicast,
            RoutingType::Anycast => self.anycast,
        }
    }

    pub fn add(&mut self, routing_type: RoutingType) {
        match routing_type {
            RoutingType::Multicast => self.multicast = true,
            RoutingType::Anycast => self.anycast = true,
        }
    }

    /// `EnumSet<RoutingType>.toString()`, for error messages.
    pub fn describe(self) -> String {
        let mut names = Vec::new();
        if self.multicast {
            names.push("MULTICAST");
        }
        if self.anycast {
            names.push("ANYCAST");
        }
        format!("[{}]", names.join(", "))
    }
}

/// `AddressInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressInfo {
    pub id: i64,
    pub name: Name,
    pub routing_types: RoutingTypes,
    pub auto_created: bool,
    pub temporary: bool,
    /// An address the broker keeps for itself.
    pub internal: bool,
}

/// A request to create a queue (`QueueConfiguration` before defaults).
/// `None` fields take the address settings' defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueSpec {
    pub name: Name,
    pub address: Name,
    pub routing_type: Option<RoutingType>,
    pub filter_string: Option<String>,
    pub durable: bool,
    pub temporary: bool,
    pub auto_created: bool,
    pub configuration_managed: bool,
    pub auto_create_address: Option<bool>,
    pub max_consumers: Option<i32>,
    pub purge_on_no_consumers: Option<bool>,
    pub exclusive: Option<bool>,
    pub last_value: Option<bool>,
    pub last_value_key: Option<String>,
    pub non_destructive: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub group_rebalance: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub group_buckets: Option<i32>,
    pub group_first_key: Option<String>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
    pub user: Option<String>,
    /// `setInternal`: a queue (and auto-created address) the broker keeps
    /// for itself.
    pub internal: bool,
}

impl QueueSpec {
    /// `QueueConfiguration.of(name).setAddress(address)`: durable, and
    /// everything else unset.
    pub fn new(name: impl Into<Name>, address: impl Into<Name>) -> Self {
        Self {
            name: name.into(),
            address: address.into(),
            routing_type: None,
            filter_string: None,
            durable: true,
            temporary: false,
            auto_created: false,
            configuration_managed: false,
            auto_create_address: None,
            max_consumers: None,
            purge_on_no_consumers: None,
            exclusive: None,
            last_value: None,
            last_value_key: None,
            non_destructive: None,
            consumers_before_dispatch: None,
            delay_before_dispatch: None,
            group_rebalance: None,
            group_rebalance_pause_dispatch: None,
            group_buckets: None,
            group_first_key: None,
            auto_delete: None,
            auto_delete_delay: None,
            auto_delete_message_count: None,
            ring_size: None,
            enabled: None,
            user: None,
            internal: false,
        }
    }

    /// `QueueConfigurationUtils.applyDynamicDefaults`.
    pub fn apply_dynamic_defaults(&mut self, s: &AddressSettings) {
        self.max_consumers.get_or_insert(s.default_max_consumers);
        self.exclusive.get_or_insert(s.default_exclusive_queue);
        self.group_rebalance
            .get_or_insert(s.default_group_rebalance);
        self.group_rebalance_pause_dispatch
            .get_or_insert(s.default_group_rebalance_pause_dispatch);
        self.group_buckets.get_or_insert(s.default_group_buckets);
        if self.group_first_key.is_none() {
            self.group_first_key = s.default_group_first_key.clone();
        }
        self.last_value.get_or_insert(s.default_last_value_queue);
        if self.last_value_key.is_none() {
            self.last_value_key = s.default_last_value_key.clone();
        }
        self.non_destructive
            .get_or_insert(s.default_non_destructive);
        self.consumers_before_dispatch
            .get_or_insert(s.default_consumers_before_dispatch);
        self.delay_before_dispatch
            .get_or_insert(s.default_delay_before_dispatch);
        self.ring_size.get_or_insert(s.default_ring_size);
        self.routing_type
            .get_or_insert(s.default_queue_routing_type);
        self.purge_on_no_consumers
            .get_or_insert(s.default_purge_on_no_consumers);
        self.auto_create_address
            .get_or_insert(s.auto_create_addresses);
        self.auto_delete.get_or_insert(
            !self.configuration_managed
                && ((self.auto_created && s.auto_delete_queues)
                    || (!self.auto_created && s.auto_delete_created_queues)),
        );
        self.auto_delete_delay
            .get_or_insert(s.auto_delete_queues_delay);
        self.auto_delete_message_count
            .get_or_insert(s.auto_delete_queues_message_count);
    }

    /// Dynamic then static defaults (`applyStaticDefaults`), giving the
    /// queue's final configuration.
    fn resolve(mut self, s: &AddressSettings) -> QueueConfig {
        self.apply_dynamic_defaults(s);
        QueueConfig {
            name: self.name,
            address: self.address,
            routing_type: self.routing_type.unwrap_or(RoutingType::Multicast),
            filter_string: self.filter_string,
            durable: self.durable,
            temporary: self.temporary,
            auto_created: self.auto_created,
            max_consumers: self.max_consumers.unwrap_or(-1),
            purge_on_no_consumers: self.purge_on_no_consumers.unwrap_or(false),
            exclusive: self.exclusive.unwrap_or(false),
            last_value: self.last_value.unwrap_or(false),
            last_value_key: self.last_value_key,
            non_destructive: self.non_destructive.unwrap_or(false),
            consumers_before_dispatch: self.consumers_before_dispatch.unwrap_or(0),
            delay_before_dispatch: self.delay_before_dispatch.unwrap_or(-1),
            group_rebalance: self.group_rebalance.unwrap_or(false),
            group_rebalance_pause_dispatch: self.group_rebalance_pause_dispatch.unwrap_or(false),
            group_buckets: self.group_buckets.unwrap_or(-1),
            group_first_key: self.group_first_key,
            auto_delete: self.auto_delete.unwrap_or(false),
            auto_delete_delay: self.auto_delete_delay.unwrap_or(0),
            auto_delete_message_count: self.auto_delete_message_count.unwrap_or(0),
            ring_size: self.ring_size.unwrap_or(-1),
            enabled: self.enabled.unwrap_or(true),
            configuration_managed: self.configuration_managed,
            user: self.user,
            internal: self.internal,
        }
    }
}

/// `QueueQueryResult`. The optional fields are null in Java for a queue
/// that does not exist (only dynamic defaults are applied to it).
#[derive(Debug, Clone, PartialEq)]
pub struct QueueQueryResult {
    pub name: Name,
    pub address: Name,
    pub durable: bool,
    pub temporary: bool,
    pub filter_string: Option<String>,
    pub consumer_count: i32,
    pub message_count: i64,
    pub exists: bool,
    pub auto_create_queues: bool,
    pub auto_created: bool,
    pub purge_on_no_consumers: bool,
    pub routing_type: RoutingType,
    pub max_consumers: i32,
    pub exclusive: Option<bool>,
    pub group_rebalance: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub group_buckets: Option<i32>,
    pub group_first_key: Option<String>,
    pub last_value: Option<bool>,
    pub last_value_key: Option<String>,
    pub non_destructive: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub default_consumer_window_size: Option<i32>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
    pub configuration_managed: Option<bool>,
}

/// `BindingQueryResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct BindingQueryResult {
    pub exists: bool,
    pub routing_types: Option<RoutingTypes>,
    pub queue_names: Vec<Name>,
    pub auto_create_queues: bool,
    pub auto_create_addresses: bool,
    pub default_purge_on_no_consumers: bool,
    pub default_max_consumers: i32,
    pub default_exclusive: bool,
    pub default_last_value: bool,
    pub default_last_value_key: Option<String>,
    pub default_non_destructive: bool,
    pub default_consumers_before_dispatch: i32,
    pub default_delay_before_dispatch: i64,
}

/// `checkAutoCreate`'s outcome (`AutoCreateResult`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoCreate {
    Existed,
    Created,
    Updated,
    NotFound,
}

struct AddressEntry {
    info: AddressInfo,
    /// When the address was created, in epoch milliseconds.
    created_at: i64,
    /// When a queue or divert last left it (`bindingRemovedTimestamp`); -1
    /// never.
    binding_removed_at: i64,
    /// Found removable by the last reaper pass.
    swept: bool,
    /// Bound queues, in binding order.
    queues: Vec<Queue>,
    /// Round-robin position over the anycast queues (they share one routing
    /// name, the address, in `LocalQueueBinding`).
    anycast_next: AtomicUsize,
    diverts: Vec<Arc<Divert>>,
}

impl AddressEntry {
    fn new(info: AddressInfo) -> Self {
        Self {
            info,
            created_at: crate::message::now_millis(),
            binding_removed_at: -1,
            swept: false,
            queues: Vec::new(),
            anycast_next: AtomicUsize::new(0),
            diverts: Vec::new(),
        }
    }
}

#[derive(Default)]
struct State {
    addresses: HashMap<Name, AddressEntry>,
    queues: HashMap<Name, Queue>,
}

pub struct PostOffice {
    /// Handed to queues, which route through the post office when they
    /// move messages.
    me: Weak<PostOffice>,
    state: RwLock<State>,
    settings: Arc<dyn SettingsRepository>,
    filters: Arc<dyn FilterFactory>,
    ids: Arc<IdGenerator>,
    wildcard: WildcardConfig,
    duplicates: crate::duplicate::DuplicateIdCaches,
    paging: Arc<crate::paging::PagingManager>,
}

impl PostOffice {
    pub fn new(
        settings: Arc<dyn SettingsRepository>,
        filters: Arc<dyn FilterFactory>,
        ids: Arc<IdGenerator>,
        wildcard: WildcardConfig,
        duplicates: crate::duplicate::DuplicateIdCaches,
        paging: Arc<crate::paging::PagingManager>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            state: RwLock::default(),
            settings,
            filters,
            ids,
            wildcard,
            duplicates,
            paging,
        })
    }

    pub fn paging(&self) -> &Arc<crate::paging::PagingManager> {
        &self.paging
    }

    /// The paging store of an address (`PagingManager.getPageStore`).
    pub fn paging_store(&self, address: &str) -> Arc<crate::paging::PagingStore> {
        self.paging.store(address, || self.settings(address))
    }

    /// `processRoute` with `PagingStore.page`: the queues of an address that
    /// pages get the message in its pages instead of a reference; a full
    /// address under DROP drops it, under FAIL refuses it. Returns what is
    /// left to route, and the page syncs to wait for.
    pub fn page_routed(
        &self,
        routed: Routed,
        tx: Option<&crate::transaction::TxRef>,
    ) -> Result<(Routed, Vec<crate::storage::Pending>)> {
        let mut out = Vec::with_capacity(routed.len());
        let mut pending = Vec::new();
        for (r, queues) in routed {
            let mut by_address: Vec<(Name, Vec<Queue>)> = Vec::new();
            for q in queues {
                let a = q.config().address.clone();
                match by_address.iter_mut().find(|(b, _)| *b == a) {
                    Some((_, g)) => g.push(q),
                    None => by_address.push((a, vec![q])),
                }
            }
            let mut keep = Vec::new();
            for (address, group) in by_address {
                match self.paging_store(&address).page(&r.message, tx, &group)? {
                    crate::paging::Paged::No => keep.extend(group),
                    crate::paging::Paged::Yes(p) => pending.extend(p),
                    crate::paging::Paged::Dropped => {}
                }
            }
            if !keep.is_empty() {
                out.push((r, keep));
            }
        }
        Ok((out, pending))
    }

    /// A new queue's cursor on its address's pages.
    fn subscribe(&self, id: i64, config: &QueueConfig) -> Arc<crate::paging::PageSubscription> {
        self.paging_store(&config.address)
            .subscribe(id, config.durable && !config.temporary)
    }

    /// A duplicate-ID cache by name, of the given size
    /// (`getDuplicateIDCache(address, size)`).
    pub fn named_duplicate_cache(&self, name: &str, size: i32) -> crate::duplicate::CacheRef {
        self.duplicates.get(name, Some(size))
    }

    /// `duplicateIDCacheExists`.
    pub fn duplicate_cache_exists(&self, name: &str) -> bool {
        self.duplicates.exists(name)
    }

    /// `deleteDuplicateCache`.
    pub fn delete_duplicate_cache(&self, name: &str) {
        let ids = self.ids.clone();
        self.duplicates.delete(name, move || ids.next());
    }

    /// The duplicate-ID cache of `address` (`getDuplicateIDCache`).
    pub fn duplicate_cache(&self, address: &str) -> crate::duplicate::CacheRef {
        let size = self.settings(address).id_cache_size;
        self.duplicates.get(address, size)
    }

    pub fn settings(&self, address: &str) -> Arc<AddressSettings> {
        self.settings.get_match(address)
    }

    pub fn filters(&self) -> &dyn FilterFactory {
        &*self.filters
    }

    /// The broker's wildcard syntax (`getWildcardConfiguration`).
    pub fn wildcard(&self) -> &WildcardConfig {
        &self.wildcard
    }

    pub fn ids(&self) -> &Arc<IdGenerator> {
        &self.ids
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }

    fn io(e: saranga_journal::JournalError) -> BrokerError {
        BrokerError::new(saranga_core_protocol::ErrorType::IoError, e.to_string())
    }

    /// An address recovered from the bindings journal.
    pub fn reload_address(&self, info: AddressInfo) {
        self.write()
            .addresses
            .entry(info.name.clone())
            .or_insert_with(|| AddressEntry::new(info));
    }

    /// A queue recovered from the bindings journal, under its persisted ID
    /// (`initQueues`). A name taken already gets a `-N` suffix.
    pub fn reload_queue(&self, id: i64, mut config: QueueConfig) -> Result<Queue> {
        let filter = self.filters.create(config.filter_string.as_deref())?;
        let mut state = self.write();
        let mut n = 0;
        let original = config.name.clone();
        while state.queues.contains_key(&config.name) {
            config.name = format!("{original}-{n}").into();
            n += 1;
        }
        if config.name != original {
            tracing::warn!(
                "AMQ224076: Queue {original} is duplicated during reload. This queue will be renamed as {}",
                config.name
            );
        }
        let entry = state
            .addresses
            .entry(config.address.clone())
            .or_insert_with(|| {
                AddressEntry::new(AddressInfo {
                    id: self.ids.next(),
                    name: config.address.clone(),
                    routing_types: RoutingTypes::of(config.routing_type),
                    auto_created: true,
                    temporary: false,
                    internal: config.internal,
                })
            });
        entry.info.routing_types.add(config.routing_type);
        let sub = self.subscribe(id, &config);
        let queue = Queue::spawn(
            id,
            config,
            filter,
            self.ids.clone(),
            self.me.clone(),
            Some(sub),
        );
        entry.queues.push(queue.clone());
        state.queues.insert(queue.name().clone(), queue.clone());
        Ok(queue)
    }

    pub fn address_info(&self, name: &str) -> Option<AddressInfo> {
        self.read().addresses.get(name).map(|e| e.info.clone())
    }

    pub fn queue(&self, name: &str) -> Option<Queue> {
        self.read().queues.get(name).cloned()
    }

    pub fn queues(&self) -> Vec<Queue> {
        self.read().queues.values().cloned().collect()
    }

    pub fn addresses(&self) -> Vec<AddressInfo> {
        self.read()
            .addresses
            .values()
            .map(|e| e.info.clone())
            .collect()
    }

    /// `addOrUpdateAddressInfo`: creates the address, or replaces the routing
    /// types of an existing one (refusing to drop a type that still has
    /// queues).
    pub fn add_or_update_address(
        &self,
        name: &str,
        routing_types: RoutingTypes,
        auto_created: bool,
        temporary: bool,
    ) -> Result<AddressInfo> {
        let mut state = self.write();
        if let Some(entry) = state.addresses.get_mut(name) {
            if entry.info.routing_types != routing_types {
                for q in &entry.queues {
                    let t = q.config().routing_type;
                    if !routing_types.contains(t) {
                        return Err(BrokerError::invalid_routing_type_delete(t.name(), name));
                    }
                }
                let mut updated = entry.info.clone();
                updated.routing_types = routing_types;
                // updateAddressInfo: the binding is rewritten under a new ID.
                if !updated.temporary
                    && let Some(s) = self.ids.storage()
                {
                    updated.id = self.ids.next();
                    s.replace_binding(
                        entry.info.id,
                        updated.id,
                        crate::storage::ids::ADDRESS_BINDING_RECORD,
                        crate::recovery::address_binding(&updated),
                    )
                    .map_err(Self::io)?;
                }
                entry.info = updated;
            }
            return Ok(entry.info.clone());
        }
        let info = AddressInfo {
            id: self.ids.next(),
            name: name.into(),
            routing_types,
            auto_created,
            temporary,
            internal: false,
        };
        if !temporary && let Some(s) = self.ids.storage() {
            crate::recovery::store_address(s, &info).map_err(Self::io)?;
        }
        state
            .addresses
            .insert(info.name.clone(), AddressEntry::new(info.clone()));
        Ok(info)
    }

    /// The auto-created address of an internal queue
    /// (`AddressInfo.setInternal`).
    fn add_internal_address(&self, name: &str, routing_type: RoutingType) -> Result<AddressInfo> {
        let mut state = self.write();
        if let Some(entry) = state.addresses.get(name) {
            return Ok(entry.info.clone());
        }
        let info = AddressInfo {
            id: self.ids.next(),
            name: name.into(),
            routing_types: RoutingTypes::of(routing_type),
            auto_created: true,
            temporary: false,
            internal: true,
        };
        if let Some(s) = self.ids.storage() {
            crate::recovery::store_address(s, &info).map_err(Self::io)?;
        }
        state
            .addresses
            .insert(info.name.clone(), AddressEntry::new(info.clone()));
        Ok(info)
    }

    /// Adds a routing type to an address, creating it if needed (the
    /// auto-create paths).
    fn add_routing_type(
        &self,
        name: &str,
        routing_type: RoutingType,
        temporary: bool,
    ) -> Result<AddressInfo> {
        let mut types = self
            .address_info(name)
            .map(|i| i.routing_types)
            .unwrap_or_default();
        types.add(routing_type);
        self.add_or_update_address(name, types, true, temporary)
    }

    /// Removes an address that has no queues (`removeAddressInfo`).
    pub fn remove_address(&self, name: &str) -> bool {
        let mut state = self.write();
        match state.addresses.get(name) {
            Some(entry) if entry.queues.is_empty() && entry.diverts.is_empty() => {
                if !entry.info.temporary
                    && let Some(s) = self.ids.storage()
                    && let Err(e) = s.delete_binding(entry.info.id)
                {
                    tracing::warn!("removing address {name}: {e}");
                }
                state.addresses.remove(name);
                true
            }
            _ => false,
        }
    }

    /// `ActiveMQServerImpl.createQueue`. With `ignore_if_exists` an existing
    /// queue of that name is returned instead of an error.
    pub fn create_queue(&self, mut spec: QueueSpec, ignore_if_exists: bool) -> Result<Queue> {
        if spec.name.is_empty() {
            return Err(BrokerError::invalid_queue_name(&spec.name));
        }
        if let Some((address, q)) = self
            .read()
            .queues
            .get(&spec.name)
            .map(|q| (q.config().address.clone(), q.clone()))
        {
            return if ignore_if_exists {
                // An auto-delete that was under way stops.
                q.set_swept(false);
                Ok(q)
            } else {
                Err(BrokerError::queue_already_exists(&spec.name, &address))
            };
        }
        let settings = self.settings(&spec.address);
        spec.apply_dynamic_defaults(&settings);
        let auto_create_address = spec.auto_create_address == Some(true) || spec.temporary;
        let config = spec.resolve(&settings);

        match self.address_info(&config.address) {
            None if auto_create_address && config.internal => {
                self.add_internal_address(&config.address, config.routing_type)?;
            }
            None if auto_create_address => {
                self.add_or_update_address(
                    &config.address,
                    RoutingTypes::of(config.routing_type),
                    true,
                    config.temporary,
                )?;
            }
            None => return Err(BrokerError::address_does_not_exist(&config.address)),
            Some(info) if !info.routing_types.contains(config.routing_type) => {
                if auto_create_address {
                    self.add_routing_type(&config.address, config.routing_type, info.temporary)?;
                } else {
                    return Err(BrokerError::invalid_routing_type_for_address(
                        config.routing_type.name(),
                        &config.address,
                        &info.routing_types.describe(),
                    ));
                }
            }
            Some(_) => {}
        }

        let filter = self.filters.create(config.filter_string.as_deref())?;
        let mut state = self.write();
        // Re-check under the write lock; another session may have won.
        if let Some(q) = state.queues.get(&config.name) {
            return if ignore_if_exists {
                Ok(q.clone())
            } else {
                Err(BrokerError::queue_already_exists(
                    &config.name,
                    &q.config().address,
                ))
            };
        }
        let Some(entry) = state.addresses.get_mut(&config.address) else {
            return Err(BrokerError::address_does_not_exist(&config.address));
        };
        let id = self.ids.next();
        if config.durable
            && !config.temporary
            && let Some(s) = self.ids.storage()
        {
            s.add_binding(
                id,
                crate::storage::ids::QUEUE_BINDING_RECORD,
                crate::recovery::queue_binding(&config),
            )
            .map_err(Self::io)?;
        }
        let sub = self.subscribe(id, &config);
        let queue = Queue::spawn(
            id,
            config,
            filter,
            self.ids.clone(),
            self.me.clone(),
            Some(sub),
        );
        entry.queues.push(queue.clone());
        state.queues.insert(queue.name().clone(), queue.clone());
        Ok(queue)
    }

    /// `PostOfficeImpl.updateQueue` with `forceUpdate`, as configuration
    /// reload calls it: the queue named in `spec` takes the settings `spec`
    /// resolves to. Name, address, durability and the kind of queue stay.
    /// `Ok(None)` if there is no such queue.
    pub fn update_queue(&self, spec: QueueSpec) -> Result<Option<Queue>> {
        let Some(queue) = self.queue(&spec.name) else {
            return Ok(None);
        };
        let old = queue.config();
        let wanted = spec.resolve(&self.settings(&old.address));
        if wanted.max_consumers != -1 && queue.consumer_count() > wanted.max_consumers {
            return Err(BrokerError::invalid_max_consumers_update(
                &old.name,
                wanted.max_consumers,
                queue.consumer_count(),
            ));
        }
        let info = self.address_info(&old.address);
        if let Some(info) = &info
            && !info.routing_types.contains(wanted.routing_type)
        {
            return Err(BrokerError::invalid_routing_type_update(
                &old.name,
                wanted.routing_type.name(),
                &old.address,
                &info.routing_types.describe(),
            ));
        }
        let mut new = (*old).clone();
        new.max_consumers = wanted.max_consumers;
        new.routing_type = wanted.routing_type;
        new.purge_on_no_consumers = wanted.purge_on_no_consumers;
        new.enabled = wanted.enabled;
        new.exclusive = wanted.exclusive;
        new.group_rebalance = wanted.group_rebalance;
        new.group_rebalance_pause_dispatch = wanted.group_rebalance_pause_dispatch;
        new.group_buckets = wanted.group_buckets;
        new.group_first_key = wanted.group_first_key;
        new.non_destructive = wanted.non_destructive;
        new.consumers_before_dispatch = wanted.consumers_before_dispatch;
        new.delay_before_dispatch = wanted.delay_before_dispatch;
        new.configuration_managed = wanted.configuration_managed;
        new.user = wanted.user;
        new.ring_size = wanted.ring_size;
        // An empty filter is no filter (`FilterImpl.createFilter("")`). The
        // new one routes, but the queue's configuration keeps the old text
        // (`QueueImpl.setFilter`).
        let filter_string = wanted.filter_string.filter(|f| !f.trim().is_empty());
        let filter = if filter_string == queue.filter_string() {
            None
        } else {
            let compiled = self.filters.create(filter_string.as_deref())?;
            Some((filter_string, compiled))
        };
        if new == *old && filter.is_none() {
            return Ok(Some(queue));
        }
        if new.durable
            && !new.temporary
            && let Some(s) = self.ids.storage()
        {
            // The filter the queue routes with. Artemis journals the stale
            // one from its QueueConfiguration; see COMPAT.md.
            let mut journalled = new.clone();
            journalled.filter_string = match &filter {
                Some((text, _)) => text.clone(),
                None => queue.filter_string(),
            };
            s.update_binding(
                queue.id(),
                crate::storage::ids::QUEUE_BINDING_RECORD,
                crate::recovery::queue_binding(&journalled),
            )
            .map_err(Self::io)?;
        }
        queue.update(new, filter);
        Ok(Some(queue))
    }

    /// `ActiveMQServerImpl.destroyQueue`. With `check_consumers` a queue that
    /// still has consumers is refused. An auto-created address left without
    /// queues is removed when `auto_delete_address`.
    pub fn delete_queue(
        &self,
        name: &str,
        check_consumers: bool,
        auto_delete_address: bool,
    ) -> Result<()> {
        let queue = self
            .queue(name)
            .ok_or_else(|| BrokerError::no_such_queue(name))?;
        if check_consumers && queue.consumer_count() > 0 {
            return Err(BrokerError::cannot_delete_queue_with_consumers(
                name,
                queue.consumer_count(),
            ));
        }
        let address = queue.config().address.clone();
        {
            let mut state = self.write();
            state.queues.remove(name);
            if let Some(entry) = state.addresses.get_mut(&address) {
                entry.queues.retain(|q| q != &queue);
                entry.binding_removed_at = crate::message::now_millis();
            }
        }
        if queue.is_durable()
            && let Some(s) = self.ids.storage()
            && let Err(e) = s.delete_binding(queue.id())
        {
            tracing::warn!("deleting queue {name}: {e}");
        }
        queue.unsubscribe();
        queue.shutdown();
        if auto_delete_address && self.address_auto_deletable(&address) {
            self.remove_address(&address);
        }
        Ok(())
    }

    /// Whether deleting a queue takes its address along: the address was
    /// auto-created, nothing is bound to it any more and it is not to wait
    /// (`auto-delete-addresses-delay` 0).
    pub fn address_auto_deletable(&self, address: &str) -> bool {
        self.address_info(address).is_some_and(|i| i.auto_created)
            && !self.has_bindings(address)
            && self.settings(address).auto_delete_addresses_delay == 0
    }

    /// `PostOfficeImpl.reapAddresses`: removes the auto-delete queues and
    /// auto-created addresses nothing uses any more. A queue or address
    /// must be found removable on two passes in a row, except on the
    /// `initial` pass at startup, which removes empty ones at once.
    pub fn reap(&self, initial: bool) {
        let now = crate::message::now_millis();
        for q in self.queues() {
            let c = q.config();
            let s = self.settings(&c.address);
            let delay_passed = if s.auto_delete_queues_skip_usage_check {
                now - q.created_at() >= c.auto_delete_delay
            } else {
                now - q.consumer_removed_at() >= c.auto_delete_delay
            };
            let used = q.was_used()
                || q.consumer_removed_at() != -1
                || s.auto_delete_queues_skip_usage_check;
            let eligible = c.auto_delete
                && !q.is_deleted()
                && q.consumer_count() == 0
                && (initial || delay_passed)
                && (c.auto_delete_message_count == -1
                    || q.message_count() <= c.auto_delete_message_count)
                && (initial || used);
            if !eligible {
                q.set_swept(false);
                continue;
            }
            let valid_initial =
                initial && q.message_count() == 0 && !self.paging_store(&c.address).is_paging();
            if valid_initial || q.swept() {
                tracing::info!(
                    "AMQ224112: Auto removing queue {} with queueID={} on address={}",
                    q.name(),
                    q.id(),
                    c.address
                );
                if let Err(e) = self.delete_queue(q.name(), true, false) {
                    tracing::warn!(
                        "AMQ222241: Unable to remove auto-created queue {}: {e}",
                        q.name()
                    );
                }
            } else {
                q.set_swept(true);
            }
        }
        let names: Vec<Name> = self.read().addresses.keys().cloned().collect();
        for name in names {
            let s = self.settings(&name);
            let remove = {
                let mut state = self.write();
                let Some(entry) = state.addresses.get_mut(&name) else {
                    continue;
                };
                let delay_passed = if s.auto_delete_addresses_skip_usage_check {
                    now - entry.created_at >= s.auto_delete_addresses_delay
                } else {
                    now - entry.binding_removed_at >= s.auto_delete_addresses_delay
                };
                let used =
                    entry.binding_removed_at != -1 || s.auto_delete_addresses_skip_usage_check;
                let removable = s.auto_delete_addresses
                    && entry.info.auto_created
                    && entry.queues.is_empty()
                    && entry.diverts.is_empty()
                    && (initial || used)
                    && (initial || delay_passed);
                if !removable {
                    entry.swept = false;
                    false
                } else if initial || entry.swept {
                    true
                } else {
                    entry.swept = true;
                    false
                }
            };
            if remove {
                tracing::info!("AMQ224113: Auto removing address {name}");
                self.remove_address(&name);
            }
        }
    }

    /// `ActiveMQServerImpl.queueQuery`.
    pub fn queue_query(&self, name: &str) -> QueueQueryResult {
        let real = queue_name_of(name);
        if let Some(q) = self.queue(real) {
            let c = q.config();
            let s = self.settings(&c.address);
            return QueueQueryResult {
                name: c.name.clone(),
                address: c.address.clone(),
                durable: c.durable,
                temporary: c.temporary,
                filter_string: c.filter_string.clone(),
                consumer_count: q.consumer_count(),
                message_count: q.message_count(),
                exists: true,
                auto_create_queues: s.auto_create_queues,
                auto_created: c.auto_created,
                purge_on_no_consumers: c.purge_on_no_consumers,
                routing_type: c.routing_type,
                max_consumers: c.max_consumers,
                exclusive: Some(c.exclusive),
                group_rebalance: Some(c.group_rebalance),
                group_rebalance_pause_dispatch: Some(c.group_rebalance_pause_dispatch),
                group_buckets: Some(c.group_buckets),
                group_first_key: c.group_first_key.clone(),
                last_value: Some(c.last_value),
                last_value_key: c.last_value_key.clone(),
                non_destructive: Some(c.non_destructive),
                consumers_before_dispatch: Some(c.consumers_before_dispatch),
                delay_before_dispatch: Some(c.delay_before_dispatch),
                auto_delete: Some(c.auto_delete),
                auto_delete_delay: Some(c.auto_delete_delay),
                auto_delete_message_count: Some(c.auto_delete_message_count),
                default_consumer_window_size: Some(s.default_consumer_window_size),
                ring_size: Some(c.ring_size),
                enabled: Some(c.enabled),
                configuration_managed: Some(c.configuration_managed),
            };
        }
        let address = split_fqqn(name).0;
        let s = self.settings(address);
        let mut spec = QueueSpec::new(real, address);
        spec.apply_dynamic_defaults(&s);
        QueueQueryResult {
            name: spec.name,
            address: spec.address,
            durable: spec.durable,
            temporary: spec.temporary,
            filter_string: None,
            consumer_count: 0,
            message_count: 0,
            exists: false,
            auto_create_queues: s.auto_create_queues,
            auto_created: false,
            purge_on_no_consumers: spec.purge_on_no_consumers.unwrap_or(false),
            routing_type: spec.routing_type.unwrap_or(RoutingType::Multicast),
            max_consumers: spec.max_consumers.unwrap_or(-1),
            exclusive: spec.exclusive,
            group_rebalance: spec.group_rebalance,
            group_rebalance_pause_dispatch: spec.group_rebalance_pause_dispatch,
            group_buckets: spec.group_buckets,
            group_first_key: spec.group_first_key,
            last_value: spec.last_value,
            last_value_key: spec.last_value_key,
            non_destructive: spec.non_destructive,
            consumers_before_dispatch: spec.consumers_before_dispatch,
            delay_before_dispatch: spec.delay_before_dispatch,
            auto_delete: spec.auto_delete,
            auto_delete_delay: spec.auto_delete_delay,
            auto_delete_message_count: spec.auto_delete_message_count,
            default_consumer_window_size: Some(s.default_consumer_window_size),
            ring_size: spec.ring_size,
            enabled: None,
            configuration_managed: Some(false),
        }
    }

    /// `ActiveMQServerImpl.bindingQuery`. Older clients (`!new_fqqn`) asking
    /// with an FQQN get FQQN queue names back.
    pub fn binding_query(&self, address: &str, new_fqqn: bool) -> BindingQueryResult {
        let (real, fqqn_queue) = split_fqqn(address);
        let s = self.settings(real);
        let state = self.read();
        let entry = state.addresses.get(real);
        let mut queue_names: Vec<Name> = Vec::new();
        let mut push = |q: &Queue| {
            queue_names.push(if !new_fqqn && fqqn_queue.is_some() {
                format!("{real}{FQQN_SEPARATOR}{}", q.name()).into()
            } else {
                q.name().clone()
            });
        };
        if self.wildcard.is_wild(real) {
            for e in state.addresses.values() {
                if self.wildcard.matches(&e.info.name, real) {
                    e.queues.iter().for_each(&mut push);
                }
            }
        } else if let Some(e) = entry {
            e.queues.iter().for_each(&mut push);
        }
        BindingQueryResult {
            exists: entry.is_some(),
            routing_types: entry.map(|e| e.info.routing_types),
            queue_names,
            auto_create_queues: s.auto_create_queues,
            auto_create_addresses: s.auto_create_addresses,
            default_purge_on_no_consumers: s.default_purge_on_no_consumers,
            default_max_consumers: s.default_max_consumers,
            default_exclusive: s.default_exclusive_queue,
            default_last_value: s.default_last_value_queue,
            default_last_value_key: s.default_last_value_key.clone(),
            default_non_destructive: s.default_non_destructive,
            default_consumers_before_dispatch: s.default_consumers_before_dispatch,
            default_delay_before_dispatch: s.default_delay_before_dispatch,
        }
    }

    /// `ServerSessionImpl.checkAutoCreate`, for a send to `address` (which
    /// may be an FQQN) with the message's routing type.
    /// `ServerSessionImpl.checkAutoCreate`: makes sure the address (and,
    /// for anycast or a fully qualified name, the queue) exists, creating
    /// what the settings allow.
    pub fn check_auto_create(
        &self,
        address: &str,
        routing_type: Option<RoutingType>,
        creator: Creator<'_>,
    ) -> Result<AutoCreate> {
        use saranga_config::CheckType;
        let Some(routing_type) = routing_type else {
            return Ok(AutoCreate::Existed);
        };
        let (real, fqqn_queue) = split_fqqn(address);
        let queue_name = fqqn_queue.unwrap_or(real);
        let s = self.settings(real);
        let mut result = match self.address_info(real) {
            None if s.auto_create_addresses => {
                creator.check(real, None, CheckType::CreateAddress)?;
                self.add_routing_type(real, routing_type, false)?;
                AutoCreate::Created
            }
            None => return Ok(AutoCreate::NotFound),
            Some(info) if info.routing_types.contains(routing_type) => AutoCreate::Existed,
            Some(info) if s.auto_create_addresses => {
                creator.check(real, None, CheckType::CreateAddress)?;
                self.add_routing_type(real, routing_type, info.temporary)?;
                AutoCreate::Updated
            }
            Some(_) => return Ok(AutoCreate::NotFound),
        };
        if routing_type == RoutingType::Anycast || fqqn_queue.is_some() {
            match self.queue(queue_name) {
                None => {
                    if fqqn_queue.is_none() && self.matching_queue(real, routing_type).is_some() {
                        result = AutoCreate::Existed;
                    } else if s.auto_create_queues {
                        let mut spec = QueueSpec::new(queue_name, real);
                        spec.routing_type = Some(routing_type);
                        spec.auto_created = true;
                        spec.user = creator.user.map(str::to_owned);
                        let check = if spec.durable {
                            CheckType::CreateDurableQueue
                        } else {
                            CheckType::CreateNonDurableQueue
                        };
                        creator.check(real, Some(queue_name), check)?;
                        match self.create_queue(spec, true) {
                            Ok(_) => result = AutoCreate::Created,
                            Err(e) => return Err(e),
                        }
                    } else {
                        return Ok(AutoCreate::NotFound);
                    }
                }
                Some(q) => {
                    if q.config().routing_type != RoutingType::Anycast && fqqn_queue.is_none() {
                        return Ok(AutoCreate::NotFound);
                    }
                }
            }
        }
        Ok(result)
    }

    /// `SimpleAddressManager.getMatchingQueue`: the queue named after the
    /// address if it has the routing type, else the first bound queue that
    /// does.
    fn matching_queue(&self, address: &str, routing_type: RoutingType) -> Option<Queue> {
        let state = self.read();
        let entry = state.addresses.get(address)?;
        entry
            .queues
            .iter()
            .find(|q| &**q.name() == address && q.config().routing_type == routing_type)
            .or_else(|| {
                entry
                    .queues
                    .iter()
                    .find(|q| q.config().routing_type == routing_type)
            })
            .cloned()
    }

    /// `PostOfficeImpl.route` without delivery: where a message sent to
    /// `address` goes, auto-creating what the settings allow. Each entry is
    /// a message and its queues: the message itself, then any copies
    /// diverts made. An empty result means no bindings (the message is
    /// dropped).
    pub fn route(
        &self,
        message: &MessageRef,
        address: &str,
        routing_type: Option<RoutingType>,
        creator: Creator<'_>,
    ) -> Result<Routed> {
        let mut out = Vec::new();
        self.route_into(
            message,
            address,
            routing_type,
            Hop::client(creator),
            &mut out,
        )?;
        Ok(out)
    }

    /// `PostOfficeImpl.route` for a message the broker itself sends (a
    /// dead-letter or expiry copy): no auto-creation, and no bindings means
    /// no queues.
    pub fn route_internal(&self, message: &MessageRef, address: &str) -> Routed {
        let mut out = Vec::new();
        let routing_type = routing_type(message.message.core());
        if let Err(e) = self.route_into(message, address, routing_type, Hop::internal(), &mut out) {
            tracing::debug!("routing to {address}: {e}");
        }
        out
    }

    fn route_into(
        &self,
        message: &MessageRef,
        address: &str,
        routing_type: Option<RoutingType>,
        hop: Hop<'_>,
        out: &mut Routed,
    ) -> Result<()> {
        let Hop {
            auto_create,
            depth,
            to_dla,
            creator,
        } = hop;
        let (real, _) = split_fqqn(address);
        // checkAddress
        if auto_create
            && self.address_info(real).is_none()
            && self.check_auto_create(address, routing_type, creator)? == AutoCreate::NotFound
        {
            return Err(BrokerError::address_does_not_exist(real));
        }
        // simpleRoute
        let mut bindings = self.bindings_route(message, address);
        if bindings.is_none() && auto_create {
            if self.check_auto_create(address, routing_type, creator)? == AutoCreate::NotFound {
                return Err(BrokerError::address_does_not_exist(real));
            }
            bindings = self.bindings_route(message, address);
        }
        let Some((queues, diverts)) = bindings else {
            return self.maybe_send_to_dla(message, real, to_dla, out);
        };
        if !queues.is_empty() {
            out.push((message.clone(), queues));
        }
        if depth >= MAX_DIVERT_DEPTH {
            if !diverts.is_empty() {
                tracing::warn!(
                    "diverts from {real} nest deeper than {MAX_DIVERT_DEPTH}; not followed"
                );
            }
            return Ok(());
        }
        // DivertImpl.route
        for d in diverts {
            for forward in &d.forwarding {
                let copy = if forward == real {
                    message.clone()
                } else {
                    let now = crate::message::now_millis();
                    let mut core = d.copy(message.message.core(), self.ids.next(), forward);
                    let s = self.settings(forward);
                    crate::message::apply_expiry_delay(&mut core, &s, now);
                    let mut r = MessageRef::new(message.message.copy(core));
                    r.scheduled_time = crate::message::scheduled_delivery_time(r.message.core());
                    r
                };
                let rt = routing_type_of(copy.message.core());
                self.route_into(
                    &copy,
                    forward,
                    rt,
                    Hop {
                        depth: depth + 1,
                        to_dla: false,
                        ..hop
                    },
                    out,
                )?;
            }
        }
        Ok(())
    }

    /// `maybeSendToDLA`: with `send-to-dla-on-no-route`, a message nothing
    /// is bound to goes to the address's dead-letter address instead (once).
    fn maybe_send_to_dla(
        &self,
        message: &MessageRef,
        address: &str,
        to_dla: bool,
        out: &mut Routed,
    ) -> Result<()> {
        let s = self.settings(address);
        if to_dla || !s.send_to_dla_on_no_route {
            return Ok(());
        }
        let Some(dla) = s.dead_letter_address.clone() else {
            tracing::warn!(
                "AMQ222056: Did not route to any bindings for address {address} and sendToDLAOnNoRoute is true but there is no DLA configured for the address, the message will be ignored."
            );
            return Ok(());
        };
        use crate::message as m;
        use saranga_buffer::{PropertyValue, SimpleString};
        let original = message.message.core();
        let mut core = original.clone();
        let p = &mut core.properties;
        // referenceOriginalMessage(message, null)
        p.insert(m::HDR_ORIGINAL_QUEUE.into(), PropertyValue::Null);
        p.insert(
            m::HDR_ORIGINAL_ADDRESS.into(),
            PropertyValue::String(original.address.clone().unwrap_or_default()),
        );
        p.insert(
            m::HDR_ORIG_MESSAGE_ID.into(),
            PropertyValue::Long(original.message_id),
        );
        if let Some(t) = routing_type(original) {
            p.insert(
                m::HDR_ORIG_ROUTING_TYPE.into(),
                PropertyValue::Byte(t.to_byte()),
            );
        }
        p.remove(&SimpleString::from(m::HDR_ROUTING_TYPE));
        core.expiration = 0;
        core.address = Some(SimpleString::from(dla.as_str()));
        let now = m::now_millis();
        m::apply_expiry_delay(&mut core, &self.settings(&dla), now);
        // The same message, rerouted: a large body is shared.
        let mut r = MessageRef::new(message.message.with_core(core));
        r.scheduled_time = message.scheduled_time;
        self.route_into(
            &r,
            &dla,
            None,
            Hop {
                to_dla: true,
                ..Hop::internal()
            },
            out,
        )
    }

    /// Whether anything is bound to `address`
    /// (`lookupBindingsForAddress` not empty).
    pub fn has_bindings(&self, address: &str) -> bool {
        self.read()
            .addresses
            .get(address)
            .is_some_and(|e| !e.queues.is_empty() || !e.diverts.is_empty())
    }

    /// The diverts bound to addresses.
    pub fn diverts(&self) -> Vec<Arc<Divert>> {
        self.read()
            .addresses
            .values()
            .flat_map(|e| e.diverts.iter().cloned())
            .collect()
    }

    pub fn divert(&self, name: &str) -> Option<Arc<Divert>> {
        self.diverts().into_iter().find(|d| *d.name == *name)
    }

    /// `destroyDivert`: unbinds a divert. False if there is none.
    pub fn destroy_divert(&self, name: &str) -> bool {
        let mut state = self.write();
        for entry in state.addresses.values_mut() {
            if let Some(i) = entry.diverts.iter().position(|d| *d.name == *name) {
                entry.diverts.remove(i);
                entry.binding_removed_at = crate::message::now_millis();
                return true;
            }
        }
        false
    }

    /// Swaps the address settings' match rules and gives every page store
    /// its new settings (`HierarchicalRepository.swap` and the paging
    /// manager's change listener).
    pub fn swap_settings(
        &self,
        rules: saranga_config::HierarchicalRepository<saranga_config::AddressSettings>,
    ) {
        if !self.settings.swap(rules) {
            return;
        }
        for store in self.paging.stores() {
            store.apply_settings(self.settings(store.address()));
        }
    }

    /// `deployDivert`: binds a divert to its address, which must exist. A
    /// transformer must be among `transformers`.
    pub fn deploy_divert(
        &self,
        d: &DivertDeployment,
        transformers: &HashMap<String, TransformerFactory>,
    ) -> Result<()> {
        let transformer = match &d.transformer {
            None => None,
            Some((class, properties)) => match transformers.get(class) {
                Some(make) => Some(make(properties)),
                None => {
                    return Err(BrokerError::internal(format!(
                        "no transformer is registered as {class}"
                    )));
                }
            },
        };
        let divert = Divert {
            name: d.name.as_str().into(),
            routing_name: d
                .routing_name
                .clone()
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
                .into(),
            address: d.address.as_str().into(),
            forwarding: d
                .forwarding_address
                .split(',')
                .map(|a| a.trim().to_owned())
                .filter(|a| !a.is_empty())
                .collect(),
            exclusive: d.exclusive,
            filter: self.filters.create(d.filter.as_deref())?,
            routing_type: d.routing_type,
            transformer,
        };
        let mut state = self.write();
        if state
            .addresses
            .values()
            .any(|e| e.diverts.iter().any(|d| d.name == divert.name))
        {
            tracing::warn!("AMQ222017: Divert {} already exists", divert.name);
            return Ok(());
        }
        let Some(entry) = state.addresses.get_mut(&*divert.address) else {
            return Err(BrokerError::address_does_not_exist(&divert.address));
        };
        entry.diverts.push(Arc::new(divert));
        Ok(())
    }

    /// Routes through the bindings of `address` and of every wildcard
    /// address that matches it (`BindingsImpl.route`): the queues, and the
    /// diverts to follow. `None` when there are no local bindings at all.
    fn bindings_route(
        &self,
        message: &MessageRef,
        address: &str,
    ) -> Option<(Vec<Queue>, Vec<Arc<Divert>>)> {
        let (real, fqqn_queue) = split_fqqn(address);
        let state = self.read();
        let mut entries: Vec<&AddressEntry> = state.addresses.get(real).into_iter().collect();
        if self.wildcard.routing_enabled && !self.wildcard.is_wild(real) {
            entries.extend(state.addresses.values().filter(|e| {
                self.wildcard.is_wild(&e.info.name) && self.wildcard.matches(real, &e.info.name)
            }));
        }
        if entries
            .iter()
            .all(|e| e.queues.is_empty() && e.diverts.is_empty())
        {
            return None;
        }
        let core = message.message.core();
        let divert_matches = |d: &Divert| d.filter.as_ref().is_none_or(|f| f.matches(core));
        // Exclusive diverts take the message from everything else.
        if fqqn_queue.is_none() {
            let exclusive: Vec<Arc<Divert>> = entries
                .iter()
                .flat_map(|e| e.diverts.iter())
                .filter(|d| d.exclusive && divert_matches(d))
                .cloned()
                .collect();
            if !exclusive.is_empty() {
                return Some((Vec::new(), exclusive));
            }
        }
        let matches = |q: &Queue| {
            fqqn_queue.is_none_or(|n| &**q.name() == n)
                && q.filter().is_none_or(|f| f.matches(core))
        };
        let accepts = |q: &Queue| {
            let c = q.config();
            c.enabled && !(c.purge_on_no_consumers && q.consumer_count() == 0)
        };
        let mut targets = Vec::new();
        let mut diverts = Vec::new();
        for entry in entries {
            // Every multicast queue is its own routing group.
            for q in &entry.queues {
                if q.config().routing_type == RoutingType::Multicast && matches(q) && accepts(q) {
                    targets.push(q.clone());
                }
            }
            // The anycast queues form one group: the next one, round robin,
            // whose filter matches (`getNextBinding`).
            let anycast: Vec<&Queue> = entry
                .queues
                .iter()
                .filter(|q| q.config().routing_type == RoutingType::Anycast)
                .collect();
            if !anycast.is_empty() {
                let start = entry.anycast_next.load(Ordering::Relaxed) % anycast.len();
                let chosen = (0..anycast.len())
                    .map(|i| (start + i) % anycast.len())
                    .find(|&i| matches(anycast[i]));
                if let Some(i) = chosen {
                    entry
                        .anycast_next
                        .store((i + 1) % anycast.len(), Ordering::Relaxed);
                    if accepts(anycast[i]) {
                        targets.push(anycast[i].clone());
                    }
                }
            }
            // Non-exclusive diverts route a copy as well.
            if fqqn_queue.is_none() {
                diverts.extend(
                    entry
                        .diverts
                        .iter()
                        .filter(|d| !d.exclusive && divert_matches(d))
                        .cloned(),
                );
            }
        }
        Some((targets, diverts))
    }
}

/// One step of a route: whether addresses and queues may be auto-created
/// (a client's send may, a broker-made copy may not), how many diverts
/// deep it is, and whether it already goes to a dead-letter address.
#[derive(Clone, Copy)]
struct Hop<'a> {
    auto_create: bool,
    depth: usize,
    to_dla: bool,
    creator: Creator<'a>,
}

impl<'a> Hop<'a> {
    fn client(creator: Creator<'a>) -> Self {
        Self {
            auto_create: true,
            depth: 0,
            to_dla: false,
            creator,
        }
    }

    fn internal() -> Self {
        Self {
            auto_create: false,
            ..Self::client(Creator::default())
        }
    }
}

/// How deep diverts may forward into further diverts.
const MAX_DIVERT_DEPTH: usize = 16;

/// Where a send went: each message with its queues.
pub type Routed = Vec<(MessageRef, Vec<Queue>)>;

fn routing_type_of(message: &saranga_message::CoreMessage) -> Option<RoutingType> {
    routing_type(message)
}

/// A divert (`DivertImpl`): a binding on `address` that sends a copy of
/// each matching message on to the forwarding addresses.
pub struct Divert {
    pub name: Name,
    pub routing_name: Name,
    pub address: Name,
    pub forwarding: Vec<String>,
    pub exclusive: bool,
    pub filter: Option<FilterRef>,
    pub routing_type: DivertRoutingType,
    pub transformer: Option<Arc<dyn Transformer>>,
}

/// `Transformer`: rewrites the copies a divert makes. Transformers are Rust
/// plugins: the embedding application registers a factory under the class
/// name `broker.xml` gives (`BrokerConfig::transformers`).
pub trait Transformer: Send + Sync {
    fn transform(&self, message: saranga_message::CoreMessage) -> saranga_message::CoreMessage;
}

/// Makes a transformer from its configured properties (`Transformer.init`).
pub type TransformerFactory =
    Arc<dyn Fn(&HashMap<String, String>) -> Arc<dyn Transformer> + Send + Sync>;

/// A divert to deploy (`DivertConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivertDeployment {
    pub name: String,
    pub routing_name: Option<String>,
    pub address: String,
    /// One address, or several separated by commas.
    pub forwarding_address: String,
    pub exclusive: bool,
    pub filter: Option<String>,
    pub routing_type: DivertRoutingType,
    /// The transformer's class name and properties.
    pub transformer: Option<(String, HashMap<String, String>)>,
}

/// `ComponentConfigurationRoutingType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivertRoutingType {
    Multicast,
    Anycast,
    Strip,
    Pass,
}

impl Divert {
    /// The copy `DivertImpl.route` sends on: a new ID, the original's
    /// address, queue (the divert) and ID recorded, its expiration kept, and
    /// its routing type set by the divert.
    fn copy(
        &self,
        original: &saranga_message::CoreMessage,
        new_id: i64,
        forward: &str,
    ) -> saranga_message::CoreMessage {
        use crate::message as m;
        use saranga_buffer::{PropertyValue, SimpleString};
        let mut copy = original.clone();
        copy.message_id = new_id;
        let p = &mut copy.properties;
        p.insert(
            m::HDR_ORIGINAL_QUEUE.into(),
            PropertyValue::String(SimpleString::from(&*self.name)),
        );
        p.insert(
            m::HDR_ORIGINAL_ADDRESS.into(),
            PropertyValue::String(original.address.clone().unwrap_or_default()),
        );
        p.insert(
            m::HDR_ORIG_MESSAGE_ID.into(),
            PropertyValue::Long(original.message_id),
        );
        if let Some(t) = routing_type(original) {
            p.insert(
                m::HDR_ORIG_ROUTING_TYPE.into(),
                PropertyValue::Byte(t.to_byte()),
            );
        }
        copy.address = Some(SimpleString::from(forward));
        copy.expiration = original.expiration;
        let key = SimpleString::from(m::HDR_ROUTING_TYPE);
        match self.routing_type {
            DivertRoutingType::Anycast => {
                copy.properties
                    .insert(key, PropertyValue::Byte(RoutingType::Anycast.to_byte()));
            }
            DivertRoutingType::Multicast => {
                copy.properties
                    .insert(key, PropertyValue::Byte(RoutingType::Multicast.to_byte()));
            }
            DivertRoutingType::Strip => {
                copy.properties.remove(&key);
            }
            DivertRoutingType::Pass => {}
        }
        match &self.transformer {
            Some(t) => t.transform(copy),
            None => copy,
        }
    }
}

/// The JMS selector engine (`FilterImpl`).
pub struct SelectorFilters;

impl crate::queue::MessageFilter for saranga_filter::Filter {
    fn matches(&self, message: &saranga_message::CoreMessage) -> bool {
        saranga_filter::Filter::matches(self, message)
    }

    fn text(&self) -> &str {
        saranga_filter::Filter::text(self)
    }
}

impl FilterFactory for SelectorFilters {
    fn create(&self, text: Option<&str>) -> Result<Option<FilterRef>> {
        match saranga_filter::Filter::create(text) {
            Ok(f) => Ok(f.map(|f| Arc::new(f) as FilterRef)),
            Err(_) => Err(BrokerError::invalid_filter(text.unwrap_or_default())),
        }
    }
}
