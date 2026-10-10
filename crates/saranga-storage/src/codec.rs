//! Bodies of the user records the storage manager writes into the bindings
//! and message journals. Ported from
//! `core/persistence/impl/journal/codec/*`, `core/persistence/config/*`,
//! `BatchingIDGenerator.IDCounterEncoding`, `PageTransactionInfoImpl`,
//! `AckRetry` and `PacketIdCorrelationKey`.
//!
//! Every codec decodes the whole body and re-encodes it byte for byte.
//! Fields that later Artemis versions appended (read only "if bytes remain")
//! are `Option`s: `None` means the record was written by a version that did
//! not have the field, and encoding leaves it out again. Such optional
//! trailing fields are written in order and encoding stops at the first
//! `None`, as only a suffix can be missing.

use saranga_buffer::{DecodeError, JString, Reader, Result, SimpleString, Writer};

/// A record body codec.
pub trait RecordCodec: Sized {
    /// Reads the body from `r`.
    fn read(r: &mut Reader<'_>) -> Result<Self>;
    /// Writes the body to `w`.
    fn write(&self, w: &mut Writer);

    /// Decodes a whole body; bytes left over are an error, so a successful
    /// decode always re-encodes to the same bytes.
    fn decode(data: &[u8]) -> Result<Self> {
        let mut r = Reader::new(data);
        let v = Self::read(&mut r)?;
        if !r.is_empty() {
            return Err(DecodeError::Invalid(format!(
                "{} trailing bytes after the record",
                r.remaining()
            )));
        }
        Ok(v)
    }

    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.write(&mut w);
        w.into_vec()
    }
}

/// Reads an optional trailing field: `None` when nothing is left.
fn trailing<'a, T>(
    r: &mut Reader<'a>,
    f: impl FnOnce(&mut Reader<'a>) -> Result<T>,
) -> Result<Option<T>> {
    if r.is_empty() {
        Ok(None)
    } else {
        f(r).map(Some)
    }
}

/// Writes optional trailing fields in order, stopping at the first `None`.
struct Tail<'w> {
    w: &'w mut Writer,
    open: bool,
}

impl<'w> Tail<'w> {
    fn new(w: &'w mut Writer) -> Self {
        Self { w, open: true }
    }

    fn field<T>(&mut self, v: &Option<T>, f: impl FnOnce(&mut Writer, &T)) -> &mut Self {
        match v {
            Some(v) if self.open => f(self.w, v),
            _ => self.open = false,
        }
        self
    }
}

/// `BufferHelper.writeNullableDouble` and friends: a boolean presence flag
/// (`0xFF` / `0x00`), then the value.
fn read_flagged<'a, T>(
    r: &mut Reader<'a>,
    f: impl FnOnce(&mut Reader<'a>) -> Result<T>,
) -> Result<Option<T>> {
    if r.bool()? { f(r).map(Some) } else { Ok(None) }
}

fn write_flagged<T>(w: &mut Writer, v: &Option<T>, f: impl FnOnce(&mut Writer, &T)) {
    w.bool(v.is_some());
    if let Some(v) = v {
        f(w, v);
    }
}

macro_rules! fixed_codec {
    ($(#[$m:meta])* $name:ident { $($(#[$fm:meta])* $f:ident : $ty:ident),* $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name {
            $($(#[$fm])* pub $f: $ty,)*
        }

        impl RecordCodec for $name {
            fn read(r: &mut Reader<'_>) -> Result<Self> {
                Ok(Self { $($f: r.$ty()?,)* })
            }
            fn write(&self, w: &mut Writer) {
                $(w.$ty(self.$f);)*
            }
        }
    };
}

fixed_codec!(
    /// `QueueEncoding` / `RefEncoding`: `ADD_REF` and `ACKNOWLEDGE_REF`.
    RefEncoding { queue_id: i64 }
);

fixed_codec!(
    /// `BatchingIDGenerator.IDCounterEncoding`: `ID_COUNTER_RECORD`.
    IdCounterEncoding { id: i64 }
);

fixed_codec!(
    /// `QueueStatusEncoding`: `QUEUE_STATUS_RECORD`. `status` is the
    /// `AddressQueueStatus` ID (see [`queue_status_name`]).
    QueueStatusEncoding { queue_id: i64, status: i16 }
);

fixed_codec!(
    /// `AddressStatusEncoding`: `ADDRESS_STATUS_RECORD`.
    AddressStatusEncoding { address_id: i64, status: i16 }
);

fixed_codec!(
    /// `DeliveryCountUpdateEncoding`: `UPDATE_DELIVERY_COUNT`.
    DeliveryCountUpdateEncoding { queue_id: i64, count: i32 }
);

fixed_codec!(
    /// `ScheduledDeliveryEncoding`: `SET_SCHEDULED_DELIVERY_TIME`.
    ScheduledDeliveryEncoding { queue_id: i64, scheduled_delivery_time: i64 }
);

fixed_codec!(
    /// `PAGE_TRANSACTION`. Added records are `PageTransactionInfoImpl`
    /// (transaction ID, number of persistent messages); update records are
    /// `PageUpdateTXEncoding` (page transaction, records). Both have this
    /// layout.
    PageTransactionEncoding { transaction_id: i64, records: i32 }
);

fixed_codec!(
    /// `CursorAckRecordEncoding`: `ACKNOWLEDGE_CURSOR` and
    /// `PAGE_CURSOR_COMPLETE`. The position is a `PagePositionImpl`.
    CursorAckRecordEncoding { queue_id: i64, page_nr: i64, message_nr: i32 }
);

fixed_codec!(
    /// `PageCountPendingImpl`: `PAGE_CURSOR_PENDING_COUNTER`.
    PageCountPendingEncoding { queue_id: i64, page_id: i64 }
);

fixed_codec!(
    /// `PendingLargeMessageEncoding`: `ADD_LARGE_MESSAGE_PENDING`.
    PendingLargeMessageEncoding { large_message_id: i64 }
);

fixed_codec!(
    /// `DeleteEncoding`: the body of some transactional deletes (not a user
    /// record type of its own).
    DeleteEncoding { record_type: u8, id: i64 }
);

/// `AddressQueueStatus.fromID`: `PAUSED` (0) or `RUNNING` (1).
pub fn queue_status_name(id: i16) -> Option<&'static str> {
    match id {
        0 => Some("PAUSED"),
        1 => Some("RUNNING"),
        _ => None,
    }
}

/// `PageCountRecord`: `PAGE_CURSOR_COUNTER_VALUE`. `persistent_size` was
/// added in 2.x.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCountRecord {
    pub queue_id: i64,
    pub value: i64,
    pub persistent_size: Option<i64>,
}

impl RecordCodec for PageCountRecord {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            queue_id: r.i64()?,
            value: r.i64()?,
            persistent_size: trailing(r, Reader::i64)?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.i64(self.queue_id).i64(self.value);
        Tail::new(w).field(&self.persistent_size, |w, v| {
            w.i64(*v);
        });
    }
}

/// `PageCountRecordInc`: `PAGE_CURSOR_COUNTER_INC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCountRecordInc {
    pub queue_id: i64,
    pub value: i32,
    pub persistent_size: Option<i64>,
}

impl RecordCodec for PageCountRecordInc {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            queue_id: r.i64()?,
            value: r.i32()?,
            persistent_size: trailing(r, Reader::i64)?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.i64(self.queue_id).i32(self.value);
        Tail::new(w).field(&self.persistent_size, |w, v| {
            w.i64(*v);
        });
    }
}

/// `GroupingEncoding`: `GROUP_RECORD`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingEncoding {
    pub group_id: SimpleString,
    pub cluster_name: SimpleString,
}

impl RecordCodec for GroupingEncoding {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            group_id: r.simple_string()?,
            cluster_name: r.simple_string()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.group_id)
            .simple_string(&self.cluster_name);
    }
}

/// `DuplicateIDEncoding`: `DUPLICATE_ID`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateIdEncoding {
    pub address: SimpleString,
    pub duplicate_id: Vec<u8>,
}

impl RecordCodec for DuplicateIdEncoding {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            address: r.simple_string()?,
            duplicate_id: r.byte_array()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.address)
            .byte_array(&self.duplicate_id);
    }
}

/// An XA transaction ID as `XidCodecSupport` encodes it. Also the body of
/// `XidEncoding`, the extra data of an XA prepare record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Xid {
    pub format_id: i32,
    pub branch_qualifier: Vec<u8>,
    pub global_transaction_id: Vec<u8>,
}

impl RecordCodec for Xid {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            format_id: r.i32()?,
            branch_qualifier: r.byte_array()?,
            global_transaction_id: r.byte_array()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.i32(self.format_id)
            .byte_array(&self.branch_qualifier)
            .byte_array(&self.global_transaction_id);
    }
}

/// `HeuristicCompletionEncoding`: `HEURISTIC_COMPLETION`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeuristicCompletionEncoding {
    pub xid: Xid,
    pub is_commit: bool,
}

impl RecordCodec for HeuristicCompletionEncoding {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            xid: Xid::read(r)?,
            is_commit: r.bool()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        self.xid.write(w);
        w.bool(self.is_commit);
    }
}

/// `PersistentQueueBindingEncoding`: `QUEUE_BINDING_RECORD`, a queue's
/// `QueueConfiguration`.
///
/// Every field after `auto_created` was appended by a later Artemis version
/// and is `None` when the record predates it. `max_consumers`,
/// `purge_on_no_consumers` and `routing_type` were added together.
/// `metadata` is `user=<name>;` (or empty); see [`Self::user`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PersistentQueueBindingEncoding {
    pub name: SimpleString,
    pub address: SimpleString,
    pub filter_string: Option<SimpleString>,
    pub metadata: Option<SimpleString>,
    pub auto_created: bool,
    /// `(max_consumers, purge_on_no_consumers, routing_type)`.
    pub consumers: Option<(i32, bool, u8)>,
    pub exclusive: Option<bool>,
    pub last_value: Option<bool>,
    pub configuration_managed: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub last_value_key: Option<Option<SimpleString>>,
    pub non_destructive: Option<bool>,
    pub group_rebalance: Option<bool>,
    pub group_buckets: Option<i32>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub group_first_key: Option<Option<SimpleString>>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub internal: Option<bool>,
}

impl PersistentQueueBindingEncoding {
    /// The `user` entry of the metadata, as `decode` extracts it: elements
    /// split on `;`, each split on `=` and kept only with exactly two parts.
    pub fn user(&self) -> Option<String> {
        let metadata = crate::java::simple_string(self.metadata.as_ref()?);
        let mut user = None;
        for element in java_split(&metadata, ';') {
            let kv = java_split(element, '=');
            if kv.len() == 2 && kv[0] == "user" {
                user = Some(kv[1].to_owned());
            }
        }
        user
    }

    /// `createMetadata`: the metadata for a queue owned by `user`.
    pub fn metadata_for(user: Option<&str>) -> SimpleString {
        match user {
            Some(u) => format!("user={u};").into(),
            None => "".into(),
        }
    }
}

/// `String.split(regex)` for a single literal character: trailing empty
/// strings are removed (and an empty input gives one empty string).
pub(crate) fn java_split(s: &str, c: char) -> Vec<&str> {
    let mut parts: Vec<&str> = s.split(c).collect();
    if s.is_empty() {
        return parts;
    }
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    if parts.len() == 1 && parts[0].is_empty() {
        parts.clear();
    }
    parts
}

impl RecordCodec for PersistentQueueBindingEncoding {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.simple_string()?,
            address: r.simple_string()?,
            filter_string: r.nullable_simple_string()?,
            metadata: r.nullable_simple_string()?,
            auto_created: r.bool()?,
            consumers: trailing(r, |r| Ok((r.i32()?, r.bool()?, r.u8()?)))?,
            exclusive: trailing(r, Reader::bool)?,
            last_value: trailing(r, Reader::bool)?,
            configuration_managed: trailing(r, Reader::bool)?,
            consumers_before_dispatch: trailing(r, Reader::i32)?,
            delay_before_dispatch: trailing(r, Reader::i64)?,
            last_value_key: trailing(r, Reader::nullable_simple_string)?,
            non_destructive: trailing(r, Reader::bool)?,
            group_rebalance: trailing(r, Reader::bool)?,
            group_buckets: trailing(r, Reader::i32)?,
            auto_delete: trailing(r, Reader::bool)?,
            auto_delete_delay: trailing(r, Reader::i64)?,
            auto_delete_message_count: trailing(r, Reader::i64)?,
            group_first_key: trailing(r, Reader::nullable_simple_string)?,
            ring_size: trailing(r, Reader::i64)?,
            enabled: trailing(r, Reader::bool)?,
            group_rebalance_pause_dispatch: trailing(r, Reader::bool)?,
            internal: trailing(r, Reader::bool)?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.name)
            .simple_string(&self.address)
            .nullable_simple_string(self.filter_string.as_ref())
            .nullable_simple_string(self.metadata.as_ref())
            .bool(self.auto_created);
        let bool = |w: &mut Writer, v: &bool| {
            w.bool(*v);
        };
        let int = |w: &mut Writer, v: &i32| {
            w.i32(*v);
        };
        let long = |w: &mut Writer, v: &i64| {
            w.i64(*v);
        };
        let nss = |w: &mut Writer, v: &Option<SimpleString>| {
            w.nullable_simple_string(v.as_ref());
        };
        Tail::new(w)
            .field(&self.consumers, |w, (max, purge, rt)| {
                w.i32(*max).bool(*purge).u8(*rt);
            })
            .field(&self.exclusive, bool)
            .field(&self.last_value, bool)
            .field(&self.configuration_managed, bool)
            .field(&self.consumers_before_dispatch, int)
            .field(&self.delay_before_dispatch, long)
            .field(&self.last_value_key, nss)
            .field(&self.non_destructive, bool)
            .field(&self.group_rebalance, bool)
            .field(&self.group_buckets, int)
            .field(&self.auto_delete, bool)
            .field(&self.auto_delete_delay, long)
            .field(&self.auto_delete_message_count, long)
            .field(&self.group_first_key, nss)
            .field(&self.ring_size, long)
            .field(&self.enabled, bool)
            .field(&self.group_rebalance_pause_dispatch, bool)
            .field(&self.internal, bool);
    }
}

/// `PersistentAddressBindingEncoding`: `ADDRESS_BINDING_RECORD`.
/// `routing_types` holds the `RoutingType` bytes in record order
/// (`0` MULTICAST, `1` ANYCAST). `internal` was added in 2.x.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentAddressBindingEncoding {
    pub name: SimpleString,
    pub routing_types: Vec<u8>,
    pub auto_created: bool,
    pub internal: Option<bool>,
}

impl RecordCodec for PersistentAddressBindingEncoding {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let name = r.simple_string()?;
        let n = r.len("routing types")?;
        let routing_types = r.bytes(n)?.to_vec();
        Ok(Self {
            name,
            routing_types,
            auto_created: r.bool()?,
            internal: trailing(r, Reader::bool)?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.name)
            .byte_array(&self.routing_types)
            .bool(self.auto_created);
        Tail::new(w).field(&self.internal, |w, v| {
            w.bool(*v);
        });
    }
}

/// `PersistedAddressSetting`: `ADDRESS_SETTING_RECORD`, the deprecated
/// binary `AddressSettings` encoding. Artemis only reads it; the settings
/// are kept as the raw `AddressSettings.encode` bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedAddressSetting {
    pub address_match: SimpleString,
    pub settings: Vec<u8>,
}

impl RecordCodec for PersistedAddressSetting {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let address_match = r.simple_string()?;
        let settings = r.bytes(r.remaining())?.to_vec();
        Ok(Self {
            address_match,
            settings,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.address_match).bytes(&self.settings);
    }
}

/// `PersistedAddressSettingJSON`: `ADDRESS_SETTING_RECORD_JSON`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedAddressSettingJson {
    pub address_match: SimpleString,
    pub json: SimpleString,
}

impl RecordCodec for PersistedAddressSettingJson {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            address_match: r.simple_string()?,
            json: r.simple_string()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.address_match)
            .simple_string(&self.json);
    }
}

/// `PersistedSecuritySetting`: `SECURITY_SETTING_RECORD`. Each role list is
/// a comma-separated nullable string. `view_roles` and `edit_roles` were
/// added in 2.x (outer `None`: absent).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PersistedSecuritySetting {
    pub address_match: SimpleString,
    pub send_roles: Option<SimpleString>,
    pub consume_roles: Option<SimpleString>,
    pub create_durable_queue_roles: Option<SimpleString>,
    pub delete_durable_queue_roles: Option<SimpleString>,
    pub create_non_durable_queue_roles: Option<SimpleString>,
    pub delete_non_durable_queue_roles: Option<SimpleString>,
    pub manage_roles: Option<SimpleString>,
    pub browse_roles: Option<SimpleString>,
    pub create_address_roles: Option<SimpleString>,
    pub delete_address_roles: Option<SimpleString>,
    pub view_roles: Option<Option<SimpleString>>,
    pub edit_roles: Option<Option<SimpleString>>,
}

impl RecordCodec for PersistedSecuritySetting {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            address_match: r.simple_string()?,
            send_roles: r.nullable_simple_string()?,
            consume_roles: r.nullable_simple_string()?,
            create_durable_queue_roles: r.nullable_simple_string()?,
            delete_durable_queue_roles: r.nullable_simple_string()?,
            create_non_durable_queue_roles: r.nullable_simple_string()?,
            delete_non_durable_queue_roles: r.nullable_simple_string()?,
            manage_roles: r.nullable_simple_string()?,
            browse_roles: r.nullable_simple_string()?,
            create_address_roles: r.nullable_simple_string()?,
            delete_address_roles: r.nullable_simple_string()?,
            view_roles: trailing(r, Reader::nullable_simple_string)?,
            edit_roles: trailing(r, Reader::nullable_simple_string)?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.simple_string(&self.address_match);
        for v in [
            &self.send_roles,
            &self.consume_roles,
            &self.create_durable_queue_roles,
            &self.delete_durable_queue_roles,
            &self.create_non_durable_queue_roles,
            &self.delete_non_durable_queue_roles,
            &self.manage_roles,
            &self.browse_roles,
            &self.create_address_roles,
            &self.delete_address_roles,
        ] {
            w.nullable_simple_string(v.as_ref());
        }
        let nss = |w: &mut Writer, v: &Option<SimpleString>| {
            w.nullable_simple_string(v.as_ref());
        };
        Tail::new(w)
            .field(&self.view_roles, nss)
            .field(&self.edit_roles, nss);
    }
}

/// `TransformerConfiguration` as diverts and bridges encode it. The
/// properties keep their record order (a Java `HashMap`'s iteration order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformerConfiguration {
    pub class_name: JString,
    pub properties: Vec<(Option<JString>, Option<JString>)>,
}

/// Reads the transformer: a nullable class name, then (only when it is not
/// null) the property count and the properties.
fn read_transformer(r: &mut Reader<'_>) -> Result<Option<TransformerConfiguration>> {
    let Some(class_name) = r.nullable_string()? else {
        return Ok(None);
    };
    let n = r.len("transformer properties")?;
    let mut properties = Vec::with_capacity(n.min(r.remaining()));
    for _ in 0..n {
        properties.push((r.nullable_string()?, r.nullable_string()?));
    }
    Ok(Some(TransformerConfiguration {
        class_name,
        properties,
    }))
}

fn write_transformer(w: &mut Writer, t: &Option<TransformerConfiguration>) {
    match t {
        None => {
            w.nullable_string(None);
        }
        Some(t) => {
            w.nullable_string(Some(&t.class_name))
                .i32(t.properties.len() as i32);
            for (k, v) in &t.properties {
                w.nullable_string(k.as_ref()).nullable_string(v.as_ref());
            }
        }
    }
}

/// `PersistedDivertConfiguration`: `DIVERT_RECORD`, a `DivertConfiguration`.
/// `routing_type` is the `ComponentConfigurationRoutingType` byte
/// (MULTICAST 0, ANYCAST 1, STRIP 2, PASS 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedDivertConfiguration {
    pub name: Option<JString>,
    pub address: Option<JString>,
    pub forwarding_address: Option<JString>,
    pub routing_name: Option<JString>,
    pub exclusive: bool,
    pub filter_string: Option<JString>,
    pub routing_type: u8,
    pub transformer: Option<TransformerConfiguration>,
}

impl RecordCodec for PersistedDivertConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.nullable_string()?,
            address: r.nullable_string()?,
            forwarding_address: r.nullable_string()?,
            routing_name: r.nullable_string()?,
            exclusive: r.bool()?,
            filter_string: r.nullable_string()?,
            routing_type: r.u8()?,
            transformer: read_transformer(r)?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.nullable_string(self.name.as_ref())
            .nullable_string(self.address.as_ref())
            .nullable_string(self.forwarding_address.as_ref())
            .nullable_string(self.routing_name.as_ref())
            .bool(self.exclusive)
            .nullable_string(self.filter_string.as_ref())
            .u8(self.routing_type);
        write_transformer(w, &self.transformer);
    }
}

/// `PersistedBridgeConfiguration`: `BRIDGE_RECORD`, a `BridgeConfiguration`.
/// `pending_ack_timeout` and `client_id` were added in 2.x (outer `None`:
/// absent).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PersistedBridgeConfiguration {
    pub name: Option<JString>,
    pub parent_name: Option<JString>,
    pub queue_name: Option<JString>,
    pub forwarding_address: Option<JString>,
    pub filter_string: Option<JString>,
    pub discovery_group_name: Option<JString>,
    pub ha: Option<bool>,
    pub retry_interval: Option<i64>,
    /// Written with `BufferHelper` (a boolean flag), unlike its neighbours.
    pub retry_interval_multiplier: Option<f64>,
    pub initial_connect_attempts: Option<i32>,
    pub reconnect_attempts: Option<i32>,
    pub reconnect_attempts_on_same_node: Option<i32>,
    pub use_duplicate_detection: Option<bool>,
    pub confirmation_window_size: Option<i32>,
    pub producer_window_size: Option<i32>,
    pub client_failure_check_period: Option<i64>,
    pub user: Option<JString>,
    pub password: Option<JString>,
    pub connection_ttl: Option<i64>,
    pub max_retry_interval: Option<i64>,
    pub min_large_message_size: Option<i32>,
    pub call_timeout: Option<i64>,
    pub concurrency: Option<i32>,
    pub configuration_managed: Option<bool>,
    pub routing_type: u8,
    pub transformer: Option<TransformerConfiguration>,
    /// An empty list is written as a count of 0 and read back as null.
    pub static_connectors: Vec<Option<JString>>,
    pub pending_ack_timeout: Option<Option<i64>>,
    pub client_id: Option<Option<JString>>,
}

impl Eq for PersistedBridgeConfiguration {}

impl RecordCodec for PersistedBridgeConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let mut b = Self {
            name: r.nullable_string()?,
            parent_name: r.nullable_string()?,
            queue_name: r.nullable_string()?,
            forwarding_address: r.nullable_string()?,
            filter_string: r.nullable_string()?,
            discovery_group_name: r.nullable_string()?,
            ha: r.nullable_bool()?,
            retry_interval: r.nullable_i64()?,
            retry_interval_multiplier: read_flagged(r, Reader::f64)?,
            initial_connect_attempts: r.nullable_i32()?,
            reconnect_attempts: r.nullable_i32()?,
            reconnect_attempts_on_same_node: r.nullable_i32()?,
            use_duplicate_detection: r.nullable_bool()?,
            confirmation_window_size: r.nullable_i32()?,
            producer_window_size: r.nullable_i32()?,
            client_failure_check_period: r.nullable_i64()?,
            user: r.nullable_string()?,
            password: r.nullable_string()?,
            connection_ttl: r.nullable_i64()?,
            max_retry_interval: r.nullable_i64()?,
            min_large_message_size: r.nullable_i32()?,
            call_timeout: r.nullable_i64()?,
            concurrency: r.nullable_i32()?,
            configuration_managed: r.nullable_bool()?,
            routing_type: r.u8()?,
            transformer: read_transformer(r)?,
            ..Default::default()
        };
        // A negative count reads as no connectors, as in Java.
        let n = r.i32()?;
        for _ in 0..n.max(0) {
            b.static_connectors.push(r.nullable_string()?);
        }
        b.pending_ack_timeout = trailing(r, Reader::nullable_i64)?;
        b.client_id = trailing(r, Reader::nullable_string)?;
        Ok(b)
    }

    fn write(&self, w: &mut Writer) {
        w.nullable_string(self.name.as_ref())
            .nullable_string(self.parent_name.as_ref())
            .nullable_string(self.queue_name.as_ref())
            .nullable_string(self.forwarding_address.as_ref())
            .nullable_string(self.filter_string.as_ref())
            .nullable_string(self.discovery_group_name.as_ref())
            .nullable_bool(self.ha)
            .nullable_i64(self.retry_interval);
        write_flagged(w, &self.retry_interval_multiplier, |w, v| {
            w.f64(*v);
        });
        w.nullable_i32(self.initial_connect_attempts)
            .nullable_i32(self.reconnect_attempts)
            .nullable_i32(self.reconnect_attempts_on_same_node)
            .nullable_bool(self.use_duplicate_detection)
            .nullable_i32(self.confirmation_window_size)
            .nullable_i32(self.producer_window_size)
            .nullable_i64(self.client_failure_check_period)
            .nullable_string(self.user.as_ref())
            .nullable_string(self.password.as_ref())
            .nullable_i64(self.connection_ttl)
            .nullable_i64(self.max_retry_interval)
            .nullable_i32(self.min_large_message_size)
            .nullable_i64(self.call_timeout)
            .nullable_i32(self.concurrency)
            .nullable_bool(self.configuration_managed)
            .u8(self.routing_type);
        write_transformer(w, &self.transformer);
        w.i32(self.static_connectors.len() as i32);
        for c in &self.static_connectors {
            w.nullable_string(c.as_ref());
        }
        Tail::new(w)
            .field(&self.pending_ack_timeout, |w, v| {
                w.nullable_i64(*v);
            })
            .field(&self.client_id, |w, v| {
                w.nullable_string(v.as_ref());
            });
    }
}

/// `PersistedUser`: `USER_RECORD`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedUser {
    pub username: JString,
    pub password: JString,
}

impl RecordCodec for PersistedUser {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            username: r.string()?,
            password: r.string()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.string(&self.username).string(&self.password);
    }
}

/// `PersistedRole`: `ROLE_RECORD`, the roles of a user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedRole {
    pub username: JString,
    pub roles: Vec<JString>,
}

impl RecordCodec for PersistedRole {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let username = r.string()?;
        let n = r.i32()?;
        let mut roles = Vec::new();
        for _ in 0..n.max(0) {
            roles.push(r.string()?);
        }
        Ok(Self { username, roles })
    }
    fn write(&self, w: &mut Writer) {
        w.string(&self.username).i32(self.roles.len() as i32);
        for role in &self.roles {
            w.string(role);
        }
    }
}

/// `PersistedKeyValuePair`: `KEY_VALUE_PAIR_RECORD`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedKeyValuePair {
    pub map_id: JString,
    pub key: JString,
    pub value: JString,
}

impl RecordCodec for PersistedKeyValuePair {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            map_id: r.string()?,
            key: r.string()?,
            value: r.string()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.string(&self.map_id).string(&self.key).string(&self.value);
    }
}

/// `PersistedConnector`: `CONNECTOR_RECORD`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedConnector {
    pub name: JString,
    pub url: JString,
}

impl RecordCodec for PersistedConnector {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.string()?,
            url: r.string()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.string(&self.name).string(&self.url);
    }
}

/// `AbstractHashMapPersister.VERSION`.
pub const HASH_MAP_RECORD_VERSION: u8 = 0;

/// `ACK_RETRY`: a `JournalHashMap.MapRecord` written by `AckRetry.Persister`
/// (the AMQP mirror's acks waiting to be retried). The collection ID is the
/// queue ID; key and value are the `AckRetry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckRetryRecord {
    /// `MapRecord.id`.
    pub id: i64,
    /// `AbstractHashMapPersister.VERSION`, always 0.
    pub version: u8,
    pub queue_id: i64,
    /// The node ID's US-ASCII bytes; empty means null.
    pub node_id: Vec<u8>,
    pub message_id: i64,
    /// `AckReason`: NORMAL 0, KILLED 1, EXPIRED 2, REPLACED 3 (anything else
    /// reads as NORMAL).
    pub reason: u8,
}

impl RecordCodec for AckRetryRecord {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            id: r.i64()?,
            version: r.u8()?,
            queue_id: r.i64()?,
            node_id: r.byte_array()?,
            message_id: r.i64()?,
            reason: r.u8()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.i64(self.id)
            .u8(self.version)
            .i64(self.queue_id)
            .byte_array(&self.node_id)
            .i64(self.message_id)
            .u8(self.reason);
    }
}

/// `MQTT_PACKET_ID_CORRELATION`: a `JournalHashMap.MapRecord` written by
/// `PacketIdCorrelationKey.Persister`. The collection ID is the MQTT client
/// ID; the value is the MQTT packet ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttPacketIdCorrelation {
    pub id: i64,
    pub version: u8,
    pub client_id: JString,
    pub core_message_id: i64,
    pub address: SimpleString,
    pub packet_id: i32,
}

impl RecordCodec for MqttPacketIdCorrelation {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            id: r.i64()?,
            version: r.u8()?,
            client_id: r.string()?,
            core_message_id: r.i64()?,
            address: r.simple_string()?,
            packet_id: r.i32()?,
        })
    }
    fn write(&self, w: &mut Writer) {
        w.i64(self.id)
            .u8(self.version)
            .string(&self.client_id)
            .i64(self.core_message_id)
            .simple_string(&self.address)
            .i32(self.packet_id);
    }
}

/// A body kept as raw bytes: `ADD_MESSAGE_BODY` (a large-message body chunk)
/// and the pre-2.0 `ADD_MESSAGE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBody(pub Vec<u8>);

impl RecordCodec for RawBody {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self(r.bytes(r.remaining())?.to_vec()))
    }
    fn write(&self, w: &mut Writer) {
        w.bytes(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T: RecordCodec + PartialEq + std::fmt::Debug>(v: T) -> Vec<u8> {
        let bytes = v.encode();
        assert_eq!(T::decode(&bytes).unwrap(), v);
        bytes
    }

    #[test]
    fn fixed_layouts() {
        assert_eq!(
            roundtrip(RefEncoding { queue_id: 0x0102 }),
            [0, 0, 0, 0, 0, 0, 1, 2]
        );
        assert_eq!(
            roundtrip(QueueStatusEncoding {
                queue_id: 3,
                status: 1
            })
            .len(),
            10
        );
        assert_eq!(
            roundtrip(DeliveryCountUpdateEncoding {
                queue_id: 1,
                count: 2
            })
            .len(),
            12
        );
        let s = roundtrip(ScheduledDeliveryEncoding {
            queue_id: 7,
            scheduled_delivery_time: 9,
        });
        assert_eq!(s[7], 7, "queue ID first");
        roundtrip(CursorAckRecordEncoding {
            queue_id: 1,
            page_nr: 2,
            message_nr: -1,
        });
        roundtrip(PageCountPendingEncoding {
            queue_id: 1,
            page_id: 2,
        });
        roundtrip(DeleteEncoding {
            record_type: 29,
            id: 4,
        });
        assert!(RefEncoding::decode(&[0; 9]).is_err(), "trailing byte");
        assert!(RefEncoding::decode(&[0; 7]).is_err(), "short");
    }

    #[test]
    fn page_counters_with_and_without_persistent_size() {
        let old = PageCountRecord {
            queue_id: 1,
            value: 2,
            persistent_size: None,
        };
        assert_eq!(roundtrip(old).len(), 16);
        let new = PageCountRecord {
            persistent_size: Some(3),
            ..old
        };
        assert_eq!(roundtrip(new).len(), 24);
        let inc = PageCountRecordInc {
            queue_id: 1,
            value: -1,
            persistent_size: None,
        };
        assert_eq!(roundtrip(inc).len(), 12);
        roundtrip(PageCountRecordInc {
            persistent_size: Some(10),
            ..inc
        });
    }

    fn full_queue() -> PersistentQueueBindingEncoding {
        PersistentQueueBindingEncoding {
            name: "q".into(),
            address: "a".into(),
            filter_string: Some("x > 1".into()),
            metadata: Some(PersistentQueueBindingEncoding::metadata_for(Some("bob"))),
            auto_created: true,
            consumers: Some((-1, false, 1)),
            exclusive: Some(false),
            last_value: Some(true),
            configuration_managed: Some(false),
            consumers_before_dispatch: Some(0),
            delay_before_dispatch: Some(-1),
            last_value_key: Some(Some("k".into())),
            non_destructive: Some(false),
            group_rebalance: Some(false),
            group_buckets: Some(-1),
            auto_delete: Some(true),
            auto_delete_delay: Some(0),
            auto_delete_message_count: Some(0),
            group_first_key: Some(None),
            ring_size: Some(-1),
            enabled: Some(true),
            group_rebalance_pause_dispatch: Some(false),
            internal: Some(false),
        }
    }

    #[test]
    fn queue_binding_versions() {
        let full = full_queue();
        let bytes = roundtrip(full.clone());
        assert_eq!(full.user().as_deref(), Some("bob"));
        // A record from a version that stopped after `exclusive`.
        let old = PersistentQueueBindingEncoding {
            last_value: None,
            configuration_managed: None,
            consumers_before_dispatch: None,
            delay_before_dispatch: None,
            last_value_key: None,
            non_destructive: None,
            group_rebalance: None,
            group_buckets: None,
            auto_delete: None,
            auto_delete_delay: None,
            auto_delete_message_count: None,
            group_first_key: None,
            ring_size: None,
            enabled: None,
            group_rebalance_pause_dispatch: None,
            internal: None,
            ..full
        };
        let old_bytes = roundtrip(old);
        assert!(bytes.starts_with(&old_bytes));
        // Encoding stops at the first missing field.
        let gap = PersistentQueueBindingEncoding {
            last_value: None,
            ..full_queue()
        };
        assert_eq!(gap.encode(), old_bytes);
    }

    #[test]
    fn metadata_user_parsing_matches_java_split() {
        let mut q = full_queue();
        for (meta, user) in [
            ("", None),
            ("user=a;", Some("a")),
            ("user=a=b;", None),
            ("x=1;user=z", Some("z")),
            ("user=;", None),
        ] {
            q.metadata = Some(meta.into());
            assert_eq!(q.user().as_deref(), user, "{meta}");
        }
        q.metadata = None;
        assert_eq!(q.user(), None);
    }

    #[test]
    fn address_binding_and_security_setting_trailing_fields() {
        let a = PersistentAddressBindingEncoding {
            name: "addr".into(),
            routing_types: vec![1, 0],
            auto_created: false,
            internal: None,
        };
        let short = roundtrip(a.clone());
        let long = roundtrip(PersistentAddressBindingEncoding {
            internal: Some(true),
            ..a
        });
        assert_eq!(long.len(), short.len() + 1);
        assert_eq!(*long.last().unwrap(), 0xff);

        let s = PersistedSecuritySetting {
            address_match: "#".into(),
            send_roles: Some("a,b".into()),
            view_roles: Some(None),
            edit_roles: Some(Some("e".into())),
            ..Default::default()
        };
        roundtrip(s.clone());
        roundtrip(PersistedSecuritySetting {
            view_roles: None,
            edit_roles: None,
            ..s
        });
    }

    #[test]
    fn divert_and_bridge() {
        let d = PersistedDivertConfiguration {
            name: Some("d".into()),
            address: Some("a".into()),
            forwarding_address: Some("f".into()),
            routing_name: Some("r".into()),
            exclusive: true,
            filter_string: None,
            routing_type: 3,
            transformer: Some(TransformerConfiguration {
                class_name: "org.example.T".into(),
                properties: vec![(Some("k".into()), Some("v".into())), (None, None)],
            }),
        };
        roundtrip(d.clone());
        roundtrip(PersistedDivertConfiguration {
            transformer: None,
            ..d
        });

        let b = PersistedBridgeConfiguration {
            name: Some("b".into()),
            queue_name: Some("q".into()),
            ha: Some(true),
            retry_interval_multiplier: Some(1.5),
            routing_type: 3,
            static_connectors: vec![Some("c1".into()), None],
            pending_ack_timeout: Some(Some(60_000)),
            client_id: Some(None),
            ..Default::default()
        };
        roundtrip(b.clone());
        roundtrip(PersistedBridgeConfiguration {
            pending_ack_timeout: None,
            client_id: None,
            ..b
        });
    }

    #[test]
    fn users_roles_and_others() {
        roundtrip(PersistedUser {
            username: "u".into(),
            password: "ENC(x)".into(),
        });
        roundtrip(PersistedRole {
            username: "u".into(),
            roles: vec!["a".into(), "a much longer role name".into()],
        });
        roundtrip(PersistedKeyValuePair {
            map_id: "m".into(),
            key: "k".into(),
            value: "v".into(),
        });
        roundtrip(PersistedConnector {
            name: "c".into(),
            url: "tcp://localhost:61616".into(),
        });
        roundtrip(GroupingEncoding {
            group_id: "g".into(),
            cluster_name: "c".into(),
        });
        roundtrip(DuplicateIdEncoding {
            address: "a".into(),
            duplicate_id: vec![1, 2, 3],
        });
        roundtrip(HeuristicCompletionEncoding {
            xid: Xid {
                format_id: 1,
                branch_qualifier: vec![1],
                global_transaction_id: vec![2, 3],
            },
            is_commit: true,
        });
        roundtrip(PersistedAddressSettingJson {
            address_match: "#".into(),
            json: "{}".into(),
        });
        roundtrip(PersistedAddressSetting {
            address_match: "#".into(),
            settings: vec![0, 0, 1],
        });
        roundtrip(AckRetryRecord {
            id: 1,
            version: 0,
            queue_id: 2,
            node_id: b"node".to_vec(),
            message_id: 3,
            reason: 2,
        });
        roundtrip(MqttPacketIdCorrelation {
            id: 1,
            version: 0,
            client_id: "client".into(),
            core_message_id: 2,
            address: "a".into(),
            packet_id: 7,
        });
        roundtrip(RawBody(vec![9; 4]));
    }
}
