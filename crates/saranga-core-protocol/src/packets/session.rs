//! Session packets: queue and address management, consumers, producers,
//! acknowledgements, transactions, metadata, flow control, and the
//! pre-2.0 (`_1X`) send and receive packets.
//!
//! Java's `QueueAbstractPacket` is only a base class holding `queueName` and
//! `address` for `SessionBindingQueryMessage`, `SessionCreateConsumerMessage`
//! and `SessionQueueQueryMessage`; each of those encodes its own fields, so it
//! has no struct of its own.
//!
//! Routing types are kept as their raw wire byte (`RoutingType.getType`:
//! 0 = MULTICAST, 1 = ANYCAST, -1 = none) so every packet re-encodes exactly.
//!
//! Several packets end with optional trailing sections, each read only if
//! bytes remain (`if (buffer.readableBytes() > 0)`), so that older clients
//! and brokers that do not send them still decode. Those packets keep a
//! `sections` count: how many trailing sections were present and are
//! written back. Artemis 2.57.0 always writes all of them (the `SECTIONS`
//! constant). Fields of absent sections are left at their defaults.

use saranga_buffer::{DecodeError, JString, Reader, Result, SimpleString, Writer};
use saranga_message::{BUFFER_HEADER_SPACE, CoreMessage};

use crate::{Context, Flagged, PacketCodec, types};

/// `RoutingType.MULTICAST.getType()`.
pub const ROUTING_TYPE_MULTICAST: i8 = 0;
/// `RoutingType.ANYCAST.getType()`.
pub const ROUTING_TYPE_ANYCAST: i8 = 1;
/// Written for a null routing type.
pub const ROUTING_TYPE_NONE: i8 = -1;

/// Counts trailing sections while decoding: `next` is true (and counts the
/// section) when bytes remain.
struct Sections(u8);

impl Sections {
    fn next(&mut self, r: &Reader<'_>) -> bool {
        if r.is_empty() {
            false
        } else {
            self.0 += 1;
            true
        }
    }
}

/// An i32 count, then that many items.
fn read_list<T: saranga_buffer::Codec>(r: &mut Reader<'_>) -> Result<Vec<T>> {
    let n = r.i32()?;
    let mut out = Vec::with_capacity(n.clamp(0, 1024) as usize);
    for _ in 0..n.max(0) {
        out.push(T::read(r)?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Addresses, queues and producers

/// `CreateAddressMessage`. `routing_types` are raw `RoutingType` bytes in
/// wire order (Java collects them into an `EnumSet`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CreateAddressMessage {
    pub address: SimpleString,
    pub routing_types: Vec<i8>,
    pub requires_response: bool,
    pub auto_created: bool,
}

impl PacketCodec for CreateAddressMessage {
    const TYPE: i8 = types::CREATE_ADDRESS;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            address: r.simple_string()?,
            routing_types: read_list(r)?,
            requires_response: r.bool()?,
            auto_created: r.bool()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.simple_string(&self.address)
            .i32(self.routing_types.len() as i32);
        for t in &self.routing_types {
            w.i8(*t);
        }
        w.bool(self.requires_response).bool(self.auto_created);
    }
}

wire_packet! {
    /// `CreateProducerMessage` (decoded by the server only).
    CreateProducerMessage = types::CREATE_PRODUCER => {
        pub id: i32,
        pub address: Option<SimpleString>,
    }
}

wire_packet! {
    /// `RemoveProducerMessage` (decoded by the server only).
    RemoveProducerMessage = types::REMOVE_PRODUCER => {
        pub id: i32,
    }
}

wire_packet! {
    /// `CreateQueueMessage`
    CreateQueueMessage = types::CREATE_QUEUE => {
        pub address: SimpleString,
        pub queue_name: SimpleString,
        pub filter_string: Option<SimpleString>,
        pub durable: bool,
        pub temporary: bool,
        pub requires_response: bool,
    }
}

/// `CreateQueueMessage_V2`: `CreateQueueMessage` plus queue attributes. The
/// nullable attributes use `BufferHelper` (`Flagged`), except the keys,
/// which are nullable SimpleStrings.
///
/// Trailing sections: 1 `exclusive`, `last_value`; 2 `last_value_key` to
/// `auto_delete_message_count`; 3 `group_first_key`; 4 `ring_size`;
/// 5 `enabled`; 6 `group_rebalance_pause_dispatch`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CreateQueueMessageV2 {
    pub address: SimpleString,
    pub queue_name: SimpleString,
    pub filter_string: Option<SimpleString>,
    pub durable: bool,
    pub temporary: bool,
    pub requires_response: bool,
    pub auto_created: bool,
    pub routing_type: i8,
    pub max_consumers: i32,
    pub purge_on_no_consumers: bool,
    pub exclusive: Flagged<bool>,
    pub last_value: Flagged<bool>,
    pub last_value_key: Option<SimpleString>,
    pub non_destructive: Flagged<bool>,
    pub consumers_before_dispatch: Flagged<i32>,
    pub delay_before_dispatch: Flagged<i64>,
    pub group_rebalance: Flagged<bool>,
    pub group_buckets: Flagged<i32>,
    pub auto_delete: Flagged<bool>,
    pub auto_delete_delay: Flagged<i64>,
    pub auto_delete_message_count: Flagged<i64>,
    pub group_first_key: Option<SimpleString>,
    pub ring_size: Flagged<i64>,
    pub enabled: Flagged<bool>,
    pub group_rebalance_pause_dispatch: Flagged<bool>,
    /// Trailing sections present (see the type docs).
    pub sections: u8,
}

impl CreateQueueMessageV2 {
    pub const SECTIONS: u8 = 6;
}

impl PacketCodec for CreateQueueMessageV2 {
    const TYPE: i8 = types::CREATE_QUEUE_V2;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        use saranga_buffer::Codec;
        let mut p = Self {
            address: r.simple_string()?,
            queue_name: r.simple_string()?,
            filter_string: r.nullable_simple_string()?,
            durable: r.bool()?,
            temporary: r.bool()?,
            requires_response: r.bool()?,
            auto_created: r.bool()?,
            routing_type: r.i8()?,
            max_consumers: r.i32()?,
            purge_on_no_consumers: r.bool()?,
            ..Default::default()
        };
        let mut s = Sections(0);
        if s.next(r) {
            p.exclusive = Codec::read(r)?;
            p.last_value = Codec::read(r)?;
        }
        if s.next(r) {
            p.last_value_key = Codec::read(r)?;
            p.non_destructive = Codec::read(r)?;
            p.consumers_before_dispatch = Codec::read(r)?;
            p.delay_before_dispatch = Codec::read(r)?;
            p.group_rebalance = Codec::read(r)?;
            p.group_buckets = Codec::read(r)?;
            p.auto_delete = Codec::read(r)?;
            p.auto_delete_delay = Codec::read(r)?;
            p.auto_delete_message_count = Codec::read(r)?;
        }
        if s.next(r) {
            p.group_first_key = Codec::read(r)?;
        }
        if s.next(r) {
            p.ring_size = Codec::read(r)?;
        }
        if s.next(r) {
            p.enabled = Codec::read(r)?;
        }
        if s.next(r) {
            p.group_rebalance_pause_dispatch = Codec::read(r)?;
        }
        p.sections = s.0;
        Ok(p)
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        use saranga_buffer::Codec;
        w.simple_string(&self.address)
            .simple_string(&self.queue_name)
            .nullable_simple_string(self.filter_string.as_ref())
            .bool(self.durable)
            .bool(self.temporary)
            .bool(self.requires_response)
            .bool(self.auto_created)
            .i8(self.routing_type)
            .i32(self.max_consumers)
            .bool(self.purge_on_no_consumers);
        let n = self.sections;
        if n >= 1 {
            self.exclusive.write(w);
            self.last_value.write(w);
        }
        if n >= 2 {
            self.last_value_key.write(w);
            self.non_destructive.write(w);
            self.consumers_before_dispatch.write(w);
            self.delay_before_dispatch.write(w);
            self.group_rebalance.write(w);
            self.group_buckets.write(w);
            self.auto_delete.write(w);
            self.auto_delete_delay.write(w);
            self.auto_delete_message_count.write(w);
        }
        if n >= 3 {
            self.group_first_key.write(w);
        }
        if n >= 4 {
            self.ring_size.write(w);
        }
        if n >= 5 {
            self.enabled.write(w);
        }
        if n >= 6 {
            self.group_rebalance_pause_dispatch.write(w);
        }
    }
}

wire_packet! {
    /// `CreateSharedQueueMessage`
    CreateSharedQueueMessage = types::CREATE_SHARED_QUEUE => {
        pub address: SimpleString,
        pub queue_name: SimpleString,
        pub filter_string: Option<SimpleString>,
        pub durable: bool,
        pub requires_response: bool,
    }
}

/// `CreateSharedQueueMessage_V2`. Unlike `CreateQueueMessage_V2`,
/// `max_consumers` and `purge_on_no_consumers` are nullable here.
///
/// Java writes every nullable attribute with `BufferHelper` (0xFF flag), but
/// reads `ring_size`, `enabled` and `group_rebalance_pause_dispatch` with
/// `ActiveMQBuffer.readNullableX` (any non-zero marker), which accepts the
/// same bytes, so all are `Flagged`.
///
/// Trailing sections: 1 `max_consumers` to `last_value`; 2 `last_value_key`
/// to `auto_delete_message_count`; 3 `group_first_key`; 4 `ring_size`;
/// 5 `enabled`; 6 `group_rebalance_pause_dispatch`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CreateSharedQueueMessageV2 {
    pub address: SimpleString,
    pub queue_name: SimpleString,
    pub filter_string: Option<SimpleString>,
    pub durable: bool,
    pub routing_type: i8,
    pub requires_response: bool,
    pub max_consumers: Flagged<i32>,
    pub purge_on_no_consumers: Flagged<bool>,
    pub exclusive: Flagged<bool>,
    pub last_value: Flagged<bool>,
    pub last_value_key: Option<SimpleString>,
    pub non_destructive: Flagged<bool>,
    pub consumers_before_dispatch: Flagged<i32>,
    pub delay_before_dispatch: Flagged<i64>,
    pub group_rebalance: Flagged<bool>,
    pub group_buckets: Flagged<i32>,
    pub auto_delete: Flagged<bool>,
    pub auto_delete_delay: Flagged<i64>,
    pub auto_delete_message_count: Flagged<i64>,
    pub group_first_key: Option<SimpleString>,
    pub ring_size: Flagged<i64>,
    pub enabled: Flagged<bool>,
    pub group_rebalance_pause_dispatch: Flagged<bool>,
    /// Trailing sections present (see the type docs).
    pub sections: u8,
}

impl CreateSharedQueueMessageV2 {
    pub const SECTIONS: u8 = 6;
}

impl PacketCodec for CreateSharedQueueMessageV2 {
    const TYPE: i8 = types::CREATE_SHARED_QUEUE_V2;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        use saranga_buffer::Codec;
        let mut p = Self {
            address: r.simple_string()?,
            queue_name: r.simple_string()?,
            filter_string: r.nullable_simple_string()?,
            durable: r.bool()?,
            routing_type: r.i8()?,
            requires_response: r.bool()?,
            ..Default::default()
        };
        let mut s = Sections(0);
        if s.next(r) {
            p.max_consumers = Codec::read(r)?;
            p.purge_on_no_consumers = Codec::read(r)?;
            p.exclusive = Codec::read(r)?;
            p.last_value = Codec::read(r)?;
        }
        if s.next(r) {
            p.last_value_key = Codec::read(r)?;
            p.non_destructive = Codec::read(r)?;
            p.consumers_before_dispatch = Codec::read(r)?;
            p.delay_before_dispatch = Codec::read(r)?;
            p.group_rebalance = Codec::read(r)?;
            p.group_buckets = Codec::read(r)?;
            p.auto_delete = Codec::read(r)?;
            p.auto_delete_delay = Codec::read(r)?;
            p.auto_delete_message_count = Codec::read(r)?;
        }
        if s.next(r) {
            p.group_first_key = Codec::read(r)?;
        }
        if s.next(r) {
            p.ring_size = Codec::read(r)?;
        }
        if s.next(r) {
            p.enabled = Codec::read(r)?;
        }
        if s.next(r) {
            p.group_rebalance_pause_dispatch = Codec::read(r)?;
        }
        p.sections = s.0;
        Ok(p)
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        use saranga_buffer::Codec;
        w.simple_string(&self.address)
            .simple_string(&self.queue_name)
            .nullable_simple_string(self.filter_string.as_ref())
            .bool(self.durable)
            .i8(self.routing_type)
            .bool(self.requires_response);
        let n = self.sections;
        if n >= 1 {
            self.max_consumers.write(w);
            self.purge_on_no_consumers.write(w);
            self.exclusive.write(w);
            self.last_value.write(w);
        }
        if n >= 2 {
            self.last_value_key.write(w);
            self.non_destructive.write(w);
            self.consumers_before_dispatch.write(w);
            self.delay_before_dispatch.write(w);
            self.group_rebalance.write(w);
            self.group_buckets.write(w);
            self.auto_delete.write(w);
            self.auto_delete_delay.write(w);
            self.auto_delete_message_count.write(w);
        }
        if n >= 3 {
            self.group_first_key.write(w);
        }
        if n >= 4 {
            self.ring_size.write(w);
        }
        if n >= 5 {
            self.enabled.write(w);
        }
        if n >= 6 {
            self.group_rebalance_pause_dispatch.write(w);
        }
    }
}

wire_packet! {
    /// `SessionDeleteQueueMessage`
    SessionDeleteQueueMessage = types::DELETE_QUEUE => {
        pub queue_name: SimpleString,
    }
}

// ---------------------------------------------------------------------------
// Queries

wire_packet! {
    /// `SessionQueueQueryMessage`
    SessionQueueQueryMessage = types::SESS_QUEUEQUERY => {
        pub queue_name: SimpleString,
    }
}

wire_packet! {
    /// `SessionQueueQueryResponseMessage`
    SessionQueueQueryResponseMessage = types::SESS_QUEUEQUERY_RESP => {
        pub exists: bool,
        pub durable: bool,
        pub temporary: bool,
        pub consumer_count: i32,
        pub message_count: i64,
        pub filter_string: Option<SimpleString>,
        pub address: Option<SimpleString>,
        pub name: Option<SimpleString>,
    }
}

wire_packet! {
    /// `SessionQueueQueryResponseMessage_V2`: adds `auto_create_queues`.
    SessionQueueQueryResponseMessageV2 = types::SESS_QUEUEQUERY_RESP_V2 => {
        pub exists: bool,
        pub durable: bool,
        pub temporary: bool,
        pub consumer_count: i32,
        pub message_count: i64,
        pub filter_string: Option<SimpleString>,
        pub address: Option<SimpleString>,
        pub name: Option<SimpleString>,
        pub auto_create_queues: bool,
    }
}

/// `SessionQueueQueryResponseMessage_V3`: the `_V2` fields plus queue
/// attributes.
///
/// Trailing sections: 1 `exclusive`, `last_value`; 2
/// `default_consumer_window_size`; 3 `last_value_key` to
/// `auto_delete_message_count`; 4 `group_first_key`; 5 `ring_size`;
/// 6 `enabled`; 7 `group_rebalance_pause_dispatch`; 8
/// `configuration_managed`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionQueueQueryResponseMessageV3 {
    pub exists: bool,
    pub durable: bool,
    pub temporary: bool,
    pub consumer_count: i32,
    pub message_count: i64,
    pub filter_string: Option<SimpleString>,
    pub address: Option<SimpleString>,
    pub name: Option<SimpleString>,
    pub auto_create_queues: bool,
    pub auto_created: bool,
    pub purge_on_no_consumers: bool,
    pub routing_type: i8,
    pub max_consumers: i32,
    pub exclusive: Flagged<bool>,
    pub last_value: Flagged<bool>,
    pub default_consumer_window_size: Flagged<i32>,
    pub last_value_key: Option<SimpleString>,
    pub non_destructive: Flagged<bool>,
    pub consumers_before_dispatch: Flagged<i32>,
    pub delay_before_dispatch: Flagged<i64>,
    pub group_rebalance: Flagged<bool>,
    pub group_buckets: Flagged<i32>,
    pub auto_delete: Flagged<bool>,
    pub auto_delete_delay: Flagged<i64>,
    pub auto_delete_message_count: Flagged<i64>,
    pub group_first_key: Option<SimpleString>,
    pub ring_size: Flagged<i64>,
    pub enabled: Flagged<bool>,
    pub group_rebalance_pause_dispatch: Flagged<bool>,
    pub configuration_managed: Flagged<bool>,
    /// Trailing sections present (see the type docs).
    pub sections: u8,
}

impl SessionQueueQueryResponseMessageV3 {
    pub const SECTIONS: u8 = 8;
}

impl PacketCodec for SessionQueueQueryResponseMessageV3 {
    const TYPE: i8 = types::SESS_QUEUEQUERY_RESP_V3;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        use saranga_buffer::Codec;
        let mut p = Self {
            exists: r.bool()?,
            durable: r.bool()?,
            temporary: r.bool()?,
            consumer_count: r.i32()?,
            message_count: r.i64()?,
            filter_string: r.nullable_simple_string()?,
            address: r.nullable_simple_string()?,
            name: r.nullable_simple_string()?,
            auto_create_queues: r.bool()?,
            auto_created: r.bool()?,
            purge_on_no_consumers: r.bool()?,
            routing_type: r.i8()?,
            max_consumers: r.i32()?,
            ..Default::default()
        };
        let mut s = Sections(0);
        if s.next(r) {
            p.exclusive = Codec::read(r)?;
            p.last_value = Codec::read(r)?;
        }
        if s.next(r) {
            p.default_consumer_window_size = Codec::read(r)?;
        }
        if s.next(r) {
            p.last_value_key = Codec::read(r)?;
            p.non_destructive = Codec::read(r)?;
            p.consumers_before_dispatch = Codec::read(r)?;
            p.delay_before_dispatch = Codec::read(r)?;
            p.group_rebalance = Codec::read(r)?;
            p.group_buckets = Codec::read(r)?;
            p.auto_delete = Codec::read(r)?;
            p.auto_delete_delay = Codec::read(r)?;
            p.auto_delete_message_count = Codec::read(r)?;
        }
        if s.next(r) {
            p.group_first_key = Codec::read(r)?;
        }
        if s.next(r) {
            p.ring_size = Codec::read(r)?;
        }
        if s.next(r) {
            p.enabled = Codec::read(r)?;
        }
        if s.next(r) {
            p.group_rebalance_pause_dispatch = Codec::read(r)?;
        }
        if s.next(r) {
            p.configuration_managed = Codec::read(r)?;
        }
        p.sections = s.0;
        Ok(p)
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        use saranga_buffer::Codec;
        w.bool(self.exists)
            .bool(self.durable)
            .bool(self.temporary)
            .i32(self.consumer_count)
            .i64(self.message_count)
            .nullable_simple_string(self.filter_string.as_ref())
            .nullable_simple_string(self.address.as_ref())
            .nullable_simple_string(self.name.as_ref())
            .bool(self.auto_create_queues)
            .bool(self.auto_created)
            .bool(self.purge_on_no_consumers)
            .i8(self.routing_type)
            .i32(self.max_consumers);
        let n = self.sections;
        if n >= 1 {
            self.exclusive.write(w);
            self.last_value.write(w);
        }
        if n >= 2 {
            self.default_consumer_window_size.write(w);
        }
        if n >= 3 {
            self.last_value_key.write(w);
            self.non_destructive.write(w);
            self.consumers_before_dispatch.write(w);
            self.delay_before_dispatch.write(w);
            self.group_rebalance.write(w);
            self.group_buckets.write(w);
            self.auto_delete.write(w);
            self.auto_delete_delay.write(w);
            self.auto_delete_message_count.write(w);
        }
        if n >= 4 {
            self.group_first_key.write(w);
        }
        if n >= 5 {
            self.ring_size.write(w);
        }
        if n >= 6 {
            self.enabled.write(w);
        }
        if n >= 7 {
            self.group_rebalance_pause_dispatch.write(w);
        }
        if n >= 8 {
            self.configuration_managed.write(w);
        }
    }
}

wire_packet! {
    /// `SessionBindingQueryMessage`
    SessionBindingQueryMessage = types::SESS_BINDINGQUERY => {
        pub address: SimpleString,
    }
}

fn write_queue_names(w: &mut Writer, names: &[SimpleString]) {
    w.i32(names.len() as i32);
    for n in names {
        w.simple_string(n);
    }
}

/// `SessionBindingQueryResponseMessage`
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionBindingQueryResponseMessage {
    pub exists: bool,
    pub queue_names: Vec<SimpleString>,
}

impl PacketCodec for SessionBindingQueryResponseMessage {
    const TYPE: i8 = types::SESS_BINDINGQUERY_RESP;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            exists: r.bool()?,
            queue_names: read_list(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.exists);
        write_queue_names(w, &self.queue_names);
    }
}

/// `SessionBindingQueryResponseMessage_V2`: adds `auto_create_queues`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionBindingQueryResponseMessageV2 {
    pub exists: bool,
    pub queue_names: Vec<SimpleString>,
    pub auto_create_queues: bool,
}

impl PacketCodec for SessionBindingQueryResponseMessageV2 {
    const TYPE: i8 = types::SESS_BINDINGQUERY_RESP_V2;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            exists: r.bool()?,
            queue_names: read_list(r)?,
            auto_create_queues: r.bool()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.exists);
        write_queue_names(w, &self.queue_names);
        w.bool(self.auto_create_queues);
    }
}

/// `SessionBindingQueryResponseMessage_V3`: adds `auto_create_addresses`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionBindingQueryResponseMessageV3 {
    pub exists: bool,
    pub queue_names: Vec<SimpleString>,
    pub auto_create_queues: bool,
    pub auto_create_addresses: bool,
}

impl PacketCodec for SessionBindingQueryResponseMessageV3 {
    const TYPE: i8 = types::SESS_BINDINGQUERY_RESP_V3;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            exists: r.bool()?,
            queue_names: read_list(r)?,
            auto_create_queues: r.bool()?,
            auto_create_addresses: r.bool()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.exists);
        write_queue_names(w, &self.queue_names);
        w.bool(self.auto_create_queues)
            .bool(self.auto_create_addresses);
    }
}

/// The fields `SessionBindingQueryResponseMessage_V4` adds, shared with
/// `_V5`. Trailing sections: 1 `default_exclusive`, `default_last_value`;
/// 2 `default_last_value_key` to `default_delay_before_dispatch`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BindingQueryDefaults {
    pub default_purge_on_no_consumers: bool,
    pub default_max_consumers: i32,
    pub default_exclusive: Flagged<bool>,
    pub default_last_value: Flagged<bool>,
    pub default_last_value_key: Option<SimpleString>,
    pub default_non_destructive: Flagged<bool>,
    pub default_consumers_before_dispatch: Flagged<i32>,
    pub default_delay_before_dispatch: Flagged<i64>,
}

impl BindingQueryDefaults {
    fn decode(r: &mut Reader<'_>, s: &mut Sections) -> Result<Self> {
        use saranga_buffer::Codec;
        let mut d = Self {
            default_purge_on_no_consumers: r.bool()?,
            default_max_consumers: r.i32()?,
            ..Default::default()
        };
        if s.next(r) {
            d.default_exclusive = Codec::read(r)?;
            d.default_last_value = Codec::read(r)?;
        }
        if s.next(r) {
            d.default_last_value_key = Codec::read(r)?;
            d.default_non_destructive = Codec::read(r)?;
            d.default_consumers_before_dispatch = Codec::read(r)?;
            d.default_delay_before_dispatch = Codec::read(r)?;
        }
        Ok(d)
    }

    fn encode(&self, w: &mut Writer, sections: u8) {
        use saranga_buffer::Codec;
        w.bool(self.default_purge_on_no_consumers)
            .i32(self.default_max_consumers);
        if sections >= 1 {
            self.default_exclusive.write(w);
            self.default_last_value.write(w);
        }
        if sections >= 2 {
            self.default_last_value_key.write(w);
            self.default_non_destructive.write(w);
            self.default_consumers_before_dispatch.write(w);
            self.default_delay_before_dispatch.write(w);
        }
    }
}

/// `SessionBindingQueryResponseMessage_V4`: adds address defaults
/// (see [`BindingQueryDefaults`] for the trailing sections).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionBindingQueryResponseMessageV4 {
    pub exists: bool,
    pub queue_names: Vec<SimpleString>,
    pub auto_create_queues: bool,
    pub auto_create_addresses: bool,
    pub defaults: BindingQueryDefaults,
    /// Trailing sections present.
    pub sections: u8,
}

impl SessionBindingQueryResponseMessageV4 {
    pub const SECTIONS: u8 = 2;
}

impl PacketCodec for SessionBindingQueryResponseMessageV4 {
    const TYPE: i8 = types::SESS_BINDINGQUERY_RESP_V4;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let exists = r.bool()?;
        let queue_names = read_list(r)?;
        let auto_create_queues = r.bool()?;
        let auto_create_addresses = r.bool()?;
        let mut s = Sections(0);
        let defaults = BindingQueryDefaults::decode(r, &mut s)?;
        Ok(Self {
            exists,
            queue_names,
            auto_create_queues,
            auto_create_addresses,
            defaults,
            sections: s.0,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.exists);
        write_queue_names(w, &self.queue_names);
        w.bool(self.auto_create_queues)
            .bool(self.auto_create_addresses);
        self.defaults.encode(w, self.sections);
    }
}

/// `SessionBindingQueryResponseMessage_V5`: adds the supported routing
/// types as a third trailing section after the `_V4` ones.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionBindingQueryResponseMessageV5 {
    pub exists: bool,
    pub queue_names: Vec<SimpleString>,
    pub auto_create_queues: bool,
    pub auto_create_addresses: bool,
    pub defaults: BindingQueryDefaults,
    pub supports_multicast: bool,
    pub supports_anycast: bool,
    /// Trailing sections present.
    pub sections: u8,
}

impl SessionBindingQueryResponseMessageV5 {
    pub const SECTIONS: u8 = 3;
}

impl PacketCodec for SessionBindingQueryResponseMessageV5 {
    const TYPE: i8 = types::SESS_BINDINGQUERY_RESP_V5;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let exists = r.bool()?;
        let queue_names = read_list(r)?;
        let auto_create_queues = r.bool()?;
        let auto_create_addresses = r.bool()?;
        let mut s = Sections(0);
        let defaults = BindingQueryDefaults::decode(r, &mut s)?;
        let (mut supports_multicast, mut supports_anycast) = (false, false);
        // Any bytes left are taken by the `_V4` sections first, so this is
        // only reached with both of them present.
        if s.next(r) {
            supports_multicast = r.bool()?;
            supports_anycast = r.bool()?;
        }
        Ok(Self {
            exists,
            queue_names,
            auto_create_queues,
            auto_create_addresses,
            defaults,
            supports_multicast,
            supports_anycast,
            sections: s.0,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.exists);
        write_queue_names(w, &self.queue_names);
        w.bool(self.auto_create_queues)
            .bool(self.auto_create_addresses);
        self.defaults.encode(w, self.sections);
        if self.sections >= 3 {
            w.bool(self.supports_multicast).bool(self.supports_anycast);
        }
    }
}

// ---------------------------------------------------------------------------
// Consumers

/// `SessionCreateConsumerMessage`. Java writes `priority` only when the
/// connection supports consumer priority (version 130+), and reads it only
/// if bytes remain, defaulting to 0 (`getDefaultConsumerPriority`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionCreateConsumerMessage {
    pub id: i64,
    pub queue_name: SimpleString,
    pub filter_string: Option<SimpleString>,
    pub browse_only: bool,
    pub requires_response: bool,
    pub priority: i32,
}

/// `ActiveMQDefaultConfiguration.getDefaultConsumerPriority()`.
pub const DEFAULT_CONSUMER_PRIORITY: i32 = 0;

impl PacketCodec for SessionCreateConsumerMessage {
    const TYPE: i8 = types::SESS_CREATECONSUMER;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            id: r.i64()?,
            queue_name: r.simple_string()?,
            filter_string: r.nullable_simple_string()?,
            browse_only: r.bool()?,
            requires_response: r.bool()?,
            priority: if r.is_empty() {
                DEFAULT_CONSUMER_PRIORITY
            } else {
                r.i32()?
            },
        })
    }

    fn encode(&self, w: &mut Writer, ctx: &Context) {
        w.i64(self.id)
            .simple_string(&self.queue_name)
            .nullable_simple_string(self.filter_string.as_ref())
            .bool(self.browse_only)
            .bool(self.requires_response);
        if ctx.is_version_support_consumer_priority() {
            w.i32(self.priority);
        }
    }
}

wire_packet! {
    /// `SessionConsumerCloseMessage`
    SessionConsumerCloseMessage = types::SESS_CONSUMER_CLOSE => {
        pub consumer_id: i64,
    }
}

wire_packet! {
    /// `SessionConsumerFlowCreditMessage`
    SessionConsumerFlowCreditMessage = types::SESS_FLOWTOKEN => {
        pub consumer_id: i64,
        pub credits: i32,
    }
}

wire_packet! {
    /// `SessionForceConsumerDelivery`
    SessionForceConsumerDelivery = types::SESS_FORCE_CONSUMER_DELIVERY => {
        pub consumer_id: i64,
        pub sequence: i64,
    }
}

wire_packet! {
    /// `DisconnectConsumerMessage`
    DisconnectConsumerMessage = types::DISCONNECT_CONSUMER => {
        pub consumer_id: i64,
    }
}

wire_packet! {
    /// `DisconnectConsumerWithKillMessage`
    DisconnectConsumerWithKillMessage = types::DISCONNECT_CONSUMER_KILL => {
        pub node_id: Option<SimpleString>,
    }
}

// ---------------------------------------------------------------------------
// Acknowledgements and transactions

wire_packet! {
    /// `SessionAcknowledgeMessage`
    SessionAcknowledgeMessage = types::SESS_ACKNOWLEDGE => {
        pub consumer_id: i64,
        pub message_id: i64,
        pub requires_response: bool,
    }
}

wire_packet! {
    /// `SessionIndividualAcknowledgeMessage`
    SessionIndividualAcknowledgeMessage = types::SESS_INDIVIDUAL_ACKNOWLEDGE => {
        pub consumer_id: i64,
        pub message_id: i64,
        pub requires_response: bool,
    }
}

wire_packet! {
    /// `SessionExpireMessage`
    SessionExpireMessage = types::SESS_EXPIRED => {
        pub consumer_id: i64,
        pub message_id: i64,
    }
}

wire_packet! {
    /// `SessionCommitMessage` (versions before 132)
    SessionCommitMessage = types::SESS_COMMIT => {}
}

/// `SessionCommitMessage_V2` (132+). The correlation ID is read only if
/// present (Java then uses -1).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionCommitMessageV2 {
    pub correlation_id: Option<i64>,
}

impl PacketCodec for SessionCommitMessageV2 {
    const TYPE: i8 = types::SESS_COMMIT;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            correlation_id: if r.remaining() >= 8 {
                Some(r.i64()?)
            } else {
                None
            },
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        if let Some(id) = self.correlation_id {
            w.i64(id);
        }
    }
}

wire_packet! {
    /// `RollbackMessage`
    RollbackMessage = types::SESS_ROLLBACK => {
        pub consider_last_message_as_delivered: bool,
    }
}

// ---------------------------------------------------------------------------
// Session lifecycle and metadata

wire_packet! {
    /// `SessionCloseMessage`
    SessionCloseMessage = types::SESS_CLOSE => {}
}

wire_packet! {
    /// `ReattachSessionMessage`
    ReattachSessionMessage = types::REATTACH_SESSION => {
        pub name: JString,
        pub last_confirmed_command_id: i32,
    }
}

wire_packet! {
    /// `ReattachSessionResponseMessage`
    ReattachSessionResponseMessage = types::REATTACH_SESSION_RESP => {
        pub last_confirmed_command_id: i32,
        pub reattached: bool,
    }
}

wire_packet! {
    /// `SessionAddMetaDataMessage`
    SessionAddMetaDataMessage = types::SESS_ADD_METADATA => {
        pub key: JString,
        pub data: JString,
    }
}

wire_packet! {
    /// `SessionAddMetaDataMessageV2`
    SessionAddMetaDataMessageV2 = types::SESS_ADD_METADATA2 => {
        pub key: JString,
        pub data: JString,
        pub requires_confirmation: bool,
    }
}

wire_packet! {
    /// `SessionUniqueAddMetaDataMessage` (a `SessionAddMetaDataMessageV2`
    /// with its own type).
    SessionUniqueAddMetaDataMessage = types::SESS_UNIQUE_ADD_METADATA => {
        pub key: JString,
        pub data: JString,
        pub requires_confirmation: bool,
    }
}

// ---------------------------------------------------------------------------
// Producer flow control

wire_packet! {
    /// `SessionRequestProducerCreditsMessage`
    SessionRequestProducerCreditsMessage = types::SESS_PRODUCER_REQUEST_CREDITS => {
        pub credits: i32,
        pub address: SimpleString,
    }
}

wire_packet! {
    /// `SessionProducerCreditsMessage`
    SessionProducerCreditsMessage = types::SESS_PRODUCER_CREDITS => {
        pub credits: i32,
        pub address: SimpleString,
    }
}

wire_packet! {
    /// `SessionProducerCreditsFailMessage`
    SessionProducerCreditsFailMessage = types::SESS_PRODUCER_FAIL_CREDITS => {
        pub credits: i32,
        pub address: SimpleString,
    }
}

// ---------------------------------------------------------------------------
// Pre-2.0 (`_1X`) send and receive

/// `CoreMessage.sendBuffer_1X` adds this to the end-of-body position: the
/// i32 end-of-message position that follows the body in the 1.x layout.
const LEGACY_EXTRA: i32 = 4;

/// The end-of-message position Artemis writes after the body in the 1.x
/// layout (`CoreMessage.sendBuffer_1X`): the packet-relative offset of the
/// end of the message, as 1.x clients computed it.
pub fn legacy_end_of_message_position(message: &CoreMessage) -> i32 {
    let mut w = Writer::new();
    message.encode_headers_and_properties(&mut w);
    // message buffer (i32 + body + headers) + this i32 + packet header
    (4 + message.body.len() + w.len()) as i32 + LEGACY_EXTRA + BUFFER_HEADER_SPACE
}

/// Decodes a message in the 1.x layout from `r` up to `trailer` bytes before
/// the end: i32 end-of-body position (offset by 4), body, i32 end-of-message
/// position, then headers and properties.
fn decode_message_1x(r: &mut Reader<'_>, trailer: usize) -> Result<(CoreMessage, i32)> {
    let rest = r.rest();
    let split = rest
        .len()
        .checked_sub(trailer)
        .ok_or(DecodeError::UnexpectedEof {
            needed: trailer,
            remaining: rest.len(),
        })?;
    let mut mr = Reader::new(&rest[..split]);
    let end_of_body = mr.i32()?;
    let body_len = usize::try_from(end_of_body - BUFFER_HEADER_SPACE - LEGACY_EXTRA)
        .map_err(|_| DecodeError::Invalid(format!("bad 1.x end-of-body position {end_of_body}")))?;
    let body = mr.bytes(body_len)?.to_vec();
    let end_of_message = mr.i32()?;
    let mut message = CoreMessage::decode_headers_and_properties(&mut mr)?;
    message.body = body.into();
    if !mr.is_empty() {
        return Err(DecodeError::Invalid(format!(
            "{} unexpected bytes after message",
            mr.remaining()
        )));
    }
    r.bytes(split)?;
    Ok((message, end_of_message))
}

fn encode_message_1x(w: &mut Writer, message: &CoreMessage, end_of_message: i32) {
    w.i32(message.body.len() as i32 + BUFFER_HEADER_SPACE + LEGACY_EXTRA)
        .bytes(&message.body)
        .i32(end_of_message);
    message.encode_headers_and_properties(w);
}

/// `SessionSendMessage_1X` (versions before 129): the message in the 1.x
/// layout. `end_of_message_position` is ignored by Artemis on decode; it is
/// kept to re-encode exactly (Artemis writes
/// [`legacy_end_of_message_position`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSendMessage1X {
    pub message: CoreMessage,
    pub end_of_message_position: i32,
    pub requires_response: bool,
}

impl PacketCodec for SessionSendMessage1X {
    const TYPE: i8 = types::SESS_SEND;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (message, end_of_message_position) = decode_message_1x(r, 1)?;
        Ok(Self {
            message,
            end_of_message_position,
            requires_response: r.bool()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        encode_message_1x(w, &self.message, self.end_of_message_position);
        w.bool(self.requires_response);
    }
}

/// `SessionReceiveMessage_1X` (versions before 129): a delivery with the
/// message in the 1.x layout (see [`SessionSendMessage1X`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionReceiveMessage1X {
    pub message: CoreMessage,
    pub end_of_message_position: i32,
    pub consumer_id: i64,
    pub delivery_count: i32,
}

impl PacketCodec for SessionReceiveMessage1X {
    const TYPE: i8 = types::SESS_RECEIVE_MSG;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let (message, end_of_message_position) = decode_message_1x(r, 12)?;
        Ok(Self {
            message,
            end_of_message_position,
            consumer_id: r.i64()?,
            delivery_count: r.i32()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        encode_message_1x(w, &self.message, self.end_of_message_position);
        w.i64(self.consumer_id).i32(self.delivery_count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Direction, Packet, read_frame};

    fn fixture(name: &str) -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/golden/packets/session")
            .join(name);
        std::fs::read(path).unwrap()
    }

    #[test]
    fn legacy_end_of_message_matches_artemis() {
        let bytes = fixture("SessionSendMessage1X@s-127.bin");
        let (frame, _) = read_frame(&bytes).unwrap().unwrap();
        let ctx = Context::new(Direction::ToServer, 127);
        let Packet::SessionSendMessage1X(p) = Packet::decode(&frame, &ctx).unwrap() else {
            panic!("not a SessionSendMessage1X");
        };
        assert_eq!(
            p.end_of_message_position,
            legacy_end_of_message_position(&p.message)
        );
    }

    #[test]
    fn missing_trailing_sections_roundtrip() {
        let ctx = Context::new(Direction::ToServer, 136);
        let full = CreateQueueMessageV2 {
            address: "a".into(),
            queue_name: "q".into(),
            exclusive: Flagged(Some(true)),
            last_value_key: Some("k".into()),
            group_first_key: Some("g".into()),
            ring_size: Flagged(Some(5)),
            sections: CreateQueueMessageV2::SECTIONS,
            ..Default::default()
        };
        for n in 0..=CreateQueueMessageV2::SECTIONS {
            let mut p = full.clone();
            p.sections = n;
            let mut w = Writer::new();
            p.encode(&mut w, &ctx);
            let mut r = Reader::new(w.as_slice());
            let d = CreateQueueMessageV2::decode(&mut r, &ctx).unwrap();
            assert!(r.is_empty());
            assert_eq!(d.sections, n);
            let mut w2 = Writer::new();
            d.encode(&mut w2, &ctx);
            assert_eq!(w.as_slice(), w2.as_slice());
        }
    }
}
