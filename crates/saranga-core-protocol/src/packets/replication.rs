//! Journal/paging/large-message replication packets, sent from the primary
//! to the backup (and the backup's responses). Both ends decode them with
//! `ServerPacketDecoder`.
//!
//! Journal record bodies are kept as the raw bytes Java writes (an i32 length
//! then the record's encoding). Page writes carry a `PagedMessage`, which is
//! not length-delimited as a whole, so it is ported structurally down to the
//! level where every part is delimited.

use saranga_buffer::{
    Codec, DecodeError, JString, Reader, Result, SimpleString, TypedProperties, Writer,
};
use saranga_message::CoreMessage;

use crate::{Context, PacketCodec, types};

fn invalid<T>(msg: String) -> Result<T> {
    Err(DecodeError::Invalid(msg))
}

/// Reads an i32 element count followed by that many i64s.
fn read_longs(r: &mut Reader<'_>, what: &str) -> Result<Vec<i64>> {
    let count = r.len(what)?;
    let mut out = Vec::with_capacity(count.min(r.remaining() / 8));
    for _ in 0..count {
        out.push(r.i64()?);
    }
    Ok(out)
}

fn write_longs(w: &mut Writer, values: &[i64]) {
    w.i32(values.len() as i32);
    for v in values {
        w.i64(*v);
    }
}

/// `ReplicationManager.ADD_OPERATION_TYPE`.
///
/// From 2.18 (version 131) it is written as its record byte; unknown bytes
/// decode as `Add`, as in `toOperation`. Before 2.18 it is an
/// `ActiveMQBuffer` boolean, true meaning `Update` (`Event` cannot be
/// expressed and is written as false).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AddOperationType {
    Update,
    #[default]
    Add,
    Event,
}

impl AddOperationType {
    pub fn to_record(self) -> u8 {
        match self {
            Self::Update => 0,
            Self::Add => 1,
            Self::Event => 2,
        }
    }

    pub fn to_operation(record: u8) -> Self {
        match record {
            0 => Self::Update,
            2 => Self::Event,
            _ => Self::Add,
        }
    }

    fn read(r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        if ctx.is_before_two_eighteen() {
            Ok(if r.bool()? { Self::Update } else { Self::Add })
        } else {
            r.u8().map(Self::to_operation)
        }
    }

    fn write(self, w: &mut Writer, ctx: &Context) {
        if ctx.is_before_two_eighteen() {
            w.bool(self == Self::Update);
        } else {
            w.u8(self.to_record());
        }
    }
}

/// `ReplicationAddMessage`: a journal add/update record. `record_data` is
/// the record's persister encoding, i32-length-prefixed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationAddMessage {
    pub journal_id: i8,
    pub operation: AddOperationType,
    pub id: i64,
    pub journal_record_type: i8,
    pub record_data: Vec<u8>,
}

impl PacketCodec for ReplicationAddMessage {
    const TYPE: i8 = types::REPLICATION_APPEND;

    fn decode(r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        Ok(Self {
            journal_id: r.i8()?,
            operation: AddOperationType::read(r, ctx)?,
            id: r.i64()?,
            journal_record_type: r.i8()?,
            record_data: r.byte_array()?,
        })
    }

    fn encode(&self, w: &mut Writer, ctx: &Context) {
        w.i8(self.journal_id);
        self.operation.write(w, ctx);
        w.i64(self.id)
            .i8(self.journal_record_type)
            .byte_array(&self.record_data);
    }
}

/// `ReplicationAddTXMessage`: a transactional journal add/update record.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationAddTXMessage {
    pub journal_id: i8,
    pub operation: AddOperationType,
    pub tx_id: i64,
    pub id: i64,
    pub record_type: i8,
    pub record_data: Vec<u8>,
}

impl PacketCodec for ReplicationAddTXMessage {
    const TYPE: i8 = types::REPLICATION_APPEND_TX;

    fn decode(r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        Ok(Self {
            journal_id: r.i8()?,
            operation: AddOperationType::read(r, ctx)?,
            tx_id: r.i64()?,
            id: r.i64()?,
            record_type: r.i8()?,
            record_data: r.byte_array()?,
        })
    }

    fn encode(&self, w: &mut Writer, ctx: &Context) {
        w.i8(self.journal_id);
        self.operation.write(w, ctx);
        w.i64(self.tx_id)
            .i64(self.id)
            .i8(self.record_type)
            .byte_array(&self.record_data);
    }
}

wire_packet! {
    /// `ReplicationCommitMessage`: commit or rollback of a journal transaction.
    ReplicationCommitMessage = types::REPLICATION_COMMIT_ROLLBACK => {
        pub journal_id: i8,
        pub rollback: bool,
        pub tx_id: i64,
    }
}

wire_packet! {
    /// `ReplicationDeleteMessage`
    ReplicationDeleteMessage = types::REPLICATION_DELETE => {
        pub journal_id: i8,
        pub id: i64,
    }
}

wire_packet! {
    /// `ReplicationDeleteTXMessage`. `record_data` is the record's
    /// `EncodingSupport` encoding, i32-length-prefixed.
    ReplicationDeleteTXMessage = types::REPLICATION_DELETE_TX => {
        pub journal_id: i8,
        pub tx_id: i64,
        pub id: i64,
        pub record_data: Vec<u8>,
    }
}

wire_packet! {
    /// `ReplicationPrepareMessage`. `record_data` is the transaction data
    /// (usually an encoded Xid), i32-length-prefixed.
    ReplicationPrepareMessage = types::REPLICATION_PREPARE => {
        pub journal_id: i8,
        pub tx_id: i64,
        pub record_data: Vec<u8>,
    }
}

wire_packet! {
    /// `ReplicationLargeMessageBeginMessage`
    ReplicationLargeMessageBeginMessage = types::REPLICATION_LARGE_MESSAGE_BEGIN => {
        pub message_id: i64,
    }
}

/// `ReplicationLargeMessageEndMessage`. Java reads the pending record ID
/// only if 8 bytes remain and `is_delete` only if a byte remains. Note the
/// Java constructor stores the negated pending record ID; this field holds
/// the value as written.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationLargeMessageEndMessage {
    pub message_id: i64,
    pub pending_record_id: Option<i64>,
    pub is_delete: Option<bool>,
}

impl PacketCodec for ReplicationLargeMessageEndMessage {
    const TYPE: i8 = types::REPLICATION_LARGE_MESSAGE_END;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let message_id = r.i64()?;
        let pending_record_id = if r.remaining() >= 8 {
            Some(r.i64()?)
        } else {
            None
        };
        let is_delete = if r.is_empty() { None } else { Some(r.bool()?) };
        Ok(Self {
            message_id,
            pending_record_id,
            is_delete,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i64(self.message_id);
        if let Some(id) = self.pending_record_id {
            w.i64(id);
        }
        if let Some(d) = self.is_delete {
            w.bool(d);
        }
    }
}

wire_packet! {
    /// `ReplicationLargeMessageWriteMessage`: a chunk of a large message body.
    ReplicationLargeMessageWriteMessage = types::REPLICATION_LARGE_MESSAGE_WRITE => {
        pub message_id: i64,
        pub body: Vec<u8>,
    }
}

/// Reads a page number: i64 from 2.24 (version 133), i32 before.
fn read_page_number(r: &mut Reader<'_>, ctx: &Context) -> Result<i64> {
    if ctx.is_version_using_long_on_page_replication() {
        r.i64()
    } else {
        r.i32().map(i64::from)
    }
}

/// Writes a page number. Before version 133 it is truncated to i32 (Java
/// refuses to encode one above `Integer.MAX_VALUE`).
fn write_page_number(w: &mut Writer, ctx: &Context, page: i64) {
    if ctx.is_version_using_long_on_page_replication() {
        w.i64(page);
    } else {
        w.i32(page as i32);
    }
}

/// `ReplicationPageEventMessage`: a page file was closed or deleted.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationPageEventMessage {
    pub store_name: SimpleString,
    pub page_number: i64,
    pub is_delete: bool,
}

impl PacketCodec for ReplicationPageEventMessage {
    const TYPE: i8 = types::REPLICATION_PAGE_EVENT;

    fn decode(r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        Ok(Self {
            store_name: r.simple_string()?,
            page_number: read_page_number(r, ctx)?,
            is_delete: r.bool()?,
        })
    }

    fn encode(&self, w: &mut Writer, ctx: &Context) {
        w.simple_string(&self.store_name);
        write_page_number(w, ctx, self.page_number);
        w.bool(self.is_delete);
    }
}

/// `ReplicationPageWriteMessage`: a message appended to a page file.
///
/// From version 133 Java always writes the (nullable) address after the
/// paged message; it reads it whenever bytes remain, at any version.
/// `address` is `None` when absent and `Some(None)` when written as null.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationPageWriteMessage {
    pub page_number: i64,
    pub paged_message: PagedMessage,
    pub address: Option<Option<SimpleString>>,
}

impl PacketCodec for ReplicationPageWriteMessage {
    const TYPE: i8 = types::REPLICATION_PAGE_WRITE;

    fn decode(r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        let page_number = read_page_number(r, ctx)?;
        let paged_message = PagedMessage::read(r)?;
        let address = if r.is_empty() {
            None
        } else {
            Some(r.nullable_simple_string()?)
        };
        Ok(Self {
            page_number,
            paged_message,
            address,
        })
    }

    fn encode(&self, w: &mut Writer, ctx: &Context) {
        write_page_number(w, ctx, self.page_number);
        self.paged_message.write(w);
        if let Some(address) = &self.address {
            w.nullable_simple_string(address.as_ref());
        }
    }
}

/// `PagedMessageImpl` encoding: transaction ID, large-message type byte, the
/// message, then the queue IDs.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PagedMessage {
    pub transaction_id: i64,
    pub message: PagedMessageBody,
    pub queue_ids: Vec<i64>,
}

/// `PagedMessageImpl.LargeMessageType` values.
pub mod large_message_type {
    pub const NONE: i8 = 0;
    pub const CORE: i8 = 1;
    pub const OLD_CORE: i8 = -1;
    pub const NOT_CORE: i8 = 2;
}

/// The message inside a [`PagedMessage`].
#[derive(Debug, Clone, PartialEq)]
pub enum PagedMessageBody {
    /// A Core large message (type byte `CORE` or `OLD_CORE`): the
    /// `LargeMessagePersister` encoding, i32-length-prefixed, kept raw.
    CoreLarge { large_type: i8, header: Vec<u8> },
    /// Any other type byte (`NONE`, or `NOT_CORE` for AMQP large messages):
    /// a `MessagePersister` encoding, selected by its persister ID byte.
    Persisted {
        large_type: i8,
        message: PersistedMessage,
    },
}

impl Default for PagedMessageBody {
    fn default() -> Self {
        Self::Persisted {
            large_type: large_message_type::NONE,
            message: PersistedMessage::Core {
                message_id: 0,
                address: None,
                message: CoreMessage::default(),
            },
        }
    }
}

/// `PersisterIDs` values.
pub mod persister_id {
    pub const CORE_MESSAGE: u8 = 1;
    pub const AMQP_MESSAGE: u8 = 2;
    pub const AMQP_MESSAGE_V2: u8 = 3;
    pub const AMQP_LARGE_MESSAGE: u8 = 4;
    pub const AMQP_MESSAGE_V3: u8 = 5;
}

/// A message as written by `Message.getPersister().encode` (persister ID
/// byte first).
#[derive(Debug, Clone, PartialEq)]
pub enum PersistedMessage {
    /// `CoreMessagePersister`: message ID, nullable address, then
    /// `CoreMessage.persist` (i32 size + the message buffer). The buffer
    /// holds its own ID and address; the persister's copies are kept apart
    /// since Java takes them from the message's fields, not its buffer.
    Core {
        message_id: i64,
        address: Option<SimpleString>,
        message: CoreMessage,
    },
    /// `AMQPMessagePersister`.
    Amqp(AmqpMessageRecord),
    /// `AMQPMessagePersisterV2`: adds the extra properties.
    AmqpV2 {
        record: AmqpMessageRecord,
        extra_properties: ExtraProperties,
    },
    /// `AMQPMessagePersisterV3`: adds the expiration.
    AmqpV3 {
        record: AmqpMessageRecord,
        extra_properties: ExtraProperties,
        expiration: i64,
    },
    /// `AMQPLargeMessagePersister`.
    AmqpLarge(AmqpLargeMessageRecord),
}

/// The fields shared by the `AMQPMessagePersister` versions.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpMessageRecord {
    pub message_id: i64,
    pub message_format: i64,
    pub address: Option<SimpleString>,
    /// The AMQP encoding (`AMQPStandardMessage.persist`), i32-length-prefixed.
    pub data: Vec<u8>,
}

impl Codec for AmqpMessageRecord {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            message_id: r.i64()?,
            message_format: r.i64()?,
            address: r.nullable_simple_string()?,
            data: r.byte_array()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.i64(self.message_id)
            .i64(self.message_format)
            .nullable_simple_string(self.address.as_ref())
            .byte_array(&self.data);
    }
}

/// AMQP extra properties: an i32 size (`TypedProperties.getEncodeSize`, or
/// 0 for none), then the properties only if the size is not 0. Java ignores
/// the size's value otherwise, so it is kept as written.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExtraProperties {
    pub size: i32,
    pub properties: Option<TypedProperties>,
}

impl Codec for ExtraProperties {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let size = r.i32()?;
        let properties = if size != 0 {
            Some(TypedProperties::decode(r)?)
        } else {
            None
        };
        Ok(Self { size, properties })
    }

    fn write(&self, w: &mut Writer) {
        w.i32(self.size);
        if let Some(p) = &self.properties {
            p.encode(w);
        }
    }
}

/// `AMQPLargeMessagePersister` encoding. The expiration and re-encoded flag
/// are read only if enough bytes remain (in a page write they always do,
/// since the queue IDs follow).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpLargeMessageRecord {
    pub message_id: i64,
    pub durable: bool,
    pub message_format: i64,
    pub address: Option<SimpleString>,
    pub extra_properties: ExtraProperties,
    /// `AMQPLargeMessage.saveEncoding`: positions and sizes (i32s)
    /// interleaved with the header, message annotations, properties and
    /// application properties sections as AMQP values. Not length-prefixed;
    /// its end is found by walking the AMQP encodings.
    pub saved_encoding: Vec<u8>,
    pub expiration: Option<i64>,
    pub reencoded: Option<bool>,
}

impl Codec for AmqpLargeMessageRecord {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let message_id = r.i64()?;
        let durable = r.bool()?;
        let message_format = r.i64()?;
        let address = r.nullable_simple_string()?;
        let extra_properties = ExtraProperties::read(r)?;
        let saved_encoding = read_amqp_saved_encoding(r)?;
        let expiration = if r.remaining() >= 8 {
            Some(r.i64()?)
        } else {
            None
        };
        let reencoded = if r.is_empty() { None } else { Some(r.bool()?) };
        Ok(Self {
            message_id,
            durable,
            message_format,
            address,
            extra_properties,
            saved_encoding,
            expiration,
            reencoded,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.i64(self.message_id)
            .bool(self.durable)
            .i64(self.message_format)
            .nullable_simple_string(self.address.as_ref());
        self.extra_properties.write(w);
        w.bytes(&self.saved_encoding);
        if let Some(e) = self.expiration {
            w.i64(e);
        }
        if let Some(b) = self.reencoded {
            w.bool(b);
        }
    }
}

/// Reads the `AMQPLargeMessage.saveEncoding` layout as raw bytes:
/// 2 ints, value, 3 ints, value, int, value, 2 ints, value.
fn read_amqp_saved_encoding(r: &mut Reader<'_>) -> Result<Vec<u8>> {
    let start = r.rest();
    let before = r.remaining();
    for ints in [2, 3, 1, 2] {
        r.bytes(4 * ints)?;
        skip_amqp_value(r)?;
    }
    Ok(start[..before - r.remaining()].to_vec())
}

/// Skips one AMQP 1.0 encoded value (its format code determines its size).
fn skip_amqp_value(r: &mut Reader<'_>) -> Result<()> {
    let code = r.u8()?;
    if code == 0x00 {
        // Described type: descriptor, then the value.
        skip_amqp_value(r)?;
        return skip_amqp_value(r);
    }
    let n = match code >> 4 {
        0x4 => 0,
        0x5 => 1,
        0x6 => 2,
        0x7 => 4,
        0x8 => 8,
        0x9 => 16,
        0xA | 0xC | 0xE => r.u8()? as usize,
        0xB | 0xD | 0xF => r.len("AMQP value")?,
        _ => return invalid(format!("invalid AMQP format code 0x{code:02x}")),
    };
    r.bytes(n)?;
    Ok(())
}

impl PersistedMessage {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            persister_id::CORE_MESSAGE => {
                let message_id = r.i64()?;
                let address = r.nullable_simple_string()?;
                let buffer = r.byte_array()?;
                let mut mr = Reader::new(&buffer);
                let message = CoreMessage::decode(&mut mr)?;
                if !mr.is_empty() {
                    return invalid(format!(
                        "{} unexpected bytes after persisted message",
                        mr.remaining()
                    ));
                }
                Self::Core {
                    message_id,
                    address,
                    message,
                }
            }
            persister_id::AMQP_MESSAGE => Self::Amqp(AmqpMessageRecord::read(r)?),
            persister_id::AMQP_MESSAGE_V2 => Self::AmqpV2 {
                record: AmqpMessageRecord::read(r)?,
                extra_properties: ExtraProperties::read(r)?,
            },
            persister_id::AMQP_MESSAGE_V3 => Self::AmqpV3 {
                record: AmqpMessageRecord::read(r)?,
                extra_properties: ExtraProperties::read(r)?,
                expiration: r.i64()?,
            },
            persister_id::AMQP_LARGE_MESSAGE => Self::AmqpLarge(AmqpLargeMessageRecord::read(r)?),
            id => return invalid(format!("unknown message persister {id}")),
        })
    }

    fn write(&self, w: &mut Writer) {
        match self {
            Self::Core {
                message_id,
                address,
                message,
            } => {
                w.u8(persister_id::CORE_MESSAGE)
                    .i64(*message_id)
                    .nullable_simple_string(address.as_ref());
                let mut inner = Writer::new();
                message.encode(&mut inner);
                w.byte_array(inner.as_slice());
            }
            Self::Amqp(record) => {
                w.u8(persister_id::AMQP_MESSAGE);
                record.write(w);
            }
            Self::AmqpV2 {
                record,
                extra_properties,
            } => {
                w.u8(persister_id::AMQP_MESSAGE_V2);
                record.write(w);
                extra_properties.write(w);
            }
            Self::AmqpV3 {
                record,
                extra_properties,
                expiration,
            } => {
                w.u8(persister_id::AMQP_MESSAGE_V3);
                record.write(w);
                extra_properties.write(w);
                w.i64(*expiration);
            }
            Self::AmqpLarge(record) => {
                w.u8(persister_id::AMQP_LARGE_MESSAGE);
                record.write(w);
            }
        }
    }
}

impl Codec for PagedMessage {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let transaction_id = r.i64()?;
        let large_type = r.i8()?;
        let message = match large_type {
            large_message_type::CORE | large_message_type::OLD_CORE => {
                PagedMessageBody::CoreLarge {
                    large_type,
                    header: r.byte_array()?,
                }
            }
            _ => PagedMessageBody::Persisted {
                large_type,
                message: PersistedMessage::read(r)?,
            },
        };
        Ok(Self {
            transaction_id,
            message,
            queue_ids: read_longs(r, "queue IDs")?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.i64(self.transaction_id);
        match &self.message {
            PagedMessageBody::CoreLarge { large_type, header } => {
                w.i8(*large_type).byte_array(header);
            }
            PagedMessageBody::Persisted {
                large_type,
                message,
            } => {
                w.i8(*large_type);
                message.write(w);
            }
        }
        write_longs(w, &self.queue_ids);
    }
}

wire_packet! {
    /// `ReplicationResponseMessage`
    ReplicationResponseMessage = types::REPLICATION_RESPONSE => {}
}

wire_packet! {
    /// `ReplicationResponseMessageV2`
    ReplicationResponseMessageV2 = types::REPLICATION_RESPONSE_V2 => {
        pub synchronization_is_finished_acknowledgement: bool,
    }
}

/// `ReplicationPrimaryIsStoppingMessage.PrimaryStopping`, written as an
/// i32 code. Java decodes 0 as `StopCalled` and anything else as `FailOver`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrimaryStopping {
    #[default]
    StopCalled,
    FailOver,
}

/// `ReplicationPrimaryIsStoppingMessage`
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationPrimaryIsStoppingMessage {
    pub primary_stopping: PrimaryStopping,
}

impl PacketCodec for ReplicationPrimaryIsStoppingMessage {
    const TYPE: i8 = types::REPLICATION_SCHEDULED_FAILOVER;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            primary_stopping: if r.i32()? == 0 {
                PrimaryStopping::StopCalled
            } else {
                PrimaryStopping::FailOver
            },
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i32(match self.primary_stopping {
            PrimaryStopping::StopCalled => 0,
            PrimaryStopping::FailOver => 1,
        });
    }
}

/// `AbstractJournalStorageManager.JournalContent`, written as its type byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalContent {
    Bindings,
    Messages,
}

impl JournalContent {
    pub fn type_byte(self) -> u8 {
        match self {
            Self::Bindings => 0,
            Self::Messages => 1,
        }
    }

    pub fn from_type_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Self::Bindings),
            1 => Ok(Self::Messages),
            _ => invalid(format!("invalid journal content byte {b}")),
        }
    }
}

/// `ReplicationStartSyncMessage.SyncDataType`, written as its code byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDataType {
    JournalBindings,
    JournalMessages,
    LargeMessages,
    ActivationSequence,
}

impl SyncDataType {
    pub fn code(self) -> u8 {
        match self {
            Self::JournalBindings => JournalContent::Bindings.type_byte(),
            Self::JournalMessages => JournalContent::Messages.type_byte(),
            Self::LargeMessages => 2,
            Self::ActivationSequence => 3,
        }
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::JournalBindings),
            1 => Ok(Self::JournalMessages),
            2 => Ok(Self::LargeMessages),
            3 => Ok(Self::ActivationSequence),
            _ => invalid(format!("invalid sync data type {code}")),
        }
    }
}

/// The data type and file IDs of a [`ReplicationStartSyncMessage`]. For
/// `ActivationSequence` the single ID is the node activation sequence.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncData {
    pub data_type: SyncDataType,
    pub ids: Vec<i64>,
}

/// `ReplicationStartSyncMessage`. Java stops decoding after the node ID if
/// no bytes remain, so `data` is optional (Java always writes it).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationStartSyncMessage {
    pub synchronization_is_finished: bool,
    pub allows_auto_fail_back: bool,
    pub node_id: JString,
    pub data: Option<SyncData>,
}

impl PacketCodec for ReplicationStartSyncMessage {
    const TYPE: i8 = types::REPLICATION_START_FINISH_SYNC;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let synchronization_is_finished = r.bool()?;
        let allows_auto_fail_back = r.bool()?;
        let node_id = r.string()?;
        let data = if r.is_empty() {
            None
        } else {
            Some(SyncData {
                data_type: SyncDataType::from_code(r.u8()?)?,
                ids: read_longs(r, "sync file IDs")?,
            })
        };
        Ok(Self {
            synchronization_is_finished,
            allows_auto_fail_back,
            node_id,
            data,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.synchronization_is_finished)
            .bool(self.allows_auto_fail_back)
            .string(&self.node_id);
        if let Some(data) = &self.data {
            w.u8(data.data_type.code());
            write_longs(w, &data.ids);
        }
    }
}

/// `ReplicationSyncFileMessage.FileType` with its type-specific field.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncFileType {
    /// Code 0, followed by the journal content byte.
    Journal(JournalContent),
    /// Code 1, followed by the page store name.
    Page(SimpleString),
    /// Code 2.
    LargeMessage,
}

/// Everything after the file ID in a [`ReplicationSyncFileMessage`].
///
/// Java writes `data_size` and then, only when it is positive, the whole
/// chunk buffer up to its writer index, which is always the full 32 KiB read
/// buffer even when fewer bytes were read. The reader takes `data_size`
/// bytes as `data` and ignores the rest, kept here as `trailing`. A
/// `data_size` of 0 or -1 marks the end of the file.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncFileChunk {
    pub file_type: SyncFileType,
    pub data_size: i32,
    pub data: Vec<u8>,
    pub trailing: Vec<u8>,
}

/// `ReplicationSyncFileMessage`: a chunk of a journal, page or large message
/// file during initial synchronization. Java encodes only the file ID when
/// it is -1 (its own decoder cannot read that form); `chunk` is `None` then.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplicationSyncFileMessage {
    pub file_id: i64,
    pub chunk: Option<SyncFileChunk>,
}

impl PacketCodec for ReplicationSyncFileMessage {
    const TYPE: i8 = types::REPLICATION_SYNC_FILE;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let file_id = r.i64()?;
        if file_id == -1 && r.is_empty() {
            return Ok(Self {
                file_id,
                chunk: None,
            });
        }
        let file_type = match r.u8()? {
            0 => SyncFileType::Journal(JournalContent::from_type_byte(r.u8()?)?),
            1 => SyncFileType::Page(r.simple_string()?),
            2 => SyncFileType::LargeMessage,
            t => return invalid(format!("unsupported sync file type {t}")),
        };
        let data_size = r.i32()?;
        let data = if data_size > 0 {
            r.bytes(data_size as usize)?.to_vec()
        } else {
            Vec::new()
        };
        let trailing = r.rest().to_vec();
        r.bytes(trailing.len())?;
        Ok(Self {
            file_id,
            chunk: Some(SyncFileChunk {
                file_type,
                data_size,
                data,
                trailing,
            }),
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i64(self.file_id);
        let Some(chunk) = &self.chunk else {
            return;
        };
        match &chunk.file_type {
            SyncFileType::Journal(content) => {
                w.u8(0).u8(content.type_byte());
            }
            SyncFileType::Page(store) => {
                w.u8(1).simple_string(store);
            }
            SyncFileType::LargeMessage => {
                w.u8(2);
            }
        }
        w.i32(chunk.data_size)
            .bytes(&chunk.data)
            .bytes(&chunk.trailing);
    }
}
