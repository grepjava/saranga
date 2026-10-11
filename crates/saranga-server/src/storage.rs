//! Persistence: the bindings and message journals and the operations the
//! broker stores in them. Ported from `AbstractJournalStorageManager`,
//! `JournalStorageManager`, `BatchingIDGenerator` and `OperationContextImpl`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use saranga_buffer::{Reader, Writer};
use saranga_journal::{Completion, Journal, JournalConfig, JournalError, PreparedTransactionInfo};
use saranga_message::CoreMessage;
use saranga_storage::RecordCodec;
use saranga_storage::codec::{
    CursorAckRecordEncoding, DeliveryCountUpdateEncoding, DuplicateIdEncoding, IdCounterEncoding,
    PageTransactionEncoding, RefEncoding, ScheduledDeliveryEncoding,
};
use saranga_storage::message::{LargeMessageRecord, PersistedMessage};
use saranga_storage::{BatchingIdGenerator, CHECKPOINT_BATCH_SIZE, IdCounterStore};
use tokio::sync::oneshot;

use crate::message::ServerMessage;
use crate::transaction::Xid;

pub use saranga_storage::ids;

/// `CoreMessagePersister.ID`.
const CORE_PERSISTER_ID: u8 = 1;

pub type StoreResult<T> = std::result::Result<T, JournalError>;

/// A journal write the caller may wait for before answering the client
/// (an `OperationContext` line-up).
#[derive(Debug)]
pub struct Pending(oneshot::Receiver<Result<(), JournalError>>);

impl Pending {
    /// A journal completion and the handle that waits for it.
    pub fn new() -> (Completion, Pending) {
        let (tx, rx) = oneshot::channel();
        (
            Box::new(move |r| {
                let _ = tx.send(r);
            }),
            Pending(rx),
        )
    }

    pub async fn wait(self) -> Result<(), JournalError> {
        self.0
            .await
            .unwrap_or_else(|_| Err(JournalError::IllegalState("journal stopped".into())))
    }
}

/// Persistence settings from `broker.xml`.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub messages: JournalConfig,
    pub bindings: JournalConfig,
    /// `journal-sync-transactional`.
    pub sync_transactional: bool,
    /// `journal-sync-non-transactional`.
    pub sync_non_transactional: bool,
    /// `max-redelivery-records`; negative for no limit.
    pub max_redelivery_records: i32,
    /// `large-messages-directory`.
    pub large_messages: std::path::PathBuf,
    /// `paging-directory`.
    pub paging: std::path::PathBuf,
    /// `large-message-sync`.
    pub large_message_sync: bool,
}

/// Where `BatchingIDGenerator` keeps its counter: `ID_COUNTER_RECORD`s in
/// the bindings journal (`storeID`, synced; `deleteID`, not).
struct BindingsIdStore(Arc<Journal>);

impl IdCounterStore for BindingsIdStore {
    fn store_id(&self, journal_id: i64, id: i64) {
        if let Err(e) = self.0.append_add(
            journal_id,
            ids::ID_COUNTER_RECORD,
            IdCounterEncoding { id }.encode(),
            true,
            None,
        ) {
            tracing::warn!("AMQ222217: Exception during batching ID: {e}");
        }
    }

    fn delete_id(&self, journal_id: i64) {
        let _ = self.0.append_delete(journal_id, false, None);
    }
}

/// A durable message's record body (`ADD_MESSAGE_PROTOCOL`), from the
/// encoding the broker already holds. Same bytes as
/// `PersistedMessage::Core(CoreMessageRecord::new(message))`.
fn encode_message(message: &CoreMessage, encoded: &[u8]) -> Vec<u8> {
    // CoreMessagePersister.encode
    let mut w = Writer::with_capacity(encoded.len() + 64);
    w.u8(CORE_PERSISTER_ID)
        .i64(message.message_id)
        .nullable_simple_string(message.address.as_ref())
        .i32(encoded.len() as i32)
        .bytes(encoded);
    w.into_vec()
}

/// A message's record: `ADD_LARGE_MESSAGE` for a large message
/// (`LargeMessagePersister`), `ADD_MESSAGE_PROTOCOL` otherwise, written by
/// the message's persister.
fn message_record(m: &ServerMessage) -> (u8, Vec<u8>) {
    if m.is_large() {
        (ids::ADD_LARGE_MESSAGE, m.encoded().to_vec())
    } else if m.native().is_some() {
        (ids::ADD_MESSAGE_PROTOCOL, m.persisted().encode())
    } else {
        (
            ids::ADD_MESSAGE_PROTOCOL,
            encode_message(m.core(), m.encoded()),
        )
    }
}

/// A message read back from the journal.
#[derive(Debug, Clone, PartialEq)]
pub enum StoredMessage {
    /// A Core message (`CoreMessagePersister`), or a large message's
    /// headers and properties.
    Core(CoreMessage),
    /// A message another protocol's persister wrote.
    Native(PersistedMessage),
}

impl StoredMessage {
    pub fn message_id(&self) -> i64 {
        match self {
            Self::Core(m) => m.message_id,
            Self::Native(m) => m.message_id(),
        }
    }
}

/// Decodes an `ADD_LARGE_MESSAGE` body (`parseLargeMessage`).
fn decode_large_message(data: &[u8]) -> StoreResult<CoreMessage> {
    LargeMessageRecord::decode(data)
        .map(|r| r.message)
        .map_err(corrupt("large message"))
}

/// Decodes an `ADD_MESSAGE_PROTOCOL` body (`MessagePersister.decode`).
pub fn decode_message(data: &[u8]) -> StoreResult<StoredMessage> {
    match PersistedMessage::decode(data) {
        Ok(PersistedMessage::Core(rec)) => Ok(StoredMessage::Core(rec.to_message())),
        Ok(other) => Ok(StoredMessage::Native(other)),
        Err(e) => Err(JournalError::Corrupt(format!("message record: {e}"))),
    }
}

fn corrupt(what: &str) -> impl Fn(saranga_storage::DecodeError) -> JournalError + '_ {
    move |e| JournalError::Corrupt(format!("{what}: {e}"))
}

fn ref_body(queue_id: i64) -> Vec<u8> {
    RefEncoding { queue_id }.encode()
}

fn ref_queue(data: &[u8]) -> StoreResult<i64> {
    Ok(RefEncoding::decode(data)
        .map_err(corrupt("reference"))?
        .queue_id)
}

/// `XidEncoding` (`XidCodecSupport.encodeXid`).
pub fn encode_xid(xid: &Xid) -> Vec<u8> {
    use saranga_buffer::Codec;
    let mut w = Writer::new();
    xid.write(&mut w);
    w.into_vec()
}

pub fn decode_xid(data: &[u8]) -> StoreResult<Xid> {
    use saranga_buffer::Codec;
    Xid::read(&mut Reader::new(data)).map_err(|e| JournalError::Corrupt(format!("xid: {e}")))
}

/// A message reference recovered from the journal.
#[derive(Debug)]
pub struct RecoveredRef {
    pub message_id: i64,
    pub delivery_count: i32,
    /// From `SET_SCHEDULED_DELIVERY_TIME`; 0 when none was written.
    pub scheduled_time: i64,
}

/// A prepared XA transaction recovered from the journal.
#[derive(Debug)]
pub struct RecoveredTx {
    pub id: i64,
    pub xid: Xid,
    /// (queue ID, message ID) routed by the transaction.
    pub sends: Vec<(i64, i64)>,
    /// (queue ID, message ID) acknowledged by the transaction.
    pub acks: Vec<(i64, i64)>,
    pub duplicates: Vec<RecoveredDuplicate>,
    /// The page transaction of messages the transaction paged.
    pub page_txs: Vec<RecoveredPageTx>,
    /// Paged messages the transaction acknowledged.
    pub cursor_acks: Vec<RecoveredCursorRecord>,
    /// (page transaction, messages) consumed by the transaction.
    pub page_tx_updates: Vec<(i64, i32)>,
}

/// A `DUPLICATE_ID` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredDuplicate {
    pub address: String,
    pub id: Vec<u8>,
    pub record: i64,
}

impl RecoveredDuplicate {
    fn decode(record: i64, data: &[u8]) -> StoreResult<Self> {
        let d = DuplicateIdEncoding::decode(data).map_err(corrupt("duplicate ID"))?;
        Ok(Self {
            address: d.address.to_string_lossy(),
            id: d.duplicate_id,
            record,
        })
    }
}

/// A page cursor record: `ACKNOWLEDGE_CURSOR` (a paged message one queue
/// acknowledged) or `PAGE_CURSOR_COMPLETE` (a page one queue finished; the
/// message number is the page's number of messages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredCursorRecord {
    pub record: i64,
    pub queue_id: i64,
    pub page: i64,
    pub message_nr: i32,
}

impl RecoveredCursorRecord {
    fn decode(record: i64, data: &[u8]) -> StoreResult<Self> {
        let e = CursorAckRecordEncoding::decode(data).map_err(corrupt("page cursor record"))?;
        Ok(Self {
            record,
            queue_id: e.queue_id,
            page: e.page_nr,
            message_nr: e.message_nr,
        })
    }
}

/// A `PAGE_TRANSACTION` record (`PageTransactionInfoImpl`): a transaction
/// that paged messages, and how many of them are still to be consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredPageTx {
    pub record: i64,
    pub tx_id: i64,
    pub messages: i32,
}

/// What the message journal holds (`loadMessageJournal`).
#[derive(Debug, Default)]
pub struct RecoveredMessages {
    pub messages: HashMap<i64, StoredMessage>,
    /// The messages that are large: their bodies are files.
    pub large: HashSet<i64>,
    /// Per queue, in journal order.
    pub queues: HashMap<i64, Vec<RecoveredRef>>,
    pub prepared: Vec<RecoveredTx>,
    /// Committed duplicate IDs, in journal order.
    pub duplicates: Vec<RecoveredDuplicate>,
    /// `ACKNOWLEDGE_CURSOR` records.
    pub cursor_acks: Vec<RecoveredCursorRecord>,
    /// `PAGE_CURSOR_COMPLETE` records.
    pub page_completes: Vec<RecoveredCursorRecord>,
    /// Page transactions with messages left to consume.
    pub page_txs: Vec<RecoveredPageTx>,
    /// Records of types the broker does not know, for the protocols that
    /// wrote them (`journalRecordsListener`).
    pub extra: Vec<ExtraRecord>,
}

/// A committed message-journal record of a type a protocol owns, such as
/// MQTT's `MQTT_PACKET_ID_CORRELATION`.
#[derive(Debug, Clone)]
pub struct ExtraRecord {
    pub id: i64,
    pub record_type: u8,
    pub data: Vec<u8>,
}

/// The broker's journals.
pub struct Storage {
    cfg: StorageConfig,
    pub(crate) messages: Journal,
    pub(crate) bindings: Arc<Journal>,
    ids: BatchingIdGenerator<BindingsIdStore>,
}

impl Storage {
    /// Opens both journals (`JournalStorageManager.start`); nothing is loaded
    /// yet.
    pub fn open(cfg: StorageConfig) -> StoreResult<Self> {
        let messages = Journal::open(cfg.messages.clone())?;
        messages.replaceable_record(ids::UPDATE_DELIVERY_COUNT);
        messages.replaceable_record(ids::SET_SCHEDULED_DELIVERY_TIME);
        let bindings = Arc::new(Journal::open(cfg.bindings.clone())?);
        Ok(Self {
            cfg,
            messages,
            ids: BatchingIdGenerator::new(
                0,
                CHECKPOINT_BATCH_SIZE,
                BindingsIdStore(bindings.clone()),
            ),
            bindings,
        })
    }

    /// `generateID`.
    pub fn generate_id(&self) -> i64 {
        // The generator only stops with the storage.
        self.ids
            .generate_id()
            .unwrap_or_else(|_| self.ids.current_id())
    }

    /// Loads the bindings journal: the ID generator's state, and the binding
    /// records for the caller (`loadBindingJournal`). Old ID counter records
    /// are deleted, keeping the last.
    pub fn load_bindings(&self) -> StoreResult<Vec<saranga_journal::RecordInfo>> {
        let loaded = self.bindings.load_all()?;
        let mut rest = Vec::new();
        for r in loaded.committed {
            if r.user_record_type == ids::ID_COUNTER_RECORD {
                self.ids
                    .load_state(r.id, &r.data)
                    .map_err(corrupt("ID counter"))?;
            } else {
                rest.push(r);
            }
        }
        self.ids.cleanup();
        Ok(rest)
    }

    /// Loads the message journal into messages, per-queue references and
    /// prepared transactions (`loadMessageJournal`). Messages no queue
    /// refers to any more are deleted (`handleNoMessageReferences`).
    pub fn load_messages(&self) -> StoreResult<RecoveredMessages> {
        let loaded = self.messages.load_all()?;
        let mut out = RecoveredMessages::default();
        // Per queue: message IDs in ADD_REF order, as a LinkedHashMap.
        let mut order: HashMap<i64, Vec<i64>> = HashMap::new();
        // Per queue: message ID to (delivery count, scheduled time).
        let mut refs: HashMap<i64, HashMap<i64, (i32, i64)>> = HashMap::new();
        // Page transactions by transaction ID, in journal order.
        let mut page_txs: Vec<RecoveredPageTx> = Vec::new();
        // Page counter records: Saranga rebuilds the counters from the
        // pages, as Artemis does on every start, so they are deleted.
        let mut counters = Vec::new();
        for r in loaded.committed {
            match r.user_record_type {
                ids::ADD_MESSAGE_PROTOCOL => {
                    out.messages.insert(r.id, decode_message(&r.data)?);
                }
                ids::ADD_LARGE_MESSAGE => {
                    out.messages
                        .insert(r.id, StoredMessage::Core(decode_large_message(&r.data)?));
                    out.large.insert(r.id);
                }
                ids::ADD_MESSAGE => {
                    return Err(JournalError::Corrupt(
                        "This is using old journal data, export your data and import at the correct version".into(),
                    ));
                }
                ids::ADD_REF => {
                    let queue = ref_queue(&r.data)?;
                    if !out.messages.contains_key(&r.id) {
                        tracing::warn!("AMQ222031: Cannot find message {}", r.id);
                        continue;
                    }
                    let q = refs.entry(queue).or_default();
                    if q.insert(r.id, (0, 0)).is_none() {
                        order.entry(queue).or_default().push(r.id);
                    }
                }
                ids::ACKNOWLEDGE_REF => {
                    let queue = ref_queue(&r.data)?;
                    match refs.get_mut(&queue) {
                        None => tracing::warn!(
                            "AMQ222030: Cannot find queue {queue} while reloading ACKNOWLEDGE_REF for message {}",
                            r.id
                        ),
                        Some(q) => {
                            if q.remove(&r.id).is_none() {
                                tracing::warn!("AMQ222031: Cannot find message {}", r.id);
                            }
                        }
                    }
                }
                ids::UPDATE_DELIVERY_COUNT => {
                    let u = DeliveryCountUpdateEncoding::decode(&r.data)
                        .map_err(corrupt("delivery count"))?;
                    if let Some(c) = refs.get_mut(&u.queue_id).and_then(|q| q.get_mut(&r.id)) {
                        c.0 = u.count;
                    }
                }
                ids::SET_SCHEDULED_DELIVERY_TIME => {
                    let s = ScheduledDeliveryEncoding::decode(&r.data)
                        .map_err(corrupt("scheduled delivery"))?;
                    match refs.get_mut(&s.queue_id) {
                        None => tracing::error!(
                            "AMQ224011: Cannot find queue messages {} for message {} while processing scheduled messages",
                            s.queue_id,
                            r.id
                        ),
                        Some(q) => match q.get_mut(&r.id) {
                            Some(c) => c.1 = s.scheduled_delivery_time,
                            None => tracing::warn!("AMQ222031: Cannot find message {}", r.id),
                        },
                    }
                }
                ids::DUPLICATE_ID => out
                    .duplicates
                    .push(RecoveredDuplicate::decode(r.id, &r.data)?),
                ids::ACKNOWLEDGE_CURSOR => out
                    .cursor_acks
                    .push(RecoveredCursorRecord::decode(r.id, &r.data)?),
                ids::PAGE_CURSOR_COMPLETE => out
                    .page_completes
                    .push(RecoveredCursorRecord::decode(r.id, &r.data)?),
                ids::PAGE_TRANSACTION => {
                    let e = PageTransactionEncoding::decode(&r.data)
                        .map_err(corrupt("page transaction"))?;
                    if r.is_update {
                        // PageUpdateTXEncoding: messages consumed since.
                        match page_txs.iter_mut().find(|p| p.tx_id == e.transaction_id) {
                            Some(p) => p.messages -= e.records,
                            None => tracing::warn!(
                                "AMQ222041: Cannot find page transaction {}",
                                e.transaction_id
                            ),
                        }
                    } else {
                        page_txs.push(RecoveredPageTx {
                            record: r.id,
                            tx_id: e.transaction_id,
                            messages: e.records,
                        });
                    }
                }
                ids::PAGE_CURSOR_COUNTER_VALUE
                | ids::PAGE_CURSOR_COUNTER_INC
                | ids::PAGE_CURSOR_PENDING_COUNTER => counters.push(r.id),
                other => {
                    tracing::debug!("Extra record type {other}");
                    out.extra.push(ExtraRecord {
                        id: r.id,
                        record_type: other,
                        data: r.data,
                    });
                }
            }
        }
        // A page transaction whose messages were all consumed goes.
        for p in page_txs {
            if p.messages > 0 {
                out.page_txs.push(p);
            } else {
                let _ = self.messages.append_delete(p.record, false, None);
            }
        }
        if !counters.is_empty() {
            let tx = self.generate_id();
            for id in counters {
                self.messages.append_delete_tx(tx, id, Vec::new())?;
            }
            self.messages.append_commit(tx, false, None)?;
        }
        for (queue, ids) in order {
            let Some(live) = refs.get(&queue) else {
                continue;
            };
            let list: Vec<RecoveredRef> = ids
                .into_iter()
                .filter_map(|id| {
                    live.get(&id).map(|&(c, t)| RecoveredRef {
                        message_id: id,
                        delivery_count: c,
                        scheduled_time: t,
                    })
                })
                .collect();
            out.queues.insert(queue, list);
        }
        for p in loaded.prepared {
            out.prepared
                .push(self.recover_prepared(p, &mut out.messages, &mut out.large)?);
        }
        // Messages with no reference left, and none held by a prepared
        // transaction, are deleted.
        let mut referenced: HashSet<i64> = out
            .queues
            .values()
            .flat_map(|q| q.iter().map(|r| r.message_id))
            .collect();
        for p in &out.prepared {
            referenced.extend(p.sends.iter().map(|(_, m)| *m));
            referenced.extend(p.acks.iter().map(|(_, m)| *m));
        }
        let unreferenced: Vec<i64> = out
            .messages
            .keys()
            .copied()
            .filter(|id| !referenced.contains(id))
            .collect();
        for id in unreferenced {
            tracing::warn!("AMQ222129: Message {id} has no references, it will be deleted");
            out.messages.remove(&id);
            out.large.remove(&id);
            let _ = self.messages.append_delete(id, false, None);
        }
        Ok(out)
    }

    /// `loadPreparedTransactions`.
    fn recover_prepared(
        &self,
        p: PreparedTransactionInfo,
        messages: &mut HashMap<i64, StoredMessage>,
        large: &mut HashSet<i64>,
    ) -> StoreResult<RecoveredTx> {
        let xid = decode_xid(&p.extra_data)?;
        let mut tx = RecoveredTx {
            id: p.id,
            xid,
            sends: Vec::new(),
            acks: Vec::new(),
            duplicates: Vec::new(),
            page_txs: Vec::new(),
            cursor_acks: Vec::new(),
            page_tx_updates: Vec::new(),
        };
        for r in p.records {
            match r.user_record_type {
                ids::ADD_MESSAGE_PROTOCOL => {
                    messages.insert(r.id, decode_message(&r.data)?);
                }
                ids::ADD_LARGE_MESSAGE => {
                    messages.insert(r.id, StoredMessage::Core(decode_large_message(&r.data)?));
                    large.insert(r.id);
                }
                ids::ADD_REF => tx.sends.push((ref_queue(&r.data)?, r.id)),
                ids::ACKNOWLEDGE_REF => tx.acks.push((ref_queue(&r.data)?, r.id)),
                ids::DUPLICATE_ID => tx
                    .duplicates
                    .push(RecoveredDuplicate::decode(r.id, &r.data)?),
                ids::ACKNOWLEDGE_CURSOR => tx
                    .cursor_acks
                    .push(RecoveredCursorRecord::decode(r.id, &r.data)?),
                ids::PAGE_TRANSACTION => {
                    let e = PageTransactionEncoding::decode(&r.data)
                        .map_err(corrupt("page transaction"))?;
                    if r.is_update {
                        tx.page_tx_updates.push((e.transaction_id, e.records));
                    } else {
                        tx.page_txs.push(RecoveredPageTx {
                            record: r.id,
                            tx_id: e.transaction_id,
                            messages: e.records,
                        });
                    }
                }
                other => tracing::debug!("prepared transaction {} record type {other}", p.id),
            }
        }
        Ok(tx)
    }

    /// Writes the ID generator's state (`persistIdGenerator`) and closes
    /// both journals.
    pub fn stop(&self) -> StoreResult<()> {
        self.ids.stop();
        self.messages.stop()?;
        self.bindings.stop()
    }

    fn completion(&self, sync: bool) -> (Option<Completion>, Option<Pending>) {
        let (c, p) = Pending::new();
        let _ = sync;
        (Some(c), Some(p))
    }

    // Message journal ------------------------------------------------------

    /// `getMaxRecordSize` of the message journal.
    pub fn max_record_size(&self) -> usize {
        self.cfg.messages.max_record_size()
    }

    /// `storeMessage`: the message record, never synced (the reference that
    /// follows is).
    pub fn store_message(&self, message: &ServerMessage) -> StoreResult<()> {
        let (record_type, body) = message_record(message);
        self.messages
            .append_add(message.message_id(), record_type, body, false, None)
    }

    /// `storeReference`: synced when it is the last reference of a send.
    pub fn store_reference(
        &self,
        queue_id: i64,
        message_id: i64,
        last: bool,
    ) -> StoreResult<Pending> {
        let sync = last && self.cfg.sync_non_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_update(
            message_id,
            ids::ADD_REF,
            ref_body(queue_id),
            sync,
            false,
            c,
        )?;
        Ok(p.expect("pending"))
    }

    /// `storeAcknowledge`.
    pub fn store_acknowledge(&self, queue_id: i64, message_id: i64) -> StoreResult<Pending> {
        let sync = self.cfg.sync_non_transactional;
        let (c, p) = self.completion(sync);
        let found = self.messages.append_update(
            message_id,
            ids::ACKNOWLEDGE_REF,
            ref_body(queue_id),
            sync,
            false,
            c,
        )?;
        if !found {
            tracing::warn!("AMQ222178: Cannot find message {message_id} on the journal");
        }
        Ok(p.expect("pending"))
    }

    /// `deleteMessage`: never synced; a missing delete is repaired on load.
    pub fn delete_message(&self, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_delete(message_id, false, None)
            .map(|_| ())
    }

    /// `updateDeliveryCount`: `false` when nothing was written (the count is
    /// already persisted, or beyond `max-redelivery-records`).
    pub fn update_delivery_count(
        &self,
        queue_id: i64,
        message_id: i64,
        count: i32,
        persisted: i32,
    ) -> StoreResult<bool> {
        if count == persisted {
            return Ok(false);
        }
        let max = self.cfg.max_redelivery_records;
        if max >= 0 && count > max {
            return Ok(false);
        }
        let body = DeliveryCountUpdateEncoding { queue_id, count }.encode();
        // With a completion the append never waits for the sync, so a queue
        // calling this is not held up.
        self.messages.append_update(
            message_id,
            ids::UPDATE_DELIVERY_COUNT,
            body,
            self.cfg.sync_non_transactional,
            true,
            Some(Box::new(|_| {})),
        )?;
        Ok(true)
    }

    /// `updateScheduledDeliveryTime`: not written past
    /// `max-redelivery-records` deliveries.
    pub fn update_scheduled_delivery_time(
        &self,
        queue_id: i64,
        message_id: i64,
        time: i64,
        delivery_count: i32,
    ) -> StoreResult<Option<Pending>> {
        let max = self.cfg.max_redelivery_records;
        if max >= 0 && delivery_count > max {
            return Ok(None);
        }
        let sync = self.cfg.sync_non_transactional;
        let (c, p) = self.completion(sync);
        let body = ScheduledDeliveryEncoding {
            queue_id,
            scheduled_delivery_time: time,
        }
        .encode();
        let found = self.messages.append_update(
            message_id,
            ids::SET_SCHEDULED_DELIVERY_TIME,
            body,
            sync,
            true,
            c,
        )?;
        if !found {
            tracing::debug!("Record {message_id} not found");
        }
        Ok(p)
    }

    /// `updateScheduledDeliveryTimeTransactional`.
    pub fn update_scheduled_delivery_time_tx(
        &self,
        tx: i64,
        queue_id: i64,
        message_id: i64,
        time: i64,
    ) -> StoreResult<()> {
        let body = ScheduledDeliveryEncoding {
            queue_id,
            scheduled_delivery_time: time,
        }
        .encode();
        self.messages
            .append_update_tx(tx, message_id, ids::SET_SCHEDULED_DELIVERY_TIME, body)
    }

    /// `storeMessageTransactional`.
    pub fn store_message_tx(&self, tx: i64, message: &ServerMessage) -> StoreResult<()> {
        let (record_type, body) = message_record(message);
        self.messages
            .append_add_tx(tx, message.message_id(), record_type, body)
    }

    /// `storeCursorAcknowledgeTransactional`: returns the record's ID.
    pub fn store_cursor_ack_tx(
        &self,
        tx: i64,
        queue_id: i64,
        page: i64,
        message_nr: i32,
    ) -> StoreResult<i64> {
        let id = self.generate_id();
        let body = CursorAckRecordEncoding {
            queue_id,
            page_nr: page,
            message_nr,
        }
        .encode();
        self.messages
            .append_add_tx(tx, id, ids::ACKNOWLEDGE_CURSOR, body)?;
        Ok(id)
    }

    /// `storePageCompleteTransactional`: a page a queue has finished, with
    /// its number of messages. Returns the record's ID.
    pub fn store_page_complete_tx(
        &self,
        tx: i64,
        queue_id: i64,
        page: i64,
        messages: i32,
    ) -> StoreResult<i64> {
        let id = self.generate_id();
        let body = CursorAckRecordEncoding {
            queue_id,
            page_nr: page,
            message_nr: messages,
        }
        .encode();
        self.messages
            .append_add_tx(tx, id, ids::PAGE_CURSOR_COMPLETE, body)?;
        Ok(id)
    }

    /// `storePageTransaction`: the messages a transaction paged. Returns the
    /// record's ID.
    pub fn store_page_transaction_tx(
        &self,
        tx: i64,
        page_tx: i64,
        messages: i32,
    ) -> StoreResult<i64> {
        let id = self.generate_id();
        let body = PageTransactionEncoding {
            transaction_id: page_tx,
            records: messages,
        }
        .encode();
        self.messages
            .append_add_tx(tx, id, ids::PAGE_TRANSACTION, body)?;
        Ok(id)
    }

    /// `updatePageTransaction`: messages of a page transaction consumed.
    pub fn update_page_transaction_tx(
        &self,
        tx: i64,
        record: i64,
        page_tx: i64,
        consumed: i32,
    ) -> StoreResult<()> {
        let body = PageTransactionEncoding {
            transaction_id: page_tx,
            records: consumed,
        }
        .encode();
        self.messages
            .append_update_tx(tx, record, ids::PAGE_TRANSACTION, body)
    }

    /// `appendDeleteRecordTransactional` for a paging record.
    pub fn delete_record_tx(&self, tx: i64, id: i64) -> StoreResult<()> {
        self.messages.append_delete_tx(tx, id, Vec::new())
    }

    /// Adds a record of a protocol's own type to the message journal
    /// (`storeMapRecord`). Returns the write to wait for.
    pub fn add_record(
        &self,
        id: i64,
        record_type: u8,
        body: Vec<u8>,
        sync: bool,
    ) -> StoreResult<Pending> {
        let (c, p) = Pending::new();
        self.messages
            .append_add(id, record_type, body, sync, Some(c))?;
        Ok(p)
    }

    /// Adds a record of a protocol's own type in the journal transaction
    /// `tx`.
    pub fn add_record_tx(
        &self,
        tx: i64,
        id: i64,
        record_type: u8,
        body: Vec<u8>,
    ) -> StoreResult<()> {
        self.messages.append_add_tx(tx, id, record_type, body)
    }

    /// `tryAppendDeleteRecord`, not synced: `deletePageComplete`,
    /// `deleteCursorAcknowledge`, `deletePageTransactional`.
    pub fn delete_record(&self, id: i64) -> StoreResult<()> {
        self.messages.append_delete(id, false, None).map(|_| ())
    }

    /// `journal-sync-transactional`.
    pub fn sync_transactional(&self) -> bool {
        self.cfg.sync_transactional
    }

    /// `journal-sync-non-transactional`.
    pub fn sync_non_transactional(&self) -> bool {
        self.cfg.sync_non_transactional
    }

    /// `storeReferenceTransactional`.
    pub fn store_reference_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_update_tx(tx, message_id, ids::ADD_REF, ref_body(queue_id))
    }

    /// `storeAcknowledgeTransactional`.
    pub fn store_acknowledge_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_update_tx(tx, message_id, ids::ACKNOWLEDGE_REF, ref_body(queue_id))
    }

    /// `prepare`: the Xid is the prepare record's extra data.
    pub fn prepare(&self, tx: i64, xid: &Xid) -> StoreResult<Pending> {
        let sync = self.cfg.sync_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_prepare(tx, encode_xid(xid), sync, c)?;
        Ok(p.expect("pending"))
    }

    /// `commit`.
    pub fn commit(&self, tx: i64) -> StoreResult<Pending> {
        let sync = self.cfg.sync_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_commit(tx, sync, c)?;
        Ok(p.expect("pending"))
    }

    /// `rollback`.
    pub fn rollback(&self, tx: i64) -> StoreResult<Pending> {
        let sync = self.cfg.sync_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_rollback(tx, sync, c)?;
        Ok(p.expect("pending"))
    }

    /// Waits for everything written so far (`afterCompleteOperations` with
    /// nothing of its own).
    pub fn written(&self) -> StoreResult<Pending> {
        let (c, p) = Pending::new();
        self.messages.on_written(c)?;
        Ok(p)
    }

    // Bindings journal -------------------------------------------------------

    /// Adds a binding record (`addQueueBinding`, `addAddressBinding`) in its
    /// own transaction, committed and synced before returning
    /// (`commitBindings`).
    pub fn add_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_add_tx(tx, id, user_type, body)?;
        self.bindings.append_commit(tx, true, None)
    }

    /// Replaces a binding record under a new ID (`updateAddressInfo`:
    /// `deleteAddressBinding` then `addAddressBinding`).
    pub fn replace_binding(
        &self,
        old_id: i64,
        new_id: i64,
        user_type: u8,
        body: Vec<u8>,
    ) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_delete_tx(tx, old_id, Vec::new())?;
        self.bindings.append_add_tx(tx, new_id, user_type, body)?;
        self.bindings.append_commit(tx, true, None)
    }

    /// Updates a binding record in place (`updateQueueBinding`) and
    /// commits.
    pub fn update_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_update_tx(tx, id, user_type, body)?;
        self.bindings.append_commit(tx, true, None)
    }

    /// Deletes a binding record (`deleteQueueBinding`,
    /// `deleteAddressBinding`) and commits.
    pub fn delete_binding(&self, id: i64) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_delete_tx(tx, id, Vec::new())?;
        self.bindings.append_commit(tx, true, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_buffer::SimpleString;

    fn config(dir: &std::path::Path) -> StorageConfig {
        StorageConfig {
            messages: JournalConfig {
                datasync: false,
                file_size: 64 * 1024,
                ..JournalConfig::messages(dir.join("journal"))
            },
            bindings: JournalConfig {
                datasync: false,
                ..JournalConfig::bindings(dir.join("bindings"))
            },
            sync_transactional: true,
            sync_non_transactional: true,
            max_redelivery_records: 10,
            large_messages: dir.join("largemessages"),
            paging: dir.join("paging"),
            large_message_sync: false,
        }
    }

    #[test]
    fn message_encoding_matches_the_core_persister() {
        use saranga_storage::message::CoreMessageRecord;
        let m = CoreMessage {
            message_id: 99,
            address: Some(SimpleString::from("addr")),
            durable: true,
            body: vec![1, 2, 3],
            ..Default::default()
        };
        let mut w = Writer::new();
        m.encode(&mut w);
        let ours = encode_message(&m, w.as_slice());
        let theirs = PersistedMessage::Core(CoreMessageRecord::new(m.clone())).encode();
        assert_eq!(ours, theirs);
        assert_eq!(decode_message(&ours).unwrap(), StoredMessage::Core(m));
    }

    #[test]
    fn ids_continue_after_a_clean_stop_and_jump_after_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        s.load_messages().unwrap();
        let first: Vec<i64> = (0..5).map(|_| s.generate_id()).collect();
        assert_eq!(first, vec![0, 2, 3, 4, 5]);
        s.stop().unwrap();
        drop(s);

        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        let next = s.generate_id();
        assert!(next > 5, "{next}");
        drop(s); // a crash: no stop record

        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        assert!(s.generate_id() > i32::MAX as i64);
    }

    #[test]
    fn messages_refs_acks_and_counts_recover() {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        s.load_messages().unwrap();
        let msg = |id: i64| CoreMessage {
            message_id: id,
            address: Some(SimpleString::from("q")),
            durable: true,
            ..Default::default()
        };
        for id in [10, 11, 12] {
            s.store_message(&ServerMessage::new(msg(id))).unwrap();
            s.store_reference(1, id, false).unwrap();
            s.store_reference(2, id, true).unwrap();
        }
        s.store_acknowledge(1, 10).unwrap();
        s.update_delivery_count(2, 11, 3, 0).unwrap();
        assert!(!s.update_delivery_count(2, 11, 11, 3).unwrap());
        s.store_acknowledge(1, 12).unwrap();
        s.store_acknowledge(2, 12).unwrap();
        s.delete_message(12).unwrap();
        s.stop().unwrap();
        drop(s);

        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        let r = s.load_messages().unwrap();
        let q = |id: i64| -> Vec<(i64, i32)> {
            r.queues[&id]
                .iter()
                .map(|x| (x.message_id, x.delivery_count))
                .collect()
        };
        assert_eq!(q(1), vec![(11, 0)]);
        assert_eq!(q(2), vec![(10, 0), (11, 3)]);
        assert_eq!(r.messages.len(), 2);
        assert!(
            matches!(&r.messages[&11], StoredMessage::Core(m) if m.address == Some(SimpleString::from("q")))
        );
    }

    #[test]
    fn prepared_transaction_recovers_with_its_xid() {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        s.load_messages().unwrap();
        let m = CoreMessage {
            message_id: 7,
            durable: true,
            ..Default::default()
        };
        let tx = s.generate_id();
        s.store_message_tx(tx, &ServerMessage::new(m)).unwrap();
        s.store_reference_tx(tx, 3, 7).unwrap();
        let xid = Xid {
            format_id: 5,
            branch_qualifier: b"b".to_vec(),
            global_transaction_id: b"g".to_vec(),
        };
        s.prepare(tx, &xid).unwrap();
        s.stop().unwrap();
        drop(s);

        let s = Storage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        let r = s.load_messages().unwrap();
        assert_eq!(r.prepared.len(), 1);
        assert_eq!(r.prepared[0].xid, xid);
        assert_eq!(r.prepared[0].sends, vec![(3, 7)]);
        assert!(r.messages.contains_key(&7));
        assert!(r.queues.is_empty());
    }
}
