//! Persistence: the bindings and message journals and the operations the
//! broker stores in them. Ported from `AbstractJournalStorageManager`,
//! `JournalStorageManager`, `BatchingIDGenerator` and `OperationContextImpl`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use saranga_buffer::{Reader, Writer};
use saranga_journal::{Completion, CriticalListener, Journal, JournalConfig, JournalError};
use saranga_message::CoreMessage;
use saranga_storage::RecordCodec;
use saranga_storage::codec::{CursorAckRecordEncoding, DuplicateIdEncoding, RefEncoding};
use saranga_storage::message::{LargeMessageRecord, PersistedMessage};
use tokio::sync::oneshot;

use crate::message::ServerMessage;
use crate::transaction::Xid;

pub use saranga_storage::ids;

mod database;
mod journal;

pub use database::{
    AddressRow, DatabaseConfig, DatabaseStorage, GenericRow, QueueRow, ReferenceRow, StoredRow,
    Tables, read_tables,
};
pub use journal::JournalStorage;

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
    /// The relational store in place of the journals.
    pub database: Option<DatabaseConfig>,
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
    if m.native().is_some() {
        // Large or not, its persister writes it (`AMQPLargeMessagePersister`).
        (ids::ADD_MESSAGE_PROTOCOL, m.persisted().encode())
    } else if m.is_large() {
        (ids::ADD_LARGE_MESSAGE, m.encoded().to_vec())
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
pub fn decode_large_message(data: &[u8]) -> StoreResult<CoreMessage> {
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
    /// Per queue, the (messages, bytes) paged in the store, not read here.
    pub paged: HashMap<i64, (i64, i64)>,
    /// Records of types the broker does not know, for the protocols that
    /// wrote them (`journalRecordsListener`).
    pub extra: Vec<ExtraRecord>,
    /// `HEURISTIC_COMPLETION` records: the record, the Xid, and whether it
    /// was committed.
    pub heuristics: Vec<(i64, Xid, bool)>,
}

/// A committed message-journal record of a type a protocol owns, such as
/// MQTT's `MQTT_PACKET_ID_CORRELATION`.
#[derive(Debug, Clone)]
pub struct ExtraRecord {
    pub id: i64,
    pub record_type: u8,
    pub data: Vec<u8>,
}

/// What the broker persists, whatever holds it: Artemis's `StorageManager`.
pub trait Store: Send + Sync {
    /// The longest write in progress on either journal, and its ID.
    fn writing(&self) -> Option<(std::time::Duration, u64)>;

    /// The message journal's directory.
    fn journal_directory(&self) -> &std::path::Path;

    /// The broker's `IOCriticalErrorListener`, told of the first I/O error
    /// writing the journals, the page files or the large message files.
    fn set_critical_listener(&self, listener: CriticalListener);

    /// `onIOError` of a file outside the journals (a page or large message
    /// file).
    fn critical_io(&self, e: &std::io::Error, file: Option<&Path>);

    /// `generateID`.
    fn generate_id(&self) -> i64;

    /// Loads the bindings journal: the ID generator's state, and the binding
    /// records for the caller (`loadBindingJournal`). Old ID counter records
    /// are deleted, keeping the last.
    fn load_bindings(&self) -> StoreResult<Vec<saranga_journal::RecordInfo>>;

    /// Loads the message journal into messages, per-queue references and
    /// prepared transactions (`loadMessageJournal`). Messages no queue
    /// refers to any more are deleted (`handleNoMessageReferences`).
    fn load_messages(&self) -> StoreResult<RecoveredMessages>;

    /// Writes the ID generator's state (`persistIdGenerator`) and closes
    /// both journals.
    fn stop(&self) -> StoreResult<()>;

    /// `getMaxRecordSize` of the message journal.
    fn max_record_size(&self) -> usize;

    /// The journal retention settings, if any.
    fn retention(&self) -> Option<&saranga_journal::Retention>;

    /// `forceBackup`: the current journal file is copied to the history.
    fn force_backup(&self) -> StoreResult<()>;

    /// `isHistory`: journal retention keeps a copy of each journal file.
    fn is_history(&self) -> bool;

    /// `pageWrite` with retention: a paged message and its references are
    /// written to the journal as events, so its history holds them too.
    fn page_write_history(&self, message: &ServerMessage, queue_ids: &[i64]);

    /// `historyBody`: a large message's body, as it is written, recorded
    /// as `ADD_MESSAGE_BODY` events of at most 10 KiB.
    fn large_body_history(&self, message_id: i64, bytes: &[u8]);

    /// `storeMessage`: the message record, never synced (the reference that
    /// follows is).
    fn store_message(&self, message: &ServerMessage) -> StoreResult<()>;

    /// `storeReference`: synced when it is the last reference of a send.
    fn store_reference(&self, queue_id: i64, message_id: i64, last: bool) -> StoreResult<Pending>;

    /// `storeAcknowledge`.
    fn store_acknowledge(&self, queue_id: i64, message_id: i64) -> StoreResult<Pending>;

    /// `deleteMessage`: never synced; a missing delete is repaired on load.
    fn delete_message(&self, message_id: i64) -> StoreResult<()>;

    /// `updateDeliveryCount`: `false` when nothing was written (the count is
    /// already persisted, or beyond `max-redelivery-records`).
    fn update_delivery_count(
        &self,
        queue_id: i64,
        message_id: i64,
        count: i32,
        persisted: i32,
    ) -> StoreResult<bool>;

    /// `updateScheduledDeliveryTime`: not written past
    /// `max-redelivery-records` deliveries.
    fn update_scheduled_delivery_time(
        &self,
        queue_id: i64,
        message_id: i64,
        time: i64,
        delivery_count: i32,
    ) -> StoreResult<Option<Pending>>;

    /// `updateScheduledDeliveryTimeTransactional`.
    fn update_scheduled_delivery_time_tx(
        &self,
        tx: i64,
        queue_id: i64,
        message_id: i64,
        time: i64,
    ) -> StoreResult<()>;

    /// `storeMessageTransactional`.
    fn store_message_tx(&self, tx: i64, message: &ServerMessage) -> StoreResult<()>;

    /// `storeCursorAcknowledgeTransactional`: returns the record's ID.
    fn store_cursor_ack_tx(
        &self,
        tx: i64,
        queue_id: i64,
        page: i64,
        message_nr: i32,
    ) -> StoreResult<i64>;

    /// `storePageCompleteTransactional`: a page a queue has finished, with
    /// its number of messages. Returns the record's ID.
    fn store_page_complete_tx(
        &self,
        tx: i64,
        queue_id: i64,
        page: i64,
        messages: i32,
    ) -> StoreResult<i64>;

    /// `storePageTransaction`: the messages a transaction paged. Returns the
    /// record's ID.
    fn store_page_transaction_tx(&self, tx: i64, page_tx: i64, messages: i32) -> StoreResult<i64>;

    /// `updatePageTransaction`: messages of a page transaction consumed.
    fn update_page_transaction_tx(
        &self,
        tx: i64,
        record: i64,
        page_tx: i64,
        consumed: i32,
    ) -> StoreResult<()>;

    /// `appendDeleteRecordTransactional` for a paging record.
    fn delete_record_tx(&self, tx: i64, id: i64) -> StoreResult<()>;

    /// Adds a record of a protocol's own type to the message journal
    /// (`storeMapRecord`). Returns the write to wait for.
    fn add_record(
        &self,
        id: i64,
        record_type: u8,
        body: Vec<u8>,
        sync: bool,
    ) -> StoreResult<Pending>;

    /// Adds a record of a protocol's own type in the journal transaction
    /// `tx`.
    fn add_record_tx(&self, tx: i64, id: i64, record_type: u8, body: Vec<u8>) -> StoreResult<()>;

    /// `storeHeuristicCompletion`: a branch management completed, synced.
    fn store_heuristic_completion(&self, id: i64, xid: &Xid, commit: bool) -> StoreResult<Pending>;

    /// `deleteHeuristicCompletion`, synced.
    fn delete_heuristic_completion(&self, id: i64) -> StoreResult<Pending>;

    /// `tryAppendDeleteRecord`, not synced: `deletePageComplete`,
    /// `deleteCursorAcknowledge`, `deletePageTransactional`.
    fn delete_record(&self, id: i64) -> StoreResult<()>;

    /// `journal-sync-transactional`.
    fn sync_transactional(&self) -> bool;

    /// `journal-sync-non-transactional`.
    fn sync_non_transactional(&self) -> bool;

    /// `storeReferenceTransactional`.
    fn store_reference_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()>;

    /// `storeAcknowledgeTransactional`.
    fn store_acknowledge_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()>;

    /// `prepare`: the Xid is the prepare record's extra data.
    fn prepare(&self, tx: i64, xid: &Xid) -> StoreResult<Pending>;

    /// `commit`.
    fn commit(&self, tx: i64) -> StoreResult<Pending>;

    /// `rollback`.
    fn rollback(&self, tx: i64) -> StoreResult<Pending>;

    /// Waits for everything written so far (`afterCompleteOperations` with
    /// nothing of its own).
    fn written(&self) -> StoreResult<Pending>;

    /// Adds a binding record (`addQueueBinding`, `addAddressBinding`) in its
    /// own transaction, committed and synced before returning
    /// (`commitBindings`).
    fn add_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()>;

    /// Replaces a binding record under a new ID (`updateAddressInfo`:
    /// `deleteAddressBinding` then `addAddressBinding`).
    fn replace_binding(
        &self,
        old_id: i64,
        new_id: i64,
        user_type: u8,
        body: Vec<u8>,
    ) -> StoreResult<()>;

    /// Updates a binding record in place (`updateQueueBinding`) and
    /// commits.
    fn update_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()>;

    /// Deletes a binding record (`deleteQueueBinding`,
    /// `deleteAddressBinding`) and commits.
    fn delete_binding(&self, id: i64) -> StoreResult<()>;

    /// Adds a bindings record outside a transaction, synced
    /// (`bindingsJournal.appendAddRecord(..., true)`), under a new ID it
    /// returns.
    fn add_binding_record(&self, user_type: u8, body: Vec<u8>) -> StoreResult<i64>;

    /// Adds a bindings record with this ID outside a transaction, synced
    /// (`addGrouping`).
    fn add_binding_record_with_id(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()>;

    /// Deletes bindings records in one transaction and commits it
    /// (`deleteGrouping` for each, then `commitBindings`).
    fn delete_bindings(&self, ids: &[i64]) -> StoreResult<()>;

    /// Deletes a bindings record outside a transaction, if it is there
    /// (`tryAppendDeleteRecord`).
    fn delete_binding_record(&self, id: i64) -> StoreResult<()>;

    /// `storeConfiguration`: a persisted configuration (address settings,
    /// a divert) under a new record ID it returns. The record it replaces,
    /// if any, is deleted in the same transaction; a new one is added
    /// outside any.
    fn store_configuration(
        &self,
        user_type: u8,
        body: Vec<u8>,
        replaces: Option<i64>,
    ) -> StoreResult<i64>;

    /// `storeQueueStatus` with `PAUSED`.
    fn store_queue_status(&self, queue_id: i64) -> StoreResult<i64>;

    /// `storeAddressStatus` with `PAUSED`.
    fn store_address_status(&self, address_id: i64) -> StoreResult<i64>;

    /// Whether paged messages stay here rather than in page files.
    fn pages_in_store(&self) -> bool {
        false
    }

    /// Keeps a message for its queues as paged, in the transaction if any.
    fn store_paged(
        &self,
        _tx: Option<i64>,
        _message: &ServerMessage,
        _queue_ids: &[i64],
    ) -> StoreResult<Option<Pending>> {
        Err(JournalError::IllegalState(
            "this store pages to files".into(),
        ))
    }

    /// Takes up to `limit` of a queue's paged messages out of paging, oldest
    /// first, each with whether it is large.
    fn read_paged(&self, _queue_id: i64, _limit: i64) -> StoreResult<Vec<(StoredMessage, bool)>> {
        Ok(Vec::new())
    }

    /// Up to `limit` of a queue's paged messages after the ID `after`, oldest
    /// first, left paged: what a management operation looks through.
    fn scan_paged(
        &self,
        _queue_id: i64,
        _limit: i64,
        _after: i64,
    ) -> StoreResult<Vec<(StoredMessage, bool)>> {
        Ok(Vec::new())
    }

    /// Returns once what was sent is written: rows a scan acted on are gone
    /// before the queue reads paged messages again.
    fn settle(&self) -> StoreResult<()> {
        Ok(())
    }

    /// A commit that is not synced, as a housekeeping delete's is. With
    /// `wait` false it does not wait for the write either.
    fn commit_unsynced(&self, tx: i64, wait: bool) -> StoreResult<()>;

    /// The message and bindings journals, for replication; `None` for a
    /// store without them.
    fn journals(&self) -> Option<(&Journal, &Journal)>;
}

/// The broker's store.
pub struct Storage(Box<dyn Store>);

impl Storage {
    /// Opens the store `cfg` names; nothing is loaded yet.
    pub fn open(cfg: StorageConfig) -> StoreResult<Self> {
        Ok(match cfg.database.clone() {
            Some(database) => Self(Box::new(DatabaseStorage::open(cfg, &database)?)),
            None => Self(Box::new(JournalStorage::open(cfg)?)),
        })
    }
}

impl std::ops::Deref for Storage {
    type Target = dyn Store;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
