//! Persistence: the bindings and message journals and the operations the
//! broker stores in them. Ported from `AbstractJournalStorageManager`,
//! `JournalStorageManager`, `BatchingIDGenerator` and `OperationContextImpl`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use saranga_buffer::{Reader, SimpleString, Writer};
use saranga_journal::{Completion, Journal, JournalConfig, JournalError, PreparedTransactionInfo};
use saranga_message::CoreMessage;
use tokio::sync::oneshot;

use crate::transaction::Xid;

/// `JournalRecordIds` used by the broker.
pub mod ids {
    pub const QUEUE_BINDING_RECORD: u8 = 21;
    pub const QUEUE_STATUS_RECORD: u8 = 22;
    pub const ID_COUNTER_RECORD: u8 = 24;
    pub const ADD_LARGE_MESSAGE: u8 = 30;
    pub const ADD_MESSAGE: u8 = 31;
    pub const ADD_REF: u8 = 32;
    pub const ACKNOWLEDGE_REF: u8 = 33;
    pub const UPDATE_DELIVERY_COUNT: u8 = 34;
    pub const SET_SCHEDULED_DELIVERY_TIME: u8 = 36;
    pub const DUPLICATE_ID: u8 = 37;
    pub const ADDRESS_BINDING_RECORD: u8 = 44;
    pub const ADD_MESSAGE_PROTOCOL: u8 = 45;
    pub const ADDRESS_STATUS_RECORD: u8 = 46;
}

/// `CoreMessagePersister.ID`.
const CORE_PERSISTER_ID: u8 = 1;

/// `AbstractJournalStorageManager.CHECKPOINT_BATCH_SIZE`.
const CHECKPOINT_BATCH_SIZE: i64 = i32::MAX as i64;

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
}

/// `BatchingIDGenerator`: IDs for messages, queues, addresses and
/// transactions, with a checkpoint in the bindings journal so a restart
/// never reuses one.
struct IdGenerator {
    counter: i64,
    next_id: i64,
    cleanup: Vec<i64>,
}

/// A durable message's record bodies (`ADD_MESSAGE_PROTOCOL`).
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

/// Decodes an `ADD_MESSAGE_PROTOCOL` body written by the Core persister.
pub fn decode_message(data: &[u8]) -> StoreResult<CoreMessage> {
    let bad = |e: saranga_buffer::DecodeError| JournalError::Corrupt(format!("message record: {e}"));
    let mut r = Reader::new(data);
    let persister = r.u8().map_err(bad)?;
    if persister != CORE_PERSISTER_ID {
        return Err(JournalError::Corrupt(format!(
            "message persister {persister} is not supported yet"
        )));
    }
    let id = r.i64().map_err(bad)?;
    let address = r.nullable_simple_string().map_err(bad)?;
    let size = r.i32().map_err(bad)?;
    let body = r.bytes(size.max(0) as usize).map_err(bad)?;
    let mut message = CoreMessage::decode(&mut Reader::new(body)).map_err(bad)?;
    message.message_id = id;
    message.address = address;
    Ok(message)
}

fn long(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn read_long(data: &[u8]) -> StoreResult<i64> {
    data.get(..8)
        .map(|b| i64::from_be_bytes(b.try_into().expect("8 bytes")))
        .ok_or_else(|| JournalError::Corrupt("record shorter than a long".into()))
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
}

/// What the message journal holds (`loadMessageJournal`).
#[derive(Debug, Default)]
pub struct RecoveredMessages {
    pub messages: HashMap<i64, CoreMessage>,
    /// Per queue, in journal order.
    pub queues: HashMap<i64, Vec<RecoveredRef>>,
    pub prepared: Vec<RecoveredTx>,
}

/// The broker's journals.
pub struct Storage {
    cfg: StorageConfig,
    pub(crate) messages: Journal,
    pub(crate) bindings: Journal,
    ids: Mutex<IdGenerator>,
}

impl Storage {
    /// Opens both journals (`JournalStorageManager.start`); nothing is loaded
    /// yet.
    pub fn open(cfg: StorageConfig) -> StoreResult<Self> {
        let messages = Journal::open(cfg.messages.clone())?;
        messages.replaceable_record(ids::UPDATE_DELIVERY_COUNT);
        messages.replaceable_record(ids::SET_SCHEDULED_DELIVERY_TIME);
        let bindings = Journal::open(cfg.bindings.clone())?;
        Ok(Self {
            cfg,
            messages,
            bindings,
            ids: Mutex::new(IdGenerator {
                counter: 0,
                next_id: 0,
                cleanup: Vec::new(),
            }),
        })
    }

    fn ids(&self) -> std::sync::MutexGuard<'_, IdGenerator> {
        self.ids.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn store_id(&self, journal_id: i64, id: i64) {
        if let Err(e) = self
            .bindings
            .append_add(journal_id, ids::ID_COUNTER_RECORD, long(id), true, None)
        {
            tracing::warn!("AMQ222217: Exception during batching ID: {e}");
        }
    }

    /// `generateID`.
    pub fn generate_id(&self) -> i64 {
        let mut g = self.ids();
        let id = g.counter;
        g.counter += 1;
        if id >= g.next_id {
            // saveCheckPoint
            g.next_id += CHECKPOINT_BATCH_SIZE;
            let journal_id = g.counter;
            g.counter += 1;
            let next = g.next_id;
            drop(g);
            self.store_id(journal_id, next);
        }
        id
    }

    /// Loads the bindings journal: the ID generator's state, and the binding
    /// records for the caller (`loadBindingJournal`). Old ID counter records
    /// are deleted, keeping the last.
    pub fn load_bindings(&self) -> StoreResult<Vec<saranga_journal::RecordInfo>> {
        let loaded = self.bindings.load_all()?;
        let mut rest = Vec::new();
        {
            let mut g = self.ids();
            for r in loaded.committed {
                if r.user_record_type == ids::ID_COUNTER_RECORD {
                    // loadState
                    g.cleanup.push(r.id);
                    g.next_id = read_long(&r.data)? + 1;
                    g.counter = g.next_id;
                } else {
                    rest.push(r);
                }
            }
        }
        // cleanup: every counter record but the last.
        let cleanup = std::mem::take(&mut self.ids().cleanup);
        if let Some((_, older)) = cleanup.split_last() {
            for &id in older {
                let _ = self.bindings.append_delete(id, false, None);
            }
        }
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
        let mut refs: HashMap<i64, HashMap<i64, i32>> = HashMap::new();
        for r in loaded.committed {
            match r.user_record_type {
                ids::ADD_MESSAGE_PROTOCOL => {
                    out.messages.insert(r.id, decode_message(&r.data)?);
                }
                ids::ADD_MESSAGE => {
                    return Err(JournalError::Corrupt(
                        "This is using old journal data, export your data and import at the correct version".into(),
                    ));
                }
                ids::ADD_REF => {
                    let queue = read_long(&r.data)?;
                    if !out.messages.contains_key(&r.id) {
                        tracing::warn!("AMQ222031: Cannot find message {}", r.id);
                        continue;
                    }
                    let q = refs.entry(queue).or_default();
                    if q.insert(r.id, 0).is_none() {
                        order.entry(queue).or_default().push(r.id);
                    }
                }
                ids::ACKNOWLEDGE_REF => {
                    let queue = read_long(&r.data)?;
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
                    let queue = read_long(&r.data)?;
                    let count = r
                        .data
                        .get(8..12)
                        .map(|b| i32::from_be_bytes(b.try_into().expect("4 bytes")))
                        .ok_or_else(|| JournalError::Corrupt("delivery count record".into()))?;
                    if let Some(c) = refs.get_mut(&queue).and_then(|q| q.get_mut(&r.id)) {
                        *c = count;
                    }
                }
                // Scheduled delivery and duplicate detection come with the
                // Phase 4 queue semantics; their records stay in the journal.
                other => tracing::debug!("Extra record type {other}"),
            }
        }
        for (queue, ids) in order {
            let Some(live) = refs.get(&queue) else {
                continue;
            };
            let list: Vec<RecoveredRef> = ids
                .into_iter()
                .filter_map(|id| {
                    live.get(&id).map(|&c| RecoveredRef {
                        message_id: id,
                        delivery_count: c,
                    })
                })
                .collect();
            out.queues.insert(queue, list);
        }
        for p in loaded.prepared {
            out.prepared.push(self.recover_prepared(p, &mut out.messages)?);
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
            let _ = self.messages.append_delete(id, false, None);
        }
        Ok(out)
    }

    /// `loadPreparedTransactions`.
    fn recover_prepared(
        &self,
        p: PreparedTransactionInfo,
        messages: &mut HashMap<i64, CoreMessage>,
    ) -> StoreResult<RecoveredTx> {
        let xid = decode_xid(&p.extra_data)?;
        let mut tx = RecoveredTx {
            id: p.id,
            xid,
            sends: Vec::new(),
            acks: Vec::new(),
        };
        for r in p.records {
            match r.user_record_type {
                ids::ADD_MESSAGE_PROTOCOL => {
                    messages.insert(r.id, decode_message(&r.data)?);
                }
                ids::ADD_REF => tx.sends.push((read_long(&r.data)?, r.id)),
                ids::ACKNOWLEDGE_REF => tx.acks.push((read_long(&r.data)?, r.id)),
                other => tracing::debug!("prepared transaction {} record type {other}", p.id),
            }
        }
        Ok(tx)
    }

    /// Writes the ID generator's state (`persistIdGenerator`) and closes
    /// both journals.
    pub fn stop(&self) -> StoreResult<()> {
        let (journal_id, id) = {
            let mut g = self.ids();
            g.counter += 1;
            (g.counter, g.counter)
        };
        self.store_id(journal_id, id);
        self.messages.stop()?;
        self.bindings.stop()
    }

    fn completion(&self, sync: bool) -> (Option<Completion>, Option<Pending>) {
        let (c, p) = Pending::new();
        let _ = sync;
        (Some(c), Some(p))
    }

    // Message journal ------------------------------------------------------

    /// `storeMessage`: the message record, never synced (the reference that
    /// follows is).
    pub fn store_message(&self, message: &CoreMessage, encoded: &[u8]) -> StoreResult<()> {
        self.messages.append_add(
            message.message_id,
            ids::ADD_MESSAGE_PROTOCOL,
            encode_message(message, encoded),
            false,
            None,
        )
    }

    /// `storeReference`: synced when it is the last reference of a send.
    pub fn store_reference(&self, queue_id: i64, message_id: i64, last: bool) -> StoreResult<Pending> {
        let sync = last && self.cfg.sync_non_transactional;
        let (c, p) = self.completion(sync);
        self.messages
            .append_update(message_id, ids::ADD_REF, long(queue_id), sync, false, c)?;
        Ok(p.expect("pending"))
    }

    /// `storeAcknowledge`.
    pub fn store_acknowledge(&self, queue_id: i64, message_id: i64) -> StoreResult<Pending> {
        let sync = self.cfg.sync_non_transactional;
        let (c, p) = self.completion(sync);
        let found = self.messages.append_update(
            message_id,
            ids::ACKNOWLEDGE_REF,
            long(queue_id),
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
        self.messages.append_delete(message_id, false, None).map(|_| ())
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
        let mut body = long(queue_id);
        body.extend_from_slice(&count.to_be_bytes());
        self.messages.append_update(
            message_id,
            ids::UPDATE_DELIVERY_COUNT,
            body,
            self.cfg.sync_non_transactional,
            true,
            None,
        )?;
        Ok(true)
    }

    /// `storeMessageTransactional`.
    pub fn store_message_tx(&self, tx: i64, message: &CoreMessage, encoded: &[u8]) -> StoreResult<()> {
        self.messages.append_add_tx(
            tx,
            message.message_id,
            ids::ADD_MESSAGE_PROTOCOL,
            encode_message(message, encoded),
        )
    }

    /// `storeReferenceTransactional`.
    pub fn store_reference_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_update_tx(tx, message_id, ids::ADD_REF, long(queue_id))
    }

    /// `storeAcknowledgeTransactional`.
    pub fn store_acknowledge_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_update_tx(tx, message_id, ids::ACKNOWLEDGE_REF, long(queue_id))
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
    /// own committed transaction, synced (`commitBindings`).
    pub fn add_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<Pending> {
        let tx = self.generate_id();
        self.bindings.append_add_tx(tx, id, user_type, body)?;
        let (c, p) = Pending::new();
        self.bindings.append_commit(tx, true, Some(c))?;
        Ok(p)
    }

    /// `deleteQueueBinding` / `deleteAddressBinding` and `commitBindings`.
    pub fn delete_binding(&self, id: i64) -> StoreResult<Pending> {
        let tx = self.generate_id();
        self.bindings.append_delete_tx(tx, id, Vec::new())?;
        let (c, p) = Pending::new();
        self.bindings.append_commit(tx, true, Some(c))?;
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        }
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
            let m = msg(id);
            let mut w = Writer::new();
            m.encode(&mut w);
            s.store_message(&m, w.as_slice()).unwrap();
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
        assert_eq!(r.messages[&11].address, Some(SimpleString::from("q")));
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
        let mut w = Writer::new();
        m.encode(&mut w);
        let tx = s.generate_id();
        s.store_message_tx(tx, &m, w.as_slice()).unwrap();
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
