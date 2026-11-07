//! The file journals as the broker's store: `AbstractJournalStorageManager`
//! and `JournalStorageManager`.

use std::sync::OnceLock;

use saranga_journal::PreparedTransactionInfo;
use saranga_storage::codec::{
    DeliveryCountUpdateEncoding, IdCounterEncoding, PageTransactionEncoding,
    ScheduledDeliveryEncoding,
};
use saranga_storage::{BatchingIdGenerator, CHECKPOINT_BATCH_SIZE, IdCounterStore};

use super::*;

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

/// The broker's journals.
pub struct JournalStorage {
    cfg: StorageConfig,
    pub(crate) messages: Journal,
    pub(crate) bindings: Arc<Journal>,
    ids: BatchingIdGenerator<BindingsIdStore>,
    critical: OnceLock<CriticalListener>,
}

impl JournalStorage {
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
            critical: OnceLock::new(),
        })
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

    fn completion(&self, sync: bool) -> (Option<Completion>, Option<Pending>) {
        let (c, p) = Pending::new();
        let _ = sync;
        (Some(c), Some(p))
    }
}

impl Store for JournalStorage {
    fn writing(&self) -> Option<(std::time::Duration, u64)> {
        match (self.messages.writing(), self.bindings.writing()) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    fn journal_directory(&self) -> &std::path::Path {
        &self.cfg.messages.directory
    }

    fn set_critical_listener(&self, listener: CriticalListener) {
        self.messages.set_critical_listener(Some(listener.clone()));
        self.bindings.set_critical_listener(Some(listener.clone()));
        let _ = self.critical.set(listener);
    }

    fn critical_io(&self, e: &std::io::Error, file: Option<&Path>) {
        match file {
            Some(f) => tracing::warn!(
                "AMQ144011: Critical IO Exception happened: {e} on {}",
                f.display()
            ),
            None => tracing::warn!("AMQ144010: Critical IO Exception happened: {e}"),
        }
        if let Some(l) = self.critical.get() {
            let e = JournalError::Io(Arc::new(std::io::Error::new(e.kind(), e.to_string())));
            l(&e, file);
        }
    }

    fn generate_id(&self) -> i64 {
        // The generator only stops with the storage.
        self.ids
            .generate_id()
            .unwrap_or_else(|_| self.ids.current_id())
    }

    fn load_bindings(&self) -> StoreResult<Vec<saranga_journal::RecordInfo>> {
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

    fn load_messages(&self) -> StoreResult<RecoveredMessages> {
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
                ids::HEURISTIC_COMPLETION => {
                    // HeuristicCompletionEncoding: the Xid, then isCommit.
                    use saranga_buffer::Codec;
                    let mut reader = Reader::new(&r.data);
                    let xid = Xid::read(&mut reader)
                        .map_err(|e| JournalError::Corrupt(format!("xid: {e}")))?;
                    let commit = reader
                        .bool()
                        .map_err(|e| JournalError::Corrupt(format!("heuristic completion: {e}")))?;
                    out.heuristics.push((r.id, xid, commit));
                }
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

    fn stop(&self) -> StoreResult<()> {
        self.ids.stop();
        // Both are closed, the second even when the first failed (as after
        // a critical I/O error).
        let messages = self.messages.stop();
        let bindings = self.bindings.stop();
        messages.and(bindings)
    }

    fn max_record_size(&self) -> usize {
        self.cfg.messages.max_record_size()
    }

    fn retention(&self) -> Option<&saranga_journal::Retention> {
        self.cfg.messages.retention.as_ref()
    }

    fn force_backup(&self) -> StoreResult<()> {
        self.messages.force_backup()
    }

    fn is_history(&self) -> bool {
        self.cfg.messages.retention.is_some()
    }

    fn page_write_history(&self, message: &ServerMessage, queue_ids: &[i64]) {
        if !self.is_history() {
            return;
        }
        let id = message.message_id();
        let (record_type, body) = message_record(message);
        let _ = self
            .messages
            .append_add_event(id, record_type, body, false, None);
        for q in queue_ids {
            let _ = self
                .messages
                .append_add_event(id, ids::ADD_REF, ref_body(*q), false, None);
        }
    }

    fn large_body_history(&self, message_id: i64, bytes: &[u8]) {
        for chunk in bytes.chunks(10 * 1024) {
            let _ = self.messages.append_add_event(
                message_id,
                ids::ADD_MESSAGE_BODY,
                chunk.to_vec(),
                false,
                None,
            );
        }
    }

    fn store_message(&self, message: &ServerMessage) -> StoreResult<()> {
        let (record_type, body) = message_record(message);
        self.messages
            .append_add(message.message_id(), record_type, body, false, None)
    }

    fn store_reference(&self, queue_id: i64, message_id: i64, last: bool) -> StoreResult<Pending> {
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

    fn store_acknowledge(&self, queue_id: i64, message_id: i64) -> StoreResult<Pending> {
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

    fn delete_message(&self, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_delete(message_id, false, None)
            .map(|_| ())
    }

    fn update_delivery_count(
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

    fn update_scheduled_delivery_time(
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

    fn update_scheduled_delivery_time_tx(
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

    fn store_message_tx(&self, tx: i64, message: &ServerMessage) -> StoreResult<()> {
        let (record_type, body) = message_record(message);
        self.messages
            .append_add_tx(tx, message.message_id(), record_type, body)
    }

    fn store_cursor_ack_tx(
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

    fn store_page_complete_tx(
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

    fn store_page_transaction_tx(&self, tx: i64, page_tx: i64, messages: i32) -> StoreResult<i64> {
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

    fn update_page_transaction_tx(
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

    fn delete_record_tx(&self, tx: i64, id: i64) -> StoreResult<()> {
        self.messages.append_delete_tx(tx, id, Vec::new())
    }

    fn add_record(
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

    fn add_record_tx(&self, tx: i64, id: i64, record_type: u8, body: Vec<u8>) -> StoreResult<()> {
        self.messages.append_add_tx(tx, id, record_type, body)
    }

    fn store_heuristic_completion(&self, id: i64, xid: &Xid, commit: bool) -> StoreResult<Pending> {
        let mut body = encode_xid(xid);
        body.push(u8::from(commit));
        self.add_record(id, ids::HEURISTIC_COMPLETION, body, true)
    }

    fn delete_heuristic_completion(&self, id: i64) -> StoreResult<Pending> {
        let (c, p) = Pending::new();
        self.messages.append_delete(id, true, Some(c))?;
        Ok(p)
    }

    fn delete_record(&self, id: i64) -> StoreResult<()> {
        self.messages.append_delete(id, false, None).map(|_| ())
    }

    fn sync_transactional(&self) -> bool {
        self.cfg.sync_transactional
    }

    fn sync_non_transactional(&self) -> bool {
        self.cfg.sync_non_transactional
    }

    fn store_reference_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_update_tx(tx, message_id, ids::ADD_REF, ref_body(queue_id))
    }

    fn store_acknowledge_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.messages
            .append_update_tx(tx, message_id, ids::ACKNOWLEDGE_REF, ref_body(queue_id))
    }

    fn prepare(&self, tx: i64, xid: &Xid) -> StoreResult<Pending> {
        let sync = self.cfg.sync_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_prepare(tx, encode_xid(xid), sync, c)?;
        Ok(p.expect("pending"))
    }

    fn commit(&self, tx: i64) -> StoreResult<Pending> {
        let sync = self.cfg.sync_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_commit(tx, sync, c)?;
        Ok(p.expect("pending"))
    }

    fn rollback(&self, tx: i64) -> StoreResult<Pending> {
        let sync = self.cfg.sync_transactional;
        let (c, p) = self.completion(sync);
        self.messages.append_rollback(tx, sync, c)?;
        Ok(p.expect("pending"))
    }

    fn written(&self) -> StoreResult<Pending> {
        let (c, p) = Pending::new();
        self.messages.on_written(c)?;
        Ok(p)
    }

    fn add_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_add_tx(tx, id, user_type, body)?;
        self.bindings.append_commit(tx, true, None)
    }

    fn replace_binding(
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

    fn update_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_update_tx(tx, id, user_type, body)?;
        self.bindings.append_commit(tx, true, None)
    }

    fn delete_binding(&self, id: i64) -> StoreResult<()> {
        let tx = self.generate_id();
        self.bindings.append_delete_tx(tx, id, Vec::new())?;
        self.bindings.append_commit(tx, true, None)
    }

    fn add_binding_record(&self, user_type: u8, body: Vec<u8>) -> StoreResult<i64> {
        let id = self.generate_id();
        self.bindings.append_add(id, user_type, body, true, None)?;
        Ok(id)
    }

    fn add_binding_record_with_id(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        self.bindings.append_add(id, user_type, body, true, None)
    }

    fn delete_bindings(&self, ids: &[i64]) -> StoreResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let tx = self.generate_id();
        for &id in ids {
            self.bindings.append_delete_tx(tx, id, Vec::new())?;
        }
        self.bindings.append_commit(tx, true, None)
    }

    fn delete_binding_record(&self, id: i64) -> StoreResult<()> {
        self.bindings.append_delete(id, true, None).map(|_| ())
    }

    fn store_configuration(
        &self,
        user_type: u8,
        body: Vec<u8>,
        replaces: Option<i64>,
    ) -> StoreResult<i64> {
        let id = self.generate_id();
        match replaces {
            Some(old) => {
                let tx = self.generate_id();
                self.bindings.append_delete_tx(tx, old, Vec::new())?;
                self.bindings.append_add_tx(tx, id, user_type, body)?;
                self.bindings.append_commit(tx, true, None)?;
            }
            None => self.bindings.append_add(id, user_type, body, true, None)?,
        }
        Ok(id)
    }

    fn store_queue_status(&self, queue_id: i64) -> StoreResult<i64> {
        let body = saranga_storage::codec::QueueStatusEncoding {
            queue_id,
            status: 0,
        }
        .encode();
        self.add_binding_record(crate::storage::ids::QUEUE_STATUS_RECORD, body)
    }

    fn store_address_status(&self, address_id: i64) -> StoreResult<i64> {
        let body = saranga_storage::codec::AddressStatusEncoding {
            address_id,
            status: 0,
        }
        .encode();
        self.add_binding_record(crate::storage::ids::ADDRESS_STATUS_RECORD, body)
    }

    fn commit_unsynced(&self, tx: i64, wait: bool) -> StoreResult<()> {
        let completion: Option<Completion> = if wait { None } else { Some(Box::new(|_| {})) };
        self.messages.append_commit(tx, false, completion)
    }

    fn journals(&self) -> Option<(&Journal, &Journal)> {
        Some((&self.messages, &self.bindings))
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
            database: None,
        }
    }

    #[test]
    fn message_encoding_matches_the_core_persister() {
        use saranga_storage::message::CoreMessageRecord;
        let m = CoreMessage {
            message_id: 99,
            address: Some(SimpleString::from("addr")),
            durable: true,
            body: vec![1, 2, 3].into(),
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
        let s = JournalStorage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        s.load_messages().unwrap();
        let first: Vec<i64> = (0..5).map(|_| s.generate_id()).collect();
        assert_eq!(first, vec![0, 2, 3, 4, 5]);
        s.stop().unwrap();
        drop(s);

        let s = JournalStorage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        let next = s.generate_id();
        assert!(next > 5, "{next}");
        drop(s); // a crash: no stop record

        let s = JournalStorage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        assert!(s.generate_id() > i32::MAX as i64);
    }

    #[test]
    fn messages_refs_acks_and_counts_recover() {
        let dir = tempfile::tempdir().unwrap();
        let s = JournalStorage::open(config(dir.path())).unwrap();
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

        let s = JournalStorage::open(config(dir.path())).unwrap();
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
        let s = JournalStorage::open(config(dir.path())).unwrap();
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

        let s = JournalStorage::open(config(dir.path())).unwrap();
        s.load_bindings().unwrap();
        let r = s.load_messages().unwrap();
        assert_eq!(r.prepared.len(), 1);
        assert_eq!(r.prepared[0].xid, xid);
        assert_eq!(r.prepared[0].sends, vec![(3, 7)]);
        assert!(r.messages.contains_key(&7));
        assert!(r.queues.is_empty());
    }
}
