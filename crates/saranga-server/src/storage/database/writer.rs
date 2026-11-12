//! Writes the database store's operations. Artemis's `DataManager` hands one
//! batch to one worker per timer tick, so its writers in use are the batch's
//! time over the flush period, however many connections there are. Here a
//! batch starts when a connection is free and there is work, with no timer,
//! and the work is shared between the free connections.
//!
//! Work arrives as [`Unit`]s, in order; a unit is atomic. Units on different
//! connections commit in any order, so a unit waits while a key of its own
//! (a message, a record, a transaction) is in a batch on another connection,
//! and behind any earlier unit waiting on the same key: an acknowledgement
//! never overtakes the insert it deletes.
//!
//! A batch is one database transaction, its statements arrays and sent
//! together: one round trip, whatever its size. Inserts ignore rows that are
//! there, as a batch whose commit answer was lost is sent again.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use saranga_journal::{Completion, JournalError};
use tokio::sync::mpsc;
use tokio_postgres::{Client, Config, NoTls, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;

/// The most units in a batch.
const MAX_BATCH: usize = 2048;
/// How far past a waiting unit a batch looks for others.
const MAX_SCAN: usize = 8192;

/// The table of a generic record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Generic {
    /// `DB_BROKER_DATA`: what the message journal would hold.
    Broker,
    /// `DB_CONFIG_DATA`: what the bindings journal would hold.
    Config,
}

/// One row's change.
#[derive(Debug, Clone)]
pub enum Op {
    Message {
        id: i64,
        tx: Option<i64>,
        record: Vec<u8>,
        memory_estimate: i32,
        large: bool,
        core: bool,
        /// A large message's body (`LARGE_BODY`).
        body: Option<Vec<u8>>,
    },
    Reference {
        message: i64,
        queue: i64,
        tx: Option<i64>,
        /// `PAGED`: waiting in the table, not in the queue's memory.
        paged: bool,
    },
    /// Deletes a reference: an acknowledgement.
    Unreference {
        message: i64,
        queue: i64,
    },
    /// A prepared acknowledgement: the reference stays, marked.
    PreparedAck {
        message: i64,
        queue: i64,
        tx: i64,
    },
    /// Deletes a message no reference is left on.
    DeleteMessage {
        id: i64,
    },
    Record {
        table: Generic,
        id: i64,
        record_type: i16,
        tx: Option<i64>,
        data: Vec<u8>,
    },
    DeleteRecord {
        table: Generic,
        id: i64,
    },
    Address {
        id: i64,
        name: String,
        multicast: bool,
        anycast: bool,
    },
    Queue {
        id: i64,
        address: i64,
        name: String,
        multicast: bool,
        anycast: bool,
        filter: Option<Vec<u8>>,
        config: Vec<u8>,
    },
    /// Deletes whichever binding has this ID.
    DeleteBinding {
        id: i64,
    },
    Prepare {
        tx: i64,
        xid: Vec<u8>,
    },
    /// Ends a prepared transaction.
    Finish {
        tx: i64,
        commit: bool,
    },
}

impl Op {
    fn key(&self) -> i64 {
        match self {
            Op::Message { id, .. }
            | Op::DeleteMessage { id }
            | Op::Record { id, .. }
            | Op::DeleteRecord { id, .. }
            | Op::Address { id, .. }
            | Op::Queue { id, .. }
            | Op::DeleteBinding { id } => *id,
            Op::Reference { message, .. }
            | Op::Unreference { message, .. }
            | Op::PreparedAck { message, .. } => *message,
            Op::Prepare { tx, .. } | Op::Finish { tx, .. } => *tx,
        }
    }

    /// A paged reference also holds its queue's place: the queue reads them
    /// in message order, so they cannot commit out of it.
    fn queue_key(&self) -> Option<i64> {
        match self {
            Op::Reference {
                queue, paged: true, ..
            } => Some(!*queue),
            _ => None,
        }
    }

    /// Whether this deletes what a later insert of the key would bring
    /// back: a batch deletes after it inserts.
    fn deletes(&self) -> bool {
        matches!(
            self,
            Op::Unreference { .. }
                | Op::DeleteMessage { .. }
                | Op::DeleteRecord { .. }
                | Op::DeleteBinding { .. }
        )
    }
}

/// Changes that commit together, and who waits for them.
pub struct Unit {
    pub ops: Vec<Op>,
    pub done: Option<Completion>,
    /// Waits for everything before it, and holds back everything after.
    pub barrier: bool,
}

impl Unit {
    fn keys(&self) -> impl Iterator<Item = i64> + '_ {
        self.ops
            .iter()
            .flat_map(|o| std::iter::once(o.key()).chain(o.queue_key()))
    }
}

/// The writer as the store sees it.
pub struct Writes {
    sender: mpsc::UnboundedSender<Unit>,
    /// Operations sent and not yet committed (`max-pending-writes`).
    pending: Arc<Credits>,
    failed: Arc<AtomicBool>,
}

struct Credits {
    count: AtomicUsize,
    max: usize,
    lock: Mutex<()>,
    freed: Condvar,
}

impl Credits {
    fn release(&self, n: usize) {
        let before = self.count.fetch_sub(n, Ordering::AcqRel);
        if before >= self.max && before - n < self.max {
            let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
            self.freed.notify_all();
        }
    }
}

impl Writes {
    /// Queues a unit, waiting while too much is unwritten.
    pub fn send(&self, unit: Unit) -> Result<(), JournalError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(JournalError::IllegalState(
                "the database store has failed".into(),
            ));
        }
        let credits = &self.pending;
        if credits.count.load(Ordering::Acquire) >= credits.max {
            let mut guard = credits.lock.lock().unwrap_or_else(|e| e.into_inner());
            while credits.count.load(Ordering::Acquire) >= credits.max
                && !self.failed.load(Ordering::Acquire)
            {
                guard = credits
                    .freed
                    .wait_timeout(guard, Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        }
        credits
            .count
            .fetch_add(unit.ops.len().max(1), Ordering::AcqRel);
        self.sender
            .send(unit)
            .map_err(|_| JournalError::IllegalState("the database store is stopped".into()))
    }
}

/// Where the database is, and how to talk to it.
#[derive(Clone)]
pub struct Target {
    pub config: Config,
    /// `None` with `sslmode=disable`.
    pub tls: Option<MakeRustlsConnect>,
}

impl std::ops::Deref for Target {
    type Target = Config;

    fn deref(&self) -> &Config {
        &self.config
    }
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("config", &self.config)
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

/// How to reach the database, and what to do when it fails.
#[derive(Debug, Clone)]
pub struct Settings {
    pub config: Target,
    pub connections: usize,
    pub max_retries: u32,
    pub retry_interval: Duration,
    pub max_pending_writes: usize,
    /// Where large-message bodies are kept, so a deleted large message's file
    /// goes with its row.
    pub large_dir: std::path::PathBuf,
}

pub fn io_error(e: impl std::fmt::Display) -> JournalError {
    JournalError::Io(Arc::new(std::io::Error::other(e.to_string())))
}

/// Connects, the connection driven on the runtime it is called on.
pub async fn connect(target: &Target) -> Result<Client, tokio_postgres::Error> {
    async fn run<S, T>(connection: tokio_postgres::Connection<S, T>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        if let Err(e) = connection.await {
            tracing::debug!("database connection ended: {e}");
        }
    }
    Ok(match &target.tls {
        Some(tls) => {
            let (client, connection) = target.config.connect(tls.clone()).await?;
            tokio::spawn(run(connection));
            client
        }
        None => {
            let (client, connection) = target.config.connect(NoTls).await?;
            tokio::spawn(run(connection));
            client
        }
    })
}

/// Starts the writers on the current runtime.
pub async fn start(
    settings: Settings,
    critical: Arc<dyn Fn(&JournalError) + Send + Sync>,
) -> Result<Writes, JournalError> {
    let mut writers = Vec::with_capacity(settings.connections);
    for _ in 0..settings.connections.max(1) {
        writers.push(Writer::connect(settings.config.clone(), settings.large_dir.clone()).await?);
    }
    let (sender, receiver) = mpsc::unbounded_channel();
    let pending = Arc::new(Credits {
        count: AtomicUsize::new(0),
        max: settings.max_pending_writes.max(1),
        lock: Mutex::new(()),
        freed: Condvar::new(),
    });
    let failed = Arc::new(AtomicBool::new(false));
    tokio::spawn(dispatch(
        receiver,
        writers,
        Run {
            settings,
            pending: pending.clone(),
            failed: failed.clone(),
            critical,
        },
    ));
    Ok(Writes {
        sender,
        pending,
        failed,
    })
}

#[derive(Clone)]
struct Run {
    settings: Settings,
    pending: Arc<Credits>,
    failed: Arc<AtomicBool>,
    critical: Arc<dyn Fn(&JournalError) + Send + Sync>,
}

struct Finished {
    writer: Writer,
    keys: Vec<i64>,
}

/// Forms batches as connections come free.
async fn dispatch(mut receiver: mpsc::UnboundedReceiver<Unit>, writers: Vec<Writer>, run: Run) {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Finished>();
    let total = writers.len();
    let mut idle = writers;
    let mut queue: VecDeque<Unit> = VecDeque::new();
    let mut in_flight: HashSet<i64> = HashSet::new();
    let mut open = true;
    loop {
        while !idle.is_empty() {
            let queued: usize = queue.iter().map(|u| u.ops.len().max(1)).sum();
            let share = queued.div_ceil(idle.len()).max(1);
            let batch = form(&mut queue, &in_flight, idle.len() == total, share, &run);
            if batch.is_empty() {
                break;
            }
            let keys: Vec<i64> = batch.iter().flat_map(|u| u.keys()).collect();
            in_flight.extend(keys.iter().copied());
            let writer = idle.pop().expect("an idle writer");
            let (done, run) = (done_tx.clone(), run.clone());
            tokio::spawn(async move {
                let writer = execute(writer, batch, &run).await;
                let _ = done.send(Finished { writer, keys });
            });
        }
        if !open && queue.is_empty() && idle.len() == total {
            return;
        }
        tokio::select! {
            unit = receiver.recv(), if open => match unit {
                Some(unit) => {
                    queue.push_back(unit);
                    while let Ok(unit) = receiver.try_recv() {
                        queue.push_back(unit);
                    }
                }
                None => open = false,
            },
            finished = done_rx.recv() => {
                let finished = finished.expect("the dispatcher holds a sender");
                for key in &finished.keys {
                    in_flight.remove(key);
                }
                idle.push(finished.writer);
            }
        }
    }
}

/// Takes units that can run now until they hold `share` operations, leaving
/// the rest in order.
fn form(
    queue: &mut VecDeque<Unit>,
    in_flight: &HashSet<i64>,
    all_idle: bool,
    share: usize,
    run: &Run,
) -> Vec<Unit> {
    let mut batch = Vec::new();
    let mut skipped = Vec::new();
    let mut waiting: HashSet<i64> = HashSet::new();
    let mut deleted: HashSet<i64> = HashSet::new();
    let mut scanned = 0;
    let mut ops = 0;
    while ops < share && batch.len() < MAX_BATCH && scanned < MAX_SCAN {
        let Some(front) = queue.front() else { break };
        if front.barrier {
            if batch.is_empty() && skipped.is_empty() && all_idle {
                let unit = queue.pop_front().expect("a front");
                run.pending.release(1);
                if let Some(done) = unit.done {
                    done(Ok(()));
                }
                continue;
            }
            break;
        }
        scanned += 1;
        let blocked = front
            .keys()
            .any(|k| in_flight.contains(&k) || waiting.contains(&k) || deleted.contains(&k));
        let unit = queue.pop_front().expect("a front");
        if blocked {
            waiting.extend(unit.keys());
            skipped.push(unit);
        } else {
            deleted.extend(unit.ops.iter().filter(|o| o.deletes()).map(Op::key));
            ops += unit.ops.len().max(1);
            batch.push(unit);
        }
    }
    for unit in skipped.into_iter().rev() {
        queue.push_front(unit);
    }
    batch
}

/// Writes a batch, again while the database lets it, then answers its units.
async fn execute(mut writer: Writer, mut batch: Vec<Unit>, run: &Run) -> Writer {
    let ops: usize = batch.iter().map(|u| u.ops.len().max(1)).sum();
    let changes = Changes::of(&mut batch);
    let mut attempt = 0;
    let result = loop {
        match writer.write(&changes).await {
            Ok(()) => break Ok(()),
            Err(e) if attempt >= run.settings.max_retries => break Err(io_error(e)),
            Err(e) => {
                attempt += 1;
                tracing::warn!(
                    "database write failed, retry {attempt} of {}: {e}",
                    run.settings.max_retries
                );
                tokio::time::sleep(run.settings.retry_interval).await;
            }
        }
    };
    if let Err(e) = &result
        && !run.failed.swap(true, Ordering::AcqRel)
    {
        (run.critical)(e);
    }
    for unit in batch {
        if let Some(done) = unit.done {
            done(result.clone());
        }
    }
    run.pending.release(ops);
    writer
}

/// A batch as the columns of its statements.
#[derive(Default)]
struct Changes {
    message_ids: Vec<i64>,
    message_txs: Vec<Option<i64>>,
    message_records: Vec<Vec<u8>>,
    message_estimates: Vec<i32>,
    message_large: Vec<String>,
    message_core: Vec<String>,
    message_bodies: Vec<Option<Vec<u8>>>,
    ref_messages: Vec<i64>,
    ref_queues: Vec<i64>,
    ref_txs: Vec<Option<i64>>,
    ref_paged: Vec<String>,
    unref_messages: Vec<i64>,
    unref_queues: Vec<i64>,
    ack_messages: Vec<i64>,
    ack_queues: Vec<i64>,
    ack_txs: Vec<i64>,
    /// The lowest and highest ID of the unreferences, and of the acks.
    unref_range: (i64, i64),
    ack_range: (i64, i64),
    deleted_messages: Vec<i64>,
    broker: Records,
    config: Records,
    /// Where each address and queue ID is in its columns: the last write
    /// of one wins, as one statement cannot change a row twice.
    address_at: HashMap<i64, usize>,
    queue_at: HashMap<i64, usize>,
    address_ids: Vec<i64>,
    address_names: Vec<String>,
    address_multicast: Vec<String>,
    address_anycast: Vec<String>,
    queue_ids: Vec<i64>,
    queue_addresses: Vec<i64>,
    queue_names: Vec<String>,
    queue_multicast: Vec<String>,
    queue_anycast: Vec<String>,
    queue_filters: Vec<Option<Vec<u8>>>,
    queue_configs: Vec<Vec<u8>>,
    deleted_bindings: Vec<i64>,
    prepared_txs: Vec<i64>,
    prepared_xids: Vec<Vec<u8>>,
    committed: Vec<i64>,
    rolled_back: Vec<i64>,
}

/// A generic table's rows: the last write of an ID wins, as one statement
/// cannot change a row twice.
#[derive(Default)]
struct Records {
    latest: HashMap<i64, usize>,
    ids: Vec<i64>,
    types: Vec<i16>,
    txs: Vec<Option<i64>>,
    data: Vec<Vec<u8>>,
    deleted: Vec<i64>,
}

impl Records {
    fn put(&mut self, id: i64, record_type: i16, tx: Option<i64>, data: Vec<u8>) {
        match self.latest.get(&id) {
            Some(&at) => {
                self.types[at] = record_type;
                self.txs[at] = tx;
                self.data[at] = data;
            }
            None => {
                self.latest.insert(id, self.ids.len());
                self.ids.push(id);
                self.types.push(record_type);
                self.txs.push(tx);
                self.data.push(data);
            }
        }
    }
}

fn range(ids: &[i64]) -> (i64, i64) {
    let low = ids.iter().copied().min().unwrap_or(0);
    let high = ids.iter().copied().max().unwrap_or(0);
    (low, high)
}

fn flag(b: bool) -> String {
    if b { "Y" } else { "N" }.to_owned()
}

impl Changes {
    /// Takes the batch's operations: their records are not copied.
    fn of(batch: &mut [Unit]) -> Self {
        let mut c = Self::default();
        for op in batch.iter_mut().flat_map(|u| std::mem::take(&mut u.ops)) {
            match op {
                Op::Message {
                    id,
                    tx,
                    record,
                    memory_estimate,
                    large,
                    core,
                    body,
                } => {
                    c.message_ids.push(id);
                    c.message_txs.push(tx);
                    c.message_records.push(record);
                    c.message_estimates.push(memory_estimate);
                    c.message_large.push(flag(large));
                    c.message_core.push(flag(core));
                    c.message_bodies.push(body);
                }
                Op::Reference {
                    message,
                    queue,
                    tx,
                    paged,
                } => {
                    c.ref_messages.push(message);
                    c.ref_queues.push(queue);
                    c.ref_txs.push(tx);
                    c.ref_paged.push(flag(paged));
                }
                Op::Unreference { message, queue } => {
                    c.unref_messages.push(message);
                    c.unref_queues.push(queue);
                }
                Op::PreparedAck { message, queue, tx } => {
                    c.ack_messages.push(message);
                    c.ack_queues.push(queue);
                    c.ack_txs.push(tx);
                }
                Op::DeleteMessage { id } => c.deleted_messages.push(id),
                Op::Record {
                    table,
                    id,
                    record_type,
                    tx,
                    data,
                } => c.records(table).put(id, record_type, tx, data),
                Op::DeleteRecord { table, id } => c.records(table).deleted.push(id),
                Op::Address {
                    id,
                    name,
                    multicast,
                    anycast,
                } => match c.address_at.get(&id) {
                    Some(&at) => {
                        c.address_names[at] = name;
                        c.address_multicast[at] = flag(multicast);
                        c.address_anycast[at] = flag(anycast);
                    }
                    None => {
                        c.address_at.insert(id, c.address_ids.len());
                        c.address_ids.push(id);
                        c.address_names.push(name);
                        c.address_multicast.push(flag(multicast));
                        c.address_anycast.push(flag(anycast));
                    }
                },
                Op::Queue {
                    id,
                    address,
                    name,
                    multicast,
                    anycast,
                    filter,
                    config,
                } => match c.queue_at.get(&id) {
                    Some(&at) => {
                        c.queue_addresses[at] = address;
                        c.queue_names[at] = name;
                        c.queue_multicast[at] = flag(multicast);
                        c.queue_anycast[at] = flag(anycast);
                        c.queue_filters[at] = filter;
                        c.queue_configs[at] = config;
                    }
                    None => {
                        c.queue_at.insert(id, c.queue_ids.len());
                        c.queue_ids.push(id);
                        c.queue_addresses.push(address);
                        c.queue_names.push(name);
                        c.queue_multicast.push(flag(multicast));
                        c.queue_anycast.push(flag(anycast));
                        c.queue_filters.push(filter);
                        c.queue_configs.push(config);
                    }
                },
                Op::DeleteBinding { id } => c.deleted_bindings.push(id),
                Op::Prepare { tx, xid } => {
                    c.prepared_txs.push(tx);
                    c.prepared_xids.push(xid);
                }
                Op::Finish { tx, commit: true } => c.committed.push(tx),
                Op::Finish { tx, commit: false } => c.rolled_back.push(tx),
            }
        }
        c.unref_range = range(&c.unref_messages);
        c.ack_range = range(&c.ack_messages);
        c
    }

    fn records(&mut self, table: Generic) -> &mut Records {
        match table {
            Generic::Broker => &mut self.broker,
            Generic::Config => &mut self.config,
        }
    }
}

/// A connection and its prepared statements.
struct Writer {
    config: Target,
    connection: Option<Prepared>,
    /// Where large-message bodies live: a committed message delete takes the
    /// file with it.
    large_dir: std::path::PathBuf,
}

struct Prepared {
    client: Client,
    begin: Statement,
    commit: Statement,
    messages: Statement,
    references: Statement,
    unreferences: Statement,
    acks: Statement,
    delete_messages: Statement,
    broker: Statement,
    config: Statement,
    delete_broker: Statement,
    delete_config: Statement,
    addresses: Statement,
    queues: Statement,
    delete_addresses: Statement,
    delete_queues: Statement,
    delete_paged: Statement,
    prepare: Statement,
    commit_messages: Statement,
    commit_references: Statement,
    commit_acks: Statement,
    commit_deletes: Statement,
    commit_marks: Statement,
    commit_broker: Statement,
    rollback_references: Statement,
    rollback_acks: Statement,
    rollback_messages: Statement,
    rollback_broker: Statement,
    delete_prepared: Statement,
}

fn records_sql(table: &str) -> String {
    format!(
        "INSERT INTO {table} (ID, RECORD_TYPE, TX_ID, DATA_RECORD) \
         SELECT * FROM UNNEST($1::bigint[], $2::smallint[], $3::bigint[], $4::bytea[]) \
         ON CONFLICT (ID) DO UPDATE SET RECORD_TYPE = EXCLUDED.RECORD_TYPE, \
         TX_ID = EXCLUDED.TX_ID, DATA_RECORD = EXCLUDED.DATA_RECORD"
    )
}

/// Deletes references: acknowledgements.
pub(super) const UNREFERENCE: &str = "DELETE FROM DB_REFERENCES r USING UNNEST($1::bigint[], $2::bigint[]) AS d(m, q) \
                 WHERE r.MESSAGE_ID >= $3 AND r.MESSAGE_ID <= $4 AND r.MESSAGE_ID = d.m AND r.QUEUE_ID = d.q";
/// Deletes messages no reference is left on, returning which of them were
/// large so their body files can go with them.
pub(super) const DELETE_MESSAGES: &str = "DELETE FROM DB_MESSAGES m \
                 WHERE m.MESSAGE_ID = ANY($1) AND NOT EXISTS \
                 (SELECT 1 FROM DB_REFERENCES r WHERE r.MESSAGE_ID = m.MESSAGE_ID) \
                 RETURNING m.MESSAGE_ID, m.IS_LARGE::text";

/// No statement can reach past its batch, in whatever order the planner joins:
/// statistics taken while a table was empty make it walk the whole table
/// against a batch, whose cost then grows with the backlog, and make a
/// sequential scan look free. A statement on one key takes it as `= ANY`. One
/// on two keys joins the array, and takes the range of its IDs as well: an
/// `= ANY` there joins the index condition and is searched again for each
/// row of the batch.
impl Prepared {
    async fn new(client: Client) -> Result<Self, tokio_postgres::Error> {
        client
            .batch_execute("SET enable_seqscan = off; SET jit = off")
            .await?;
        let c = &client;
        let begin = c.prepare("BEGIN").await?;
        let commit = c.prepare("COMMIT").await?;
        let messages = c
            .prepare(
                "INSERT INTO DB_MESSAGES (MESSAGE_ID, TX_ID, MESSAGE_RECORD, MEMORY_ESTIMATE, IS_LARGE, IS_CORE, LARGE_BODY) \
                 SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::bytea[], $4::int[], $5::text[], $6::text[], $7::bytea[]) \
                 ON CONFLICT (MESSAGE_ID) DO NOTHING",
            )
            .await?;
        let references = c
            .prepare(
                "INSERT INTO DB_REFERENCES (MESSAGE_ID, QUEUE_ID, PAGED, TX_ID) \
                 SELECT m, q, p, t FROM UNNEST($1::bigint[], $2::bigint[], $3::text[], $4::bigint[]) AS d(m, q, p, t) \
                 ON CONFLICT (MESSAGE_ID, QUEUE_ID) DO NOTHING",
            )
            .await?;
        let unreferences = c.prepare(UNREFERENCE).await?;
        let acks = c
            .prepare(
                "UPDATE DB_REFERENCES r SET ACK_TX_ID = d.t \
                 FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[]) AS d(m, q, t) \
                 WHERE r.MESSAGE_ID >= $4 AND r.MESSAGE_ID <= $5 AND r.MESSAGE_ID = d.m AND r.QUEUE_ID = d.q",
            )
            .await?;
        let delete_messages = c.prepare(DELETE_MESSAGES).await?;
        let broker = c.prepare(&records_sql("DB_BROKER_DATA")).await?;
        let config = c.prepare(&records_sql("DB_CONFIG_DATA")).await?;
        let delete_broker = c
            .prepare("DELETE FROM DB_BROKER_DATA WHERE ID = ANY($1)")
            .await?;
        let delete_config = c
            .prepare("DELETE FROM DB_CONFIG_DATA WHERE ID = ANY($1)")
            .await?;
        let addresses = c
            .prepare(
                "INSERT INTO DB_ADDRESS (ADDRESS_ID, ADDRESS_NAME, IS_MULTICAST, IS_ANYCAST) \
                 SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::text[]) \
                 ON CONFLICT (ADDRESS_ID) DO UPDATE SET ADDRESS_NAME = EXCLUDED.ADDRESS_NAME, \
                 IS_MULTICAST = EXCLUDED.IS_MULTICAST, IS_ANYCAST = EXCLUDED.IS_ANYCAST",
            )
            .await?;
        let queues = c
            .prepare(
                "INSERT INTO DB_QUEUE (QUEUE_ID, ADDRESS_ID, QUEUE_NAME, IS_MULTICAST, IS_ANYCAST, FILTER_STRING, QUEUE_CONFIG) \
                 SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::bytea[], $7::bytea[]) \
                 ON CONFLICT (QUEUE_ID) DO UPDATE SET ADDRESS_ID = EXCLUDED.ADDRESS_ID, \
                 QUEUE_NAME = EXCLUDED.QUEUE_NAME, IS_MULTICAST = EXCLUDED.IS_MULTICAST, \
                 IS_ANYCAST = EXCLUDED.IS_ANYCAST, FILTER_STRING = EXCLUDED.FILTER_STRING, \
                 QUEUE_CONFIG = EXCLUDED.QUEUE_CONFIG",
            )
            .await?;
        let delete_addresses = c
            .prepare("DELETE FROM DB_ADDRESS WHERE ADDRESS_ID = ANY($1)")
            .await?;
        let delete_queues = c
            .prepare("DELETE FROM DB_QUEUE WHERE QUEUE_ID = ANY($1)")
            .await?;
        let delete_paged = c
            .prepare("DELETE FROM DB_REFERENCES WHERE QUEUE_ID = ANY($1) AND PAGED = 'Y'")
            .await?;
        let prepare = c
            .prepare(
                "INSERT INTO DB_PREP_TX (TX_ID, XID) SELECT * FROM UNNEST($1::bigint[], $2::bytea[]) \
                 ON CONFLICT (TX_ID) DO NOTHING",
            )
            .await?;
        let commit_messages = c
            .prepare("UPDATE DB_MESSAGES SET TX_ID = NULL WHERE TX_ID = ANY($1)")
            .await?;
        let commit_references = c
            .prepare("UPDATE DB_REFERENCES SET TX_ID = NULL WHERE TX_ID = ANY($1)")
            .await?;
        let commit_acks = c
            .prepare("DELETE FROM DB_REFERENCES WHERE ACK_TX_ID = ANY($1)")
            .await?;
        // What `PREPARED_DELETE` rows name, as their bytes are big-endian.
        let commit_deletes = c
            .prepare(
                "DELETE FROM DB_BROKER_DATA WHERE ID IN \
                 (SELECT ('x' || encode(DATA_RECORD, 'hex'))::bit(64)::bigint FROM DB_BROKER_DATA \
                 WHERE TX_ID = ANY($1) AND RECORD_TYPE = 201)",
            )
            .await?;
        let commit_marks = c
            .prepare("DELETE FROM DB_BROKER_DATA WHERE TX_ID = ANY($1) AND RECORD_TYPE = 201")
            .await?;
        let commit_broker = c
            .prepare("UPDATE DB_BROKER_DATA SET TX_ID = NULL WHERE TX_ID = ANY($1)")
            .await?;
        let rollback_references = c
            .prepare("DELETE FROM DB_REFERENCES WHERE TX_ID = ANY($1)")
            .await?;
        let rollback_acks = c
            .prepare("UPDATE DB_REFERENCES SET ACK_TX_ID = NULL WHERE ACK_TX_ID = ANY($1)")
            .await?;
        let rollback_messages = c
            .prepare("DELETE FROM DB_MESSAGES WHERE TX_ID = ANY($1)")
            .await?;
        let rollback_broker = c
            .prepare("DELETE FROM DB_BROKER_DATA WHERE TX_ID = ANY($1)")
            .await?;
        let delete_prepared = c
            .prepare("DELETE FROM DB_PREP_TX WHERE TX_ID = ANY($1)")
            .await?;
        Ok(Self {
            begin,
            commit,
            messages,
            references,
            unreferences,
            acks,
            delete_messages,
            broker,
            config,
            delete_broker,
            delete_config,
            addresses,
            queues,
            delete_addresses,
            delete_queues,
            delete_paged,
            prepare,
            commit_messages,
            commit_references,
            commit_acks,
            commit_deletes,
            commit_marks,
            commit_broker,
            rollback_references,
            rollback_acks,
            rollback_messages,
            rollback_broker,
            delete_prepared,
            client,
        })
    }
}

type Step<'a> = Pin<Box<dyn Future<Output = Result<u64, tokio_postgres::Error>> + Send + 'a>>;

impl Writer {
    async fn connect(config: Target, large_dir: std::path::PathBuf) -> Result<Self, JournalError> {
        let client = connect(&config).await.map_err(io_error)?;
        let connection = Some(Prepared::new(client).await.map_err(io_error)?);
        Ok(Self {
            config,
            connection,
            large_dir,
        })
    }

    /// One database transaction, sent at once. The order is fixed: inserts,
    /// then what changes them, then deletes. A failed statement leaves the
    /// `COMMIT` to roll back.
    async fn write(&mut self, c: &Changes) -> Result<(), tokio_postgres::Error> {
        if self
            .connection
            .as_ref()
            .is_none_or(|p| p.client.is_closed())
        {
            self.connection = None;
            let client = connect(&self.config).await?;
            self.connection = Some(Prepared::new(client).await?);
        }
        let p = self.connection.as_ref().expect("a connection");
        let db = &p.client;
        // The IDs of large messages whose rows this batch deletes: their body
        // files go once the batch commits.
        let deleted_large: Arc<Mutex<Vec<i64>>> = Arc::default();
        let mut steps: Vec<Step<'_>> =
            vec![Box::pin(async move { db.execute(&p.begin, &[]).await })];
        if !c.message_ids.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(
                    &p.messages,
                    &[
                        &c.message_ids,
                        &c.message_txs,
                        &c.message_records,
                        &c.message_estimates,
                        &c.message_large,
                        &c.message_core,
                        &c.message_bodies,
                    ],
                )
                .await
            }));
        }
        if !c.ref_messages.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(
                    &p.references,
                    &[&c.ref_messages, &c.ref_queues, &c.ref_paged, &c.ref_txs],
                )
                .await
            }));
        }
        for (records, statement) in [(&c.broker, &p.broker), (&c.config, &p.config)] {
            if !records.ids.is_empty() {
                steps.push(Box::pin(async move {
                    db.execute(
                        statement,
                        &[&records.ids, &records.types, &records.txs, &records.data],
                    )
                    .await
                }));
            }
        }
        if !c.address_ids.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(
                    &p.addresses,
                    &[
                        &c.address_ids,
                        &c.address_names,
                        &c.address_multicast,
                        &c.address_anycast,
                    ],
                )
                .await
            }));
        }
        if !c.queue_ids.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(
                    &p.queues,
                    &[
                        &c.queue_ids,
                        &c.queue_addresses,
                        &c.queue_names,
                        &c.queue_multicast,
                        &c.queue_anycast,
                        &c.queue_filters,
                        &c.queue_configs,
                    ],
                )
                .await
            }));
        }
        if !c.prepared_txs.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(&p.prepare, &[&c.prepared_txs, &c.prepared_xids])
                    .await
            }));
        }
        if !c.ack_messages.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(
                    &p.acks,
                    &[
                        &c.ack_messages,
                        &c.ack_queues,
                        &c.ack_txs,
                        &c.ack_range.0,
                        &c.ack_range.1,
                    ],
                )
                .await
            }));
        }
        if !c.committed.is_empty() {
            for statement in [
                &p.commit_messages,
                &p.commit_references,
                &p.commit_acks,
                &p.commit_deletes,
                &p.commit_marks,
                &p.commit_broker,
                &p.delete_prepared,
            ] {
                steps.push(Box::pin(async move {
                    db.execute(statement, &[&c.committed]).await
                }));
            }
        }
        if !c.rolled_back.is_empty() {
            for statement in [
                &p.rollback_references,
                &p.rollback_acks,
                &p.rollback_messages,
                &p.rollback_broker,
                &p.delete_prepared,
            ] {
                steps.push(Box::pin(async move {
                    db.execute(statement, &[&c.rolled_back]).await
                }));
            }
        }
        if !c.unref_messages.is_empty() {
            steps.push(Box::pin(async move {
                db.execute(
                    &p.unreferences,
                    &[
                        &c.unref_messages,
                        &c.unref_queues,
                        &c.unref_range.0,
                        &c.unref_range.1,
                    ],
                )
                .await
            }));
        }
        if !c.deleted_messages.is_empty() {
            let large = deleted_large.clone();
            steps.push(Box::pin(async move {
                let rows = db.query(&p.delete_messages, &[&c.deleted_messages]).await?;
                let mut large = large.lock().unwrap_or_else(|e| e.into_inner());
                for row in &rows {
                    let id: i64 = row.get(0);
                    let is_large: String = row.get(1);
                    if is_large == "Y" {
                        large.push(id);
                    }
                }
                Ok(rows.len() as u64)
            }));
        }
        for (records, statement) in [(&c.broker, &p.delete_broker), (&c.config, &p.delete_config)] {
            if !records.deleted.is_empty() {
                steps.push(Box::pin(async move {
                    db.execute(statement, &[&records.deleted]).await
                }));
            }
        }
        if !c.deleted_bindings.is_empty() {
            for statement in [
                &p.delete_addresses,
                &p.delete_queues,
                &p.delete_paged,
                &p.delete_config,
            ] {
                steps.push(Box::pin(async move {
                    db.execute(statement, &[&c.deleted_bindings]).await
                }));
            }
        }
        steps.push(Box::pin(async move { db.execute(&p.commit, &[]).await }));
        let result = futures_util::future::try_join_all(steps).await.map(|_| ());
        match &result {
            // The large bodies of the messages the commit deleted are files of
            // their own; they go now the rows are gone. A missing file (the
            // body was kept in the row, or already deleted) is no error.
            Ok(()) => {
                for id in deleted_large
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .iter()
                {
                    let path = self.large_dir.join(format!("{id}.msg"));
                    if let Err(e) = std::fs::remove_file(&path)
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        tracing::warn!(
                            "AMQ222010: Critical IO Error, failed to delete large message file {}: {e}",
                            path.display()
                        );
                    }
                }
            }
            Err(_) => {
                // The statements after the failed one may still be in flight;
                // a new connection starts clean.
                self.connection = None;
            }
        }
        result
    }
}
