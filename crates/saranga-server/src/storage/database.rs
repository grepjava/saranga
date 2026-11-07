//! The relational store: `<database-store>` with `database-storage`, on
//! PostgreSQL, in the tables of Artemis's `DatabaseStorageManager`, so that
//! either broker opens what the other wrote.
//!
//! What those tables have no column for goes in `DB_BROKER_DATA` and
//! `DB_CONFIG_DATA` under record types Artemis passes over: a reference's
//! delivery count and scheduled time, which Artemis does not keep yet, and
//! an address's full binding, whose `internal` and `auto-created` its row
//! does not hold.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use saranga_buffer::json::Json;
use saranga_journal::{CriticalListener, Journal, RecordInfo, Retention};
use saranga_storage::codec::{
    DeliveryCountUpdateEncoding, IdCounterEncoding, PageTransactionEncoding,
    PersistentAddressBindingEncoding, PersistentQueueBindingEncoding, ScheduledDeliveryEncoding,
};
use saranga_storage::{BatchingIdGenerator, CHECKPOINT_BATCH_SIZE, IdCounterStore};
use saranga_transport::connector::ServerCheck;
use std::path::PathBuf;
use tokio_postgres::Config;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

use super::*;

mod writer;

use writer::{Generic, Op, Settings, Unit, Writes, io_error};

/// A page transaction's consumed messages. The journal updates the record
/// in place; here it is a record of its own, Saranga's.
const PAGE_TRANSACTION_UPDATE: u8 = 200;
/// A record a prepared transaction deletes when it commits: the ID, as eight
/// bytes. A delete has no row of its own to wait in, so this is one.
pub(super) const PREPARED_DELETE: u8 = 201;

/// The session lock that keeps a database to one Saranga broker: the bytes of "SARANGA".
const LOCK_KEY: i64 = 0x5341_5241_4e47_4100;

/// How long a starting broker waits for the lock, for the session of a
/// broker that just died to end.
const LOCK_WAIT: Duration = Duration::from_secs(5);

/// `RoutingType.MULTICAST`'s and `ANYCAST`'s bytes.
const MULTICAST: u8 = 0;
const ANYCAST: u8 = 1;

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS DB_MESSAGES(MESSAGE_ID BIGINT NOT NULL, TX_ID BIGINT, MESSAGE_RECORD BYTEA, MEMORY_ESTIMATE INT, IS_LARGE CHAR(1) NOT NULL, LARGE_BODY BYTEA, IS_CORE CHAR(1) NOT NULL, PRIMARY KEY (MESSAGE_ID))",
    "CREATE TABLE IF NOT EXISTS DB_REFERENCES(MESSAGE_ID BIGINT NOT NULL, QUEUE_ID BIGINT NOT NULL, PAGED CHAR NOT NULL, TX_ID BIGINT, ACK_TX_ID BIGINT, PRIMARY KEY (MESSAGE_ID, QUEUE_ID))",
    "CREATE TABLE IF NOT EXISTS DB_ADDRESS(ADDRESS_ID BIGINT NOT NULL, ADDRESS_NAME VARCHAR(255) NOT NULL, IS_MULTICAST CHAR, IS_ANYCAST CHAR, PRIMARY KEY (ADDRESS_ID))",
    "CREATE TABLE IF NOT EXISTS DB_QUEUE(QUEUE_ID BIGINT NOT NULL, ADDRESS_ID BIGINT NOT NULL, QUEUE_NAME VARCHAR(255), IS_MULTICAST CHAR, IS_ANYCAST CHAR, FILTER_STRING BYTEA, QUEUE_CONFIG BYTEA, PRIMARY KEY (QUEUE_ID))",
    "CREATE TABLE IF NOT EXISTS DB_BROKER_DATA(ID BIGINT NOT NULL, RECORD_TYPE SMALLINT NOT NULL, TX_ID BIGINT, DATA_RECORD BYTEA, PRIMARY KEY (ID))",
    "CREATE TABLE IF NOT EXISTS DB_CONFIG_DATA(ID BIGINT NOT NULL, RECORD_TYPE SMALLINT NOT NULL, TX_ID BIGINT, DATA_RECORD BYTEA, PRIMARY KEY (ID))",
    "CREATE TABLE IF NOT EXISTS DB_PREP_TX(TX_ID BIGINT NOT NULL, XID BYTEA, PRIMARY KEY (TX_ID))",
    // Ending a prepared transaction finds its rows by its ID. Few rows have
    // one, so these stay small; Artemis works without them.
    "CREATE INDEX IF NOT EXISTS DB_MESSAGES_TX ON DB_MESSAGES (TX_ID) WHERE TX_ID IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS DB_REFERENCES_TX ON DB_REFERENCES (TX_ID) WHERE TX_ID IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS DB_REFERENCES_ACK_TX ON DB_REFERENCES (ACK_TX_ID) WHERE ACK_TX_ID IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS DB_BROKER_DATA_TX ON DB_BROKER_DATA (TX_ID) WHERE TX_ID IS NOT NULL",
    // A queue reads its paged references in order; only those are indexed.
    "CREATE INDEX IF NOT EXISTS DB_REFERENCES_PAGED ON DB_REFERENCES (QUEUE_ID, MESSAGE_ID) WHERE PAGED = 'Y'",
];

/// Reaching the database, from `<database-store>`.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// `jdbc-connection-url`.
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub connections: i32,
    pub network_timeout_millis: i32,
    pub max_retries: i32,
    pub retry_interval_millis: i64,
    pub max_pending_writes: i32,
}

/// A `jdbc:postgresql:` URL as a client configuration: `//host:port/database`
/// or `database`, with `user` and `password` parameters and those of TLS, as
/// the JDBC driver takes them: `ssl`, `sslmode`, `sslrootcert`, `sslcert` and
/// `sslkey`. Its other parameters are its own.
pub fn client_config(d: &DatabaseConfig) -> Result<writer::Target, String> {
    let rest = d
        .url
        .strip_prefix("jdbc:postgresql:")
        .ok_or_else(|| format!("not a PostgreSQL URL: {}", mask_url(&d.url)))?;
    let (location, query) = rest.split_once('?').unwrap_or((rest, ""));
    let mut config = Config::new();
    let database = match location.strip_prefix("//") {
        Some(authority) => {
            let (server, database) = authority.split_once('/').unwrap_or((authority, ""));
            if server.contains(',') {
                return Err("a URL with several hosts is not supported".to_owned());
            }
            let (host, port) = match server.rsplit_once(':') {
                Some((host, port)) if !host.ends_with(']') || server.starts_with('[') => (
                    host,
                    Some(
                        port.parse::<u16>()
                            .map_err(|_| format!("not a port: {port}"))?,
                    ),
                ),
                _ => (server, None),
            };
            config.host(if host.is_empty() { "localhost" } else { host });
            if let Some(port) = port {
                config.port(port);
            }
            database
        }
        None => {
            config.host("localhost");
            location
        }
    };
    if !database.is_empty() {
        config.dbname(database);
    }
    let (mut ssl, mut root, mut cert, mut key) = (None, None, None, None);
    for parameter in query.split('&').filter(|p| !p.is_empty()) {
        let (name, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        match name {
            "user" => {
                config.user(value);
            }
            "password" => {
                config.password(value);
            }
            "ssl" => {
                ssl = Some(
                    if value == "false" {
                        "disable"
                    } else {
                        "verify-full"
                    }
                    .to_owned(),
                )
            }
            "sslmode" => ssl = Some(value.to_owned()),
            "sslrootcert" => root = Some(PathBuf::from(decoded(value))),
            "sslcert" => cert = Some(PathBuf::from(decoded(value))),
            "sslkey" => key = Some(PathBuf::from(decoded(value))),
            other => tracing::debug!("JDBC URL parameter {other} ignored"),
        }
    }
    if let Some(user) = &d.user {
        config.user(user);
    }
    if let Some(password) = &d.password {
        config.password(password);
    }
    if d.network_timeout_millis > 0 {
        config.connect_timeout(Duration::from_millis(d.network_timeout_millis as u64));
    }
    // As the JDBC driver: encrypted if the server can, unless told otherwise.
    let (mode, check) = match ssl.as_deref().unwrap_or("prefer") {
        "disable" => (SslMode::Disable, None),
        "allow" | "prefer" => (SslMode::Prefer, Some(ServerCheck::Nothing)),
        "require" => (SslMode::Require, Some(ServerCheck::Nothing)),
        "verify-ca" => (SslMode::Require, Some(ServerCheck::Chain)),
        "verify-full" => (SslMode::Require, Some(ServerCheck::Host)),
        other => return Err(format!("sslmode={other}: not an sslmode")),
    };
    config.ssl_mode(mode);
    let client = match (&cert, &key) {
        (Some(c), Some(k)) => Some((c.as_path(), k.as_path())),
        (None, None) => None,
        _ => return Err("sslcert and sslkey go together".to_owned()),
    };
    // The driver's default for the roots, where that file is.
    let root = root.or_else(|| {
        let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
        let file = Path::new(&home).join(".postgresql/root.crt");
        file.is_file().then_some(file)
    });
    let tls = check
        .map(|check| {
            saranga_transport::connector::client_tls(check, root.as_deref(), client)
                .map(MakeRustlsConnect::new)
                .map_err(|e| format!("TLS to the database: {e}"))
        })
        .transpose()?;
    Ok(writer::Target { config, tls })
}

/// A URL parameter's `%xx`, as a path has them.
fn decoded(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = (bytes[i] == b'%')
            .then(|| value.get(i + 1..i + 3))
            .flatten()
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match hex {
            Some(b) => {
                out.push(b);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The URL for a message, without what follows the `?`: it may hold a
/// password.
fn mask_url(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

/// Where `BatchingIDGenerator` keeps its counter.
struct DatabaseIdStore(Arc<Writes>);

impl IdCounterStore for DatabaseIdStore {
    fn store_id(&self, journal_id: i64, id: i64) {
        let op = Op::Record {
            table: Generic::Config,
            id: journal_id,
            record_type: i16::from(ids::ID_COUNTER_RECORD),
            tx: None,
            data: IdCounterEncoding { id }.encode(),
        };
        if let Err(e) = write(&self.0, vec![op]) {
            tracing::warn!("AMQ222217: Exception during batching ID: {e}");
        }
    }

    fn delete_id(&self, journal_id: i64) {
        let _ = self.0.send(Unit {
            ops: vec![Op::DeleteRecord {
                table: Generic::Config,
                id: journal_id,
            }],
            done: None,
            barrier: false,
        });
    }
}

/// Sends changes and waits for their commit, as a synced journal append.
fn write(writes: &Writes, ops: Vec<Op>) -> StoreResult<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    writes.send(Unit {
        ops,
        done: Some(Box::new(move |r| {
            let _ = tx.send(r);
        })),
        barrier: false,
    })?;
    rx.recv()
        .unwrap_or_else(|_| Err(JournalError::IllegalState("the store stopped".into())))
}

/// A transaction's changes before its commit.
#[derive(Default)]
struct OpenTx {
    ops: Vec<Op>,
    /// Sent with the commit even of a prepared transaction: a delete has no
    /// row to wait in.
    deletes: Vec<Op>,
}

/// The store's runtime. The store may be dropped on another runtime's
/// thread, where a runtime cannot wait for its tasks.
struct Background(Option<tokio::runtime::Runtime>);

impl Background {
    fn spawn(&self, work: impl Future<Output = ()> + Send + 'static) {
        if let Some(runtime) = &self.0 {
            runtime.spawn(work);
        }
    }

    /// Runs borrowing work to its end, from any thread, a broker runtime's
    /// included.
    fn block_in(&self, work: impl Future<Output = ()> + Send) {
        if let Some(runtime) = &self.0 {
            std::thread::scope(|scope| {
                scope.spawn(|| runtime.block_on(work));
            });
        }
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// The side records of a reference: their record IDs.
#[derive(Default, Clone, Copy)]
struct Side {
    delivery: Option<i64>,
    scheduled: Option<i64>,
}

/// The broker's tables.
pub struct DatabaseStorage {
    cfg: StorageConfig,
    runtime: Background,
    connection: writer::Target,
    writes: Arc<Writes>,
    ids: BatchingIdGenerator<DatabaseIdStore>,
    critical: Arc<OnceLock<CriticalListener>>,
    transactions: Mutex<HashMap<i64, OpenTx>>,
    prepared: Mutex<HashSet<i64>>,
    addresses: Mutex<HashMap<String, i64>>,
    sides: Mutex<HashMap<(i64, i64), Side>>,
    /// Per queue, the last paged message read. An index scan from the start
    /// would step over every row already read and not yet vacuumed.
    read_from: Mutex<HashMap<i64, i64>>,
    /// The connection queues read paged messages on.
    reader: tokio::sync::Mutex<Option<PagedReader>>,
}

/// Takes a queue's next paged references out of paging, and returns their
/// messages: one round trip, and atomic.
const READ_PAGED: &str = "WITH picked AS (\
      SELECT MESSAGE_ID FROM DB_REFERENCES \
      WHERE QUEUE_ID = $1 AND PAGED = 'Y' AND TX_ID IS NULL AND MESSAGE_ID > $3       ORDER BY MESSAGE_ID LIMIT $2), \
    flipped AS (\
      UPDATE DB_REFERENCES r SET PAGED = 'N' FROM picked p \
      WHERE r.QUEUE_ID = $1 AND r.MESSAGE_ID = p.MESSAGE_ID RETURNING r.MESSAGE_ID) \
    SELECT m.MESSAGE_ID, m.MESSAGE_RECORD, m.IS_LARGE::text, m.IS_CORE::text, m.LARGE_BODY \
    FROM flipped f JOIN DB_MESSAGES m ON m.MESSAGE_ID = f.MESSAGE_ID";

/// A queue's paged messages, left paged. Sorted as `READ_PAGED`'s are.
const SCAN_PAGED: &str = "SELECT m.MESSAGE_ID, m.MESSAGE_RECORD, m.IS_LARGE::text, m.IS_CORE::text, m.LARGE_BODY \
    FROM (SELECT MESSAGE_ID FROM DB_REFERENCES \
      WHERE QUEUE_ID = $1 AND PAGED = 'Y' AND TX_ID IS NULL AND MESSAGE_ID > $3 \
      ORDER BY MESSAGE_ID LIMIT $2) p \
    JOIN DB_MESSAGES m ON m.MESSAGE_ID = p.MESSAGE_ID";

/// The session paged messages are read on, and its two statements.
struct PagedReader {
    client: tokio_postgres::Client,
    read: tokio_postgres::Statement,
    scan: tokio_postgres::Statement,
}

/// The reader's session, which runs nothing but `READ_PAGED` and `SCAN_PAGED`. A flip lost in
/// a crash leaves the row paged, to be read again, so the queue need not wait
/// for its fsync. That statement has one right plan, an index in order as far
/// as the limit and then each message by key: statistics taken
/// before the backlog built make the planner expect one row and fetch and
/// sort the whole backlog instead, on every read, until the next analyze. Its
/// result is sorted here, as an `ORDER BY` invites a walk of all the messages
/// in order.
const READER_SESSION: &str = "SET synchronous_commit = off; SET jit = off;     SET enable_seqscan = off; SET enable_bitmapscan = off;     SET enable_hashjoin = off; SET enable_mergejoin = off";

/// Takes the database for this broker, on a session of its own that stays
/// open: the database gives the lock back if the session ends, however the
/// broker ended. Artemis keeps a primary only with a local file lock, so
/// nothing stops two brokers on two hosts; this stops two of these. Losing
/// the session loses the lock, and the store stops.
async fn hold_lock(
    wait: Duration,
    config: &writer::Target,
    lost: Arc<dyn Fn(&JournalError) + Send + Sync>,
) -> StoreResult<()> {
    let client = writer::connect(config).await.map_err(io_error)?;
    // A host that vanished is noticed in under a minute, not hours.
    client
        .batch_execute(
            "SET tcp_keepalives_idle = 15; SET tcp_keepalives_interval = 5; SET tcp_keepalives_count = 3",
        )
        .await
        .map_err(|e| table_error("locking the database", e))?;
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let held: bool = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&LOCK_KEY])
            .await
            .map_err(|e| table_error("locking the database", e))?
            .get(0);
        if held {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(JournalError::IllegalState(
                "another Saranga broker is using this database".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::spawn(async move {
        while !client.is_closed() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        lost(&io_error("the database lock's session ended"));
    });
    Ok(())
}

fn table_error(what: &str, e: tokio_postgres::Error) -> JournalError {
    io_error(format!("{what}: {e}"))
}

impl DatabaseStorage {
    /// Connects and creates the tables Artemis would; nothing is loaded yet.
    pub fn open(cfg: StorageConfig, database: &DatabaseConfig) -> StoreResult<Self> {
        let connection = client_config(database).map_err(JournalError::IllegalState)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("database-store")
            .enable_all()
            .build()
            .map_err(|e| JournalError::Io(Arc::new(e)))?;
        let critical: Arc<OnceLock<CriticalListener>> = Arc::new(OnceLock::new());
        let settings = Settings {
            config: connection.clone(),
            connections: database.connections.max(1) as usize,
            max_retries: database.max_retries.max(0) as u32,
            retry_interval: Duration::from_millis(database.retry_interval_millis.max(0) as u64),
            max_pending_writes: database.max_pending_writes.max(1) as usize,
        };
        let listener = critical.clone();
        let on_failure: Arc<dyn Fn(&JournalError) + Send + Sync> = Arc::new(move |e| {
            tracing::warn!("AMQ144010: Critical IO Exception happened: {e}");
            if let Some(l) = listener.get() {
                l(e, None);
            }
        });
        let schema = connection.clone();
        let client_lock = LOCK_WAIT;
        let lost = on_failure.clone();
        let (started, setup) = std::sync::mpsc::channel();
        runtime.spawn(async move {
            let work = async move {
                let client = writer::connect(&schema).await.map_err(io_error)?;
                hold_lock(client_lock, &schema, lost).await?;
                // `IF NOT EXISTS` otherwise notes every table already there.
                client
                    .batch_execute("SET client_min_messages = warning")
                    .await
                    .map_err(io_error)?;
                for statement in SCHEMA {
                    client
                        .batch_execute(statement)
                        .await
                        .map_err(|e| table_error("creating the tables", e))?;
                }
                writer::start(settings, on_failure).await
            };
            let _ = started.send(work.await);
        });
        let writes = Arc::new(setup.recv().unwrap_or_else(|_| {
            Err(JournalError::IllegalState("the store did not start".into()))
        })?);
        Ok(Self {
            cfg,
            connection,
            ids: BatchingIdGenerator::new(
                0,
                CHECKPOINT_BATCH_SIZE,
                DatabaseIdStore(writes.clone()),
            ),
            writes,
            runtime: Background(Some(runtime)),
            critical,
            transactions: Mutex::new(HashMap::new()),
            prepared: Mutex::new(HashSet::new()),
            addresses: Mutex::new(HashMap::new()),
            sides: Mutex::new(HashMap::new()),
            read_from: Mutex::new(HashMap::new()),
            reader: tokio::sync::Mutex::new(None),
        })
    }

    /// Runs a query on the store's runtime, from any thread.
    fn run<T: Send + 'static>(
        &self,
        work: impl Future<Output = StoreResult<T>> + Send + 'static,
    ) -> StoreResult<T> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.runtime.spawn(async move {
            let _ = tx.send(work.await);
        });
        rx.recv()
            .unwrap_or_else(|_| Err(JournalError::IllegalState("the store stopped".into())))
    }

    fn sent(&self, ops: Vec<Op>) -> StoreResult<()> {
        self.writes.send(Unit {
            ops,
            done: None,
            barrier: false,
        })
    }

    fn pending(&self, ops: Vec<Op>) -> StoreResult<Pending> {
        let (c, p) = Pending::new();
        self.writes.send(Unit {
            ops,
            done: Some(c),
            barrier: false,
        })?;
        Ok(p)
    }

    fn in_tx(&self, tx: i64, op: Op) {
        self.transactions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(tx)
            .or_default()
            .ops
            .push(op);
    }

    fn broker_record(id: i64, record_type: u8, tx: Option<i64>, data: Vec<u8>) -> Op {
        Op::Record {
            table: Generic::Broker,
            id,
            record_type: i16::from(record_type),
            tx,
            data,
        }
    }

    fn config_record(id: i64, record_type: u8, data: Vec<u8>) -> Op {
        Op::Record {
            table: Generic::Config,
            id,
            record_type: i16::from(record_type),
            tx: None,
            data,
        }
    }

    fn message_op(message: &ServerMessage, tx: Option<i64>) -> StoreResult<Op> {
        let (_, record) = message_record(message);
        let body = match message.large() {
            Some(b) => Some(b.read_all().map_err(|e| JournalError::Io(Arc::new(e)))?),
            None => None,
        };
        Ok(Op::Message {
            id: message.message_id(),
            tx,
            record,
            memory_estimate: i32::try_from(message.memory_estimate()).unwrap_or(i32::MAX),
            large: message.is_large(),
            core: message.native().is_none(),
            body,
        })
    }

    /// A large body in the table, as the file the broker reads it from.
    fn body_file(&self, id: i64, body: Option<Vec<u8>>) -> StoreResult<()> {
        let Some(body) = body else {
            return Ok(());
        };
        let path = self.cfg.large_messages.join(format!("{id}.msg"));
        std::fs::create_dir_all(&self.cfg.large_messages)
            .and_then(|()| std::fs::write(path, body))
            .map_err(|e| JournalError::Io(Arc::new(e)))
    }

    fn decoded(&self, id: i64, data: &[u8], large: bool, core: bool) -> StoreResult<StoredMessage> {
        let _ = id;
        if large && core {
            Ok(StoredMessage::Core(decode_large_message(data)?))
        } else {
            decode_message(data)
        }
    }

    /// A reference's side records go with it.
    fn unreference(&self, queue_id: i64, message_id: i64) -> Vec<Op> {
        let mut ops = vec![Op::Unreference {
            message: message_id,
            queue: queue_id,
        }];
        let side = self
            .sides
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(message_id, queue_id));
        if let Some(side) = side {
            for id in [side.delivery, side.scheduled].into_iter().flatten() {
                ops.push(Op::DeleteRecord {
                    table: Generic::Broker,
                    id,
                });
            }
        }
        ops
    }

    /// A side record's body: the message's ID, then the journal record's.
    fn side_record(
        &self,
        queue_id: i64,
        message_id: i64,
        record_type: u8,
        tx: Option<i64>,
        body: &[u8],
    ) -> Op {
        let mut sides = self.sides.lock().unwrap_or_else(|e| e.into_inner());
        let side = sides.entry((message_id, queue_id)).or_default();
        let slot = if record_type == ids::UPDATE_DELIVERY_COUNT {
            &mut side.delivery
        } else {
            &mut side.scheduled
        };
        let id = *slot.get_or_insert_with(|| self.generate_id());
        let mut data = Vec::with_capacity(8 + body.len());
        data.extend_from_slice(&message_id.to_be_bytes());
        data.extend_from_slice(body);
        Self::broker_record(id, record_type, tx, data)
    }

    fn binding_ops(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<Vec<Op>> {
        match user_type {
            ids::ADDRESS_BINDING_RECORD => {
                let b = PersistentAddressBindingEncoding::decode(&body)
                    .map_err(corrupt("address binding"))?;
                let name = b.name.to_string_lossy();
                self.addresses
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(name.clone(), id);
                Ok(vec![
                    Op::Address {
                        id,
                        name,
                        multicast: b.routing_types.contains(&MULTICAST),
                        anycast: b.routing_types.contains(&ANYCAST),
                    },
                    Self::config_record(id, user_type, body),
                ])
            }
            ids::QUEUE_BINDING_RECORD => {
                let b = PersistentQueueBindingEncoding::decode(&body)
                    .map_err(corrupt("queue binding"))?;
                let address = b.address.to_string_lossy();
                let address_id = self
                    .addresses
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&address)
                    .copied()
                    .unwrap_or(-1);
                let routing = b.consumers.map(|(_, _, r)| r);
                Ok(vec![Op::Queue {
                    id,
                    address: address_id,
                    name: b.name.to_string_lossy(),
                    multicast: routing == Some(MULTICAST),
                    anycast: routing == Some(ANYCAST),
                    filter: b
                        .filter_string
                        .as_ref()
                        .map(|f| f.to_string_lossy().into_bytes()),
                    config: queue_json(id, &b).into_bytes(),
                }])
            }
            _ => Ok(vec![Self::config_record(id, user_type, body)]),
        }
    }

    fn read_paged_after(
        &self,
        queue_id: i64,
        limit: i64,
        after: i64,
    ) -> StoreResult<Vec<(StoredMessage, bool)>> {
        self.paged_rows(false, queue_id, limit, after)
    }

    /// `READ_PAGED`, or with `scan` `SCAN_PAGED`.
    fn paged_rows(
        &self,
        scan: bool,
        queue_id: i64,
        limit: i64,
        after: i64,
    ) -> StoreResult<Vec<(StoredMessage, bool)>> {
        let rows = {
            let (done, result) = std::sync::mpsc::channel();
            let reader = &self.reader;
            let config = &self.connection;
            // The reader is the store's own, so this stays on the store's
            // runtime for as long as the query takes.
            self.runtime.block_in(async {
                let mut slot = reader.lock().await;
                if slot.as_ref().is_none_or(|r| r.client.is_closed()) {
                    let client = writer::connect(config).await.map_err(io_error);
                    let client = match client {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = done.send(Err(e));
                            return;
                        }
                    };
                    let prepared = async {
                        client.batch_execute(READER_SESSION).await?;
                        Ok::<_, tokio_postgres::Error>((
                            client.prepare(READ_PAGED).await?,
                            client.prepare(SCAN_PAGED).await?,
                        ))
                    };
                    match prepared.await {
                        Ok((read, scan)) => *slot = Some(PagedReader { client, read, scan }),
                        Err(e) => {
                            let _ = done.send(Err(io_error(e)));
                            return;
                        }
                    }
                }
                let r = slot.as_ref().expect("a reader");
                let statement = if scan { &r.scan } else { &r.read };
                let read = r
                    .client
                    .query(statement, &[&queue_id, &limit, &after])
                    .await
                    .map_err(|e| table_error("reading paged messages", e));
                let _ = done.send(read);
            });
            result
                .recv()
                .unwrap_or_else(|_| Err(JournalError::IllegalState("the store stopped".into())))?
        };
        let mut rows = rows;
        rows.sort_unstable_by_key(|r| r.get::<_, i64>(0));
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id: i64 = r.get(0);
            let data: Vec<u8> = r.get::<_, Option<Vec<u8>>>(1).unwrap_or_default();
            let large = r.get::<_, Option<String>>(2).as_deref() == Some("Y");
            let core = r.get::<_, Option<String>>(3).as_deref() == Some("Y");
            if large {
                self.body_file(id, r.get(4))?;
            }
            out.push((self.decoded(id, &data, large, core)?, large));
        }
        Ok(out)
    }

    /// Ends a transaction. What was not prepared is written now, all at
    /// once; what was prepared is in the tables, marked with its ID.
    fn finish(&self, tx: i64, commit: bool) -> StoreResult<Pending> {
        let open = self
            .transactions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&tx)
            .unwrap_or_default();
        let prepared = self
            .prepared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&tx);
        let ops = if prepared {
            let mut ops = vec![Op::Finish { tx, commit }];
            if commit {
                ops.extend(open.deletes);
            }
            ops
        } else if commit {
            let mut ops = open.ops;
            ops.extend(open.deletes);
            ops
        } else {
            Vec::new()
        };
        self.pending(ops)
    }
}

fn add(fields: &mut Vec<String>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        fields.push(format!("{}:{value}", json_string(key)));
    }
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `QueueConfiguration.toJSON` of a queue binding.
fn queue_json(id: i64, b: &PersistentQueueBindingEncoding) -> String {
    let text = |s: &saranga_buffer::SimpleString| json_string(&s.to_string_lossy());
    let number = |v: &Option<i64>| v.map(|v| v.to_string());
    let int = |v: &Option<i32>| v.map(|v| v.to_string());
    let boolean = |v: &Option<bool>| v.map(|v| v.to_string());
    let mut f = Vec::new();
    add(&mut f, "id", Some(id.to_string()));
    add(&mut f, "name", Some(text(&b.name)));
    add(&mut f, "address", Some(text(&b.address)));
    if let Some((max_consumers, purge, routing)) = b.consumers {
        let routing = if routing == ANYCAST {
            "ANYCAST"
        } else {
            "MULTICAST"
        };
        add(&mut f, "routing-type", Some(json_string(routing)));
        add(&mut f, "filter-string", b.filter_string.as_ref().map(text));
        add(&mut f, "durable", Some("true".to_owned()));
        add(&mut f, "user", b.user().as_deref().map(json_string));
        add(&mut f, "max-consumers", Some(max_consumers.to_string()));
        add(&mut f, "exclusive", boolean(&b.exclusive));
        add(&mut f, "group-rebalance", boolean(&b.group_rebalance));
        add(
            &mut f,
            "group-rebalance-pause-dispatch",
            boolean(&b.group_rebalance_pause_dispatch),
        );
        add(&mut f, "group-buckets", int(&b.group_buckets));
        add(
            &mut f,
            "group-first-key",
            b.group_first_key
                .as_ref()
                .and_then(|k| k.as_ref())
                .map(text),
        );
        add(&mut f, "last-value", boolean(&b.last_value));
        add(
            &mut f,
            "last-value-key",
            b.last_value_key.as_ref().and_then(|k| k.as_ref()).map(text),
        );
        add(&mut f, "non-destructive", boolean(&b.non_destructive));
        add(&mut f, "purge-on-no-consumers", Some(purge.to_string()));
    } else {
        add(&mut f, "filter-string", b.filter_string.as_ref().map(text));
        add(&mut f, "durable", Some("true".to_owned()));
        add(&mut f, "user", b.user().as_deref().map(json_string));
    }
    add(&mut f, "enabled", boolean(&b.enabled));
    add(
        &mut f,
        "consumers-before-dispatch",
        int(&b.consumers_before_dispatch),
    );
    add(
        &mut f,
        "delay-before-dispatch",
        number(&b.delay_before_dispatch),
    );
    add(&mut f, "auto-delete", boolean(&b.auto_delete));
    add(&mut f, "auto-delete-delay", number(&b.auto_delete_delay));
    add(
        &mut f,
        "auto-delete-message-count",
        number(&b.auto_delete_message_count),
    );
    add(&mut f, "ring-size", number(&b.ring_size));
    add(
        &mut f,
        "configuration-managed",
        boolean(&b.configuration_managed),
    );
    add(&mut f, "internal", boolean(&b.internal));
    add(&mut f, "auto-created", Some(b.auto_created.to_string()));
    format!("{{{}}}", f.join(","))
}

/// `QueueConfiguration.fromJSON` as the binding it was made from.
fn queue_binding_of(json: &str, name: &str) -> StoreResult<PersistentQueueBindingEncoding> {
    let j = Json::parse(json).map_err(|e| JournalError::Corrupt(format!("queue {name}: {e}")))?;
    let text = |k: &str| j.get(k).and_then(Json::as_str).map(str::to_owned);
    let simple = |k: &str| text(k).map(saranga_buffer::SimpleString::from);
    let boolean = |k: &str| j.get(k).and_then(Json::as_bool);
    let long = |k: &str| j.get(k).and_then(Json::as_i64);
    let int = |k: &str| long(k).map(|v| v as i32);
    let routing = match text("routing-type").as_deref() {
        Some("ANYCAST") => ANYCAST,
        _ => MULTICAST,
    };
    Ok(PersistentQueueBindingEncoding {
        name: simple("name").unwrap_or_else(|| name.into()),
        address: simple("address").unwrap_or_else(|| name.into()),
        filter_string: simple("filter-string"),
        metadata: Some(PersistentQueueBindingEncoding::metadata_for(
            text("user").as_deref(),
        )),
        auto_created: boolean("auto-created").unwrap_or(false),
        consumers: Some((
            int("max-consumers").unwrap_or(-1),
            boolean("purge-on-no-consumers").unwrap_or(false),
            routing,
        )),
        exclusive: boolean("exclusive"),
        last_value: boolean("last-value"),
        configuration_managed: boolean("configuration-managed"),
        consumers_before_dispatch: int("consumers-before-dispatch"),
        delay_before_dispatch: long("delay-before-dispatch"),
        last_value_key: simple("last-value-key").map(Some),
        non_destructive: boolean("non-destructive"),
        group_rebalance: boolean("group-rebalance"),
        group_buckets: int("group-buckets"),
        auto_delete: boolean("auto-delete"),
        auto_delete_delay: long("auto-delete-delay"),
        auto_delete_message_count: long("auto-delete-message-count"),
        group_first_key: simple("group-first-key").map(Some),
        ring_size: long("ring-size"),
        enabled: boolean("enabled"),
        group_rebalance_pause_dispatch: boolean("group-rebalance-pause-dispatch"),
        internal: boolean("internal"),
    })
}

fn record(id: i64, user_record_type: u8, data: Vec<u8>) -> RecordInfo {
    RecordInfo {
        id,
        user_record_type,
        data,
        is_update: false,
        replaceable_update: false,
        compact_count: 0,
    }
}

/// The rows `load_bindings` reads.
struct BindingRows {
    addresses: Vec<(i64, String, bool, bool)>,
    queues: Vec<(i64, String, Option<Vec<u8>>)>,
    config: Vec<(i64, i16, Option<Vec<u8>>)>,
}

/// `DB_ADDRESS`, as the data tools read it.
#[derive(Debug)]
pub struct AddressRow {
    pub id: i64,
    pub name: String,
    pub multicast: bool,
    pub anycast: bool,
}

/// `DB_QUEUE`.
#[derive(Debug)]
pub struct QueueRow {
    pub id: i64,
    pub address: i64,
    pub name: String,
    pub multicast: bool,
    pub anycast: bool,
    pub filter: Option<String>,
    /// `QueueConfiguration.toJSON`.
    pub config: String,
}

/// `DB_MESSAGES`.
#[derive(Debug)]
pub struct StoredRow {
    pub id: i64,
    pub tx: Option<i64>,
    pub record: Vec<u8>,
    pub memory_estimate: i32,
    pub large: bool,
    pub core: bool,
    pub body: Option<Vec<u8>>,
}

/// `DB_REFERENCES`.
#[derive(Debug)]
pub struct ReferenceRow {
    pub message: i64,
    pub queue: i64,
    pub paged: bool,
    pub tx: Option<i64>,
    pub ack_tx: Option<i64>,
}

/// `DB_CONFIG_DATA` or `DB_BROKER_DATA`.
#[derive(Debug)]
pub struct GenericRow {
    pub id: i64,
    pub record_type: i16,
    pub tx: Option<i64>,
    pub data: Vec<u8>,
}

/// What the store's tables hold, for the data tools.
#[derive(Debug, Default)]
pub struct Tables {
    pub addresses: Vec<AddressRow>,
    pub queues: Vec<QueueRow>,
    pub messages: Vec<StoredRow>,
    pub references: Vec<ReferenceRow>,
    pub config: Vec<GenericRow>,
    pub broker: Vec<GenericRow>,
    /// Transaction and encoded Xid.
    pub prepared: Vec<(i64, Vec<u8>)>,
}

fn is_y(row: &tokio_postgres::Row, column: usize) -> bool {
    row.get::<_, Option<String>>(column).as_deref() == Some("Y")
}

fn text_of(bytes: Option<Vec<u8>>) -> Option<String> {
    bytes.map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// Reads the tables, without taking the store: nothing here writes.
pub fn read_tables(d: &DatabaseConfig) -> StoreResult<Tables> {
    // On a thread of its own, as the tools run on a runtime already.
    std::thread::scope(|s| {
        s.spawn(|| tables_of(d)).join().unwrap_or_else(|_| {
            Err(JournalError::IllegalState(
                "reading the store failed".into(),
            ))
        })
    })
}

fn tables_of(d: &DatabaseConfig) -> StoreResult<Tables> {
    let config = client_config(d).map_err(JournalError::IllegalState)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| JournalError::Io(Arc::new(e)))?;
    runtime.block_on(async {
        let client = writer::connect(&config).await.map_err(io_error)?;
        let rows = async |sql: &str| {
            client
                .query(sql, &[])
                .await
                .map_err(|e| table_error("reading the store", e))
        };
        let generic = async |table: &str| -> StoreResult<Vec<GenericRow>> {
            let sql =
                format!("SELECT ID, RECORD_TYPE, TX_ID, DATA_RECORD FROM {table} ORDER BY ID");
            Ok(rows(&sql)
                .await?
                .iter()
                .map(|r| GenericRow {
                    id: r.get(0),
                    record_type: r.get(1),
                    tx: r.get(2),
                    data: r.get::<_, Option<Vec<u8>>>(3).unwrap_or_default(),
                })
                .collect())
        };
        Ok(Tables {
            addresses: rows(
                "SELECT ADDRESS_ID, ADDRESS_NAME, IS_MULTICAST::text, IS_ANYCAST::text \
                 FROM DB_ADDRESS ORDER BY ADDRESS_ID",
            )
            .await?
            .iter()
            .map(|r| AddressRow {
                id: r.get(0),
                name: r.get(1),
                multicast: is_y(r, 2),
                anycast: is_y(r, 3),
            })
            .collect(),
            queues: rows(
                "SELECT QUEUE_ID, ADDRESS_ID, QUEUE_NAME, IS_MULTICAST::text, IS_ANYCAST::text, \
                 FILTER_STRING, QUEUE_CONFIG FROM DB_QUEUE ORDER BY QUEUE_ID",
            )
            .await?
            .iter()
            .map(|r| QueueRow {
                id: r.get(0),
                address: r.get(1),
                name: r.get::<_, Option<String>>(2).unwrap_or_default(),
                multicast: is_y(r, 3),
                anycast: is_y(r, 4),
                filter: text_of(r.get(5)),
                config: text_of(r.get(6)).unwrap_or_default(),
            })
            .collect(),
            messages: rows(
                "SELECT MESSAGE_ID, TX_ID, MESSAGE_RECORD, MEMORY_ESTIMATE, IS_LARGE::text, \
                 IS_CORE::text, LARGE_BODY FROM DB_MESSAGES ORDER BY MESSAGE_ID",
            )
            .await?
            .iter()
            .map(|r| StoredRow {
                id: r.get(0),
                tx: r.get(1),
                record: r.get::<_, Option<Vec<u8>>>(2).unwrap_or_default(),
                memory_estimate: r.get::<_, Option<i32>>(3).unwrap_or(0),
                large: is_y(r, 4),
                core: is_y(r, 5),
                body: r.get(6),
            })
            .collect(),
            references: rows(
                "SELECT MESSAGE_ID, QUEUE_ID, PAGED::text, TX_ID, ACK_TX_ID \
                 FROM DB_REFERENCES ORDER BY MESSAGE_ID, QUEUE_ID",
            )
            .await?
            .iter()
            .map(|r| ReferenceRow {
                message: r.get(0),
                queue: r.get(1),
                paged: is_y(r, 2),
                tx: r.get(3),
                ack_tx: r.get(4),
            })
            .collect(),
            config: generic("DB_CONFIG_DATA").await?,
            broker: generic("DB_BROKER_DATA").await?,
            prepared: rows("SELECT TX_ID, XID FROM DB_PREP_TX ORDER BY TX_ID")
                .await?
                .iter()
                .map(|r| (r.get(0), r.get::<_, Option<Vec<u8>>>(1).unwrap_or_default()))
                .collect(),
        })
    })
}

/// A `DB_MESSAGES` row: ID, transaction, record, large, core, large body.
type MessageRow = (i64, Option<i64>, Vec<u8>, bool, bool, Option<Vec<u8>>);
/// A `DB_REFERENCES` row: message, queue, paged, transaction, ack transaction.
type ReferenceTuple = (i64, i64, bool, Option<i64>, Option<i64>);
/// A `DB_BROKER_DATA` row: ID, type, transaction, data.
type RecordRow = (i64, i16, Option<i64>, Option<Vec<u8>>);
/// What a side record changes on its reference.
type SideUpdate = Box<dyn Fn(&mut RecoveredRef)>;

/// The rows `load_messages` reads.
struct MessageRows {
    messages: Vec<MessageRow>,
    paged: Vec<(i64, i64, i64)>,
    references: Vec<ReferenceTuple>,
    records: Vec<RecordRow>,
    prepared: Vec<(i64, Vec<u8>)>,
}

impl Store for DatabaseStorage {
    fn writing(&self) -> Option<(std::time::Duration, u64)> {
        None
    }

    fn journal_directory(&self) -> &Path {
        &self.cfg.messages.directory
    }

    fn set_critical_listener(&self, listener: CriticalListener) {
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
        self.ids
            .generate_id()
            .unwrap_or_else(|_| self.ids.current_id())
    }

    fn load_bindings(&self) -> StoreResult<Vec<RecordInfo>> {
        let config = self.connection.clone();
        let rows = self.run(async move {
            let client = writer::connect(&config).await.map_err(io_error)?;
            let q = |e| table_error("reading the bindings", e);
            let addresses = client
                .query(
                    "SELECT ADDRESS_ID, ADDRESS_NAME, IS_MULTICAST::text, IS_ANYCAST::text FROM DB_ADDRESS ORDER BY ADDRESS_ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| {
                    let flag = |i: usize| r.get::<_, Option<String>>(i).as_deref() == Some("Y");
                    (r.get(0), r.get(1), flag(2), flag(3))
                })
                .collect();
            let queues = client
                .query(
                    "SELECT QUEUE_ID, QUEUE_NAME, QUEUE_CONFIG FROM DB_QUEUE ORDER BY QUEUE_ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| (r.get(0), r.get::<_, Option<String>>(1).unwrap_or_default(), r.get(2)))
                .collect();
            let config = client
                .query(
                    "SELECT ID, RECORD_TYPE, DATA_RECORD FROM DB_CONFIG_DATA ORDER BY ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect();
            Ok(BindingRows {
                addresses,
                queues,
                config,
            })
        })?;
        let mut full_addresses: HashMap<i64, Vec<u8>> = HashMap::new();
        let mut out = Vec::new();
        for (id, record_type, data) in rows.config {
            let data = data.unwrap_or_default();
            let record_type = record_type as u8;
            match record_type {
                ids::ID_COUNTER_RECORD => self
                    .ids
                    .load_state(id, &data)
                    .map_err(corrupt("ID counter"))?,
                ids::ADDRESS_BINDING_RECORD => {
                    full_addresses.insert(id, data);
                }
                other => out.push(record(id, other, data)),
            }
        }
        self.ids.cleanup();
        let mut known = self.addresses.lock().unwrap_or_else(|e| e.into_inner());
        for (id, name, multicast, anycast) in rows.addresses {
            let body = full_addresses.remove(&id).unwrap_or_else(|| {
                // A row Artemis wrote: all it knows of the address.
                let mut routing_types = Vec::new();
                if multicast {
                    routing_types.push(MULTICAST);
                }
                if anycast {
                    routing_types.push(ANYCAST);
                }
                PersistentAddressBindingEncoding {
                    name: name.as_str().into(),
                    routing_types,
                    auto_created: false,
                    internal: None,
                }
                .encode()
            });
            known.insert(name, id);
            out.push(record(id, ids::ADDRESS_BINDING_RECORD, body));
        }
        for (id, name, config) in rows.queues {
            let json = config.ok_or_else(|| {
                JournalError::Corrupt(format!("queue {name} has no configuration"))
            })?;
            let json = String::from_utf8(json)
                .map_err(|e| JournalError::Corrupt(format!("queue {name}: {e}")))?;
            out.push(record(
                id,
                ids::QUEUE_BINDING_RECORD,
                queue_binding_of(&json, &name)?.encode(),
            ));
        }
        Ok(out)
    }

    fn load_messages(&self) -> StoreResult<RecoveredMessages> {
        let config = self.connection.clone();
        let rows = self.run(async move {
            let client = writer::connect(&config).await.map_err(io_error)?;
            let q = |e| table_error("reading the messages", e);
            let yes = |v: Option<String>| v.as_deref() == Some("Y");
            let messages = client
                .query(
                    // Paged messages stay in the table until read.
                    "SELECT m.MESSAGE_ID, m.TX_ID, m.MESSAGE_RECORD, m.IS_LARGE::text, m.IS_CORE::text, m.LARGE_BODY \
                     FROM DB_MESSAGES m WHERE EXISTS (SELECT 1 FROM DB_REFERENCES r \
                     WHERE r.MESSAGE_ID = m.MESSAGE_ID AND (r.PAGED = 'N' OR r.TX_ID IS NOT NULL)) \
                     ORDER BY m.MESSAGE_ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| {
                    (
                        r.get(0),
                        r.get(1),
                        r.get::<_, Option<Vec<u8>>>(2).unwrap_or_default(),
                        yes(r.get(3)),
                        yes(r.get(4)),
                        r.get(5),
                    )
                })
                .collect();
            let paged = client
                .query(
                    "SELECT r.QUEUE_ID, COUNT(*), COALESCE(SUM(LENGTH(m.MESSAGE_RECORD)), 0)::bigint \
                     FROM DB_REFERENCES r JOIN DB_MESSAGES m ON m.MESSAGE_ID = r.MESSAGE_ID \
                     WHERE r.PAGED = 'Y' AND r.TX_ID IS NULL GROUP BY r.QUEUE_ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect();
            // handleOrphanedMessages.
            client
                .execute(
                    "DELETE FROM DB_MESSAGES m WHERE NOT EXISTS \
                     (SELECT 1 FROM DB_REFERENCES r WHERE r.MESSAGE_ID = m.MESSAGE_ID)",
                    &[],
                )
                .await
                .map_err(q)?;
            let references = client
                .query(
                    "SELECT MESSAGE_ID, QUEUE_ID, PAGED::text, TX_ID, ACK_TX_ID FROM DB_REFERENCES \
                     WHERE PAGED = 'N' OR TX_ID IS NOT NULL ORDER BY MESSAGE_ID, QUEUE_ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| (r.get(0), r.get(1), yes(r.get(2)), r.get(3), r.get(4)))
                .collect();
            let records = client
                .query(
                    "SELECT ID, RECORD_TYPE, TX_ID, DATA_RECORD FROM DB_BROKER_DATA ORDER BY ID",
                    &[],
                )
                .await
                .map_err(q)?
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                .collect();
            let prepared = client
                .query("SELECT TX_ID, XID FROM DB_PREP_TX ORDER BY TX_ID", &[])
                .await
                .map_err(q)?
                .iter()
                .map(|r| (r.get(0), r.get::<_, Option<Vec<u8>>>(1).unwrap_or_default()))
                .collect();
            Ok(MessageRows {
                messages,
                paged,
                references,
                records,
                prepared,
            })
        })?;

        let mut out = RecoveredMessages::default();
        for (id, _tx, data, large, core, body) in rows.messages {
            out.messages
                .insert(id, self.decoded(id, &data, large, core)?);
            if large {
                out.large.insert(id);
                self.body_file(id, body)?;
            }
        }
        out.paged = rows
            .paged
            .into_iter()
            .map(|(q, n, s)| (q, (n, s)))
            .collect();

        let mut prepared: HashMap<i64, RecoveredTx> = HashMap::new();
        for (tx, xid) in rows.prepared {
            prepared.insert(
                tx,
                RecoveredTx {
                    id: tx,
                    xid: decode_xid(&xid)?,
                    sends: Vec::new(),
                    acks: Vec::new(),
                    duplicates: Vec::new(),
                    page_txs: Vec::new(),
                    cursor_acks: Vec::new(),
                    page_tx_updates: Vec::new(),
                },
            );
            self.prepared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(tx);
        }

        // Per queue, in message order, with its side records to come.
        let mut queues: HashMap<i64, Vec<RecoveredRef>> = HashMap::new();
        let mut at: HashMap<(i64, i64), usize> = HashMap::new();
        for (message, queue, _paged, tx, ack_tx) in rows.references {
            if !out.messages.contains_key(&message) {
                tracing::warn!("AMQ222031: Cannot find message {message}");
                continue;
            }
            if let Some(t) = tx.and_then(|t| prepared.get_mut(&t)) {
                t.sends.push((queue, message));
                continue;
            }
            if let Some(t) = ack_tx.and_then(|t| prepared.get_mut(&t)) {
                t.acks.push((queue, message));
            }
            let list = queues.entry(queue).or_default();
            at.insert((message, queue), list.len());
            list.push(RecoveredRef {
                message_id: message,
                delivery_count: 0,
                scheduled_time: 0,
            });
        }

        let mut page_txs: Vec<RecoveredPageTx> = Vec::new();
        let mut page_updates: Vec<(i64, i64, i32)> = Vec::new();
        let mut stale = Vec::new();
        let mut sides = self.sides.lock().unwrap_or_else(|e| e.into_inner());
        for (id, record_type, tx, data) in rows.records {
            let data = data.unwrap_or_default();
            let holder = tx.and_then(|t| prepared.get_mut(&t));
            match record_type as u8 {
                t @ (ids::UPDATE_DELIVERY_COUNT | ids::SET_SCHEDULED_DELIVERY_TIME) => {
                    let Some((message, body)) = data.split_first_chunk::<8>() else {
                        return Err(JournalError::Corrupt(format!("side record {id}")));
                    };
                    let message = i64::from_be_bytes(*message);
                    let (queue, apply): (i64, SideUpdate) = if t == ids::UPDATE_DELIVERY_COUNT {
                        let u = DeliveryCountUpdateEncoding::decode(body)
                            .map_err(corrupt("delivery count"))?;
                        (u.queue_id, Box::new(move |r| r.delivery_count = u.count))
                    } else {
                        let s = ScheduledDeliveryEncoding::decode(body)
                            .map_err(corrupt("scheduled delivery"))?;
                        let time = s.scheduled_delivery_time;
                        (s.queue_id, Box::new(move |r| r.scheduled_time = time))
                    };
                    match at.get(&(message, queue)) {
                        Some(&i) => {
                            apply(&mut queues.get_mut(&queue).expect("its queue")[i]);
                            let side = sides.entry((message, queue)).or_default();
                            if t == ids::UPDATE_DELIVERY_COUNT {
                                side.delivery = Some(id);
                            } else {
                                side.scheduled = Some(id);
                            }
                        }
                        // Its reference is gone: Artemis acknowledged it.
                        None => stale.push(id),
                    }
                }
                ids::DUPLICATE_ID => {
                    let d = RecoveredDuplicate::decode(id, &data)?;
                    match holder {
                        Some(t) => t.duplicates.push(d),
                        None => out.duplicates.push(d),
                    }
                }
                ids::ACKNOWLEDGE_CURSOR => {
                    let c = RecoveredCursorRecord::decode(id, &data)?;
                    match holder {
                        Some(t) => t.cursor_acks.push(c),
                        None => out.cursor_acks.push(c),
                    }
                }
                ids::PAGE_CURSOR_COMPLETE => out
                    .page_completes
                    .push(RecoveredCursorRecord::decode(id, &data)?),
                ids::PAGE_TRANSACTION => {
                    let e = PageTransactionEncoding::decode(&data)
                        .map_err(corrupt("page transaction"))?;
                    let p = RecoveredPageTx {
                        record: id,
                        tx_id: e.transaction_id,
                        messages: e.records,
                    };
                    match holder {
                        Some(t) => t.page_txs.push(p),
                        None => page_txs.push(p),
                    }
                }
                PAGE_TRANSACTION_UPDATE => {
                    let e = PageTransactionEncoding::decode(&data)
                        .map_err(corrupt("page transaction"))?;
                    match holder {
                        Some(t) => t.page_tx_updates.push((e.transaction_id, e.records)),
                        None => page_updates.push((id, e.transaction_id, e.records)),
                    }
                }
                ids::HEURISTIC_COMPLETION => {
                    use saranga_buffer::Codec;
                    let mut reader = Reader::new(&data);
                    let xid = Xid::read(&mut reader)
                        .map_err(|e| JournalError::Corrupt(format!("xid: {e}")))?;
                    let commit = reader
                        .bool()
                        .map_err(|e| JournalError::Corrupt(format!("heuristic completion: {e}")))?;
                    out.heuristics.push((id, xid, commit));
                }
                other => out.extra.push(ExtraRecord {
                    id,
                    record_type: other,
                    data,
                }),
            }
        }
        drop(sides);

        let mut cleanup: Vec<Op> = stale
            .into_iter()
            .map(|id| Op::DeleteRecord {
                table: Generic::Broker,
                id,
            })
            .collect();
        for (record_id, tx_id, consumed) in page_updates {
            match page_txs.iter_mut().find(|p| p.tx_id == tx_id) {
                Some(p) => p.messages -= consumed,
                None => cleanup.push(Op::DeleteRecord {
                    table: Generic::Broker,
                    id: record_id,
                }),
            }
        }
        for p in page_txs {
            if p.messages > 0 {
                out.page_txs.push(p);
            } else {
                cleanup.push(Op::DeleteRecord {
                    table: Generic::Broker,
                    id: p.record,
                });
            }
        }
        out.queues = queues;
        out.prepared = prepared.into_values().collect();
        out.prepared.sort_by_key(|t| t.id);

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
            cleanup.push(Op::DeleteMessage { id });
        }
        if !cleanup.is_empty() {
            self.sent(cleanup)?;
        }
        Ok(out)
    }

    fn stop(&self) -> StoreResult<()> {
        self.ids.stop();
        let (tx, rx) = std::sync::mpsc::channel();
        self.writes.send(Unit {
            ops: Vec::new(),
            done: Some(Box::new(move |r| {
                let _ = tx.send(r);
            })),
            barrier: true,
        })?;
        rx.recv()
            .unwrap_or_else(|_| Err(JournalError::IllegalState("the store stopped".into())))
    }

    fn max_record_size(&self) -> usize {
        self.cfg.messages.max_record_size()
    }

    fn retention(&self) -> Option<&Retention> {
        None
    }

    fn force_backup(&self) -> StoreResult<()> {
        Err(JournalError::IllegalState(
            "the database store has no journal files to retain".into(),
        ))
    }

    fn is_history(&self) -> bool {
        false
    }

    fn page_write_history(&self, _message: &ServerMessage, _queue_ids: &[i64]) {}

    fn large_body_history(&self, _message_id: i64, _bytes: &[u8]) {}

    fn store_message(&self, message: &ServerMessage) -> StoreResult<()> {
        self.sent(vec![Self::message_op(message, None)?])
    }

    fn pages_in_store(&self) -> bool {
        true
    }

    fn store_paged(
        &self,
        tx: Option<i64>,
        message: &ServerMessage,
        queue_ids: &[i64],
        written: Box<dyn FnOnce() + Send>,
    ) -> StoreResult<Option<Pending>> {
        let mut ops = vec![Self::message_op(message, None)?];
        ops.extend(queue_ids.iter().map(|&queue| Op::Reference {
            message: message.message_id(),
            queue,
            tx: None,
            paged: true,
        }));
        match tx {
            Some(tx) => {
                ops.into_iter().for_each(|op| self.in_tx(tx, op));
                Ok(None)
            }
            None => {
                let (c, p) = Pending::new();
                self.writes.send(Unit {
                    ops,
                    done: Some(Box::new(move |r| {
                        c(r);
                        written();
                    })),
                    barrier: false,
                })?;
                Ok(Some(p))
            }
        }
    }

    fn read_paged(&self, queue_id: i64, limit: i64) -> StoreResult<Vec<(StoredMessage, bool)>> {
        let after = self
            .read_from
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&queue_id)
            .copied()
            .unwrap_or(i64::MIN);
        let mut read = self.read_paged_after(queue_id, limit, after)?;
        // A transaction that committed late may hold lower IDs.
        if read.is_empty() && after != i64::MIN {
            read = self.read_paged_after(queue_id, limit, i64::MIN)?;
        }
        let mut from = self.read_from.lock().unwrap_or_else(|e| e.into_inner());
        match read.last() {
            Some((m, _)) => {
                from.insert(queue_id, m.message_id());
            }
            None => {
                from.remove(&queue_id);
            }
        }
        Ok(read)
    }

    fn scan_paged(
        &self,
        queue_id: i64,
        limit: i64,
        after: i64,
    ) -> StoreResult<Vec<(StoredMessage, bool)>> {
        self.paged_rows(true, queue_id, limit, after)
    }

    fn settle(&self) -> StoreResult<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.writes.send(Unit {
            ops: Vec::new(),
            done: Some(Box::new(move |r| {
                let _ = tx.send(r);
            })),
            barrier: true,
        })?;
        rx.recv()
            .unwrap_or_else(|_| Err(JournalError::IllegalState("the store stopped".into())))
    }

    fn store_reference(&self, queue_id: i64, message_id: i64, _last: bool) -> StoreResult<Pending> {
        self.pending(vec![Op::Reference {
            message: message_id,
            queue: queue_id,
            tx: None,
            paged: false,
        }])
    }

    fn store_acknowledge(&self, queue_id: i64, message_id: i64) -> StoreResult<Pending> {
        self.pending(self.unreference(queue_id, message_id))
    }

    fn delete_message(&self, message_id: i64) -> StoreResult<()> {
        self.sent(vec![Op::DeleteMessage { id: message_id }])
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
        self.sent(vec![self.side_record(
            queue_id,
            message_id,
            ids::UPDATE_DELIVERY_COUNT,
            None,
            &body,
        )])?;
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
        let body = ScheduledDeliveryEncoding {
            queue_id,
            scheduled_delivery_time: time,
        }
        .encode();
        self.pending(vec![self.side_record(
            queue_id,
            message_id,
            ids::SET_SCHEDULED_DELIVERY_TIME,
            None,
            &body,
        )])
        .map(Some)
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
        let op = self.side_record(
            queue_id,
            message_id,
            ids::SET_SCHEDULED_DELIVERY_TIME,
            None,
            &body,
        );
        self.in_tx(tx, op);
        Ok(())
    }

    fn store_message_tx(&self, tx: i64, message: &ServerMessage) -> StoreResult<()> {
        self.in_tx(tx, Self::message_op(message, None)?);
        Ok(())
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
        self.in_tx(
            tx,
            Self::broker_record(id, ids::ACKNOWLEDGE_CURSOR, None, body),
        );
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
        self.in_tx(
            tx,
            Self::broker_record(id, ids::PAGE_CURSOR_COMPLETE, None, body),
        );
        Ok(id)
    }

    fn store_page_transaction_tx(&self, tx: i64, page_tx: i64, messages: i32) -> StoreResult<i64> {
        let id = self.generate_id();
        let body = PageTransactionEncoding {
            transaction_id: page_tx,
            records: messages,
        }
        .encode();
        self.in_tx(
            tx,
            Self::broker_record(id, ids::PAGE_TRANSACTION, None, body),
        );
        Ok(id)
    }

    fn update_page_transaction_tx(
        &self,
        tx: i64,
        _record: i64,
        page_tx: i64,
        consumed: i32,
    ) -> StoreResult<()> {
        let id = self.generate_id();
        let body = PageTransactionEncoding {
            transaction_id: page_tx,
            records: consumed,
        }
        .encode();
        self.in_tx(
            tx,
            Self::broker_record(id, PAGE_TRANSACTION_UPDATE, None, body),
        );
        Ok(())
    }

    fn delete_record_tx(&self, tx: i64, id: i64) -> StoreResult<()> {
        self.transactions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(tx)
            .or_default()
            .deletes
            .push(Op::DeleteRecord {
                table: Generic::Broker,
                id,
            });
        Ok(())
    }

    fn add_record(
        &self,
        id: i64,
        record_type: u8,
        body: Vec<u8>,
        _sync: bool,
    ) -> StoreResult<Pending> {
        self.pending(vec![Self::broker_record(id, record_type, None, body)])
    }

    fn add_record_tx(&self, tx: i64, id: i64, record_type: u8, body: Vec<u8>) -> StoreResult<()> {
        self.in_tx(tx, Self::broker_record(id, record_type, None, body));
        Ok(())
    }

    fn store_heuristic_completion(&self, id: i64, xid: &Xid, commit: bool) -> StoreResult<Pending> {
        let mut body = encode_xid(xid);
        body.push(u8::from(commit));
        self.add_record(id, ids::HEURISTIC_COMPLETION, body, true)
    }

    fn delete_heuristic_completion(&self, id: i64) -> StoreResult<Pending> {
        self.pending(vec![Op::DeleteRecord {
            table: Generic::Broker,
            id,
        }])
    }

    fn delete_record(&self, id: i64) -> StoreResult<()> {
        self.sent(vec![Op::DeleteRecord {
            table: Generic::Broker,
            id,
        }])
    }

    fn sync_transactional(&self) -> bool {
        self.cfg.sync_transactional
    }

    fn sync_non_transactional(&self) -> bool {
        self.cfg.sync_non_transactional
    }

    fn store_reference_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        self.in_tx(
            tx,
            Op::Reference {
                message: message_id,
                queue: queue_id,
                tx: None,
                paged: false,
            },
        );
        Ok(())
    }

    fn store_acknowledge_tx(&self, tx: i64, queue_id: i64, message_id: i64) -> StoreResult<()> {
        // A prepared transaction marks the reference instead: `prepare`
        // turns this into that.
        self.in_tx(
            tx,
            Op::Unreference {
                message: message_id,
                queue: queue_id,
            },
        );
        Ok(())
    }

    /// Writes the transaction's rows marked with its ID, and the Xid.
    fn prepare(&self, tx: i64, xid: &Xid) -> StoreResult<Pending> {
        let open = {
            let mut transactions = self.transactions.lock().unwrap_or_else(|e| e.into_inner());
            let open = transactions.entry(tx).or_default();
            let mut deletes = std::mem::take(&mut open.deletes);
            let ops = std::mem::take(&mut open.ops);
            let mut marked = Vec::with_capacity(ops.len() + 1);
            for op in ops {
                match op {
                    Op::Message {
                        id,
                        record,
                        memory_estimate,
                        large,
                        core,
                        body,
                        ..
                    } => marked.push(Op::Message {
                        id,
                        tx: Some(tx),
                        record,
                        memory_estimate,
                        large,
                        core,
                        body,
                    }),
                    Op::Reference {
                        message,
                        queue,
                        paged,
                        ..
                    } => marked.push(Op::Reference {
                        message,
                        queue,
                        tx: Some(tx),
                        paged,
                    }),
                    Op::Unreference { message, queue } => {
                        marked.push(Op::PreparedAck { message, queue, tx });
                        // Its side records go when the acknowledgement does.
                        let side = self
                            .sides
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&(message, queue));
                        if let Some(side) = side {
                            for id in [side.delivery, side.scheduled].into_iter().flatten() {
                                deletes.push(Op::DeleteRecord {
                                    table: Generic::Broker,
                                    id,
                                });
                            }
                        }
                    }
                    Op::Record {
                        table,
                        id,
                        record_type,
                        data,
                        ..
                    } => marked.push(Op::Record {
                        table,
                        id,
                        record_type,
                        tx: Some(tx),
                        data,
                    }),
                    other => marked.push(other),
                }
            }
            // Deletes that must survive a crash before the commit.
            let (records, others): (Vec<Op>, Vec<Op>) = deletes.into_iter().partition(|op| {
                matches!(
                    op,
                    Op::DeleteRecord {
                        table: Generic::Broker,
                        ..
                    }
                )
            });
            for op in records {
                if let Op::DeleteRecord { id, .. } = op {
                    marked.push(Self::broker_record(
                        self.generate_id(),
                        PREPARED_DELETE,
                        Some(tx),
                        id.to_be_bytes().to_vec(),
                    ));
                }
            }
            open.deletes = others;
            marked
        };
        self.prepared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tx);
        let mut ops = open;
        ops.push(Op::Prepare {
            tx,
            xid: encode_xid(xid),
        });
        self.pending(ops)
    }

    fn commit(&self, tx: i64) -> StoreResult<Pending> {
        self.finish(tx, true)
    }

    fn rollback(&self, tx: i64) -> StoreResult<Pending> {
        self.finish(tx, false)
    }

    fn written(&self) -> StoreResult<Pending> {
        let (c, p) = Pending::new();
        self.writes.send(Unit {
            ops: Vec::new(),
            done: Some(c),
            barrier: true,
        })?;
        Ok(p)
    }

    fn add_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        write(&self.writes, self.binding_ops(id, user_type, body)?)
    }

    fn replace_binding(
        &self,
        old_id: i64,
        new_id: i64,
        user_type: u8,
        body: Vec<u8>,
    ) -> StoreResult<()> {
        let mut ops = vec![Op::DeleteBinding { id: old_id }];
        ops.extend(self.binding_ops(new_id, user_type, body)?);
        write(&self.writes, ops)
    }

    fn update_binding(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        write(&self.writes, self.binding_ops(id, user_type, body)?)
    }

    fn delete_binding(&self, id: i64) -> StoreResult<()> {
        self.addresses
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, v| *v != id);
        write(&self.writes, vec![Op::DeleteBinding { id }])
    }

    fn add_binding_record(&self, user_type: u8, body: Vec<u8>) -> StoreResult<i64> {
        let id = self.generate_id();
        write(&self.writes, vec![Self::config_record(id, user_type, body)])?;
        Ok(id)
    }

    fn add_binding_record_with_id(&self, id: i64, user_type: u8, body: Vec<u8>) -> StoreResult<()> {
        write(&self.writes, vec![Self::config_record(id, user_type, body)])
    }

    fn delete_bindings(&self, ids: &[i64]) -> StoreResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        write(
            &self.writes,
            ids.iter()
                .map(|&id| Op::DeleteRecord {
                    table: Generic::Config,
                    id,
                })
                .collect(),
        )
    }

    fn delete_binding_record(&self, id: i64) -> StoreResult<()> {
        write(
            &self.writes,
            vec![Op::DeleteRecord {
                table: Generic::Config,
                id,
            }],
        )
    }

    fn store_configuration(
        &self,
        user_type: u8,
        body: Vec<u8>,
        replaces: Option<i64>,
    ) -> StoreResult<i64> {
        let id = self.generate_id();
        let mut ops = vec![Self::config_record(id, user_type, body)];
        if let Some(old) = replaces {
            ops.push(Op::DeleteRecord {
                table: Generic::Config,
                id: old,
            });
        }
        write(&self.writes, ops)?;
        Ok(id)
    }

    fn store_queue_status(&self, queue_id: i64) -> StoreResult<i64> {
        let body = saranga_storage::codec::QueueStatusEncoding {
            queue_id,
            status: 0,
        }
        .encode();
        self.add_binding_record(ids::QUEUE_STATUS_RECORD, body)
    }

    fn store_address_status(&self, address_id: i64) -> StoreResult<i64> {
        let body = saranga_storage::codec::AddressStatusEncoding {
            address_id,
            status: 0,
        }
        .encode();
        self.add_binding_record(ids::ADDRESS_STATUS_RECORD, body)
    }

    fn commit_unsynced(&self, tx: i64, wait: bool) -> StoreResult<()> {
        let pending = self.finish(tx, true)?;
        if wait {
            let (done, rx) = std::sync::mpsc::channel();
            self.runtime.spawn(async move {
                let _ = done.send(pending.wait().await);
            });
            return rx
                .recv()
                .unwrap_or_else(|_| Err(JournalError::IllegalState("the store stopped".into())));
        }
        Ok(())
    }

    fn journals(&self) -> Option<(&Journal, &Journal)> {
        None
    }
}

#[cfg(test)]
mod tests;
