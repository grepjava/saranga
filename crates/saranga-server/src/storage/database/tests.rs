//! The store on a real PostgreSQL: run with `--ignored`, with
//! `SARANGA_TEST_POSTGRES` naming a JDBC URL if not the local default. They
//! empty the tables, so they take turns.

use std::sync::{Mutex, MutexGuard};

use saranga_buffer::SimpleString;
use saranga_message::CoreMessage;
use saranga_storage::codec::{PersistentAddressBindingEncoding, PersistentQueueBindingEncoding};

use super::*;
use crate::storage::{JournalConfig, Pending};

static TURN: Mutex<()> = Mutex::new(());

fn url() -> String {
    std::env::var("SARANGA_TEST_POSTGRES").unwrap_or_else(|_| {
        "jdbc:postgresql://127.0.0.1:5432/saranga?user=saranga&password=saranga".to_owned()
    })
}

fn database() -> DatabaseConfig {
    DatabaseConfig {
        url: url(),
        user: None,
        password: None,
        connections: 6,
        read_connections: 3,
        read_idle_millis: 5000,
        network_timeout_millis: 20_000,
        max_retries: 0,
        retry_interval_millis: 10,
        max_pending_writes: 100_000,
    }
}

fn config(dir: &Path) -> StorageConfig {
    StorageConfig {
        messages: JournalConfig::messages(dir.join("journal")),
        bindings: JournalConfig::bindings(dir.join("bindings")),
        sync_transactional: true,
        sync_non_transactional: true,
        max_redelivery_records: 10,
        large_messages: dir.join("largemessages"),
        paging: dir.join("paging"),
        large_message_sync: false,
        database: Some(database()),
    }
}

struct Fixture {
    _turn: MutexGuard<'static, ()>,
    dir: tempfile::TempDir,
}

impl Fixture {
    /// Empties the tables.
    fn new() -> Self {
        let turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let s = DatabaseStorage::open(config(dir.path()), &database()).unwrap();
        let connection = s.connection.clone();
        s.run(async move {
            let client = writer::connect(&connection).await.map_err(io_error)?;
            client
                .batch_execute(
                    "TRUNCATE DB_MESSAGES, DB_REFERENCES, DB_ADDRESS, DB_QUEUE, DB_BROKER_DATA, DB_CONFIG_DATA, DB_PREP_TX",
                )
                .await
                .map_err(io_error)
        })
        .unwrap();
        Self { _turn: turn, dir }
    }

    fn open(&self) -> DatabaseStorage {
        DatabaseStorage::open(config(self.dir.path()), &database()).unwrap()
    }

    /// Opened and loaded, as a broker starts.
    fn start(&self) -> (DatabaseStorage, RecoveredMessages) {
        let s = self.open();
        s.load_bindings().unwrap();
        let r = s.load_messages().unwrap();
        (s, r)
    }
}

fn message(id: i64) -> Arc<ServerMessage> {
    ServerMessage::new(CoreMessage {
        message_id: id,
        address: Some(SimpleString::from("q")),
        durable: true,
        body: vec![7; 16].into(),
        ..Default::default()
    })
}

fn wait(p: Pending) {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(p.wait())
        .unwrap();
}

fn counted(r: &RecoveredMessages, queue: i64) -> Vec<(i64, i32)> {
    r.queues[&queue]
        .iter()
        .map(|x| (x.message_id, x.delivery_count))
        .collect()
}

#[test]
fn urls() {
    let of = |url: &str| {
        client_config(&DatabaseConfig {
            url: url.to_owned(),
            ..database()
        })
    };
    let c =
        of("jdbc:postgresql://db.example:6000/broker?user=u&password=p&loggerLevel=OFF").unwrap();
    assert_eq!(c.get_user(), Some("u"));
    assert_eq!(c.get_password(), Some(&b"p"[..]));
    assert_eq!(c.get_dbname(), Some("broker"));
    assert_eq!(c.get_ports(), &[6000]);
    assert_eq!(
        of("jdbc:postgresql:broker").unwrap().get_dbname(),
        Some("broker")
    );
    assert!(of("jdbc:mysql://h/d").is_err());
    assert!(of("jdbc:postgresql://a,b/d").is_err());
    assert!(of("jdbc:postgresql://h/d").unwrap().tls.is_some());
    assert!(
        of("jdbc:postgresql://h/d?sslmode=disable")
            .unwrap()
            .tls
            .is_none()
    );
    assert!(of("jdbc:postgresql://h/d?ssl=false").unwrap().tls.is_none());
    assert!(
        of("jdbc:postgresql://h/d?sslmode=require")
            .unwrap()
            .tls
            .is_some()
    );
    let wrong = of("jdbc:postgresql://h/d?sslmode=maybe").unwrap_err();
    assert!(wrong.contains("sslmode"), "{wrong}");
    let half = of("jdbc:postgresql://h/d?sslcert=c.pem").unwrap_err();
    assert!(half.contains("sslkey"), "{half}");
    let missing = of("jdbc:postgresql://h/d?sslmode=verify-full&sslrootcert=/no/such.pem");
    assert!(missing.unwrap_err().contains("TLS"));
    assert_eq!(decoded("C%3A%5Cca.pem"), "C:\\ca.pem");
    // A password stays out of the message.
    let other = of("jdbc:mysql://h/d?password=secret").unwrap_err();
    assert!(!other.contains("secret"), "{other}");
}

#[test]
fn queue_configuration_goes_through_json() {
    let b = PersistentQueueBindingEncoding {
        name: "q\"1".into(),
        address: "a".into(),
        filter_string: Some("color = 'red'".into()),
        metadata: Some(PersistentQueueBindingEncoding::metadata_for(Some("bob"))),
        auto_created: true,
        consumers: Some((3, true, ANYCAST)),
        exclusive: Some(true),
        last_value: Some(false),
        configuration_managed: Some(true),
        consumers_before_dispatch: Some(2),
        delay_before_dispatch: Some(5000),
        last_value_key: Some(Some("k".into())),
        non_destructive: Some(true),
        group_rebalance: Some(false),
        group_buckets: Some(4),
        auto_delete: Some(true),
        auto_delete_delay: Some(9),
        auto_delete_message_count: Some(8),
        group_first_key: Some(Some("g".into())),
        ring_size: Some(100),
        enabled: Some(true),
        group_rebalance_pause_dispatch: Some(true),
        internal: Some(false),
    };
    let json = queue_json(12, &b);
    assert!(
        json.starts_with(r#"{"id":12,"name":"q\"1","address":"a","routing-type":"ANYCAST""#),
        "{json}"
    );
    assert_eq!(queue_binding_of(&json, "q").unwrap(), b);
}

#[test]
#[ignore = "needs PostgreSQL"]
fn messages_refs_acks_and_counts_recover() {
    let f = Fixture::new();
    let (s, _) = f.start();
    for id in [10, 11, 12] {
        s.store_message(&message(id)).unwrap();
        s.store_reference(1, id, false).unwrap();
        wait(s.store_reference(2, id, true).unwrap());
    }
    s.store_acknowledge(1, 10).unwrap();
    s.update_delivery_count(2, 11, 3, 0).unwrap();
    assert!(!s.update_delivery_count(2, 11, 11, 3).unwrap());
    s.store_acknowledge(1, 12).unwrap();
    s.store_acknowledge(2, 12).unwrap();
    s.delete_message(12).unwrap();
    s.stop().unwrap();
    drop(s);

    let (_s, r) = f.start();
    assert_eq!(counted(&r, 1), vec![(11, 0)]);
    assert_eq!(counted(&r, 2), vec![(10, 0), (11, 3)]);
    assert_eq!(r.messages.len(), 2);
    assert!(
        matches!(&r.messages[&11], StoredMessage::Core(m) if m.address == Some(SimpleString::from("q")))
    );
}

/// Artemis's store leaves these out; a restart here keeps them.
#[test]
#[ignore = "needs PostgreSQL"]
fn scheduled_time_survives_and_goes_with_the_reference() {
    let f = Fixture::new();
    let (s, _) = f.start();
    s.store_message(&message(20)).unwrap();
    wait(s.store_reference(1, 20, true).unwrap());
    wait(
        s.update_scheduled_delivery_time(1, 20, 1234, 0)
            .unwrap()
            .unwrap(),
    );
    s.stop().unwrap();
    drop(s);

    let (s, r) = f.start();
    assert_eq!(r.queues[&1][0].scheduled_time, 1234);
    wait(s.store_acknowledge(1, 20).unwrap());
    s.delete_message(20).unwrap();
    s.stop().unwrap();
    drop(s);

    let (_s, r) = f.start();
    assert!(r.queues.is_empty() && r.messages.is_empty() && r.extra.is_empty());
}

#[test]
#[ignore = "needs PostgreSQL"]
fn a_transaction_is_all_or_nothing() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let (kept, dropped) = (s.generate_id(), s.generate_id());
    s.store_message_tx(kept, &message(30)).unwrap();
    s.store_reference_tx(kept, 1, 30).unwrap();
    s.store_message_tx(dropped, &message(31)).unwrap();
    s.store_reference_tx(dropped, 1, 31).unwrap();
    wait(s.commit(kept).unwrap());
    wait(s.rollback(dropped).unwrap());
    // Never finished: a crash loses it.
    let open = s.generate_id();
    s.store_message_tx(open, &message(32)).unwrap();
    s.store_reference_tx(open, 1, 32).unwrap();
    s.stop().unwrap();
    drop(s);

    let (_s, r) = f.start();
    assert_eq!(counted(&r, 1), vec![(30, 0)]);
    assert_eq!(r.messages.len(), 1);
}

#[test]
#[ignore = "needs PostgreSQL"]
fn prepared_transactions_recover_and_finish() {
    let f = Fixture::new();
    let (s, _) = f.start();
    s.store_message(&message(40)).unwrap();
    wait(s.store_reference(1, 40, true).unwrap());
    let xid = |b: &[u8]| Xid {
        format_id: 5,
        branch_qualifier: b.to_vec(),
        global_transaction_id: b"g".to_vec(),
    };
    let (a, b) = (s.generate_id(), s.generate_id());
    for (tx, id) in [(a, 41), (b, 42)] {
        s.store_message_tx(tx, &message(id)).unwrap();
        s.store_reference_tx(tx, 3, id).unwrap();
    }
    s.store_acknowledge_tx(a, 1, 40).unwrap();
    wait(s.prepare(a, &xid(b"a")).unwrap());
    wait(s.prepare(b, &xid(b"b")).unwrap());
    drop(s); // a crash

    let (s, r) = f.start();
    assert_eq!(r.prepared.len(), 2);
    assert_eq!(r.prepared[0].xid, xid(b"a"));
    assert_eq!(r.prepared[0].sends, vec![(3, 41)]);
    assert_eq!(r.prepared[0].acks, vec![(1, 40)]);
    assert!(r.messages.contains_key(&41) && !r.queues.contains_key(&3));
    wait(s.commit(a).unwrap());
    wait(s.rollback(b).unwrap());
    s.stop().unwrap();
    drop(s);

    let (_s, r) = f.start();
    assert!(r.prepared.is_empty());
    assert_eq!(counted(&r, 3), vec![(41, 0)]);
    assert!(!r.queues.contains_key(&1));
    assert!(!r.messages.contains_key(&42));
}

#[test]
#[ignore = "needs PostgreSQL"]
fn ids_continue_after_a_clean_stop_and_jump_after_a_crash() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let last = (0..5).map(|_| s.generate_id()).last().unwrap();
    s.stop().unwrap();
    drop(s);

    let (s, _) = f.start();
    let next = s.generate_id();
    assert!(next > last, "{next}");
    drop(s); // a crash: no stop record

    let (s, _) = f.start();
    assert!(s.generate_id() > i32::MAX as i64);
}

#[test]
#[ignore = "needs PostgreSQL"]
fn bindings_recover_with_what_their_rows_lack() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let address = PersistentAddressBindingEncoding {
        name: "a".into(),
        routing_types: vec![ANYCAST],
        auto_created: true,
        internal: Some(true),
    };
    s.add_binding(100, ids::ADDRESS_BINDING_RECORD, address.encode())
        .unwrap();
    let queue = PersistentQueueBindingEncoding {
        name: "q".into(),
        address: "a".into(),
        filter_string: None,
        metadata: Some(PersistentQueueBindingEncoding::metadata_for(None)),
        auto_created: false,
        consumers: Some((-1, false, ANYCAST)),
        exclusive: Some(false),
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
    };
    s.add_binding(101, ids::QUEUE_BINDING_RECORD, queue.encode())
        .unwrap();
    let status = s.store_queue_status(101).unwrap();
    s.stop().unwrap();
    drop(s);

    let s = f.open();
    let records = s.load_bindings().unwrap();
    let of = |t: u8| records.iter().find(|r| r.user_record_type == t).unwrap();
    assert_eq!(
        PersistentAddressBindingEncoding::decode(&of(ids::ADDRESS_BINDING_RECORD).data).unwrap(),
        address
    );
    assert_eq!(
        PersistentQueueBindingEncoding::decode(&of(ids::QUEUE_BINDING_RECORD).data).unwrap(),
        queue
    );
    assert_eq!(of(ids::QUEUE_STATUS_RECORD).id, status);
    s.delete_binding(101).unwrap();
    s.delete_binding(100).unwrap();
    s.stop().unwrap();
    drop(s);

    let s = f.open();
    let left: Vec<u8> = s
        .load_bindings()
        .unwrap()
        .iter()
        .map(|r| r.user_record_type)
        .collect();
    assert_eq!(left, vec![ids::QUEUE_STATUS_RECORD]);
}

/// Many messages through six connections, acknowledged at once: an
/// acknowledgement that overtook its insert would leave a reference behind.
#[test]
#[ignore = "needs PostgreSQL"]
fn acknowledgements_never_overtake_their_inserts() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let total = 20_000;
    let mut waits = Vec::with_capacity(total);
    for id in 1_000..1_000 + total as i64 {
        s.store_message(&message(id)).unwrap();
        s.store_reference(1, id, true).unwrap();
        if id % 2 == 0 {
            waits.push(s.store_acknowledge(1, id).unwrap());
            s.delete_message(id).unwrap();
        }
    }
    waits.into_iter().for_each(wait);
    s.stop().unwrap();
    drop(s);

    let (_s, r) = f.start();
    assert_eq!(r.queues[&1].len(), total / 2);
    assert!(r.queues[&1].iter().all(|x| x.message_id % 2 == 1));
    assert_eq!(r.messages.len(), total / 2);
}

/// What a failed write does: the caller hears, and later writes are refused.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_failed_write_is_reported() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = seen.clone();
    s.set_critical_listener(Arc::new(move |_, _| {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
    }));
    let connection = s.connection.clone();
    s.run(async move {
        let client = writer::connect(&connection).await.map_err(io_error)?;
        client
            .batch_execute("ALTER TABLE DB_REFERENCES RENAME TO DB_REFERENCES_AWAY")
            .await
            .map_err(io_error)
    })
    .unwrap();
    let failed = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(s.store_reference(1, 50, true).unwrap().wait());
    let connection = s.connection.clone();
    s.run(async move {
        let client = writer::connect(&connection).await.map_err(io_error)?;
        client
            .batch_execute("ALTER TABLE DB_REFERENCES_AWAY RENAME TO DB_REFERENCES")
            .await
            .map_err(io_error)
    })
    .unwrap();
    assert!(failed.is_err());
    assert!(seen.load(std::sync::atomic::Ordering::SeqCst));
    assert!(s.store_reference(1, 51, true).is_err());
}

fn ids_of(rows: &[(StoredMessage, bool)]) -> Vec<i64> {
    rows.iter().map(|(m, _)| m.message_id()).collect()
}

/// Paged messages wait in the table, in order, for each queue on its own,
/// and a restart leaves them there.
#[test]
#[ignore = "needs PostgreSQL"]
fn paged_messages_are_read_in_order_and_stay_out_of_memory() {
    let f = Fixture::new();
    let (s, _) = f.start();
    for id in 60..70 {
        wait(
            s.store_paged(None, &message(id), &[1, 2], Box::new(|| ()))
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(ids_of(&s.read_paged(1, 3).unwrap()), vec![60, 61, 62]);
    s.stop().unwrap();
    drop(s);

    let (s, r) = f.start();
    // Read ones are ordinary references now.
    assert_eq!(counted(&r, 1), vec![(60, 0), (61, 0), (62, 0)]);
    assert_eq!(r.paged[&1].0, 7);
    assert_eq!(r.paged[&2].0, 10);
    assert_eq!(r.messages.len(), 3);
    assert_eq!(
        ids_of(&s.read_paged(1, 100).unwrap()),
        (63..70).collect::<Vec<_>>()
    );
    assert!(s.read_paged(1, 100).unwrap().is_empty());
    assert_eq!(ids_of(&s.read_paged(2, 2).unwrap()), vec![60, 61]);
    // The message stays while another queue has it.
    wait(s.store_acknowledge(1, 60).unwrap());
    wait(s.store_acknowledge(2, 60).unwrap());
    s.stop().unwrap();
    drop(s);
    let (_s, r) = f.start();
    assert!(!r.messages.contains_key(&60));
    assert_eq!(r.paged[&2].0, 8);
}

/// A record a prepared transaction deletes goes at its commit, even after a
/// crash in between, and stays at its rollback.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_prepared_delete_survives_a_crash() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let rows = |s: &DatabaseStorage| {
        let connection = s.connection.clone();
        s.run(async move {
            let client = writer::connect(&connection).await.map_err(io_error)?;
            let rows = client
                .query(
                    "SELECT ID FROM DB_BROKER_DATA WHERE ID IN (5001, 5002) OR RECORD_TYPE = 201 ORDER BY ID",
                    &[],
                )
                .await
                .map_err(io_error)?;
            Ok(rows.iter().map(|r| r.get::<_, i64>(0)).collect::<Vec<_>>())
        })
        .unwrap()
    };
    let xid = |b: &[u8]| Xid {
        format_id: 5,
        branch_qualifier: b.to_vec(),
        global_transaction_id: b"g".to_vec(),
    };
    s.add_record(5001, 199, vec![1], false).unwrap();
    s.add_record(5002, 199, vec![2], false).unwrap();
    let (a, b) = (s.generate_id(), s.generate_id());
    s.delete_record_tx(a, 5001).unwrap();
    s.delete_record_tx(b, 5002).unwrap();
    wait(s.prepare(a, &xid(b"a")).unwrap());
    wait(s.prepare(b, &xid(b"b")).unwrap());
    assert_eq!(rows(&s).len(), 4, "the records and what will delete them");
    drop(s); // a crash

    let (s, r) = f.start();
    assert_eq!(r.prepared.len(), 2);
    wait(s.commit(a).unwrap());
    wait(s.rollback(b).unwrap());
    assert_eq!(rows(&s), [5002]);

    // One left by Artemis, which finished its transaction without it.
    let connection = s.connection.clone();
    s.run(async move {
        let client = writer::connect(&connection).await.map_err(io_error)?;
        client
            .execute(
                "INSERT INTO DB_BROKER_DATA (ID, RECORD_TYPE, TX_ID) VALUES (5003, 201, 999999)",
                &[],
            )
            .await
            .map_err(io_error)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(rows(&s), [5002, 5003]);
    drop(s);
    let (s, _) = f.start();
    assert_eq!(rows(&s), [5002]);
}

/// What a management operation looks through stays paged, for the queue to
/// read as if nothing had looked.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_scan_leaves_paged_messages_paged() {
    let f = Fixture::new();
    let (s, _) = f.start();
    for id in 1..=5 {
        wait(
            s.store_paged(None, &message(id), &[1], Box::new(|| ()))
                .unwrap()
                .unwrap(),
        );
    }
    wait(
        s.store_paged(None, &message(6), &[2], Box::new(|| ()))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(ids_of(&s.scan_paged(1, 2, i64::MIN).unwrap()), [1, 2]);
    assert_eq!(ids_of(&s.scan_paged(1, 2, 2).unwrap()), [3, 4]);
    assert_eq!(ids_of(&s.scan_paged(1, 10, 4).unwrap()), [5]);
    assert_eq!(ids_of(&s.read_paged(1, 10).unwrap()), [1, 2, 3, 4, 5]);
    assert!(s.scan_paged(1, 10, i64::MIN).unwrap().is_empty());
}

/// A row a scan acted on is gone once the store has settled.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_settled_acknowledgement_keeps_a_read_from_its_row() {
    let f = Fixture::new();
    let (s, _) = f.start();
    for id in 1..=2000 {
        wait(
            s.store_paged(None, &message(id), &[1], Box::new(|| ()))
                .unwrap()
                .unwrap(),
        );
    }
    for id in 1..=1999 {
        drop(s.store_acknowledge(1, id).unwrap());
        s.delete_message(id).unwrap();
    }
    s.settle().unwrap();
    assert_eq!(ids_of(&s.read_paged(1, 10).unwrap()), [2000]);
}

#[test]
#[ignore = "needs PostgreSQL"]
fn paged_messages_of_a_transaction_appear_at_its_commit() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let tx = s.generate_id();
    assert!(
        s.store_paged(Some(tx), &message(80), &[1], Box::new(|| ()))
            .unwrap()
            .is_none()
    );
    wait(s.written().unwrap());
    assert!(s.read_paged(1, 10).unwrap().is_empty());
    wait(s.commit(tx).unwrap());
    assert_eq!(ids_of(&s.read_paged(1, 10).unwrap()), vec![80]);
    let tx = s.generate_id();
    s.store_paged(Some(tx), &message(81), &[1], Box::new(|| ()))
        .unwrap();
    wait(s.rollback(tx).unwrap());
    assert!(s.read_paged(1, 10).unwrap().is_empty());
}

/// Deleting a queue takes the references it never read.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_deleted_queue_leaves_no_paged_references() {
    let f = Fixture::new();
    let (s, _) = f.start();
    wait(
        s.store_paged(None, &message(90), &[5], Box::new(|| ()))
            .unwrap()
            .unwrap(),
    );
    s.delete_binding(5).unwrap();
    assert!(s.read_paged(5, 10).unwrap().is_empty());
    s.stop().unwrap();
    drop(s);
    let (_s, r) = f.start();
    assert!(r.messages.is_empty() && r.paged.is_empty());
}

/// One broker to a database; a dead one does not hold it.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_second_broker_is_refused_until_the_first_is_gone() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let refused = DatabaseStorage::open(config(f.dir.path()), &database())
        .err()
        .expect("refused");
    assert!(refused.to_string().contains("another"), "{refused}");
    drop(s); // a crash
    f.start();
}

/// The plans that keep a read and a batch to their own rows, with statistics
/// from before a backlog built, as on a new broker, where every ID lies past
/// the histogram and the planner expects one row, and then with statistics of
/// the backlog. Each state has its own wrong plan, which fetches or walks the
/// whole backlog every time.
#[test]
#[ignore = "needs PostgreSQL"]
fn no_statement_walks_the_backlog_whatever_the_statistics() {
    let f = Fixture::new();
    let (s, _) = f.start();
    let connection = s.connection.clone();
    let plans = s
        .run(async move {
            let client = writer::connect(&connection).await.map_err(io_error)?;
            let fill = |from: i64, to: i64| {
                format!(
                    "INSERT INTO DB_MESSAGES (MESSAGE_ID, MESSAGE_RECORD, MEMORY_ESTIMATE, IS_LARGE, IS_CORE)                      SELECT g, decode('00', 'hex'), 1, 'N', 'Y' FROM generate_series({from}, {to}) g;                      INSERT INTO DB_REFERENCES (MESSAGE_ID, QUEUE_ID, PAGED)                      SELECT g, 1, 'Y' FROM generate_series({from}, {to}) g"
                )
            };
            let analyze = "ANALYZE DB_MESSAGES; ANALYZE DB_REFERENCES";
            let explain = async |sql: &str, types: &str, args: &str| {
                client
                    .batch_execute(&format!("PREPARE p({types}) AS {sql}"))
                    .await
                    .map_err(io_error)?;
                let rows = client
                    .simple_query(&format!("EXPLAIN EXECUTE p({args})"))
                    .await
                    .map_err(io_error)?;
                client.batch_execute("DEALLOCATE p").await.map_err(io_error)?;
                Ok::<String, JournalError>(
                    rows.iter()
                        .filter_map(|m| match m {
                            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("
"),
                )
            };
            client
                .batch_execute(READER_SESSION)
                .await
                .map_err(io_error)?;
            client.batch_execute(&fill(1, 100)).await.map_err(io_error)?;
            client.batch_execute(analyze).await.map_err(io_error)?;
            client
                .batch_execute(&fill(101, 50_000))
                .await
                .map_err(io_error)?;
            let mut plans = Vec::new();
            for fresh in [false, true] {
                if fresh {
                    client.batch_execute(analyze).await.map_err(io_error)?;
                }
                let ids = "'{20000,20001}'";
                plans.push((
                    explain(writer::DELETE_MESSAGES, "bigint[]", ids).await?,
                    explain(
                        writer::UNREFERENCE,
                        "bigint[], bigint[], bigint, bigint",
                        &format!("{ids}, '{{1,1}}', 20000, 20001"),
                    )
                    .await?,
                    explain(READ_PAGED, "bigint, bigint, bigint", "1, 200, 150").await?,
                ));
            }
            Ok(plans)
        })
        .unwrap();
    for (delete, unreference, read) in plans {
        assert!(delete.contains("message_id = ANY"), "{delete}");
        // Bounded by the batch's IDs, and not by an array searched per row.
        assert!(unreference.contains("message_id >="), "{unreference}");
        assert!(!unreference.contains("ANY"), "{unreference}");
        // Either index will do: in order, it stops at the limit. A sort or a
        // bitmap under the limit has fetched the whole backlog first.
        assert!(!read.contains("Sort"), "{read}");
        assert!(!read.contains("Bitmap"), "{read}");
        // Each message is found by its key, not by walking the index.
        assert!(!read.contains("Join Filter"), "{read}");
        assert!(
            read.contains("Index Cond: (message_id = f.message_id)"),
            "{read}"
        );
    }
}

/// The modes against a server with TLS, its certificate for `localhost` only:
/// `SARANGA_TEST_POSTGRES_TLS` names the directory with `ca.crt`, `other.crt`,
/// `client.crt` and `client.key`.
#[test]
#[ignore = "needs PostgreSQL with TLS"]
fn tls_is_what_the_url_asks_for() {
    let Ok(dir) = std::env::var("SARANGA_TEST_POSTGRES_TLS") else {
        panic!("SARANGA_TEST_POSTGRES_TLS is not set");
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Whether it connected, and over TLS, with the client's name if it gave one.
    let attempt = |host: &str, parameters: String| {
        let target = client_config(&DatabaseConfig {
            url: format!(
                "jdbc:postgresql://{host}:5432/saranga?user=saranga&password=saranga&{parameters}"
            ),
            ..database()
        })
        .unwrap();
        runtime.block_on(async {
            let client = writer::connect(&target).await?;
            let row = client
                .query_one(
                    "SELECT ssl, client_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                    &[],
                )
                .await?;
            Ok::<(bool, Option<String>), tokio_postgres::Error>((row.get(0), row.get(1)))
        })
    };
    let ca = format!("sslrootcert={dir}/ca.crt");
    let other = format!("sslrootcert={dir}/other.crt");

    assert!(!attempt("localhost", "sslmode=disable".into()).unwrap().0);
    assert!(attempt("localhost", "sslmode=prefer".into()).unwrap().0);
    assert!(
        attempt("localhost", String::new()).unwrap().0,
        "the default"
    );
    // Encrypted, whoever it is.
    assert!(
        attempt("127.0.0.1", format!("sslmode=require&{other}"))
            .unwrap()
            .0
    );
    // The chain and not the name.
    assert!(
        attempt("127.0.0.1", format!("sslmode=verify-ca&{ca}"))
            .unwrap()
            .0
    );
    assert!(attempt("localhost", format!("sslmode=verify-ca&{other}")).is_err());
    // Both.
    assert!(
        attempt("localhost", format!("sslmode=verify-full&{ca}"))
            .unwrap()
            .0
    );
    assert!(attempt("localhost", format!("ssl=true&{ca}")).unwrap().0);
    assert!(attempt("127.0.0.1", format!("sslmode=verify-full&{ca}")).is_err());
    assert!(attempt("localhost", format!("sslmode=verify-full&{other}")).is_err());
    // A client certificate reaches the server.
    let (ssl, name) = attempt(
        "localhost",
        format!("sslmode=verify-full&{ca}&sslcert={dir}/client.crt&sslkey={dir}/client.key"),
    )
    .unwrap();
    assert!(ssl);
    assert!(name.is_some_and(|n| n.contains("broker-client")));
}

/// `database-max-read-connections`, whose default is half of the writers.
#[test]
fn read_connections_follow_the_setting() {
    let of = |connections, read_connections| {
        read_connections_of(&DatabaseConfig {
            connections,
            read_connections,
            ..database()
        })
    };
    assert_eq!(of(6, -1), 3);
    assert_eq!(of(1, -1), 1);
    assert_eq!(of(6, 0), 1);
    assert_eq!(of(6, 5), 5);
}

/// `database-read-idle-timeout`: a read connection left unused is closed, on
/// the server too, and the next read opens one again.
#[test]
#[ignore = "needs PostgreSQL"]
fn an_idle_read_connection_closes() {
    let f = Fixture::new();
    let database = DatabaseConfig {
        read_idle_millis: 300,
        ..database()
    };
    let s = DatabaseStorage::open(config(f.dir.path()), &database).unwrap();
    s.load_bindings().unwrap();
    s.load_messages().unwrap();
    for id in [101, 102] {
        wait(
            s.store_paged(None, &message(id), &[1], Box::new(|| ()))
                .unwrap()
                .unwrap(),
        );
    }
    // The sessions whose last statement read paged messages.
    let readers = |s: &DatabaseStorage| {
        let connection = s.connection.clone();
        s.run(async move {
            let client = writer::connect(&connection).await.map_err(io_error)?;
            let row = client
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE pid <> pg_backend_pid() AND query LIKE 'WITH picked AS%'",
                    &[],
                )
                .await
                .map_err(io_error)?;
            Ok(row.get::<_, i64>(0))
        })
        .unwrap()
    };
    assert_eq!(ids_of(&s.read_paged(1, 1).unwrap()), vec![101]);
    assert_eq!(readers(&s), 1);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(readers(&s), 0, "still open");
    assert_eq!(ids_of(&s.read_paged(1, 1).unwrap()), vec![102]);
    assert_eq!(readers(&s), 1);
}
