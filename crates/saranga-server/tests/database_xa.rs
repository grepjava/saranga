//! XA on the database store, with a real PostgreSQL: run with `--ignored`,
//! with `SARANGA_TEST_POSTGRES` naming a JDBC URL if not the local default.
//! They empty the tables, so they take turns with the store's other tests.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_config as cfg;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

fn url() -> String {
    std::env::var("SARANGA_TEST_POSTGRES").unwrap_or_else(|_| {
        "jdbc:postgresql://127.0.0.1:5432/saranga?user=saranga&password=saranga".to_owned()
    })
}

/// Empties the store's tables (there are none before a first broker).
async fn empty() {
    let (client, connection) =
        tokio_postgres::connect(url().trim_start_matches("jdbc:"), tokio_postgres::NoTls)
            .await
            .expect("PostgreSQL is reachable");
    tokio::spawn(connection);
    let _ = client
        .batch_execute(
            "TRUNCATE DB_MESSAGES, DB_REFERENCES, DB_ADDRESS, DB_QUEUE, DB_BROKER_DATA, DB_CONFIG_DATA, DB_PREP_TX",
        )
        .await;
}

/// A broker on the store whose queue `q` pages everything.
async fn start(dir: &Path) -> Arc<Broker> {
    let url = url().replace('&', "&amp;");
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <store><database-store>
               <message-table-name>MESSAGES</message-table-name>
               <bindings-table-name>BINDINGS</bindings-table-name>
               <large-message-table-name>LARGE_MESSAGES</large-message-table-name>
               <page-store-table-name>PAGE_STORE</page-store-table-name>
               <database-storage>true</database-storage>
               <jdbc-connection-url>{url}</jdbc-connection-url>
             </database-store></store>
             <addresses><address name="q"><anycast><queue name="q"/></anycast></address></addresses>
             <address-settings><address-setting match="q">
               <max-size-bytes>1</max-size-bytes>
               <address-full-policy>PAGE</address-full-policy>
             </address-setting></address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>, xa: bool) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("db-xa"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "db-xa".into(),
            username: None,
            password: None,
            xa,
            auto_commit_sends: !xa,
            auto_commit_acks: !xa,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn xid() -> Xid {
    Xid {
        format_id: 7,
        branch_qualifier: b"db".to_vec(),
        global_transaction_id: b"db".to_vec(),
    }
}

struct Record(Arc<Mutex<Vec<i64>>>);

impl DeliverySink for Record {
    fn deliver(&self, _: i64, m: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(m.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        1
    }
    fn chunk_size(&self) -> usize {
        1024
    }
}

async fn stop(broker: Arc<Broker>) {
    broker.stop().unwrap();
    drop(broker);
    // The store lets go of its lock.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// A non-durable message paged in an XA branch that was prepared, then
/// committed after a restart and acknowledged, is gone from the store: its
/// recovered reference is the store's, so its acknowledgement deletes it.
/// (Acknowledged with the message before it, which took the address over
/// its limit.)
#[tokio::test]
#[ignore = "needs PostgreSQL"]
async fn a_recovered_prepared_send_is_deleted_when_acknowledged() {
    empty().await;
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    // A first message takes the address over its limit: the next pages.
    let mut p = session(&broker, false);
    p.send(
        saranga_message::CoreMessage {
            address: Some("q".into()),
            durable: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    p.take_completion().wait().await.unwrap();
    drop(p);
    let mut s = session(&broker, true);
    s.xa_start(xid()).unwrap();
    s.send(
        saranga_message::CoreMessage {
            address: Some("q".into()),
            durable: false,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.xa_end(xid()).unwrap();
    s.xa_prepare(xid()).unwrap();
    s.take_completion().wait().await.unwrap();
    drop(s);
    stop(broker).await;

    // Committed after a restart, then consumed.
    let broker = start(dir.path()).await;
    let mut s = session(&broker, true);
    s.xa_commit(xid(), false).unwrap();
    s.take_completion().wait().await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut c = session(&broker, false);
    let record = seen.clone();
    c.create_consumer(1, "q", None, 0, false, move |_| {
        Arc::new(Record(record.clone()))
    })
    .await
    .unwrap();
    c.consumer_credits(1, -1);
    c.set_started(true).await;
    // The first message, then the committed one.
    let id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(id) = seen.lock().unwrap().get(1) {
                return *id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the committed message");
    c.acknowledge(1, id, false).await.unwrap();
    c.take_completion().wait().await.unwrap();
    drop((s, c));
    stop(broker).await;

    // Acknowledged, it does not come back.
    let broker = start(dir.path()).await;
    let q = broker.post_office().queue("q").unwrap();
    assert_eq!(q.message_count(), 0, "the acknowledged message came back");
    stop(broker).await;
}
