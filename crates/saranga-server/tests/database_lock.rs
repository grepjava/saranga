//! The database store stops writing once it loses its database lock, so
//! that the broker that takes the database next is the only one writing.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::SimpleString;
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

fn url() -> String {
    std::env::var("SARANGA_TEST_POSTGRES").unwrap_or_else(|_| {
        "jdbc:postgresql://127.0.0.1:5432/saranga?user=saranga&password=saranga".to_owned()
    })
}

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

async fn start(dir: &Path, database: bool) -> Arc<Broker> {
    let url = url().replace('&', "&amp;");
    let store = if database {
        format!(
            r#"<store><database-store>
               <message-table-name>MESSAGES</message-table-name>
               <bindings-table-name>BINDINGS</bindings-table-name>
               <large-message-table-name>LARGE_MESSAGES</large-message-table-name>
               <page-store-table-name>PAGE_STORE</page-store-table-name>
               <database-storage>true</database-storage>
               <jdbc-connection-url>{url}</jdbc-connection-url>
             </database-store></store>"#
        )
    } else {
        String::new()
    };
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             {store}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

/// Once the session holding the database's lock ends, the store takes no
/// more writes, and another broker can have the database.
#[tokio::test]
#[ignore = "needs PostgreSQL"]
async fn losing_the_lock_stops_the_writes() {
    empty().await;
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    let mut s = session(&broker);
    let m = || CoreMessage {
        address: Some(SimpleString::from("t")),
        durable: true,
        ..Default::default()
    };
    broker
        .management()
        .invoke(
            &broker,
            "broker",
            "createQueue",
            &[
                saranga_server::management::Json::from("t"),
                saranga_server::management::Json::from("ANYCAST"),
                saranga_server::management::Json::from("t"),
                saranga_server::management::Json::Null,
                saranga_server::management::Json::Bool(true),
                saranga_server::management::Json::Long(-1),
                saranga_server::management::Json::Bool(false),
                saranga_server::management::Json::Bool(true),
            ],
            Default::default(),
        )
        .await
        .unwrap();
    s.send(m(), None).await.unwrap();
    s.take_completion().wait().await.unwrap();
    // The lock's session ends (a network failure, an administrator).
    let (client, connection) =
        tokio_postgres::connect(url().trim_start_matches("jdbc:"), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(connection);
    let killed: i64 = client
        .query_one(
            "SELECT COUNT(pg_terminate_backend(pid)) FROM pg_locks WHERE locktype = 'advisory' AND granted",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(killed, 1);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    // A second broker takes the database.
    let other = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <store><database-store>
               <message-table-name>MESSAGES</message-table-name>
               <bindings-table-name>BINDINGS</bindings-table-name>
               <large-message-table-name>LARGE_MESSAGES</large-message-table-name>
               <page-store-table-name>PAGE_STORE</page-store-table-name>
               <database-storage>true</database-storage>
               <jdbc-connection-url>{}</jdbc-connection-url>
             </database-store></store>
           </core></configuration>"#,
        url().replace('&', "&amp;")
    ))
    .unwrap();
    let config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), other.path()).unwrap();
    assert!(
        Broker::start(config).await.is_ok(),
        "the second broker takes the database"
    );
    // The first still writes.
    let sent = s.send(m(), None).await;
    let written = match sent {
        Ok(()) => s.take_completion().wait().await.is_ok(),
        Err(_) => false,
    };
    assert!(
        !written,
        "the first broker went on writing without the lock"
    );
}
