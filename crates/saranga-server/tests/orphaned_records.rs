//! What a start leaves behind in the message journal, as Artemis's does not:
//! a message whose queue is gone, duplicate IDs once `persist-id-cache` is
//! off, and a page transaction no paged message names.

use std::path::Path;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::storage::{RecoveredMessages, Storage};
use saranga_server::{Broker, BrokerConfig};

fn file(persist_ids: bool) -> cfg::BrokerConfig {
    cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <persist-id-cache>{persist_ids}</persist-id-cache>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#
    ))
    .unwrap()
}

async fn start(dir: &Path, persist_ids: bool) -> Arc<Broker> {
    let config =
        BrokerConfig::from_file_config(&file(persist_ids), Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn create_queue(b: &Arc<Broker>) -> i64 {
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
    b.post_office().queue("q").unwrap().id()
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

async fn send(broker: &Arc<Broker>, duplicate_id: Option<&str>) {
    let mut m = CoreMessage {
        address: Some("q".into()),
        durable: true,
        ..Default::default()
    };
    if let Some(id) = duplicate_id {
        m.properties.insert(
            SimpleString::from("_AMQ_DUPL_ID"),
            PropertyValue::String(SimpleString::from(id)),
        );
    }
    let mut s = session(broker);
    s.send(m, None).await.unwrap();
    s.take_completion().wait().await.unwrap();
    s.close(false).await;
}

/// The message journal as the next start would find it.
fn journal(dir: &Path) -> RecoveredMessages {
    let storage = Storage::open(saranga_server::config::storage_config(&file(true), dir)).unwrap();
    storage.load_bindings().unwrap();
    let loaded = storage.load_messages().unwrap();
    storage.stop().unwrap();
    loaded
}

#[tokio::test]
async fn a_message_whose_queue_is_gone_is_deleted_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), true).await;
    let queue = create_queue(&b);
    send(&b, None).await;
    // The queue's binding goes, its reference stays.
    b.storage().unwrap().delete_binding(queue).unwrap();
    b.stop().unwrap();
    assert_eq!(journal(dir.path()).messages.len(), 1);

    let b = start(dir.path(), true).await;
    assert!(b.post_office().queue("q").is_none());
    b.stop().unwrap();
    assert!(journal(dir.path()).messages.is_empty(), "message kept");
}

#[tokio::test]
async fn duplicate_ids_go_once_the_cache_is_not_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), true).await;
    create_queue(&b);
    send(&b, Some("d1")).await;
    b.stop().unwrap();
    assert_eq!(journal(dir.path()).duplicates.len(), 1);

    let b = start(dir.path(), false).await;
    b.stop().unwrap();
    assert!(
        journal(dir.path()).duplicates.is_empty(),
        "duplicate IDs kept"
    );
}

#[tokio::test]
async fn a_page_transaction_no_paged_message_names_is_deleted_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), true).await;
    create_queue(&b);
    let storage = b.storage().unwrap().clone();
    let tx = storage.generate_id();
    storage.store_page_transaction_tx(tx, tx, 1).unwrap();
    storage.commit(tx).unwrap().wait().await.unwrap();
    b.stop().unwrap();
    assert_eq!(journal(dir.path()).page_txs.len(), 1);

    let b = start(dir.path(), true).await;
    b.stop().unwrap();
    assert!(
        journal(dir.path()).page_txs.is_empty(),
        "page transaction kept"
    );
}
