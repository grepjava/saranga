//! A prepared transaction that cannot be loaded whole is rolled back, as
//! Artemis's `failedToPrepareException` rolls it back: it is neither
//! committed in part nor in the way of the broker starting.

use std::sync::Arc;

use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    Broker::start(BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap())
        .await
        .unwrap()
}

/// A prepared send whose message record is gone: the branch is rolled
/// back at the start, not recovered with its other send alone.
#[tokio::test]
async fn a_prepared_send_without_its_message_rolls_back_the_branch() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path()).await;
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    let q = b.post_office().create_queue(spec, false).unwrap();
    let storage = b.storage().unwrap().clone();
    let po = b.post_office();
    let tx = po.ids().next();
    // One send with its message, one whose message was never stored.
    let message = saranga_server::message::ServerMessage::new(saranga_message::CoreMessage {
        message_id: po.ids().next(),
        durable: true,
        address: Some("q".into()),
        ..Default::default()
    });
    storage.store_message_tx(tx, &message).unwrap();
    storage
        .store_reference_tx(tx, q.id(), message.message_id())
        .unwrap();
    storage
        .store_reference_tx(tx, q.id(), po.ids().next())
        .unwrap();
    let xid = Xid {
        format_id: 1,
        branch_qualifier: b"b".to_vec(),
        global_transaction_id: b"g".to_vec(),
    };
    storage.prepare(tx, &xid).unwrap().wait().await.unwrap();
    b.stop().unwrap();
    drop(q);
    drop(b);

    let b = start(dir.path()).await;
    assert!(
        b.resource_manager().prepared().is_empty(),
        "not recovered in part"
    );
    assert_eq!(b.post_office().queue("q").unwrap().message_count(), 0);
    b.stop().unwrap();
    drop(b);
    // Rolled back for good: the journal holds no prepared transaction.
    let mut config =
        saranga_journal::JournalConfig::messages(dir.path().join("data").join("journal"));
    config.file_size = 102400;
    let journal = saranga_journal::Journal::open(config).unwrap();
    assert!(journal.load_all().unwrap().prepared.is_empty());
    journal.stop().unwrap();
}

/// A prepared transaction whose Xid cannot be read is rolled back, and the
/// broker starts.
#[tokio::test]
async fn a_prepared_transaction_with_a_damaged_xid_does_not_stop_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path()).await;
    b.stop().unwrap();
    drop(b);
    let journal_config = || {
        let mut c =
            saranga_journal::JournalConfig::messages(dir.path().join("data").join("journal"));
        c.file_size = 102400;
        c
    };
    {
        let journal = saranga_journal::Journal::open(journal_config()).unwrap();
        let loaded = journal.load_all().unwrap();
        let tx = loaded.max_id + 100;
        // A duplicate ID record, then a prepare whose Xid is two bytes.
        journal
            .append_add_tx(tx, tx + 1, saranga_storage::ids::DUPLICATE_ID, vec![0; 8])
            .unwrap();
        journal.append_prepare(tx, vec![1, 2], true, None).unwrap();
        journal.stop().unwrap();
    }
    let b = start(dir.path()).await;
    assert!(b.resource_manager().prepared().is_empty());
    b.stop().unwrap();
    drop(b);
    let journal = saranga_journal::Journal::open(journal_config()).unwrap();
    assert!(
        journal.load_all().unwrap().prepared.is_empty(),
        "rolled back"
    );
    journal.stop().unwrap();
}
