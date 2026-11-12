//! A reject-duplicates move journals the destination's duplicate ID, so with
//! `persist-id-cache` the ID still blocks a resend after a restart, the way
//! the normal send path's `addDuplicateID` does.

use std::path::Path;
use std::sync::Arc;

use saranga_client::message;
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::queue::{Selection, SelectionAction};
use saranga_server::{Acceptor, Broker, BrokerConfig};

fn config(dir: &Path) -> BrokerConfig {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
           </core></configuration>"#,
    )
    .unwrap();
    BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap()
}

async fn start(dir: &Path) -> (Arc<Broker>, String) {
    let broker = Broker::start(config(dir)).await.unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let address = format!("127.0.0.1:{port}");
    broker
        .add_acceptor("artemis", &address, Acceptor::default())
        .await
        .unwrap();
    (broker, address)
}

async fn make_queue(session: &Session, name: &str) {
    session
        .create_queue(
            QueueConfiguration::new(name)
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn a_rejected_duplicate_move_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start(dir.path()).await;
    let conn = Connection::connect(&address, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    make_queue(&session, "src").await;
    make_queue(&session, "dst").await;

    // One message on `src`, carrying a duplicate-detection ID.
    let mut m = message::bytes_message(b"body");
    m.put_property(
        message::HDR_DUPLICATE_DETECTION_ID,
        PropertyValue::String("dup-1".into()),
    );
    let producer = session.create_producer(Some("src")).await.unwrap();
    producer.send(m).await.unwrap();
    session.queue_query("src").await.unwrap();

    // Move it to `dst`, rejecting duplicates: `dst` takes the ID into its
    // duplicate-ID cache.
    let src = broker.post_office().queue("src").unwrap();
    let dst = broker.post_office().queue("dst").unwrap();
    let moved = src
        .act_or_fail(
            Selection::filter(None),
            SelectionAction::Move {
                address: "dst".to_owned(),
                target: Some(dst),
                reject_duplicates: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(moved, 1, "the message is moved");
    // The ID is taken into the cache when the move's journal write lands.
    wait_for_cached(&broker, "dst").await;

    drop(conn);
    broker.stop().unwrap();

    // After a restart the ID must still be there: the move journaled it, so a
    // resend of the same ID is still a duplicate.
    let broker = Broker::start(config(dir.path())).await.unwrap();
    assert_eq!(
        cache_len(&broker, "dst"),
        1,
        "a reject-duplicates move must journal the duplicate ID so it survives a restart"
    );
    broker.stop().unwrap();
}

fn cache_len(broker: &Broker, address: &str) -> usize {
    let cache = broker.post_office().duplicate_cache(address);
    saranga_server::duplicate::lock(&cache).len()
}

/// Waits for the move's asynchronous after-commit to take the ID.
async fn wait_for_cached(broker: &Broker, address: &str) {
    for _ in 0..100 {
        if cache_len(broker, address) == 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the ID is in the cache after the move");
}
