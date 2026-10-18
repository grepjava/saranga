//! An address's size counts its messages from when they are routed, as
//! Artemis's `processRoute` counts them (`refUp`): before they are stored,
//! and in a transaction before it commits. So an address pages the message
//! after the one that filled it, and a transaction sending more than the
//! address takes pages the rest; its rollback gives the size back.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_client::message;
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};

const WAIT: Duration = Duration::from_secs(10);

fn config(dir: &Path) -> BrokerConfig {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <address-settings>
               <address-setting match="paged">
                 <max-size-bytes>10240</max-size-bytes>
                 <page-size-bytes>20480</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
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

fn paging(broker: &Broker) -> bool {
    broker
        .post_office()
        .paging()
        .existing_store("paged")
        .is_some_and(|s| s.is_paging())
}

fn address_size(broker: &Broker) -> i64 {
    broker
        .post_office()
        .paging()
        .existing_store("paged")
        .map_or(0, |s| s.address_size())
}

fn numbered(n: i32) -> CoreMessage {
    let mut m = message::bytes_message(&[b'x'; 1024]);
    m.put_property("n", PropertyValue::Int(n));
    m
}

#[tokio::test]
async fn a_transaction_pages_before_it_commits() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start(dir.path()).await;
    let conn = Connection::connect(&address, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions {
            auto_commit_sends: false,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    session
        .create_queue(
            QueueConfiguration::new("paged")
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await
        .unwrap();
    let producer = session.create_producer(Some("paged")).await.unwrap();

    // Thirty 1 KiB messages are three times the address's 10 KiB.
    for n in 0..30 {
        producer.send(numbered(n)).await.unwrap();
    }
    // A transacted session does not block on its sends; a query on it
    // returns once the broker has handled them, in order.
    session.queue_query("paged").await.unwrap();
    assert!(paging(&broker), "the transaction filled the address");

    // Rolled back, the messages in memory give their size back.
    session.rollback(false).await.unwrap();
    session.queue_query("paged").await.unwrap();
    assert_eq!(address_size(&broker), 0, "the rollback gives the size back");

    for n in 0..30 {
        producer.send(numbered(n)).await.unwrap();
    }
    session.commit().await.unwrap();
    let consumer_session = conn
        .create_session(SessionOptions {
            ack_batch_size: 0,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    let consumer = consumer_session
        .create_consumer("paged", None, false)
        .await
        .unwrap();
    consumer_session.start().await.unwrap();
    for n in 0..30 {
        let d = consumer
            .receive(WAIT)
            .await
            .unwrap()
            .expect("every committed message arrives");
        assert_eq!(d.message.property("n"), Some(&PropertyValue::Int(n)));
        consumer.acknowledge(&d).await.unwrap();
    }
    assert!(
        consumer
            .receive(Duration::from_millis(500))
            .await
            .unwrap()
            .is_none(),
        "the rolled-back messages never arrive"
    );
    drop(conn);
    broker.stop().unwrap();
}
