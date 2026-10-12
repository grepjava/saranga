//! A full disk through an in-process Saranga broker: producers to an address
//! block, but what is sent to the management address still goes through, as
//! the management address has no paging store.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message;
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;

async fn start_broker(dir: &std::path::Path) -> (Arc<Broker>, String) {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <min-disk-free>1000000000000000000</min-disk-free>
             <disk-scan-period>50</disk-scan-period>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    (broker, address.to_string())
}

#[tokio::test]
async fn management_is_not_blocked_by_a_full_disk() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start_broker(dir.path()).await;
    for _ in 0..100 {
        if broker.post_office().paging().is_disk_full() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(broker.post_office().paging().is_disk_full());

    let conn = Connection::connect(&address, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    session
        .create_queue(
            QueueConfiguration::new("full")
                .routing_type(RoutingType::Anycast)
                .durable(false),
        )
        .await
        .unwrap();

    let blocked = session.create_producer(Some("full")).await.unwrap();
    let send = blocked.send(message::text_message("blocked"));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), send)
            .await
            .is_err(),
        "a send to a full disk's address went through"
    );

    let management = session
        .create_producer(Some("activemq.management"))
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        management.send(message::text_message("ping")),
    )
    .await
    .expect("a send to the management address blocked")
    .unwrap();
    broker.stop().unwrap();
}
