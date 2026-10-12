//! A broker stopped and another started in its place on the same data and
//! ports, in the same process, as the network check restarts one.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_config as cfg;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpStream;

async fn start(dir: &Path, address: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             <message-expiry-scan-period>10</message-expiry-scan-period>
             <address-queue-scan-period>10</address-queue-scan-period>
             <transaction-timeout-scan-period>10</transaction-timeout-scan-period>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    broker
        .add_acceptor("artemis", address, Acceptor::default())
        .await
        .unwrap();
    broker
}

async fn op(broker: &Arc<Broker>, resource: &str, operation: &str, params: Vec<Json>) -> Json {
    broker
        .management()
        .invoke(broker, resource, operation, &params, Default::default())
        .await
        .unwrap()
}

/// What `saranga run` does to stop a broker.
async fn stop(broker: &Arc<Broker>) {
    broker.pause_acceptors().await;
    broker.close_unestablished();
    for c in broker.registry().connections() {
        c.fail();
    }
    while !broker.registry().all_closed() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    broker.stop().unwrap();
}

/// A free port on 127.0.0.1.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_broker_is_freed_and_another_starts_on_its_data() {
    let dir = tempfile::tempdir().unwrap();
    let address = format!("127.0.0.1:{}", free_port());
    let broker = start(dir.path(), &address).await;
    op(
        &broker,
        "broker",
        "createQueue",
        vec![
            Json::from("orders"),
            Json::from("ANYCAST"),
            Json::from("orders"),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await;
    op(
        &broker,
        "address.orders",
        "sendMessage",
        vec![
            Json::Object(Vec::new()),
            Json::Long(3),
            Json::from("kept"),
            Json::Bool(true),
            Json::Null,
            Json::Null,
        ],
    )
    .await;
    // A client connected when it stops.
    let _client = TcpStream::connect(&address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    stop(&broker).await;
    let weak = Arc::downgrade(&broker);
    drop(broker);
    let freed = async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), freed)
        .await
        .expect("the stopped broker is still referenced");

    let broker = start(dir.path(), &address).await;
    assert_eq!(
        op(&broker, "queue.orders", "countMessages", vec![]).await,
        Json::from(1i64)
    );
    TcpStream::connect(&address).await.unwrap();
    stop(&broker).await;
}
