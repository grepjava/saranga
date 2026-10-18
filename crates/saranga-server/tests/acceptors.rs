//! Acceptors started, stopped and reloaded through management
//! (`AcceptorControl`).

use std::sync::Arc;
use std::time::Duration;

use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpStream;

async fn op(broker: &Arc<Broker>, operation: &str) -> Result<Json, String> {
    broker
        .management()
        .invoke(
            broker,
            "acceptor.artemis",
            operation,
            &[],
            Default::default(),
        )
        .await
}

async fn started(broker: &Arc<Broker>) -> Json {
    broker
        .management()
        .attribute(broker, "acceptor.artemis", "Started", Default::default())
        .await
        .unwrap()
}

/// A free port on 127.0.0.1.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A Core PING on channel 0 (length, type 10, channel 0, connection TTL),
/// answered by the broker.
async fn ping(s: &mut TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut frame = Vec::new();
    frame.extend_from_slice(&17u32.to_be_bytes());
    frame.push(10);
    frame.extend_from_slice(&0i64.to_be_bytes());
    frame.extend_from_slice(&60_000i64.to_be_bytes());
    s.write_all(&frame).await.unwrap();
    let mut reply = [0u8; 21];
    tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut reply))
        .await
        .expect("the broker answers the ping")
        .unwrap();
    assert_eq!(&reply[..5], &frame[..5]);
}

async fn accepts(address: &str) -> bool {
    tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(address))
        .await
        .is_ok_and(|r| r.is_ok())
}

#[tokio::test]
async fn management_stops_starts_and_reloads_an_acceptor() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let address = format!("127.0.0.1:{}", free_port());
    broker
        .add_acceptor("artemis", &address, Acceptor::default())
        .await
        .unwrap();
    assert_eq!(started(&broker).await, Json::from(true));
    assert!(accepts(&address).await);

    // A connection made before the stop is closed by it.
    let mut before = TcpStream::connect(&address).await.unwrap();
    op(&broker, "stop").await.unwrap();
    assert_eq!(started(&broker).await, Json::from(false));
    assert_eq!(op(&broker, "isStarted").await, Ok(Json::from(false)));
    assert!(!accepts(&address).await);
    let mut b = [0u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::AsyncReadExt::read(&mut before, &mut b),
    )
    .await;
    assert!(matches!(read, Ok(Ok(0)) | Ok(Err(_))), "{read:?}");

    // A stopped acceptor stays stopped through a reload.
    op(&broker, "reload").await.unwrap();
    assert!(!accepts(&address).await);

    op(&broker, "start").await.unwrap();
    assert!(accepts(&address).await);
    // Starting a started acceptor does nothing.
    op(&broker, "start").await.unwrap();

    // A reload binds again; connections stay. The ping before it makes sure
    // the connection was accepted: one still in the listen backlog goes
    // with the listener, in Netty too.
    let mut kept = TcpStream::connect(&address).await.unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut kept, b"ARTEMIS")
        .await
        .unwrap();
    ping(&mut kept).await;
    op(&broker, "reload").await.unwrap();
    assert_eq!(started(&broker).await, Json::from(true));
    assert!(accepts(&address).await);
    ping(&mut kept).await;

    assert_eq!(
        op(&broker, "frobnicate").await,
        Err("AMQ229069: no operation frobnicate/0".to_owned())
    );
}
