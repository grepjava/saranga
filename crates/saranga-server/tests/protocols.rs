//! Protocol detection on an acceptor (`ProtocolHandler`).

use std::sync::Arc;

use saranga_server::postoffice::SelectorFilters;
use saranga_server::protocol::{Connection, ProtocolManager, ServeFuture};
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Answers `<NAME> saw <first line>` to a client whose first bytes start
/// with `prefix`.
struct Echo {
    name: &'static str,
    prefix: &'static [u8],
}

impl ProtocolManager for Echo {
    fn name(&self) -> &'static str {
        self.name
    }

    fn is_protocol(&self, header: &[u8]) -> bool {
        header.starts_with(self.prefix)
    }

    fn serve(&self, _broker: Arc<Broker>, mut c: Connection) -> ServeFuture {
        let name = self.name;
        Box::pin(async move {
            let mut line = Vec::new();
            let mut b = [0u8; 1];
            while c.stream.read_exact(&mut b).await.is_ok() && b[0] != b'\n' {
                line.push(b[0]);
            }
            let reply = format!("{name} saw {}\n", String::from_utf8_lossy(&line));
            let _ = c.stream.write_all(reply.as_bytes()).await;
        })
    }
}

async fn start(acceptor: Acceptor) -> std::net::SocketAddr {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.protocols = vec![
        Arc::new(Echo {
            name: "STOMP",
            prefix: b"CONNECT",
        }),
        Arc::new(Echo {
            name: "AMQP",
            prefix: b"AMQP",
        }),
    ];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.serve_acceptor(listener, acceptor));
    address
}

async fn exchange(address: std::net::SocketAddr, send: &[u8]) -> String {
    let mut s = TcpStream::connect(address).await.unwrap();
    s.write_all(send).await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out).into_owned()
}

#[tokio::test]
async fn first_bytes_pick_the_protocol() {
    let address = start(Acceptor::default()).await;
    // The protocol sees the bytes it was detected from.
    assert_eq!(exchange(address, b"CONNECT\n").await, "STOMP saw CONNECT\n");
    assert_eq!(
        exchange(address, b"AMQP\x00\x01\x00\x00\n").await,
        "AMQP saw AMQP\x00\x01\x00\x00\n"
    );
}

#[tokio::test]
async fn only_the_listed_protocols() {
    let address = start(Acceptor {
        protocols: Some(vec!["AMQP".to_owned()]),
        ..Acceptor::default()
    })
    .await;
    assert_eq!(
        exchange(address, b"AMQP\x00\x01\x00\x00\n").await,
        "AMQP saw AMQP\x00\x01\x00\x00\n"
    );
    // STOMP is not enabled, and Core is not there to fall back to.
    assert_eq!(exchange(address, b"CONNECT\n\n\0\n").await, "");
}

#[tokio::test]
async fn unknown_bytes_are_core() {
    let address = start(Acceptor::default()).await;
    // Not the ARTEMIS handshake: Core closes the connection.
    assert_eq!(exchange(address, b"GARBAGE!\n").await, "");
    // A Core client gets an answer to its ping.
    let mut s = TcpStream::connect(address).await.unwrap();
    s.write_all(b"ARTEMIS").await.unwrap();
    // PING on channel 0: length, type 10, channel 0, connection TTL.
    let mut frame = Vec::new();
    frame.extend_from_slice(&17u32.to_be_bytes());
    frame.push(10);
    frame.extend_from_slice(&0i64.to_be_bytes());
    frame.extend_from_slice(&60_000i64.to_be_bytes());
    s.write_all(&frame).await.unwrap();
    let mut reply = [0u8; 21];
    tokio::time::timeout(std::time::Duration::from_secs(5), s.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply[..5], &frame[..5]);
}

#[tokio::test]
async fn a_client_too_slow_to_say_who_it_is_is_closed() {
    let address = start(Acceptor {
        handshake_timeout: Some(std::time::Duration::from_millis(200)),
        ..Acceptor::default()
    })
    .await;
    let mut s = TcpStream::connect(address).await.unwrap();
    s.write_all(b"CON").await.unwrap();
    let mut out = Vec::new();
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut out))
        .await
        .expect("closed by the broker")
        .unwrap_or(0);
    assert_eq!(n, 0);
}
