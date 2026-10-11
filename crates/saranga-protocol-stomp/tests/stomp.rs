//! A STOMP client against the broker: connect, subscribe, send, receive,
//! acknowledge and transactions. The full suite against Artemis is
//! tests/interop/stomp/Stomp.java.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_stomp::StompProtocol;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start() -> std::net::SocketAddr {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.protocols = vec![Arc::new(StompProtocol)];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.serve_acceptor(listener, Acceptor::default()));
    address
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(address: std::net::SocketAddr, version: &str) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let mut c = Self {
            stream,
            buf: Vec::new(),
        };
        c.send(&format!(
            "CONNECT\naccept-version:{version}\nhost:localhost\n\n"
        ))
        .await;
        let f = c.frame().await;
        assert!(f.starts_with("CONNECTED\n"), "{f}");
        c
    }

    /// Sends a frame given without its NUL.
    async fn send(&mut self, frame: &str) {
        let mut bytes = frame.as_bytes().to_vec();
        bytes.push(0);
        self.stream.write_all(&bytes).await.unwrap();
    }

    /// The next frame, without its NUL, skipping heart-beats.
    async fn frame(&mut self) -> String {
        loop {
            while self.buf.first() == Some(&b'\n') {
                self.buf.remove(0);
            }
            if let Some(end) = self.buf.iter().position(|&b| b == 0) {
                let frame: Vec<u8> = self.buf.drain(..=end).collect();
                return String::from_utf8_lossy(&frame[..end]).into_owned();
            }
            let mut chunk = [0u8; 4096];
            let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut chunk))
                .await
                .expect("a frame within 5 s")
                .unwrap();
            assert!(n > 0, "connection closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn no_frame(&mut self) {
        let mut chunk = [0u8; 64];
        let r =
            tokio::time::timeout(Duration::from_millis(300), self.stream.read(&mut chunk)).await;
        assert!(r.is_err(), "unexpected bytes");
    }
}

fn header<'a>(frame: &'a str, name: &str) -> Option<&'a str> {
    frame
        .lines()
        .skip(1)
        .take_while(|l| !l.is_empty())
        .find_map(|l| l.strip_prefix(name).and_then(|r| r.strip_prefix(':')))
}

#[tokio::test]
async fn send_and_receive() {
    let address = start().await;
    let mut c = Client::connect(address, "1.2").await;
    c.send("SUBSCRIBE\ndestination:q1\ndestination-type:ANYCAST\nsubscription-type:ANYCAST\nid:s1\nreceipt:r1\n\n")
        .await;
    let receipt = c.frame().await;
    assert_eq!(header(&receipt, "receipt-id"), Some("r1"));
    c.send("SEND\ndestination:q1\ndestination-type:ANYCAST\nfoo:bar\n\nhello")
        .await;
    let m = c.frame().await;
    assert!(m.starts_with("MESSAGE\n"), "{m}");
    assert_eq!(header(&m, "subscription"), Some("s1"));
    assert_eq!(header(&m, "destination"), Some("q1"));
    assert_eq!(header(&m, "foo"), Some("bar"));
    assert_eq!(header(&m, "content-length"), Some("5"));
    assert!(m.ends_with("\n\nhello"), "{m}");
}

#[tokio::test]
async fn client_ack_and_redelivery() {
    let address = start().await;
    let mut c = Client::connect(address, "1.1").await;
    c.send("SEND\ndestination:q2\ndestination-type:ANYCAST\n\none")
        .await;
    c.send("SUBSCRIBE\ndestination:q2\nid:s\nack:client\n\n")
        .await;
    let m = c.frame().await;
    assert_eq!(header(&m, "redelivered"), Some("false"));
    drop(c);
    // Not acknowledged: the next subscriber gets it again.
    let mut c = Client::connect(address, "1.1").await;
    c.send("SUBSCRIBE\ndestination:q2\nid:s\nack:client\n\n")
        .await;
    let m = c.frame().await;
    assert_eq!(header(&m, "redelivered"), Some("true"));
    let id = header(&m, "message-id").unwrap().to_owned();
    c.send(&format!(
        "ACK\nmessage-id:{id}\nsubscription:s\nreceipt:a\n\n"
    ))
    .await;
    assert!(c.frame().await.starts_with("RECEIPT\n"));
    drop(c);
    let mut c = Client::connect(address, "1.1").await;
    c.send("SUBSCRIBE\ndestination:q2\nid:s\n\n").await;
    c.no_frame().await;
}

#[tokio::test]
async fn transactions() {
    let address = start().await;
    let mut c = Client::connect(address, "1.2").await;
    c.send("SUBSCRIBE\ndestination:q3\ndestination-type:ANYCAST\nsubscription-type:ANYCAST\nid:s\nreceipt:r\n\n")
        .await;
    c.frame().await;
    c.send("BEGIN\ntransaction:t\n\n").await;
    c.send("SEND\ndestination:q3\ntransaction:t\n\nin tx").await;
    c.no_frame().await;
    c.send("COMMIT\ntransaction:t\n\n").await;
    let m = c.frame().await;
    assert!(m.ends_with("\n\nin tx"), "{m}");
    c.send("COMMIT\ntransaction:t\n\n").await;
    let e = c.frame().await;
    assert_eq!(header(&e, "message"), Some("AMQ339013 Error committing t"));
}

#[tokio::test]
async fn version_errors() {
    let address = start().await;
    let mut s = TcpStream::connect(address).await.unwrap();
    s.write_all(b"CONNECT\naccept-version:3.0\n\n\0")
        .await
        .unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.starts_with(
            "ERROR\nmessage:AMQ339002 Stomp versions not supported 3.0\nversion:1.0,1.1,1.2\n"
        ),
        "{text}"
    );
    assert!(text.ends_with("Supported protocol versions are v1.0 v1.1 v1.2\0\n"));
}
