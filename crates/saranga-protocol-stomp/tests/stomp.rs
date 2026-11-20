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
    start_with(Acceptor::default()).await
}

async fn start_with(acceptor: Acceptor) -> std::net::SocketAddr {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.protocols = vec![Arc::new(StompProtocol)];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.serve_acceptor(listener, acceptor));
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

/// Messages going to a client that sends nothing do not keep its
/// connection alive past the TTL: only bytes from it do.
#[tokio::test]
async fn a_receiving_client_that_sends_nothing_times_out() {
    let mut acceptor = Acceptor::default();
    acceptor
        .params
        .insert("connectionTtl".to_owned(), "1000".to_owned());
    let address = start_with(acceptor).await;
    let mut consumer = Client::connect(address, "1.2").await;
    consumer
        .send("SUBSCRIBE\ndestination:ttl\ndestination-type:ANYCAST\nsubscription-type:ANYCAST\nid:s\nack:auto\n\n")
        .await;
    let mut producer = Client::connect(address, "1.2").await;
    let sending = tokio::spawn(async move {
        loop {
            producer
                .send("SEND\ndestination:ttl\ndestination-type:ANYCAST\n\nm")
                .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let error = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let f = consumer.frame().await;
            if !f.starts_with("MESSAGE\n") {
                break f;
            }
        }
    })
    .await
    .expect("closed within 3 s");
    sending.abort();
    assert!(error.starts_with("ERROR\n"), "{error}");
    assert!(
        header(&error, "message").is_some_and(|m| m.starts_with("AMQ229014")),
        "{error}"
    );
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

/// While the disk is full a SEND fails the connection, as STOMP has no
/// flow control to hold the client back: the ERROR it failed with, the
/// SEND's own ERROR with its receipt, and the connection closes.
#[tokio::test]
async fn a_send_while_the_disk_is_full_fails_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <min-disk-free>1000000000000000000</min-disk-free>
             <disk-scan-period>50</disk-scan-period>
           </core></configuration>"#,
    )
    .unwrap();
    let mut config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    config.protocols = vec![Arc::new(StompProtocol)];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    for _ in 0..100 {
        if broker.post_office().paging().is_disk_full() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(broker.post_office().paging().is_disk_full());

    let mut c = Client::connect(address, "1.2").await;
    c.send("SEND\ndestination:full\nreceipt:r1\n\nhello").await;
    let failure = c.frame().await;
    assert!(failure.starts_with("ERROR\n"), "{failure}");
    assert!(
        header(&failure, "message")
            .is_some_and(|m| m.starts_with(r"AMQ229119\c Free storage space is at ")),
        "{failure}"
    );
    assert_eq!(header(&failure, "receipt-id"), None);
    let error = c.frame().await;
    assert!(
        header(&error, "message").is_some_and(|m| m.starts_with("AMQ339011 Error sending message")),
        "{error}"
    );
    assert_eq!(header(&error, "receipt-id"), Some("r1"));
    let mut chunk = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), c.stream.read(&mut chunk))
        .await
        .expect("closed within 5 s")
        .unwrap_or(0);
    assert_eq!(n, 0, "the connection closed");
    broker.stop().unwrap();
}

/// A frame that keeps coming before the client logs in is refused once it
/// passes 64 KiB: the client gets an ERROR and the connection closes.
#[tokio::test]
async fn an_endless_frame_before_login_is_refused() {
    let address = start().await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"CONNECT\naccept-version:1.2\nhost:localhost\n")
        .await
        .unwrap();
    let line = format!("x:{}\n", "y".repeat(1000));
    for _ in 0..100 {
        if stream.write_all(line.as_bytes()).await.is_err() {
            break;
        }
    }
    let mut got = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut got)).await;
    let got = String::from_utf8_lossy(&got);
    assert!(read.is_ok(), "still open");
    assert!(got.starts_with("ERROR\n"), "{got}");
}

/// Once logged in, a client sends frames larger than that.
#[tokio::test]
async fn a_large_frame_after_login_is_taken() {
    let address = start().await;
    let mut c = Client::connect(address, "1.2").await;
    c.send(
        "SUBSCRIBE\ndestination:big\ndestination-type:ANYCAST\nsubscription-type:ANYCAST\nid:s\n\n",
    )
    .await;
    let body = "z".repeat(200 * 1024);
    c.send(&format!(
        "SEND\ndestination:big\ndestination-type:ANYCAST\nreceipt:r\n\n{body}"
    ))
    .await;
    let mut message = String::new();
    for _ in 0..2 {
        let f = c.frame().await;
        if f.starts_with("MESSAGE\n") {
            message = f;
        }
    }
    assert!(message.ends_with(&body), "the large message");
}

/// An ACK naming the wrong subscription is rejected, but under STOMP 1.1 the
/// connection stays open, so the pending acknowledgement must survive for the
/// client to retry with the right subscription. (Before, the entry was removed
/// before the subscription was checked, so the retry failed as "no message" and
/// the individual delivery was stuck, uncredited, until disconnect.)
#[tokio::test]
async fn a_rejected_ack_can_be_retried() {
    let address = start().await;
    let mut c = Client::connect(address, "1.1").await;
    c.send("SEND\ndestination:q_retry\ndestination-type:ANYCAST\n\none")
        .await;
    c.send("SUBSCRIBE\ndestination:q_retry\ndestination-type:ANYCAST\nsubscription-type:ANYCAST\nid:s\nack:client-individual\n\n")
        .await;
    let m = c.frame().await;
    assert!(m.starts_with("MESSAGE\n"), "{m}");
    let id = header(&m, "message-id").unwrap().to_owned();

    // ACK naming the wrong subscription: rejected, connection stays open.
    c.send(&format!(
        "ACK\nmessage-id:{id}\nsubscription:wrong\nreceipt:bad\n\n"
    ))
    .await;
    let err = c.frame().await;
    assert!(err.starts_with("ERROR\n"), "{err}");

    // Retry with the right subscription: the entry is still there, so it acks.
    c.send(&format!(
        "ACK\nmessage-id:{id}\nsubscription:s\nreceipt:good\n\n"
    ))
    .await;
    let ok = c.frame().await;
    assert!(ok.starts_with("RECEIPT\n"), "{ok}");
    assert_eq!(header(&ok, "receipt-id"), Some("good"));
}

/// A committed or aborted transaction's session goes with it: a client
/// giving each transaction its own ID does not pile up sessions until it
/// disconnects. What the commits sent is there by each receipt.
#[tokio::test]
async fn an_ended_transaction_leaves_no_session() {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.protocols = vec![Arc::new(StompProtocol)];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    let mut c = Client::connect(address, "1.2").await;
    for i in 0..100 {
        c.send(&format!("BEGIN\ntransaction:t{i}\n\n")).await;
        c.send(&format!(
            "SEND\ndestination:q9\ndestination-type:ANYCAST\ntransaction:t{i}\n\nm{i}"
        ))
        .await;
        let (end, receipt) = if i % 2 == 0 {
            ("COMMIT", "c")
        } else {
            ("ABORT", "a")
        };
        c.send(&format!(
            "{end}\ntransaction:t{i}\nreceipt:{receipt}{i}\n\n"
        ))
        .await;
        let r = c.frame().await;
        assert_eq!(
            header(&r, "receipt-id"),
            Some(&*format!("{receipt}{i}")),
            "{r}"
        );
    }
    let sessions = broker.registry().sessions().len();
    assert!(
        sessions <= 1,
        "{sessions} sessions left after 100 ended transactions"
    );
    let queued = broker.post_office().queue("q9").map(|q| q.message_count());
    assert_eq!(
        queued,
        Some(50),
        "each commit's message is there by its receipt"
    );
}

/// Every message a cumulative ACK acknowledges gives its window credit
/// back, on a non-destructive queue too, where the messages stay.
#[tokio::test]
async fn a_cumulative_ack_on_a_non_destructive_queue_credits_every_message() {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.protocols = vec![Arc::new(StompProtocol)];
    let broker = Broker::start(config).await.unwrap();
    let mut spec = saranga_server::postoffice::QueueSpec::new("nd", "nd");
    spec.auto_create_address = Some(true);
    spec.non_destructive = Some(true);
    spec.routing_type = Some(saranga_server::RoutingType::Anycast);
    broker.post_office().create_queue(spec, false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));

    let mut c = Client::connect(address, "1.2").await;
    for i in 0..12 {
        c.send(&format!(
            "SEND\ndestination:nd\ndestination-type:ANYCAST\n\nmessage {i:02} {}",
            "x".repeat(60)
        ))
        .await;
    }
    c.send("SUBSCRIBE\ndestination:nd\ndestination-type:ANYCAST\nsubscription-type:ANYCAST\nid:s\nack:client\nconsumer-window-size:500\n\n")
        .await;
    // What the window lets through.
    async fn batch(c: &mut Client) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            let more = tokio::time::timeout(Duration::from_millis(500), c.frame()).await;
            match more {
                Ok(m) => out.push(header(&m, "ack").unwrap().to_owned()),
                Err(_) => return out,
            }
        }
    }
    let first = batch(&mut c).await;
    assert!(first.len() > 1, "a window of several messages: {first:?}");
    c.send(&format!("ACK\nid:{}\n\n", first.last().unwrap()))
        .await;
    let second = batch(&mut c).await;
    assert_eq!(
        second.len(),
        first.len(),
        "the whole window back: {first:?} then {second:?}"
    );
}

/// An auto-acknowledged subscription whose client reads nothing: as Netty's
/// writability holds Artemis's consumer, deliveries stop once the writer has
/// the high-water mark's worth to write, and the rest stay on the queue
/// rather than being acknowledged into memory and lost with the connection.
#[tokio::test]
async fn an_auto_ack_subscriber_that_does_not_read_holds_deliveries_back() {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.protocols = vec![Arc::new(StompProtocol)];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));

    const COUNT: usize = 2000;
    let body = "x".repeat(4096);
    let mut producer = Client::connect(address, "1.2").await;
    for i in 0..COUNT {
        let receipt = if i + 1 == COUNT { "receipt:done\n" } else { "" };
        producer
            .send(&format!(
                "SEND\ndestination:slow\ndestination-type:ANYCAST\n{receipt}\n{body}"
            ))
            .await;
    }
    let r = producer.frame().await;
    assert_eq!(header(&r, "receipt-id"), Some("done"), "{r}");
    let queue = broker.post_office().queue("slow").unwrap();
    assert_eq!(queue.message_count(), COUNT as i64);

    // A consumer with a small receive buffer that never reads.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let stream = socket.connect(address).await.unwrap();
    let mut consumer = Client {
        stream,
        buf: Vec::new(),
    };
    consumer
        .send("CONNECT\naccept-version:1.2\nhost:localhost\n\n")
        .await;
    consumer
        .send("SUBSCRIBE\ndestination:slow\nsubscription-type:ANYCAST\nid:s\nack:auto\n\n")
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let left = queue.message_count();
    assert!(
        left > (COUNT / 2) as i64,
        "only what the socket and the writer hold may be acknowledged: {left} left"
    );
    // Reading, the client drains the connection and the queue delivers
    // again: every message arrives.
    let connected = consumer.frame().await;
    assert!(
        connected.starts_with(
            "CONNECTED
"
        ),
        "{connected}"
    );
    for _ in 0..COUNT {
        let m = consumer.frame().await;
        assert!(
            m.starts_with(
                "MESSAGE
"
            ),
            "{m}"
        );
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(queue.message_count(), 0);
}

/// A no-local subscriber does not get its own connection's sends, also
/// when they are sent in a transaction; another connection's still come.
#[tokio::test]
async fn a_no_local_subscriber_skips_its_own_transacted_sends() {
    let address = start().await;
    let mut c = Client::connect(address, "1.2").await;
    c.send("SUBSCRIBE\ndestination:nl\nsubscription-type:MULTICAST\nid:s\nno-local:true\nreceipt:r1\n\n")
        .await;
    assert_eq!(header(&c.frame().await, "receipt-id"), Some("r1"));
    c.send("SEND\ndestination:nl\ndestination-type:MULTICAST\nreceipt:r2\n\nplain")
        .await;
    assert_eq!(header(&c.frame().await, "receipt-id"), Some("r2"));
    c.send("BEGIN\ntransaction:tx1\n\n").await;
    c.send("SEND\ndestination:nl\ndestination-type:MULTICAST\ntransaction:tx1\n\ntransacted")
        .await;
    c.send("COMMIT\ntransaction:tx1\nreceipt:r3\n\n").await;
    assert_eq!(header(&c.frame().await, "receipt-id"), Some("r3"));
    c.no_frame().await;

    let mut other = Client::connect(address, "1.2").await;
    other.send("BEGIN\ntransaction:tx2\n\n").await;
    other
        .send("SEND\ndestination:nl\ndestination-type:MULTICAST\ntransaction:tx2\n\nfrom another")
        .await;
    other.send("COMMIT\ntransaction:tx2\n\n").await;
    let m = c.frame().await;
    assert!(m.starts_with("MESSAGE\n"), "{m}");
    assert!(m.ends_with("from another"), "{m}");
}

/// An auto-acknowledged subscription given a consumer window gets its
/// credits back as its deliveries are acknowledged, so it does not stop
/// after one window.
#[tokio::test]
async fn an_auto_ack_subscription_with_a_window_keeps_receiving() {
    let address = start().await;
    let mut c = Client::connect(address, "1.2").await;
    c.send("SUBSCRIBE\ndestination:aw\nsubscription-type:ANYCAST\nid:s\nack:auto\nconsumer-window-size:500\nreceipt:r1\n\n")
        .await;
    assert_eq!(header(&c.frame().await, "receipt-id"), Some("r1"));
    let mut p = Client::connect(address, "1.2").await;
    const COUNT: usize = 20;
    for i in 0..COUNT {
        p.send(&format!(
            "SEND\ndestination:aw\ndestination-type:ANYCAST\n\nmessage {i:02} {}",
            "x".repeat(100)
        ))
        .await;
    }
    for i in 0..COUNT {
        let m = c.frame().await;
        assert!(m.starts_with("MESSAGE\n"), "message {i}: {m}");
    }
}
