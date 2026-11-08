//! MQTT clients against the broker, over a raw socket. The full suite
//! against Artemis is tests/interop/mqtt/Mqtt.java; these cover what it
//! cannot, such as a restart.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_config as cfg;
use saranga_protocol_mqtt::MqttProtocol;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start(
    instance: Option<&Path>,
) -> (
    Arc<Broker>,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let mut config = match instance {
        Some(dir) => {
            let file = cfg::BrokerConfig::parse(
                r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
                     <persistence-enabled>true</persistence-enabled>
                     <journal-pool-files>2</journal-pool-files>
                     <journal-file-size>65536</journal-file-size>
                   </core></configuration>"#,
            )
            .unwrap();
            BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap()
        }
        None => BrokerConfig::new(Arc::new(SelectorFilters)),
    };
    config.protocols = vec![Arc::new(MqttProtocol::default())];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn({
        let broker = broker.clone();
        async move {
            let _ = broker.serve_acceptor(listener, Acceptor::default()).await;
        }
    });
    (broker, address, task)
}

fn packet(byte1: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![byte1];
    let mut n = body.len();
    loop {
        let mut d = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            d |= 0x80;
        }
        out.push(d);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

fn string(s: &str) -> Vec<u8> {
    let mut out = (s.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(s.as_bytes());
    out
}

fn connect(client_id: &str, clean: bool) -> Vec<u8> {
    let mut body = vec![
        0,
        4,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        if clean { 2 } else { 0 },
        0,
        60,
    ];
    body.extend(string(client_id));
    packet(0x10, &body)
}

struct Client(TcpStream);

impl Client {
    async fn open(address: std::net::SocketAddr, client_id: &str, clean: bool) -> (Self, Vec<u8>) {
        let mut c = Self(TcpStream::connect(address).await.unwrap());
        c.send(&connect(client_id, clean)).await;
        let connack = c.packet().await.expect("CONNACK");
        (c, connack)
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.0.write_all(bytes).await.unwrap();
    }

    /// The next packet, or `None` when the connection closed.
    async fn packet(&mut self) -> Option<Vec<u8>> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut b1 = [0u8; 1];
            if self.0.read_exact(&mut b1).await.is_err() {
                return None;
            }
            let mut out = vec![b1[0]];
            let (mut len, mut mult) = (0usize, 1usize);
            loop {
                let mut d = [0u8; 1];
                self.0.read_exact(&mut d).await.ok()?;
                out.push(d[0]);
                len += usize::from(d[0] & 127) * mult;
                mult *= 128;
                if d[0] & 128 == 0 {
                    break;
                }
            }
            let mut rest = vec![0u8; len];
            self.0.read_exact(&mut rest).await.ok()?;
            out.extend(rest);
            Some(out)
        })
        .await
        .expect("a packet within 5 s")
    }

    async fn subscribe(&mut self, filter: &str, qos: u8) {
        let mut body = vec![0, 1];
        body.extend(string(filter));
        body.push(qos);
        self.send(&packet(0x82, &body)).await;
        assert_eq!(self.packet().await.unwrap(), vec![0x90, 3, 0, 1, qos]);
    }

    async fn publish(&mut self, topic: &str, payload: &[u8], packet_id: u16) {
        self.publish_with(0x32, topic, payload, packet_id).await;
        let puback = self.packet().await.unwrap();
        assert_eq!(puback[0], 0x40);
    }

    /// A QoS 1 PUBLISH with `flags`, unanswered yet.
    async fn publish_with(&mut self, flags: u8, topic: &str, payload: &[u8], packet_id: u16) {
        let mut body = string(topic);
        body.extend(packet_id.to_be_bytes());
        body.extend_from_slice(payload);
        self.send(&packet(flags, &body)).await;
    }

    /// A retained QoS 1 PUBLISH, acknowledged.
    async fn retain(&mut self, topic: &str, payload: &[u8], packet_id: u16) {
        self.publish_with(0x33, topic, payload, packet_id).await;
        let puback = self.packet().await.expect("a PUBACK");
        assert_eq!(puback[0], 0x40);
    }
}

/// A PUBLISH's flags, packet ID and payload.
fn parse_publish(p: &[u8]) -> (u8, u16, Vec<u8>) {
    let mut i = 1;
    while p[i] & 0x80 != 0 {
        i += 1;
    }
    i += 1;
    let topic_len = usize::from(u16::from_be_bytes([p[i], p[i + 1]]));
    i += 2 + topic_len;
    let id = u16::from_be_bytes([p[i], p[i + 1]]);
    (p[0], id, p[i + 2..].to_vec())
}

#[tokio::test]
async fn publish_and_subscribe() {
    let (_broker, address, _task) = start(None).await;
    let (mut sub, connack) = Client::open(address, "sub", true).await;
    assert_eq!(connack, vec![0x20, 2, 0, 0]);
    sub.subscribe("a/+", 1).await;
    let (mut publisher, _) = Client::open(address, "pub", true).await;
    publisher.publish("a/b", b"hello", 7).await;
    let p = sub.packet().await.unwrap();
    let (flags, _, payload) = parse_publish(&p);
    assert_eq!(flags, 0x32);
    assert_eq!(payload, b"hello");
}

/// An unacknowledged QoS 1 delivery survives a restart with its session,
/// and comes again as a duplicate with the same packet ID: the session
/// store and the packet-ID records are reloaded.
#[tokio::test]
async fn session_and_packet_ids_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, task) = start(Some(dir.path())).await;
    let (mut sub, _) = Client::open(address, "durable", false).await;
    sub.subscribe("restart/t", 1).await;
    let (mut publisher, _) = Client::open(address, "pub", true).await;
    publisher.publish("restart/t", b"m1", 1).await;
    let (flags, packet_id, payload) = parse_publish(&sub.packet().await.unwrap());
    assert_eq!((flags, payload.as_slice()), (0x32, &b"m1"[..]));
    // Gone without acknowledging.
    sub.send(&[0xE0, 0]).await;
    assert_eq!(sub.packet().await, None);
    publisher.send(&[0xE0, 0]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    broker.stop().unwrap();
    task.abort();
    drop(broker);

    let (_broker, address, _task) = start(Some(dir.path())).await;
    let (mut sub, connack) = Client::open(address, "durable", false).await;
    assert_eq!(connack, vec![0x20, 2, 1, 0], "session present");
    let (flags, again, payload) = parse_publish(&sub.packet().await.unwrap());
    assert_eq!(flags, 0x3A, "a duplicate");
    assert_eq!(again, packet_id, "the same packet ID");
    assert_eq!(payload, b"m1");
    sub.send(&[0x40, 2, (again >> 8) as u8, again as u8]).await;
    // Still subscribed after the restart.
    let (mut publisher, _) = Client::open(address, "pub", true).await;
    publisher.publish("restart/t", b"m2", 2).await;
    let (_, _, payload) = parse_publish(&sub.packet().await.unwrap());
    assert_eq!(payload, b"m2");
}

/// While the disk is full a PUBLISH fails the connection, as MQTT has no
/// flow control to hold the client back: it closes, unacknowledged.
#[tokio::test]
async fn a_publish_while_the_disk_is_full_closes_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <min-disk-free>1000000000000000000</min-disk-free>
             <disk-scan-period>50</disk-scan-period>
           </core></configuration>"#,
    )
    .unwrap();
    let mut config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    config.protocols = vec![Arc::new(MqttProtocol::default())];
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

    let (mut publisher, connack) = Client::open(address, "full", true).await;
    assert_eq!(connack, vec![0x20, 2, 0, 0]);
    let mut body = string("full/t");
    body.extend(1u16.to_be_bytes());
    body.extend_from_slice(b"m");
    publisher.send(&packet(0x32, &body)).await;
    assert_eq!(publisher.packet().await, None, "closed without a PUBACK");
    broker.stop().unwrap();
}

/// A retained message too big for a journal record (64 KiB here) is kept
/// as a large message, as the message itself is.
#[tokio::test]
async fn a_retained_message_larger_than_a_journal_record_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, _task) = start(Some(dir.path())).await;
    let big: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
    let (mut publisher, _) = Client::open(address, "big-pub", true).await;
    publisher.retain("big/t", &big, 1).await;
    let (mut sub, _) = Client::open(address, "big-sub", true).await;
    sub.subscribe("big/t", 1).await;
    let (flags, _, payload) = parse_publish(&sub.packet().await.unwrap());
    assert_eq!(flags & 1, 1, "retained");
    assert_eq!(payload, big);
    broker.stop().unwrap();
}

/// Clients replacing one topic's retained message at once leave it one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_replacements_leave_one_retained_message() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, _task) = start(Some(dir.path())).await;
    let queue = "$sys.mqtt.retain.race.t";
    for round in 0..5u16 {
        let mut clients = Vec::new();
        for n in 0..16 {
            clients.push(Client::open(address, &format!("race-{n}"), true).await.0);
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(clients.len()));
        let publishes: Vec<_> = clients
            .into_iter()
            .enumerate()
            .map(|(n, mut c)| {
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    c.retain("race/t", format!("v{round}-{n}").as_bytes(), 1)
                        .await;
                })
            })
            .collect();
        for p in publishes {
            p.await.unwrap();
        }
        let q = broker.post_office().queue(queue).expect("the retain queue");
        let mut count = 0;
        for _ in 0..50 {
            count = q.message_count();
            if count <= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(count, 1, "round {round}");
    }
    broker.stop().unwrap();
}

/// A retained message whose replacement fails stays: it is removed in
/// the replacement's transaction, which rolls back.
#[tokio::test]
async fn a_failed_replacement_keeps_the_retained_message() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, _task) = start(Some(dir.path())).await;
    let (mut publisher, _) = Client::open(address, "r-pub", true).await;
    publisher.retain("r/t", b"v1", 1).await;
    // The replacement is large: the message's file opens, its retained
    // copy's file does not.
    let fault = saranga_fault::Fault::new(saranga_fault::Op::Open, dir.path())
        .named(".msg")
        .after(1)
        .arm();
    let big = vec![7u8; 100_000];
    publisher.publish_with(0x33, "r/t", &big, 2).await;
    assert_eq!(publisher.packet().await, None, "closed without a PUBACK");
    assert_eq!(fault.hits(), 1, "the retained copy's file failed");
    drop(fault);
    let (mut sub, _) = Client::open(address, "r-sub", true).await;
    sub.subscribe("r/t", 1).await;
    let (flags, _, payload) = parse_publish(&sub.packet().await.expect("the old message"));
    assert_eq!(flags & 1, 1, "retained");
    assert_eq!(payload, b"v1");
    broker.stop().unwrap();
}

/// A QoS 2 PUBLISH sent again with its packet ID before the first is
/// written (pipelined, both in one write) is the same message: the
/// subscriber gets it once, and the publisher a PUBREC for each.
#[tokio::test]
async fn a_qos2_publish_sent_twice_at_once_is_delivered_once() {
    let dir = tempfile::tempdir().unwrap();
    let (_broker, address, _task) = start(Some(dir.path())).await;
    let (mut sub, _) = Client::open(address, "sub2", true).await;
    sub.subscribe("twice/t", 1).await;
    let (mut publisher, _) = Client::open(address, "pub2", true).await;
    let mut body = string("twice/t");
    body.extend(5u16.to_be_bytes());
    body.extend_from_slice(b"once");
    let one = packet(0x34, &body);
    publisher.send(&[one.clone(), one].concat()).await;
    for _ in 0..2 {
        assert_eq!(publisher.packet().await.unwrap()[0], 0x50, "a PUBREC");
    }
    let (_, id, payload) = parse_publish(&sub.packet().await.unwrap());
    assert_eq!(payload, b"once");
    sub.send(&[0x40, 2, (id >> 8) as u8, id as u8]).await;
    // No second copy.
    let again = tokio::time::timeout(Duration::from_millis(500), sub.0.read_u8()).await;
    assert!(again.is_err(), "delivered twice");
}
