//! MQTT clients against the broker, over a raw socket. The full suite
//! against Artemis is tests/interop/mqtt/Mqtt.java; these cover what it
//! cannot, such as a restart.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_config as cfg;
use saranga_protocol_mqtt::{MqttConfig, MqttProtocol};
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
    start_scanning(instance, 5000).await
}

/// [`start`] with sessions scanned every `scan_ms`.
async fn start_scanning(
    instance: Option<&Path>,
    scan_ms: i64,
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
    config.protocols = vec![Arc::new(MqttProtocol::new(MqttConfig {
        session_scan_interval: scan_ms,
        ..MqttConfig::default()
    }))];
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

/// A 3.1.1 CONNECT with a QoS 0 will.
fn connect_will(client_id: &str, clean: bool, topic: &str, message: &[u8]) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4];
    body.push(if clean { 0x06 } else { 0x04 });
    body.extend([0, 60]);
    body.extend(string(client_id));
    body.extend(string(topic));
    body.extend((message.len() as u16).to_be_bytes());
    body.extend_from_slice(message);
    packet(0x10, &body)
}

/// An MQTT 5 CONNECT with a session expiry interval and, maybe, a QoS 0
/// will without a delay.
fn connect5(client_id: &str, clean: bool, expiry: u32, will: Option<(&str, &[u8])>) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5];
    body.push(u8::from(clean) << 1 | if will.is_some() { 0x04 } else { 0 });
    body.extend([0, 60]);
    body.extend([5, 0x11]);
    body.extend(expiry.to_be_bytes());
    body.extend(string(client_id));
    if let Some((topic, message)) = will {
        body.push(0);
        body.extend(string(topic));
        body.extend((message.len() as u16).to_be_bytes());
        body.extend_from_slice(message);
    }
    packet(0x10, &body)
}

struct Client(TcpStream);

impl Client {
    /// Connects with `connect`, a CONNECT packet; the CONNACK comes back.
    async fn open_with(address: std::net::SocketAddr, connect: &[u8]) -> (Self, Vec<u8>) {
        let mut c = Self(TcpStream::connect(address).await.unwrap());
        c.send(connect).await;
        let connack = c.packet().await.expect("CONNACK");
        assert_eq!(connack[0], 0x20, "a CONNACK");
        (c, connack)
    }

    /// Nothing comes within `ms`.
    async fn silent_for(&mut self, ms: u64) -> bool {
        tokio::time::timeout(Duration::from_millis(ms), self.0.read_u8())
            .await
            .is_err()
    }

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

/// An MQTT 5 client that fails with a will and no Will Delay Interval has
/// its will published at once, though its session outlives the
/// connection (5.0 section 3.1.3.2.2): not when the session expires.
#[tokio::test]
async fn a_will_without_a_delay_goes_at_once_though_the_session_stays() {
    let (_broker, address, _task) = start_scanning(None, 100).await;
    let (mut watch, _) = Client::open(address, "watch5", true).await;
    watch.subscribe("will5/t", 0).await;
    let (dying, connack) = Client::open_with(
        address,
        &connect5("dying5", true, 60, Some(("will5/t", b"gone"))),
    )
    .await;
    assert_eq!(connack[3], 0, "accepted");
    drop(dying);
    let will = watch.packet().await.expect("the will, at once");
    assert_eq!(will[0], 0x30, "a QoS 0 PUBLISH");
    assert!(will.ends_with(b"will5/tgone"), "the will: {will:?}");
}

/// A session resumed by a CONNECT without a will has none: the will of
/// the connection before it, which disconnected normally, is not sent
/// when the new connection fails.
#[tokio::test]
async fn a_resumed_session_keeps_no_will_from_its_last_connection() {
    let (_broker, address, _task) = start(None).await;
    let (mut watch, _) = Client::open(address, "watch3", true).await;
    watch.subscribe("will3/t", 0).await;
    let (mut first, _) =
        Client::open_with(address, &connect_will("resumed", false, "will3/t", b"old")).await;
    first.send(&[0xE0, 0]).await;
    assert_eq!(first.packet().await, None);
    let (second, connack) = Client::open(address, "resumed", false).await;
    assert_eq!(connack, vec![0x20, 2, 1, 0], "session present");
    drop(second);
    assert!(watch.silent_for(1000).await, "the old will was sent");
}

/// A session restored from the store that expires before its client comes
/// back is gone for good: after a second restart it is not there.
#[tokio::test]
async fn a_restored_session_that_expires_stays_gone_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, task) = start_scanning(Some(dir.path()), 100).await;
    let (mut c, _) = Client::open_with(address, &connect5("expiring", true, 1, None)).await;
    let mut body = vec![0, 1, 0];
    body.extend(string("expiring/t"));
    body.push(1);
    c.send(&packet(0x82, &body)).await;
    assert_eq!(c.packet().await.unwrap()[0], 0x90, "a SUBACK");
    c.send(&[0xE0, 0]).await;
    assert_eq!(c.packet().await, None);
    tokio::time::sleep(Duration::from_millis(200)).await;
    broker.stop().unwrap();
    task.abort();
    drop(broker);

    // Restored, never connected to, and expired by the scan.
    let (broker, _, task) = start_scanning(Some(dir.path()), 100).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    broker.stop().unwrap();
    task.abort();
    drop(broker);

    let (broker, address, _task) = start_scanning(Some(dir.path()), 100).await;
    let (_c, connack) = Client::open_with(address, &connect5("expiring", false, 60, None)).await;
    assert_eq!(connack[2], 0, "no session present");
    let queues: Vec<String> = broker
        .post_office()
        .queues()
        .into_iter()
        .map(|q| q.name().to_string())
        .filter(|q| q.contains("expiring"))
        .collect();
    assert!(queues.is_empty(), "subscription queues left: {queues:?}");
}

/// A message straight onto `queue`, as a producer without a client ID
/// (STOMP, the Core API) leaves it.
fn enqueue(queue: &saranga_server::queue::Queue, address: &str, id: i64) {
    let message = saranga_message::CoreMessage {
        message_id: id,
        address: Some(address.into()),
        body: vec![0, 0, 0, 0].into(),
        ..Default::default()
    };
    queue.add(saranga_server::message::MessageRef::new(
        saranga_server::message::ServerMessage::new(message),
    ));
}

/// A no-local subscription takes a client ID with a quote, and gets the
/// messages that came without a client ID.
#[tokio::test]
async fn no_local_quotes_the_client_id_and_takes_messages_without_one() {
    let (broker, address, _task) = start(None).await;
    let (mut c, _) = Client::open_with(address, &connect5("reader'one", true, 0, None)).await;
    let mut body = vec![0, 1, 0];
    body.extend(string("nl/t"));
    body.push(0x05); // QoS 1, no local
    c.send(&packet(0x82, &body)).await;
    let suback = c.packet().await.expect("a SUBACK");
    assert_eq!(
        (suback[0], *suback.last().unwrap()),
        (0x90, 1),
        "{suback:?}"
    );
    let queue = broker
        .post_office()
        .queues()
        .into_iter()
        .find(|q| q.name().to_string().ends_with("nl.t"))
        .expect("the subscription queue");
    enqueue(&queue, "nl.t", 3);
    let p = c.packet().await.expect("the message without a client ID");
    assert_eq!(p[0] & 0xF0, 0x30, "a PUBLISH");
    // Its own still do not come back.
    let mut own = string("nl/t");
    own.extend([0, 9, 0]);
    own.extend_from_slice(b"own");
    c.send(&packet(0x32, &own)).await;
    assert_eq!(c.packet().await.expect("a PUBACK")[0], 0x40);
    assert!(c.silent_for(500).await, "its own message came back");
}

/// A CONNECT for MQTT 5 with a Receive Maximum of 1, so each client holds
/// one message at a time.
fn connect5_one_at_a_time(client_id: &str) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 60, 3, 0x21, 0, 1];
    body.extend(string(client_id));
    packet(0x10, &body)
}

/// User properties go out in the order of their positions.
#[tokio::test]
async fn user_properties_go_out_in_order() {
    let (broker, address, _task) = start(None).await;
    let (mut c, _) = Client::open_with(address, &connect5("props", true, 0, None)).await;
    let mut body = vec![0, 1, 0];
    body.extend(string("up/t"));
    body.push(0);
    c.send(&packet(0x82, &body)).await;
    assert_eq!(c.packet().await.expect("a SUBACK")[0], 0x90);
    let queue = broker
        .post_office()
        .queues()
        .into_iter()
        .find(|q| q.name().to_string().ends_with("up.t"))
        .expect("the subscription queue");
    let mut message = saranga_message::CoreMessage {
        message_id: 7,
        address: Some("up.t".into()),
        body: vec![0, 0, 0, 0].into(),
        ..Default::default()
    };
    let props = &mut message.properties;
    props.insert(
        "mqtt.user.property.exists".into(),
        saranga_buffer::PropertyValue::Int(2),
    );
    for (i, v) in [(1, "second"), (0, "first")] {
        props.insert(
            format!("mqtt.ordered.user.property.{i}.k").as_str().into(),
            saranga_buffer::PropertyValue::String(v.into()),
        );
    }
    queue.add(saranga_server::message::MessageRef::new(
        saranga_server::message::ServerMessage::new(message),
    ));
    let p = c.packet().await.expect("the message");
    assert_eq!(p[0] & 0xF0, 0x30, "a PUBLISH");
    let at = |s: &[u8]| p.windows(s.len()).position(|w| w == s);
    let first = at(b"\x26\x00\x01k\x00\x05first").expect("the first property");
    let second = at(b"\x26\x00\x01k\x00\x06second").expect("the second property");
    assert!(first < second, "out of order");
}

/// A member leaving a shared subscription on a configured queue leaves the
/// backlog to the members still on it; the last one leaving empties it.
#[tokio::test]
async fn a_member_leaving_keeps_a_configured_shared_backlog() {
    let (broker, address, _task) = start(None).await;
    let mut spec = saranga_server::postoffice::QueueSpec::new("g.orders", "orders");
    spec.configuration_managed = true;
    spec.auto_create_address = Some(true);
    let queue = broker.post_office().create_queue(spec, false).unwrap();
    let mut subscribe = vec![0, 1, 0];
    subscribe.extend(string("$share/g/orders"));
    subscribe.push(1);
    let (mut a, _) = Client::open_with(address, &connect5_one_at_a_time("member-a")).await;
    a.send(&packet(0x82, &subscribe)).await;
    assert_eq!(a.packet().await.expect("a SUBACK")[0], 0x90);
    let (mut b, _) = Client::open_with(address, &connect5_one_at_a_time("member-b")).await;
    b.send(&packet(0x82, &subscribe)).await;
    assert_eq!(b.packet().await.expect("a SUBACK")[0], 0x90);
    for id in 1..=4 {
        let mut message = saranga_message::CoreMessage {
            message_id: id,
            durable: true,
            address: Some("orders".into()),
            body: vec![0, 0, 0, 0].into(),
            ..Default::default()
        };
        message.properties.insert(
            "mqtt.qos.level".into(),
            saranga_buffer::PropertyValue::Int(1),
        );
        queue.add(saranga_server::message::MessageRef::new(
            saranga_server::message::ServerMessage::new(message),
        ));
    }
    // Both hold messages they have not acknowledged.
    assert_eq!(a.packet().await.expect("a message for A")[0] & 0xF0, 0x30);
    let mut held = b.packet().await.expect("a message for B");

    let mut unsubscribe = vec![0, 2, 0];
    unsubscribe.extend(string("$share/g/orders"));
    a.send(&packet(0xA2, &unsubscribe)).await;
    while a.packet().await.expect("an UNSUBACK")[0] != 0xB0 {}

    // B gets all four: what A held goes back to the queue, and the rest stay.
    for got in 1..=4 {
        assert_eq!(held[0] & 0xF0, 0x30, "a PUBLISH");
        let id = parse_publish(&held).1;
        b.send(&packet(0x40, &id.to_be_bytes())).await;
        if got < 4 {
            held = b.packet().await.expect("the backlog after A left");
        }
    }
    assert!(b.silent_for(300).await);

    // The last member leaving ends the subscription: nothing is kept.
    enqueue(&queue, "orders", 9);
    assert_eq!(b.packet().await.expect("one more")[0] & 0xF0, 0x30);
    b.send(&packet(0xA2, &unsubscribe)).await;
    assert_eq!(b.packet().await.expect("an UNSUBACK")[0], 0xB0);
    assert_eq!(queue.message_count(), 0, "the last member left");
}

/// A Message Expiry Interval is an unsigned count of seconds, however long.
#[tokio::test]
async fn a_long_message_expiry_is_far_off() {
    let (broker, address, _task) = start(None).await;
    let mut spec = saranga_server::postoffice::QueueSpec::new("kept", "long.t");
    spec.auto_create_address = Some(true);
    let queue = broker.post_office().create_queue(spec, false).unwrap();
    let (mut c, _) = Client::open_with(address, &connect5("expiry", true, 0, None)).await;
    for (id, seconds) in [(1u16, 3_000_000u32), (2, u32::MAX)] {
        let mut body = string("long/t");
        body.extend(id.to_be_bytes());
        body.extend([5, 0x02]);
        body.extend(seconds.to_be_bytes());
        body.extend_from_slice(b"payload");
        c.send(&packet(0x32, &body)).await;
        assert_eq!(c.packet().await.expect("a PUBACK")[0], 0x40);
    }
    let now = saranga_server::message::now_millis();
    let mut expirations: Vec<i64> = queue
        .browse()
        .await
        .iter()
        .map(|m| m.message.core().expiration - now)
        .collect();
    expirations.sort_unstable();
    assert_eq!(expirations.len(), 2, "both messages kept");
    assert!(
        (2_999_000_000..=3_000_000_000).contains(&expirations[0]),
        "{expirations:?}"
    );
    assert!(expirations[1] >= 4_294_966_000_000, "{expirations:?}");
}

/// A Maximum Packet Size is an unsigned four-byte count: the largest there
/// is lets every message through.
#[tokio::test]
async fn the_largest_maximum_packet_size_takes_every_message() {
    let (_broker, address, _task) = start(None).await;
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 60, 5, 0x27];
    body.extend(u32::MAX.to_be_bytes());
    body.extend(string("max-size"));
    let (mut c, _) = Client::open_with(address, &packet(0x10, &body)).await;
    let mut sub = vec![0, 1, 0];
    sub.extend(string("sized/t"));
    sub.push(0);
    c.send(&packet(0x82, &sub)).await;
    assert_eq!(c.packet().await.expect("a SUBACK")[0], 0x90);
    let (mut publisher, _) = Client::open(address, "publisher", true).await;
    let mut publish = string("sized/t");
    publish.extend(b"hello");
    publisher.send(&packet(0x30, &publish)).await;
    let p = c.packet().await.expect("the message");
    assert_eq!(p[0] & 0xF0, 0x30, "a PUBLISH");
    assert!(p.ends_with(b"hello"));
}
