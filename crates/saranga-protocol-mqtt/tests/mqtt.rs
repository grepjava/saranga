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

    /// SUBSCRIBE without asserting success; returns the SUBACK packet so a
    /// caller can read the granted-QoS-or-failure code.
    async fn subscribe_raw(&mut self, filter: &str, qos: u8) -> Vec<u8> {
        let mut body = vec![0, 1];
        body.extend(string(filter));
        body.push(qos);
        self.send(&packet(0x82, &body)).await;
        self.packet().await.unwrap()
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

/// A DISCONNECT with reason 0x04 is a clean disconnect that still asks for
/// the will to be published (5.0 3.14.2.1).
#[tokio::test]
async fn a_disconnect_with_reason_4_publishes_the_will() {
    let (broker, address, _task) = start(None).await;
    let (mut watcher, _) = Client::open(address, "watcher", true).await;
    watcher.subscribe("wills/t", 0).await;
    // MQTT 5, clean start, will flag; empty CONNECT and will properties.
    let (mut willing, _) = Client::open_with(
        address,
        &connect5("willing", true, 0, Some(("wills/t", b"bye"))),
    )
    .await;
    // 0xE0 0x01 0x04: DISCONNECT with reason 0x04.
    willing.send(&[0xE0, 0x01, 0x04]).await;
    let p = watcher.packet().await.expect("the will after reason 0x04");
    assert_eq!(p[0] & 0xF0, 0x30, "a PUBLISH");
    assert!(p.ends_with(b"bye"), "{p:?}");
    broker.stop().unwrap();
}

/// A normal DISCONNECT (reason 0x00) still suppresses the will.
#[tokio::test]
async fn a_normal_disconnect_keeps_the_will_back() {
    let (broker, address, _task) = start(None).await;
    let (mut watcher, _) = Client::open(address, "watcher", true).await;
    watcher.subscribe("wills/t", 0).await;
    let (mut willing, _) = Client::open_with(
        address,
        &connect5("willing2", true, 0, Some(("wills/t", b"bye"))),
    )
    .await;
    willing.send(&[0xE0, 0x00]).await; // reason 0x00
    assert!(
        watcher.silent_for(500).await,
        "the will should not be published"
    );
    broker.stop().unwrap();
}

/// A session expiry interval with the high bit set is a large unsigned count
/// of seconds, not a negative that never expires.
#[tokio::test]
async fn a_high_bit_session_expiry_still_expires() {
    use saranga_protocol_mqtt::state::StateManager;
    let (broker, _address, _task) = start(None).await;
    let manager = StateManager::new(&broker, false, "scan".into());
    let state = manager.session_state("expired");
    {
        let mut s = state.lock().unwrap();
        s.client_session_expiry_interval = 0x8000_0000u32 as i32; // ~68 years
        s.attached = false;
        s.disconnected_time =
            saranga_server::message::now_millis() - (i64::from(0x8000_0000u32) + 1) * 1000;
    }
    manager.scan_sessions().await;
    assert!(
        !manager.has_session_state("expired"),
        "a finite (if large) expiry that has passed should remove the session"
    );
    // 0xFFFFFFFF (-1) never expires.
    let never = manager.session_state("never");
    {
        let mut s = never.lock().unwrap();
        s.client_session_expiry_interval = -1;
        s.attached = false;
        s.disconnected_time = 0;
    }
    manager.scan_sessions().await;
    assert!(
        manager.has_session_state("never"),
        "0xFFFFFFFF never expires"
    );
    broker.stop().unwrap();
}

/// An MQTT 5 CONNECT with a raw property block and, maybe, a QoS 0 will.
fn connect5_props(client_id: &str, props: &[u8], will: Option<(&str, &[u8])>) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5];
    body.push(0x02 | if will.is_some() { 0x04 } else { 0 }); // clean start
    body.extend([0, 60]);
    body.push(props.len() as u8);
    body.extend_from_slice(props);
    body.extend(string(client_id));
    if let Some((topic, message)) = will {
        body.push(0); // will properties
        body.extend(string(topic));
        body.extend((message.len() as u16).to_be_bytes());
        body.extend_from_slice(message);
    }
    packet(0x10, &body)
}

/// A SUBSCRIBE with a raw subscription-options byte (QoS, No Local, Retain As
/// Published, Retain Handling).
fn subscribe_option(filter: &str, option: u8) -> Vec<u8> {
    let mut body = vec![0, 1, 0];
    body.extend(string(filter));
    body.push(option);
    packet(0x82, &body)
}

/// A publication discarded for its size leaves no topic alias behind: the
/// next publication on the topic still carries the full topic that defines
/// the alias.
#[tokio::test]
async fn a_discarded_publication_leaves_no_topic_alias() {
    let (broker, address, _task) = start(None).await;
    // Maximum Packet Size 40, Topic Alias Maximum 1.
    let (mut sub, _) = Client::open_with(
        address,
        &connect5_props("s", &[0x27, 0, 0, 0, 40, 0x22, 0, 1], None),
    )
    .await;
    sub.send(&subscribe_option("topic", 0)).await;
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);
    let (mut publisher, _) = Client::open(address, "p", true).await;
    // Over 40 bytes: discarded. Then a small one that is sent.
    publisher
        .send(&packet(0x30, &{
            let mut b = string("topic");
            b.extend(vec![b'x'; 100]);
            b
        }))
        .await;
    publisher
        .send(&packet(0x30, &{
            let mut b = string("topic");
            b.extend_from_slice(b"ok");
            b
        }))
        .await;
    let p = sub.packet().await.expect("the ok publication");
    // Topic length 5 ("topic"), i.e. not aliased away to empty, and it still
    // carries alias 1 as the defining packet.
    assert_eq!(&p[2..4], &[0, 5], "the full topic defines the alias: {p:?}");
    assert!(
        p.windows(3).any(|w| w == [0x23, 0, 1]),
        "alias 1 present: {p:?}"
    );
    broker.stop().unwrap();
}

/// Acknowledging on one subscription frees the connection's send slot and
/// wakes a message waiting on another subscription.
#[tokio::test]
async fn an_ack_wakes_a_message_on_another_subscription() {
    let (broker, address, _task) = start(None).await;
    // Receive Maximum 1.
    let (mut sub, _) = Client::open_with(address, &connect5_props("s", &[0x21, 0, 1], None)).await;
    sub.send(&subscribe_option("a", 1)).await;
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);
    sub.send(&subscribe_option("b", 1)).await;
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);
    let (mut publisher, _) = Client::open(address, "p", true).await;
    publisher.publish("a", b"one", 1).await;
    let first = sub.packet().await.expect("a/one");
    let (_, first_id, _) = parse_publish(&first);
    publisher.publish("b", b"two", 1).await;
    // Only one slot: b/two waits.
    assert!(sub.silent_for(200).await);
    // Acknowledge a/one; the freed slot must let b/two through.
    sub.send(&packet(0x40, &first_id.to_be_bytes())).await;
    let p = sub.packet().await.expect("b/two after the ack");
    assert!(parse_publish(&p).2.ends_with(b"two"), "{p:?}");
    broker.stop().unwrap();
}

/// A wildcard subscription with Retain As Published keeps the RETAIN flag on
/// a live retained publication.
#[tokio::test]
async fn a_wildcard_retain_as_published_keeps_the_flag() {
    let (broker, address, _task) = start(None).await;
    let (mut sub, _) = Client::open_with(address, &connect5_props("s", &[], None)).await;
    sub.send(&subscribe_option("a/#", 0x08)).await; // Retain As Published
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);
    let (mut publisher, _) = Client::open(address, "p", true).await;
    // A retained QoS 0 publish to a/b.
    publisher
        .send(&packet(0x31, &{
            let mut b = string("a/b");
            b.extend_from_slice(b"retained");
            b
        }))
        .await;
    let p = sub.packet().await.expect("the publication");
    assert_eq!(p[0] & 0xF0, 0x30, "a PUBLISH");
    assert_eq!(p[0] & 0x01, 0x01, "RETAIN kept: {p:?}");
    broker.stop().unwrap();
}

/// A client takeover publishes the replaced client's immediate will.
#[tokio::test]
async fn a_takeover_publishes_the_immediate_will() {
    let (broker, address, _task) = start(None).await;
    let (mut watcher, _) = Client::open(address, "watcher", true).await;
    watcher.subscribe("will/t", 0).await;
    let (_old, _) =
        Client::open_with(address, &connect5_props("p", &[], Some(("will/t", b"bye")))).await;
    // A second connection with the same client ID takes over.
    let (_replacement, _) = Client::open_with(address, &connect5_props("p", &[], None)).await;
    let p = watcher.packet().await.expect("the will on takeover");
    assert_eq!(p[0] & 0xF0, 0x30, "a PUBLISH");
    assert!(p.ends_with(b"bye"), "{p:?}");
    broker.stop().unwrap();
}

/// The outgoing Maximum Packet Size check counts every property length prefix,
/// so a packet whose true encoded size exceeds the client's limit is discarded
/// even when Artemis's undercounting estimate would have let it through.
#[tokio::test]
async fn an_oversized_publication_counts_property_lengths_and_is_discarded() {
    let (broker, address, _task) = start(None).await;
    // Maximum Packet Size 25.
    let (mut sub, _) =
        Client::open_with(address, &connect5_props("s", &[0x27, 0, 0, 0, 25], None)).await;
    sub.send(&subscribe_option("orders", 1)).await;
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);
    // A v5 publisher sends a QoS 1 PUBLISH with a User Property key=value and
    // payload "hello". Rebuilt for the subscriber this encodes to 31 bytes;
    // undercounted (as Artemis does) it is 25 and would be sent.
    let (mut publisher, _) = Client::open_with(address, &connect5_props("p", &[], None)).await;
    let mut props = vec![0x26]; // User Property
    props.extend(string("key"));
    props.extend(string("value"));
    let mut body = string("orders");
    body.extend(1u16.to_be_bytes());
    body.push(props.len() as u8);
    body.extend(props);
    body.extend_from_slice(b"hello");
    publisher.send(&packet(0x32, &body)).await;
    assert_eq!(
        publisher.packet().await.unwrap()[0],
        0x40,
        "the publisher is acknowledged"
    );
    assert!(
        sub.silent_for(400).await,
        "the oversized delivery is discarded, so the subscriber receives nothing"
    );
    broker.stop().unwrap();
}

/// A large Message Expiry Interval is forwarded whole. The remaining seconds
/// are an unsigned value kept by its bits in the codec's signed field, not
/// saturated at i32::MAX.
#[tokio::test]
async fn a_large_outgoing_expiry_interval_is_forwarded_whole() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version, prop};
    let (broker, address, _task) = start(None).await;
    let (mut sub, _) = Client::open_with(address, &connect5_props("s", &[], None)).await;
    sub.send(&subscribe_option("orders", 1)).await;
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);
    // A v5 QoS 1 PUBLISH with Message Expiry Interval 0xFFFFFFFF, payload "hi".
    let (mut publisher, _) = Client::open_with(address, &connect5_props("p", &[], None)).await;
    let mut body = string("orders");
    body.extend(1u16.to_be_bytes());
    let props = [0x02, 0xFF, 0xFF, 0xFF, 0xFF]; // Message Expiry Interval
    body.push(props.len() as u8);
    body.extend_from_slice(&props);
    body.extend_from_slice(b"hi");
    publisher.send(&packet(0x32, &body)).await;
    assert_eq!(publisher.packet().await.unwrap()[0], 0x40, "a PUBACK");
    let mut bytes = sub.packet().await.expect("the forwarded publication");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut bytes)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    let forwarded = msg.properties.int(prop::MESSAGE_EXPIRY_INTERVAL).unwrap() as u32;
    assert!(
        forwarded > i32::MAX as u32,
        "the unsigned interval is kept whole, not saturated to i32::MAX: {forwarded}"
    );
    broker.stop().unwrap();
}

/// Receive Maximum bounds QoS 1 and 2 PUBLISH whatever their persistence: a
/// non-durable Core message delivered at QoS 1 counts against the quota, so a
/// second is held until the first is acknowledged.
#[tokio::test]
async fn nonpersistent_core_messages_respect_receive_maximum() {
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    let (broker, address, _task) = start(None).await;
    // Receive Maximum 1.
    let (mut sub, _) =
        Client::open_with(address, &connect5_props("quota", &[0x21, 0, 1], None)).await;
    sub.send(&subscribe_option("orders", 1)).await;
    assert_eq!(sub.packet().await.unwrap()[0], 0x90);

    let conn = Arc::new(ClientConnection::new(
        Transport::internal("probe"),
        Arc::new(saranga_security::ConnectionInfo::default()),
        vec![],
    ));
    let mut producer = ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "core-producer".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    for _ in 0..2 {
        producer
            .send(
                saranga_message::CoreMessage {
                    address: Some("orders".into()),
                    durable: false,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        producer.take_completion().wait().await.unwrap();
    }
    // The first PUBLISH arrives; the second is held by the single quota slot.
    let first = sub.packet().await.expect("the first PUBLISH");
    assert_eq!(first[0] & 0xF0, 0x30, "a PUBLISH");
    assert!(
        sub.silent_for(300).await,
        "the second is held until the first is acknowledged"
    );
    // Acknowledge the first; the freed slot lets the second through.
    let (_, id, _) = parse_publish(&first);
    sub.send(&packet(0x40, &id.to_be_bytes())).await;
    let second = sub
        .packet()
        .await
        .expect("the second PUBLISH after the ack");
    assert_eq!(second[0] & 0xF0, 0x30, "a PUBLISH");
    broker.stop().unwrap();
}

/// A will keeps its MQTT 5 publication properties: when published, the will
/// carries its Content Type and a Message Expiry Interval (started at publish),
/// not an empty property block.
#[tokio::test]
async fn a_will_keeps_its_expiry_and_content_properties() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version, prop};
    let (broker, address, _task) = start(None).await;
    let (mut watcher, _) = Client::open_with(address, &connect5_props("w", &[], None)).await;
    watcher.send(&subscribe_option("will/t", 0)).await;
    assert_eq!(watcher.packet().await.unwrap()[0], 0x90);

    // CONNECT v5 with a will on will/t carrying will properties:
    // Message Expiry Interval 60 and Content Type "application/json".
    let mut will_props = vec![0x02, 0, 0, 0, 60]; // Message Expiry Interval = 60
    will_props.push(0x03); // Content Type
    will_props.extend(string("application/json"));
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5];
    body.push(0x02 | 0x04); // clean start + will flag
    body.extend([0, 60]);
    body.push(0); // CONNECT properties
    body.extend(string("willer"));
    body.push(will_props.len() as u8); // will properties length
    body.extend(will_props);
    body.extend(string("will/t"));
    body.extend((b"bye".len() as u16).to_be_bytes());
    body.extend_from_slice(b"bye");
    let (mut willer, _) = Client::open_with(address, &packet(0x10, &body)).await;

    // DISCONNECT with reason 0x04 publishes the will at once.
    willer.send(&[0xE0, 1, 4]).await;

    let mut bytes = watcher.packet().await.expect("the will publication");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut bytes)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    assert_eq!(
        msg.properties.string(prop::CONTENT_TYPE),
        Some("application/json"),
        "the will keeps its Content Type"
    );
    assert!(
        msg.properties.int(prop::MESSAGE_EXPIRY_INTERVAL).is_some(),
        "the will keeps a Message Expiry Interval, started at publish"
    );
    assert!(msg.payload.ends_with(b"bye"), "the will payload");
    broker.stop().unwrap();
}

/// Receive Maximum is reserved atomically, so one publication fanning out to
/// many overlapping subscription queues at once still puts only one QoS 1
/// PUBLISH in flight until it is acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_subscription_queues_respect_receive_maximum() {
    let (broker, address, _task) = start(None).await;
    let (mut s, _) = Client::open_with(address, &connect5_props("rq", &[0x21, 0, 1], None)).await;
    // 128 overlapping filters that all match one topic, so a single publish
    // dispatches on many subscription queues concurrently.
    for mask in 0..128u32 {
        let filter = (0..7)
            .map(|i| if mask & (1 << i) == 0 { "a" } else { "+" })
            .collect::<Vec<_>>()
            .join("/");
        s.send(&subscribe_option(&filter, 1)).await;
        assert_eq!(s.packet().await.unwrap()[0], 0x90);
    }
    let (mut p, _) = Client::open(address, "pp", true).await;
    p.publish("a/a/a/a/a/a/a", b"hello", 1).await;
    // Exactly one PUBLISH is in flight; the rest wait for a PUBACK.
    let first = s.packet().await.expect("the first PUBLISH");
    assert_eq!(first[0] & 0xF0, 0x30, "a PUBLISH");
    assert!(
        s.silent_for(500).await,
        "Receive Maximum 1 holds the rest until a PUBACK"
    );
    broker.stop().unwrap();
}

/// A successful QoS 2 exchange holds its Receive Maximum slot until PUBCOMP: a
/// PUBREC frees nothing, and the next publication waits until the client
/// completes the exchange.
#[tokio::test]
async fn a_qos2_slot_is_held_until_pubcomp() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version};
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    let (broker, address, _task) = start(None).await;
    let (mut s, _) = Client::open_with(address, &connect5_props("q2", &[0x21, 0, 1], None)).await;
    s.send(&subscribe_option("orders", 2)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);

    // A Core producer gives these deliveries effective QoS 2.
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("probe"),
        Arc::new(saranga_security::ConnectionInfo::default()),
        vec![],
    ));
    let mut producer = ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "q2-producer".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    for _ in 0..2 {
        producer
            .send(
                saranga_message::CoreMessage {
                    address: Some("orders".into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        producer.take_completion().wait().await.unwrap();
    }

    // The first QoS 2 PUBLISH; the second is held by the single quota slot.
    let mut first = s.packet().await.expect("the first PUBLISH");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut first)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    assert_eq!(msg.qos, 2, "QoS 2");
    assert!(s.silent_for(200).await, "the second waits");
    let pid = msg.packet_id as u16;

    // PUBREC: the server replies with PUBREL, and the slot is still held.
    s.send(&packet(0x50, &pid.to_be_bytes())).await;
    let pubrel = s.packet().await.expect("a PUBREL");
    assert_eq!(
        pubrel[0] & 0xF0,
        0x60,
        "PUBREL, not a second PUBLISH: {pubrel:?}"
    );
    assert!(
        s.silent_for(300).await,
        "the slot is held until PUBCOMP, so no second PUBLISH yet"
    );

    // PUBCOMP frees the slot; the second PUBLISH now arrives.
    s.send(&packet(0x70, &pid.to_be_bytes())).await;
    let second = s.packet().await.expect("the second PUBLISH after PUBCOMP");
    assert_eq!(second[0] & 0xF0, 0x30, "a PUBLISH: {second:?}");
    broker.stop().unwrap();
}

/// A Core producer session for giving MQTT subscribers effective QoS 1/2
/// deliveries.
async fn core_producer(broker: &Arc<Broker>, name: &str) -> saranga_server::session::ServerSession {
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("probe"),
        Arc::new(saranga_security::ConnectionInfo::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: name.into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

/// Sends one Core message and waits for it to be routed.
async fn send_core(
    producer: &mut saranga_server::session::ServerSession,
    message: saranga_message::CoreMessage,
) {
    producer.send(message, None).await.unwrap();
    producer.take_completion().wait().await.unwrap();
}

/// A resumed session's PUBCOMP for a QoS 2 exchange that outlived its
/// connection releases no Receive Maximum slot: the durable PUBREC cache is
/// not the new connection's credit, so completing an old exchange cannot push
/// the quota below zero and let an extra PUBLISH slip past Receive Maximum.
#[tokio::test]
async fn a_resumed_qos2_pubcomp_grants_no_extra_credit() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version};
    let (broker, address, _task) = start(None).await;

    // Receive Maximum 1 and a session expiry, so the session and its PUBREC
    // cache survive a reconnect.
    let props: &[u8] = &[0x11, 0, 0, 1, 0x2C, 0x21, 0, 1];
    let connect = |clean: bool| {
        let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5];
        body.push(if clean { 0x02 } else { 0x00 });
        body.extend([0, 60]);
        body.push(props.len() as u8);
        body.extend_from_slice(props);
        body.extend(string("q2resume"));
        packet(0x10, &body)
    };

    let (mut s, _) = Client::open_with(address, &connect(true)).await;
    s.send(&subscribe_option("orders", 2)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);

    let mut producer = core_producer(&broker, "q2resume-producer").await;
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("orders".into()),
            ..Default::default()
        },
    )
    .await;

    // Receive the QoS 2 PUBLISH and take the exchange as far as PUBREC/PUBREL.
    let mut first = s.packet().await.expect("the first PUBLISH");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut first)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    assert_eq!(msg.qos, 2, "QoS 2");
    let pid = msg.packet_id as u16;
    s.send(&packet(0x50, &pid.to_be_bytes())).await; // PUBREC
    assert_eq!(
        s.packet().await.expect("a PUBREL")[0] & 0xF0,
        0x60,
        "PUBREL"
    );

    // Disconnect without PUBCOMP; the slot is reset but the PUBREC cache
    // survives for the resumable session.
    s.send(&[0xE0, 0]).await; // DISCONNECT, normal
    drop(s);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Resume; the old exchange's PUBREL is retransmitted.
    let (mut s, _) = Client::open_with(address, &connect(false)).await;
    let pubrel = s.packet().await.expect("the retransmitted PUBREL");
    assert_eq!(pubrel[0] & 0xF0, 0x60, "PUBREL: {pubrel:?}");
    assert_eq!(
        u16::from_be_bytes([pubrel[2], pubrel[3]]),
        pid,
        "for the old exchange"
    );

    // Two new messages arrive at the resumed subscription; Receive Maximum 1
    // puts exactly one PUBLISH in flight.
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("orders".into()),
            ..Default::default()
        },
    )
    .await;
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("orders".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        s.packet().await.expect("one new PUBLISH")[0] & 0xF0,
        0x30,
        "a PUBLISH"
    );
    assert!(s.silent_for(300).await, "the second waits for a slot");

    // Completing the old exchange must not free a slot the new connection
    // never reserved: the second new PUBLISH stays held.
    s.send(&packet(0x70, &pid.to_be_bytes())).await; // PUBCOMP for the old pid
    assert!(
        s.silent_for(400).await,
        "a resumed PUBCOMP grants no extra credit, so no second PUBLISH"
    );
    broker.stop().unwrap();
}

/// A PUBACK that arrives after its subscription was unsubscribed still frees
/// the connection's Receive Maximum slot, so the connection's other
/// subscriptions keep delivering instead of stalling until reconnect.
#[tokio::test]
async fn an_ack_after_unsubscribe_frees_the_slot() {
    let (broker, address, _task) = start(None).await;
    let (mut s, _) =
        Client::open_with(address, &connect5_props("unsub", &[0x21, 0, 1], None)).await;
    s.send(&subscribe_option("orders", 1)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);
    s.send(&subscribe_option("other", 1)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);

    // A QoS 1 message on `orders` takes the single slot.
    let (mut p, _) = Client::open(address, "pub", true).await;
    p.publish("orders", b"first", 1).await;
    let first = s.packet().await.expect("the orders PUBLISH");
    assert_eq!(first[0] & 0xF0, 0x30, "a PUBLISH");
    let (_, id, _) = parse_publish(&first);

    // Unsubscribe from `orders`, closing its consumer, before acknowledging.
    let mut unsub = vec![0, 2, 0];
    unsub.extend(string("orders"));
    s.send(&packet(0xA2, &unsub)).await;
    assert_eq!(s.packet().await.expect("an UNSUBACK")[0], 0xB0);

    // Acknowledge the in-flight message whose consumer is now gone.
    s.send(&packet(0x40, &id.to_be_bytes())).await;

    // The freed slot lets `other` deliver.
    p.publish("other", b"second", 2).await;
    let second = s
        .packet()
        .await
        .expect("the other PUBLISH once the slot frees");
    assert_eq!(second[0] & 0xF0, 0x30, "a PUBLISH");
    broker.stop().unwrap();
}

/// A Core TEXT message with a null body reaches the client as an empty MQTT
/// payload rather than failing the publish: its Receive Maximum slot is spent
/// and freed normally, so a later message on the connection is not stranded
/// behind a leaked slot.
#[tokio::test]
async fn a_null_text_body_is_delivered_empty_and_frees_its_slot() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version};
    let (broker, address, _task) = start(None).await;
    let (mut s, _) =
        Client::open_with(address, &connect5_props("nulltext", &[0x21, 0, 1], None)).await;
    s.send(&subscribe_option("orders", 1)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);

    let mut producer = core_producer(&broker, "null-producer").await;
    // A TEXT message whose body is a null nullable-SimpleString (marker 0).
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("orders".into()),
            message_type: saranga_message::message_type::TEXT,
            body: vec![0u8].into(),
            ..Default::default()
        },
    )
    .await;
    // A following ordinary message.
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("orders".into()),
            body: b"second".to_vec().into(),
            ..Default::default()
        },
    )
    .await;

    // The null-text message arrives with an empty payload.
    let mut first = s.packet().await.expect("the null-text PUBLISH");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut first)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    assert!(
        msg.payload.is_empty(),
        "null text becomes an empty payload: {:?}",
        msg.payload
    );

    // Its slot frees on PUBACK, so the second message is delivered.
    s.send(&packet(0x40, &(msg.packet_id as u16).to_be_bytes()))
        .await;
    let second = s
        .packet()
        .await
        .expect("the second PUBLISH once the slot frees");
    assert_eq!(second[0] & 0xF0, 0x30, "a PUBLISH");
    broker.stop().unwrap();
}

/// Overlapping subscriptions keep their packet IDs across a reconnect. One
/// message routed to two overlapping subscriptions is delivered twice, each
/// exchange with its own packet ID; after a reconnect without acknowledging
/// either, the same two IDs come back rather than one being reused and a
/// fresh one generated for the other.
#[tokio::test]
async fn overlapping_subscriptions_keep_their_packet_ids_on_reconnect() {
    let (broker, address, _task) = start(None).await;
    let (mut s, _) = Client::open(address, "overlap", false).await;
    s.subscribe("review/+", 1).await;
    s.subscribe("review/t", 1).await;
    let (mut p, _) = Client::open(address, "pub", true).await;
    p.publish("review/t", b"m", 1).await;
    // One delivery per overlapping subscription, each with its own packet ID.
    let (_, id1, _) = parse_publish(&s.packet().await.expect("the first delivery"));
    let (_, id2, _) = parse_publish(&s.packet().await.expect("the second delivery"));
    let mut before = [id1, id2];
    before.sort();

    // Reconnect without acknowledging either delivery.
    drop(s);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (mut s, connack) = Client::open(address, "overlap", false).await;
    assert_eq!(connack[2], 1, "the session is present");
    let (_, r1, _) = parse_publish(&s.packet().await.expect("the first redelivery"));
    let (_, r2, _) = parse_publish(&s.packet().await.expect("the second redelivery"));
    let mut after = [r1, r2];
    after.sort();
    assert_eq!(
        after, before,
        "the redelivered packet IDs are the originals, not a reused ID and a fresh one"
    );
    broker.stop().unwrap();
}

/// A Session Expiry Interval lowered on DISCONNECT is persisted: after a
/// restart the session expires on the revised interval, not the one from
/// CONNECT.
#[tokio::test]
async fn a_disconnect_persists_a_shortened_session_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, task) = start_scanning(Some(dir.path()), 20).await;
    // CONNECT v5 with Session Expiry Interval 60.
    let (mut c, _) = Client::open_with(address, &connect5("exp", true, 60, None)).await;
    // DISCONNECT lowering the expiry to 1 second.
    c.send(&packet(0xE0, &[0x00, 5, 0x11, 0, 0, 0, 1])).await;
    assert_eq!(c.packet().await, None);
    tokio::time::sleep(Duration::from_millis(200)).await;
    broker.stop().unwrap();
    task.abort();
    drop(broker);

    // Restart and wait past the revised one-second expiry.
    let (broker2, address, _task) = start_scanning(Some(dir.path()), 20).await;
    tokio::time::sleep(Duration::from_millis(1400)).await;
    let (_c, connack) = Client::open_with(address, &connect5("exp", false, 1, None)).await;
    assert_eq!(
        connack[2], 0,
        "the one-second expiry elapsed, so no session is present"
    );
    broker2.stop().unwrap();
}

/// A persistent session survives unsubscribing from its last topic: an empty
/// subscription set does not end the session, so after a restart the session
/// is still present.
#[tokio::test]
async fn a_persistent_session_survives_its_final_unsubscribe() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, task) = start(Some(dir.path())).await;
    let (mut c, _) = Client::open(address, "keep", false).await;
    c.subscribe("review/t", 1).await;
    // Unsubscribe from the only topic (MQTT 3.1.1 UNSUBSCRIBE: no properties).
    let mut unsub = vec![0, 1];
    unsub.extend(string("review/t"));
    c.send(&packet(0xA2, &unsub)).await;
    assert_eq!(c.packet().await.expect("an UNSUBACK")[0], 0xB0);
    c.send(&[0xE0, 0]).await;
    assert_eq!(c.packet().await, None);
    tokio::time::sleep(Duration::from_millis(200)).await;
    broker.stop().unwrap();
    task.abort();
    drop(broker);

    let (broker2, address, _task) = start(Some(dir.path())).await;
    let (_c, connack) = Client::open(address, "keep", false).await;
    assert_eq!(
        connack[2], 1,
        "the persistent session survives its final unsubscribe"
    );
    broker2.stop().unwrap();
}

/// A Core message whose address cannot be represented as an MQTT topic (a
/// literal '+') is discarded, not left to stall the connection: its Receive
/// Maximum slot frees, so a following deliverable message arrives.
#[tokio::test]
async fn an_unrepresentable_core_topic_is_discarded_and_frees_its_slot() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version};
    let (broker, address, _task) = start(None).await;
    let (mut s, _) =
        Client::open_with(address, &connect5_props("badtopic", &[0x21, 0, 1], None)).await;
    s.send(&subscribe_option("#", 1)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);

    let mut producer = core_producer(&broker, "badtopic-producer").await;
    // A Core address with a literal '+' is an MQTT wildcard character, so the
    // topic cannot be published to this subscriber.
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("orders+east".into()),
            body: b"dropped".to_vec().into(),
            ..Default::default()
        },
    )
    .await;
    // A following deliverable message on an ordinary address.
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("other".into()),
            body: b"second".to_vec().into(),
            ..Default::default()
        },
    )
    .await;

    // Only the deliverable message arrives — the unrepresentable one was
    // discarded and freed its slot rather than stranding the connection.
    let mut first = s.packet().await.expect("the deliverable PUBLISH");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut first)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    assert_eq!(
        msg.topic, "other",
        "the deliverable message, not the dropped one"
    );
    assert_eq!(msg.payload, b"second");
    broker.stop().unwrap();
}

/// A Core address too long for an MQTT topic's two-byte length is discarded
/// rather than encoded with a wrapped length into a malformed packet: the slot
/// frees and the next deliverable message arrives intact.
#[tokio::test]
async fn a_core_topic_over_the_wire_length_is_discarded() {
    use saranga_protocol_mqtt::codec::{Decoder, Packet, Version};
    let (broker, address, _task) = start(None).await;
    let (mut s, _) =
        Client::open_with(address, &connect5_props("longtopic", &[0x21, 0, 1], None)).await;
    s.send(&subscribe_option("#", 1)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);

    let mut producer = core_producer(&broker, "longtopic-producer").await;
    // 65,536 bytes: one past the u16 topic-length limit.
    let long = "a".repeat(65_536);
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some(long.into()),
            body: b"dropped".to_vec().into(),
            ..Default::default()
        },
    )
    .await;
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("other".into()),
            body: b"second".to_vec().into(),
            ..Default::default()
        },
    )
    .await;

    // The over-long topic is discarded; the next message arrives, decoding
    // cleanly (no wrapped topic length corrupting the packet).
    let mut first = s.packet().await.expect("the deliverable PUBLISH");
    let Packet::Publish(msg) = Decoder::for_version(Version::V5)
        .decode(&mut first)
        .unwrap()
        .unwrap()
    else {
        panic!("a PUBLISH");
    };
    assert_eq!(
        msg.topic, "other",
        "the deliverable message, not the dropped one"
    );
    assert_eq!(msg.payload, b"second");
    broker.stop().unwrap();
}

/// A session the scan chose to expire, but to which a client has since
/// reconnected (marking its state attached), is not removed: the re-check and
/// removal are one atomic step, so an active client keeps its session and
/// queues.
#[tokio::test]
async fn a_reattached_session_is_not_expired() {
    use saranga_protocol_mqtt::state::{StateManager, lock};
    let (broker, _address, _task) = start(None).await;
    let mgr = StateManager::new(&broker, false, String::new());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // An expiry-eligible session: detached, a one-second expiry, long gone.
    let attached = mgr.session_state("attached");
    {
        let mut s = lock(&attached);
        s.client_session_expiry_interval = 1;
        s.disconnected_time = now - 10_000;
        s.attached = false;
    }
    // A client reconnects to it.
    let (_state, present, _prev) = mgr.attach_session("attached");
    assert!(present, "the session already existed");
    // The scan must not claim it now that it is attached.
    assert!(
        mgr.claim_expired("attached", now).is_none(),
        "an attached session is not expired"
    );
    assert!(mgr.has_session_state("attached"), "it is still present");

    // A still-detached, expired session is claimed and removed.
    let gone = mgr.session_state("gone");
    {
        let mut s = lock(&gone);
        s.client_session_expiry_interval = 1;
        s.disconnected_time = now - 10_000;
        s.attached = false;
    }
    assert!(
        mgr.claim_expired("gone", now).is_some(),
        "a detached expired session is removed"
    );
    assert!(!mgr.has_session_state("gone"));
    broker.stop().unwrap();
}

/// ISSUE 68: the core subscription queue name is `<client>.<address>`, so two
/// distinct (client, topic) pairs can collide (client `a.b` + topic `c` and
/// client `a` + topic `b/c` both name the queue `a.b.c`). Artemis reuses the
/// existing queue without checking its address and serves one client the
/// other's messages; Saranga refuses the colliding subscription (SUBACK 0x80)
/// so it cannot be mis-delivered, while a non-colliding subscription is granted.
#[tokio::test]
async fn a_colliding_subscription_queue_name_is_refused() {
    let (broker, address, _task) = start(None).await;

    // Client `a.b` subscribes to `c` -> durable-enough queue `a.b.c` on
    // address `c`; it stays connected so the queue exists.
    let (mut ab, _) = Client::open(address, "a.b", false).await;
    ab.subscribe("c", 0).await;

    // Client `a` subscribes to `b/c` -> the same queue name `a.b.c`, but its
    // address is `b.c`, not `c`: the subscription is refused.
    let (mut a, _) = Client::open(address, "a", false).await;
    assert_eq!(
        a.subscribe_raw("b/c", 0).await,
        vec![0x90, 3, 0, 1, 0x80],
        "a colliding queue name is refused, not silently reused"
    );

    // A client whose name does not collide subscribes to `b/c` normally.
    let (mut z, _) = Client::open(address, "zed", false).await;
    assert_eq!(
        z.subscribe_raw("b/c", 0).await,
        vec![0x90, 3, 0, 1, 0],
        "a non-colliding subscription is granted"
    );

    broker.stop().unwrap();
}
