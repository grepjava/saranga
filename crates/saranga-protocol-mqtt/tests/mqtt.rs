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
        let mut body = string(topic);
        body.extend(packet_id.to_be_bytes());
        body.extend_from_slice(payload);
        self.send(&packet(0x32, &body)).await;
        let puback = self.packet().await.unwrap();
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
