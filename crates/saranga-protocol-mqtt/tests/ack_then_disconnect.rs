//! A PUBREC or PUBACK followed at once by DISCONNECT keeps what the
//! acknowledgement's transaction holds: the queue's answer is handled before
//! the session goes (Artemis commits it with the acknowledgement), so the
//! QoS 2 exchange gets its PUBREL on resumption and a QoS 1 packet ID's
//! record goes.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_mqtt::{MqttConfig, MqttProtocol};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start() -> (Arc<Broker>, std::net::SocketAddr, Arc<MqttProtocol>) {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    let proto = Arc::new(MqttProtocol::new(MqttConfig {
        session_scan_interval: 5000,
        ..MqttConfig::default()
    }));
    config.protocols = vec![proto.clone()];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn({
        let broker = broker.clone();
        async move {
            let _ = broker.serve_acceptor(listener, Acceptor::default()).await;
        }
    });
    (broker, address, proto)
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

fn subscribe3(filter: &str, qos: u8) -> Vec<u8> {
    let mut body = vec![0, 1];
    body.extend(string(filter));
    body.push(qos);
    packet(0x82, &body)
}

struct Client(TcpStream);

impl Client {
    async fn open_with(address: std::net::SocketAddr, connect: &[u8]) -> (Self, Vec<u8>) {
        let mut c = Self(TcpStream::connect(address).await.unwrap());
        c.send(connect).await;
        let connack = c.packet().await.expect("CONNACK");
        (c, connack)
    }
    async fn send(&mut self, bytes: &[u8]) {
        self.0.write_all(bytes).await.unwrap();
    }
    async fn packet(&mut self) -> Option<Vec<u8>> {
        self.packet_within(5000).await
    }
    async fn packet_within(&mut self, ms: u64) -> Option<Vec<u8>> {
        tokio::time::timeout(Duration::from_millis(ms), async {
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
        .unwrap_or(None)
    }
}

/// Packet ID of a PUBLISH (QoS > 0).
fn publish_id(p: &[u8]) -> u16 {
    let mut i = 1;
    while p[i] & 0x80 != 0 {
        i += 1;
    }
    i += 1;
    let topic_len = usize::from(u16::from_be_bytes([p[i], p[i + 1]]));
    i += 2 + topic_len;
    u16::from_be_bytes([p[i], p[i + 1]])
}

/// Payload of a QoS 0 PUBLISH (3.1.1).
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

async fn send_core(
    producer: &mut saranga_server::session::ServerSession,
    message: saranga_message::CoreMessage,
) {
    producer.send(message, None).await.unwrap();
    producer.take_completion().wait().await.unwrap();
}

/// A Core message whose MQTT user-property names do not parse fails the
/// publish after the Receive Maximum slot was reserved: the slot leaks and
/// the next message never comes (sibling of the null-text-body fix).
async fn start_persistent(
    dir: &std::path::Path,
) -> (
    Arc<Broker>,
    std::net::SocketAddr,
    Arc<MqttProtocol>,
    tokio::task::JoinHandle<()>,
) {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let mut config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let proto = Arc::new(MqttProtocol::default());
    config.protocols = vec![proto.clone()];
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn({
        let broker = broker.clone();
        async move {
            let _ = broker.serve_acceptor(listener, Acceptor::default()).await;
        }
    });
    (broker, address, proto, task)
}

#[tokio::test]
async fn a_pubrec_then_disconnect_gets_its_pubrel_on_resumption() {
    let (broker, address, _proto) = start().await;
    let (mut s, _) = Client::open_with(address, &connect("q2lost", false)).await;
    s.send(&subscribe3("q2", 2)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);
    let mut producer = core_producer(&broker, "q2-producer").await;
    send_core(
        &mut producer,
        saranga_message::CoreMessage {
            address: Some("q2".into()),
            body: b"x".to_vec().into(),
            durable: true,
            ..Default::default()
        },
    )
    .await;
    let p = s.packet().await.expect("PUBLISH");
    assert_eq!(p[0] & 0xF6, 0x34, "QoS 2 PUBLISH {p:?}");
    let pid = publish_id(&p);
    let mut both = packet(0x50, &pid.to_be_bytes());
    both.extend([0xE0, 0]);
    s.send(&both).await;
    let _ = s.packet_within(500).await;
    drop(s);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (mut s, _) = Client::open_with(address, &connect("q2lost", false)).await;
    let next = s.packet_within(1500).await;
    assert!(
        next.is_some_and(|p| p[0] & 0xF0 == 0x60),
        "no PUBREL for the PUBREC'd QoS 2 delivery after resumption"
    );
    broker.stop().unwrap();
}

/// A PUBACK followed at once by DISCONNECT: the message is acknowledged, but
/// the transaction deleting its packet-ID record is dropped, so the record
/// comes back after a restart and holds the packet ID for good.
#[tokio::test]
async fn a_puback_then_disconnect_leaves_no_packet_id_record() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address, _proto, task) = start_persistent(dir.path()).await;
    let (mut s, _) = Client::open_with(address, &connect("q1orphan", false)).await;
    s.send(&subscribe3("q1", 1)).await;
    assert_eq!(s.packet().await.unwrap()[0], 0x90);
    let (mut p, _) = Client::open_with(address, &connect("q1pub", true)).await;
    let mut body = string("q1");
    body.extend(1u16.to_be_bytes());
    body.extend_from_slice(b"m");
    p.send(&packet(0x32, &body)).await;
    assert_eq!(p.packet().await.unwrap()[0], 0x40);
    let m = s.packet().await.expect("PUBLISH");
    let pid = publish_id(&m);
    let mut both = packet(0x40, &pid.to_be_bytes());
    both.extend([0xE0, 0]);
    s.send(&both).await;
    let _ = s.packet_within(500).await;
    drop(s);
    tokio::time::sleep(Duration::from_millis(500)).await;
    broker.stop().unwrap();
    task.abort();
    drop(broker);
    let (broker, _address, proto, _task) = start_persistent(dir.path()).await;
    let held = proto
        .state_manager()
        .unwrap()
        .correlations
        .contains_value("q1orphan", i32::from(pid));
    assert!(
        !held,
        "an acknowledged delivery's packet-ID record survived"
    );
    broker.stop().unwrap();
}
