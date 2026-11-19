//! An EXPIRED acknowledgement (type 6) expires the messages it names on a
//! non-destructive queue too: `AMQConsumer.acknowledge` checks it before
//! anything else, so the queue takes them off for good (or to its expiry
//! address) instead of keeping them.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_openwire::commands::*;
use saranga_protocol_openwire::primitive::{self, Primitive, PrimitiveMap};
use saranga_protocol_openwire::{OpenWireProtocol, WireFormat};
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Acceptor, Broker, BrokerConfig, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Client {
    s: TcpStream,
    wf: WireFormat,
}

impl Client {
    async fn send(&mut self, cmd: DataStructure) {
        let frame = self.wf.marshal(Some(&cmd)).unwrap();
        self.s.write_all(&frame).await.unwrap();
    }

    async fn recv(&mut self) -> DataStructure {
        let n = self.s.read_u32().await.unwrap();
        let mut b = n.to_be_bytes().to_vec();
        b.resize(4 + n as usize, 0);
        self.s.read_exact(&mut b[4..]).await.unwrap();
        self.wf.unmarshal(&b).unwrap().unwrap()
    }

    /// The answer to command `id`: true for a response, false for an
    /// exception.
    async fn answer(&mut self, id: i32) -> bool {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self.recv().await {
                    DataStructure::Response(r) if r.correlation_id == id => return true,
                    DataStructure::IntegerResponse(r) if r.correlation_id == id => return true,
                    DataStructure::ExceptionResponse(e) if e.correlation_id == id => return false,
                    _ => {}
                }
            }
        })
        .await
        .expect("an answer")
    }

    /// The next dispatched message's ID within `wait`.
    async fn dispatched(&mut self, wait: Duration) -> Option<MessageId> {
        tokio::time::timeout(wait, async {
            loop {
                if let DataStructure::MessageDispatch(d) = self.recv().await
                    && let Some(m) = d.message
                {
                    return m.message_id.unwrap();
                }
            }
        })
        .await
        .ok()
    }
}

#[tokio::test]
async fn an_expired_ack_expires_on_a_non_destructive_queue() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols.push(Arc::new(OpenWireProtocol::new()));
    let broker = Broker::start(cfg).await.unwrap();
    let mut spec = QueueSpec::new("nd", "nd");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    spec.non_destructive = Some(true);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    q.add(MessageRef::new(ServerMessage::new(
        saranga_message::CoreMessage {
            message_id: 100,
            address: Some("nd".into()),
            ..Default::default()
        },
    )));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let task = tokio::spawn(broker.clone().serve_acceptor(l, Acceptor::default()));
    let mut c = Client {
        s: TcpStream::connect(addr).await.unwrap(),
        wf: WireFormat::new(12),
    };
    let mut props = PrimitiveMap::new();
    props.insert("MaxFrameSize".into(), Primitive::Long(i64::MAX));
    let mut bytes = Vec::new();
    primitive::write_map(&mut bytes, Some(&props)).unwrap();
    c.send(DataStructure::WireFormatInfo(WireFormatInfo {
        magic: b"ActiveMQ".to_vec(),
        version: 12,
        marshalled_properties: Some(bytes),
    }))
    .await;
    c.send(DataStructure::ConnectionInfo(ConnectionInfo {
        command_id: 1,
        response_required: true,
        connection_id: Some(ConnectionId {
            value: Some("c".into()),
        }),
        client_id: Some("c-client".into()),
        ..Default::default()
    }))
    .await;
    assert!(c.answer(1).await);
    c.send(DataStructure::SessionInfo(SessionInfo {
        command_id: 2,
        response_required: true,
        session_id: Some(SessionId {
            connection_id: Some("c".into()),
            value: 1,
        }),
    }))
    .await;
    assert!(c.answer(2).await);
    let consumer = ConsumerId {
        connection_id: Some("c".into()),
        session_id: 1,
        value: 1,
    };
    let dest = Destination {
        data_type: 100,
        physical_name: Some("nd".into()),
    };
    c.send(DataStructure::ConsumerInfo(ConsumerInfo {
        command_id: 3,
        consumer_id: Some(consumer.clone()),
        destination: Some(dest.clone()),
        prefetch_size: 10,
        ..Default::default()
    }))
    .await;
    let id = c
        .dispatched(Duration::from_secs(5))
        .await
        .expect("dispatched");
    c.send(DataStructure::MessageAck(MessageAck {
        command_id: 4,
        response_required: true,
        destination: Some(dest),
        consumer_id: Some(consumer),
        ack_type: 6,
        first_message_id: Some(id.clone()),
        last_message_id: Some(id),
        message_count: 1,
        ..Default::default()
    }))
    .await;
    assert!(c.answer(4).await);
    let mut count = q.message_count();
    for _ in 0..50 {
        if count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        count = q.message_count();
    }
    assert_eq!(count, 0, "the expired message stayed on the queue");
    task.abort();
    broker.stop().unwrap();
}
