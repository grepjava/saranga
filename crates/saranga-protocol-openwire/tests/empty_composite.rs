//! A send to a composite destination with no members (`,`) is answered: it
//! goes nowhere, and the producer is not left waiting (nor the connection
//! broken) for an answer no destination will give.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_openwire::commands::*;
use saranga_protocol_openwire::primitive::{self, Primitive, PrimitiveMap};
use saranga_protocol_openwire::{OpenWireProtocol, WireFormat};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
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
}

#[tokio::test]
async fn a_send_to_an_empty_composite_is_answered() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols.push(Arc::new(OpenWireProtocol::new()));
    let broker = Broker::start(cfg).await.unwrap();
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
    let pid = ProducerId {
        connection_id: Some("c".into()),
        session_id: 1,
        value: 1,
    };
    c.send(DataStructure::ProducerInfo(ProducerInfo {
        command_id: 3,
        response_required: true,
        producer_id: Some(pid.clone()),
        ..Default::default()
    }))
    .await;
    assert!(c.answer(3).await);
    c.send(DataStructure::Message(Message {
        data_type: 23,
        command_id: 4,
        response_required: true,
        producer_id: Some(pid.clone()),
        destination: Some(Destination {
            data_type: 100,
            physical_name: Some(",".into()),
        }),
        message_id: Some(MessageId {
            producer_id: Some(pid),
            producer_sequence_id: 1,
            ..Default::default()
        }),
        ..Default::default()
    }))
    .await;
    assert!(c.answer(4).await, "the send was refused");
    // The connection is still served.
    c.send(DataStructure::SessionInfo(SessionInfo {
        command_id: 5,
        response_required: true,
        session_id: Some(SessionId {
            connection_id: Some("c".into()),
            value: 2,
        }),
    }))
    .await;
    assert!(c.answer(5).await);
    task.abort();
    broker.stop().unwrap();
}
