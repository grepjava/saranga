//! XA completion on the wire: a commit refused for its phase keeps the
//! branch's acknowledgements and the credits its consumer gets back, so the
//! commit that follows lets the consumer go on.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_openwire::commands::*;
use saranga_protocol_openwire::primitive::{self, Primitive, PrimitiveMap};
use saranga_protocol_openwire::{OpenWireProtocol, TransactionId, WireFormat};
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

    async fn transaction(&mut self, command_id: i32, xid: &TransactionId, type_: i8) -> bool {
        self.send(DataStructure::TransactionInfo(TransactionInfo {
            command_id,
            response_required: true,
            connection_id: Some(ConnectionId {
                value: Some("xa".into()),
            }),
            transaction_id: Some(xid.clone()),
            type_,
        }))
        .await;
        self.answer(command_id).await
    }
}

/// A consumer with a prefetch of one acknowledges in an XA branch that is
/// prepared; a one-phase commit of it is refused, and the two-phase commit
/// after it lets the consumer take the next message.
#[tokio::test]
async fn a_commit_refused_for_its_phase_keeps_the_consumers_credit() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols.push(Arc::new(OpenWireProtocol::new()));
    let broker = Broker::start(cfg).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    for id in [100, 101] {
        q.add(MessageRef::new(ServerMessage::new(
            saranga_message::CoreMessage {
                message_id: id,
                address: Some("q".into()),
                ..Default::default()
            },
        )));
    }
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
            value: Some("xa".into()),
        }),
        client_id: Some("xa-client".into()),
        ..Default::default()
    }))
    .await;
    assert!(c.answer(1).await);
    c.send(DataStructure::SessionInfo(SessionInfo {
        command_id: 2,
        response_required: true,
        session_id: Some(SessionId {
            connection_id: Some("xa".into()),
            value: 1,
        }),
    }))
    .await;
    assert!(c.answer(2).await);
    let consumer = ConsumerId {
        connection_id: Some("xa".into()),
        session_id: 1,
        value: 1,
    };
    let dest = Destination {
        data_type: 100,
        physical_name: Some("q".into()),
    };
    c.send(DataStructure::ConsumerInfo(ConsumerInfo {
        command_id: 3,
        response_required: true,
        consumer_id: Some(consumer.clone()),
        destination: Some(dest.clone()),
        prefetch_size: 1,
        ..Default::default()
    }))
    .await;
    let first = c
        .dispatched(Duration::from_secs(5))
        .await
        .expect("the first message");

    let xid = TransactionId::Xa(XaTransactionId {
        format_id: 1,
        global_transaction_id: Some(b"g".to_vec()),
        branch_qualifier: Some(b"b".to_vec()),
    });
    assert!(c.transaction(4, &xid, 0).await, "begun");
    c.send(DataStructure::MessageAck(MessageAck {
        command_id: 5,
        response_required: true,
        destination: Some(dest),
        transaction_id: Some(xid.clone()),
        consumer_id: Some(consumer),
        ack_type: 2,
        first_message_id: Some(first.clone()),
        last_message_id: Some(first),
        message_count: 1,
        poison_cause: None,
    }))
    .await;
    assert!(c.answer(5).await, "acknowledged");
    assert!(c.transaction(6, &xid, 7).await, "ended");
    assert!(c.transaction(7, &xid, 1).await, "prepared");
    assert!(
        !c.transaction(8, &xid, 2).await,
        "a one-phase commit of a prepared branch is refused"
    );
    assert!(c.transaction(9, &xid, 3).await, "committed in two phases");
    assert!(
        c.dispatched(Duration::from_secs(3)).await.is_some(),
        "the consumer has its credit back"
    );
    assert_eq!(q.message_count(), 1, "the first message is gone");

    task.abort();
    broker.stop().unwrap();
}
