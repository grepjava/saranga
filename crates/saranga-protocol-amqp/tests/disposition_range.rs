//! A peer-controlled AMQP disposition with `first > last` must not panic the
//! connection task (which would unwind past the consumer cleanup and strand the
//! queue's consumer). The broker rejects it as a protocol error and closes the
//! connection through the normal path, leaving no consumer behind.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, frame, messaging::*, transport::*};
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Acceptor, Broker, BrokerConfig, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn send(s: &mut TcpStream, p: Performative) {
    let mut b = vec![];
    frame::encode_amqp(&mut b, 0, &p, &[]);
    s.write_all(&b).await.unwrap();
}

async fn recv(s: &mut TcpStream) -> Performative {
    let mut h = [0; 8];
    tokio::time::timeout(Duration::from_secs(3), s.read_exact(&mut h))
        .await
        .unwrap()
        .unwrap();
    let n = u32::from_be_bytes(h[..4].try_into().unwrap()) as usize;
    let mut b = h.to_vec();
    b.resize(n, 0);
    s.read_exact(&mut b[8..]).await.unwrap();
    match frame::decode(&b).unwrap().body {
        frame::FrameBody::Amqp(p, _) => p,
        x => panic!("{x:?}"),
    }
}

#[tokio::test]
async fn a_reversed_disposition_range_does_not_strand_the_consumer() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
    let broker = Broker::start(cfg).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    q.add(MessageRef::new(ServerMessage::new(
        saranga_message::CoreMessage {
            message_id: 100,
            address: Some("q".into()),
            ..Default::default()
        },
    )));

    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let task = tokio::spawn(broker.clone().serve_acceptor(l, Acceptor::default()));

    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frame::AMQP_HEADER).await.unwrap();
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    send(
        &mut s,
        Performative::Open(Open {
            container_id: "review".into(),
            ..Default::default()
        }),
    )
    .await;
    recv(&mut s).await;
    send(
        &mut s,
        Performative::Begin(Begin {
            next_outgoing_id: 0,
            incoming_window: 100,
            outgoing_window: 100,
            ..Default::default()
        }),
    )
    .await;
    recv(&mut s).await;
    send(
        &mut s,
        Performative::Attach(Attach {
            name: "r".into(),
            handle: 0,
            role: Role::Receiver,
            source: Some(Source {
                address: Some("q".into()),
                capabilities: Some(vec!["queue".into()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await;
    recv(&mut s).await;
    send(
        &mut s,
        Performative::Flow(Flow {
            incoming_window: 100,
            next_outgoing_id: 0,
            outgoing_window: 100,
            handle: Some(0),
            delivery_count: Some(0),
            link_credit: Some(1),
            ..Default::default()
        }),
    )
    .await;
    loop {
        if matches!(recv(&mut s).await, Performative::Transfer(_)) {
            break;
        }
    }
    assert_eq!(q.consumer_count(), 1);

    // A disposition with first (2) > last (1): must not panic the task.
    send(
        &mut s,
        Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: 2,
            last: Some(1),
            settled: true,
            state: Some(DeliveryState::Accepted),
            ..Default::default()
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(s);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        q.consumer_count(),
        0,
        "the consumer must be removed after the malformed frame closes its connection"
    );

    task.abort();
    broker.stop().unwrap();
}
