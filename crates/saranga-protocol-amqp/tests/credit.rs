//! A receiver that withdraws credit while a delivery is on the way leaves
//! the sender's credit negative, as Proton's signed credit has it, and not
//! wrapped round to some four billion deliveries.

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
    s.read_exact(&mut h).await.unwrap();
    let n = u32::from_be_bytes(h[..4].try_into().unwrap()) as usize;
    let mut b = h.to_vec();
    b.resize(n, 0);
    s.read_exact(&mut b[8..]).await.unwrap();
    match frame::decode(&b).unwrap().body {
        frame::FrameBody::Amqp(p, _) => p,
        x => panic!("{x:?}"),
    }
}

/// The next transfer within `wait`, skipping other frames.
async fn transfer(s: &mut TcpStream, wait: Duration) -> Option<Transfer> {
    tokio::time::timeout(wait, async {
        loop {
            if let Performative::Transfer(t) = recv(s).await {
                return t;
            }
        }
    })
    .await
    .ok()
}

fn flow(delivery_count: u32, link_credit: u32) -> Performative {
    Performative::Flow(Flow {
        incoming_window: 100,
        next_outgoing_id: 0,
        outgoing_window: 100,
        handle: Some(0),
        delivery_count: Some(delivery_count),
        link_credit: Some(link_credit),
        ..Default::default()
    })
}

#[tokio::test]
async fn credit_withdrawn_under_a_delivery_in_flight_stays_withdrawn() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
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
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frame::AMQP_HEADER).await.unwrap();
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    send(
        &mut s,
        Performative::Open(Open {
            container_id: "credit".into(),
            ..Default::default()
        }),
    )
    .await;
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
    send(&mut s, flow(0, 1)).await;
    assert!(
        transfer(&mut s, Duration::from_secs(5)).await.is_some(),
        "one delivery for the one credit"
    );

    // Sent before the receiver counted that delivery: no more credit from
    // where it counted, so the sender's is 0 + 0 - 1.
    send(&mut s, flow(0, 0)).await;
    assert!(
        transfer(&mut s, Duration::from_millis(500)).await.is_none(),
        "no credit is left for the second message"
    );

    task.abort();
    broker.stop().unwrap();
}
