//! A delivery the queue handed to a link before the client detached it does
//! not go out on the next link attached on the same handle: each event names
//! the link it is for (ProtonServerSenderContext per link in Artemis).

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, frame, messaging::*, transport::*};
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Acceptor, Broker, BrokerConfig, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn enc(b: &mut Vec<u8>, p: Performative) {
    frame::encode_amqp(b, 0, &p, &[]);
}

async fn recv(s: &mut TcpStream) -> Option<Performative> {
    let mut h = [0; 8];
    s.read_exact(&mut h).await.ok()?;
    let n = u32::from_be_bytes(h[..4].try_into().unwrap()) as usize;
    let mut b = h.to_vec();
    b.resize(n, 0);
    s.read_exact(&mut b[8..]).await.ok()?;
    match frame::decode(&b).unwrap().body {
        frame::FrameBody::Amqp(p, _) => Some(p),
        _ => Some(Performative::Close(Close::default())),
    }
}

fn attach(name: &str, handle: u32, queue: &str) -> Performative {
    Performative::Attach(Attach {
        name: name.into(),
        handle,
        role: Role::Receiver,
        source: Some(Source {
            address: Some(queue.into()),
            capabilities: Some(vec!["queue".into()]),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn flow(handle: u32, credit: u32) -> Performative {
    Performative::Flow(Flow {
        incoming_window: 1000,
        next_outgoing_id: 0,
        outgoing_window: 1000,
        handle: Some(handle),
        delivery_count: Some(0),
        link_credit: Some(credit),
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_detached_link_s_deliveries_do_not_reach_the_next_link() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
    let broker = Broker::start(cfg).await.unwrap();
    let mk = |name: &str| {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.routing_type = Some(RoutingType::Anycast);
        broker.post_office().create_queue(spec, false).unwrap()
    };
    let q1 = mk("q1");
    mk("q2");
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let task = tokio::spawn(broker.clone().serve_acceptor(l, Acceptor::default()));

    let mut hits = 0;
    let rounds = 5;
    for round in 0..rounds {
        for i in 0..3000 {
            q1.add(MessageRef::new(ServerMessage::new(
                saranga_message::CoreMessage {
                    message_id: 1_000_000 + round * 10_000 + i,
                    address: Some("q1".into()),
                    durable: false,
                    ..Default::default()
                },
            )));
        }
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(&frame::AMQP_HEADER).await.unwrap();
        let mut h = [0; 8];
        s.read_exact(&mut h).await.unwrap();
        let mut b = vec![];
        enc(
            &mut b,
            Performative::Open(Open {
                container_id: format!("c{round}"),
                ..Default::default()
            }),
        );
        enc(
            &mut b,
            Performative::Begin(Begin {
                next_outgoing_id: 0,
                incoming_window: 1000,
                outgoing_window: 1000,
                ..Default::default()
            }),
        );
        enc(&mut b, attach("r1", 0, "q1"));
        s.write_all(&b).await.unwrap();
        // Open, Begin, Attach back.
        for _ in 0..3 {
            recv(&mut s).await.unwrap();
        }
        // All at once: credit for r1, detach it, attach r2 (on empty q2) with
        // credit.
        let mut b = vec![];
        enc(&mut b, flow(0, 3000));
        s.write_all(&b).await.unwrap();
        let mut normal = 0;
        loop {
            if let Some(Performative::Transfer(_)) = recv(&mut s).await {
                normal += 1;
                break;
            }
        }
        let mut b = vec![];
        enc(
            &mut b,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
        );
        enc(&mut b, attach("r2", 1, "q2"));
        enc(&mut b, flow(1, 10));
        s.write_all(&b).await.unwrap();
        let mut r2_attached = false;
        let mut stray = false;
        let _ = tokio::time::timeout(Duration::from_millis(1500), async {
            while let Some(p) = recv(&mut s).await {
                match p {
                    Performative::Attach(a) if a.name == "r2" => r2_attached = true,
                    Performative::Transfer(_) if r2_attached => {
                        stray = true;
                    }
                    Performative::Transfer(_) => normal += 1,
                    _ => {}
                }
            }
        })
        .await;
        if stray {
            hits += 1;
        }
        drop(s);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    task.abort();
    broker.stop().unwrap();
    assert_eq!(
        hits, 0,
        "a delivery for a detached link went out on a new link"
    );
}
