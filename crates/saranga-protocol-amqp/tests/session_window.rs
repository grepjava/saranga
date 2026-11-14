//! The client's session incoming window: lowered below the transfers
//! already on their way, it lets none through until the client opens it
//! again; and a link's transfers waiting for it go with the link.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, frame, messaging::*, transport::*};
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Acceptor, Broker, BrokerConfig, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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

/// A session flow: the client has had the transfers before
/// `next_incoming_id`, and takes `window` more.
fn session_flow(next_incoming_id: u32, window: u32) -> Performative {
    Performative::Flow(Flow {
        next_incoming_id: Some(next_incoming_id),
        incoming_window: window,
        next_outgoing_id: 0,
        outgoing_window: 100,
        ..Default::default()
    })
}

/// A broker with two messages on queue `q`, and a client whose session
/// takes one transfer at a time, its receiver on `q`, in sender settle
/// mode `settle`, given credit for both. The first transfer has come.
async fn receiving(
    settle: SenderSettleMode,
) -> (
    Arc<Broker>,
    JoinHandle<std::io::Result<()>>,
    TcpStream,
    saranga_server::queue::Queue,
) {
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
            container_id: "window".into(),
            ..Default::default()
        }),
    )
    .await;
    send(
        &mut s,
        Performative::Begin(Begin {
            next_outgoing_id: 0,
            incoming_window: 1,
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
            snd_settle_mode: settle,
            source: Some(Source {
                address: Some("q".into()),
                capabilities: Some(vec!["queue".into()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await;
    send(
        &mut s,
        Performative::Flow(Flow {
            next_incoming_id: Some(1),
            incoming_window: 1,
            next_outgoing_id: 0,
            outgoing_window: 100,
            handle: Some(0),
            delivery_count: Some(0),
            link_credit: Some(2),
            ..Default::default()
        }),
    )
    .await;
    assert!(
        transfer(&mut s, Duration::from_secs(5)).await.is_some(),
        "one transfer for the window of one"
    );
    (broker, task, s, q)
}

#[tokio::test]
async fn a_window_lowered_under_a_transfer_in_flight_lets_none_through() {
    let (broker, task, mut s, _) = receiving(SenderSettleMode::Mixed).await;

    // Sent before the client had transfer 1: no room from where it
    // counted, so the window is 1 + 0 - 2.
    send(&mut s, session_flow(1, 0)).await;
    assert!(
        transfer(&mut s, Duration::from_millis(500)).await.is_none(),
        "no room is left for the second transfer"
    );

    // Opened again, once it had it.
    send(&mut s, session_flow(2, 1)).await;
    assert!(
        transfer(&mut s, Duration::from_secs(5)).await.is_some(),
        "the second transfer, once there is room"
    );

    task.abort();
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_detached_links_waiting_transfers_are_not_sent() {
    let (broker, task, mut s, _) = receiving(SenderSettleMode::Mixed).await;

    // The second transfer waits for the window; the link goes first.
    send(
        &mut s,
        Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match recv(&mut s).await {
                Performative::Detach(_) => break,
                Performative::Transfer(t) => panic!("a transfer before the detach: {t:?}"),
                _ => {}
            }
        }
    })
    .await
    .expect("the broker's detach");

    send(&mut s, session_flow(2, 10)).await;
    assert!(
        transfer(&mut s, Duration::from_millis(500)).await.is_none(),
        "no transfer for the detached link"
    );

    task.abort();
    broker.stop().unwrap();
}

/// The queue's message count, once it has settled at `want` (or after a
/// while).
async fn count(q: &saranga_server::queue::Queue, want: i64) -> i64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while q.message_count() != want && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    q.message_count()
}

/// A presettled delivery still waiting for the window when its link goes
/// was never sent, so it is not consumed: its message stays on the queue.
#[tokio::test]
async fn a_presettled_delivery_not_yet_sent_is_not_consumed() {
    let (broker, task, mut s, q) = receiving(SenderSettleMode::Settled).await;
    send(
        &mut s,
        Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(recv(&mut s).await, Performative::Detach(_)) {}
    })
    .await
    .expect("the broker's detach");
    assert_eq!(count(&q, 1).await, 1, "the unsent message is still there");
    task.abort();
    broker.stop().unwrap();
}

/// A presettled delivery waiting for the window is consumed once it is
/// sent.
#[tokio::test]
async fn a_presettled_delivery_is_consumed_once_sent() {
    let (broker, task, mut s, q) = receiving(SenderSettleMode::Settled).await;
    assert_eq!(
        count(&q, 1).await,
        1,
        "the first is consumed, not the second"
    );
    send(&mut s, session_flow(2, 1)).await;
    assert!(
        transfer(&mut s, Duration::from_secs(5)).await.is_some(),
        "the second transfer"
    );
    assert_eq!(count(&q, 0).await, 0, "both consumed once sent");
    task.abort();
    broker.stop().unwrap();
}
