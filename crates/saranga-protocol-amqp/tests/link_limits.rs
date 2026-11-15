//! A link's limits on the wire: the credit an aborted delivery used is given
//! back (`ProtonAbstractReceiver.handleAbortedDelivery`), and handles stay
//! within both ends' handle-max.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::transaction::Coordinator;
use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, frame, messaging::*, transport::*};
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

/// The next frame matching `f` within `wait`, skipping others.
async fn next<T>(
    s: &mut TcpStream,
    wait: Duration,
    mut f: impl FnMut(Performative) -> Option<T>,
) -> Option<T> {
    tokio::time::timeout(wait, async {
        loop {
            if let Some(t) = f(recv(s).await) {
                return t;
            }
        }
    })
    .await
    .ok()
}

/// A broker with queue `q` and an acceptor with `params`, and a connection
/// to it that has sent Open and Begin (with `handle_max`) and read the
/// broker's.
async fn connect(
    params: &[(&str, &str)],
    handle_max: u32,
) -> (Arc<Broker>, tokio::task::JoinHandle<()>, TcpStream) {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
    let broker = Broker::start(cfg).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    broker.post_office().create_queue(spec, false).unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let mut acceptor = Acceptor::default();
    for (k, v) in params {
        acceptor.params.insert((*k).into(), (*v).into());
    }
    let serving = broker.clone();
    let task = tokio::spawn(async move {
        let _ = serving.serve_acceptor(l, acceptor).await;
    });
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frame::AMQP_HEADER).await.unwrap();
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    send(
        &mut s,
        Performative::Open(Open {
            container_id: "limits".into(),
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
            handle_max,
            ..Default::default()
        }),
    )
    .await;
    next(&mut s, Duration::from_secs(5), |p| {
        matches!(p, Performative::Begin(_)).then_some(())
    })
    .await
    .expect("the broker's begin");
    (broker, task, s)
}

fn aborted(delivery_id: u32) -> Performative {
    Performative::Transfer(Transfer {
        handle: 0,
        delivery_id: Some(delivery_id),
        delivery_tag: Some(vec![delivery_id as u8]),
        message_format: Some(0),
        aborted: true,
        ..Default::default()
    })
}

/// The credit a link's next Flow from the broker grants, within `wait`.
async fn granted(s: &mut TcpStream, wait: Duration) -> Option<u32> {
    next(s, wait, |p| match p {
        Performative::Flow(f) if f.handle == Some(0) => f.link_credit,
        _ => None,
    })
    .await
}

/// A producer whose deliveries are all aborted keeps its credit: each
/// abort gives back the credit it used, so the link never runs dry.
#[tokio::test]
async fn aborted_deliveries_give_their_credit_back() {
    let (broker, task, mut s) =
        connect(&[("amqpCredits", "2"), ("amqpLowCredits", "0")], u32::MAX).await;
    send(
        &mut s,
        Performative::Attach(Attach {
            name: "p".into(),
            handle: 0,
            role: Role::Sender,
            target: Some(TargetKind::Target(Target {
                address: Some("q".into()),
                ..Default::default()
            })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(granted(&mut s, Duration::from_secs(5)).await, Some(2));
    send(&mut s, aborted(0)).await;
    send(&mut s, aborted(1)).await;
    let credit = granted(&mut s, Duration::from_secs(3)).await;
    assert!(
        credit.is_some_and(|c| c > 0),
        "credit after two aborted deliveries: {credit:?}"
    );

    task.abort();
    broker.stop().unwrap();
}

/// The same on a transaction coordinator: aborted declares leave it credit.
#[tokio::test]
async fn aborted_declares_leave_the_coordinator_credit() {
    let (broker, task, mut s) =
        connect(&[("amqpCredits", "2"), ("amqpLowCredits", "1")], u32::MAX).await;
    send(
        &mut s,
        Performative::Attach(Attach {
            name: "txn".into(),
            handle: 0,
            role: Role::Sender,
            target: Some(TargetKind::Coordinator(Coordinator::default())),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(granted(&mut s, Duration::from_secs(5)).await, Some(2));
    send(&mut s, aborted(0)).await;
    send(&mut s, aborted(1)).await;
    let credit = granted(&mut s, Duration::from_secs(3)).await;
    assert!(
        credit.is_some_and(|c| c > 0),
        "coordinator credit after two aborted declares: {credit:?}"
    );

    task.abort();
    broker.stop().unwrap();
}

fn receiver(name: &str, handle: u32) -> Performative {
    Performative::Attach(Attach {
        name: name.into(),
        handle,
        role: Role::Receiver,
        source: Some(Source {
            address: Some("q".into()),
            capabilities: Some(vec!["queue".into()]),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// The handles the broker attaches with, and the connection's close.
async fn attaches_until_close(s: &mut TcpStream) -> (Vec<u32>, Option<Close>) {
    let mut handles = Vec::new();
    let close = next(s, Duration::from_secs(5), |p| match p {
        Performative::Attach(a) => {
            handles.push(a.handle);
            None
        }
        Performative::Close(c) => Some(c),
        _ => None,
    })
    .await;
    (handles, close)
}

/// A peer whose Begin allows handle 0 only that attaches a second link is
/// refused it, with the connection closed, rather than answered on handle 1.
#[tokio::test]
async fn no_link_is_attached_above_the_peers_handle_max() {
    let (broker, task, mut s) = connect(&[], 0).await;
    send(&mut s, receiver("a", 0)).await;
    send(&mut s, receiver("b", 1)).await;
    let (handles, close) = attaches_until_close(&mut s).await;
    assert_eq!(handles, vec![0], "only handle 0 is used");
    assert_eq!(
        close.and_then(|c| c.error).map(|e| e.condition),
        Some(condition::RESOURCE_LIMIT_EXCEEDED.into())
    );

    task.abort();
    broker.stop().unwrap();
}

/// An attach on a handle above the handle-max the broker's Begin gave ends
/// the connection, as Proton-J's `handleAttach` does.
#[tokio::test]
async fn an_attach_above_the_brokers_handle_max_is_refused() {
    let (broker, task, mut s) = connect(&[], u32::MAX).await;
    send(&mut s, receiver("a", 65536)).await;
    let (handles, close) = attaches_until_close(&mut s).await;
    assert!(handles.is_empty(), "attached: {handles:?}");
    let error = close.and_then(|c| c.error).expect("closed with an error");
    assert_eq!(error.condition, condition::FRAMING_ERROR);
    assert_eq!(error.description.as_deref(), Some("handle-max exceeded"));

    task.abort();
    broker.stop().unwrap();
}

fn limited_receiver(name: &str, handle: u32, max_message_size: Option<u64>) -> Performative {
    let Performative::Attach(mut a) = receiver(name, handle) else {
        unreachable!()
    };
    a.max_message_size = max_message_size;
    Performative::Attach(a)
}

fn flow(handle: u32, link_credit: u32) -> Performative {
    Performative::Flow(Flow {
        incoming_window: 100,
        next_outgoing_id: 0,
        outgoing_window: 100,
        handle: Some(handle),
        delivery_count: Some(0),
        link_credit: Some(link_credit),
        ..Default::default()
    })
}

/// A message larger than a receiver's max-message-size is not sent to it:
/// it stays on the queue, and goes to a consumer that accepts it.
#[tokio::test]
async fn a_message_above_the_receivers_max_message_size_is_not_sent_to_it() {
    let (broker, task, mut s) = connect(&[], u32::MAX).await;
    let q = broker.post_office().queue("q").unwrap();
    q.add(saranga_server::message::MessageRef::new(
        saranga_server::message::ServerMessage::new(saranga_message::CoreMessage {
            message_id: 100,
            address: Some("q".into()),
            ..Default::default()
        }),
    ));
    send(&mut s, limited_receiver("small", 0, Some(10))).await;
    send(&mut s, flow(0, 10)).await;
    let sent = next(&mut s, Duration::from_secs(1), |p| match p {
        Performative::Transfer(t) => Some(t.handle),
        _ => None,
    })
    .await;
    assert_eq!(sent, None, "nothing goes to the limited receiver");
    assert_eq!(q.message_count(), 1, "the message stays on the queue");

    send(&mut s, limited_receiver("any", 1, None)).await;
    send(&mut s, flow(1, 10)).await;
    let sent = next(&mut s, Duration::from_secs(5), |p| match p {
        Performative::Transfer(t) => Some(t.handle),
        _ => None,
    })
    .await;
    assert_eq!(sent, Some(1), "the receiver without a limit gets it");

    task.abort();
    broker.stop().unwrap();
}
