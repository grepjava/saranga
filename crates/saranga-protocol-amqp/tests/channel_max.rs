//! The peer's channel-max: no session is begun on a channel above it.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, frame, transport::*};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn send(s: &mut TcpStream, channel: u16, p: Performative) {
    let mut b = vec![];
    frame::encode_amqp(&mut b, channel, &p, &[]);
    s.write_all(&b).await.unwrap();
}

async fn recv(s: &mut TcpStream) -> (u16, Performative) {
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    let n = u32::from_be_bytes(h[..4].try_into().unwrap()) as usize;
    let mut b = h.to_vec();
    b.resize(n, 0);
    s.read_exact(&mut b[8..]).await.unwrap();
    let f = frame::decode(&b).unwrap();
    match f.body {
        frame::FrameBody::Amqp(p, _) => (f.channel, p),
        x => panic!("{x:?}"),
    }
}

fn begin() -> Performative {
    Performative::Begin(Begin {
        next_outgoing_id: 0,
        incoming_window: 100,
        outgoing_window: 100,
        ..Default::default()
    })
}

/// A peer allowing channel 0 only that begins a second session is refused
/// it, with the connection closed, rather than answered on channel 1.
#[tokio::test]
async fn no_session_is_begun_above_the_peers_channel_max() {
    let mut cfg = BrokerConfig::new(Arc::new(SelectorFilters));
    cfg.protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
    let broker = Broker::start(cfg).await.unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let task = tokio::spawn(broker.clone().serve_acceptor(l, Acceptor::default()));
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frame::AMQP_HEADER).await.unwrap();
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    send(
        &mut s,
        0,
        Performative::Open(Open {
            container_id: "channels".into(),
            channel_max: 0,
            ..Default::default()
        }),
    )
    .await;
    send(&mut s, 0, begin()).await;
    send(&mut s, 1, begin()).await;

    let mut begun = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match recv(&mut s).await {
                (ch, Performative::Begin(_)) => begun.push(ch),
                (_, Performative::Close(c)) => return c,
                _ => {}
            }
        }
    })
    .await
    .expect("the connection is closed");
    assert_eq!(begun, vec![0], "only channel 0 is used");
    assert_eq!(
        closed.error.map(|e| e.condition),
        Some(condition::RESOURCE_LIMIT_EXCEEDED.into())
    );

    task.abort();
    broker.stop().unwrap();
}
