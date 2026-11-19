//! A transfer the client sent on a link the broker had just detached (here
//! its drain timed out once the address was full) is dropped with the link,
//! as Proton-J keeps the peer's handle bound until the peer's detach; the
//! connection and its session go on.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, frame, messaging::*, transport::*};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Acceptor, Broker, BrokerConfig, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn enc(b: &mut Vec<u8>, p: Performative, payload: &[u8]) {
    frame::encode_amqp(b, 0, &p, payload);
}

async fn recv(s: &mut TcpStream) -> Option<Performative> {
    let mut h = [0; 8];
    s.read_exact(&mut h).await.ok()?;
    let n = u32::from_be_bytes(h[..4].try_into().unwrap()) as usize;
    let mut b = h.to_vec();
    b.resize(n, 0);
    s.read_exact(&mut b[8..]).await.ok()?;
    match frame::decode(&b).ok()?.body {
        frame::FrameBody::Amqp(p, _) => Some(p),
        _ => Some(Performative::End(End::default())),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_crossing_a_forced_detach_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <address-settings>
               <address-setting match="q">
                 <max-size-bytes>1</max-size-bytes>
                 <address-full-policy>FAIL</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let mut cfg =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
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
    acceptor
        .params
        .insert("amqpLinkQuiesceTimeout".into(), "200".into());
    let task = tokio::spawn(broker.clone().serve_acceptor(l, acceptor));
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frame::AMQP_HEADER).await.unwrap();
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    let mut b = vec![];
    enc(
        &mut b,
        Performative::Open(Open {
            container_id: "d".into(),
            ..Default::default()
        }),
        &[],
    );
    enc(
        &mut b,
        Performative::Begin(Begin {
            next_outgoing_id: 0,
            incoming_window: 1000,
            outgoing_window: 1000,
            ..Default::default()
        }),
        &[],
    );
    enc(
        &mut b,
        Performative::Attach(Attach {
            name: "s".into(),
            handle: 0,
            role: Role::Sender,
            target: Some(TargetKind::Target(Target {
                address: Some("q".into()),
                ..Default::default()
            })),
            ..Default::default()
        }),
        &[],
    );
    s.write_all(&b).await.unwrap();
    loop {
        if let Some(Performative::Flow(f)) = recv(&mut s).await
            && f.handle == Some(0)
        {
            break;
        }
    }
    let msg = [0x00, 0x53, 0x77, 0xa1, 0x05, b'h', b'e', b'l', b'l', b'o'];
    let transfer = |id: u32| {
        let mut b = vec![];
        enc(
            &mut b,
            Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: Some(id),
                delivery_tag: Some(vec![id as u8]),
                message_format: Some(0),
                settled: Some(false),
                ..Default::default()
            }),
            &msg,
        );
        b
    };
    // Fill the address (the first is taken, max-size 1), then one refused.
    for id in 0..3 {
        s.write_all(&transfer(id)).await.unwrap();
    }
    // Read until the forced detach comes; the client is "slow" and has
    // one more transfer on the wire, sent before it saw the detach.
    let mut detached = false;
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(p) = recv(&mut s).await {
            if let Performative::Detach(_) = &p {
                detached = true;
                break;
            }
        }
    })
    .await;
    assert!(detached, "no forced detach");
    // The crossing transfer, then the peer's own detach and a new link.
    let mut b = transfer(3);
    enc(
        &mut b,
        Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }),
        &[],
    );
    enc(
        &mut b,
        Performative::Attach(Attach {
            name: "s2".into(),
            handle: 1,
            role: Role::Sender,
            target: Some(TargetKind::Target(Target {
                address: Some("q".into()),
                ..Default::default()
            })),
            ..Default::default()
        }),
        &[],
    );
    s.write_all(&b).await.unwrap();
    let mut closed = None;
    let mut attached = false;
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(p) = recv(&mut s).await {
            match p {
                Performative::Close(c) => {
                    closed = Some(c);
                    break;
                }
                Performative::Attach(a) if a.name == "s2" => {
                    attached = true;
                    break;
                }
                _ => {}
            }
        }
    })
    .await;
    task.abort();
    let _ = broker.stop();
    assert!(
        closed.is_none(),
        "the connection closed for a transfer crossing the link's detach: {closed:?}"
    );
    assert!(attached, "the session took no new link");
}
