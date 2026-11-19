//! An AMQP send whose journal write fails is not answered Accepted: the
//! client is never told a message is stored that is not (afterIO's onError
//! sends no outcome).

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
async fn a_send_whose_journal_sync_fails_is_not_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
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
    spec.durable = true;
    broker.post_office().create_queue(spec, false).unwrap();

    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let task = tokio::spawn(broker.clone().serve_acceptor(l, Acceptor::default()));
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&frame::AMQP_HEADER).await.unwrap();
    let mut h = [0; 8];
    s.read_exact(&mut h).await.unwrap();
    let mut b = vec![];
    enc(
        &mut b,
        Performative::Open(Open {
            container_id: "f".into(),
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
    // Wait for the credit.
    loop {
        if let Some(Performative::Flow(f)) = recv(&mut s).await
            && f.handle == Some(0)
        {
            break;
        }
    }
    // The write is taken; its sync, on the journal's own thread, fails.
    let _y = saranga_fault::Fault::new(saranga_fault::Op::Sync, dir.path()).arm();
    // A durable message: header durable=true, amqp-value "x".
    let msg = [
        0x00, 0x53, 0x70, 0xc0, 0x02, 0x01, 0x41, 0x00, 0x53, 0x77, 0xa1, 0x01, b'x',
    ];
    let mut b = vec![];
    enc(
        &mut b,
        Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(0),
            delivery_tag: Some(vec![1]),
            message_format: Some(0),
            settled: Some(false),
            ..Default::default()
        }),
        &msg,
    );
    s.write_all(&b).await.unwrap();
    let mut seen = vec![];
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(p) = recv(&mut s).await {
            seen.push(format!("{p:?}"));
        }
    })
    .await;
    assert!(broker.is_critical_io_failed());
    task.abort();
    let _ = broker.stop();
    assert!(
        !seen
            .iter()
            .any(|f| f.starts_with("Disposition") && f.contains("Accepted")),
        "a message whose journal write failed was Accepted"
    );
}
