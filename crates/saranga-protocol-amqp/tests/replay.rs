//! Replaying a large AMQP message from the retained journal: replayed whole
//! when the retained files hold its whole body, and not at all when the
//! oldest of them, with the start of its body, has been removed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use saranga_buffer::json::Json;
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::native::AmqpNative;
use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

/// The retained copies, oldest first: by date, then file ID as a number.
fn copies(dir: &Path) -> Vec<PathBuf> {
    let mut copies: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    let key = |p: &PathBuf| {
        let stem = p.file_stem().unwrap().to_string_lossy().into_owned();
        let mut parts = stem.rsplitn(3, '-');
        let id: i64 = parts.next().unwrap().parse().unwrap();
        let date = parts.next().unwrap().to_owned();
        (date, id)
    };
    copies.sort_by_key(key);
    copies
}

/// The encoding of a message with a properties section (an empty list)
/// and a 200,000-byte data section.
fn encoding() -> Vec<u8> {
    let mut bytes = vec![0x00, 0x53, 0x73, 0x45];
    let payload = vec![b'x'; 200_000];
    bytes.extend([0x00, 0x53, 0x75, 0xb0]);
    bytes.extend((payload.len() as u32).to_be_bytes());
    bytes.extend(payload);
    bytes
}

async fn replayed(remove_oldest: bool) -> (usize, Vec<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
             <journal-file-size>65536</journal-file-size>
             <journal-retention-directory unit="DAYS" period="1">history</journal-retention-directory>
           </core></configuration>"#,
    )
    .unwrap();
    let mut config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    config
        .protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
    let broker = Broker::start(config).await.unwrap();
    for name in ["orders", "replayed"] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        broker.post_office().create_queue(spec, false).unwrap();
    }
    let id = broker.post_office().ids().next();
    let mut m = AmqpMessage::new(0, encoding(), 0).unwrap();
    m.set_message_id(id);
    m.set_address("orders".into());
    m.set_durable(true);
    m.make_large();
    let mut writer = broker.large_messages().create(id, true).unwrap();
    for chunk in m.data().chunks(10_000) {
        writer.append(chunk).unwrap();
    }
    let body = writer.finish().unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut session = ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "test".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    session
        .send_native_large(AmqpNative::arc(m), body)
        .await
        .unwrap();
    session.take_completion().wait().await.unwrap();
    let copies = copies(&dir.path().join("history"));
    assert!(copies.len() > 2, "the body spans several journal files");
    if remove_oldest {
        std::fs::remove_file(&copies[0]).unwrap();
    }
    broker
        .management()
        .invoke(
            &broker,
            "broker",
            "replay",
            &[Json::from("orders"), Json::from("replayed"), Json::Null],
            Default::default(),
        )
        .await
        .unwrap();
    let q = broker.post_office().queue("replayed").unwrap();
    let sizes = q
        .browse()
        .await
        .iter()
        .map(|r| r.message.large().map_or(0, |b| b.size()))
        .collect();
    let count = q.message_count() as usize;
    session.close(false).await;
    broker.stop().unwrap();
    (count, sizes)
}

#[tokio::test]
async fn a_large_message_is_replayed_whole() {
    let (count, sizes) = replayed(false).await;
    assert_eq!((count, sizes), (1, vec![encoding().len() as u64]));
}

#[tokio::test]
async fn a_large_message_missing_the_start_of_its_body_is_not_replayed() {
    let (count, sizes) = replayed(true).await;
    assert_eq!(count, 0, "replayed with body sizes {sizes:?}");
}
