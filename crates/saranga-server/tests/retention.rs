//! Journal retention: the history copies and `replay`, which routes the
//! messages they hold again.

use std::path::Path;
use std::sync::Arc;

use saranga_config as cfg;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, retention: bool) -> Arc<Broker> {
    let retention = if retention {
        r#"<journal-retention-directory unit="DAYS" period="1">history</journal-retention-directory>"#
    } else {
        ""
    };
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             {retention}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

async fn op(
    broker: &Arc<Broker>,
    resource: &str,
    operation: &str,
    params: Vec<Json>,
) -> Result<Json, String> {
    broker
        .management()
        .invoke(broker, resource, operation, &params, Default::default())
        .await
        .map_err(|e| e.to_string())
}

fn s(v: &str) -> Json {
    Json::from(v)
}

async fn queue(broker: &Arc<Broker>, name: &str) {
    op(
        broker,
        "broker",
        "createQueue",
        vec![
            s(name),
            s("ANYCAST"),
            s(name),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await
    .unwrap();
}

async fn send(broker: &Arc<Broker>, address: &str, color: &str, body: &str) {
    let headers = Json::Object(vec![("color".to_owned(), s(color))]);
    op(
        broker,
        &format!("address.{address}"),
        "sendMessage",
        vec![
            headers,
            Json::Long(3),
            s(body),
            Json::Bool(true),
            Json::Null,
            Json::Null,
        ],
    )
    .await
    .unwrap();
}

async fn count(broker: &Arc<Broker>, queue: &str) -> Json {
    op(broker, &format!("queue.{queue}"), "countMessages", vec![])
        .await
        .unwrap()
}

#[tokio::test]
async fn replay_routes_retained_messages_again() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    queue(&broker, "orders").await;
    queue(&broker, "replayed").await;
    for i in 0..10 {
        send(
            &broker,
            "orders",
            if i % 2 == 0 { "red" } else { "blue" },
            &format!("m{i}"),
        )
        .await;
    }
    // Consumed or not, the messages stay in the history.
    op(&broker, "queue.orders", "removeAllMessages", vec![])
        .await
        .unwrap();
    assert_eq!(count(&broker, "orders").await, Json::Long(0));

    op(
        &broker,
        "broker",
        "replay",
        vec![s("orders"), s("replayed"), s("color = 'red'")],
    )
    .await
    .unwrap();
    assert_eq!(count(&broker, "replayed").await, Json::Long(5));

    // To the same address, with dates around now.
    op(
        &broker,
        "broker",
        "replay",
        vec![
            s("20000101000000"),
            s("29991231235959"),
            s("orders"),
            Json::Null,
            Json::Null,
        ],
    )
    .await
    .unwrap();
    assert_eq!(count(&broker, "orders").await, Json::Long(10));

    // Dates before any copy: nothing.
    op(
        &broker,
        "broker",
        "replay",
        vec![
            s("20000101000000"),
            s("20000102000000"),
            s("orders"),
            s("replayed"),
            Json::Null,
        ],
    )
    .await
    .unwrap();
    assert_eq!(count(&broker, "replayed").await, Json::Long(5));

    let e = op(
        &broker,
        "broker",
        "replay",
        vec![s("x"), s("y"), s("orders"), Json::Null, Json::Null],
    )
    .await
    .unwrap_err();
    assert!(e.contains("Unparseable date: \"x\""), "{e}");
    let e = op(
        &broker,
        "broker",
        "replay",
        vec![s("orders"), Json::Null, s("color = ")],
    )
    .await
    .unwrap_err();
    assert!(e.contains("AMQ229020"), "{e}");

    let copies = std::fs::read_dir(dir.path().join("history"))
        .unwrap()
        .count();
    assert!(copies > 0);
    broker.stop().unwrap();
}

#[tokio::test]
async fn replay_needs_retention() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), false).await;
    let e = op(
        &broker,
        "broker",
        "replay",
        vec![s("orders"), Json::Null, Json::Null],
    )
    .await
    .unwrap_err();
    assert!(
        e.contains("AMQ229239: There is no retention configured"),
        "{e}"
    );
    broker.stop().unwrap();
}

/// A large message whose body is only partly in the retained files (an
/// earlier file with its first chunks was removed) is not replayed: the
/// part left is not the message. Its whole body replays.
#[tokio::test]
async fn replay_refuses_a_large_message_missing_part_of_its_body() {
    use saranga_buffer::{PropertyValue, SimpleString};
    use saranga_message::CoreMessage;
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};

    for remove_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start(dir.path(), true).await;
        queue(&broker, "orders").await;
        queue(&broker, "replayed").await;

        // A 200 KB body: twenty 10 KiB chunks across several 64 KiB files.
        let mut body_bytes = b"prefix".to_vec();
        body_bytes.extend(std::iter::repeat_n(b'.', 200_000));
        body_bytes.extend_from_slice(b"suffix");
        let id = broker.post_office().ids().next();
        let mut w = broker.large_messages().create(id, true).unwrap();
        for chunk in body_bytes.chunks(10_000) {
            w.append(chunk).unwrap();
        }
        let body = w.finish().unwrap();
        let mut message = CoreMessage {
            message_id: id,
            address: Some(SimpleString::from("orders")),
            durable: true,
            ..Default::default()
        };
        message.properties.insert(
            SimpleString::from("_AMQ_LARGE_SIZE"),
            PropertyValue::Long(body_bytes.len() as i64),
        );
        let conn = Arc::new(ClientConnection::new(
            Transport::internal("test"),
            Arc::new(Default::default()),
            Vec::new(),
        ));
        let mut session = ServerSession::new(
            broker.clone(),
            conn,
            SessionOptions {
                name: "s".into(),
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
        session.send(message, Some(body)).await.unwrap();
        session.take_completion().wait().await.unwrap();
        assert_eq!(count(&broker, "orders").await, Json::Long(1));

        if remove_first {
            // Retention removes the oldest copy, with the body's first chunks.
            let mut copies: Vec<_> = std::fs::read_dir(dir.path().join("history"))
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect();
            copies.sort();
            assert!(copies.len() > 2, "{copies:?}");
            std::fs::remove_file(&copies[0]).unwrap();
        }
        op(
            &broker,
            "broker",
            "replay",
            vec![s("orders"), s("replayed"), Json::Null],
        )
        .await
        .unwrap();
        let expected = if remove_first { 0 } else { 1 };
        assert_eq!(
            count(&broker, "replayed").await,
            Json::Long(expected),
            "first copy removed: {remove_first}"
        );
        session.close(false).await;
        broker.stop().unwrap();
    }
}

/// A copy of a large message (here `copyMessage`; diverts, dead-lettering
/// and expiry copy the same way) is recorded under its own ID, body and all,
/// so replaying the copy's address replays it whole.
#[tokio::test]
async fn a_copied_large_message_can_be_replayed() {
    use saranga_buffer::{PropertyValue, SimpleString};
    use saranga_message::CoreMessage;
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};

    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    queue(&broker, "orders").await;
    queue(&broker, "copies").await;
    queue(&broker, "replayed").await;
    let body_bytes = vec![b'x'; 50_000];
    let id = broker.post_office().ids().next();
    let mut w = broker.large_messages().create(id, true).unwrap();
    w.append(&body_bytes).unwrap();
    let body = w.finish().unwrap();
    let mut message = CoreMessage {
        message_id: id,
        address: Some(SimpleString::from("orders")),
        durable: true,
        ..Default::default()
    };
    message.properties.insert(
        SimpleString::from("_AMQ_LARGE_SIZE"),
        PropertyValue::Long(body_bytes.len() as i64),
    );
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    let mut session = ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
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
    session.send(message, Some(body)).await.unwrap();
    session.take_completion().wait().await.unwrap();

    let copied = op(
        &broker,
        "queue.orders",
        "copyMessage",
        vec![Json::Long(id), s("copies")],
    )
    .await
    .unwrap();
    assert_eq!(copied, Json::Bool(true));
    assert_eq!(count(&broker, "copies").await, Json::Long(1));

    op(
        &broker,
        "broker",
        "replay",
        vec![s("copies"), s("replayed"), Json::Null],
    )
    .await
    .unwrap();
    assert_eq!(
        count(&broker, "replayed").await,
        Json::Long(1),
        "the copy's body must be in the retained journal under its own ID"
    );
    session.close(false).await;
    broker.stop().unwrap();
}
