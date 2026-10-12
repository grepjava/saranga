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
