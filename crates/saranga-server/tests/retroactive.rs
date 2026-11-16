//! Retroactive addresses (`retroactive-message-count`), as Artemis makes
//! them: an internal address with a ring queue per routing type, fed by a
//! divert, gives a queue made later the messages it holds; the ring queues
//! follow the settings, outlive a restart, and go with their address.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::postoffice::{QueueSpec, RoutingTypes, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

const RING: &str = "$.artemis.internal.news.queue.multicast.retro";
const ANY_RING: &str = "$.artemis.internal.news.queue.anycast.retro";
const RETRO_ADDRESS: &str = "$.artemis.internal.news.address.retro";
const DIVERT: &str = "$.artemis.internal.news.divert.retro";

async fn start(dir: &Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             <address-settings><address-setting match="news">
               <retroactive-message-count>3</retroactive-message-count>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    Broker::start(BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap())
        .await
        .unwrap()
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
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
    )
}

async fn send(s: &mut ServerSession, i: i32) {
    let mut m = CoreMessage {
        address: Some(SimpleString::from("news")),
        durable: true,
        ..Default::default()
    };
    m.properties
        .insert(SimpleString::from("i"), PropertyValue::Int(i));
    s.send(m, None).await.unwrap();
    s.take_completion().wait().await.unwrap();
}

fn queue(broker: &Broker, name: &str, filter: Option<&str>) {
    let mut spec = QueueSpec::new(name, "news");
    spec.routing_type = Some(RoutingType::Multicast);
    spec.filter_string = filter.map(str::to_owned);
    broker.post_office().create_queue(spec, false).unwrap();
}

/// The `i` of each message `name` holds, once it holds `n`. A queue on the
/// address has them under its address; the ring queue under its own.
async fn held(broker: &Broker, name: &str, n: usize) -> Vec<i32> {
    let q = broker.post_office().queue(name).unwrap();
    for _ in 0..200 {
        if q.message_count() as usize >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    q.browse()
        .await
        .iter()
        .map(|r| {
            let address = if name == RING { RETRO_ADDRESS } else { "news" };
            assert_eq!(
                r.message.core().address.as_ref().map(|a| a.to_string()),
                Some(address.to_owned()),
                "{name}"
            );
            match r.message.core().properties.get(&SimpleString::from("i")) {
                Some(PropertyValue::Int(i)) => *i,
                other => panic!("{other:?}"),
            }
        })
        .collect()
}

async fn attribute(broker: &Arc<Broker>, resource: &str, name: &str) -> Json {
    broker
        .management()
        .attribute(broker, resource, name, Default::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_queue_made_later_gets_what_the_address_kept() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let po = broker.post_office();
    po.add_or_update_address(
        "news",
        RoutingTypes::of(RoutingType::Multicast),
        false,
        false,
    )
    .unwrap();
    // The resources, as Artemis names and makes them.
    let ring = po.queue(RING).unwrap();
    assert_eq!(ring.config().ring_size, 3);
    assert_eq!(ring.config().max_consumers, 0);
    assert!(po.queue(ANY_RING).is_some());
    assert!(po.divert(DIVERT).is_some_and(|d| !d.exclusive));
    let info = po.address_info(RETRO_ADDRESS).unwrap();
    assert!(!info.internal);
    for (resource, retro) in [
        (format!("address.{RETRO_ADDRESS}"), true),
        (format!("queue.{RING}"), true),
        (format!("divert.{DIVERT}"), true),
        ("address.news".to_owned(), false),
    ] {
        assert_eq!(
            attribute(&broker, &resource, "RetroactiveResource").await,
            Json::Bool(retro),
            "{resource}"
        );
    }

    // Sent with no queue there, the last 3 are kept.
    let mut s = session(&broker);
    for i in 1..=5 {
        send(&mut s, i).await;
    }
    assert_eq!(held(&broker, RING, 3).await, vec![3, 4, 5]);
    queue(&broker, "all", None);
    assert_eq!(held(&broker, "all", 3).await, vec![3, 4, 5]);
    queue(&broker, "some", Some("i > 3"));
    assert_eq!(held(&broker, "some", 2).await, vec![4, 5]);
    assert_eq!(ring.message_count(), 3, "the ring queue keeps them");

    // The ring queues follow the settings.
    let settings = saranga_config::AddressSettings {
        retroactive_message_count: Some(2),
        ..Default::default()
    };
    po.add_address_settings("news", settings, "{\"retroactiveMessageCount\":2}")
        .unwrap();
    assert_eq!(po.queue(RING).unwrap().config().ring_size, 2);
    assert_eq!(po.queue(ANY_RING).unwrap().config().ring_size, 2);
    let settings = saranga_config::AddressSettings {
        retroactive_message_count: Some(3),
        ..Default::default()
    };
    po.add_address_settings("news", settings, "{\"retroactiveMessageCount\":3}")
        .unwrap();
    assert_eq!(po.queue(RING).unwrap().config().ring_size, 3);
    broker.stop().unwrap();
    drop(broker);

    // After a restart, the kept messages and the divert are there.
    let broker = start(dir.path()).await;
    let po = broker.post_office();
    assert!(po.divert(DIVERT).is_some());
    let mut s = session(&broker);
    send(&mut s, 6).await;
    assert_eq!(held(&broker, RING, 3).await, vec![4, 5, 6]);
    queue(&broker, "late", None);
    assert_eq!(held(&broker, "late", 3).await, vec![4, 5, 6]);

    // Removed (forcibly: the divert is one of its bindings), the address
    // takes its resources with it.
    let removed = broker
        .management()
        .invoke(
            &broker,
            "broker",
            "deleteAddress",
            &[Json::from("news"), Json::Bool(true)],
            Default::default(),
        )
        .await;
    assert!(removed.is_ok(), "{removed:?}");
    assert!(po.address_info("news").is_none());
    assert!(po.queue(RING).is_none() && po.queue(ANY_RING).is_none());
    assert!(po.divert(DIVERT).is_none());
    assert!(po.address_info(RETRO_ADDRESS).is_none());
    broker.stop().unwrap();
}
