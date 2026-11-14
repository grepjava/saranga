//! A management copy (`copyMessage`) is routed as Artemis's `copy` routes it,
//! through the post office in a transaction: a destination full under
//! `FAIL` refuses it as it refuses a send, and a copy that cannot be made
//! is reported, not counted as done.

use std::path::Path;
use std::sync::Arc;

use saranga_buffer::SimpleString;
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, persistence: bool) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>{persistence}</persistence-enabled>
             <address-settings>
               <address-setting match="full">
                 <max-size-bytes>10000</max-size-bytes>
                 <address-full-policy>FAIL</address-full-policy>
               </address-setting>
             </address-settings>
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
}

async fn create_queue(broker: &Arc<Broker>, name: &str) {
    op(
        broker,
        "broker",
        "createQueue",
        vec![
            Json::from(name),
            Json::from("ANYCAST"),
            Json::from(name),
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

async fn count(broker: &Arc<Broker>, queue: &str) -> Json {
    op(broker, &format!("queue.{queue}"), "countMessages", vec![])
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

/// Sends 4 KB to `address`; the error's message if refused.
async fn send(s: &mut ServerSession, address: &str) -> Result<(), String> {
    let message = CoreMessage {
        address: Some(SimpleString::from(address)),
        durable: true,
        body: vec![7u8; 4000].into(),
        ..Default::default()
    };
    let sent = s.send(message, None).await;
    s.take_completion()
        .wait()
        .await
        .map_err(|e| e.to_string())?;
    sent.map_err(|e| e.message)
}

async fn first_id(broker: &Arc<Broker>, queue: &str) -> i64 {
    let q = broker.post_office().queue(queue).unwrap();
    q.browse().await[0].message_id()
}

/// A copy to an address full under `FAIL` is refused as a send there is.
#[tokio::test]
async fn a_copy_to_a_full_fail_address_is_refused() {
    for persistence in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start(dir.path(), persistence).await;
        create_queue(&broker, "full").await;
        create_queue(&broker, "src").await;
        let mut s = session(&broker);
        let mut filled = 0;
        while send(&mut s, "full").await.is_ok() {
            filled += 1;
            assert!(filled < 10, "the address never filled");
        }
        let held = count(&broker, "full").await;
        send(&mut s, "src").await.unwrap();
        let id = first_id(&broker, "src").await;

        let copied = op(
            &broker,
            "queue.src",
            "copyMessage",
            vec![Json::Long(id), Json::from("full")],
        )
        .await;
        assert!(
            copied.is_err(),
            "persistence {persistence}: the copy must be refused: {copied:?}"
        );
        assert_eq!(
            count(&broker, "full").await,
            held,
            "persistence {persistence}"
        );
        assert_eq!(count(&broker, "src").await, Json::Long(1));
        s.close(false).await;
        broker.stop().unwrap();
    }
}

/// A large message whose body cannot be copied is not copied, and the
/// operation says so rather than report a copy that never arrived.
#[tokio::test]
async fn a_large_copy_that_cannot_be_made_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    create_queue(&broker, "src").await;
    create_queue(&broker, "dst").await;
    let id = broker.post_office().ids().next();
    let mut w = broker.large_messages().create(id, true).unwrap();
    w.append(&[1u8; 200_000]).unwrap();
    let body = w.finish().unwrap();
    let path = body.path().expect("a durable body has a file").to_owned();
    let message = ServerMessage::new_large(
        CoreMessage {
            message_id: id,
            durable: true,
            address: Some(SimpleString::from("src")),
            ..Default::default()
        },
        body,
    );
    broker
        .post_office()
        .queue("src")
        .unwrap()
        .add(MessageRef::new(message));
    // Its body file goes missing: the copy cannot write the new body.
    std::fs::remove_file(&path).unwrap();

    let copied = op(
        &broker,
        "queue.src",
        "copyMessage",
        vec![Json::Long(id), Json::from("dst")],
    )
    .await;
    assert!(
        copied.is_err(),
        "the failed copy must be reported: {copied:?}"
    );
    assert_eq!(count(&broker, "dst").await, Json::Long(0));
    assert_eq!(count(&broker, "src").await, Json::Long(1));
    broker.stop().unwrap();
}

/// Refuses to route messages to one address, as a plugin can.
struct RefuseTo(&'static str);

impl saranga_server::plugin::BrokerPlugin for RefuseTo {
    fn before_message_route(
        &self,
        m: &ServerMessage,
    ) -> Result<(), saranga_server::error::BrokerError> {
        if m.core().address.as_ref().is_some_and(|a| a.eq_str(self.0)) {
            return Err(saranga_server::error::BrokerError::internal("refused"));
        }
        Ok(())
    }
}

/// A move whose copy cannot be routed (a plugin refuses it) fails and
/// keeps the original, as Artemis's `move` throws out of its routing: it
/// does not acknowledge the source with the copy gone nowhere.
#[tokio::test]
async fn a_move_whose_copy_is_refused_keeps_the_message() {
    for persistence in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let broker = start(dir.path(), persistence).await;
        create_queue(&broker, "src").await;
        create_queue(&broker, "dst").await;
        let id = broker.post_office().ids().next();
        broker
            .post_office()
            .queue("src")
            .unwrap()
            .add(MessageRef::new(ServerMessage::new(CoreMessage {
                message_id: id,
                durable: true,
                address: Some(SimpleString::from("src")),
                ..Default::default()
            })));
        broker.register_plugin(Arc::new(RefuseTo("dst")));
        let _ = op(
            &broker,
            "queue.src",
            "moveMessages",
            vec![Json::Null, Json::from("dst")],
        )
        .await;
        assert_eq!(
            count(&broker, "src").await,
            Json::Long(1),
            "persistence {persistence}: the original stays"
        );
        assert_eq!(count(&broker, "dst").await, Json::Long(0));
        broker.stop().unwrap();
    }
}

/// A copy a plugin refuses is not made, and the operation says so.
#[tokio::test]
async fn a_copy_a_plugin_refuses_is_not_made() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), true).await;
    create_queue(&broker, "src").await;
    create_queue(&broker, "dst").await;
    let id = broker.post_office().ids().next();
    broker
        .post_office()
        .queue("src")
        .unwrap()
        .add(MessageRef::new(ServerMessage::new(CoreMessage {
            message_id: id,
            durable: true,
            address: Some(SimpleString::from("src")),
            ..Default::default()
        })));
    broker.register_plugin(Arc::new(RefuseTo("dst")));
    let copied = op(
        &broker,
        "queue.src",
        "copyMessage",
        vec![Json::Long(id), Json::from("dst")],
    )
    .await;
    assert!(copied.is_err(), "the refused copy is reported: {copied:?}");
    assert_eq!(count(&broker, "src").await, Json::Long(1));
    assert_eq!(count(&broker, "dst").await, Json::Long(0));
    broker.stop().unwrap();
}

/// An expiry whose copy a plugin refuses keeps the message where it is
/// (`move` routed by address, as dead-letter and expiry moves are).
#[tokio::test]
async fn an_expiry_whose_copy_is_refused_keeps_the_message() {
    use saranga_server::settings::{AddressSettings, UniformSettings};
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.settings = Arc::new(UniformSettings(Arc::new(AddressSettings {
        expiry_address: Some("exp".into()),
        ..Default::default()
    })));
    let broker = Broker::start(config).await.unwrap();
    create_queue(&broker, "src").await;
    create_queue(&broker, "exp").await;
    let q = broker.post_office().queue("src").unwrap();
    q.add(MessageRef::new(ServerMessage::new(CoreMessage {
        message_id: broker.post_office().ids().next(),
        address: Some(SimpleString::from("src")),
        expiration: 1,
        ..Default::default()
    })));
    broker.register_plugin(Arc::new(RefuseTo("exp")));
    q.expire_scan();
    assert_eq!(q.browse().await.len(), 1, "the original stays");
    assert_eq!(count(&broker, "exp").await, Json::Long(0));
    broker.stop().unwrap();
}
