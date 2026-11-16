//! Which failures the plugins' exception hooks hear of, as in Artemis: a
//! refusal by `beforeSend` or `beforeMessageRoute` is not an exception of
//! the send or the route (those hooks run before the `try`), and whatever
//! fails after `beforeSend` is reported to `onSendException`.

use std::sync::{Arc, Mutex};

use saranga_buffer::SimpleString;
use saranga_message::CoreMessage;
use saranga_server::error::BrokerError;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::plugin::BrokerPlugin;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::registry::SessionEntry;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

#[derive(Default)]
struct Plugin {
    events: Mutex<Vec<&'static str>>,
    refuse_send: bool,
    refuse_route: bool,
}

impl Plugin {
    fn take(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

impl BrokerPlugin for Plugin {
    fn before_send(
        &self,
        _s: &SessionEntry,
        _m: &CoreMessage,
        _transacted: bool,
    ) -> Result<(), BrokerError> {
        self.events.lock().unwrap().push("before_send");
        match self.refuse_send {
            true => Err(BrokerError::internal("no")),
            false => Ok(()),
        }
    }
    fn on_send_exception(&self, _s: &SessionEntry, _m: &CoreMessage, _e: &BrokerError) {
        self.events.lock().unwrap().push("on_send_exception");
    }
    fn before_message_route(&self, _m: &ServerMessage) -> Result<(), BrokerError> {
        self.events.lock().unwrap().push("before_message_route");
        match self.refuse_route {
            true => Err(BrokerError::internal("no")),
            false => Ok(()),
        }
    }
    fn on_message_route_exception(&self, _m: &ServerMessage, _e: &BrokerError) {
        self.events
            .lock()
            .unwrap()
            .push("on_message_route_exception");
    }
}

async fn start(plugin: Arc<Plugin>) -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.plugins.push(plugin);
    Broker::start(config).await.unwrap()
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

#[tokio::test]
async fn a_send_refused_by_before_send_is_no_send_exception() {
    let plugin = Arc::new(Plugin {
        refuse_send: true,
        ..Default::default()
    });
    let broker = start(plugin.clone()).await;
    let mut s = session(&broker);
    let m = CoreMessage {
        address: Some(SimpleString::from("a")),
        ..Default::default()
    };
    assert!(s.send(m, None).await.is_err());
    assert_eq!(plugin.take(), vec!["before_send"]);
}

#[tokio::test]
async fn a_send_with_no_address_is_a_send_exception() {
    let plugin = Arc::new(Plugin::default());
    let broker = start(plugin.clone()).await;
    let mut s = session(&broker);
    assert!(s.send(CoreMessage::default(), None).await.is_err());
    assert_eq!(plugin.take(), vec!["before_send", "on_send_exception"]);
}

#[tokio::test]
async fn a_route_refused_by_before_message_route_is_no_route_exception() {
    let plugin = Arc::new(Plugin {
        refuse_route: true,
        ..Default::default()
    });
    let broker = start(plugin.clone()).await;
    let r = MessageRef::new(ServerMessage::new(CoreMessage {
        address: Some(SimpleString::from("a")),
        ..Default::default()
    }));
    assert!(broker.post_office().route_internal(&r, "a").is_err());
    let q = broker
        .post_office()
        .create_queue(saranga_server::postoffice::QueueSpec::new("q", "a"), false)
        .unwrap();
    plugin.take();
    assert!(broker.post_office().route_moved(&r, q).is_err());
    assert_eq!(plugin.take(), vec!["before_message_route"]);
}
