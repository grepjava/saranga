//! Broker plugins: the hooks a message's life calls, a plugin refusing what
//! is about to happen, `can_accept` keeping a message from a consumer, the
//! `<broker-plugins>` of broker.xml and the notification plugin.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::error::BrokerError;
use saranga_server::message::ServerMessage;
use saranga_server::plugin::{BrokerPlugin, RoutingStatus};
use saranga_server::postoffice::{AddressInfo, QueueSpec, SelectorFilters};
use saranga_server::queue::{AckReason, DeliverySink, Queue};
use saranga_server::registry::{ConsumerEntry, SessionEntry};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Transaction;
use saranga_server::{Broker, BrokerConfig};

/// Writes down the hooks called, by name.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<String>>,
    /// `before_create_queue` refuses these queues.
    refuse_queue: Option<&'static str>,
    /// `can_accept` refuses messages with this property.
    refuse_property: Option<&'static str>,
}

impl Recorder {
    fn push(&self, e: String) {
        self.events.lock().unwrap().push(e);
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

impl BrokerPlugin for Recorder {
    fn registered(&self, _broker: &Arc<Broker>) {
        self.push("registered".into());
    }
    fn unregistered(&self, _broker: &Broker) {
        self.push("unregistered".into());
    }
    fn after_create_connection(&self, _c: &ClientConnection) {
        self.push("after_create_connection".into());
    }
    fn after_destroy_connection(&self, _c: &ClientConnection) {
        self.push("after_destroy_connection".into());
    }
    fn after_create_session(&self, s: &SessionEntry) {
        self.push(format!("after_create_session {}", s.name));
    }
    fn before_close_session(&self, s: &SessionEntry, failed: bool) {
        self.push(format!("before_close_session {} {failed}", s.name));
    }
    fn after_close_session(&self, s: &SessionEntry, failed: bool) {
        self.push(format!("after_close_session {} {failed}", s.name));
    }
    fn before_create_consumer(
        &self,
        id: i64,
        queue: &str,
        _filter: Option<&str>,
        _browse_only: bool,
    ) -> Result<(), BrokerError> {
        self.push(format!("before_create_consumer {id} {queue}"));
        Ok(())
    }
    fn after_create_consumer(&self, c: &ConsumerEntry) {
        self.push(format!("after_create_consumer {}", c.id));
    }
    fn before_close_consumer(&self, c: &ConsumerEntry, _failed: bool) {
        self.push(format!("before_close_consumer {}", c.id));
    }
    fn after_close_consumer(&self, c: &ConsumerEntry, _failed: bool) {
        self.push(format!("after_close_consumer {}", c.id));
    }
    fn after_add_address(&self, info: &AddressInfo, reload: bool) {
        self.push(format!("after_add_address {} {reload}", info.name));
    }
    fn after_remove_address(&self, address: &str, info: Option<&AddressInfo>) {
        self.push(format!("after_remove_address {address} {}", info.is_some()));
    }
    fn before_create_queue(&self, spec: &QueueSpec) -> Result<(), BrokerError> {
        self.push(format!("before_create_queue {}", spec.name));
        match self.refuse_queue {
            Some(q) if *spec.name == *q => Err(BrokerError::internal("refused")),
            _ => Ok(()),
        }
    }
    fn after_create_queue(&self, queue: &Queue) {
        self.push(format!("after_create_queue {}", queue.name()));
    }
    fn before_destroy_queue(&self, name: &str) -> Result<(), BrokerError> {
        self.push(format!("before_destroy_queue {name}"));
        Ok(())
    }
    fn after_destroy_queue(&self, queue: &Queue, _check: bool, _auto_delete: bool) {
        self.push(format!("after_destroy_queue {}", queue.name()));
    }
    fn after_add_binding(&self, name: &str, address: &str) {
        self.push(format!("after_add_binding {name} {address}"));
    }
    fn after_remove_binding(&self, name: &str, address: &str) {
        self.push(format!("after_remove_binding {name} {address}"));
    }
    fn before_send(
        &self,
        _session: &SessionEntry,
        message: &CoreMessage,
        transacted: bool,
    ) -> Result<(), BrokerError> {
        self.push(format!("before_send {} {transacted}", message.message_id));
        Ok(())
    }
    fn after_send(
        &self,
        _session: &SessionEntry,
        message: &ServerMessage,
        result: RoutingStatus,
        _tx: Option<&Transaction>,
    ) {
        self.push(format!(
            "after_send {} {}",
            message.message_id(),
            result.name()
        ));
    }
    fn before_message_route(&self, message: &ServerMessage) -> Result<(), BrokerError> {
        self.push(format!("before_message_route {}", message.message_id()));
        Ok(())
    }
    fn after_message_route(&self, message: &ServerMessage, result: RoutingStatus) {
        self.push(format!(
            "after_message_route {} {}",
            message.message_id(),
            result.name()
        ));
    }
    fn can_accept(&self, _consumer: &ConsumerEntry, message: &ServerMessage) -> bool {
        self.refuse_property
            .is_none_or(|p| !message.core().properties.get_str(p).is_some())
    }
    fn before_deliver(&self, c: &ConsumerEntry, message: &ServerMessage) {
        self.push(format!("before_deliver {} {}", c.id, message.message_id()));
    }
    fn after_deliver(&self, c: &ConsumerEntry, message: &ServerMessage) {
        self.push(format!("after_deliver {} {}", c.id, message.message_id()));
    }
    fn message_acknowledged(
        &self,
        reference: &saranga_server::message::MessageRef,
        queue: &Queue,
        reason: AckReason,
    ) {
        self.push(format!(
            "message_acknowledged {} {} {reason:?}",
            reference.message_id(),
            queue.name()
        ));
    }
}

/// Keeps the IDs of the messages delivered.
#[derive(Default)]
struct Sink(Mutex<Vec<i64>>);

impl DeliverySink for Sink {
    fn deliver(
        &self,
        _consumer_id: i64,
        message: &Arc<ServerMessage>,
        _delivery_count: i32,
    ) -> i64 {
        self.0.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        unreachable!()
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        unreachable!()
    }
    fn chunk_size(&self) -> usize {
        100 * 1024
    }
}

async fn start(plugin: Arc<dyn BrokerPlugin>) -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.plugins = vec![plugin];
    Broker::start(config).await.unwrap()
}

fn connection() -> Arc<ClientConnection> {
    Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(saranga_security::ConnectionInfo {
            remote_address: "test".into(),
            certificates: None,
            security_domain: None,
            connection_id: None,
        }),
        Vec::new(),
    ))
}

fn session(broker: &Arc<Broker>, conn: &Arc<ClientConnection>) -> ServerSession {
    ServerSession::new(
        broker.clone(),
        conn.clone(),
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

fn message(broker: &Broker, address: &str, property: Option<&str>) -> CoreMessage {
    let mut m = CoreMessage {
        message_id: broker.post_office().ids().next(),
        address: Some(SimpleString::from(address)),
        ..Default::default()
    };
    if let Some(p) = property {
        m.properties
            .insert(SimpleString::from(p), PropertyValue::Bool(true));
    }
    m
}

async fn wait_for(what: impl Fn() -> bool) {
    for _ in 0..200 {
        if what() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out");
}

#[tokio::test]
async fn hooks_follow_a_message() {
    let recorder = Arc::new(Recorder::default());
    let broker = start(recorder.clone()).await;
    broker
        .post_office()
        .create_queue(QueueSpec::new("q", "a"), false)
        .unwrap();
    let conn = connection();
    let registration = broker.registry().add(&conn);
    let mut s = session(&broker, &conn);
    let sink = Arc::new(Sink::default());
    let consumer_sink = sink.clone();
    s.create_consumer(7, "q", None, 0, false, move |_| consumer_sink)
        .await
        .unwrap();
    s.consumer_credits(7, -1);
    s.set_started(true).await;
    let m = message(&broker, "a", None);
    let id = m.message_id;
    s.send_with_id(m).await.unwrap();
    wait_for(|| sink.0.lock().unwrap().contains(&id)).await;
    s.acknowledge(7, id, false).await.unwrap();
    let q = broker.post_office().queue("q").unwrap();
    wait_for(|| q.message_count() == 0).await;
    s.close(false).await;
    drop(registration);
    broker
        .post_office()
        .delete_queue_awaited("q", false, true)
        .await
        .unwrap();
    broker.stop().unwrap();

    let events = recorder.events();
    let expected = [
        "registered".to_owned(),
        "before_create_queue q".to_owned(),
        "after_add_address a false".to_owned(),
        "after_add_binding q a".to_owned(),
        "after_create_queue q".to_owned(),
        "after_create_connection".to_owned(),
        "after_create_session s".to_owned(),
        "before_create_consumer 7 q".to_owned(),
        "after_create_consumer 7".to_owned(),
        format!("before_send {id} false"),
        format!("before_message_route {id}"),
        format!("after_message_route {id} OK"),
        format!("after_send {id} OK"),
        format!("before_deliver 7 {id}"),
        format!("after_deliver 7 {id}"),
        format!("message_acknowledged {id} q Normal"),
        "before_close_session s false".to_owned(),
        "before_close_consumer 7".to_owned(),
        "after_close_consumer 7".to_owned(),
        "after_close_session s false".to_owned(),
        "after_destroy_connection".to_owned(),
        "before_destroy_queue q".to_owned(),
        "after_remove_binding q a".to_owned(),
        "after_destroy_queue q".to_owned(),
        "after_remove_address a true".to_owned(),
        "unregistered".to_owned(),
    ];
    // The broker's own addresses (the notification address) come and go,
    // and its notifications are routed, too; only what this test did is
    // compared.
    let ours: Vec<_> = events
        .into_iter()
        .filter(|e| !e.contains("activemq.notifications"))
        .filter(|e| !e.contains("_message_route ") || e.contains(&format!(" {id}")))
        .collect();
    assert_eq!(ours, expected);
}

#[tokio::test]
async fn a_message_sent_nowhere_has_no_bindings() {
    let recorder = Arc::new(Recorder::default());
    let broker = start(recorder.clone()).await;
    broker
        .post_office()
        .add_or_update_address(
            "nowhere",
            saranga_server::postoffice::RoutingTypes::of(saranga_server::RoutingType::Multicast),
            false,
            false,
        )
        .unwrap();
    let conn = connection();
    let mut s = session(&broker, &conn);
    let m = message(&broker, "nowhere", None);
    let id = m.message_id;
    s.send_with_id(m).await.unwrap();
    let events = recorder.events();
    assert!(
        events.contains(&format!("after_message_route {id} NO_BINDINGS")),
        "{events:?}"
    );
    assert!(
        events.contains(&format!("after_send {id} NO_BINDINGS")),
        "{events:?}"
    );
}

#[tokio::test]
async fn a_plugin_can_refuse_a_queue() {
    let recorder = Arc::new(Recorder {
        refuse_queue: Some("no"),
        ..Default::default()
    });
    let broker = start(recorder.clone()).await;
    let e = broker
        .post_office()
        .create_queue(QueueSpec::new("no", "no"), false)
        .unwrap_err();
    assert!(e.to_string().contains("refused"), "{e}");
    assert!(broker.post_office().queue("no").is_none());
    assert!(broker.post_office().address_info("no").is_none());
}

#[tokio::test]
async fn can_accept_keeps_a_message_from_a_consumer() {
    let recorder = Arc::new(Recorder {
        refuse_property: Some("secret"),
        ..Default::default()
    });
    let broker = start(recorder).await;
    broker
        .post_office()
        .create_queue(QueueSpec::new("q", "q"), false)
        .unwrap();
    let conn = connection();
    let mut s = session(&broker, &conn);
    let sink = Arc::new(Sink::default());
    let consumer_sink = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| consumer_sink)
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    let secret = message(&broker, "q", Some("secret"));
    let open = message(&broker, "q", None);
    let open_id = open.message_id;
    s.send_with_id(secret).await.unwrap();
    s.send_with_id(open).await.unwrap();
    wait_for(|| sink.0.lock().unwrap().contains(&open_id)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*sink.0.lock().unwrap(), [open_id]);
    assert_eq!(broker.post_office().queue("q").unwrap().message_count(), 2);
}

fn file_config(plugins: &str) -> cfg::Result<BrokerConfig> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <broker-plugins>{plugins}</broker-plugins>
           </core></configuration>"#
    ))?;
    BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), std::path::Path::new("."))
}

#[test]
fn broker_xml_names_the_plugins() {
    let config = file_config(
        r#"<broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin">
             <property key="LOG_ALL_EVENTS" value="true"/>
           </broker-plugin>
           <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.BrokerMessageAuthorizationPlugin"/>"#,
    )
    .unwrap();
    assert_eq!(config.plugins.len(), 2);

    // Java code Saranga cannot run stops the broker from starting.
    let e = file_config(r#"<broker-plugin class-name="com.example.MyPlugin"/>"#)
        .err()
        .unwrap();
    assert!(e.to_string().contains("com.example.MyPlugin"), "{e}");
    // As does a plugin's own configuration error (sanityCheckConfig).
    let e = file_config(
        r#"<broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.ConnectionPeriodicExpiryPlugin"/>"#,
    )
    .err()
    .unwrap();
    assert!(e.to_string().contains("acceptorMatchRegex"), "{e}");
}

#[tokio::test]
async fn the_notification_plugin_announces_addresses() {
    let mut config = file_config(
        r#"<broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.NotificationActiveMQServerPlugin">
             <property key="SEND_ADDRESS_NOTIFICATIONS" value="true"/>
           </broker-plugin>"#,
    )
    .unwrap();
    config.storage = None;
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("watch", "activemq.notifications");
    spec.durable = false;
    spec.routing_type = Some(saranga_server::RoutingType::Multicast);
    let watch = broker.post_office().create_queue(spec, false).unwrap();
    broker
        .post_office()
        .add_or_update_address(
            "x",
            saranga_server::postoffice::RoutingTypes::of(saranga_server::RoutingType::Anycast),
            false,
            false,
        )
        .unwrap();
    let kind = |r: &saranga_server::message::MessageRef, key: &str| {
        r.message.core().properties.get_str(key).cloned()
    };
    let mut found = None;
    for _ in 0..200 {
        found = watch.browse().await.into_iter().find(|r| {
            kind(r, "_AMQ_NotifType") == Some(PropertyValue::String("ADDRESS_ADDED".into()))
                && kind(r, "_AMQ_Address") == Some(PropertyValue::String("x".into()))
        });
        if found.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let found = found.expect("an ADDRESS_ADDED notification for x");
    // ANYCAST.
    assert_eq!(
        kind(&found, "_AMQ_Routing_Type"),
        Some(PropertyValue::Byte(1))
    );
}
