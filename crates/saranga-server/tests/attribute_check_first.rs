//! An attribute read over management checks the caller's permission on
//! its getter before the getter runs (`ManagementServiceImpl.getAttribute`):
//! a refused caller has no value worked out and no read audited. The
//! getter is `is<Attribute>` for the boolean attributes the control
//! declares so, without reading the value first.

use std::sync::{Arc, Mutex};

use saranga_security::SecurityAuth;
use saranga_security::jaas::{Principal, Subject};
use saranga_server::management::Caller;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Broker, BrokerConfig};

/// Every event's message, of any target.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<String>>>);

struct Message<'a>(&'a mut String);

impl tracing::field::Visit for Message<'_> {
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        if f.name() == "message" {
            *self.0 = format!("{v:?}");
        }
    }
}

impl tracing::Subscriber for Events {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, e: &tracing::Event<'_>) {
        let mut m = String::new();
        e.record(&mut Message(&mut m));
        self.0.lock().unwrap().push(m);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

async fn broker(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r##"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <security-enabled>true</security-enabled>
             <management-message-rbac>true</management-message-rbac>
             <security-settings>
               <security-setting match="mops.queue.q.isPaused">
                 <permission type="view" roles="viewer"/>
               </security-setting>
             </security-settings>
           </core></configuration>"##,
    )
    .unwrap();
    let mut config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    config.security = Some(Arc::new(saranga_security::SecurityStore::new(
        saranga_security::SecurityStoreConfig::from_broker(&file),
        file.security_repository().unwrap(),
        Arc::new(saranga_security::JaasSecurityManager::new(
            saranga_security::LoginConfig::default(),
            None,
            "activemq",
            None,
        )),
    )));
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    broker.post_office().create_queue(spec, false).unwrap();
    broker
}

fn auth(role: Option<&str>) -> SecurityAuth {
    let mut subject = Subject::default();
    subject.add(Principal::User("u".to_owned()));
    if let Some(role) = role {
        subject.add(Principal::Role(role.to_owned()));
    }
    SecurityAuth {
        subject: Some(Arc::new(subject)),
        management: true,
        ..SecurityAuth::default()
    }
}

#[tokio::test]
async fn a_refused_read_is_neither_worked_out_nor_audited() {
    let dir = tempfile::tempdir().unwrap();
    let broker = broker(dir.path()).await;
    let store = broker.security().unwrap().clone();
    let events = Events::default();
    let _guard = tracing::subscriber::set_default(events.clone());

    let refused = auth(None);
    let caller = Caller {
        security: Some((&store, &refused)),
        who: Some(saranga_server::audit::Who {
            subject: refused.subject.as_deref(),
            remote: Some("test"),
        }),
    };
    let m = broker.management();
    assert!(
        m.attribute(&broker, "queue.q", "messageCount", caller)
            .await
            .is_err()
    );
    let seen = events.0.lock().unwrap().clone();
    assert!(
        seen.iter()
            .any(|e| e.contains("mops.queue.q.getMessageCount")),
        "the refusal names the getter: {seen:?}"
    );
    assert!(
        !seen.iter().any(|e| e.contains("is getting message count")),
        "a refused read was audited as run: {seen:?}"
    );

    // A boolean attribute is checked as its `is` getter before it is read.
    let viewer = auth(Some("viewer"));
    let caller = Caller {
        security: Some((&store, &viewer)),
        who: None,
    };
    assert_eq!(
        m.getter(&broker, "queue.q", "paused").as_deref(),
        Some("isPaused")
    );
    assert!(
        m.attribute(&broker, "queue.q", "paused", caller)
            .await
            .is_ok()
    );
    broker.stop().unwrap();
}
