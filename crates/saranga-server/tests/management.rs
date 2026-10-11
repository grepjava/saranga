//! Management state that outlives a restart: a persisted queue pause,
//! address settings and diverts added through management, all kept in the
//! bindings journal as Artemis keeps them. The operations themselves are
//! covered against Artemis by tests/interop/management/Management.java.

use std::path::Path;
use std::sync::Arc;

use saranga_config as cfg;
use saranga_server::management::Json;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

async fn op(broker: &Arc<Broker>, resource: &str, operation: &str, params: Vec<Json>) -> Json {
    broker
        .management()
        .invoke(broker, resource, operation, &params, Default::default())
        .await
        .unwrap_or_else(|e| panic!("{operation}: {e}"))
}

async fn attr(broker: &Arc<Broker>, resource: &str, attribute: &str) -> Json {
    broker
        .management()
        .attribute(broker, resource, attribute, Default::default())
        .await
        .unwrap_or_else(|e| panic!("{attribute}: {e}"))
}

fn s(v: &str) -> Json {
    Json::from(v)
}

#[tokio::test]
async fn management_state_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    op(
        &broker,
        "broker",
        "createAddress",
        vec![s("a"), s("ANYCAST")],
    )
    .await;
    op(
        &broker,
        "broker",
        "createQueue",
        vec![
            s("a"),
            s("ANYCAST"),
            s("a"),
            Json::Null,
            Json::from(true),
            Json::from(-1),
            Json::from(false),
            Json::from(false),
        ],
    )
    .await;
    op(
        &broker,
        "broker",
        "createAddress",
        vec![s("b"), s("ANYCAST")],
    )
    .await;
    op(&broker, "queue.a", "pause", vec![Json::from(true)]).await;
    op(&broker, "address.b", "pause", vec![Json::from(true)]).await;
    op(
        &broker,
        "broker",
        "addAddressSettings",
        vec![
            s("a.#"),
            s(r#"{"maxDeliveryAttempts":7,"deadLetterAddress":"X"}"#),
        ],
    )
    .await;
    // Replaced: only the second record remains.
    op(
        &broker,
        "broker",
        "addAddressSettings",
        vec![
            s("a.#"),
            s(r#"{"maxDeliveryAttempts":8,"deadLetterAddress":"Y"}"#),
        ],
    )
    .await;
    op(
        &broker,
        "broker",
        "createDivert",
        vec![
            s("d"),
            s("d"),
            s("a"),
            s("b"),
            Json::from(false),
            Json::Null,
            Json::Null,
            s("ANYCAST"),
        ],
    )
    .await;
    broker.stop().unwrap();
    drop(broker);

    let broker = start(dir.path()).await;
    assert_eq!(attr(&broker, "queue.a", "Paused").await, Json::from(true));
    assert_eq!(
        attr(&broker, "queue.a", "PersistedPause").await,
        Json::from(true)
    );
    assert_eq!(attr(&broker, "address.b", "Paused").await, Json::from(true));
    let settings = op(
        &broker,
        "broker",
        "getAddressSettingsAsJSON",
        vec![s("a.x")],
    )
    .await;
    let settings = Json::parse(settings.as_str().unwrap()).unwrap();
    assert_eq!(settings.get("maxDeliveryAttempts"), Some(&Json::from(8)));
    assert_eq!(settings.get("deadLetterAddress"), Some(&s("Y")));
    assert_eq!(attr(&broker, "divert.d", "ForwardingAddress").await, s("b"));

    // Resumed and removed, nothing comes back.
    op(&broker, "queue.a", "resume", vec![]).await;
    op(&broker, "address.b", "resume", vec![]).await;
    op(&broker, "broker", "removeAddressSettings", vec![s("a.#")]).await;
    op(&broker, "broker", "destroyDivert", vec![s("d")]).await;
    broker.stop().unwrap();
    drop(broker);

    let broker = start(dir.path()).await;
    assert_eq!(attr(&broker, "queue.a", "Paused").await, Json::from(false));
    assert_eq!(
        attr(&broker, "address.b", "Paused").await,
        Json::from(false)
    );
    let settings = op(
        &broker,
        "broker",
        "getAddressSettingsAsJSON",
        vec![s("a.x")],
    )
    .await;
    assert_eq!(settings.as_str(), Some("{}"));
    assert!(
        broker
            .management()
            .attribute(&broker, "divert.d", "ForwardingAddress", Default::default())
            .await
            .is_err()
    );
}

/// An XA session on a connection of its own.
fn xa_session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(saranga_security::ConnectionInfo {
            remote_address: "test".into(),
            certificates: None,
            security_domain: None,
            connection_id: None,
        }),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "xa".into(),
            username: None,
            password: None,
            xa: true,
            auto_commit_sends: false,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn heuristics(j: Json) -> Vec<Json> {
    match j {
        Json::Array(v) => v,
        other => panic!("not an array: {other}"),
    }
}

#[tokio::test]
async fn heuristic_completion_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let xid = Xid {
        format_id: 7,
        branch_qualifier: b"bq".to_vec(),
        global_transaction_id: b"gtx".to_vec(),
    };
    let b64 = saranga_server::transaction::xid_base64(&xid);
    let mut session = xa_session(&broker);
    session.xa_start(xid.clone()).unwrap();
    session.xa_end(xid.clone()).unwrap();
    session.xa_prepare(xid.clone()).unwrap();
    session.take_completion().wait().await.unwrap();
    assert_eq!(
        op(
            &broker,
            "broker",
            "commitPreparedTransaction",
            vec![s(&b64)]
        )
        .await,
        Json::from(true)
    );
    broker.stop().unwrap();
    drop(session);
    drop(broker);

    let broker = start(dir.path()).await;
    let committed = op(
        &broker,
        "broker",
        "listHeuristicCommittedTransactions",
        vec![],
    )
    .await;
    assert_eq!(heuristics(committed), vec![s(&b64)]);
    assert_eq!(broker.resource_manager().in_doubt(), vec![xid.clone()]);
    let mut session = xa_session(&broker);
    session.xa_forget(xid.clone()).unwrap();
    session.take_completion().wait().await.unwrap();
    broker.stop().unwrap();
    drop(session);
    drop(broker);

    let broker = start(dir.path()).await;
    let committed = op(
        &broker,
        "broker",
        "listHeuristicCommittedTransactions",
        vec![],
    )
    .await;
    assert!(heuristics(committed).is_empty());
}

/// A broker with a security store (security disabled, as the store exists
/// either way).
async fn start_with_security(dir: &Path) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let mut config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let manager = saranga_security::JaasSecurityManager::new(
        saranga_security::LoginConfig::default(),
        None,
        "activemq",
        None,
    );
    config.security = Some(Arc::new(saranga_security::SecurityStore::new(
        saranga_security::SecurityStoreConfig::from_broker(&file),
        file.security_repository().unwrap(),
        Arc::new(manager),
    )));
    Broker::start(config).await.unwrap()
}

#[tokio::test]
async fn security_settings_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_with_security(dir.path()).await;
    let set = |json: &str| vec![s("secured.#"), s(json)];
    op(
        &broker,
        "broker",
        "addSecuritySettings",
        set(r#"{"send":["x"]}"#),
    )
    .await;
    // Replaced: only the second record remains.
    op(
        &broker,
        "broker",
        "addSecuritySettings",
        set(r#"{"send":["a","b"],"consume":["a"],"manage":["admin"],"view":["a"]}"#),
    )
    .await;
    op(
        &broker,
        "broker",
        "addSecuritySettings",
        vec![s("gone.#"), s(r#"{"send":["g"]}"#)],
    )
    .await;
    op(
        &broker,
        "broker",
        "removeSecuritySettings",
        vec![s("gone.#")],
    )
    .await;
    broker.stop().unwrap();
    drop(broker);

    let broker = start_with_security(dir.path()).await;
    let roles = op(&broker, "broker", "getRolesAsJSON", vec![s("secured.x")]).await;
    let roles = Json::parse(roles.as_str().unwrap()).unwrap();
    let Json::Array(roles) = roles else { panic!() };
    let by_name = |n: &str| {
        roles
            .iter()
            .find(|r| r.get("name") == Some(&s(n)))
            .unwrap_or_else(|| panic!("no role {n}"))
            .clone()
    };
    assert_eq!(roles.len(), 3);
    assert_eq!(by_name("a").get("send"), Some(&Json::from(true)));
    assert_eq!(by_name("a").get("consume"), Some(&Json::from(true)));
    // Artemis restores the view and edit roles as none, and grants manage to
    // a role whose name appears in the manage list's text.
    assert_eq!(by_name("a").get("view"), Some(&Json::from(false)));
    assert_eq!(by_name("a").get("manage"), Some(&Json::from(true)));
    assert_eq!(by_name("b").get("manage"), Some(&Json::from(false)));
    assert_eq!(by_name("admin").get("manage"), Some(&Json::from(true)));
    let gone = op(&broker, "broker", "getRolesAsJSON", vec![s("gone.x")]).await;
    assert_eq!(gone.as_str(), Some("[]"));
}

#[tokio::test]
async fn security_violations_are_notified() {
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <security-enabled>true</security-enabled>
           </core></configuration>"#,
    )
    .unwrap();
    let mut config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let manager = saranga_security::JaasSecurityManager::new(
        saranga_security::LoginConfig::default(),
        None,
        "activemq",
        None,
    );
    let store = Arc::new(saranga_security::SecurityStore::new(
        saranga_security::SecurityStoreConfig::from_broker(&file),
        file.security_repository().unwrap(),
        Arc::new(manager),
    ));
    config.security = Some(store.clone());
    let broker = Broker::start(config).await.unwrap();
    // The notification address exists once the broker is active.
    assert!(
        broker
            .post_office()
            .address_info("activemq.notifications")
            .is_some()
    );
    let mut queue = QueueSpec::new("notified", "activemq.notifications");
    queue.routing_type = Some(saranga_server::RoutingType::Multicast);
    queue.durable = false;
    broker.post_office().create_queue(queue, false).unwrap();

    let info = saranga_security::ConnectionInfo {
        remote_address: "/10.0.0.1:1234".into(),
        certificates: None,
        security_domain: None,
        connection_id: None,
    };
    assert!(store.authenticate(Some("bob"), Some("x"), &info).is_err());
    let auth = saranga_security::SecurityAuth {
        username: Some("carol".into()),
        password: None,
        connection: Arc::new(info),
        subject: None,
    };
    assert!(
        store
            .check("orders::q", Some("q"), cfg::CheckType::Send, &auth)
            .is_err()
    );
    let q = broker.post_office().queue("notified").unwrap();
    for _ in 0..100 {
        // The queue's own BINDING_ADDED, then the two violations.
        if q.message_count() == 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let listed = op(&broker, "queue.notified", "listMessages", vec![s("")]).await;
    let Json::Array(messages) = listed else {
        panic!("{listed}")
    };
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].get("_AMQ_NotifType"), Some(&s("BINDING_ADDED")));
    let auth_failed = &messages[1];
    assert_eq!(
        auth_failed.get("_AMQ_NotifType"),
        Some(&s("SECURITY_AUTHENTICATION_VIOLATION"))
    );
    assert_eq!(auth_failed.get("_AMQ_User"), Some(&s("bob")));
    assert_eq!(
        auth_failed.get("_AMQ_RemoteAddress"),
        Some(&s("10.0.0.1:1234"))
    );
    assert_eq!(
        auth_failed.get("_AMQ_CertSubjectDN"),
        Some(&s("unavailable"))
    );
    let denied = &messages[2];
    assert_eq!(
        denied.get("_AMQ_NotifType"),
        Some(&s("SECURITY_PERMISSION_VIOLATION"))
    );
    assert_eq!(denied.get("_AMQ_Address"), Some(&s("orders")));
    assert_eq!(denied.get("_AMQ_CheckType"), Some(&s("SEND")));
    assert_eq!(denied.get("_AMQ_User"), Some(&s("carol")));
}
