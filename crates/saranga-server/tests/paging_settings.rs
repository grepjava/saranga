//! Address settings applied to a store already in use (a reload,
//! `addAddressSettings`) take effect at once, as Artemis's `applySetting`:
//! a producer blocked under the old limit goes on under a higher one
//! (`size.setMax` → `underSized`), and new page limits are checked
//! (`checkClearPageLimit`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &std::path::Path, settings: &str) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings><address-setting match="a">{settings}</address-setting></address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap(),
    )
    .await
    .unwrap();
    let mut spec = QueueSpec::new("a", "a");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
    b
}

fn session(b: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        b.clone(),
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

async fn send(s: &mut ServerSession) -> Result<(), String> {
    s.send(
        CoreMessage {
            address: Some("a".into()),
            durable: true,
            body: vec![1; 1000].into(),
            ..Default::default()
        },
        None,
    )
    .await
    .map_err(|e| format!("{e:?}"))?;
    s.take_completion()
        .wait()
        .await
        .map_err(|e| format!("{e:?}"))
}

/// Under BLOCK, a producer waiting for room goes on once the limit is
/// raised above what the address holds.
#[tokio::test]
async fn a_higher_max_size_releases_a_blocked_producer() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(
        dir.path(),
        "<max-size-bytes>1000</max-size-bytes><address-full-policy>BLOCK</address-full-policy>",
    )
    .await;
    let mut s = session(&b);
    send(&mut s).await.unwrap();
    let store = b.post_office().paging_store("a");
    assert!(store.is_full());
    let granted = Arc::new(AtomicBool::new(false));
    let g = granted.clone();
    assert!(store.check_memory(
        false,
        Box::new(move || g.store(true, Ordering::SeqCst)),
        None,
        None
    ));
    assert!(!granted.load(Ordering::SeqCst), "blocked");
    let mut settings = (*store.address_settings()).clone();
    settings.max_size_bytes = 1_000_000;
    store.apply_settings(Arc::new(settings));
    assert!(
        granted.load(Ordering::SeqCst),
        "released under the higher limit"
    );
    b.stop().unwrap();
}

/// A store found full by `page-limit-messages` takes messages again once
/// the limit is raised, without waiting for a cleanup.
#[tokio::test]
async fn a_higher_page_limit_lets_the_address_page_again() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(
        dir.path(),
        "<max-size-bytes>1</max-size-bytes>
         <page-size-bytes>10240</page-size-bytes>
         <address-full-policy>PAGE</address-full-policy>
         <page-limit-messages>2</page-limit-messages>
         <page-full-policy>FAIL</page-full-policy>",
    )
    .await;
    let mut s = session(&b);
    let mut refused = false;
    for _ in 0..6 {
        if send(&mut s).await.is_err() {
            refused = true;
            break;
        }
    }
    assert!(refused, "the page limit refuses messages");
    let store = b.post_office().paging_store("a");
    let mut settings = (*store.address_settings()).clone();
    settings.page_limit_messages = Some(100);
    store.apply_settings(Arc::new(settings));
    send(&mut s).await.expect("pages again under the new limit");

    // And a lower page-limit-bytes is found over at once.
    let mut settings = (*store.address_settings()).clone();
    settings.page_limit_messages = None;
    settings.page_limit_bytes = Some(1);
    store.apply_settings(Arc::new(settings));
    assert!(send(&mut s).await.is_err(), "over the new page limit");
    b.stop().unwrap();
}

/// `global` in broker.xml; `global_max_size`, if any, set as broker
/// properties may set it (broker.xml takes only a positive size).
async fn start_global(
    dir: &std::path::Path,
    global: &str,
    global_max_size: Option<i64>,
    settings: &str,
) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r##"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             {global}
             <address-settings><address-setting match="#">{settings}</address-setting></address-settings>
           </core></configuration>"##
    ))
    .unwrap();
    let mut config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    if let Some(size) = global_max_size {
        config.global_max_size = size;
    }
    let b = Broker::start(config).await.unwrap();
    for name in ["a", "b"] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.durable = true;
        b.post_office().create_queue(spec, false).unwrap();
    }
    b
}

fn flag() -> (Arc<AtomicBool>, saranga_server::paging::Grant) {
    let granted = Arc::new(AtomicBool::new(false));
    let g = granted.clone();
    (granted, Box::new(move || g.store(true, Ordering::SeqCst)))
}

/// Under BLOCK with only the broker's `global-max-messages`, a producer
/// blocked by another address's messages goes on once they are consumed.
#[tokio::test]
async fn a_producer_blocked_by_the_global_message_limit_is_released() {
    let dir = tempfile::tempdir().unwrap();
    let b = start_global(
        dir.path(),
        "<global-max-messages>1</global-max-messages>",
        Some(-1),
        "<address-full-policy>BLOCK</address-full-policy>",
    )
    .await;
    let mut s = session(&b);
    send(&mut s).await.unwrap();
    // Address b holds nothing: it is blocked by a's message.
    let store = b.post_office().paging_store("b");
    let (granted, grant) = flag();
    assert!(store.check_memory(false, grant, None, None));
    assert!(!granted.load(Ordering::SeqCst), "blocked");
    b.post_office()
        .queue("a")
        .unwrap()
        .delete_matching(None)
        .await;
    assert!(granted.load(Ordering::SeqCst), "released");
    b.stop().unwrap();
}

/// What waits for the broker's memory alone (an anonymous AMQP producer)
/// goes on once there is room, though no address blocked.
#[tokio::test]
async fn a_wait_for_the_global_memory_is_released_without_a_blocked_store() {
    let dir = tempfile::tempdir().unwrap();
    let b = start_global(
        dir.path(),
        "<global-max-size>1000</global-max-size>",
        None,
        "<address-full-policy>PAGE</address-full-policy>",
    )
    .await;
    let mut s = session(&b);
    send(&mut s).await.unwrap();
    let manager = b.post_office().paging_store("a").manager().unwrap();
    assert!(manager.is_global_full());
    let (granted, grant) = flag();
    manager.check_memory(grant);
    assert!(!granted.load(Ordering::SeqCst), "waits for memory");
    b.post_office()
        .queue("a")
        .unwrap()
        .delete_matching(None)
        .await;
    assert!(granted.load(Ordering::SeqCst), "released");
    b.stop().unwrap();
}

/// A paging address blocked through management lets its producers go when
/// unblocked, though it is still past its size, as it always is while it
/// pages.
#[tokio::test]
async fn an_unblocked_paging_address_releases_its_producers() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(
        dir.path(),
        "<max-size-bytes>1</max-size-bytes><address-full-policy>PAGE</address-full-policy>",
    )
    .await;
    let mut s = session(&b);
    send(&mut s).await.unwrap();
    let store = b.post_office().paging_store("a");
    assert!(store.is_full());
    store.block();
    let (granted, grant) = flag();
    assert!(!store.check_memory(false, grant, None, None));
    assert!(!granted.load(Ordering::SeqCst), "blocked");
    store.unblock();
    assert!(granted.load(Ordering::SeqCst), "released");
    b.stop().unwrap();
}

/// A send with a duplicate ID (so in a journal transaction of its own)
/// paged on one address and refused by another: the page transaction on
/// the first goes with the failed send, and its page can be cleaned up.
#[tokio::test]
async fn a_failed_send_rolls_back_what_it_paged_elsewhere() {
    use saranga_buffer::{PropertyValue, SimpleString, TypedProperties};
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <addresses>
               <address name="src"><anycast><queue name="src"/></anycast></address>
               <address name="dst"><anycast><queue name="dst"/></anycast></address>
             </addresses>
             <diverts>
               <divert name="d">
                 <address>src</address>
                 <forwarding-address>dst</forwarding-address>
                 <exclusive>false</exclusive>
               </divert>
             </diverts>
             <address-settings>
               <address-setting match="src">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>10240</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
               <address-setting match="dst">
                 <max-size-bytes>1</max-size-bytes>
                 <address-full-policy>FAIL</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let mut s = session(&b);
    let message = |dup: Option<&str>| {
        let mut properties = TypedProperties::default();
        if let Some(d) = dup {
            properties.insert(
                SimpleString::from("_AMQ_DUPL_ID"),
                PropertyValue::String(SimpleString::from(d)),
            );
        }
        CoreMessage {
            address: Some("src".into()),
            durable: true,
            body: vec![1; 1000].into(),
            properties,
            ..Default::default()
        }
    };
    // The first fills dst (and src).
    s.send(message(None), None).await.unwrap();
    s.take_completion().wait().await.unwrap();
    let store = b.post_office().paging_store("src");
    assert!(store.is_paging(), "src pages");
    assert!(
        s.send(message(Some("d1")), None).await.is_err(),
        "dst refuses"
    );
    let _ = s.take_completion().wait().await;

    for q in ["src", "dst"] {
        b.post_office()
            .queue(q)
            .unwrap()
            .delete_matching(None)
            .await;
    }
    for _ in 0..50 {
        store.cleanup();
        if !store.is_paging() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!store.is_paging(), "src's pages are cleaned up");
    b.stop().unwrap();
}
