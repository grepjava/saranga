//! A page removed from the middle of the stream is counted out once: the
//! regular stream walking over its missing file later does not count it
//! again (PagingStoreImpl.depage puts the count back), so paging goes on
//! while the current page still holds messages.

use std::path::Path;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings>
               <address-setting match="pg">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>2500</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
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

async fn send(session: &mut ServerSession, n: i32) {
    let mut message = CoreMessage {
        address: Some(SimpleString::from("pg")),
        durable: true,
        body: vec![7u8; 1000].into(),
        ..Default::default()
    };
    message.properties.insert("n".into(), PropertyValue::Int(n));
    session.send(message, None).await.unwrap();
    session.take_completion().wait().await.unwrap();
}

fn page_files(dir: &Path) -> usize {
    let paging = dir.join("data").join("paging");
    let mut n = 0;
    for folder in std::fs::read_dir(paging).unwrap().flatten() {
        if folder.path().is_dir() {
            n += std::fs::read_dir(folder.path())
                .unwrap()
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(".page"))
                .count();
        }
    }
    n
}

#[tokio::test]
async fn a_page_removed_mid_stream_is_counted_once() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    op(
        &broker,
        "broker",
        "createQueue",
        vec![
            Json::from("pg"),
            Json::from("ANYCAST"),
            Json::from("pg"),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await;
    let mut s = session(&broker);
    for n in 0..10 {
        send(&mut s, n).await;
    }
    let store = broker.post_office().paging_store("pg");
    let remove = |f: &'static str| {
        let broker = broker.clone();
        async move {
            let r = op(&broker, "queue.pg", "removeMessages", vec![Json::from(f)]).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            r
        }
    };
    for f in ["n=3", "n=5 OR n=6", "n=1 OR n=2", "n=4", "n=7", "n=8"] {
        remove(f).await;
    }
    // m0 (in memory) and m9 (in the current page) are left.
    assert_eq!(
        op(&broker, "queue.pg", "countMessages", vec![]).await,
        Json::Long(2),
        "two messages are left"
    );
    assert_eq!(
        store.number_of_pages() as usize,
        page_files(dir.path()),
        "the page count matches the page files"
    );
    assert!(store.is_paging(), "the current page still holds m9");
}
