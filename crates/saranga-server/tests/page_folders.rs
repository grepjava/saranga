//! Page folders go with their addresses (`deletePageStore`), and with
//! `purge-page-folders` once an address stops paging (`purgeFolder`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, purge: bool) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <purge-page-folders>{purge}</purge-page-folders>
             <address-settings><address-setting match="a">
               <max-size-bytes>1</max-size-bytes>
               <page-size-bytes>10240</page-size-bytes>
               <address-full-policy>PAGE</address-full-policy>
             </address-setting></address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    Broker::start(BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap())
        .await
        .unwrap()
}

fn page_folders(dir: &Path) -> Vec<PathBuf> {
    let paging = dir.join("data").join("paging");
    std::fs::read_dir(&paging)
        .map(|r| {
            r.flatten()
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect()
        })
        .unwrap_or_default()
}

/// Two messages sent to `a`, the second paged.
async fn page(b: &Arc<Broker>) {
    let mut spec = QueueSpec::new("a", "a");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
    let mut s = ServerSession::new(
        b.clone(),
        Arc::new(ClientConnection::new(
            Transport::internal("test"),
            Arc::new(Default::default()),
            vec![],
        )),
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
    for _ in 0..2 {
        s.send(
            CoreMessage {
                address: Some("a".into()),
                durable: true,
                body: vec![1; 100].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    s.close(false).await;
    assert!(b.post_office().paging_store("a").is_paging());
}

#[tokio::test]
async fn a_removed_address_takes_its_page_folder() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), false).await;
    page(&b).await;
    assert_eq!(page_folders(dir.path()).len(), 1);
    let q = b.post_office().queue("a").unwrap();
    q.delete_matching(None).await;
    drop(q);
    b.post_office()
        .delete_queue_awaited("a", false, false)
        .await
        .unwrap();
    assert!(b.post_office().remove_address("a"));
    assert!(
        page_folders(dir.path()).is_empty(),
        "{:?}",
        page_folders(dir.path())
    );
    b.stop().unwrap();
}

#[tokio::test]
async fn a_folder_without_its_address_goes_at_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), false).await;
    page(&b).await;
    b.stop().unwrap();
    drop(b);
    // An address no longer there, with a folder.
    let stray = dir.path().join("data").join("paging").join("stray");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(stray.join("address.txt"), "gone\n").unwrap();
    let b = start(dir.path(), false).await;
    assert!(!stray.exists(), "removed");
    assert_eq!(page_folders(dir.path()).len(), 1, "a's is kept");
    b.stop().unwrap();
}

#[tokio::test]
async fn purge_page_folders_removes_the_folder_once_paging_stops() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), true).await;
    page(&b).await;
    assert_eq!(page_folders(dir.path()).len(), 1);
    b.post_office()
        .queue("a")
        .unwrap()
        .delete_matching(None)
        .await;
    let store = b.post_office().paging_store("a");
    for _ in 0..50 {
        store.cleanup();
        if !store.is_paging() && page_folders(dir.path()).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!store.is_paging());
    assert!(page_folders(dir.path()).is_empty(), "purged");
    b.stop().unwrap();
}
