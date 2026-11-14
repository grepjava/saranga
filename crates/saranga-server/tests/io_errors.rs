//! Storage I/O errors outside the journals: what is a critical I/O error
//! (the broker stops) and what is only logged.

use std::sync::Arc;

use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};

async fn broker(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    Broker::start(BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap())
        .await
        .unwrap()
}

/// Writing a large message's copy (a divert, dead-letter or expiry copy) is
/// a critical I/O error when it fails, as Artemis's `copyInto` writes it.
#[tokio::test]
async fn a_large_copy_that_cannot_be_written_is_critical() {
    let dir = tempfile::tempdir().unwrap();
    let b = broker(dir.path()).await;
    let mut w = b.large_messages().create(90001, true).unwrap();
    w.append(b"body").unwrap();
    let body = w.finish().unwrap();
    // A directory where the copy's file would go.
    let target = body.path().unwrap().parent().unwrap().join("90002.msg");
    std::fs::create_dir(&target).unwrap();
    assert!(body.copy(90002, true).is_err());
    assert!(b.is_critical_io_failed());
}

/// A copy whose source body is gone fails, but nothing is wrong with the
/// storage: the broker goes on.
#[tokio::test]
async fn a_large_copy_without_its_source_is_not_critical() {
    let dir = tempfile::tempdir().unwrap();
    let b = broker(dir.path()).await;
    let mut w = b.large_messages().create(90003, true).unwrap();
    w.append(b"body").unwrap();
    let body = w.finish().unwrap();
    std::fs::remove_file(body.path().unwrap()).unwrap();
    assert!(body.copy(90004, true).is_err());
    assert!(!b.is_critical_io_failed());
    b.stop().unwrap();
}

/// A large message's body that cannot be deleted is logged, as Artemis's
/// `SequentialFile.delete` logs it, and left for the next start: not a
/// critical I/O error.
#[cfg(windows)]
#[tokio::test]
async fn a_large_body_that_cannot_be_deleted_is_not_critical() {
    use std::os::windows::fs::OpenOptionsExt;
    let dir = tempfile::tempdir().unwrap();
    let b = broker(dir.path()).await;
    let mut w = b.large_messages().create(90005, true).unwrap();
    w.append(b"body").unwrap();
    let body = w.finish().unwrap();
    let path = body.path().unwrap().to_owned();
    // Open without FILE_SHARE_DELETE, so the deletion fails.
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(3)
        .open(&path)
        .unwrap();
    drop(body);
    let remains = path.exists();
    drop(held);
    assert!(remains, "the deletion failed");
    assert!(!b.is_critical_io_failed());
    b.stop().unwrap();
}

/// A page whose messages are all acknowledged but whose file cannot be
/// deleted keeps its acknowledgements, and the broker stops on the
/// critical I/O error: after a restart the messages are still
/// acknowledged. (Artemis deletes the acknowledgements anyway, and the
/// messages come back.)
#[cfg(windows)]
#[tokio::test]
async fn a_page_that_cannot_be_deleted_keeps_its_acknowledgements() {
    use saranga_server::postoffice::QueueSpec;
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    use std::os::windows::fs::OpenOptionsExt;
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings>
               <address-setting match="q">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>10240</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let config =
        || BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let b = Broker::start(config()).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    let q = b.post_office().create_queue(spec, false).unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut s = ServerSession::new(
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
    );
    for _ in 0..8 {
        s.send(
            saranga_message::CoreMessage {
                address: Some("q".into()),
                durable: true,
                body: vec![1; 2000].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    fn pages(p: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(p).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                pages(&p, out);
            } else if p.extension().is_some_and(|e| e == "page") {
                out.push(p);
            }
        }
    }
    let mut paths = vec![];
    pages(dir.path(), &mut paths);
    assert!(!paths.is_empty(), "paged");
    // Opened without FILE_SHARE_DELETE, so they cannot be deleted.
    let held: Vec<_> = paths
        .iter()
        .map(|p| {
            std::fs::OpenOptions::new()
                .read(true)
                .share_mode(3)
                .open(p)
                .unwrap()
        })
        .collect();
    for _ in 0..10 {
        q.delete_matching(None).await;
        b.post_office().paging_store("q").cleanup();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert_eq!(q.message_count(), 0);
    assert!(paths.iter().any(|p| p.exists()), "a page was left");
    assert!(b.is_critical_io_failed(), "a critical I/O error");
    drop(held);
    s.close(true).await;
    let _ = b.stop();
    drop(s);
    drop(q);
    drop(b);

    let b = Broker::start(config()).await.unwrap();
    let count = b.post_office().queue("q").unwrap().message_count();
    b.stop().unwrap();
    // The first page's messages stay acknowledged. The later ones'
    // acknowledgements reach the journal after the critical I/O error has
    // stopped its writes, so they are not stored and those messages come
    // back.
    assert!(
        count <= 3,
        "the acknowledged messages stay acknowledged: {count} back"
    );
}

/// After a critical I/O error outside the journals (here a large message
/// copy), the journals take no more writes while the broker stops.
#[tokio::test]
async fn a_critical_error_stops_the_journal_writes() {
    let dir = tempfile::tempdir().unwrap();
    let b = broker(dir.path()).await;
    let mut w = b.large_messages().create(90011, true).unwrap();
    w.append(b"body").unwrap();
    let body = w.finish().unwrap();
    let target = body.path().unwrap().parent().unwrap().join("90012.msg");
    std::fs::create_dir(&target).unwrap();
    assert!(body.copy(90012, true).is_err());
    assert!(b.is_critical_io_failed());
    let message = saranga_server::message::ServerMessage::new(saranga_message::CoreMessage {
        message_id: 90013,
        durable: true,
        address: Some("q".into()),
        ..Default::default()
    });
    let storage = b.storage().unwrap();
    assert!(
        storage.store_message(&message).is_err(),
        "the message journal takes no more writes"
    );
    assert!(
        storage.add_binding(90014, 44, vec![1]).is_err(),
        "the bindings journal takes no more writes"
    );
}

/// An exclusive divert whose large message copy cannot be made fails the
/// send rather than drop the message it takes.
#[tokio::test]
async fn a_divert_that_cannot_copy_a_large_body_fails_the_send() {
    use saranga_server::message::{MessageRef, ServerMessage};
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
                 <exclusive>true</exclusive>
               </divert>
             </diverts>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let id = b.post_office().ids().next();
    let mut w = b.large_messages().create(id, true).unwrap();
    w.append(&[1u8; 200_000]).unwrap();
    let body = w.finish().unwrap();
    let path = body.path().unwrap().to_owned();
    let message = ServerMessage::new_large(
        saranga_message::CoreMessage {
            message_id: id,
            durable: true,
            address: Some("src".into()),
            ..Default::default()
        },
        body,
    );
    // Its body file goes missing: the divert cannot copy it.
    std::fs::remove_file(&path).unwrap();
    let routed = b
        .post_office()
        .route(&MessageRef::new(message), "src", None, Default::default());
    assert!(routed.is_err(), "the send fails: {routed:?}");
    b.stop().unwrap();
}

/// A transaction that pages across several page files has all of them on
/// disk when it commits: leaving a page syncs it, as Artemis's
/// `Page.close` does.
#[tokio::test]
async fn a_commit_has_every_page_it_wrote_synced() {
    use saranga_fault::{Fault, Op};
    use saranga_server::postoffice::QueueSpec;
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings>
               <address-setting match="q">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>10240</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
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
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut s = ServerSession::new(
        b.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: false,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    // Syncing the first page fails.
    let fault = Fault::new(Op::Sync, dir.path())
        .named(saranga_paging::page_file_name(1))
        .arm();
    let mut sent = true;
    for _ in 0..20 {
        let r = s
            .send(
                saranga_message::CoreMessage {
                    address: Some("q".into()),
                    durable: true,
                    body: vec![1; 2000].into(),
                    ..Default::default()
                },
                None,
            )
            .await;
        if r.is_err() {
            sent = false;
            break;
        }
    }
    let committed = sent && s.commit().is_ok() && s.take_completion().wait().await.is_ok();
    assert!(fault.hits() > 0, "the first page was synced");
    assert!(!committed, "the transaction does not commit");
    drop(fault);
    s.close(true).await;
    let _ = b.stop();
}
