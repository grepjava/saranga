//! Moving a message to a paging destination pages the copy inside the same
//! transaction that acknowledges the source (Artemis's `QueueImpl.move`). If
//! the move's journal write fails, neither half may take effect: the copy must
//! not reach the destination's pages while the source stays unacknowledged
//! (a duplicate). The old move paged the copy non-transactionally, so a failed
//! acknowledgement left the copy behind.

use std::path::Path;
use std::sync::Arc;

use saranga_buffer::SimpleString;
use saranga_fault::{Fault, Op};
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::queue::{Selection, SelectionAction};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings>
               <address-setting match="dst">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>10240</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
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

async fn send(session: &mut ServerSession, address: &str) {
    let message = CoreMessage {
        address: Some(SimpleString::from(address)),
        durable: true,
        body: vec![7u8; 2000].into(),
        ..Default::default()
    };
    session.send(message, None).await.unwrap();
    session.take_completion().wait().await.unwrap();
}

async fn create_queue(broker: &Arc<Broker>, name: &str) {
    broker
        .management()
        .invoke(
            broker,
            "broker",
            "createQueue",
            &[
                Json::from(name),
                Json::from("ANYCAST"),
                Json::from(name),
                Json::Null,
                Json::Bool(true),
                Json::Long(-1),
                Json::Bool(false),
                Json::Bool(true),
            ],
            Default::default(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn a_failed_move_to_a_paging_address_leaves_no_copy() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    create_queue(&broker, "src").await;
    create_queue(&broker, "dst").await;

    let mut s = session(&broker);
    // The destination fills and starts paging on this first message, which
    // stays in its queue; the moved copy is what gets paged.
    send(&mut s, "dst").await;
    send(&mut s, "src").await;

    let src = broker.post_office().queue("src").unwrap();
    let dst = broker.post_office().queue("dst").unwrap();
    assert!(
        broker.post_office().paging_store("dst").is_paging(),
        "the destination must be paging for the move to page its copy"
    );
    assert_eq!(src.message_count(), 1);
    assert_eq!(dst.message_count(), 1);

    // Fail every journal write while the move runs: its transaction cannot
    // commit, so the copy must not be counted at the destination.
    let fault = Fault::new(Op::Write, dir.path().join("data").join("journal")).arm();
    src.act(
        Selection::filter(None),
        SelectionAction::Move {
            address: "dst".into(),
            target: None,
            reject_duplicates: false,
        },
    )
    .await;
    // Let any deferred journal completion fail and roll back.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (src_count, dst_count) = (src.message_count(), dst.message_count());
    drop(fault);

    assert_eq!(src_count, 1, "the source message is not acknowledged");
    assert_eq!(
        dst_count, 1,
        "a failed move must not leave the copy in the destination's pages"
    );
}
