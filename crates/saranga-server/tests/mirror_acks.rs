//! An acknowledgement mirrored to several `sync` mirrors completes once each
//! one's peer has it, not only the last one's.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::mirror::{MirrorController, Origin};
use saranga_server::postoffice::{AddressInfo, QueueSpec, SelectorFilters};
use saranga_server::queue::{AckReason, Queue};
use saranga_server::storage::Pending;
use saranga_server::{Broker, BrokerConfig};

/// A `sync` mirror whose peer has an acknowledgement when the test says so.
#[derive(Default)]
struct SyncMirror(Mutex<Vec<saranga_journal::Completion>>);

impl SyncMirror {
    fn peer_has_it(&self) {
        for c in self.0.lock().unwrap().drain(..) {
            c(Ok(()));
        }
    }
}

impl MirrorController for SyncMirror {
    fn add_address(&self, _: &AddressInfo) {}
    fn delete_address(&self, _: &AddressInfo) {}
    fn create_queue(&self, _: &Queue) {}
    fn delete_queue(&self, _: &Queue) {}
    fn send_message(&self, _: &MessageRef, _: Option<&Origin>) -> Option<Queue> {
        None
    }
    fn sync_wait(&self, _: i64) -> Option<Pending> {
        None
    }
    fn acknowledged(
        &self,
        _: &Queue,
        _: &MessageRef,
        _: AckReason,
        _: Option<&Origin>,
    ) -> Option<Pending> {
        let (c, p) = Pending::new();
        self.0.lock().unwrap().push(c);
        Some(p)
    }
    fn remote_mirror_id(&self) -> Option<String> {
        None
    }
}

#[tokio::test]
async fn an_acknowledgement_waits_for_every_sync_mirror() {
    let b = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    let q = b.post_office().create_queue(spec, false).unwrap();
    let mirrors: Vec<Arc<SyncMirror>> = (0..2).map(|_| Arc::default()).collect();
    for m in &mirrors {
        b.post_office().mirrors().install(m.clone());
    }
    let r = MessageRef::new(ServerMessage::new(CoreMessage {
        message_id: 1,
        address: Some("q".into()),
        ..Default::default()
    }));
    let pending = q.post_acknowledge(&r).expect("sync mirrors to wait for");
    let mut wait = Box::pin(pending.wait());
    // The last mirror's peer has it; the first's does not yet.
    mirrors[1].peer_has_it();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut wait)
            .await
            .is_err(),
        "completed before the first mirror's peer had it"
    );
    mirrors[0].peer_has_it();
    tokio::time::timeout(Duration::from_secs(3), wait)
        .await
        .expect("completes once both have it")
        .unwrap();
}
