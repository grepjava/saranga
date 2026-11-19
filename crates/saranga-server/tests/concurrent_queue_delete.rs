//! Deletions of one queue that overlap: one takes it and the other finds
//! no such queue (`removeBinding` finds nothing the second time), and a
//! queue made again under the name meanwhile is left alone.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_server::plugin::BrokerPlugin;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::Queue;
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn broker() -> Arc<Broker> {
    Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap()
}

fn spec(name: &str, address: &str, rt: RoutingType) -> QueueSpec {
    let mut s = QueueSpec::new(name, address);
    s.auto_create_address = Some(true);
    s.routing_type = Some(rt);
    s
}

/// Pauses the first `before_destroy_queue` until released, counts
/// `after_destroy_queue`.
#[derive(Default)]
struct Pause {
    calls: AtomicUsize,
    destroyed: AtomicUsize,
    gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

impl BrokerPlugin for Pause {
    fn before_destroy_queue(&self, _: &str) -> Result<(), saranga_server::BrokerError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(tx) = self.entered.lock().unwrap().take() {
                tx.send(()).unwrap();
            }
            let rx = self.gate.lock().unwrap().take().unwrap();
            rx.recv().unwrap();
        }
        Ok(())
    }
    fn after_destroy_queue(&self, _: &Queue, _: bool, _: bool) {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
    }
}

fn paused(
    po: &saranga_server::postoffice::PostOffice,
) -> (
    Arc<Pause>,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
) {
    let (gate_tx, gate_rx) = std::sync::mpsc::channel();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let plugin = Arc::new(Pause::default());
    *plugin.gate.lock().unwrap() = Some(gate_rx);
    *plugin.entered.lock().unwrap() = Some(entered_tx);
    po.plugins().register(plugin.clone());
    (plugin, gate_tx, entered_rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_of_two_overlapping_deletions_takes_the_queue() {
    let b = broker().await;
    let po = b.post_office().clone();
    let (plugin, gate, entered) = paused(&po);
    po.create_queue(spec("q", "q", RoutingType::Anycast), false)
        .unwrap();
    let po1 = po.clone();
    let first = std::thread::spawn(move || po1.delete_queue("q", false, false));
    entered.recv().unwrap();
    let po2 = po.clone();
    let second = std::thread::spawn(move || po2.delete_queue("q", false, false));
    std::thread::sleep(Duration::from_millis(200));
    gate.send(()).unwrap();
    let (first, second) = (first.join().unwrap(), second.join().unwrap());
    assert!(first.is_ok() != second.is_ok(), "{first:?} {second:?}");
    assert_eq!(plugin.destroyed.load(Ordering::SeqCst), 1);
    b.stop().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_deletion_leaves_a_queue_made_again() {
    let b = broker().await;
    let po = b.post_office().clone();
    let (_plugin, gate, entered) = paused(&po);
    po.create_queue(spec("q", "q", RoutingType::Anycast), false)
        .unwrap();
    let po1 = po.clone();
    let first = std::thread::spawn(move || po1.delete_queue("q", false, false));
    entered.recv().unwrap();
    po.delete_queue("q", false, false).unwrap();
    let fresh = po
        .create_queue(spec("q", "q", RoutingType::Anycast), false)
        .unwrap();
    gate.send(()).unwrap();
    assert!(
        first.join().unwrap().is_err(),
        "the queue it looked up is gone"
    );
    assert_eq!(po.queue("q").map(|q| q.id()), Some(fresh.id()));
    b.stop().unwrap();
}
