//! A large message body is read, copied and synced (and a page file read)
//! off the runtime's workers: on a slow disk, the other tasks of the worker
//! doing it keep running. The disk is made slow with a stalling fault.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use saranga_fault::{Fault, Op};
use saranga_server::large::LargeMessageStore;

const STALL: Duration = Duration::from_millis(400);

/// A task counting every 10 ms.
fn ticker() -> (Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let ticks = Arc::new(AtomicUsize::new(0));
    let counted = ticks.clone();
    let task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            counted.fetch_add(1, Ordering::Relaxed);
        }
    });
    (ticks, task)
}

/// Runs `f` on a task of a one-worker runtime while the ticker runs, and
/// returns how often it ticked meanwhile.
async fn ticks_during(f: impl FnOnce() + Send + 'static) -> usize {
    let (ticks, task) = ticker();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = ticks.load(Ordering::Relaxed);
    tokio::spawn(async move { f() }).await.unwrap();
    let during = ticks.load(Ordering::Relaxed) - before;
    task.abort();
    during
}

fn body(store: &Arc<LargeMessageStore>, id: i64) -> Arc<saranga_server::large::LargeBody> {
    let mut w = store.create(id, true).unwrap();
    w.append(&vec![7u8; 64 * 1024]).unwrap();
    w.finish().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_large_body_is_copied_and_read_off_the_workers() {
    let dir = tempfile::tempdir().unwrap();
    let store = LargeMessageStore::new(dir.path().to_owned(), true);
    store.start().unwrap();
    let original = body(&store, 1);

    let _slow = Fault::new(Op::Read, dir.path()).stall(STALL).arm();
    let source = original.clone();
    let copied = ticks_during(move || {
        let copy = source.copy(2, true).unwrap();
        assert_eq!(copy.size(), 64 * 1024);
    })
    .await;
    assert!(
        copied >= 10,
        "the worker stopped while a copy waited: {copied} ticks"
    );

    let source = original.clone();
    let read = ticks_during(move || {
        assert_eq!(source.read_all().unwrap().len(), 64 * 1024);
    })
    .await;
    assert!(
        read >= 10,
        "the worker stopped while a read waited: {read} ticks"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_large_body_is_synced_off_the_workers() {
    let dir = tempfile::tempdir().unwrap();
    let store = LargeMessageStore::new(dir.path().to_owned(), true);
    store.start().unwrap();
    let _slow = Fault::new(Op::Sync, dir.path()).stall(STALL).arm();
    let writing = store.clone();
    let synced = ticks_during(move || {
        body(&writing, 3);
    })
    .await;
    assert!(
        synced >= 10,
        "the worker stopped while a sync waited: {synced} ticks"
    );
}
