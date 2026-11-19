//! Acknowledgements the target confirmed but the bridge's acknowledging task
//! has not yet made when the connection fails are made, under the consumer
//! the messages went to, before the bridge's consumer goes: the messages are
//! not given back to be sent twice, and no pending acknowledgement is left
//! for a stop to wait out. A plugin holds the acknowledging task on its first
//! acknowledgement (standing in for the queue round trip of each) while the
//! connection fails.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use saranga_client::message;
use saranga_client::*;
use saranga_server::plugin::BrokerPlugin;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;

const N: u32 = 20;

#[derive(Default)]
struct Gate {
    calls: AtomicUsize,
    released: Mutex<bool>,
    cv: Condvar,
}

impl BrokerPlugin for Gate {
    fn after_acknowledge_bridge(&self, _bridge: &str, _id: i64) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut r = self.released.lock().unwrap();
            while !*r {
                r = self.cv.wait(r).unwrap();
            }
        }
    }
}

const QUEUE: &str = r#"<addresses>
     <address name="dst"><anycast><queue name="dst"/></anycast></address>
     <address name="src"><anycast><queue name="src"/></anycast></address>
   </addresses>"#;

async fn start(core: &str, plugin: Option<Arc<Gate>>) -> (Arc<Broker>, u16) {
    let dir = tempfile::tempdir().unwrap().keep();
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <security-enabled>false</security-enabled>
             {core}
           </core></configuration>"#
    ))
    .unwrap();
    let mut config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), &dir).unwrap();
    if let Some(p) = plugin {
        config.plugins.push(p);
    }
    let broker = Broker::start(config).await.unwrap();
    let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(broker.clone().serve_acceptor(l, Acceptor::default()));
    (broker, port)
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..500 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmed_sends_are_acknowledged_across_a_connection_failure() {
    scenario(2000).await;
}

async fn scenario(retry: u32) {
    let gate = Arc::new(Gate::default());
    let (target, tport) = start(QUEUE, None).await;
    let (source, sport) = start(
        &format!(
            r#"<connectors><connector name="target">tcp://127.0.0.1:{tport}</connector></connectors>
               {QUEUE}
               <bridges><bridge name="br">
                 <queue-name>src</queue-name>
                 <forwarding-address>dst</forwarding-address>
                 <retry-interval>{retry}</retry-interval>
                 <reconnect-attempts>-1</reconnect-attempts>
                 <use-duplicate-detection>false</use-duplicate-detection>
                 <confirmation-window-size>1</confirmation-window-size>
                 <pending-ack-timeout>3000</pending-ack-timeout>
                 <static-connectors><connector-ref>target</connector-ref></static-connectors>
               </bridge></bridges>"#
        ),
        Some(gate.clone()),
    )
    .await;
    let bridge = source.bridges().get("br").unwrap();
    wait_until("connect", || bridge.is_connected()).await;

    let conn = Connection::connect(("127.0.0.1", sport), ConnectionOptions::default())
        .await
        .unwrap();
    let s = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    let p = s.create_producer(Some("src")).await.unwrap();
    for i in 0..N {
        let mut m = message::text_message(&format!("m{i}"));
        m.durable = true;
        p.send(m).await.unwrap();
    }
    let dst = target.post_office().queue("dst").unwrap();
    let src = source.post_office().queue("src").unwrap();
    wait_until("target has all", || dst.message_count() == N as i64).await;
    // Confirmations for all N reach the source; the acker is held on the first.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The target drops the bridge's connection.
    for c in target.registry().connections() {
        c.fail();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    *gate.released.lock().unwrap() = true;
    gate.cv.notify_all();

    wait_until("reconnect", || bridge.is_connected()).await;
    wait_until("source empties", || src.message_count() == 0).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(
        dst.message_count(),
        N as i64,
        "messages the target had were sent again"
    );
    let t = Instant::now();
    bridge.stop().await.unwrap();
    assert!(
        t.elapsed() < Duration::from_millis(2000),
        "a stop with nothing in flight waited {:?} for a pending acknowledgement",
        t.elapsed()
    );
    conn.close().await;
}
