//! Core federation between in-process Saranga brokers: an upstream's
//! address and queue policies make consumers on the other broker, and a
//! downstream asks the other broker to federate from this one.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

struct Node {
    broker: Arc<Broker>,
    port: u16,
    acceptor: JoinHandle<std::io::Result<()>>,
}

async fn start(dir: &std::path::Path, listener: TcpListener, core: &str) -> Node {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
             {core}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    Node {
        broker,
        port,
        acceptor,
    }
}

async fn stop(n: Node) {
    n.acceptor.abort();
    let _ = n.acceptor.await;
    for c in n.broker.registry().connections() {
        c.disconnect();
    }
    n.broker.stop().unwrap();
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn session(port: u16) -> (Connection, Session) {
    let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions {
            ack_batch_size: 0,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    session.start().await.unwrap();
    (conn, session)
}

async fn send(session: &Session, address: &str, n: u32, hops: Option<i32>) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in 0..n {
        let mut m = message::text_message(&format!("m{i}"));
        if let Some(h) = hops {
            m.put_property("_AMQ_Hops", PropertyValue::Bytes(h.to_be_bytes().to_vec()));
        }
        producer.send(m).await.unwrap();
    }
    producer.close();
}

async fn drain(consumer: &Consumer, n: usize) -> Vec<CoreMessage> {
    let mut out = Vec::new();
    while out.len() < n {
        let d = consumer
            .receive(WAIT)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("only {} of {n} messages", out.len()));
        consumer.acknowledge(&d).await.unwrap();
        out.push(d.message);
    }
    out
}

fn texts(ms: &[CoreMessage]) -> Vec<String> {
    ms.iter().map(|m| m.text().unwrap().unwrap()).collect()
}

const ADDRESSES: &str = r#"<addresses>
    <address name="news"><multicast/></address>
    <address name="orders"><anycast><queue name="orders"/></anycast></address>
  </addresses>"#;

/// The upstream broker, then the one federating from it with `federation`.
async fn pair(
    dirs: &(tempfile::TempDir, tempfile::TempDir),
    federation: impl Fn(u16, u16) -> String,
) -> (Node, Node) {
    let lu = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let ld = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let (pu, pd) = (
        lu.local_addr().unwrap().port(),
        ld.local_addr().unwrap().port(),
    );
    let up = start(dirs.0.path(), lu, ADDRESSES).await;
    let down = start(
        dirs.1.path(),
        ld,
        &format!(
            r#"<connectors>
                 <connector name="me">tcp://127.0.0.1:{pd}</connector>
                 <connector name="up">tcp://127.0.0.1:{pu}</connector>
               </connectors>
               {ADDRESSES}
               {}"#,
            federation(pu, pd)
        ),
    )
    .await;
    (up, down)
}

fn dirs() -> (tempfile::TempDir, tempfile::TempDir) {
    (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
}

fn upstream(_: u16, _: u16) -> String {
    r##"<federations>
         <federation name="f">
           <upstream name="up">
             <static-connectors><connector-ref>up</connector-ref></static-connectors>
             <policy ref="both"/>
           </upstream>
           <policy-set name="both"><policy ref="news"/><policy ref="orders"/></policy-set>
           <queue-policy name="orders"><include queue-match="#" address-match="orders"/></queue-policy>
           <address-policy name="news"><include address-match="news"/></address-policy>
         </federation>
       </federations>"##
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_address_with_a_queue_here_federates_its_messages() {
    let d = dirs();
    let (up, down) = pair(&d, upstream).await;
    let (_cd, sd) = session(down.port).await;
    sd.create_queue(
        QueueConfiguration::new("sub")
            .address("news")
            .routing_type(RoutingType::Multicast),
    )
    .await
    .unwrap();
    let subscriber = sd.create_consumer("sub", None, false).await.unwrap();
    // The upstream gets the federation's queue and its consumer.
    let remote = "federated.f.up.news.multicast";
    wait_until("the upstream has the federated queue", || {
        up.broker
            .post_office()
            .queue(remote)
            .is_some_and(|q| q.consumer_count() == 1)
    })
    .await;
    let q = up.broker.post_office().queue(remote).unwrap();
    let c = q.config();
    assert!(c.durable && c.auto_delete);
    assert_eq!(c.auto_delete_delay, 3_600_000);
    assert_eq!(
        c.filter_string.as_deref(),
        Some("_AMQ_Hops IS NULL OR _AMQ_Hops<0")
    );

    let (_cu, su) = session(up.port).await;
    send(&su, "news", 5, None).await;
    let got = drain(&subscriber, 5).await;
    assert_eq!(texts(&got), ["m0", "m1", "m2", "m3", "m4"]);
    // One hop more.
    assert!(
        got.iter()
            .all(|m| { m.property("_AMQ_Hops") == Some(&PropertyValue::Bytes(vec![0, 0, 0, 1])) })
    );
    // A message that crossed a federation already is not taken again.
    send(&su, "news", 1, Some(1)).await;
    send(&su, "news", 1, None).await;
    let got = drain(&subscriber, 1).await;
    assert_eq!(texts(&got), ["m0"]);
    assert!(
        subscriber
            .receive(Duration::from_millis(300))
            .await
            .unwrap()
            .is_none()
    );

    // Without queues here, the consumer goes.
    subscriber.close().await;
    sd.delete_queue("sub").await.unwrap();
    wait_until("the federated consumer closes", || q.consumer_count() == 0).await;
    stop(down).await;
    stop(up).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_with_consumers_here_takes_the_upstream_queue_messages() {
    let d = dirs();
    let (up, down) = pair(&d, upstream).await;
    let qu = up.broker.post_office().queue("orders").unwrap();
    let (_cu, su) = session(up.port).await;
    send(&su, "orders", 3, None).await;
    assert_eq!(qu.message_count(), 3);

    let (_cd, sd) = session(down.port).await;
    let consumer = sd
        .create_consumer_with(
            "orders",
            ConsumerOptions {
                priority: 5,
                ..ConsumerOptions::default()
            },
        )
        .await
        .unwrap();
    let got = drain(&consumer, 3).await;
    assert_eq!(texts(&got), ["m0", "m1", "m2"]);
    // The remote consumer has the local one's priority less one, and is
    // the federation's: the upstream federates none of its consumers back.
    let entries: Vec<_> = up
        .broker
        .registry()
        .sessions()
        .into_iter()
        .flat_map(|s| s.consumers())
        .filter(|c| &**c.queue.name() == "orders")
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].priority, 4);
    assert_eq!(
        up.broker
            .session_metadata(&entries[0].session, "federation-name")
            .as_deref(),
        Some("f")
    );
    assert_eq!(
        up.broker
            .session_metadata(&entries[0].session, "federation-upstream-name")
            .as_deref(),
        Some("up")
    );
    send(&su, "orders", 2, None).await;
    assert_eq!(texts(&drain(&consumer, 2).await), ["m0", "m1"]);
    consumer.close().await;
    wait_until("the remote consumer closes", || qu.consumer_count() == 0).await;
    send(&su, "orders", 1, None).await;
    assert_eq!(qu.message_count(), 1);
    stop(down).await;
    stop(up).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_downstream_makes_the_other_broker_federate_from_this_one() {
    let d = dirs();
    // The downstream is configured on the broker messages come from: the
    // other broker gets an upstream back to it.
    let lu = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let ld = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let (pu, pd) = (
        lu.local_addr().unwrap().port(),
        ld.local_addr().unwrap().port(),
    );
    let down = start(d.1.path(), ld, ADDRESSES).await;
    let up = start(
        d.0.path(),
        lu,
        &format!(
            r#"<connectors>
                 <connector name="me">tcp://127.0.0.1:{pu}</connector>
                 <connector name="down">tcp://127.0.0.1:{pd}</connector>
               </connectors>
               {ADDRESSES}
               <federations>
                 <federation name="f">
                   <downstream name="d">
                     <static-connectors><connector-ref>down</connector-ref></static-connectors>
                     <policy ref="news"/>
                     <upstream-connector-ref>me</upstream-connector-ref>
                   </downstream>
                   <address-policy name="news"><include address-match="news"/></address-policy>
                 </federation>
               </federations>"#
        ),
    )
    .await;
    wait_until("the other broker deploys the upstream", || {
        down.broker
            .federation()
            .get("f-upstream")
            .is_some_and(|f| f.upstream("d-upstream").is_some())
    })
    .await;
    let (_cd, sd) = session(down.port).await;
    sd.create_queue(
        QueueConfiguration::new("sub")
            .address("news")
            .routing_type(RoutingType::Multicast),
    )
    .await
    .unwrap();
    let subscriber = sd.create_consumer("sub", None, false).await.unwrap();
    wait_until("this broker has the federated queue", || {
        up.broker
            .post_office()
            .queue("federated.f-upstream.d-upstream.news.multicast")
            .is_some_and(|q| q.consumer_count() == 1)
    })
    .await;
    let (_cu, su) = session(up.port).await;
    send(&su, "news", 3, None).await;
    assert_eq!(texts(&drain(&subscriber, 3).await), ["m0", "m1", "m2"]);
    // When this broker goes, the other undeploys what it asked for.
    stop(up).await;
    wait_until("the upstream is undeployed", || {
        down.broker.federation().get("f-upstream").is_none()
    })
    .await;
    stop(down).await;
}
