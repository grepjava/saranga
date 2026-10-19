//! Core connections over TLS, as `NettyConnector` makes them: the server
//! checked against a truststore (or trusted whatever it is with
//! `trustAll`), its host name checked unless `verifyHost=false`, the
//! client's certificate from a keystore, and a bridge whose connector has
//! `sslEnabled`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use saranga_transport::{TlsAcceptor, TlsSettings};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/tls")
        .join(name)
        .display()
        .to_string()
}

struct Running {
    broker: Arc<Broker>,
    acceptor: JoinHandle<std::io::Result<()>>,
    port: u16,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.acceptor.abort();
    }
}

/// A broker with one acceptor, with TLS from `tls` (acceptor parameters).
async fn start(dir: &std::path::Path, tls: &[(&str, String)], core: &str) -> Running {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <security-enabled>false</security-enabled>
             <addresses><address name="q"><anycast><queue name="q"/></anycast></address>
                        <address name="src"><anycast><queue name="src"/></anycast></address></addresses>
             {core}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let tls = (!tls.is_empty()).then(|| {
        let mut all = vec![("sslEnabled", "true".to_owned())];
        all.extend(tls.iter().cloned());
        let settings = TlsSettings::from_params(
            |k| all.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone()),
            |v| Ok(v.to_owned()),
        )
        .unwrap()
        .unwrap();
        TlsAcceptor::new(settings).unwrap()
    });
    let acceptor = Acceptor {
        tls,
        ..Acceptor::default()
    };
    let acceptor = tokio::spawn(broker.clone().serve_acceptor(listener, acceptor));
    Running {
        broker,
        acceptor,
        port,
    }
}

fn server_tls() -> Vec<(&'static str, String)> {
    vec![
        ("keyStorePath", fixture("server.p12")),
        ("keyStorePassword", "changeit".into()),
        ("keyStoreType", "PKCS12".into()),
    ]
}

/// A connector to `host:port` with these parameters, as a broker announces
/// or configures one.
fn connector(host: &str, port: u16, params: &[(&str, &str)]) -> Connector {
    let mut all = vec![
        ("host".to_owned(), host.to_owned()),
        ("port".to_owned(), port.to_string()),
    ];
    all.extend(params.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let tc = TransportConfiguration {
        params: all
            .iter()
            .map(|(k, v)| (k.as_str().into(), ParamValue::String(v.as_str().into())))
            .collect(),
        ..TransportConfiguration::default()
    };
    Connector::from_transport(&tc).unwrap()
}

fn trusted() -> Vec<(&'static str, String)> {
    vec![
        ("sslEnabled", "true".into()),
        ("trustStorePath", fixture("truststore.jks")),
        ("trustStorePassword", "changeit".into()),
    ]
}

fn borrow<'a>(p: &'a [(&'a str, String)]) -> Vec<(&'a str, &'a str)> {
    p.iter().map(|(k, v)| (*k, v.as_str())).collect()
}

async fn connects(c: Connector) -> bool {
    let options = ConnectionOptions {
        initial_connect_attempts: 1,
        ..ConnectionOptions::default()
    };
    match tokio::time::timeout(WAIT, Connection::connect_to(vec![c], options)).await {
        Ok(Ok(conn)) => {
            conn.close().await;
            true
        }
        _ => false,
    }
}

/// Sends a message and takes it back.
async fn round_trip(conn: &Connection, text: &str) {
    let session = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    let producer = session.create_producer(Some("q")).await.unwrap();
    producer.send(message::text_message(text)).await.unwrap();
    let consumer = session.create_consumer("q", None, false).await.unwrap();
    session.start().await.unwrap();
    let d = consumer.receive(WAIT).await.unwrap().expect("the message");
    consumer.acknowledge(&d).await.unwrap();
    assert_eq!(d.message.text().unwrap().as_deref(), Some(text));
    session.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connector_with_ssl_enabled_runs_tls() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), &server_tls(), "").await;

    let t = trusted();
    let conn = Connection::connect_to(
        vec![connector("127.0.0.1", b.port, &borrow(&t))],
        ConnectionOptions::default(),
    )
    .await
    .unwrap();
    round_trip(&conn, "over TLS").await;
    conn.close().await;

    // A connector without TLS gets nowhere with a TLS acceptor.
    assert!(!connects(connector("127.0.0.1", b.port, &[])).await);
    // The server's certificate must be trusted...
    let untrusted = [
        ("sslEnabled", "true".to_owned()),
        ("trustStorePath", fixture("untrusted.p12")),
        ("trustStorePassword", "changeit".into()),
        ("trustStoreType", "PKCS12".into()),
    ];
    assert!(!connects(connector("127.0.0.1", b.port, &borrow(&untrusted))).await);
    // ...unless anything is (`trustAll`).
    let mut all = untrusted.to_vec();
    all.push(("trustAll", "true".into()));
    assert!(connects(connector("127.0.0.1", b.port, &borrow(&all))).await);
    // A PEM truststore, the same CA.
    let pem = [
        ("sslEnabled", "true".to_owned()),
        ("trustStorePath", fixture("ca.pem")),
        ("trustStoreType", "PEMCA".into()),
    ];
    assert!(connects(connector("127.0.0.1", b.port, &borrow(&pem))).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_host_name_is_checked_unless_verify_host_is_false() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), &server_tls(), "").await;
    // The certificate names localhost and 127.0.0.1.
    let t = trusted();
    assert!(connects(connector("localhost", b.port, &borrow(&t))).await);
    // sniHost is the name the certificate is checked against.
    let mut sni = t.clone();
    sni.push(("sniHost", "broker.example.com".into()));
    assert!(!connects(connector("127.0.0.1", b.port, &borrow(&sni))).await);
    sni.push(("verifyHost", "false".into()));
    assert!(connects(connector("127.0.0.1", b.port, &borrow(&sni))).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_certificate_comes_from_the_keystore() {
    let dir = tempfile::tempdir().unwrap();
    let mut acceptor = server_tls();
    acceptor.extend([
        ("needClientAuth", "true".to_owned()),
        ("trustStorePath", fixture("truststore.jks")),
        ("trustStorePassword", "changeit".into()),
    ]);
    let b = start(dir.path(), &acceptor, "").await;
    let t = trusted();
    assert!(!connects(connector("127.0.0.1", b.port, &borrow(&t))).await);
    let mut client = t.clone();
    client.extend([
        ("keyStorePath", fixture("client.p12")),
        ("keyStorePassword", "changeit".into()),
        ("keyStoreType", "PKCS12".into()),
    ]);
    let conn = Connection::connect_to(
        vec![connector("127.0.0.1", b.port, &borrow(&client))],
        ConnectionOptions::default(),
    )
    .await
    .unwrap();
    round_trip(&conn, "with a client certificate").await;
    conn.close().await;
    // A keystore holding no key has no certificate to send.
    let mut stranger = t.clone();
    stranger.extend([
        ("keyStorePath", fixture("untrusted.p12")),
        ("keyStorePassword", "changeit".into()),
        ("keyStoreType", "PKCS12".into()),
    ]);
    assert!(!connects(connector("127.0.0.1", b.port, &borrow(&stranger))).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bridge_connects_over_tls() {
    let target_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let target = start(target_dir.path(), &server_tls(), "").await;
    let source = start(
        source_dir.path(),
        &[],
        &format!(
            r#"<connectors><connector name="target">tcp://127.0.0.1:{}?sslEnabled=true;trustStorePath={};trustStorePassword=changeit</connector></connectors>
               <bridges><bridge name="br">
                 <queue-name>src</queue-name>
                 <forwarding-address>q</forwarding-address>
                 <retry-interval>100</retry-interval>
                 <static-connectors><connector-ref>target</connector-ref></static-connectors>
               </bridge></bridges>"#,
            target.port,
            fixture("truststore.jks")
        ),
    )
    .await;
    let bridge = source.broker.bridges().get("br").unwrap();
    for _ in 0..500 {
        if bridge.is_connected() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(bridge.is_connected(), "the bridge did not connect");

    let src = Connection::connect(("127.0.0.1", source.port), ConnectionOptions::default())
        .await
        .unwrap();
    let session = src.create_session(SessionOptions::default()).await.unwrap();
    let producer = session.create_producer(Some("src")).await.unwrap();
    producer
        .send(message::text_message("bridged"))
        .await
        .unwrap();
    session.close().await;
    src.close().await;

    let t = trusted();
    let dst = Connection::connect_to(
        vec![connector("127.0.0.1", target.port, &borrow(&t))],
        ConnectionOptions::default(),
    )
    .await
    .unwrap();
    let session = dst.create_session(SessionOptions::default()).await.unwrap();
    let consumer = session.create_consumer("q", None, false).await.unwrap();
    session.start().await.unwrap();
    let d = consumer
        .receive(WAIT)
        .await
        .unwrap()
        .expect("the bridged message");
    assert_eq!(d.message.text().unwrap().as_deref(), Some("bridged"));
    dst.close().await;
}
