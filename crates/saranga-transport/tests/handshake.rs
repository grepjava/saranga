//! TLS handshakes against an acceptor built from acceptor parameters.

use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use saranga_transport::{TlsAcceptor, TlsSettings};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/tls")
        .join(name)
        .display()
        .to_string()
}

fn settings(params: &[(&str, String)]) -> TlsSettings {
    let mut all: Vec<(&str, String)> = vec![("sslEnabled", "true".into())];
    all.extend(params.iter().cloned());
    TlsSettings::from_params(
        |k| all.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone()),
        |v| Ok(v.to_owned()),
    )
    .unwrap()
    .unwrap()
}

fn server_params() -> Vec<(&'static str, String)> {
    vec![
        ("keyStorePath", fixture("server.p12")),
        ("keyStorePassword", "changeit".into()),
        ("keyStoreType", "PKCS12".into()),
    ]
}

fn client_auth_params(mode: &str) -> Vec<(&'static str, String)> {
    let mut p = server_params();
    p.push((
        if mode == "need" {
            "needClientAuth"
        } else {
            "wantClientAuth"
        },
        "true".into(),
    ));
    p.push(("trustStorePath", fixture("truststore.jks")));
    p.push(("trustStorePassword", "changeit".into()));
    p
}

/// The client's certificate and key, from a PKCS#12 fixture.
fn client_identity(name: &str) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let store = saranga_transport::keystore::KeyStore::load(
        "PKCS12",
        Some(&fixture(&format!("{name}.p12"))),
        Some("changeit"),
        None,
    )
    .unwrap();
    store.server_key(Some(name)).unwrap()
}

struct Outcome {
    ok: bool,
    /// The peer certificate subject the server saw.
    peer: Option<String>,
    version: Option<rustls::ProtocolVersion>,
}

/// One connection: the server echoes a byte; the client sends one.
async fn connect(acceptor: Arc<TlsAcceptor>, client_cert: Option<&str>, sni: &str) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(tcp).await.ok()?;
        let (_, conn) = tls.get_ref();
        let peer = saranga_transport::tls::peer_certificates(conn.peer_certificates())
            .map(|c| c[0].subject_dn.clone());
        let version = conn.protocol_version();
        let mut b = [0u8; 1];
        tls.read_exact(&mut b).await.ok()?;
        tls.write_all(&b).await.ok()?;
        tls.flush().await.ok()?;
        Some((peer, version))
    });

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(fixture("ca.pem")).unwrap())
        .unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    let config = match client_cert {
        Some(name) => {
            let (chain, key) = client_identity(name);
            builder.with_client_auth_cert(chain, key).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr).await.unwrap();
    let client_ok = async {
        let mut tls = connector
            .connect(ServerName::try_from(sni.to_owned()).unwrap(), tcp)
            .await
            .ok()?;
        tls.write_all(b"x").await.ok()?;
        tls.flush().await.ok()?;
        let mut b = [0u8; 1];
        tls.read_exact(&mut b).await.ok()?;
        Some(())
    }
    .await
    .is_some();
    let server_result = server.await.unwrap();
    Outcome {
        ok: client_ok && server_result.is_some(),
        peer: server_result.as_ref().and_then(|r| r.0.clone()),
        version: server_result.and_then(|r| r.1),
    }
}

#[tokio::test]
async fn server_only() {
    for (ty, path, extra) in [
        ("PKCS12", "server.p12", None),
        ("JKS", "server.jks", Some("keypass")),
        ("PEM", "server.pem", None),
    ] {
        let mut p = vec![
            ("keyStorePath", fixture(path)),
            ("keyStorePassword", "changeit".to_owned()),
            ("keyStoreType", ty.to_owned()),
        ];
        if let Some(k) = extra {
            p.push(("keyPassword", k.to_owned()));
        }
        let acceptor = TlsAcceptor::new(settings(&p)).unwrap();
        let o = connect(acceptor, None, "localhost").await;
        assert!(o.ok, "{path}");
        assert_eq!(o.peer, None);
    }
}

#[tokio::test]
async fn need_client_auth() {
    let acceptor = TlsAcceptor::new(settings(&client_auth_params("need"))).unwrap();
    assert!(!connect(acceptor.clone(), None, "localhost").await.ok);
    let o = connect(acceptor, Some("client"), "localhost").await;
    assert!(o.ok);
    assert_eq!(o.peer.as_deref(), Some("CN=client,OU=Test,O=Saranga"));
}

#[tokio::test]
async fn want_client_auth() {
    let acceptor = TlsAcceptor::new(settings(&client_auth_params("want"))).unwrap();
    let o = connect(acceptor.clone(), None, "localhost").await;
    assert!(o.ok);
    assert_eq!(o.peer, None);
    let o = connect(acceptor, Some("client"), "localhost").await;
    assert_eq!(o.peer.as_deref(), Some("CN=client,OU=Test,O=Saranga"));
}

#[tokio::test]
async fn revoked_client() {
    let mut p = client_auth_params("need");
    p.push(("crlPath", fixture("revoked.crl")));
    let acceptor = TlsAcceptor::new(settings(&p)).unwrap();
    assert!(
        !connect(acceptor.clone(), Some("other"), "localhost")
            .await
            .ok
    );
    assert!(connect(acceptor, Some("client"), "localhost").await.ok);
}

#[tokio::test]
async fn sni_host() {
    let mut p = server_params();
    p.push(("sniHost", "local.*".into()));
    let acceptor = TlsAcceptor::new(settings(&p)).unwrap();
    assert!(connect(acceptor.clone(), None, "LOCALHOST").await.ok);
    // A name the pattern does not match is refused (the client would not
    // trust the certificate for it either, but the server refuses first).
    assert!(!connect(acceptor.clone(), None, "evil.example").await.ok);
    // An IP address sends no SNI: let in.
    assert!(connect(acceptor, None, "127.0.0.1").await.ok);
}

#[tokio::test]
async fn protocols_and_suites() {
    let mut p = server_params();
    p.push(("enabledProtocols", "TLSv1.2".into()));
    let acceptor = TlsAcceptor::new(settings(&p)).unwrap();
    let o = connect(acceptor, None, "localhost").await;
    assert!(o.ok);
    assert_eq!(o.version, Some(rustls::ProtocolVersion::TLSv1_2));

    let mut p = server_params();
    p.push(("enabledCipherSuites", "TLS_AES_256_GCM_SHA384".into()));
    let acceptor = TlsAcceptor::new(settings(&p)).unwrap();
    let o = connect(acceptor, None, "localhost").await;
    assert_eq!(o.version, Some(rustls::ProtocolVersion::TLSv1_3));

    let mut p = server_params();
    p.push(("enabledProtocols", "SSLv3".into()));
    assert!(settings(&p).server_config().is_err());
}

#[test]
fn settings_from_params() {
    let p = |pairs: &[(&str, &str)]| {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        TlsSettings::from_params(
            |k| pairs.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()),
            |v| {
                if v == "ENC(x)" {
                    Ok("decoded".into())
                } else {
                    Ok(v.into())
                }
            },
        )
    };
    assert!(p(&[]).unwrap().is_none());
    let s = p(&[("sslEnabled", "true"), ("keyStorePassword", "ENC(x)")])
        .unwrap()
        .unwrap();
    assert_eq!(s.key_store_password.as_deref(), Some("decoded"));
    assert_eq!(s.key_store_type, "JKS");
    assert!(p(&[("sslEnabled", "true"), ("trustManagerFactoryPlugin", "x")]).is_err());
}

#[tokio::test]
async fn auto_reload() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("server.p12");
    std::fs::copy(fixture("server.p12"), &store).unwrap();
    let mut p = server_params();
    p[0].1 = store.display().to_string();
    p.push(("sslAutoReload", "true".into()));
    let acceptor = TlsAcceptor::new(settings(&p)).unwrap();
    // A broken store keeps the old configuration...
    std::fs::write(&store, b"garbage").unwrap();
    acceptor.reload_if_changed();
    assert!(connect(acceptor.clone(), None, "localhost").await.ok);
    // ... and a good one is picked up.
    std::fs::copy(fixture("server.p12"), &store).unwrap();
    acceptor.reload_if_changed();
    assert!(connect(acceptor, None, "localhost").await.ok);
}
