use std::path::PathBuf;

use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

use super::*;

fn fixture(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/tls")
        .join(name);
    p.display().to_string()
}

fn server_cert() -> CertificateDer<'static> {
    CertificateDer::from_pem_file(fixture("server-cert.pem")).unwrap()
}

fn ca_cert() -> CertificateDer<'static> {
    CertificateDer::from_pem_file(fixture("ca.pem")).unwrap()
}

fn check_server_store(store: &KeyStore) {
    let (chain, _key) = store.server_key(None).unwrap();
    assert_eq!(chain[0], server_cert(), "leaf");
    assert_eq!(
        chain.last().unwrap(),
        &ca_cert(),
        "the chain ends at the CA"
    );
}

#[test]
fn jks_with_key_password() {
    let store = KeyStore::load(
        "JKS",
        Some(&fixture("server.jks")),
        Some("changeit"),
        Some("keypass"),
    )
    .unwrap();
    check_server_store(&store);
    assert_eq!(store.keys().next().unwrap().0, "server");
    // The store password does not open the key, and a wrong store password
    // fails the integrity check.
    let e =
        KeyStore::load("JKS", Some(&fixture("server.jks")), Some("changeit"), None).unwrap_err();
    assert_eq!(e.to_string(), "Cannot recover key");
    let e = KeyStore::load(
        "JKS",
        Some(&fixture("server.jks")),
        Some("wrong"),
        Some("keypass"),
    )
    .unwrap_err();
    assert_eq!(
        e.to_string(),
        "Keystore was tampered with, or password was incorrect"
    );
}

#[test]
fn pkcs12_modern_and_legacy() {
    for name in ["server.p12", "server-legacy.p12"] {
        for ty in ["PKCS12", "JKS", "pkcs12"] {
            let store = KeyStore::load(ty, Some(&fixture(name)), Some("changeit"), None).unwrap();
            check_server_store(&store);
        }
        assert!(KeyStore::load("PKCS12", Some(&fixture(name)), Some("wrong"), None).is_err());
    }
}

#[test]
fn pem_types() {
    let store = KeyStore::load("PEM", Some(&fixture("server.pem")), None, None).unwrap();
    check_server_store(&store);
    assert_eq!(store.keys().next().unwrap().0, "pem");
    let store = KeyStore::load("PEMCFG", Some(&fixture("server.pemcfg")), None, None).unwrap();
    let (chain, _) = store.server_key(Some("server")).unwrap();
    assert_eq!(chain, vec![server_cert()]);
    // As a truststore, PEM trusts its first certificate, PEMCA all of them.
    let store = KeyStore::load("PEM", Some(&fixture("ca.pem")), None, None).unwrap();
    assert_eq!(store.trusted_certificates(), vec![ca_cert()]);
    let store = KeyStore::load("PEMCA", Some(&fixture("server.pem")), None, None).unwrap();
    assert_eq!(store.trusted_certificates().len(), 2);
    assert_eq!(store.entries[1].0, "pem-2");
}

#[test]
fn truststores() {
    for (ty, name) in [("JKS", "truststore.jks"), ("PKCS12", "truststore.p12")] {
        let store = KeyStore::load(ty, Some(&fixture(name)), Some("changeit"), None).unwrap();
        assert_eq!(store.trusted_certificates(), vec![ca_cert()], "{name}");
        assert!(store.keys().next().is_none());
    }
    // Java reads a JKS truststore without a password, unchecked.
    let store = KeyStore::load("JKS", Some(&fixture("truststore.jks")), None, None).unwrap();
    assert_eq!(store.trusted_certificates(), vec![ca_cert()]);
}

#[test]
fn paths_and_errors() {
    assert!(
        KeyStore::load("JKS", None, None, None)
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        KeyStore::load("JKS", Some("NONE"), None, None)
            .unwrap()
            .entries
            .is_empty()
    );
    let url = format!(
        "file:///{}",
        fixture("ca.pem").replace('\\', "/").trim_start_matches('/')
    );
    assert_eq!(
        KeyStore::load("PEM", Some(&url), None, None)
            .unwrap()
            .entries
            .len(),
        1
    );
    assert!(matches!(
        KeyStore::load("JKS", Some("no/such/store"), None, None),
        Err(KeyStoreError::NotFound(_))
    ));
    assert!(matches!(
        KeyStore::load("JCEKS", Some(&fixture("ca.pem")), None, None),
        Err(KeyStoreError::UnsupportedType(_))
    ));
}

#[test]
fn subject_names() {
    let client = CertificateDer::from_pem_file(fixture("client-cert.pem")).unwrap();
    let (rfc, legacy) = crate::dn::subject_names(&client).unwrap();
    assert_eq!(rfc, "CN=client,OU=Test,O=Saranga");
    assert_eq!(legacy, "CN=client, OU=Test, O=Saranga");
}
