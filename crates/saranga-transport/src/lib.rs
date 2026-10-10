//! Transport for acceptors: TLS with the keystore formats Artemis reads
//! (JKS, PKCS#12, PEM), client certificates, SNI and reload.

pub mod tls;

pub use saranga_security::{dn, keystore};

pub use tls::{TlsAcceptor, TlsError, TlsSettings};
