//! Transport: TLS for acceptors and connectors, with the keystore formats
//! Artemis reads (JKS, PKCS#12, PEM), client certificates, SNI and reload;
//! WebSockets.

pub mod connector;
pub mod tls;
pub mod websocket;

pub use saranga_security::{dn, keystore};

pub use connector::{ConnectorTlsSettings, TlsConnector};
pub use tls::{TlsAcceptor, TlsError, TlsSettings};
