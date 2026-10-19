//! TLS connectors. Ported from the SSL parts of `NettyConnector` and the
//! client side of `SSLSupport` and `DefaultSSLContextFactory`: a
//! connector URI's `sslEnabled`, keystore (the client's certificate) and
//! truststore parameters, `trustAll`, `verifyHost`, `sniHost`, protocols,
//! cipher suites and CRLs, and the `javax.net.ssl.*` and
//! `org.apache.activemq.ssl.*` system properties that stand in for them.
//!
//! Without a truststore, the server is checked against the platform's
//! trusted certificate authorities, as Java checks it against its own
//! (`cacerts`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use saranga_security::keystore::KeyStore;

use crate::tls::{TlsError, select_suites, select_versions};

/// How long `NettyConnector.createConnection` waits for the handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// The JVM's system properties, as the broker or the command line was
/// given them (`-D` in `JAVA_ARGS` or on `run`).
static SYSTEM_PROPERTIES: RwLock<Vec<(String, String)>> = RwLock::new(Vec::new());

/// Sets the system properties the connectors read (`System.getProperty`).
/// The last of a name wins.
pub fn set_system_properties(properties: Vec<(String, String)>) {
    *SYSTEM_PROPERTIES.write().unwrap_or_else(|e| e.into_inner()) = properties;
}

fn system_property(name: &str) -> Option<String> {
    SYSTEM_PROPERTIES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .rev()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
}

/// The parameters naming Java classes or checks that cannot run here.
const UNSUPPORTED: &[&str] = &[
    "trustManagerFactoryPlugin",
    "sslContext",
    "crcOptions",
    "ocspResponderURL",
];

/// Refuses the parameters of a TLS connector that cannot be honoured here:
/// a Java class to load or an online revocation check. The broker checks
/// its connectors with this when it starts.
pub fn check_params(param: impl Fn(&str) -> Option<String>) -> Result<(), TlsError> {
    if !param("sslEnabled").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return Ok(());
    }
    for unsupported in UNSUPPORTED {
        if param(unsupported).is_some() {
            return Err(TlsError::Config(format!(
                "the connector parameter {unsupported} is not supported"
            )));
        }
    }
    Ok(())
}

/// The TLS settings of one connector, with the system properties applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorTlsSettings {
    pub key_store_type: String,
    pub key_store_path: Option<String>,
    pub key_store_password: Option<String>,
    pub key_password: Option<String>,
    pub key_store_alias: Option<String>,
    pub trust_store_type: String,
    pub trust_store_path: Option<String>,
    pub trust_store_password: Option<String>,
    pub crl_path: Option<String>,
    pub trust_all: bool,
    pub verify_host: bool,
    pub sni_host: Option<String>,
    pub enabled_protocols: Option<Vec<String>>,
    pub enabled_cipher_suites: Option<Vec<String>>,
}

impl ConnectorTlsSettings {
    /// The connector's TLS settings, `None` unless `sslEnabled`. `param`
    /// reads a URI parameter; `password` resolves a (possibly masked)
    /// password parameter as `ConfigurationHelper.getPasswordProperty`
    /// does.
    ///
    /// As `NettyConnector.createConnection` takes them, the
    /// `org.apache.activemq.ssl.*` and then the `javax.net.ssl.*` system
    /// properties come before the connector's own parameters unless
    /// `forceSSLParameters`; with `useDefaultSslContext`, the JVM's default
    /// context is used (`SSLContext.getDefault`), made from the
    /// `javax.net.ssl.*` properties alone.
    pub fn from_params(
        param: impl Fn(&str) -> Option<String>,
        password: impl Fn(&str) -> Result<String, String>,
    ) -> Result<Option<Self>, TlsError> {
        let flag = |name: &str| param(name).is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let flag_or = |name: &str, default: bool| {
            param(name).map_or(default, |v| v.eq_ignore_ascii_case("true"))
        };
        if !flag("sslEnabled") {
            return Ok(None);
        }
        check_params(&param)?;
        let secret = |name: &str| {
            param(name)
                .map(|v| password(&v).map_err(|e| TlsError::Config(format!("{name}: {e}"))))
                .transpose()
        };
        let list = |name: &str| {
            param(name).map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
        };
        let mut s = Self {
            key_store_type: param("keyStoreType").unwrap_or_else(|| "JKS".into()),
            key_store_path: param("keyStorePath"),
            key_store_password: secret("keyStorePassword")?,
            key_password: secret("keyPassword")?,
            key_store_alias: param("keyStoreAlias"),
            trust_store_type: param("trustStoreType").unwrap_or_else(|| "JKS".into()),
            trust_store_path: param("trustStorePath"),
            trust_store_password: secret("trustStorePassword")?,
            crl_path: param("crlPath"),
            trust_all: flag("trustAll"),
            verify_host: flag_or("verifyHost", true),
            sni_host: param("sniHost"),
            enabled_protocols: list("enabledProtocols"),
            enabled_cipher_suites: list("enabledCipherSuites"),
        };
        if flag("useDefaultSslContext") {
            s.default_context()?;
        } else if !flag("forceSSLParameters") {
            s.apply_system_properties(param("activemq.passwordcodec").as_deref())?;
        }
        Ok(Some(s))
    }

    /// The system properties before the connector's parameters; a password
    /// taken from one is unmasked with `org.apache.activemq.ssl.passwordCodec`
    /// or the connector's codec (`processSslPasswordProperty`).
    fn apply_system_properties(&mut self, codec: Option<&str>) -> Result<(), TlsError> {
        let codec = system_property("org.apache.activemq.ssl.passwordCodec")
            .or_else(|| codec.map(str::to_owned));
        let pick = |names: [&str; 2]| names.into_iter().find_map(system_property);
        let unmask = |v: String, name: &str| {
            saranga_config::mask::resolve_mask(None, &v, codec.as_deref())
                .map_err(|e| TlsError::Config(format!("{name}: {e}")))
        };
        if let Some(v) = pick(["org.apache.activemq.ssl.keyStore", "javax.net.ssl.keyStore"]) {
            self.key_store_path = Some(v);
        }
        if let Some(v) = pick([
            "org.apache.activemq.ssl.keyStorePassword",
            "javax.net.ssl.keyStorePassword",
        ]) && self.key_store_password.as_ref() != Some(&v)
        {
            self.key_store_password = Some(unmask(v, "keyStorePassword")?);
        }
        if let Some(v) = pick([
            "org.apache.activemq.ssl.keyStoreType",
            "javax.net.ssl.keyStoreType",
        ]) {
            self.key_store_type = v;
        }
        if let Some(v) = pick([
            "org.apache.activemq.ssl.trustStore",
            "javax.net.ssl.trustStore",
        ]) {
            self.trust_store_path = Some(v);
        }
        if let Some(v) = pick([
            "org.apache.activemq.ssl.trustStorePassword",
            "javax.net.ssl.trustStorePassword",
        ]) && self.trust_store_password.as_ref() != Some(&v)
        {
            self.trust_store_password = Some(unmask(v, "trustStorePassword")?);
        }
        if let Some(v) = pick([
            "org.apache.activemq.ssl.trustStoreType",
            "javax.net.ssl.trustStoreType",
        ]) {
            self.trust_store_type = v;
        }
        Ok(())
    }

    /// `SSLContext.getDefault`: the JVM's default key and trust managers,
    /// which read the `javax.net.ssl.*` properties only. The JVM's default
    /// store type reads both JKS and PKCS#12 files (`keystore.type.compat`).
    fn default_context(&mut self) -> Result<(), TlsError> {
        self.key_store_path = system_property("javax.net.ssl.keyStore");
        self.key_store_password = system_property("javax.net.ssl.keyStorePassword");
        self.key_password = None;
        self.key_store_alias = None;
        self.key_store_type =
            system_property("javax.net.ssl.keyStoreType").unwrap_or_else(|| "JKS".into());
        self.trust_store_path = system_property("javax.net.ssl.trustStore");
        self.trust_store_password = system_property("javax.net.ssl.trustStorePassword");
        self.trust_store_type =
            system_property("javax.net.ssl.trustStoreType").unwrap_or_else(|| "JKS".into());
        self.crl_path = None;
        self.trust_all = false;
        Ok(())
    }

    /// The files the configuration is read from: when one changes, the
    /// configuration is built again.
    fn files(&self) -> Vec<PathBuf> {
        [&self.key_store_path, &self.trust_store_path, &self.crl_path]
            .into_iter()
            .flatten()
            .filter(|p| !p.eq_ignore_ascii_case("NONE"))
            .map(|p| saranga_config::file_uri_path(p).unwrap_or_else(|| PathBuf::from(p)))
            .collect()
    }

    /// Builds the rustls configuration: `SSLSupport.createContext` and the
    /// engine `NettyConnector` sets up.
    pub fn client_config(&self) -> Result<ClientConfig, TlsError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let provider = Arc::new(CryptoProvider {
            cipher_suites: select_suites(self.enabled_cipher_suites.as_deref())?,
            ..(*provider).clone()
        });
        let versions = select_versions(self.enabled_protocols.as_deref())?;
        let verifier: Arc<dyn ServerCertVerifier> = if self.trust_all {
            // `InsecureTrustManagerFactory`.
            Arc::new(TrustAll(provider.clone()))
        } else {
            let roots = self.roots()?;
            let webpki =
                WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .with_crls(crate::tls::load_crls(self.crl_path.as_deref())?)
                    .build()
                    .map_err(|e| TlsError::Config(format!("server certificate verifier: {e}")))?;
            if self.verify_host {
                webpki
            } else {
                Arc::new(AnyHost(webpki))
            }
        };
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&versions)?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        let key_store = KeyStore::load(
            &self.key_store_type,
            self.key_store_path.as_deref(),
            self.key_store_password.as_deref(),
            self.key_password.as_deref(),
        )?;
        let mut keys = Vec::new();
        for (alias, key, chain) in key_store.keys() {
            if chain.is_empty()
                || self
                    .key_store_alias
                    .as_deref()
                    .is_some_and(|a| !a.eq_ignore_ascii_case(alias))
            {
                continue;
            }
            let signing = provider
                .key_provider
                .load_private_key(key.clone_key())
                .map_err(|e| {
                    TlsError::Config(format!("the private key {alias} cannot be used: {e}"))
                })?;
            let chain: Vec<_> = chain.iter().cloned().map(CertificateDer::from).collect();
            let issuers = chain.iter().filter_map(|c| issuer(c)).collect();
            keys.push(ClientKey {
                key: Arc::new(CertifiedKey::new(chain, signing)),
                issuers,
            });
        }
        if let Some(alias) = &self.key_store_alias
            && keys.is_empty()
            && self.key_store_path.is_some()
        {
            return Err(TlsError::Config(format!(
                "no private key entry with alias {alias}"
            )));
        }
        let mut config = if keys.is_empty() {
            builder.with_no_client_auth()
        } else {
            builder.with_client_cert_resolver(Arc::new(ClientKeys(keys)))
        };
        config.alpn_protocols = Vec::new();
        Ok(config)
    }

    /// The certificates the server's is checked against: the truststore's,
    /// else the platform's.
    fn roots(&self) -> Result<RootCertStore, TlsError> {
        let mut roots = RootCertStore::empty();
        let trusted = if self
            .trust_store_path
            .as_deref()
            .is_some_and(|p| !p.is_empty())
        {
            KeyStore::load(
                &self.trust_store_type,
                self.trust_store_path.as_deref(),
                self.trust_store_password.as_deref(),
                None,
            )
            .map_err(TlsError::TrustStore)?
            .trusted_certificates()
        } else {
            let found = rustls_native_certs::load_native_certs();
            for e in &found.errors {
                tracing::debug!("reading the platform's trusted certificates: {e}");
            }
            found.certs
        };
        let (_, ignored) = roots.add_parsable_certificates(trusted);
        if ignored > 0 {
            tracing::debug!("{ignored} trusted certificates could not be read");
        }
        if roots.is_empty() {
            return Err(TlsError::Config(
                "the truststore holds no trusted certificate".into(),
            ));
        }
        Ok(roots)
    }
}

/// The DER of a certificate's issuer, as the server's certificate
/// authority names are given.
fn issuer(cert: &CertificateDer<'_>) -> Option<Vec<u8>> {
    use x509_parser::prelude::FromDer;
    let (_, c) = x509_parser::certificate::X509Certificate::from_der(cert).ok()?;
    Some(c.issuer().as_raw().to_vec())
}

#[derive(Debug)]
struct ClientKey {
    key: Arc<CertifiedKey>,
    /// The issuers of the chain's certificates.
    issuers: Vec<Vec<u8>>,
}

/// `SunX509KeyManagerImpl.chooseClientAlias`: the first key entry of a
/// type the server takes, one of whose chain's certificates a certificate
/// authority the server names issued, if it names any.
#[derive(Debug)]
struct ClientKeys(Vec<ClientKey>);

impl ResolvesClientCert for ClientKeys {
    fn resolve(
        &self,
        root_hint_subjects: &[&[u8]],
        sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        self.0
            .iter()
            .filter(|k| k.key.key.choose_scheme(sigschemes).is_some())
            .find(|k| {
                root_hint_subjects.is_empty()
                    || k.issuers
                        .iter()
                        .any(|i| root_hint_subjects.contains(&i.as_slice()))
            })
            .map(|k| k.key.clone())
    }

    fn has_certs(&self) -> bool {
        !self.0.is_empty()
    }
}

/// Notes whether a connection's handshake sent a certificate of this end.
#[derive(Debug)]
struct Sent {
    inner: Arc<dyn ResolvesClientCert>,
    sent: Arc<AtomicBool>,
}

impl ResolvesClientCert for Sent {
    fn resolve(
        &self,
        root_hint_subjects: &[&[u8]],
        sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        let key = self.inner.resolve(root_hint_subjects, sigschemes);
        if key.is_some() {
            self.sent.store(true, Ordering::SeqCst);
        }
        key
    }

    fn has_certs(&self) -> bool {
        self.inner.has_certs()
    }
}

/// `InsecureTrustManagerFactory`: any certificate, the handshake's
/// signatures still checked.
#[derive(Debug)]
struct TrustAll(Arc<CryptoProvider>);

impl ServerCertVerifier for TrustAll {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Without `verifyHost`: the chain checked, the host name not
/// (no `HTTPS` endpoint identification).
#[derive(Debug)]
struct AnyHost(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for AnyHost {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// `SNIHostName`'s rule, which the JDK applies to the host it connects to:
/// a name with a dot, not an address.
fn sends_sni(host: &str) -> bool {
    host.parse::<std::net::IpAddr>().is_err() && host.contains('.') && !host.ends_with('.')
}

/// A connector's TLS: its settings, or why they cannot be used, and the
/// configuration built from them, built again when a store file changes.
pub struct TlsConnector {
    settings: Result<ConnectorTlsSettings, String>,
    built: Mutex<Option<Built>>,
}

struct Built {
    stamps: Vec<Option<SystemTime>>,
    /// With and without SNI.
    sni: Arc<ClientConfig>,
    no_sni: Arc<ClientConfig>,
}

impl std::fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.settings {
            Ok(_) => f.write_str("TlsConnector"),
            Err(e) => write!(f, "TlsConnector({e})"),
        }
    }
}

impl TlsConnector {
    pub fn new(settings: ConnectorTlsSettings) -> Self {
        Self {
            settings: Ok(settings),
            built: Mutex::new(None),
        }
    }

    /// A connector whose settings could not be read: each connection
    /// fails with `error`, rather than going out without TLS.
    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            settings: Err(error.into()),
            built: Mutex::new(None),
        }
    }

    /// The TLS of a connector's parameters, `None` unless `sslEnabled`.
    pub fn from_params(
        param: impl Fn(&str) -> Option<String>,
        password: impl Fn(&str) -> Result<String, String>,
    ) -> Option<Self> {
        match ConnectorTlsSettings::from_params(param, password) {
            Ok(s) => s.map(Self::new),
            Err(e) => Some(Self::failed(e.to_string())),
        }
    }

    /// As [`TlsConnector::from_params`], with passwords unmasked as
    /// `ConfigurationHelper.getPasswordProperty` unmasks them, by the
    /// parameters' own `activemq.usemaskedpassword` and
    /// `activemq.passwordcodec`.
    pub fn from_uri_params(param: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let mask = param("activemq.usemaskedpassword").map(|m| m.eq_ignore_ascii_case("true"));
        let codec = param("activemq.passwordcodec");
        Self::from_params(&param, |v| {
            saranga_config::mask::resolve_mask(mask, v, codec.as_deref()).map_err(|e| e.to_string())
        })
    }

    pub fn settings(&self) -> Result<&ConnectorTlsSettings, &str> {
        self.settings.as_ref().map_err(String::as_str)
    }

    fn config(&self, host: &str) -> Result<(Arc<ClientConfig>, ServerName<'static>), String> {
        let settings = self.settings.as_ref().map_err(Clone::clone)?;
        let name = settings.sni_host.as_deref().unwrap_or(host);
        let server_name = ServerName::try_from(name.to_owned())
            .map_err(|e| format!("invalid host name {name:?}: {e}"))?;
        let stamps = stamps(&settings.files());
        let mut built = self.built.lock().unwrap_or_else(|e| e.into_inner());
        if built.as_ref().is_none_or(|b| b.stamps != stamps) {
            let config = settings.client_config().map_err(|e| e.to_string())?;
            let mut no_sni = config.clone();
            no_sni.enable_sni = false;
            *built = Some(Built {
                stamps,
                sni: Arc::new(config),
                no_sni: Arc::new(no_sni),
            });
        }
        let b = built.as_ref().expect("built");
        let config = if settings.sni_host.is_some() || sends_sni(host) {
            b.sni.clone()
        } else {
            b.no_sni.clone()
        };
        Ok((config, server_name))
    }

    /// The handshake on a connection to `host`, within 30 seconds. A
    /// failure is logged as `NettyConnector` logs it.
    pub async fn connect(
        &self,
        host: &str,
        stream: TcpStream,
    ) -> std::io::Result<TlsStream<TcpStream>> {
        Ok(self.connect_reporting(host, stream).await?.0)
    }

    /// As [`TlsConnector::connect`], also saying whether this end sent a
    /// certificate of its own (`CertificateUtil.getLocalPrincipalFromConnection`).
    pub async fn connect_reporting(
        &self,
        host: &str,
        stream: TcpStream,
    ) -> std::io::Result<(TlsStream<TcpStream>, bool)> {
        let (config, name) = self.config(host).map_err(|e| {
            tracing::error!("AMQ214016: Failed to create netty connection: {e}");
            std::io::Error::other(e)
        })?;
        let sent = Arc::new(AtomicBool::new(false));
        let mut config = (*config).clone();
        config.client_auth_cert_resolver = Arc::new(Sent {
            inner: config.client_auth_cert_resolver.clone(),
            sent: sent.clone(),
        });
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(name, stream)).await {
            Ok(Ok(s)) => Ok((s, sent.load(Ordering::SeqCst))),
            Ok(Err(e)) => {
                tracing::error!("AMQ214016: Failed to create netty connection: {e}");
                Err(e)
            }
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "TLS handshake timed out",
            )),
        }
    }
}

fn stamps(files: &[PathBuf]) -> Vec<Option<SystemTime>> {
    files
        .iter()
        .map(|f| std::fs::metadata(f).and_then(|m| m.modified()).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sni_as_the_jdk_sends_it() {
        assert!(sends_sni("broker.example.com"));
        assert!(!sends_sni("localhost"));
        assert!(!sends_sni("127.0.0.1"));
        assert!(!sends_sni("::1"));
        assert!(!sends_sni("host."));
    }
}
