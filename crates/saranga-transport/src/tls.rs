//! TLS acceptors. Ported from the SSL parts of `NettyAcceptor` and
//! `SSLSupport`: the acceptor URI's `sslEnabled`, keystore and truststore
//! parameters, client authentication, SNI, protocols and cipher suites.
//!
//! `sniHost` is a regular expression, matched case-insensitively against
//! the whole host name a client asks for (`SNIHostName.createSNIMatcher`);
//! a client that asks for no name is let in, as in Java.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use rustls::pki_types::{CertificateDer, CertificateRevocationListDer};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{RootCertStore, ServerConfig, SupportedCipherSuite, SupportedProtocolVersion};
use saranga_security::PeerCertificate;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

use saranga_security::keystore::{KeyStore, KeyStoreError};

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("keystore: {0}")]
    KeyStore(#[from] KeyStoreError),
    #[error("truststore: {0}")]
    TrustStore(KeyStoreError),
    #[error("{0}")]
    Config(String),
    #[error("TLS: {0}")]
    Rustls(#[from] rustls::Error),
}

/// `TransportConstants.DEFAULT_SSL_HANDSHAKE_TIMEOUT`, in seconds.
pub const DEFAULT_HANDSHAKE_TIMEOUT: u64 = 10;

/// The TLS settings of one acceptor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsSettings {
    pub key_store_type: String,
    pub key_store_path: Option<String>,
    pub key_store_password: Option<String>,
    pub key_password: Option<String>,
    pub key_store_alias: Option<String>,
    pub trust_store_type: String,
    pub trust_store_path: Option<String>,
    pub trust_store_password: Option<String>,
    pub crl_path: Option<String>,
    pub need_client_auth: bool,
    pub want_client_auth: bool,
    pub enabled_protocols: Option<Vec<String>>,
    pub enabled_cipher_suites: Option<Vec<String>>,
    pub sni_host: Option<String>,
    pub auto_reload: bool,
    pub handshake_timeout: Duration,
}

impl TlsSettings {
    /// The acceptor's TLS settings, `None` unless `sslEnabled`. `param` reads
    /// a URI parameter; `password` resolves a (possibly masked) password
    /// parameter as `ConfigurationHelper.getPasswordProperty` does.
    pub fn from_params(
        param: impl Fn(&str) -> Option<String>,
        password: impl Fn(&str) -> Result<String, String>,
    ) -> Result<Option<Self>, TlsError> {
        let flag = |name: &str| param(name).is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if !flag("sslEnabled") {
            return Ok(None);
        }
        for unsupported in [
            "trustManagerFactoryPlugin",
            "sslContext",
            "crcOptions",
            "ocspResponderURL",
        ] {
            if param(unsupported).is_some() {
                return Err(TlsError::Config(format!(
                    "the acceptor parameter {unsupported} is not supported"
                )));
            }
        }
        if flag("useDefaultSslContext") {
            return Err(TlsError::Config(
                "useDefaultSslContext is not supported: name a keystore".into(),
            ));
        }
        if flag("verifyHost") {
            tracing::warn!("verifyHost is not supported on acceptors and was ignored");
        }
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
        let timeout = param("sslHandshakeTimeout")
            .map(|v| {
                v.parse::<u64>()
                    .map_err(|_| TlsError::Config(format!("invalid sslHandshakeTimeout {v:?}")))
            })
            .transpose()?
            .unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT);
        Ok(Some(Self {
            key_store_type: param("keyStoreType").unwrap_or_else(|| "JKS".into()),
            key_store_path: param("keyStorePath"),
            key_store_password: secret("keyStorePassword")?,
            key_password: secret("keyPassword")?,
            key_store_alias: param("keyStoreAlias"),
            trust_store_type: param("trustStoreType").unwrap_or_else(|| "JKS".into()),
            trust_store_path: param("trustStorePath"),
            trust_store_password: secret("trustStorePassword")?,
            crl_path: param("crlPath"),
            need_client_auth: flag("needClientAuth"),
            want_client_auth: flag("wantClientAuth"),
            enabled_protocols: list("enabledProtocols"),
            enabled_cipher_suites: list("enabledCipherSuites"),
            sni_host: param("sniHost"),
            auto_reload: flag("sslAutoReload"),
            handshake_timeout: Duration::from_secs(timeout),
        }))
    }

    /// The files the configuration is read from, watched for
    /// `sslAutoReload`.
    pub fn files(&self) -> Vec<PathBuf> {
        [&self.key_store_path, &self.trust_store_path, &self.crl_path]
            .into_iter()
            .flatten()
            .filter(|p| !p.eq_ignore_ascii_case("NONE"))
            .map(|p| saranga_config::file_uri_path(p).unwrap_or_else(|| PathBuf::from(p)))
            .collect()
    }

    /// Builds the rustls configuration: `SSLSupport.createNettyContext`
    /// and `NettyAcceptor.createSSLEngine`.
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, TlsError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let key_store = KeyStore::load(
            &self.key_store_type,
            self.key_store_path.as_deref(),
            self.key_store_password.as_deref(),
            self.key_password.as_deref(),
        )?;
        let (chain, key) = key_store.server_key(self.key_store_alias.as_deref())?;
        let signing = provider
            .key_provider
            .load_private_key(key)
            .map_err(|e| TlsError::Config(format!("the private key cannot be used: {e}")))?;
        let certified = Arc::new(CertifiedKey::new(chain, signing));

        let suites = select_suites(self.enabled_cipher_suites.as_deref())?;
        let provider = Arc::new(rustls::crypto::CryptoProvider {
            cipher_suites: suites,
            ..(*provider).clone()
        });
        let versions = select_versions(self.enabled_protocols.as_deref())?;
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&versions)?;

        let builder = if self.need_client_auth || self.want_client_auth {
            let trust = KeyStore::load(
                &self.trust_store_type,
                self.trust_store_path.as_deref(),
                self.trust_store_password.as_deref(),
                None,
            )
            .map_err(TlsError::TrustStore)?;
            let mut roots = RootCertStore::empty();
            for c in trust.trusted_certificates() {
                roots
                    .add(c)
                    .map_err(|e| TlsError::Config(format!("bad trusted certificate: {e}")))?;
            }
            if roots.is_empty() {
                return Err(TlsError::Config(
                    "client authentication needs a truststore with trusted certificates (trustStorePath)".into(),
                ));
            }
            let mut verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .with_crls(load_crls(self.crl_path.as_deref())?);
            if !self.need_client_auth {
                verifier = verifier.allow_unauthenticated();
            }
            let verifier = verifier
                .build()
                .map_err(|e| TlsError::Config(format!("client certificate verifier: {e}")))?;
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };

        let resolver: Arc<dyn ResolvesServerCert> = match &self.sni_host {
            Some(pattern) => Arc::new(SniCheck {
                pattern: regex::Regex::new(&format!("(?i)^(?:{pattern})$"))
                    .map_err(|e| TlsError::Config(format!("invalid sniHost {pattern:?}: {e}")))?,
                key: certified,
            }),
            None => Arc::new(rustls::sign::SingleCertAndKey::from(certified)),
        };
        let mut config = builder.with_cert_resolver(resolver);
        config.alpn_protocols = Vec::new();
        Ok(Arc::new(config))
    }
}

/// `SSLSupport.loadCRL`: PEM or DER CRLs.
pub(crate) fn load_crls(
    path: Option<&str>,
) -> Result<Vec<CertificateRevocationListDer<'static>>, TlsError> {
    use rustls::pki_types::pem::PemObject;
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let p = saranga_config::file_uri_path(path).unwrap_or_else(|| PathBuf::from(path));
    let data = std::fs::read(&p).map_err(|e| TlsError::Config(format!("crlPath {path}: {e}")))?;
    let pem: Vec<_> = CertificateRevocationListDer::pem_slice_iter(&data)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::Config(format!("crlPath {path}: {e}")))?;
    Ok(if pem.is_empty() {
        vec![CertificateRevocationListDer::from(data)]
    } else {
        pem
    })
}

/// Java's protocol names to rustls versions; empty means the defaults
/// (TLS 1.3 and 1.2).
pub(crate) fn select_versions(
    names: Option<&[String]>,
) -> Result<Vec<&'static SupportedProtocolVersion>, TlsError> {
    let Some(names) = names else {
        return Ok(rustls::DEFAULT_VERSIONS.to_vec());
    };
    let mut out = Vec::new();
    for n in names {
        match n.as_str() {
            "TLSv1.3" => out.push(&rustls::version::TLS13),
            "TLSv1.2" => out.push(&rustls::version::TLS12),
            other => tracing::warn!("enabledProtocols: {other} is not supported and was ignored"),
        }
    }
    if out.is_empty() {
        return Err(TlsError::Config(
            "enabledProtocols names no supported protocol (TLSv1.2, TLSv1.3)".into(),
        ));
    }
    Ok(out)
}

/// The IANA name Java uses for a suite (`TLS_AES_128_GCM_SHA256`, ...).
pub fn suite_name(s: &SupportedCipherSuite) -> String {
    let name = format!("{:?}", s.suite());
    match name.strip_prefix("TLS13_") {
        Some(rest) => format!("TLS_{rest}"),
        None => name,
    }
}

pub(crate) fn select_suites(
    names: Option<&[String]>,
) -> Result<Vec<SupportedCipherSuite>, TlsError> {
    let all = rustls::crypto::ring::ALL_CIPHER_SUITES;
    let Some(names) = names else {
        return Ok(rustls::crypto::ring::DEFAULT_CIPHER_SUITES.to_vec());
    };
    let mut out = Vec::new();
    for n in names {
        match all.iter().find(|s| suite_name(s) == *n) {
            Some(s) => out.push(*s),
            None => tracing::warn!("enabledCipherSuites: {n} is not supported and was ignored"),
        }
    }
    if out.is_empty() {
        return Err(TlsError::Config(
            "enabledCipherSuites names no supported cipher suite".into(),
        ));
    }
    Ok(out)
}

/// Refuses a client that asks for a host name `sniHost` does not match.
#[derive(Debug)]
struct SniCheck {
    pattern: regex::Regex,
    key: Arc<CertifiedKey>,
}

impl ResolvesServerCert for SniCheck {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        match hello.server_name() {
            Some(name) if !self.pattern.is_match(name) => {
                tracing::debug!("refusing SNI host {name}");
                None
            }
            _ => Some(self.key.clone()),
        }
    }
}

/// A TLS acceptor whose configuration can be swapped while it runs.
pub struct TlsAcceptor {
    settings: TlsSettings,
    current: RwLock<(Arc<ServerConfig>, Vec<Option<SystemTime>>)>,
}

impl TlsAcceptor {
    pub fn new(settings: TlsSettings) -> Result<Arc<Self>, TlsError> {
        let config = settings.server_config()?;
        let stamps = stamps(&settings.files());
        Ok(Arc::new(Self {
            settings,
            current: RwLock::new((config, stamps)),
        }))
    }

    pub fn settings(&self) -> &TlsSettings {
        &self.settings
    }

    fn config(&self) -> Arc<ServerConfig> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .clone()
    }

    /// The TLS handshake, within `sslHandshakeTimeout`.
    pub async fn accept(&self, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
        let acceptor = tokio_rustls::TlsAcceptor::from(self.config());
        match tokio::time::timeout(self.settings.handshake_timeout, acceptor.accept(stream)).await {
            Ok(result) => result,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "TLS handshake timed out",
            )),
        }
    }

    /// Reads the stores again if their files changed (`sslAutoReload`).
    pub fn reload_if_changed(&self) {
        let now = stamps(&self.settings.files());
        let changed = self.current.read().unwrap_or_else(|e| e.into_inner()).1 != now;
        if !changed {
            return;
        }
        match self.settings.server_config() {
            Ok(config) => {
                tracing::info!("reloaded the TLS keystore and truststore");
                *self.current.write().unwrap_or_else(|e| e.into_inner()) = (config, now);
            }
            Err(e) => tracing::warn!("TLS reload failed, keeping the previous stores: {e}"),
        }
    }

    /// Reads the stores again (`NettyAcceptor.reload`), keeping the
    /// previous ones if they cannot be read.
    pub fn reload(&self) -> Result<(), TlsError> {
        let now = stamps(&self.settings.files());
        let config = self.settings.server_config()?;
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = (config, now);
        Ok(())
    }

    /// With `sslAutoReload`, checks the store files every `period`.
    pub fn spawn_reloader(self: &Arc<Self>, period: Duration) {
        if !self.settings.auto_reload {
            return;
        }
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let Some(a) = this.upgrade() else { break };
                a.reload_if_changed();
            }
        });
    }
}

fn stamps(files: &[PathBuf]) -> Vec<Option<SystemTime>> {
    files
        .iter()
        .map(|f| std::fs::metadata(f).and_then(|m| m.modified()).ok())
        .collect()
}

/// The client's certificate chain, as the security store sees it.
pub fn peer_certificates(certs: Option<&[CertificateDer<'_>]>) -> Option<Vec<PeerCertificate>> {
    let certs = certs?;
    if certs.is_empty() {
        return None;
    }
    Some(
        certs
            .iter()
            .map(|c| {
                let (rfc, legacy) = saranga_security::dn::subject_names(c).unwrap_or_default();
                PeerCertificate {
                    der: c.to_vec(),
                    subject_dn: rfc,
                    subject_dn_legacy: legacy,
                    san_uris: saranga_security::dn::san_uris(c),
                }
            })
            .collect(),
    )
}
