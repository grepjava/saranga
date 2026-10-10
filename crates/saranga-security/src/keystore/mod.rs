//! Keystores, as `SSLSupport.loadKeystore` opens them: `JKS` (or a PKCS#12
//! file given as `JKS`, which Java accepts too), `PKCS12`, and the PEM
//! types of the `pem-keystore` provider Artemis ships: `PEM` (one file,
//! one entry), `PEMCA` (one entry per certificate) and `PEMCFG` (a
//! properties file naming the PEM files of one entry).

mod jks;

use std::path::{Path, PathBuf};

use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

#[derive(Debug, thiserror::Error)]
pub enum KeyStoreError {
    #[error("Failed to find a store at {0}")]
    NotFound(String),
    #[error("cannot read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("{0}")]
    Format(String),
    #[error("{0}")]
    Password(String),
    #[error("keystore type {0} is not supported")]
    UnsupportedType(String),
}

/// One keystore entry.
#[derive(Debug)]
pub enum Entry {
    /// A private key and its certificate chain (DER), leaf first.
    Key {
        key: PrivateKeyDer<'static>,
        chain: Vec<Vec<u8>>,
    },
    /// A trusted certificate (DER).
    Trusted(Vec<u8>),
    /// A JKS key not yet decrypted.
    ProtectedKey {
        protected: Vec<u8>,
        chain: Vec<Vec<u8>>,
    },
}

/// Aliases and entries, in the order the store lists them.
#[derive(Debug, Default)]
pub struct KeyStore {
    pub entries: Vec<(String, Entry)>,
}

impl KeyStore {
    /// `SSLSupport.loadKeystore`. `path` may be a file path or a `file:`
    /// URL; `None` or `NONE` gives an empty store.
    pub fn load(
        store_type: &str,
        path: Option<&str>,
        password: Option<&str>,
        key_password: Option<&str>,
    ) -> Result<Self, KeyStoreError> {
        let Some(path) = path.filter(|p| !p.is_empty() && !p.eq_ignore_ascii_case("NONE")) else {
            return Ok(Self::default());
        };
        let file = store_file(path)?;
        let read = |p: &Path| {
            std::fs::read(p).map_err(|source| KeyStoreError::Io {
                path: p.display().to_string(),
                source,
            })
        };
        let data = read(&file)?;
        match store_type.to_ascii_uppercase().as_str() {
            // `keystore.type.compat`: a JKS keystore object reads PKCS#12
            // files as well.
            "JKS" if data.starts_with(&jks::MAGIC.to_be_bytes()) => {
                jks::load(&data, password, key_password)
            }
            "JKS" | "PKCS12" => pkcs12(&data, password),
            "PEM" => pem(&[&data], "pem", false),
            "PEMCA" => pem(&[&data], "pem", true),
            "PEMCFG" => pem_config(&data, &file),
            other => Err(KeyStoreError::UnsupportedType(other.to_owned())),
        }
    }

    /// The private key entries.
    pub fn keys(&self) -> impl Iterator<Item = (&str, &PrivateKeyDer<'static>, &[Vec<u8>])> {
        self.entries.iter().filter_map(|(alias, e)| match e {
            Entry::Key { key, chain } => Some((alias.as_str(), key, chain.as_slice())),
            _ => None,
        })
    }

    /// The certificates a trust manager made from this store trusts: every
    /// trusted certificate entry and the leaf of every key entry
    /// (`TrustStoreUtil.getTrustedCerts`).
    pub fn trusted_certificates(&self) -> Vec<CertificateDer<'static>> {
        self.entries
            .iter()
            .filter_map(|(_, e)| match e {
                Entry::Trusted(c) => Some(c.clone()),
                Entry::Key { chain, .. } => chain.first().cloned(),
                Entry::ProtectedKey { .. } => None,
            })
            .map(CertificateDer::from)
            .collect()
    }

    /// The key a server presents: the entry `alias` names, or the first
    /// key entry.
    pub fn server_key(
        &self,
        alias: Option<&str>,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), KeyStoreError> {
        let found = match alias {
            Some(a) => self
                .keys()
                .find(|(name, _, _)| name.eq_ignore_ascii_case(a)),
            None => self.keys().next(),
        };
        let Some((_, key, chain)) = found else {
            return Err(KeyStoreError::Format(match alias {
                Some(a) => format!("no private key entry with alias {a}"),
                None => "the keystore holds no private key".into(),
            }));
        };
        if chain.is_empty() {
            return Err(KeyStoreError::Format(
                "the private key has no certificate".into(),
            ));
        }
        Ok((
            chain.iter().cloned().map(CertificateDer::from).collect(),
            key.clone_key(),
        ))
    }
}

/// `SSLSupport.validateStoreURL`: a URL, else a file.
fn store_file(path: &str) -> Result<PathBuf, KeyStoreError> {
    if let Some(p) = saranga_config_file_uri(path) {
        return Ok(p);
    }
    let p = PathBuf::from(path);
    if p.is_file() {
        Ok(p)
    } else {
        Err(KeyStoreError::NotFound(path.to_owned()))
    }
}

fn saranga_config_file_uri(path: &str) -> Option<PathBuf> {
    if !path.starts_with("file:") {
        return None;
    }
    saranga_config::file_uri_path(path)
}

fn pkcs12(data: &[u8], password: Option<&str>) -> Result<KeyStore, KeyStoreError> {
    use p12_keystore::{KeyStoreEntry, Pkcs12ImportPolicy};
    let store = p12_keystore::KeyStore::from_pkcs12(
        data,
        password.unwrap_or_default(),
        Pkcs12ImportPolicy::Relaxed,
    )
    .map_err(|e| KeyStoreError::Password(format!("cannot read the PKCS#12 keystore: {e}")))?;
    let mut out = KeyStore::default();
    for (alias, entry) in store.entries() {
        match entry {
            KeyStoreEntry::PrivateKeyChain(chain) => out.entries.push((
                alias.clone(),
                Entry::Key {
                    key: pkcs8(chain.key().as_der()),
                    chain: chain.certs().iter().map(|c| c.as_der().to_vec()).collect(),
                },
            )),
            KeyStoreEntry::Certificate(cert) => out
                .entries
                .push((alias.clone(), Entry::Trusted(cert.as_der().to_vec()))),
            KeyStoreEntry::Secret(_) => {}
        }
    }
    Ok(out)
}

pub(crate) fn pkcs8(der: &[u8]) -> PrivateKeyDer<'static> {
    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der.to_vec()))
}

/// The private key and the certificates of a PEM file, in order.
fn pem_items(data: &[u8]) -> Result<(Option<PrivateKeyDer<'static>>, Vec<Vec<u8>>), KeyStoreError> {
    use rustls_pki_types::pem::{PemObject, SectionKind};
    let mut key = None;
    let mut certs = Vec::new();
    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(data) {
        let (kind, der) = item.map_err(|e| KeyStoreError::Format(format!("bad PEM: {e}")))?;
        match kind {
            SectionKind::Certificate => certs.push(der),
            SectionKind::PrivateKey => key = Some(pkcs8(&der)),
            SectionKind::RsaPrivateKey => {
                key = Some(PrivateKeyDer::Pkcs1(der.into()));
            }
            SectionKind::EcPrivateKey => key = Some(PrivateKeyDer::Sec1(der.into())),
            _ => {}
        }
    }
    Ok((key, certs))
}

/// `PemUtils.loadFrom`: one entry `alias` holding the key and the chain,
/// or in bundle mode one entry per certificate (`alias-1`, ...).
fn pem(files: &[&[u8]], alias: &str, bundle: bool) -> Result<KeyStore, KeyStoreError> {
    let mut key = None;
    let mut certs = Vec::new();
    for data in files {
        let (k, c) = pem_items(data)?;
        key = k.or(key);
        certs.extend(c);
    }
    let mut out = KeyStore::default();
    if bundle {
        for (i, c) in certs.into_iter().enumerate() {
            out.entries
                .push((format!("{alias}-{}", i + 1), Entry::Trusted(c)));
        }
    } else if let Some(key) = key {
        out.entries
            .push((alias.to_owned(), Entry::Key { key, chain: certs }));
    } else if let Some(first) = certs.into_iter().next() {
        out.entries.push((alias.to_owned(), Entry::Trusted(first)));
    }
    Ok(out)
}

/// `PemUtils.loadFromConfiguration`: `alias` (default `pem`) and
/// `source.*` file names, merged into one entry.
fn pem_config(data: &[u8], config: &Path) -> Result<KeyStore, KeyStoreError> {
    let props = saranga_security_properties(data);
    let alias = props
        .iter()
        .find(|(k, _)| k == "alias")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "pem".to_owned());
    let mut files = Vec::new();
    for (k, v) in &props {
        if !k.starts_with("source.") {
            continue;
        }
        let path = v.strip_prefix("file://").unwrap_or(v);
        let mut p = PathBuf::from(path);
        if p.is_relative()
            && let Some(dir) = config.parent()
        {
            p = dir.join(p);
        }
        files.push(std::fs::read(&p).map_err(|source| KeyStoreError::Io {
            path: p.display().to_string(),
            source,
        })?);
    }
    let refs: Vec<&[u8]> = files.iter().map(Vec::as_slice).collect();
    pem(&refs, &alias, false)
}

fn saranga_security_properties(data: &[u8]) -> Vec<(String, String)> {
    crate::properties::parse(data)
}

#[cfg(test)]
mod tests;
