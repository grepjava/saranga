//! Java's proprietary `JKS` keystore format. Ported from
//! `sun.security.provider.JavaKeyStore` and `KeyProtector`.
//!
//! ```text
//! magic 0xFEEDFEED, version 1 or 2, entry count
//! entry: tag 1 (private key) | tag 2 (trusted certificate), alias (UTF),
//!        creation date (long), ...
//! SHA-1 over the password (UTF-16BE), "Mighty Aphrodite" and all of the above
//! ```
//!
//! Private keys are an `EncryptedPrivateKeyInfo` whose data is a salt, the
//! PKCS#8 key XORed with a SHA-1 keystream of the password and salt, and a
//! SHA-1 check of the password and the plain key.

use sha1::{Digest, Sha1};

use super::{Entry, KeyStore, KeyStoreError};
use crate::der;

pub const MAGIC: u32 = 0xFEED_FEED;
/// `KeyProtector.KEY_PROTECTOR_OID`: 1.3.6.1.4.1.42.2.17.1.1.
const KEY_PROTECTOR_OID: &[u8] = &[0x2b, 0x06, 0x01, 0x04, 0x01, 0x2a, 0x02, 0x11, 0x01, 0x01];
const DIGEST_LEN: usize = 20;

/// The password as Java's `char[]` feeds it to SHA-1: UTF-16, big-endian.
fn password_bytes(password: &str) -> Vec<u8> {
    password.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

struct Input<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Input<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], KeyStoreError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.data.len())
            .ok_or_else(|| KeyStoreError::Format("JKS keystore is truncated".into()))?;
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32, KeyStoreError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64, KeyStoreError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    /// `DataInputStream.readUTF`: a 16-bit length and modified UTF-8.
    fn utf(&mut self) -> Result<String, KeyStoreError> {
        let len = u16::from_be_bytes(self.take(2)?.try_into().expect("2 bytes")) as usize;
        Ok(decode_modified_utf8(self.take(len)?))
    }

    fn bytes(&mut self) -> Result<&'a [u8], KeyStoreError> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}

/// Modified UTF-8 is UTF-8 except for NUL (`C0 80`) and supplementary
/// characters (as surrogate pairs); aliases rarely hold either.
fn decode_modified_utf8(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0xC0 && bytes.get(i + 1) == Some(&0x80) {
            out.push(0);
            i += 2;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `JavaKeyStore.engineLoad`, then `engineGetKey` for each private key.
/// `key_password` decrypts private keys (the store password when `None`).
pub fn load(
    data: &[u8],
    password: Option<&str>,
    key_password: Option<&str>,
) -> Result<KeyStore, KeyStoreError> {
    let mut input = Input { data, pos: 0 };
    if input.u32()? != MAGIC {
        return Err(KeyStoreError::Format("not a JKS keystore".into()));
    }
    let version = input.u32()?;
    if version != 1 && version != 2 {
        return Err(KeyStoreError::Format(format!(
            "unsupported JKS version {version}"
        )));
    }
    let count = input.u32()?;
    let mut store = KeyStore::default();
    for _ in 0..count {
        let tag = input.u32()?;
        let alias = input.utf()?;
        let _created = input.i64()?;
        match tag {
            1 => {
                let protected = input.bytes()?.to_vec();
                let n = input.u32()?;
                let mut chain = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    if version == 2 {
                        let _type = input.utf()?;
                    }
                    chain.push(input.bytes()?.to_vec());
                }
                store
                    .entries
                    .push((alias, Entry::ProtectedKey { protected, chain }));
            }
            2 => {
                if version == 2 {
                    let _type = input.utf()?;
                }
                store
                    .entries
                    .push((alias, Entry::Trusted(input.bytes()?.to_vec())));
            }
            other => {
                return Err(KeyStoreError::Format(format!(
                    "unrecognized JKS keystore entry tag {other}"
                )));
            }
        }
    }
    if let Some(password) = password {
        let mut md = Sha1::new();
        md.update(password_bytes(password));
        md.update(b"Mighty Aphrodite");
        md.update(&data[..input.pos]);
        let expected = input.take(DIGEST_LEN)?;
        if md.finalize().as_slice() != expected {
            return Err(KeyStoreError::Password(
                "Keystore was tampered with, or password was incorrect".into(),
            ));
        }
    }
    // `engineGetKey`: decrypt with the key password.
    let key_password = key_password.or(password);
    for (_, entry) in &mut store.entries {
        if let Entry::ProtectedKey { protected, chain } = entry {
            let Some(pw) = key_password else {
                return Err(KeyStoreError::Password(
                    "a password is needed to recover the private key".into(),
                ));
            };
            let key = recover(protected, pw)?;
            *entry = Entry::Key {
                key: super::pkcs8(&key),
                chain: std::mem::take(chain),
            };
        }
    }
    Ok(store)
}

/// `KeyProtector.recover`: the PKCS#8 encoding of a protected key.
fn recover(protected: &[u8], password: &str) -> Result<Vec<u8>, KeyStoreError> {
    let bad = |what: &str| KeyStoreError::Format(format!("JKS private key: {what}"));
    // EncryptedPrivateKeyInfo ::= SEQUENCE { AlgorithmIdentifier, OCTET STRING }
    let (info, _) = der::read(protected, der::SEQUENCE).ok_or_else(|| bad("not DER"))?;
    let (alg, rest) = der::read(info, der::SEQUENCE).ok_or_else(|| bad("no algorithm"))?;
    let (oid, _) = der::read(alg, der::OID).ok_or_else(|| bad("no algorithm OID"))?;
    if oid != KEY_PROTECTOR_OID {
        return Err(KeyStoreError::Format(
            "JKS private key is not protected by the Sun key protector".into(),
        ));
    }
    let (data, _) = der::read(rest, der::OCTET_STRING).ok_or_else(|| bad("no data"))?;
    if data.len() < 2 * DIGEST_LEN {
        return Err(bad("too short"));
    }
    let salt = &data[..DIGEST_LEN];
    let encrypted = &data[DIGEST_LEN..data.len() - DIGEST_LEN];
    let check = &data[data.len() - DIGEST_LEN..];
    let pw = password_bytes(password);
    let mut digest = salt.to_vec();
    let mut keystream = Vec::with_capacity(encrypted.len() + DIGEST_LEN);
    while keystream.len() < encrypted.len() {
        let mut md = Sha1::new();
        md.update(&pw);
        md.update(&digest);
        digest = md.finalize().to_vec();
        keystream.extend_from_slice(&digest);
    }
    let plain: Vec<u8> = encrypted
        .iter()
        .zip(&keystream)
        .map(|(a, b)| a ^ b)
        .collect();
    let mut md = Sha1::new();
    md.update(&pw);
    md.update(&plain);
    if md.finalize().as_slice() != check {
        return Err(KeyStoreError::Password("Cannot recover key".into()));
    }
    Ok(plain)
}
