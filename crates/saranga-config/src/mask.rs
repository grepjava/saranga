//! Password masking. Ported from `PasswordMaskingUtil`,
//! `DefaultSensitiveStringCodec`, `SecureHashProcessor` and
//! `NoHashProcessor`.
//!
//! A masked value is written `ENC(...)`. The default codec has two
//! algorithms: `two-way` (the default, deprecated in Artemis) encrypts with
//! Blowfish under a fixed or configured key and can be decoded; `one-way`
//! stores a salted PBKDF2-HMAC-SHA1 hash (`iterations:salt:hash`, in hex),
//! which `artemis create` writes into `artemis-users.properties`, and can
//! only be verified.
//!
//! Only `DefaultSensitiveStringCodec` is available: a `password-codec` that
//! names another class is an error, as it is in Artemis when the class is
//! missing.

use blowfish::Blowfish;
use blowfish::cipher::generic_array::GenericArray;
use blowfish::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};

pub const BEGIN_ENC: &str = "ENC(";
pub const END_ENC: &str = ")";

/// `DefaultSensitiveStringCodec`'s class name, the default `password-codec`.
pub const DEFAULT_CODEC: &str = "org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec";

/// The Blowfish key used when none is configured.
const DEFAULT_TWO_WAY_KEY: &[u8] = b"clusterpassword";

/// `DefaultSensitiveStringCodec.KEY_SYSTEM_PROPERTY` as an environment
/// variable (`envVarNameFromSystemPropertyName`).
pub const KEY_ENV_VAR: &str = "ARTEMIS_DEFAULT_SENSITIVE_STRING_CODEC_KEY";

const PBKDF2_ITERATIONS: u32 = 1024;
const PBKDF2_KEY_BYTES: usize = 64;
const PBKDF2_SALT_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MaskError {
    #[error("codec class {0} is not available; only {DEFAULT_CODEC} is supported")]
    UnknownCodec(String),
    #[error("invalid codec property {0:?}")]
    InvalidProperty(String),
    #[error("invalid algorithm: {0}")]
    InvalidAlgorithm(String),
    #[error("password must be encrypted")]
    NotEncrypted,
    #[error("the masked password cannot be decoded")]
    BadCiphertext,
    #[error("algorithm doesn't support decoding")]
    OneWay,
}

pub fn is_enc_masked(value: &str) -> bool {
    value.starts_with(BEGIN_ENC) && value.ends_with(END_ENC) && value.len() >= 5
}

/// `ENC(x)` -> `x`.
pub fn unwrap(value: &str) -> &str {
    &value[BEGIN_ENC.len()..value.len() - END_ENC.len()]
}

pub fn wrap(value: &str) -> String {
    format!("{BEGIN_ENC}{value}{END_ENC}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Algorithm {
    TwoWay { key: Vec<u8> },
    OneWay,
}

/// A configured `DefaultSensitiveStringCodec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Codec {
    algorithm: Algorithm,
}

impl Codec {
    /// `PasswordMaskingUtil.getCodec`: `class;key=value;...`, or the default
    /// codec when `None`.
    pub fn from_desc(desc: Option<&str>) -> Result<Self, MaskError> {
        Self::from_desc_with(desc, |name| std::env::var(name).ok())
    }

    /// As [`from_desc`](Self::from_desc), reading the key's environment
    /// variable through `env`.
    pub fn from_desc_with(
        desc: Option<&str>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, MaskError> {
        let Some(desc) = desc else {
            return Self::init(&[], env);
        };
        let mut parts = desc.split(';');
        let class = parts.next().unwrap_or_default();
        if class != DEFAULT_CODEC {
            return Err(MaskError::UnknownCodec(class.to_owned()));
        }
        let mut params = Vec::new();
        for part in parts {
            // `split("=")` must give exactly two pieces.
            let kv: Vec<&str> = crate::java_split(part, '=');
            if kv.len() != 2 {
                return Err(MaskError::InvalidProperty(part.to_owned()));
            }
            params.push((kv[0].to_owned(), kv[1].to_owned()));
        }
        Self::init(&params, env)
    }

    /// `DefaultSensitiveStringCodec.init`.
    pub fn init(
        params: &[(String, String)],
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, MaskError> {
        let get = |name: &str| {
            params
                .iter()
                .rev()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let algorithm = match get("algorithm") {
            None | Some("two-way") => {
                let key = match get("key") {
                    Some(k) => k.as_bytes().to_vec(),
                    None => match env(KEY_ENV_VAR) {
                        Some(k) => k.into_bytes(),
                        None => DEFAULT_TWO_WAY_KEY.to_vec(),
                    },
                };
                Algorithm::TwoWay { key }
            }
            Some("one-way") => Algorithm::OneWay,
            Some(other) => return Err(MaskError::InvalidAlgorithm(other.to_owned())),
        };
        Ok(Self { algorithm })
    }

    /// The default codec with the one-way algorithm, which verifies the
    /// hashes in users files.
    pub fn one_way() -> Self {
        Self {
            algorithm: Algorithm::OneWay,
        }
    }

    pub fn is_one_way(&self) -> bool {
        self.algorithm == Algorithm::OneWay
    }

    pub fn decode(&self, secret: &str) -> Result<String, MaskError> {
        match &self.algorithm {
            Algorithm::TwoWay { key } => blowfish_decode(key, secret),
            Algorithm::OneWay => Err(MaskError::OneWay),
        }
    }

    pub fn encode(&self, secret: &str) -> Result<String, MaskError> {
        match &self.algorithm {
            Algorithm::TwoWay { key } => blowfish_encode(key, secret),
            Algorithm::OneWay => {
                let mut salt = [0u8; PBKDF2_SALT_BYTES];
                rand::RngCore::fill_bytes(&mut rand::rng(), &mut salt);
                Ok(pbkdf2_encode(secret, &salt, PBKDF2_ITERATIONS))
            }
        }
    }

    /// `SensitiveDataCodec.verify`: does `input` match the stored (unwrapped)
    /// value?
    pub fn verify(&self, input: &str, stored: &str) -> bool {
        match &self.algorithm {
            Algorithm::TwoWay { key } => blowfish_encode(key, input).is_ok_and(|e| e == stored),
            Algorithm::OneWay => pbkdf2_verify(input, stored),
        }
    }
}

/// `PasswordMaskingUtil.resolveMask`: the plain text of a configured
/// password. With `mask_password` unset, an `ENC(...)` value is decoded and
/// anything else is taken as it is; set to true, the whole value is masked;
/// set to false, it is plain.
pub fn resolve_mask(
    mask_password: Option<bool>,
    password: &str,
    codec: Option<&str>,
) -> Result<String, MaskError> {
    match mask_password {
        None if is_enc_masked(password) => Codec::from_desc(codec)?.decode(unwrap(password)),
        None | Some(false) => Ok(password.to_owned()),
        Some(true) => Codec::from_desc(codec)?.decode(password),
    }
}

/// `HashProcessor.compare` as `PasswordMaskingUtil.getHashProcessor`
/// chooses it: a stored `ENC(...)` value is verified by `codec` (the
/// one-way default when `None`), anything else must equal the input.
pub fn password_matches(input: &str, stored: &str, codec: Option<&Codec>) -> bool {
    if !is_enc_masked(stored) {
        return input == stored;
    }
    let default = Codec::one_way();
    codec.unwrap_or(&default).verify(input, unwrap(stored))
}

fn pbkdf2_encode(secret: &str, salt: &[u8], iterations: u32) -> String {
    let mut hash = [0u8; PBKDF2_KEY_BYTES];
    pbkdf2::pbkdf2_hmac::<sha1::Sha1>(secret.as_bytes(), salt, iterations, &mut hash);
    format!(
        "{iterations}:{}:{}",
        crate::bytes_to_hex(salt),
        crate::bytes_to_hex(&hash)
    )
}

/// `PBKDF2Algorithm.verify`. Java's PBKDF2 feeds the password to HMAC as
/// UTF-8.
fn pbkdf2_verify(input: &str, stored: &str) -> bool {
    let parts: Vec<&str> = crate::java_split(stored, ':');
    let [iterations, salt, hash] = parts[..] else {
        return false;
    };
    let (Ok(iterations), Some(salt), Some(expected)) = (
        iterations.parse::<u32>(),
        crate::hex_to_bytes(salt),
        crate::hex_to_bytes(hash),
    ) else {
        return false;
    };
    if iterations == 0 || expected.is_empty() {
        return false;
    }
    let mut derived = vec![0u8; expected.len()];
    pbkdf2::pbkdf2_hmac::<sha1::Sha1>(input.as_bytes(), &salt, iterations, &mut derived);
    derived == expected
}

fn blowfish(key: &[u8]) -> Result<Blowfish, MaskError> {
    Blowfish::new_from_slice(key).map_err(|_| MaskError::BadCiphertext)
}

/// `Blowfish/ECB/PKCS5Padding`, then `new BigInteger(bytes).toString(16)`.
fn blowfish_encode(key: &[u8], secret: &str) -> Result<String, MaskError> {
    let cipher = blowfish(key)?;
    let mut data = secret.as_bytes().to_vec();
    let pad = 8 - data.len() % 8;
    data.extend(std::iter::repeat_n(pad as u8, pad));
    for block in data.chunks_mut(8) {
        cipher.encrypt_block(GenericArray::from_mut_slice(block));
    }
    Ok(signed_to_hex(&data))
}

/// `new BigInteger(secret, 16).toByteArray()`, padded with leading zeros to
/// whole blocks, then decrypted.
fn blowfish_decode(key: &[u8], secret: &str) -> Result<String, MaskError> {
    let mut data = hex_to_signed(secret).ok_or(MaskError::NotEncrypted)?;
    if data.len() % 8 != 0 {
        let pad = 8 - data.len() % 8;
        data.splice(0..0, std::iter::repeat_n(0u8, pad));
    }
    let cipher = blowfish(key)?;
    for block in data.chunks_mut(8) {
        cipher.decrypt_block(GenericArray::from_mut_slice(block));
    }
    let pad = *data.last().ok_or(MaskError::BadCiphertext)? as usize;
    if pad == 0
        || pad > 8
        || pad > data.len()
        || data[data.len() - pad..].iter().any(|&b| b as usize != pad)
    {
        return Err(MaskError::BadCiphertext);
    }
    data.truncate(data.len() - pad);
    Ok(String::from_utf8_lossy(&data).into_owned())
}

/// `new BigInteger(bytes).toString(16)`: the bytes as a signed big-endian
/// two's complement number, in lowercase hex without leading zeros.
fn signed_to_hex(bytes: &[u8]) -> String {
    let negative = bytes.first().is_some_and(|b| b & 0x80 != 0);
    let magnitude = if negative {
        negate(bytes)
    } else {
        bytes.to_vec()
    };
    let hex: String = magnitude.iter().map(|b| format!("{b:02x}")).collect();
    let trimmed = hex.trim_start_matches('0');
    let digits = if trimmed.is_empty() { "0" } else { trimmed };
    if negative {
        format!("-{digits}")
    } else {
        digits.to_owned()
    }
}

/// `new BigInteger(hex, 16).toByteArray()`: the minimal two's complement
/// encoding. `None` when the text is not a hex number.
fn hex_to_signed(text: &str) -> Option<Vec<u8>> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let padded = if digits.len() % 2 == 1 {
        format!("0{digits}")
    } else {
        digits.to_owned()
    };
    let mut magnitude: Vec<u8> = (0..padded.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&padded[i..i + 2], 16).unwrap_or_default())
        .collect();
    while magnitude.len() > 1 && magnitude[0] == 0 {
        magnitude.remove(0);
    }
    if magnitude == [0] {
        return Some(vec![0]);
    }
    if !negative {
        // A sign byte when the top bit is set.
        if magnitude[0] & 0x80 != 0 {
            magnitude.insert(0, 0);
        }
        return Some(magnitude);
    }
    // Negative: the shortest two's complement whose top bit is set.
    let mut bytes = negate(&magnitude);
    if bytes[0] & 0x80 == 0 {
        bytes.insert(0, 0xff);
    }
    while bytes.len() > 1 && bytes[0] == 0xff && bytes[1] & 0x80 != 0 {
        bytes.remove(0);
    }
    Some(bytes)
}

/// Two's complement negation at the same width.
fn negate(bytes: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = bytes.iter().map(|b| !b).collect();
    for b in out.iter_mut().rev() {
        let (v, carry) = b.overflowing_add(1);
        *b = v;
        if !carry {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Written by `artemis create --user admin --password secret` (2.57.0).
    const ADMIN_SECRET: &str = "ENC(1024:C64C51B47FE4EF732634D1434FD3D8C6091321464F97D4D03C031C85C051BBBF:9418F3F3A278B5532F8A87AD4E578A7D0B142AFA919DD900FD220917221E9B48A78D724BE8712D4E66B5E983E3EC4DABA5991BD85468EF214241B76B4108A5C9)";

    #[test]
    fn verifies_artemis_create_hash() {
        assert!(password_matches("secret", ADMIN_SECRET, None));
        assert!(!password_matches("Secret", ADMIN_SECRET, None));
        assert!(!password_matches("", ADMIN_SECRET, None));
    }

    /// `DefaultSensitiveStringCodec.main` and `artemis mask` (2.57.0).
    #[test]
    fn matches_java_two_way() {
        let codec = Codec::from_desc_with(None, |_| None).unwrap();
        for (plain, masked) in [
            ("secret", "-41e444c3ed07d6dd"),
            ("password", "3a34fd21b82bf2a822fa49a8d8fa115d"),
            (
                "a longer secret than one block",
                "7b43caeb9ceedf3117fd3829b3cb250e3ca80b97e766c69ab41756a816111dec",
            ),
            ("x", "-2dbd0e4caa70a911"),
        ] {
            assert_eq!(codec.encode(plain).unwrap(), masked);
            assert_eq!(codec.decode(masked).unwrap(), plain);
        }
        let keyed = Codec::init(&[("key".into(), "mykey".into())], |_| None).unwrap();
        assert_eq!(keyed.encode("secret").unwrap(), "-1425f1ea4cff5d91");
        let from_env = Codec::from_desc_with(None, |_| Some("mykey".into())).unwrap();
        assert_eq!(from_env.decode("-1425f1ea4cff5d91").unwrap(), "secret");
        // `artemis mask --hash`.
        assert!(password_matches(
            "secret",
            "ENC(1024:A06F08AD15C8C819B1C97B8000DB4416A45043A41E67BA0093B6A88D070D2664:52DB35301FF9F90D20BF602313D8EB208C383E57EA8FD794142B193174C4868E89833C27B0214C6F6527E3618D194D55898835889D174B20E56436E664CA73AE)",
            None
        ));
    }

    #[test]
    fn plain_passwords_compare_exactly() {
        assert!(password_matches("pw", "pw", None));
        assert!(!password_matches("pw", "pw ", None));
    }

    #[test]
    fn one_way_round_trip() {
        let codec = Codec::one_way();
        let stored = codec.encode("hello").unwrap();
        assert!(stored.starts_with("1024:"));
        assert!(codec.verify("hello", &stored));
        assert!(!codec.verify("hellO", &stored));
        assert_eq!(codec.decode(&stored), Err(MaskError::OneWay));
    }

    #[test]
    fn two_way_round_trip() {
        let codec = Codec::from_desc_with(None, |_| None).unwrap();
        for secret in [
            "",
            "a",
            "password",
            "12345678",
            "a longer secret than one block",
        ] {
            let masked = codec.encode(secret).unwrap();
            assert_eq!(codec.decode(&masked).unwrap(), secret, "{masked}");
            assert!(codec.verify(secret, &masked));
        }
    }

    #[test]
    fn codec_description() {
        assert!(
            Codec::from_desc_with(Some(&format!("{DEFAULT_CODEC};algorithm=one-way")), |_| {
                None
            })
            .unwrap()
            .is_one_way()
        );
        assert_eq!(
            Codec::from_desc(Some("com.example.Codec")),
            Err(MaskError::UnknownCodec("com.example.Codec".into()))
        );
        assert_eq!(
            Codec::from_desc(Some(&format!("{DEFAULT_CODEC};algorithm"))),
            Err(MaskError::InvalidProperty("algorithm".into()))
        );
    }

    #[test]
    fn signed_hex() {
        assert_eq!(signed_to_hex(&[0x00, 0x01]), "1");
        assert_eq!(signed_to_hex(&[0xff, 0xff]), "-1");
        assert_eq!(signed_to_hex(&[0x80, 0x00]), "-8000");
        assert_eq!(hex_to_signed("1"), Some(vec![1]));
        assert_eq!(hex_to_signed("80"), Some(vec![0, 0x80]));
        assert_eq!(hex_to_signed("-1"), Some(vec![0xff]));
        assert_eq!(hex_to_signed("-80"), Some(vec![0x80]));
        assert_eq!(hex_to_signed("-81"), Some(vec![0xff, 0x7f]));
        assert_eq!(hex_to_signed("xyz"), None);
    }

    #[test]
    fn resolve() {
        let masked = Codec::from_desc(None).unwrap().encode("pw").unwrap();
        assert_eq!(resolve_mask(None, &wrap(&masked), None).unwrap(), "pw");
        assert_eq!(resolve_mask(None, "pw", None).unwrap(), "pw");
        assert_eq!(resolve_mask(Some(true), &masked, None).unwrap(), "pw");
        assert_eq!(resolve_mask(Some(false), &masked, None).unwrap(), masked);
    }
}
