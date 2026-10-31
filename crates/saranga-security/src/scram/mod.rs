//! SCRAM (RFC 5802) as Artemis computes it: the mechanisms (`SCRAM`), the
//! data a server keeps for a user (`UserData`), the key derivations
//! (`ScramUtils`) and SASLprep for user names (`StringPrep`).

mod string_prep;
mod tables;

pub use string_prep::{StringPrepError, is_containing_prohibited_characters, prep_as_query_string};

use ring::{digest, hmac};

/// `SCRAM`, in order of precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scram {
    Sha512,
    Sha256,
}

impl Scram {
    /// `SCRAM.values()`.
    pub const ALL: [Scram; 2] = [Scram::Sha512, Scram::Sha256];

    /// `getName`: the SASL mechanism.
    pub fn name(self) -> &'static str {
        match self {
            Scram::Sha256 => "SCRAM-SHA-256",
            Scram::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// The enum constant's name (`SCRAM.name()`), which users files put
    /// after a user name: `user|SHA256`.
    pub fn constant(self) -> &'static str {
        match self {
            Scram::Sha256 => "SHA256",
            Scram::Sha512 => "SHA512",
        }
    }

    pub fn from_name(mechanism: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.name() == mechanism)
    }

    /// The `MessageDigest`.
    pub fn digest(self, data: &[u8]) -> Vec<u8> {
        let algorithm = match self {
            Scram::Sha256 => &digest::SHA256,
            Scram::Sha512 => &digest::SHA512,
        };
        digest::digest(algorithm, data).as_ref().to_vec()
    }

    /// `ScramUtils.computeHmac`: the `Mac` of `data` under `key`. An empty
    /// key fails, as `SecretKeySpec` refuses one.
    pub fn hmac(self, key: &[u8], data: &[u8]) -> Result<Vec<u8>, ScramError> {
        if key.is_empty() {
            return Err(ScramError::new("Empty key"));
        }
        let algorithm = match self {
            Scram::Sha256 => hmac::HMAC_SHA256,
            Scram::Sha512 => hmac::HMAC_SHA512,
        };
        Ok(hmac::sign(&hmac::Key::new(algorithm, key), data)
            .as_ref()
            .to_vec())
    }
}

/// `ScramException`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ScramError(pub String);

impl ScramError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// `UserData`: what a server keeps of a user's password, Base64 encoded.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserData {
    pub salt: String,
    pub iterations: i32,
    pub server_key: String,
    pub stored_key: String,
}

/// `String.getBytes(US_ASCII)`: a character outside ASCII becomes `?`.
pub fn ascii(s: &str) -> Vec<u8> {
    s.chars()
        .map(|c| if c.is_ascii() { c as u8 } else { b'?' })
        .collect()
}

/// `ScramUtils.generateSaltedPassword`: `Hi(password, salt, i)`, the
/// password taken as ASCII.
pub fn salted_password(
    scram: Scram,
    password: &str,
    salt: &[u8],
    iterations: i32,
) -> Result<Vec<u8>, ScramError> {
    let key = ascii(password);
    let mut first = salt.to_vec();
    first.extend_from_slice(&[0, 0, 0, 1]);
    let mut result = scram.hmac(&key, &first)?;
    let mut previous = result.clone();
    for _ in 1..iterations {
        previous = scram.hmac(&key, &previous)?;
        for (r, p) in result.iter_mut().zip(&previous) {
            *r ^= p;
        }
    }
    Ok(result)
}

/// `ScramUtils.NewPasswordByteArrayData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPassword {
    pub salted_password: Vec<u8>,
    pub salt: Vec<u8>,
    pub client_key: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: i32,
}

impl NewPassword {
    /// `byteArrayToStringData`, as the `UserData` it gives.
    pub fn user_data(&self) -> UserData {
        UserData {
            salt: base64_encode(&self.salt),
            iterations: self.iterations,
            server_key: base64_encode(&self.server_key),
            stored_key: base64_encode(&self.stored_key),
        }
    }
}

/// `ScramUtils.newPassword`: the keys of a password in clear.
pub fn new_password(
    scram: Scram,
    password: &str,
    salt: &[u8],
    iterations: i32,
) -> Result<NewPassword, ScramError> {
    let salted = salted_password(scram, password, salt, iterations)?;
    let client_key = scram.hmac(&salted, b"Client Key")?;
    let stored_key = scram.digest(&client_key);
    let server_key = scram.hmac(&salted, b"Server Key")?;
    Ok(NewPassword {
        salted_password: salted,
        salt: salt.to_vec(),
        client_key,
        stored_key,
        server_key,
        iterations,
    })
}

/// `SecureRandom.nextBytes`.
pub fn random_bytes(n: usize) -> Vec<u8> {
    use ring::rand::SecureRandom;
    let mut out = vec![0u8; n];
    ring::rand::SystemRandom::new()
        .fill(&mut out)
        .expect("the system's random number generator");
    out
}

/// `UUID.randomUUID().toString()`, the nonces Artemis uses.
pub fn random_uuid() -> String {
    let mut b = random_bytes(16);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `Base64.getEncoder().encodeToString`.
pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let v = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(B64[((v >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// `Base64.getDecoder().decode`: `None` where Java throws
/// `IllegalArgumentException`. Padding may be left out, but where there is
/// some it must be right, and nothing may follow it.
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    let end = bytes.iter().position(|&c| c == b'=').unwrap_or(bytes.len());
    let (data, padding) = bytes.split_at(end);
    let mut out = Vec::with_capacity(data.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for &c in data {
        let v = B64.iter().position(|&x| x == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    match (data.len() % 4, padding.len()) {
        (1, _) => None,
        (_, 0) => Some(out),
        (2, 2) | (3, 1) if padding.iter().all(|&c| c == b'=') => Some(out),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's example: user "user", password "pencil", salt
    /// W22ZaJ0SNY7soEsUEjb6gQ== and 4096 iterations.
    #[test]
    fn keys_are_those_of_rfc_7677() {
        let salt = base64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let p = new_password(Scram::Sha256, "pencil", &salt, 4096).unwrap();
        let client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let without_proof = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let auth = format!("{client_first_bare},{server_first},{without_proof}");
        let signature = Scram::Sha256.hmac(&p.stored_key, auth.as_bytes()).unwrap();
        let proof: Vec<u8> = p
            .client_key
            .iter()
            .zip(&signature)
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(
            base64_encode(&proof),
            "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        let server = Scram::Sha256.hmac(&p.server_key, auth.as_bytes()).unwrap();
        assert_eq!(
            base64_encode(&server),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn base64_decodes_as_java_does() {
        assert_eq!(base64_decode("YWI=").unwrap(), b"ab");
        assert_eq!(base64_decode("YWI").unwrap(), b"ab");
        assert_eq!(base64_decode("YWJj").unwrap(), b"abc");
        assert!(base64_decode("YWI==").is_none());
        assert!(base64_decode("YWJjZ").is_none());
        assert!(base64_decode("YW=I").is_none());
        assert!(base64_decode("YW*I").is_none());
        assert_eq!(base64_encode(b"ab"), "YWI=");
    }

    #[test]
    fn an_empty_password_is_an_empty_key() {
        assert!(salted_password(Scram::Sha256, "", b"salt", 4096).is_err());
    }

    #[test]
    fn a_nonce_is_a_version_4_uuid() {
        let u = random_uuid();
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
    }
}
