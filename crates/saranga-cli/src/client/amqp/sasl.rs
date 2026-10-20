//! Qpid JMS's SASL client (`SaslMechanismFinder`, the mechanisms of
//! `org.apache.qpid.jms.sasl`, `AmqpSaslAuthenticator`): the mechanism
//! chosen from what the server offers, and its exchange.

use ring::{digest, hmac, pbkdf2};

/// A mechanism this client can run, with its priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    External,
    Scram(Sha),
    Plain,
    XOAuth2,
    Anonymous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sha {
    Sha1,
    Sha256,
    Sha512,
}

impl Mechanism {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "EXTERNAL" => Self::External,
            "SCRAM-SHA-512" => Self::Scram(Sha::Sha512),
            "SCRAM-SHA-256" => Self::Scram(Sha::Sha256),
            "SCRAM-SHA-1" => Self::Scram(Sha::Sha1),
            "PLAIN" => Self::Plain,
            "XOAUTH2" => Self::XOAuth2,
            "ANONYMOUS" => Self::Anonymous,
            _ => return None,
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::External => "EXTERNAL",
            Self::Scram(Sha::Sha512) => "SCRAM-SHA-512",
            Self::Scram(Sha::Sha256) => "SCRAM-SHA-256",
            Self::Scram(Sha::Sha1) => "SCRAM-SHA-1",
            Self::Plain => "PLAIN",
            Self::XOAuth2 => "XOAUTH2",
            Self::Anonymous => "ANONYMOUS",
        }
    }

    fn priority(&self) -> u8 {
        match self {
            Self::External => 8,
            Self::Scram(Sha::Sha512) => 7,
            Self::Scram(Sha::Sha256) => 6,
            Self::Scram(Sha::Sha1) => 5,
            Self::Plain => 2,
            Self::XOAuth2 => 1,
            Self::Anonymous => 0,
        }
    }

    /// `isApplicable(username, password, localPrincipal)`.
    fn applicable(&self, user: Option<&str>, password: Option<&str>, principal: bool) -> bool {
        let both = user.is_some_and(|u| !u.is_empty()) && password.is_some_and(|p| !p.is_empty());
        match self {
            Self::External => principal,
            Self::Scram(_) | Self::Plain => both,
            Self::XOAuth2 => {
                both && password.is_some_and(|p| p.bytes().all(|b| (0x20..=0x7f).contains(&b)))
            }
            Self::Anonymous => true,
        }
    }
}

/// `SaslMechanismFinder.findMatchingMechanism`: of the offered mechanisms
/// this client knows and can use (within `restriction`), the one of the
/// highest priority, the later offered on a tie.
pub fn choose(
    offered: &[String],
    user: Option<&str>,
    password: Option<&str>,
    principal: bool,
    restriction: Option<&[String]>,
) -> Result<Mechanism, String> {
    let mut found: Vec<Mechanism> = offered
        .iter()
        .filter(|n| restriction.is_none_or(|r| r.iter().any(|x| x == *n)))
        .filter_map(|n| Mechanism::from_name(n))
        .filter(|m| m.applicable(user, password, principal))
        .collect();
    // A stable sort, the last taken.
    found.sort_by_key(Mechanism::priority);
    found.last().copied().ok_or_else(|| {
        format!(
            "Could not find a suitable SASL mechanism. No supported mechanism, or none usable with the available credentials. Server offered: [{}]",
            offered.join(", ")
        )
    })
}

/// A mechanism's exchange in progress.
pub struct Exchange {
    pub mechanism: Mechanism,
    scram: Option<Scram>,
}

struct Scram {
    sha: Sha,
    user: String,
    password: String,
    nonce: String,
    client_first_bare: String,
    server_signature: Option<Vec<u8>>,
    done: bool,
}

impl Exchange {
    pub fn new(mechanism: Mechanism, user: Option<&str>, password: Option<&str>) -> Self {
        let scram = match mechanism {
            Mechanism::Scram(sha) => Some(Scram {
                sha,
                user: user.unwrap_or_default().to_owned(),
                password: password.unwrap_or_default().to_owned(),
                nonce: random_uuid(),
                client_first_bare: String::new(),
                server_signature: None,
                done: false,
            }),
            _ => None,
        };
        Self { mechanism, scram }
    }

    /// `getInitialResponse`.
    pub fn initial_response(
        &mut self,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Option<Vec<u8>>, String> {
        let user = user.unwrap_or_default();
        let password = password.unwrap_or_default();
        Ok(Some(match self.mechanism {
            Mechanism::External | Mechanism::Anonymous => Vec::new(),
            Mechanism::Plain => {
                let mut v = vec![0];
                v.extend_from_slice(user.as_bytes());
                v.push(0);
                v.extend_from_slice(password.as_bytes());
                v
            }
            Mechanism::XOAuth2 => {
                format!("user={user}\x01auth=Bearer {password}\x01\x01").into_bytes()
            }
            Mechanism::Scram(_) => {
                let s = self.scram.as_mut().expect("scram");
                if !s.user.is_ascii() || !s.password.is_ascii() {
                    return Err(
                        "Can only encode names and passwords which are restricted to ASCII characters"
                            .to_owned(),
                    );
                }
                let name = s.user.replace('=', "=3D").replace(',', "=2C");
                s.client_first_bare = format!("n={name},r={}", s.nonce);
                format!("n,,{}", s.client_first_bare).into_bytes()
            }
        }))
    }

    /// `getChallengeResponse`.
    pub fn challenge(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let Some(s) = self.scram.as_mut() else {
            return Ok(Some(Vec::new()));
        };
        if s.server_signature.is_none() {
            let text = String::from_utf8_lossy(challenge).into_owned();
            let field = |k: &str| {
                text.split(',')
                    .find_map(|p| p.strip_prefix(&format!("{k}=")).map(str::to_owned))
            };
            let nonce = field("r").ok_or("Server challenge did not contain a nonce")?;
            if !nonce.starts_with(&s.nonce) {
                return Err("Server challenge did not use correct client nonce".to_owned());
            }
            let salt = field("s")
                .and_then(|v| base64_decode(&v))
                .ok_or("Server challenge did not contain a salt")?;
            let iterations: u32 = field("i")
                .and_then(|v| v.parse().ok())
                .ok_or("Server challenge did not contain an iteration count")?;
            let (alg, digest_alg, len) = s.sha.algorithms();
            let mut salted = vec![0u8; len];
            pbkdf2::derive(
                alg,
                std::num::NonZeroU32::new(iterations.max(1)).expect("non-zero"),
                &salt,
                s.password.as_bytes(),
                &mut salted,
            );
            let key = hmac::Key::new(s.sha.hmac(), &salted);
            let client_key = hmac::sign(&key, b"Client Key");
            let stored_key = digest::digest(digest_alg, client_key.as_ref());
            let client_final_bare = format!("c=biws,r={nonce}");
            let auth_message = format!("{},{text},{client_final_bare}", s.client_first_bare);
            let signature = hmac::sign(
                &hmac::Key::new(s.sha.hmac(), stored_key.as_ref()),
                auth_message.as_bytes(),
            );
            let proof: Vec<u8> = client_key
                .as_ref()
                .iter()
                .zip(signature.as_ref())
                .map(|(a, b)| a ^ b)
                .collect();
            let server_key = hmac::sign(&key, b"Server Key");
            s.server_signature = Some(
                hmac::sign(
                    &hmac::Key::new(s.sha.hmac(), server_key.as_ref()),
                    auth_message.as_bytes(),
                )
                .as_ref()
                .to_vec(),
            );
            return Ok(Some(
                format!("{client_final_bare},p={}", base64_encode(&proof)).into_bytes(),
            ));
        }
        // The server's final message: its signature checked.
        let text = String::from_utf8_lossy(challenge).into_owned();
        let v = text
            .strip_prefix("v=")
            .and_then(base64_decode)
            .ok_or("Server final message did not contain verifier")?;
        if Some(v) != s.server_signature {
            return Err("Server signature did not match".to_owned());
        }
        s.done = true;
        Ok(None)
    }

    /// `verifyCompletion`.
    pub fn verify_completion(&self) -> Result<(), String> {
        match &self.scram {
            Some(s) if !s.done => Err(
                "SASL exchange was not fully completed. Expected state COMPLETE but actual state CLIENT_PROOF_SENT"
                    .to_owned(),
            ),
            _ => Ok(()),
        }
    }
}

impl Sha {
    fn algorithms(&self) -> (pbkdf2::Algorithm, &'static digest::Algorithm, usize) {
        match self {
            Sha::Sha1 => (
                pbkdf2::PBKDF2_HMAC_SHA1,
                &digest::SHA1_FOR_LEGACY_USE_ONLY,
                20,
            ),
            Sha::Sha256 => (pbkdf2::PBKDF2_HMAC_SHA256, &digest::SHA256, 32),
            Sha::Sha512 => (pbkdf2::PBKDF2_HMAC_SHA512, &digest::SHA512, 64),
        }
    }

    fn hmac(&self) -> hmac::Algorithm {
        match self {
            Sha::Sha1 => hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
            Sha::Sha256 => hmac::HMAC_SHA256,
            Sha::Sha512 => hmac::HMAC_SHA512,
        }
    }
}

/// `UUID.randomUUID().toString()`.
pub fn random_uuid() -> String {
    let mut b = [0u8; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b).expect("random bytes");
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: Vec<String> = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        h[0..4].concat(),
        h[4..6].concat(),
        h[6..8].concat(),
        h[8..10].concat(),
        h[10..16].concat()
    )
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let n = chunk.len();
        let v = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= n {
                out.push(B64[((v >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = B64.iter().position(|x| *x == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mechanisms_are_chosen_as_qpid_chooses_them() {
        let offered = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let m = choose(
            &offered(&["PLAIN", "ANONYMOUS"]),
            Some("a"),
            Some("b"),
            false,
            None,
        );
        assert_eq!(m, Ok(Mechanism::Plain));
        let m = choose(&offered(&["PLAIN", "ANONYMOUS"]), None, None, false, None);
        assert_eq!(m, Ok(Mechanism::Anonymous));
        let m = choose(
            &offered(&["SCRAM-SHA-256", "PLAIN"]),
            Some("a"),
            Some("b"),
            false,
            None,
        );
        assert_eq!(m, Ok(Mechanism::Scram(Sha::Sha256)));
        assert_eq!(
            choose(&offered(&["PLAIN"]), None, None, false, None).unwrap_err(),
            "Could not find a suitable SASL mechanism. No supported mechanism, or none usable with the available credentials. Server offered: [PLAIN]"
        );
    }

    /// RFC 7677's example exchange for SCRAM-SHA-256.
    #[test]
    fn scram_sha256_proof() {
        let mut e = Exchange::new(Mechanism::Scram(Sha::Sha256), Some("user"), Some("pencil"));
        let s = e.scram.as_mut().unwrap();
        s.nonce = "rOprNGfwEbeRWgbNEkqO".into();
        let first = e
            .initial_response(Some("user"), Some("pencil"))
            .unwrap()
            .unwrap();
        assert_eq!(first, b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let fin = e
            .challenge(b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
            .unwrap()
            .unwrap();
        assert_eq!(
            String::from_utf8(fin).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        assert_eq!(
            e.challenge(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
                .unwrap(),
            None
        );
        assert!(e.verify_completion().is_ok());
    }
}
