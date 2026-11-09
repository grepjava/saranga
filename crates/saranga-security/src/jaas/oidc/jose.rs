//! What `OIDCLoginModule` takes from Nimbus JOSE + JWT: parsing a JWT
//! (`JWTParser`), choosing the provider's keys for its header
//! (`JWSVerificationKeySelector`, `JWKMatcher.forJWSHeader`), checking the
//! signature (`RSASSAVerifier`, `ECDSAVerifier`) and the claims
//! (`DefaultJWTClaimsVerifier`), as `DefaultJWTProcessor` does them.
//!
//! RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384 and ES512 are
//! verified. ES256K is not: Java has had no secp256k1 since JDK 16, so a
//! token signed with it fails in Artemis too. Errors carry Nimbus's
//! messages, which Artemis logs with `debug=true`.

use std::collections::BTreeSet;

use saranga_buffer::json::Json;

/// A JWT's failure: `ParseException` (a token that is not one) or
/// `BadJOSEException` (one that does not pass).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JoseError {
    Parse(String),
    Rejected(String),
}

/// A parsed JWT (`JWTParser.parse`).
#[derive(Debug, Clone)]
pub(crate) struct Jwt {
    pub kind: Kind,
    pub header: Json,
    /// The payload, `None` for an encrypted JWT.
    payload: Option<Vec<u8>>,
    /// `header.payload`, as signed.
    signing_input: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Plain,
    Signed,
    Encrypted,
}

/// Base64url, padded or not (`Base64URL.decode`).
pub(crate) fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    if s.contains(['+', '/']) {
        return None;
    }
    let std: String = s
        .trim_end_matches('=')
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            c => c,
        })
        .collect();
    crate::scram::base64_decode(&std)
}

/// `Base64.getUrlEncoder().withoutPadding()`.
pub(crate) fn base64url_encode(data: &[u8]) -> String {
    crate::scram::base64_encode(data)
        .trim_end_matches('=')
        .replace('+', "-")
        .replace('/', "_")
}

fn json_object(bytes: &[u8], what: &str) -> Result<Json, JoseError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| JoseError::Parse(format!("Invalid {what}: not UTF-8")))?;
    match Json::parse(text) {
        Ok(j @ Json::Object(_)) => Ok(j),
        Ok(_) => Err(JoseError::Parse(format!(
            "Invalid {what}: not a JSON object"
        ))),
        Err(e) => Err(JoseError::Parse(format!("Invalid {what}: {e}"))),
    }
}

impl Jwt {
    /// `JWTParser.parse`: three parts, or five for an encrypted one; the
    /// header names `alg`, `none` for a plain JWT, and `enc` too for an
    /// encrypted one.
    pub(crate) fn parse(token: &str) -> Result<Self, JoseError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 && parts.len() != 5 {
            return Err(JoseError::Parse(
                "Invalid serialized unsecured/JWS/JWE object: Missing part delimiters".into(),
            ));
        }
        let header_bytes = base64url_decode(parts[0]).ok_or_else(|| {
            JoseError::Parse("Invalid unsecured/JWS/JWE header: Invalid JSON".into())
        })?;
        let header = json_object(&header_bytes, "unsecured/JWS/JWE header")?;
        let alg = match header.get("alg") {
            Some(Json::String(a)) => a.clone(),
            Some(_) => {
                return Err(JoseError::Parse(
                    "Unexpected type of JSON object member with key alg".into(),
                ));
            }
            None => {
                return Err(JoseError::Parse(
                    "Missing \"alg\" in header JSON object".into(),
                ));
            }
        };
        let kind = if alg == "none" {
            Kind::Plain
        } else if header.get("enc").is_some() {
            Kind::Encrypted
        } else {
            Kind::Signed
        };
        match (kind, parts.len()) {
            (Kind::Encrypted, 5) => {
                return Ok(Self {
                    kind,
                    header,
                    payload: None,
                    signing_input: Vec::new(),
                    signature: Vec::new(),
                });
            }
            (Kind::Encrypted, _) => {
                return Err(JoseError::Parse(
                    "Unexpected number of Base64URL parts, must be five".into(),
                ));
            }
            (_, 3) => {}
            _ => {
                return Err(JoseError::Parse(
                    "Unexpected number of Base64URL parts, must be three".into(),
                ));
            }
        }
        if kind == Kind::Plain && !parts[2].is_empty() {
            return Err(JoseError::Parse("Unexpected third Base64URL part".into()));
        }
        let payload = base64url_decode(parts[1])
            .ok_or_else(|| JoseError::Parse("Invalid JWT payload".into()))?;
        let signature = base64url_decode(parts[2])
            .ok_or_else(|| JoseError::Parse("Invalid JWS signature".into()))?;
        Ok(Self {
            kind,
            header,
            payload: Some(payload),
            signing_input: format!("{}.{}", parts[0], parts[1]).into_bytes(),
            signature,
        })
    }

    /// `getJWTClaimsSet`: `None` for an encrypted JWT, not yet decrypted.
    pub(crate) fn claims(&self) -> Result<Option<Claims>, JoseError> {
        match &self.payload {
            None => Ok(None),
            Some(p) => {
                let json = json_object(p, "JWT claims set").map_err(|_| {
                    JoseError::Parse("Payload of JWS object is not a valid JSON object".into())
                })?;
                Claims::new(json).map(Some)
            }
        }
    }

    fn header_str(&self, name: &str) -> Option<&str> {
        self.header.get(name).and_then(Json::as_str)
    }
}

/// A JWT claims set, its registered claims of the right types
/// (`JWTClaimsSet.parse`).
#[derive(Debug, Clone)]
pub(crate) struct Claims(pub Json);

impl Claims {
    fn new(json: Json) -> Result<Self, JoseError> {
        let bad = |k: &str| {
            Err(JoseError::Parse(format!(
                "Unexpected type of JSON object member with key {k}"
            )))
        };
        for (k, v) in members(&json) {
            match (k.as_str(), v) {
                ("iss" | "sub" | "jti", Json::String(_) | Json::Null) => {}
                ("iss" | "sub" | "jti", _) => return bad(k),
                ("aud", Json::String(_) | Json::Null) => {}
                ("aud", Json::Array(a)) if a.iter().all(|x| matches!(x, Json::String(_))) => {}
                ("aud", _) => return bad(k),
                ("exp" | "nbf" | "iat", Json::Long(_) | Json::Double(_) | Json::Null) => {}
                ("exp" | "nbf" | "iat", _) => return bad(k),
                _ => {}
            }
        }
        Ok(Self(json))
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Json> {
        self.0.get(name).filter(|v| !v.is_null())
    }

    fn names(&self) -> BTreeSet<&str> {
        members(&self.0).iter().map(|(k, _)| k.as_str()).collect()
    }

    /// `getIssuer`: the `iss` claim, if it is a string.
    fn issuer(&self) -> Option<&str> {
        self.get("iss").and_then(Json::as_str)
    }

    /// `getAudience`.
    fn audience(&self) -> Vec<String> {
        match self.get("aud") {
            Some(Json::String(s)) => vec![s.clone()],
            Some(Json::Array(a)) => a
                .iter()
                .filter_map(Json::as_str)
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        }
    }

    /// A date claim, in milliseconds: the seconds a number gives, whole.
    fn date(&self, name: &str) -> Option<i64> {
        match self.get(name)? {
            Json::Long(s) => Some(s.saturating_mul(1000)),
            Json::Double(s) => Some((*s as i64).saturating_mul(1000)),
            _ => None,
        }
    }
}

fn members(json: &Json) -> &[(String, Json)] {
    match json {
        Json::Object(m) => m,
        _ => &[],
    }
}

/// The verifier's settings (`DefaultJWTClaimsVerifier`).
#[derive(Debug, Clone)]
pub(crate) struct ClaimsRules {
    /// The provider's issuer: a token's `iss` must match it exactly (OIDC
    /// Core 3.1.3.7).
    pub issuer: String,
    pub audience: Option<BTreeSet<String>>,
    pub required: BTreeSet<String>,
    pub max_clock_skew_seconds: i64,
}

impl ClaimsRules {
    /// `DefaultJWTClaimsVerifier.verify`, at `now` (milliseconds).
    pub(crate) fn verify(&self, claims: &Claims, now: i64) -> Result<(), JoseError> {
        let rejected = |m: String| Err(JoseError::Rejected(m));
        let names = claims.names();
        let missing: Vec<&str> = self
            .required
            .iter()
            .map(String::as_str)
            .filter(|r| !names.contains(r))
            .collect();
        if !missing.is_empty() {
            return rejected(format!(
                "JWT missing required claims: [{}]",
                missing.join(", ")
            ));
        }
        match claims.issuer() {
            Some(iss) if iss == self.issuer => {}
            _ => return rejected(format!("JWT issuer rejected: expected [{}]", self.issuer)),
        }
        if let Some(accepted) = &self.audience {
            let aud = claims.audience();
            if aud.is_empty() {
                return rejected("JWT missing required audience".into());
            }
            if !aud.iter().any(|a| accepted.contains(a)) {
                return rejected(format!("JWT audience rejected: [{}]", aud.join(", ")));
            }
        }
        let skew = self.max_clock_skew_seconds.saturating_mul(1000);
        if let Some(exp) = claims.date("exp")
            && exp.saturating_add(skew) <= now
        {
            return rejected("Expired JWT".into());
        }
        if let Some(nbf) = claims.date("nbf")
            && nbf.saturating_sub(skew) >= now
        {
            return rejected("JWT before use time".into());
        }
        Ok(())
    }
}

/// A public key of the provider's JWK set (`RSAKey`, `ECKey`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Jwk {
    kid: Option<String>,
    key_use: Option<String>,
    alg: Option<String>,
    x5t256: Option<String>,
    key: PublicKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PublicKey {
    Rsa { n: Vec<u8>, e: Vec<u8> },
    Ec { crv: String, x: Vec<u8>, y: Vec<u8> },
}

/// `JWKSet.parse`: keys of a type Nimbus does not know are left out, and a
/// key it knows that does not parse fails the set.
pub(crate) fn parse_jwk_set(text: &str) -> Result<Vec<Jwk>, String> {
    let json = Json::parse(text).map_err(|e| format!("Invalid JSON: {e}"))?;
    let Some(Json::Array(keys)) = json.get("keys") else {
        return Err("Missing required \"keys\" member".into());
    };
    let mut out = Vec::new();
    for k in keys {
        let s = |name: &str| k.get(name).and_then(Json::as_str).map(str::to_owned);
        let b = |name: &str| -> Result<Vec<u8>, String> {
            s(name)
                .and_then(|v| base64url_decode(&v))
                .ok_or_else(|| format!("The '{name}' JWK member is missing or invalid"))
        };
        let key = match s("kty").as_deref() {
            Some("RSA") => PublicKey::Rsa {
                n: b("n")?,
                e: b("e")?,
            },
            Some("EC") => PublicKey::Ec {
                crv: s("crv").ok_or("The 'crv' JWK member is missing")?,
                x: b("x")?,
                y: b("y")?,
            },
            Some("oct" | "OKP") => continue,
            Some(other) => {
                let _ = other;
                continue;
            }
            None => return Err("Missing key type \"kty\" parameter".into()),
        };
        out.push(Jwk {
            kid: s("kid"),
            key_use: s("use"),
            alg: s("alg"),
            x5t256: s("x5t#S256"),
            key,
        });
    }
    Ok(out)
}

/// The algorithms verified, by name: the hash's output size and the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Algorithm {
    Rs(usize),
    Ps(usize),
    Es(&'static str),
}

fn algorithm(name: &str) -> Option<Algorithm> {
    Some(match name {
        "RS256" => Algorithm::Rs(256),
        "RS384" => Algorithm::Rs(384),
        "RS512" => Algorithm::Rs(512),
        "PS256" => Algorithm::Ps(256),
        "PS384" => Algorithm::Ps(384),
        "PS512" => Algorithm::Ps(512),
        "ES256" => Algorithm::Es("P-256"),
        "ES384" => Algorithm::Es("P-384"),
        "ES512" => Algorithm::Es("P-521"),
        // In the EC family, but no JDK since 16 verifies it.
        "ES256K" => Algorithm::Es("secp256k1"),
        _ => return None,
    })
}

/// `JWKMatcher.forJWSHeader`: the key type and curve of the algorithm, the
/// header's `kid` and `x5t#S256` where it has them, a signing key or one of
/// no use, for the algorithm or none.
fn matches(
    jwk: &Jwk,
    alg_name: &str,
    alg: Algorithm,
    kid: Option<&str>,
    x5t: Option<&str>,
) -> bool {
    let typed = match (alg, &jwk.key) {
        (Algorithm::Rs(_) | Algorithm::Ps(_), PublicKey::Rsa { .. }) => true,
        (Algorithm::Es(c), PublicKey::Ec { crv, .. }) => crv == c,
        _ => false,
    };
    typed
        && kid.is_none_or(|k| jwk.kid.as_deref() == Some(k))
        && x5t.is_none_or(|t| jwk.x5t256.as_deref() == Some(t))
        && jwk.key_use.as_deref().is_none_or(|u| u == "sig")
        && jwk.alg.as_deref().is_none_or(|a| a == alg_name)
}

fn verify_with(alg: Algorithm, key: &PublicKey, message: &[u8], signature: &[u8]) -> bool {
    use ring::signature as rs;
    match (alg, key) {
        (Algorithm::Rs(bits) | Algorithm::Ps(bits), PublicKey::Rsa { n, e }) => {
            let params: &rs::RsaParameters = match (alg, bits) {
                (Algorithm::Rs(_), 256) => &rs::RSA_PKCS1_2048_8192_SHA256,
                (Algorithm::Rs(_), 384) => &rs::RSA_PKCS1_2048_8192_SHA384,
                (Algorithm::Rs(_), _) => &rs::RSA_PKCS1_2048_8192_SHA512,
                (_, 256) => &rs::RSA_PSS_2048_8192_SHA256,
                (_, 384) => &rs::RSA_PSS_2048_8192_SHA384,
                _ => &rs::RSA_PSS_2048_8192_SHA512,
            };
            rs::RsaPublicKeyComponents { n, e }
                .verify(params, message, signature)
                .is_ok()
        }
        (Algorithm::Es(curve), PublicKey::Ec { x, y, .. }) => {
            let size = match curve {
                "P-256" => 32,
                "P-384" => 48,
                "P-521" => 66,
                _ => return false,
            };
            // ECDSAVerifier: R and S of the curve's size each.
            if signature.len() != 2 * size || x.len() != size || y.len() != size {
                return false;
            }
            let mut point = vec![4u8];
            point.extend_from_slice(x);
            point.extend_from_slice(y);
            match curve {
                "P-256" => rs::UnparsedPublicKey::new(&rs::ECDSA_P256_SHA256_FIXED, &point)
                    .verify(message, signature)
                    .is_ok(),
                "P-384" => rs::UnparsedPublicKey::new(&rs::ECDSA_P384_SHA384_FIXED, &point)
                    .verify(message, signature)
                    .is_ok(),
                _ => {
                    use p521::ecdsa::signature::Verifier;
                    let (Ok(key), Ok(sig)) = (
                        p521::ecdsa::VerifyingKey::from_sec1_bytes(&point),
                        p521::ecdsa::Signature::from_slice(signature),
                    ) else {
                        return false;
                    };
                    key.verify(message, &sig).is_ok()
                }
            }
        }
        _ => false,
    }
}

/// `DefaultJWTProcessor.process`: the header's type, then for a signed JWT
/// a key of the set that verifies it, then the claims. A plain JWT passes
/// only where `allow_plain` (`ConfigurableDefaultJWTProcessor`).
pub(crate) fn process(
    jwt: &Jwt,
    claims: Option<&Claims>,
    keys: &[Jwk],
    rules: &ClaimsRules,
    allow_plain: bool,
    now: i64,
) -> Result<(), JoseError> {
    let rejected = |m: &str| Err(JoseError::Rejected(m.to_owned()));
    match jwt.kind {
        Kind::Encrypted => {
            return rejected("Encrypted JWT rejected: No JWE key selector is configured");
        }
        Kind::Plain if !allow_plain => {
            return rejected("Unsecured (plain) JWTs are rejected, extend class to handle");
        }
        _ => {}
    }
    // DefaultJOSEObjectTypeVerifier.JWS: no type, or JWT.
    if let Some(typ) = jwt.header.get("typ") {
        match typ.as_str() {
            Some(t) if t.eq_ignore_ascii_case("JWT") => {}
            Some(t) => {
                return Err(JoseError::Rejected(format!(
                    "JOSE header typ (type) {t} not allowed"
                )));
            }
            None => {
                return Err(JoseError::Parse(
                    "Unexpected type of JSON object member with key typ".into(),
                ));
            }
        }
    }
    let Some(claims) = claims else {
        return rejected("Payload of JWS object is not a valid JSON object");
    };
    if jwt.kind == Kind::Signed {
        let alg_name = jwt.header_str("alg").unwrap_or_default();
        let candidates: Vec<&Jwk> = match algorithm(alg_name) {
            None => Vec::new(),
            Some(alg) => keys
                .iter()
                .filter(|k| {
                    matches(
                        k,
                        alg_name,
                        alg,
                        jwt.header_str("kid"),
                        jwt.header_str("x5t#S256"),
                    )
                })
                .collect(),
        };
        if candidates.is_empty() {
            return rejected(
                "Signed JWT rejected: Another algorithm expected, or no matching key(s) found",
            );
        }
        // Critical header parameters: none is understood.
        let critical = matches!(jwt.header.get("crit"), Some(Json::Array(a)) if !a.is_empty());
        let alg = algorithm(alg_name).expect("chosen above");
        let verified = !critical
            && candidates
                .iter()
                .any(|k| verify_with(alg, &k.key, &jwt.signing_input, &jwt.signature));
        if !verified {
            return rejected("Signed JWT rejected: Invalid signature");
        }
    }
    rules.verify(claims, now)
}

/// `OIDCSupport.stringArrayForPath`: the string, or array of strings, at a
/// dotted path of the claims. A string is split at whitespace, the items of
/// an array are not.
pub(crate) fn strings_at(claims: &Claims, path: &str) -> Option<Vec<String>> {
    if path.trim().is_empty() {
        return None;
    }
    let segments: Vec<&str> = java_split_dots(path);
    let mut current = &claims.0;
    for (i, segment) in segments.iter().enumerate() {
        let v = current.get(segment)?;
        if i < segments.len() - 1 {
            if !matches!(v, Json::Object(_)) {
                return None;
            }
            current = v;
        } else {
            return match v {
                Json::String(s) => Some(java_split_whitespace(s.trim())),
                Json::Array(items) => items
                    .iter()
                    .map(|x| x.as_str().map(|s| s.trim().to_owned()))
                    .collect(),
                _ => None,
            };
        }
    }
    None
}

/// `String.split("\\.")`: trailing empty parts dropped.
fn java_split_dots(s: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = s.split('.').collect();
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    parts
}

/// `String.split("\\s+")` of a trimmed string: an empty one gives one
/// empty part.
fn java_split_whitespace(s: &str) -> Vec<String> {
    if s.is_empty() {
        return vec![String::new()];
    }
    s.split(|c: char| c.is_whitespace())
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `OIDCSupport.getThumbprint`: the certificate thumbprints a token is bound
/// to, RFC 8705's `cnf.x5t#S256`, and `cnf.x5t#256` as some tokens carry
/// it. Empty if it is bound to none; a member that is not a
/// string counts, so that it never matches.
pub(crate) fn thumbprints(claims: &Claims) -> Vec<Option<&str>> {
    let Some(cnf) = claims.get("cnf") else {
        return Vec::new();
    };
    ["x5t#S256", "x5t#256"]
        .iter()
        .filter_map(|m| cnf.get(m).map(Json::as_str))
        .collect()
}

/// Whether a certificate, DER-encoded, is the one every thumbprint names.
pub(crate) fn bound_to(thumbprints: &[Option<&str>], der: &[u8]) -> bool {
    let digest = base64url_encode(&crate::scram::Scram::Sha256.digest(der));
    !thumbprints.is_empty() && thumbprints.iter().all(|t| *t == Some(digest.as_str()))
}

/// `OIDCSupport.getTokenSummary`, for messages.
pub(crate) fn summary(claims: Option<&Claims>) -> Option<String> {
    let claims = claims?;
    let mut msg = None;
    for c in ["jti", "sid", "iat", "sub"] {
        match claims.get(c) {
            Some(Json::String(s)) => msg = Some(format!("{c}={s}")),
            Some(Json::Long(n)) => msg = Some(format!("{c}={n}")),
            Some(Json::Double(n)) => msg = Some(format!("{c}={}", *n as i64)),
            _ => continue,
        }
        break;
    }
    let mut msg = msg?;
    match claims.get("aud") {
        Some(Json::String(s)) => msg.push_str(&format!(", aud={s}")),
        Some(Json::Array(a)) => {
            let items: Vec<String> = a
                .iter()
                .map(|e| {
                    e.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| "<null>".into())
                })
                .collect();
            msg.push_str(&format!(", aud=[{}]", items.join(",")));
        }
        _ => {}
    }
    Some(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(cnf: &str) -> Claims {
        Claims::new(Json::parse(&format!(r#"{{"sub":"x","cnf":{cnf}}}"#)).unwrap()).unwrap()
    }

    /// A token is bound to a certificate by RFC 8705's `x5t#S256`, or by
    /// `x5t#256`; to it, and only to it.
    #[test]
    fn a_token_is_bound_by_either_thumbprint() {
        let der = b"a certificate";
        let other = b"another certificate";
        let t = base64url_encode(&crate::scram::Scram::Sha256.digest(der));
        for member in ["x5t#S256", "x5t#256"] {
            let c = claims(&format!(r#"{{"{member}":"{t}"}}"#));
            assert!(bound_to(&thumbprints(&c), der), "{member}");
            assert!(
                !bound_to(&thumbprints(&c), other),
                "{member} with another certificate"
            );
        }
        let both = claims(&format!(r#"{{"x5t#S256":"{t}","x5t#256":"{t}"}}"#));
        assert!(bound_to(&thumbprints(&both), der));
        let mixed = claims(&format!(r#"{{"x5t#S256":"{t}","x5t#256":"other"}}"#));
        assert!(
            !bound_to(&thumbprints(&mixed), der),
            "every thumbprint must match"
        );
        assert!(
            !bound_to(&thumbprints(&claims(r#"{"x5t#S256":1}"#)), der),
            "not a string"
        );
        assert!(
            thumbprints(&claims(r#"{"jkt":"k"}"#)).is_empty(),
            "bound by another method"
        );
        assert!(!bound_to(&[], der), "bound to nothing");
    }
}
