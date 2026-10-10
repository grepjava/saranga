//! Certificate subject names as Java prints them:
//! `X500Principal.getName()` (RFC 2253: `CN=client,OU=test,O=acme`), which
//! the certificate login modules match users against, and
//! `X509Certificate.getSubjectDN().getName()` (`CN=client, OU=test,
//! O=acme`), which Artemis puts in messages.
//!
//! Both list the relative names last first. RFC 2253 knows a short list of
//! keywords and writes other attributes as `OID=#hex-of-DER`; the older form
//! knows more keywords (`EMAILADDRESS`, `SERIALNUMBER`, ...).

use x509_parser::prelude::{FromDer, X509Certificate};
use x509_parser::x509::{AttributeTypeAndValue, X509Name};

/// Keywords both forms use.
const RFC2253_KEYWORDS: &[(&str, &str)] = &[
    ("2.5.4.3", "CN"),
    ("2.5.4.6", "C"),
    ("2.5.4.7", "L"),
    ("2.5.4.8", "ST"),
    ("2.5.4.10", "O"),
    ("2.5.4.11", "OU"),
    ("2.5.4.9", "STREET"),
    ("0.9.2342.19200300.100.1.25", "DC"),
    ("0.9.2342.19200300.100.1.1", "UID"),
];

/// Further keywords of `X500Name.toString`.
const LEGACY_KEYWORDS: &[(&str, &str)] = &[
    ("2.5.4.12", "T"),
    ("2.5.4.4", "SURNAME"),
    ("2.5.4.42", "GIVENNAME"),
    ("2.5.4.43", "INITIALS"),
    ("2.5.4.44", "GENERATION"),
    ("2.5.4.46", "DNQ"),
    ("2.5.4.5", "SERIALNUMBER"),
    ("1.2.840.113549.1.9.1", "EMAILADDRESS"),
    ("1.3.6.1.4.1.42.2.11.2.1", "IP"),
];

/// The subject of a DER certificate: (RFC 2253, legacy) forms.
pub fn subject_names(der: &[u8]) -> Option<(String, String)> {
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let subject = cert.subject();
    Some((rfc2253(subject), legacy(subject)))
}

/// The URIs among a DER certificate's subject alternative names.
pub fn san_uris(der: &[u8]) -> Vec<String> {
    use x509_parser::extensions::GeneralName;
    let Ok((_, cert)) = X509Certificate::from_der(der) else {
        return Vec::new();
    };
    let Ok(Some(san)) = cert.subject_alternative_name() else {
        return Vec::new();
    };
    san.value
        .general_names
        .iter()
        .filter_map(|n| match n {
            GeneralName::URI(u) => Some((*u).to_owned()),
            _ => None,
        })
        .collect()
}

fn keyword(oid: &str, table: &[&[(&str, &'static str)]]) -> Option<&'static str> {
    table
        .iter()
        .flat_map(|t| t.iter())
        .find(|(o, _)| *o == oid)
        .map(|(_, k)| *k)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The DER encoding of an attribute value (tag, length, contents).
fn value_der(ava: &AttributeTypeAndValue<'_>) -> Vec<u8> {
    let any = ava.attr_value();
    // Universal, primitive string types: the tag number is the tag byte.
    let mut out = vec![any.tag().0 as u8];
    let len = any.data.len();
    if len < 0x80 {
        out.push(len as u8);
    } else {
        let bytes: Vec<u8> = len
            .to_be_bytes()
            .into_iter()
            .skip_while(|&b| b == 0)
            .collect();
        out.push(0x80 | bytes.len() as u8);
        out.extend(bytes);
    }
    out.extend_from_slice(any.data);
    out
}

/// `X500Principal.getName()`: `AVA.toRFC2253String`.
pub fn rfc2253(name: &X509Name<'_>) -> String {
    let rdns: Vec<String> = name
        .iter()
        .map(|rdn| {
            rdn.iter()
                .map(|ava| {
                    let oid = ava.attr_type().to_id_string();
                    match (keyword(&oid, &[RFC2253_KEYWORDS]), ava.as_str()) {
                        (Some(k), Ok(v)) => format!("{k}={}", escape_rfc2253(v)),
                        (Some(k), Err(_)) => format!("{k}=#{}", hex(&value_der(ava))),
                        (None, _) => format!("{oid}=#{}", hex(&value_der(ava))),
                    }
                })
                .collect::<Vec<_>>()
                .join("+")
        })
        .collect();
    rdns.into_iter().rev().collect::<Vec<_>>().join(",")
}

fn escape_rfc2253(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let chars: Vec<char> = v.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        let edge = (i == 0 && (c == ' ' || c == '#')) || (i == chars.len() - 1 && c == ' ');
        if ",+<>;\"\\".contains(c) || edge {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// `X500Name.toString()`.
pub fn legacy(name: &X509Name<'_>) -> String {
    let rdns: Vec<String> = name
        .iter()
        .map(|rdn| {
            rdn.iter()
                .map(|ava| {
                    let oid = ava.attr_type().to_id_string();
                    let key = keyword(&oid, &[RFC2253_KEYWORDS, LEGACY_KEYWORDS])
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("OID.{oid}"));
                    match ava.as_str() {
                        Ok(v) => format!("{key}={}", quote_legacy(v)),
                        Err(_) => format!("{key}=#{}", hex(&value_der(ava))),
                    }
                })
                .collect::<Vec<_>>()
                .join(" + ")
        })
        .collect();
    rdns.into_iter().rev().collect::<Vec<_>>().join(", ")
}

/// RFC 1779 quoting: a value with special characters is quoted.
fn quote_legacy(v: &str) -> String {
    let special = v.chars().any(|c| ",+=\n<>#;\\\"".contains(c))
        || v.starts_with(' ')
        || v.ends_with(' ')
        || v.is_empty();
    if special && !v.is_empty() {
        format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        v.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes() {
        assert_eq!(escape_rfc2253("a,b"), "a\\,b");
        assert_eq!(escape_rfc2253(" x "), "\\ x\\ ");
        assert_eq!(escape_rfc2253("#1"), "\\#1");
        assert_eq!(quote_legacy("a,b"), "\"a,b\"");
        assert_eq!(quote_legacy("plain"), "plain");
    }
}
