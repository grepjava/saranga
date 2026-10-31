//! `saranga mask --scram <user> <password>`: what
//! `java -cp "lib/*" org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule
//! <user> <password> [<iterations>]` prints (`SCRAMPropertiesLoginModule.main`),
//! the user's lines for a users file served by `SCRAMPropertiesLoginModule`.

use saranga_security::scram::{self, Scram};

/// `MIN_ITERATIONS`.
const MIN_ITERATIONS: i32 = 4096;

/// The lines, in `Properties.store`'s form: a comment, the date, and one
/// `user|SHA512` and one `user|SHA256` entry of
/// `ENC(salt:iterations:serverKey:storedKey)`.
pub fn lines(
    user: &str,
    password: &str,
    iterations: Option<&str>,
    date: &str,
) -> anyhow::Result<Vec<u8>> {
    let iterations = match iterations {
        None => MIN_ITERATIONS,
        Some(i) => {
            let i: i32 = i
                .parse()
                .map_err(|_| anyhow::anyhow!("For input string: \"{i}\""))?;
            if i < MIN_ITERATIONS {
                anyhow::bail!("minimum of {MIN_ITERATIONS} required!");
            }
            i
        }
    };
    let encoded_user = scram::prep_as_query_string(user)?;
    let mut entries = Vec::new();
    for s in Scram::ALL {
        let salt = scram::random_bytes(32);
        let d = scram::new_password(s, password, &salt, iterations)?.user_data();
        let value = saranga_config::mask::wrap(&format!(
            "{}:{}:{}:{}",
            d.salt, d.iterations, d.server_key, d.stored_key
        ));
        entries.push((format!("{encoded_user}|{}", s.constant()), value));
    }
    let mut out = Vec::new();
    comment(
        &mut out,
        &format!(
            "Insert the lines stating with '{encoded_user}' into the desired user properties file"
        ),
    );
    out.extend_from_slice(format!("#{date}\n").as_bytes());
    for (key, value) in hash_table_order(entries) {
        out.extend(save_convert(&key, true));
        out.push(b'=');
        out.extend(save_convert(&value, false));
        out.push(b'\n');
    }
    Ok(out)
}

/// `Properties` iterates its `ConcurrentHashMap` of 16 bins: by
/// `spread(hashCode) & 15`, ties in the order put.
fn hash_table_order(mut entries: Vec<(String, String)>) -> Vec<(String, String)> {
    let bin = |k: &str| {
        let h = k
            .encode_utf16()
            .fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(i32::from(u)));
        ((h ^ ((h as u32) >> 16) as i32) & 15) as u32
    };
    entries.sort_by_key(|(k, _)| bin(k));
    entries
}

/// `Properties.writeComments`, to an ISO 8859-1 stream: a character past
/// Latin-1 as `\uXXXX`, each line a comment.
fn comment(out: &mut Vec<u8>, text: &str) {
    out.push(b'#');
    for c in text.chars() {
        match c {
            '\n' | '\r' => out.extend_from_slice(b"\n#"),
            c if (c as u32) > 0xff => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    out.extend_from_slice(format!("\\u{u:04X}").as_bytes());
                }
            }
            c => out.push(c as u32 as u8),
        }
    }
    out.push(b'\n');
}

/// `Properties.saveConvert`, escaping Unicode: a key's spaces escaped, a
/// value's leading one.
fn save_convert(s: &str, key: bool) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, c) in s.chars().enumerate() {
        match c {
            ' ' => {
                if i == 0 || key {
                    out.push(b'\\');
                }
                out.push(b' ');
            }
            '\t' => out.extend_from_slice(b"\\t"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            '=' | ':' | '#' | '!' | '\\' => {
                out.push(b'\\');
                out.push(c as u8);
            }
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    out.extend_from_slice(format!("\\u{u:04X}").as_bytes());
                }
            }
            c => out.push(c as u8),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_read_back_as_the_user_data() {
        let out = lines("al ice", "pw", None, "Sat Oct 31 12:00:00 IST 2026").unwrap();
        let text = String::from_utf8(out.clone()).unwrap();
        assert!(text.starts_with(
            "#Insert the lines stating with 'al ice' into the desired user properties file\n#Sat Oct 31 12:00:00 IST 2026\n"
        ));
        let entries = saranga_config::properties::parse(&out);
        assert_eq!(entries.len(), 2);
        for (key, value) in entries {
            let (user, mechanism) = key.split_once('|').unwrap();
            assert_eq!(user, "al ice");
            let s = Scram::ALL
                .into_iter()
                .find(|s| s.constant() == mechanism)
                .unwrap();
            let bare = saranga_config::mask::unwrap(&value);
            let p: Vec<&str> = bare.split(':').collect();
            assert_eq!(p[1], "4096");
            let salt = scram::base64_decode(p[0]).unwrap();
            let d = scram::new_password(s, "pw", &salt, 4096)
                .unwrap()
                .user_data();
            assert_eq!((p[2], p[3]), (d.server_key.as_str(), d.stored_key.as_str()));
        }
        assert!(lines("u", "pw", Some("100"), "d").is_err());
        assert!(lines("u", "pw", Some("x"), "d").is_err());
    }

    #[test]
    fn entries_iterate_as_a_properties_object_does() {
        // As Java 21 prints them.
        let order = hash_table_order(vec![
            ("user|SHA512".into(), String::new()),
            ("user|SHA256".into(), String::new()),
        ]);
        assert_eq!(order[0].0, "user|SHA256");
    }
}
