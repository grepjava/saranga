//! A base DN in LDAPLoginModule's `connectionURL` roots the searches, as
//! JNDI's context does: `userBase` (and `roleBase`) are relative to it,
//! and an empty base is the DN itself.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use saranga_security::jaas::{Callbacks, LoginConfig, ModuleFactory, login};

fn read_len(s: &mut impl Read) -> usize {
    let mut b = [0u8; 1];
    s.read_exact(&mut b).unwrap();
    if b[0] & 0x80 == 0 {
        return b[0] as usize;
    }
    let n = (b[0] & 0x7f) as usize;
    let mut v = vec![0u8; n];
    s.read_exact(&mut v).unwrap();
    v.iter().fold(0, |a, x| (a << 8) | *x as usize)
}

fn tlv(tag: u8, c: &[u8]) -> Vec<u8> {
    let mut o = vec![tag];
    if c.len() < 128 {
        o.push(c.len() as u8);
    } else {
        o.push(0x82);
        o.extend((c.len() as u16).to_be_bytes());
    }
    o.extend_from_slice(c);
    o
}

fn result(id: &[u8], op: u8, code: u8) -> Vec<u8> {
    let body = [tlv(0x0a, &[code]), tlv(4, b""), tlv(4, b"")].concat();
    tlv(0x30, &[tlv(2, id), tlv(op, &body)].concat())
}

/// A directory that answers binds and finds nothing, recording the base of
/// each search.
fn directory() -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let bases = Arc::new(Mutex::new(Vec::<String>::new()));
    let b2 = bases.clone();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            let Ok(mut s) = s else { return };
            let b2 = b2.clone();
            std::thread::spawn(move || {
                loop {
                    let mut tag = [0u8; 1];
                    if s.read_exact(&mut tag).is_err() {
                        return;
                    }
                    let len = read_len(&mut s);
                    let mut msg = vec![0u8; len];
                    s.read_exact(&mut msg).unwrap();
                    let idlen = msg[1] as usize;
                    let id = msg[2..2 + idlen].to_vec();
                    let op = &msg[2 + idlen..];
                    match op[0] {
                        0x60 => s.write_all(&result(&id, 0x61, 0)).unwrap(),
                        0x63 => {
                            // SearchRequest: its first member is the base.
                            let mut i = 1;
                            let l = op[i] as usize;
                            i += if l & 0x80 != 0 { 1 + (l & 0x7f) } else { 1 };
                            assert_eq!(op[i], 4);
                            let bl = op[i + 1] as usize;
                            let base = String::from_utf8_lossy(&op[i + 2..i + 2 + bl]).into_owned();
                            b2.lock().unwrap().push(base);
                            s.write_all(&result(&id, 0x65, 0)).unwrap();
                        }
                        _ => return,
                    }
                }
            });
        }
    });
    (port, bases)
}

fn search_base(port: u16, user_base: &str) {
    let text = format!(
        r#"a {{ org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule required
            connectionURL="ldap://127.0.0.1:{port}/dc=example,dc=com"
            connectionUsername="cn=admin" connectionPassword=secret authentication=simple
            userBase="{user_base}" userSearchMatching="(uid={{0}})"; }};"#
    );
    let entries = LoginConfig::parse(&text, |_| None)
        .unwrap()
        .entry("a")
        .unwrap()
        .to_vec();
    let r = login(
        &entries,
        &ModuleFactory::new(None),
        &Callbacks {
            user: Some("jdoe"),
            password: Some("pw"),
            ..Default::default()
        },
    );
    assert!(r.is_err(), "the directory finds no one");
}

#[test]
fn search_bases_are_relative_to_the_url_dn() {
    let (port, bases) = directory();
    search_base(port, "ou=People");
    search_base(port, "");
    assert_eq!(
        *bases.lock().unwrap(),
        ["ou=People,dc=example,dc=com", "dc=example,dc=com"]
    );
}
