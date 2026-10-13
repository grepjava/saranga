//! `ServerStatus`: the broker's status as one JSON object, which the
//! components that report in it (the configuration, the JAAS properties
//! files, the server itself) update by path. As in Artemis it is one per
//! process.

use std::sync::Mutex;

use crate::json::Json;

pub const SERVER_COMPONENT: &str = "server";
pub const CONFIGURATION_COMPONENT: &str = "configuration";
pub const JAAS_COMPONENT: &str = "server/jaas";

static STATUS: Mutex<Json> = Mutex::new(Json::Object(Vec::new()));

fn status() -> std::sync::MutexGuard<'static, Json> {
    STATUS.lock().unwrap_or_else(|e| e.into_inner())
}

/// `ServerStatus.update`: merges `value` in at `path` (`a/b/c`).
pub fn update(path: &str, value: Json) {
    let mut status = status();
    *status = status.merge_and_update(&Json::at_path(path, value));
}

/// `ServerStatus.clear`, as a broker starts.
pub fn clear() {
    *status() = Json::object();
}

/// `ServerStatus.asJson`: the status, with the server's own attributes
/// (`updateServerStatus`) merged in first.
pub fn as_json(server: Json) -> Json {
    update(SERVER_COMPONENT, server);
    status().clone()
}

/// A `java.util.HashMap<String, String>` turned into JSON
/// (`JsonUtil.toJsonObject`): its members in the map's iteration order.
/// `capacity` is the table's as [`crate::java_hash::hash_map_order`]
/// takes it; `None` for `new HashMap<>()`.
pub fn hash_map(members: Vec<(&str, Json)>, capacity: Option<(usize, f32)>) -> Json {
    let hashes: Vec<i32> = members.iter().map(|(k, _)| string_hash(k)).collect();
    let mut members: Vec<Option<(&str, Json)>> = members.into_iter().map(Some).collect();
    let mut out = Json::object();
    for i in crate::java_hash::hash_map_order(&hashes, capacity) {
        if let Some((k, v)) = members[i].take() {
            out.put(k, v);
        }
    }
    out
}

/// `String.hashCode`.
fn string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(i32::from(u)))
}

/// `java.util.zip.Adler32`.
#[derive(Debug, Clone, Copy)]
pub struct Adler32 {
    a: u32,
    b: u32,
}

impl Default for Adler32 {
    fn default() -> Self {
        Self { a: 1, b: 0 }
    }
}

impl Adler32 {
    pub fn update(&mut self, bytes: &[u8]) {
        const MOD: u32 = 65521;
        for chunk in bytes.chunks(5552) {
            for &byte in chunk {
                self.a += u32::from(byte);
                self.b += self.a;
            }
            self.a %= MOD;
            self.b %= MOD;
        }
    }

    pub fn value(&self) -> u32 {
        (self.b << 16) | self.a
    }

    /// The checksum of `key=value` pairs in order, as `Properties.put`
    /// overrides and `parsePrefixedProperties` compute it.
    pub fn of_entries<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> u32 {
        let mut adler = Self::default();
        for (k, v) in entries {
            adler.update(k.as_bytes());
            adler.update(b"=");
            adler.update(v.as_bytes());
        }
        adler.value()
    }

    pub fn of(bytes: &[u8]) -> u32 {
        let mut adler = Self::default();
        adler.update(bytes);
        adler.value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adler32() {
        assert_eq!(Adler32::of(b""), 1);
        assert_eq!(Adler32::of(b"Wikipedia"), 0x11E6_0398);
        assert_eq!(Adler32::of(&vec![0xffu8; 100_000]), {
            let (mut a, mut b) = (1u64, 0u64);
            for _ in 0..100_000 {
                a = (a + 255) % 65521;
                b = (b + a) % 65521;
            }
            ((b << 16) | a) as u32
        });
    }

    #[test]
    fn hash_map_order_of_the_server_attributes() {
        let m = hash_map(
            ["version", "identity", "nodeId", "uptime", "state"]
                .into_iter()
                .map(|k| (k, Json::Null))
                .collect(),
            // putAll of one entry into an empty map, then four puts.
            Some((2, 0.75)),
        );
        let Json::Object(members) = m else { panic!() };
        let keys: Vec<&str> = members.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["state", "version", "nodeId", "identity", "uptime"]);
        let m = hash_map(
            ["Alder32", "Adler32", "reloadTime"]
                .into_iter()
                .map(|k| (k, Json::Null))
                .collect(),
            None,
        );
        let Json::Object(members) = m else { panic!() };
        let keys: Vec<&str> = members.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["reloadTime", "Alder32", "Adler32"]);
    }

    #[test]
    fn merge() {
        let a = Json::at_path("server/jaas/properties/u", Json::from(1));
        let b = Json::at_path("server/jaas/properties/r", Json::from(2));
        let c = Json::at_path("server", Json::object().with("state", "STARTED"));
        let m = a.merge_and_update(&b).merge_and_update(&c);
        assert_eq!(
            m.to_string(),
            r#"{"server":{"jaas":{"properties":{"u":1,"r":2}},"state":"STARTED"}}"#
        );
    }
}
