//! The policies `PolicyFactoryResolver` knows: which ready target a key
//! goes to, and the key itself transformed first.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use saranga_buffer::json::Json;

use super::target::{BROKER, Target};

/// `getHash`: 32-bit FNV-1a over the key's UTF-16 code units.
pub fn hash(s: &str) -> i32 {
    s.encode_utf16().fold(0x811c_9dc5_u32 as i32, |h, c| {
        (h ^ i32::from(c)).wrapping_mul(0x0100_0193)
    })
}

enum Kind {
    FirstElement,
    RoundRobin,
    ConsistentHash,
    /// `MODULO`.
    ConsistentHashModulo(i32),
    /// `CONNECTION_COUNT_THRESHOLD`, and each target's last connection
    /// count (`connectionCountCache`).
    LeastConnections {
        threshold: i32,
        counts: Mutex<Vec<(Weak<Target>, i32)>>,
    },
}

pub struct Policy {
    name: String,
    kind: Kind,
    /// `RoundRobinPolicy.pos`, from a random start.
    pos: Mutex<i32>,
}

/// `Integer.parseInt`, with its exception's message.
fn parse_int(v: Option<&String>) -> Result<i32, String> {
    match v {
        None => Err("java.lang.NumberFormatException: Cannot parse null string: null".to_owned()),
        Some(s) => s
            .parse()
            .map_err(|_| format!("java.lang.NumberFormatException: For input string: \"{s}\"")),
    }
}

impl Policy {
    /// `PolicyFactoryResolver.resolve`, `create` and `init`.
    pub fn create(name: &str, properties: &[(String, String)]) -> Result<Self, String> {
        let get = |k: &str| properties.iter().find(|(p, _)| p == k).map(|(_, v)| v);
        let kind = match name {
            "FIRST_ELEMENT" => Kind::FirstElement,
            "ROUND_ROBIN" => Kind::RoundRobin,
            "CONSISTENT_HASH" => Kind::ConsistentHash,
            "CONSISTENT_HASH_MODULO" => Kind::ConsistentHashModulo(parse_int(get("MODULO"))?),
            "LEAST_CONNECTIONS" => Kind::LeastConnections {
                threshold: match get("CONNECTION_COUNT_THRESHOLD") {
                    Some(v) => parse_int(Some(v))?,
                    None => 0,
                },
                counts: Mutex::default(),
            },
            other => {
                return Err(format!(
                    "java.lang.ClassNotFoundException: No PolicyFactory found for the policy {other}"
                ));
            }
        };
        // RandomUtil.randomInterval(0, Integer.MAX_VALUE).
        let random = uuid::Uuid::new_v4().as_u128() as u32 & 0x7fff_ffff;
        Ok(Self {
            name: name.to_owned(),
            kind,
            pos: Mutex::new(random as i32),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether it has a target probe (`getTargetProbe`): least
    /// connections, which counts each target's connections.
    pub fn has_probe(&self) -> bool {
        matches!(self.kind, Kind::LeastConnections { .. })
    }

    /// `UPDATE_CONNECTION_COUNT_PROBE`: the target's `ConnectionCount`,
    /// below the threshold taken as 0.
    pub async fn probe(&self, target: &Arc<Target>) -> bool {
        let Kind::LeastConnections { threshold, counts } = &self.kind else {
            return true;
        };
        match target.attribute(BROKER, "ConnectionCount").await {
            Ok(Json::Long(n)) => {
                let mut count = n as i32;
                if count < *threshold {
                    tracing::debug!(
                        "Updating the connection count to 0/{count} for the target {target}"
                    );
                    count = 0;
                } else {
                    tracing::debug!(
                        "Updating the connection count to {count} for the target {target}"
                    );
                }
                let mut counts = counts.lock().unwrap_or_else(|e| e.into_inner());
                counts.retain(|(t, _)| t.strong_count() > 0);
                match counts
                    .iter_mut()
                    .find(|(t, _)| std::ptr::eq(t.as_ptr(), Arc::as_ptr(target)))
                {
                    Some(slot) => slot.1 = count,
                    None => counts.push((Arc::downgrade(target), count)),
                }
                true
            }
            Ok(other) => {
                tracing::warn!(
                    "Error on updating the connectionCount for the target {target}: {other}"
                );
                false
            }
            Err(e) => {
                tracing::warn!(
                    "Error on updating the connectionCount for the target {target}: {e}"
                );
                false
            }
        }
    }

    /// `transformKey`: the modulo policy's key is the bucket its hash
    /// falls in.
    pub fn transform_key(&self, key: &str) -> String {
        match self.kind {
            Kind::ConsistentHashModulo(modulo) if modulo > 0 => {
                ((hash(key) & i32::MAX) % modulo).to_string()
            }
            _ => key.to_owned(),
        }
    }

    /// `selectTarget` among the ready targets.
    pub fn select_target(&self, targets: &[Arc<Target>], key: &str) -> Option<Arc<Target>> {
        match &self.kind {
            Kind::FirstElement => targets.first().cloned(),
            Kind::RoundRobin => self.round_robin(targets),
            Kind::ConsistentHash => {
                if targets.len() > 1 {
                    let mut ring: BTreeMap<i32, &Arc<Target>> = BTreeMap::new();
                    for t in targets {
                        ring.insert(hash(&t.node_id().unwrap_or_default()), t);
                    }
                    let h = hash(key);
                    ring.range(..=h)
                        .next_back()
                        .or_else(|| ring.iter().next())
                        .map(|(_, t)| (*t).clone())
                } else {
                    targets.first().cloned()
                }
            }
            // The modulo policy picks no target: a key not local is
            // refused.
            Kind::ConsistentHashModulo(_) => None,
            Kind::LeastConnections { counts, .. } => {
                if targets.len() > 1 {
                    let mut sorted: BTreeMap<i32, Vec<Arc<Target>>> = BTreeMap::new();
                    {
                        let counts = counts.lock().unwrap_or_else(|e| e.into_inner());
                        for t in targets {
                            let count = counts
                                .iter()
                                .find(|(c, _)| std::ptr::eq(c.as_ptr(), Arc::as_ptr(t)))
                                .map_or(i32::MAX, |(_, n)| *n);
                            sorted.entry(count).or_default().push(t.clone());
                        }
                    }
                    let least = sorted.into_iter().next().map(|(_, v)| v)?;
                    if least.len() > 1 {
                        self.round_robin(&least)
                    } else {
                        least.into_iter().next()
                    }
                } else {
                    targets.first().cloned()
                }
            }
        }
    }

    fn round_robin(&self, targets: &[Arc<Target>]) -> Option<Arc<Target>> {
        if targets.is_empty() {
            return None;
        }
        let mut pos = self.pos.lock().unwrap_or_else(|e| e.into_inner());
        *pos %= targets.len() as i32;
        let t = targets[*pos as usize].clone();
        *pos += 1;
        Some(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_hash_as_java() {
        // FNV-1a's offset basis, and its published value for "a".
        assert_eq!(hash(""), 0x811c9dc5_u32 as i32);
        assert_eq!(hash("a"), 0xe40c292c_u32 as i32);
    }

    #[test]
    fn modulo_buckets() {
        let p = Policy::create("CONSISTENT_HASH_MODULO", &[("MODULO".into(), "2".into())]).unwrap();
        let k = p.transform_key("a");
        assert_eq!(k, ((hash("a") & i32::MAX) % 2).to_string());
        assert!(Policy::create("CONSISTENT_HASH_MODULO", &[]).is_err());
        assert!(Policy::create("NOPE", &[]).is_err());
    }
}
