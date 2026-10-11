//! `org.apache.activemq.command.ActiveMQDestination`: what a destination's
//! name means.

use crate::commands::Destination;

pub const QUEUE: u8 = 100;
pub const TOPIC: u8 = 101;
pub const TEMP_QUEUE: u8 = 102;
pub const TEMP_TOPIC: u8 = 103;

/// `ActiveMQDestination.QUEUE_TYPE` and the others, for `create`.
pub const QUEUE_TYPE: u8 = 1;
pub const TOPIC_TYPE: u8 = 2;
pub const TEMP_QUEUE_TYPE: u8 = 5;
pub const TEMP_TOPIC_TYPE: u8 = 6;

pub const QUEUE_QUALIFIED_PREFIX: &str = "queue://";
pub const TOPIC_QUALIFIED_PREFIX: &str = "topic://";
pub const TEMP_QUEUE_QUALIFIED_PREFIX: &str = "temp-queue://";
pub const TEMP_TOPIC_QUALIFIED_PREFIX: &str = "temp-topic://";

impl Destination {
    pub fn new(data_type: u8, name: impl Into<String>) -> Self {
        Destination {
            data_type,
            physical_name: Some(name.into()),
        }
    }

    pub fn queue(name: impl Into<String>) -> Self {
        Self::new(QUEUE, name)
    }

    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(TOPIC, name)
    }

    /// `createDestination(name, defaultType)`: a qualified name, or a name
    /// of the default type.
    pub fn create(name: &str, default_type: u8) -> Option<Self> {
        for (prefix, t) in [
            (QUEUE_QUALIFIED_PREFIX, QUEUE),
            (TOPIC_QUALIFIED_PREFIX, TOPIC),
            (TEMP_QUEUE_QUALIFIED_PREFIX, TEMP_QUEUE),
            (TEMP_TOPIC_QUALIFIED_PREFIX, TEMP_TOPIC),
        ] {
            if let Some(rest) = name.strip_prefix(prefix) {
                return Some(Self::new(t, rest));
            }
        }
        let t = match default_type {
            QUEUE_TYPE => QUEUE,
            TOPIC_TYPE => TOPIC,
            TEMP_QUEUE_TYPE => TEMP_QUEUE,
            TEMP_TOPIC_TYPE => TEMP_TOPIC,
            _ => return None,
        };
        Some(Self::new(t, name))
    }

    /// The name as `setPhysicalName` keeps it: trimmed, without options.
    pub fn physical_name(&self) -> String {
        let n = self.physical_name.as_deref().unwrap_or("").trim();
        n.split_once('?').map_or(n, |(name, _)| name).to_owned()
    }

    /// The options after `?`, as `URISupport.parseQuery` reads them.
    pub fn options(&self) -> Vec<(String, String)> {
        let n = self.physical_name.as_deref().unwrap_or("").trim();
        let Some((_, query)) = n.split_once('?') else {
            return Vec::new();
        };
        query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| match p.split_once('=') {
                Some((k, v)) => (url_decode(k), url_decode(v)),
                None => (url_decode(p), String::new()),
            })
            .collect()
    }

    pub fn option(&self, key: &str) -> Option<String> {
        self.options()
            .into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub fn is_queue(&self) -> bool {
        matches!(self.data_type, QUEUE | TEMP_QUEUE)
    }

    pub fn is_topic(&self) -> bool {
        matches!(self.data_type, TOPIC | TEMP_TOPIC)
    }

    pub fn is_temporary(&self) -> bool {
        matches!(self.data_type, TEMP_QUEUE | TEMP_TOPIC)
    }

    /// A comma before any `?` makes a composite destination.
    pub fn is_composite(&self) -> bool {
        let n = self.physical_name.as_deref().unwrap_or("");
        n.split('?').next().unwrap_or("").contains(',')
    }

    /// Whether the name holds a wildcard (`*` or `>`) and is not composite.
    pub fn is_pattern(&self) -> bool {
        !self.is_composite() && {
            let n = self.physical_name();
            n.contains('*') || n.contains('>')
        }
    }

    /// The destinations of a composite, in the order Java's `HashSet`
    /// keeps their names. A name may carry its own `queue://` or
    /// `topic://` prefix; otherwise it has this destination's type.
    pub fn composite_destinations(&self) -> Vec<Destination> {
        let mut names: Vec<String> = Vec::new();
        for n in self.physical_name().split(',') {
            let n = n.trim();
            if !n.is_empty() && !names.iter().any(|x| x == n) {
                names.push(n.to_owned());
            }
        }
        let hashes: Vec<i32> = names
            .iter()
            .map(|n| saranga_storage::java::string_hash(&n.encode_utf16().collect::<Vec<_>>()))
            .collect();
        let default_type = match self.data_type {
            TOPIC => TOPIC_TYPE,
            TEMP_QUEUE => TEMP_QUEUE_TYPE,
            TEMP_TOPIC => TEMP_TOPIC_TYPE,
            _ => QUEUE_TYPE,
        };
        saranga_storage::java::hash_map_order(&hashes, None)
            .into_iter()
            .filter_map(|i| Destination::create(&names[i], default_type))
            .collect()
    }

    pub fn qualified_prefix(&self) -> &'static str {
        match self.data_type {
            TOPIC => TOPIC_QUALIFIED_PREFIX,
            TEMP_QUEUE => TEMP_QUEUE_QUALIFIED_PREFIX,
            TEMP_TOPIC => TEMP_TOPIC_QUALIFIED_PREFIX,
            _ => QUEUE_QUALIFIED_PREFIX,
        }
    }

    /// `getQualifiedName`.
    pub fn qualified_name(&self) -> String {
        if self.is_composite() {
            self.physical_name()
        } else {
            format!("{}{}", self.qualified_prefix(), self.physical_name())
        }
    }
}

fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                Ok(v) => {
                    out.push(v);
                    i += 2;
                }
                Err(_) => out.push(b'%'),
            },
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        let d = Destination::queue(" orders?consumer.prefetchSize=10&selectorAware=true");
        assert_eq!(d.physical_name(), "orders");
        assert_eq!(d.option("selectorAware").as_deref(), Some("true"));
        assert_eq!(d.qualified_name(), "queue://orders");
        let c = Destination::queue("a,topic://b");
        assert!(c.is_composite());
        assert_eq!(c.qualified_name(), "a,topic://b");
        assert_eq!(c.composite_destinations().len(), 2);
        assert_eq!(
            Destination::create("temp-topic://x", QUEUE_TYPE)
                .unwrap()
                .data_type,
            TEMP_TOPIC
        );
    }
}
