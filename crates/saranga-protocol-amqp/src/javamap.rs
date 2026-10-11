//! Maps that iterate the way the Java maps Artemis keeps them in do, so
//! that what the broker re-encodes matches Artemis byte for byte.

use saranga_buffer::SimpleString;
use saranga_storage::java;

/// How a key hashes in Java.
pub trait JavaHash {
    fn java_hash(&self) -> i32;
}

impl JavaHash for SimpleString {
    /// `SimpleString.hashCode`.
    fn java_hash(&self) -> i32 {
        java::simple_string_hash(self)
    }
}

impl JavaHash for crate::Symbol {
    /// `Symbol.hashCode`: its string's.
    fn java_hash(&self) -> i32 {
        let units: Vec<u16> = self.0.encode_utf16().collect();
        java::string_hash(&units)
    }
}

/// The kind of Java map.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    /// A `LinkedHashMap` (what Proton-J decodes maps into): insertion
    /// order.
    Linked,
    /// A `HashMap` made with this initial capacity and load factor (`None`
    /// for the no-argument constructor): bucket order.
    Hash(Option<(usize, f32)>),
}

/// A map keeping its entries in insertion order and iterating as the Java
/// map it stands for.
#[derive(Debug, Clone, PartialEq)]
pub struct JavaMap<K, V> {
    entries: Vec<(K, V)>,
    kind: Kind,
}

impl<K: PartialEq + JavaHash + Clone, V: Clone> JavaMap<K, V> {
    pub fn new(kind: Kind) -> Self {
        Self {
            entries: Vec::new(),
            kind,
        }
    }

    /// A `LinkedHashMap` of these entries, in this order.
    pub fn linked(entries: Vec<(K, V)>) -> Self {
        Self {
            entries,
            kind: Kind::Linked,
        }
    }

    /// `new HashMap<>(numHeaders, 1.0f)` filled in this order, as
    /// `TypedProperties.decode` does.
    pub fn decoded(entries: Vec<(K, V)>) -> Self {
        let n = entries.len();
        Self {
            entries,
            kind: Kind::Hash(Some((n, 1.0))),
        }
    }

    /// `new HashMap<>(other)`: the copy constructor, filled in the other's
    /// iteration order.
    pub fn hash_copy(&self) -> Self {
        let n = self.entries.len();
        // HashMap.putMapEntries: the table size for size / 0.75 + 1.
        let capacity = ((n as f32 / 0.75) + 1.0) as usize;
        Self {
            entries: self.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            kind: Kind::Hash(Some((capacity, 0.75))),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// `put`: a new key goes last, an existing one keeps its place.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(e) => Some(std::mem::replace(&mut e.1, value)),
            None => {
                self.entries.push((key, value));
                None
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let i = self.entries.iter().position(|(k, _)| k == key)?;
        Some(self.entries.remove(i).1)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries in the Java map's iteration order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        let order: Vec<usize> = match self.kind {
            Kind::Linked => (0..self.entries.len()).collect(),
            Kind::Hash(capacity) => {
                let hashes: Vec<i32> = self.entries.iter().map(|(k, _)| k.java_hash()).collect();
                java::hash_map_order(&hashes, capacity)
            }
        };
        order.into_iter().map(|i| {
            let (k, v) = &self.entries[i];
            (k, v)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Symbol;

    #[test]
    fn hash_maps_iterate_in_bucket_order() {
        // The order Artemis writes these annotations into a new HashMap,
        // read from an Artemis-written message.
        let mut m = JavaMap::new(Kind::Hash(None));
        for k in [
            "x-opt-ORIG-QUEUE",
            "x-opt-ORIG-ADDRESS",
            "x-opt-ORIG-MESSAGE-ID",
            "x-opt-ACTUAL-EXPIRY",
        ] {
            m.insert(Symbol::from(k), ());
        }
        let order: Vec<&str> = m.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            order,
            [
                "x-opt-ORIG-MESSAGE-ID",
                "x-opt-ACTUAL-EXPIRY",
                "x-opt-ORIG-QUEUE",
                "x-opt-ORIG-ADDRESS"
            ]
        );
    }
}
