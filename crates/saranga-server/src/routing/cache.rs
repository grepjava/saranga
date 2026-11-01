//! `LocalCache`: the target each key was routed to, by node ID, kept
//! while it is used within `timeout` (for ever with 0), and with
//! `persisted` in the bindings journal, from which a start restores it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::store::KeyValuePairs;

pub struct Cache {
    /// `$.BC.` and the router's name: the journal's map of it.
    id: String,
    persisted: bool,
    /// `expireAfterAccess`; `None` for 0, which keeps every entry.
    timeout: Option<Duration>,
    store: Arc<KeyValuePairs>,
    /// Each key's node ID and when it was last read or written.
    entries: Mutex<HashMap<String, (String, Instant)>>,
    /// `persistedCacheEntries`: the node IDs in the journal, by key.
    persisted_entries: Mutex<HashMap<String, String>>,
    running: AtomicBool,
}

impl Cache {
    pub fn new(id: String, persisted: bool, timeout: i32, store: Arc<KeyValuePairs>) -> Self {
        Self {
            id,
            persisted,
            timeout: (timeout != 0).then(|| Duration::from_millis(timeout.max(0) as u64)),
            store,
            entries: Mutex::default(),
            persisted_entries: Mutex::default(),
            running: AtomicBool::new(false),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn persisted(&self) -> bool {
        self.persisted
    }

    pub fn timeout(&self) -> i32 {
        self.timeout.map_or(0, |t| t.as_millis() as i32)
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, (String, Instant)>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn persisted_entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.persisted_entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// With `persisted`, the journal's entries go into the cache, each as
    /// if just written.
    pub fn start(&self) {
        if self.persisted {
            let restored = self.store.get_all(&self.id);
            let now = Instant::now();
            let mut entries = self.entries();
            for (key, node_id) in &restored {
                entries.insert(key.clone(), (node_id.clone(), now));
                tracing::debug!("Restored persisted cache entry during start: {key}={node_id}");
            }
            *self.persisted_entries() = restored;
        }
        self.running.store(true, Ordering::Release);
    }

    /// `cleanUp`, which evicts what expired (and deletes it from the
    /// journal, while running), then the persisted entries are forgotten.
    pub fn stop(&self) {
        self.evict_expired();
        self.persisted_entries().clear();
        self.running.store(false, Ordering::Release);
    }

    /// `getIfPresent`: the node ID of an entry that has not expired, whose
    /// access time it renews.
    pub fn get(&self, key: &str) -> Option<String> {
        self.evict_expired();
        let mut entries = self.entries();
        let (node_id, accessed) = entries.get_mut(key)?;
        *accessed = Instant::now();
        Some(node_id.clone())
    }

    /// With `persisted`, a node ID the journal does not have for the key is
    /// stored first.
    pub fn put(&self, key: &str, node_id: &str) {
        self.evict_expired();
        if self.persisted {
            let mut persisted = self.persisted_entries();
            if persisted.get(key).map(String::as_str) != Some(node_id) {
                if let Err(e) = self.store.store(&self.id, key, node_id) {
                    tracing::warn!(
                        "Failed to persist the cache entry {key} of {}: {e}",
                        self.id
                    );
                }
                persisted.insert(key.to_owned(), node_id.to_owned());
            }
        }
        self.entries()
            .insert(key.to_owned(), (node_id.to_owned(), Instant::now()));
    }

    /// Caffeine's expiry with the removal listener (`onRemoval`): an
    /// expired entry goes, and with `persisted` its journal record, while
    /// the cache runs.
    fn evict_expired(&self) {
        let Some(timeout) = self.timeout else {
            return;
        };
        let expired: Vec<String> = {
            let mut entries = self.entries();
            let now = Instant::now();
            let keys: Vec<String> = entries
                .iter()
                .filter(|(_, (_, at))| now.duration_since(*at) >= timeout)
                .map(|(k, _)| k.clone())
                .collect();
            for k in &keys {
                entries.remove(k);
            }
            keys
        };
        if self.running.load(Ordering::Acquire) && self.persisted {
            for key in expired {
                if self.persisted_entries().remove(&key).is_some()
                    && let Err(e) = self.store.delete(&self.id, &key)
                {
                    tracing::warn!("Failed to delete the cache entry {key} of {}: {e}", self.id);
                }
            }
        }
    }
}
