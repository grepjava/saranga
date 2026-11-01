//! The key-value pairs the bindings journal keeps for connection routers'
//! persisted caches (`KEY_VALUE_PAIR_RECORD`; `storeKeyValuePair`,
//! `deleteKeyValuePair`, `getPersistedKeyValuePairs` and
//! `mapPersistedKeyValuePairs` in `AbstractJournalStorageManager`).
//!
//! Without persistence there is no store, as with Artemis's
//! `NullStorageManager`: nothing is kept.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use saranga_storage::RecordCodec;
use saranga_storage::codec::PersistedKeyValuePair;

use crate::storage::{Storage, ids};

/// Each map's keys, with their record IDs and values.
type Maps = HashMap<String, HashMap<String, (i64, String)>>;

#[derive(Default)]
pub struct KeyValuePairs {
    storage: Mutex<Option<Arc<Storage>>>,
    maps: Mutex<Maps>,
}

impl KeyValuePairs {
    fn maps(&self) -> std::sync::MutexGuard<'_, Maps> {
        self.maps.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A `KEY_VALUE_PAIR_RECORD` found loading the bindings journal
    /// (`insertPersistedKeyValuePair`): a later record for the same key
    /// replaces the earlier one.
    pub(crate) fn load(&self, id: i64, data: &[u8]) -> Result<(), String> {
        let p = PersistedKeyValuePair::decode(data).map_err(|e| e.to_string())?;
        self.maps()
            .entry(p.map_id.to_string())
            .or_default()
            .insert(p.key.to_string(), (id, p.value.to_string()));
        Ok(())
    }

    /// The broker's storage, once the journals are loaded.
    pub(crate) fn attach(&self, storage: Arc<Storage>) {
        *self.storage.lock().unwrap_or_else(|e| e.into_inner()) = Some(storage);
    }

    fn storage(&self) -> Option<Arc<Storage>> {
        self.storage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `getPersistedKeyValuePairs`: a copy of one map.
    pub fn get_all(&self, map_id: &str) -> HashMap<String, String> {
        self.maps()
            .get(map_id)
            .map(|m| m.iter().map(|(k, (_, v))| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// `storeKeyValuePair` (`storeConfiguration`): the pair under a new
    /// record, the one it replaces deleted in the same transaction.
    pub fn store(&self, map_id: &str, key: &str, value: &str) -> Result<(), String> {
        let Some(storage) = self.storage() else {
            return Ok(());
        };
        let mut maps = self.maps();
        let map = maps.entry(map_id.to_owned()).or_default();
        let old = map.get(key).map(|(id, _)| *id);
        let record = PersistedKeyValuePair {
            map_id: map_id.into(),
            key: key.into(),
            value: value.into(),
        };
        let id = storage
            .store_configuration(ids::KEY_VALUE_PAIR_RECORD, record.encode(), old)
            .map_err(|e| e.to_string())?;
        map.insert(key.to_owned(), (id, value.to_owned()));
        Ok(())
    }

    /// `deleteKeyValuePair`: the pair's record deleted, if there is one.
    pub fn delete(&self, map_id: &str, key: &str) -> Result<(), String> {
        let Some(storage) = self.storage() else {
            return Ok(());
        };
        let removed = self.maps().get_mut(map_id).and_then(|m| m.remove(key));
        if let Some((id, _)) = removed {
            storage
                .delete_binding_record(id)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
