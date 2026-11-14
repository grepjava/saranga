//! Duplicate detection: per address, the last `id-cache-size` duplicate IDs
//! seen (`_AMQ_DUPL_ID`). Ported from `PersistentDuplicateIDCache` and
//! `InMemoryDuplicateIDCache`; with `persist-id-cache` each ID is a
//! `DUPLICATE_ID` record in the message journal, deleted when the ID falls
//! out of the cache.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_storage::RecordCodec;
use saranga_storage::codec::DuplicateIdEncoding;

use crate::message::HDR_DUPLICATE_DETECTION_ID;
use crate::storage::{Storage, StoreResult, ids};

/// `Message.getDuplicateIDBytes`: a string ID as its `SimpleString` bytes,
/// a byte array as it is.
pub fn duplicate_id_bytes(message: &CoreMessage) -> Option<Vec<u8>> {
    match message.properties.get_str(HDR_DUPLICATE_DETECTION_ID)? {
        PropertyValue::String(s) => Some(s.as_bytes().to_vec()),
        PropertyValue::Bytes(b) => Some(b.clone()),
        _ => None,
    }
}

/// One address's cache: a ring of `size` slots, each holding an ID and its
/// journal record.
pub struct DuplicateIdCache {
    address: String,
    size: usize,
    /// ID to its slot.
    index: HashMap<Vec<u8>, usize>,
    slots: Vec<Option<(Vec<u8>, i64)>>,
    /// The next slot to fill.
    pos: usize,
    /// IDs taken by sends not yet done, with how many: they count as
    /// present, but take no slot until the send succeeds.
    reserved: HashMap<Vec<u8>, usize>,
    /// `None` for an in-memory cache.
    storage: Option<Arc<Storage>>,
}

impl DuplicateIdCache {
    pub fn new(address: &str, size: usize, storage: Option<Arc<Storage>>) -> Self {
        Self {
            address: address.to_owned(),
            size,
            index: HashMap::new(),
            slots: Vec::new(),
            pos: 0,
            reserved: HashMap::new(),
            storage,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    /// Whether IDs are journaled.
    pub fn is_persistent(&self) -> bool {
        self.storage.is_some()
    }

    /// Whether the ID is held, or taken by a send not yet done. A cache of
    /// size 0 holds nothing (`NoOpDuplicateIDCache`).
    pub fn contains(&self, id: &[u8]) -> bool {
        self.size > 0 && (self.index.contains_key(id) || self.reserved.contains_key(id))
    }

    /// Takes an ID for a send under way: another send of it is a duplicate
    /// from now on, but nothing leaves the cache until [`Self::confirm`].
    pub fn reserve(&mut self, id: &[u8]) {
        if self.size == 0 {
            return;
        }
        *self.reserved.entry(id.to_vec()).or_default() += 1;
    }

    /// Gives back an ID [`Self::reserve`] took, for a send that failed.
    pub fn release(&mut self, id: &[u8]) {
        if let Some(n) = self.reserved.get_mut(id) {
            *n -= 1;
            if *n == 0 {
                self.reserved.remove(id);
            }
        }
    }

    /// The send that reserved the ID succeeded: it takes its slot.
    pub fn confirm(&mut self, id: Vec<u8>, record: i64) {
        self.release(&id);
        self.add_in_memory(id, record);
    }

    /// `load`: the IDs found in the journal, oldest first. Those beyond the
    /// cache size are deleted, in one transaction.
    pub fn load(&mut self, ids: Vec<(Vec<u8>, i64)>, next_id: impl Fn() -> i64) -> StoreResult<()> {
        let mut excess = ids.len().saturating_sub(self.size);
        let mut tx = None;
        for (id, record) in ids {
            if excess > 0 {
                excess -= 1;
                if let Some(s) = &self.storage {
                    let t = *tx.get_or_insert_with(&next_id);
                    s.delete_record_tx(t, record)?;
                }
                continue;
            }
            self.index.insert(id.clone(), self.slots.len());
            self.slots.push(Some((id, record)));
        }
        if let (Some(s), Some(t)) = (&self.storage, tx) {
            s.commit_unsynced(t, false)?;
        }
        self.pos = if self.slots.len() == self.size {
            0
        } else {
            self.slots.len()
        };
        Ok(())
    }

    /// `addToCacheInMemory`: takes the next slot, deleting the record of the
    /// ID it held.
    pub fn add_in_memory(&mut self, id: Vec<u8>, record: i64) {
        if self.size == 0 {
            return;
        }
        if self.pos < self.slots.len() {
            if let Some((old, old_record)) = self.slots[self.pos].take() {
                // The ID may be held again in a later slot: the index then
                // points there, and keeps it.
                if self.index.get(&old) == Some(&self.pos) {
                    self.index.remove(&old);
                }
                if let Some(s) = &self.storage
                    && let Err(e) = s.delete_record(old_record)
                {
                    tracing::warn!("AMQ222057: Error deleting duplicate cache: {e}");
                }
            }
            self.index.insert(id.clone(), self.pos);
            self.slots[self.pos] = Some((id, record));
        } else {
            self.index.insert(id.clone(), self.pos);
            self.slots.push(Some((id, record)));
        }
        self.pos += 1;
        if self.pos == self.size {
            self.pos = 0;
        }
    }

    /// `deleteFromCache`: removes an ID and deletes its record. False when
    /// it was not there.
    pub fn remove(&mut self, id: &[u8]) -> bool {
        let Some(slot) = self.index.remove(id) else {
            return false;
        };
        if let Some((_, record)) = self.slots[slot].take()
            && let Some(s) = &self.storage
            && let Err(e) = s.delete_record(record)
        {
            tracing::warn!("AMQ222057: Error deleting duplicate cache: {e}");
        }
        true
    }

    /// `getMap`: the IDs held, oldest first, with their records.
    pub fn entries(&self) -> Vec<(Vec<u8>, i64)> {
        let n = self.slots.len();
        (0..n)
            .map(|i| (self.pos + i) % n.max(1))
            .filter_map(|i| self.slots[i].clone())
            .collect()
    }

    /// How many IDs it holds (`getMap().size()`).
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// `clear`: every ID's record is deleted, in one transaction.
    pub fn clear(&mut self, next_id: impl Fn() -> i64) {
        let records: Vec<i64> = self.slots.iter().flatten().map(|(_, r)| *r).collect();
        self.slots.clear();
        self.index.clear();
        self.pos = 0;
        let Some(s) = &self.storage else {
            return;
        };
        if records.is_empty() {
            return;
        }
        let tx = next_id();
        let written = records
            .into_iter()
            .try_for_each(|r| s.delete_record_tx(tx, r))
            .and_then(|()| s.commit_unsynced(tx, true));
        if let Err(e) = written {
            tracing::warn!("AMQ222057: Error deleting duplicate cache: {e}");
        }
    }

    /// Journals a new ID inside transaction `tx` (`storeDuplicateIDTransactional`).
    /// A cache of size 0 journals nothing (`NoOpDuplicateIDCache`): it would
    /// never hold the ID, so nothing would delete its record.
    pub fn store_tx(&self, tx: i64, id: &[u8], record: i64) -> StoreResult<()> {
        let Some(s) = &self.storage else {
            return Ok(());
        };
        if self.size == 0 {
            return Ok(());
        }
        let body = DuplicateIdEncoding {
            address: SimpleString::from(self.address.as_str()),
            duplicate_id: id.to_vec(),
        }
        .encode();
        s.add_record_tx(tx, record, ids::DUPLICATE_ID, body)
    }
}

pub type CacheRef = Arc<Mutex<DuplicateIdCache>>;

pub fn lock(c: &CacheRef) -> MutexGuard<'_, DuplicateIdCache> {
    c.lock().unwrap_or_else(|e| e.into_inner())
}

/// An ID a transaction adds to its cache once it commits
/// (`AddDuplicateIDOperation`).
#[derive(Clone)]
pub struct PendingId {
    pub cache: CacheRef,
    pub id: Vec<u8>,
    pub record: i64,
}

impl std::fmt::Debug for PendingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PendingId({})", self.record)
    }
}

impl PendingId {
    pub fn apply(self) {
        lock(&self.cache).add_in_memory(self.id, self.record);
    }

    /// Holds the ID, already checked, as a duplicate: a prepared branch's
    /// after recovery, as it held it once prepared.
    pub fn reserve(&self) -> Reservation {
        lock(&self.cache).reserve(&self.id);
        Reservation {
            cache: self.cache.clone(),
            id: self.id.clone(),
        }
    }

    /// Takes a committing transaction's IDs, each as a duplicate from now
    /// on, unless one is already held (by a send, or by another
    /// transaction that committed or is committing it): that ID is
    /// returned, and none is taken. The guards are dropped once
    /// [`Self::apply`] has given the IDs their slots, or when the commit's
    /// record could not be written.
    pub fn hold(ids: &[PendingId]) -> Result<Vec<Reservation>, Vec<u8>> {
        let mut held = Vec::with_capacity(ids.len());
        for d in ids {
            let mut c = lock(&d.cache);
            if c.contains(&d.id) {
                return Err(d.id.clone());
            }
            c.reserve(&d.id);
            drop(c);
            held.push(Reservation {
                cache: d.cache.clone(),
                id: d.id.clone(),
            });
        }
        Ok(held)
    }
}

/// An ID [`PendingId::hold`] holds, given back when this is dropped.
pub struct Reservation {
    cache: CacheRef,
    id: Vec<u8>,
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Reservation({})", String::from_utf8_lossy(&self.id))
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        lock(&self.cache).release(&self.id);
    }
}

/// The caches of every address (`PostOfficeImpl.duplicateIDCaches`).
pub struct DuplicateIdCaches {
    caches: Mutex<HashMap<String, CacheRef>>,
    default_size: usize,
    storage: Option<Arc<Storage>>,
}

impl DuplicateIdCaches {
    /// `storage` is set when IDs are journaled (persistence and
    /// `persist-id-cache` both on).
    pub fn new(default_size: i32, storage: Option<Arc<Storage>>) -> Self {
        Self {
            caches: Mutex::default(),
            default_size: default_size.max(0) as usize,
            storage,
        }
    }

    /// `duplicateIDCacheExists`.
    pub fn exists(&self, address: &str) -> bool {
        self.caches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(address)
    }

    /// `deleteDuplicateCache`: the cache goes, and its records with it.
    pub fn delete(&self, address: &str, next_id: impl Fn() -> i64) {
        let removed = self
            .caches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(address);
        if let Some(c) = removed {
            lock(&c).clear(next_id);
        }
    }

    /// `getDuplicateIDCache`: created on first use, sized by the address's
    /// `id-cache-size` or the broker's.
    pub fn get(&self, address: &str, size: Option<i32>) -> CacheRef {
        let mut caches = self.caches.lock().unwrap_or_else(|e| e.into_inner());
        caches
            .entry(address.to_owned())
            .or_insert_with(|| {
                let size = size.map_or(self.default_size, |s| s.max(0) as usize);
                Arc::new(Mutex::new(DuplicateIdCache::new(
                    address,
                    size,
                    self.storage.clone(),
                )))
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_evicts_the_oldest() {
        let mut c = DuplicateIdCache::new("a", 2, None);
        c.add_in_memory(b"1".to_vec(), 10);
        c.add_in_memory(b"2".to_vec(), 11);
        assert!(c.contains(b"1") && c.contains(b"2"));
        c.add_in_memory(b"3".to_vec(), 12);
        assert!(!c.contains(b"1"));
        assert!(c.contains(b"2") && c.contains(b"3"));
    }

    /// An ID held twice stays until its newer slot is evicted.
    #[test]
    fn an_id_held_twice_stays_until_its_newer_slot_goes() {
        let mut c = DuplicateIdCache::new("a", 3, None);
        c.add_in_memory(b"same".to_vec(), 1);
        c.add_in_memory(b"same".to_vec(), 2);
        c.add_in_memory(b"other".to_vec(), 3);
        c.add_in_memory(b"new".to_vec(), 4);
        assert!(c.contains(b"same"), "evicting the older slot dropped it");
        c.add_in_memory(b"newer".to_vec(), 5);
        assert!(!c.contains(b"same"));
        // The same ID replacing itself in its own slot is kept.
        let mut c = DuplicateIdCache::new("a", 1, None);
        c.add_in_memory(b"x".to_vec(), 1);
        c.add_in_memory(b"x".to_vec(), 2);
        assert!(c.contains(b"x"));
    }

    /// A reserved ID counts as present but takes no slot: giving it back
    /// leaves the cache as it was, confirming it takes the next slot.
    #[test]
    fn a_reservation_takes_a_slot_only_once_confirmed() {
        let mut c = DuplicateIdCache::new("a", 1, None);
        c.add_in_memory(b"1".to_vec(), 1);
        c.reserve(b"2");
        assert!(c.contains(b"1") && c.contains(b"2"));
        c.release(b"2");
        assert!(c.contains(b"1") && !c.contains(b"2"));
        c.reserve(b"2");
        c.reserve(b"2");
        c.release(b"2");
        assert!(c.contains(b"2"), "one of two reservations is left");
        c.confirm(b"2".to_vec(), 2);
        assert!(!c.contains(b"1") && c.contains(b"2"));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn load_keeps_the_newest() {
        let mut c = DuplicateIdCache::new("a", 2, None);
        c.load(
            vec![(b"1".to_vec(), 1), (b"2".to_vec(), 2), (b"3".to_vec(), 3)],
            || 99,
        )
        .unwrap();
        assert!(!c.contains(b"1"));
        assert!(c.contains(b"2") && c.contains(b"3"));
        c.add_in_memory(b"4".to_vec(), 4);
        assert!(!c.contains(b"2") && c.contains(b"4"));
    }

    #[test]
    fn string_and_bytes_ids() {
        let mut m = CoreMessage::default();
        m.properties.insert(
            HDR_DUPLICATE_DETECTION_ID.into(),
            PropertyValue::String(SimpleString::from("ab")),
        );
        assert_eq!(duplicate_id_bytes(&m), Some(vec![b'a', 0, b'b', 0]));
        m.properties.insert(
            HDR_DUPLICATE_DETECTION_ID.into(),
            PropertyValue::Bytes(vec![1, 2]),
        );
        assert_eq!(duplicate_id_bytes(&m), Some(vec![1, 2]));
    }
}
