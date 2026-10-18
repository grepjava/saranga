//! Paging: addresses whose messages outgrow memory keep them in page
//! files, and their queues read them back as they consume. Ported from
//! `PagingManagerImpl`, `PagingStoreImpl`, `PageCursorProviderImpl`,
//! `PageSubscriptionImpl`, `PageTransactionInfoImpl`,
//! `PageSubscriptionCounterImpl` and `PageCounterRebuildManager`.
//!
//! Each address has a [`PagingStore`] that accounts the memory its messages
//! use. Under the PAGE policy, once the address (or the broker, against
//! `global-max-size`) is full, the store starts paging: messages routed to
//! the address are written to its current page file instead of becoming
//! queue references. BLOCK holds producer credits back, FAIL fails sends,
//! DROP drops messages.
//!
//! Every queue has a [`PageSubscription`] on its address's store: its
//! cursor through the pages. The queue reads paged messages into memory
//! as it has room ([`PagingStore::next`]); acknowledging one writes an
//! `ACKNOWLEDGE_CURSOR` record instead of `ACKNOWLEDGE_REF`, as the message
//! never was in the journal. A page every subscription has finished is
//! deleted, its acknowledgements first consolidated into a
//! `PAGE_CURSOR_COMPLETE` record per queue. When no page is left the
//! address stops paging.
//!
//! A transaction that pages messages has a [`PageTx`]: its messages are in
//! the page as they are sent, and are delivered only once its
//! `PAGE_TRANSACTION` record has committed.
//!
//! Message counts of paged messages are kept in memory and rebuilt from the
//! pages and the cursor records on every start, as Artemis does by default.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak, mpsc};

use saranga_core_protocol::packets::replication::ReplicationPageEventMessage;
use saranga_journal::Completion;
use saranga_paging::{PagedBody, PagedMessage};
use saranga_storage::message::PersistedMessage;

use crate::large::LargeMessageStore;
use crate::message::{MessageDecoders, PagePosition, ServerMessage};
use crate::settings::{AddressFullPolicy, AddressSettings, DiskFullPolicy, PageFullPolicy};
use crate::storage::{Pending, RecoveredCursorRecord, RecoveredPageTx, Storage, StoreResult};
use crate::{BrokerError, Name};

/// `MessageReferenceImpl.memoryOffset`: what a queue reference adds to its
/// address's size.
pub const REF_MEMORY_ESTIMATE: i64 = 72;

/// A page file to sync, and who waits for it.
type SyncRequest = (Arc<File>, PathBuf, Completion);

/// `PageCache` size: decoded pages kept for the cursors.
const PAGE_CACHE: usize = 5;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `Message.getMemoryEstimate`: what a message in memory adds to its
/// address's size.
pub fn memory_estimate(m: &ServerMessage) -> i64 {
    m.memory_estimate()
}

/// `getPersistentSize`: what a paged message counts as in its queues'
/// page counters.
pub fn persistent_size(m: &ServerMessage) -> i64 {
    m.encode_size() as i64 + m.large().map_or(0, |b| b.size() as i64)
}

// ------------------------------------------------------------------ manager

impl std::fmt::Debug for PagingManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PagingManager({:?})", self.dir)
    }
}

/// `PagingManagerImpl`: the paging stores, the broker-wide memory limit and
/// the page transactions.
pub struct PagingManager {
    /// `paging-directory`; `None` without persistence, when nothing pages.
    dir: Option<PathBuf>,
    storage: Option<Arc<Storage>>,
    large: Arc<LargeMessageStore>,
    /// Reads the messages of every protocol's persisters from pages.
    decoders: OnceLock<MessageDecoders>,
    /// `global-max-size`; -1 for none.
    global_max_size: i64,
    /// `global-max-messages`; -1 for none.
    global_max_messages: i64,
    global_size: AtomicI64,
    global_messages: AtomicI64,
    stores: Mutex<HashMap<Name, Arc<PagingStore>>>,
    /// Page transactions by transaction ID (`transactions`).
    transactions: Mutex<HashMap<i64, Arc<PageTx>>>,
    /// Stores holding producers back until global memory is freed.
    blocked: Mutex<Vec<Weak<PagingStore>>>,
    syncer: Mutex<Option<mpsc::Sender<SyncRequest>>>,
    /// Queues read their pages only once the cursors are recovered.
    loaded: AtomicBool,
    /// The disk is past `max-disk-usage` or `min-disk-free` (`diskFull`).
    disk_full: AtomicBool,
    /// The usable and total bytes the disk monitor last read
    /// (`diskUsableSpace`, `diskTotalSpace`).
    disk_usable: AtomicU64,
    disk_total: AtomicU64,
    /// The backup page files are replicated to.
    replication: crate::replication::Slot,
    /// No page is deleted while a backup copies them (`disableCleanup`).
    cleanup_paused: AtomicBool,
    me: Weak<PagingManager>,
}

impl PagingManager {
    pub fn new(
        dir: Option<PathBuf>,
        storage: Option<Arc<Storage>>,
        large: Arc<LargeMessageStore>,
        global_max_size: i64,
        global_max_messages: i64,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            dir: dir.filter(|_| storage.is_some()),
            storage,
            large,
            decoders: OnceLock::new(),
            global_max_size,
            global_max_messages,
            global_size: AtomicI64::new(0),
            global_messages: AtomicI64::new(0),
            disk_full: AtomicBool::new(false),
            disk_usable: AtomicU64::new(0),
            disk_total: AtomicU64::new(0),
            stores: Mutex::default(),
            transactions: Mutex::default(),
            blocked: Mutex::default(),
            syncer: Mutex::new(None),
            loaded: AtomicBool::new(false),
            replication: crate::replication::Slot::default(),
            cleanup_paused: AtomicBool::new(false),
            me: me.clone(),
        })
    }

    /// Sets how messages of other protocols are read from pages, before
    /// any page is read.
    /// The decoders of every protocol's persisters.
    pub fn decoders(&self) -> MessageDecoders {
        self.decoders.get().cloned().unwrap_or_default()
    }

    pub fn set_decoders(&self, decoders: MessageDecoders) {
        let _ = self.decoders.set(decoders);
    }

    /// Lets the queues read their pages: recovery is over (or there was
    /// none).
    pub fn set_loaded(&self) {
        self.loaded.store(true, Ordering::SeqCst);
        for store in self.stores() {
            let subs: Vec<Arc<PageSubscription>> =
                lock(&store.state).subscriptions.values().cloned().collect();
            for sub in subs {
                sub.wake();
            }
        }
    }

    pub fn storage(&self) -> Option<&Arc<Storage>> {
        self.storage.as_ref()
    }

    /// `reloadStores`: the stores of the addresses that have a directory
    /// under the paging directory. Each finds its pages and starts paging
    /// again if it was.
    pub fn reload_stores(&self, settings: impl Fn(&str) -> Arc<AddressSettings>) -> io::Result<()> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        fs::create_dir_all(dir)?;
        for entry in fs::read_dir(dir)? {
            let folder = entry?.path();
            if !folder.is_dir() {
                continue;
            }
            let address_file = folder.join(saranga_paging::ADDRESS_FILE);
            let Ok(text) = fs::read_to_string(&address_file) else {
                tracing::warn!(
                    "AMQ222068: Directory {} did not have an identification file {}",
                    folder.display(),
                    saranga_paging::ADDRESS_FILE
                );
                continue;
            };
            let Some(address) = text.lines().next().filter(|a| !a.is_empty()) else {
                tracing::warn!(
                    "AMQ222184: File {} is empty on {}",
                    saranga_paging::ADDRESS_FILE,
                    folder.display()
                );
                continue;
            };
            let address: Name = address.into();
            let store = PagingStore::new(&address, self.me.clone(), settings(&address));
            store.start(folder)?;
            lock(&self.stores).insert(address, store);
        }
        Ok(())
    }

    /// `getPageStore`: the address's store, created on first use.
    pub fn store(
        &self,
        address: &str,
        settings: impl FnOnce() -> Arc<AddressSettings>,
    ) -> Arc<PagingStore> {
        let mut stores = lock(&self.stores);
        if let Some(s) = stores.get(address) {
            return s.clone();
        }
        let address: Name = address.into();
        let store = PagingStore::new(&address, self.me.clone(), settings());
        stores.insert(address, store.clone());
        store
    }

    pub fn existing_store(&self, address: &str) -> Option<Arc<PagingStore>> {
        lock(&self.stores).get(address).cloned()
    }

    pub fn stores(&self) -> Vec<Arc<PagingStore>> {
        lock(&self.stores).values().cloned().collect()
    }

    /// The subscription of a queue, whatever its address.
    pub fn subscription(&self, queue_id: i64) -> Option<Arc<PageSubscription>> {
        self.stores()
            .into_iter()
            .find_map(|s| lock(&s.state).subscriptions.get(&queue_id).cloned())
    }

    /// `isGlobalFull`.
    /// `global-max-size`; -1 for none.
    pub fn global_max_size(&self) -> i64 {
        self.global_max_size
    }

    pub fn is_global_full(&self) -> bool {
        self.is_disk_full()
            || (self.global_max_size >= 0
                && self.global_size.load(Ordering::SeqCst) >= self.global_max_size)
            || (self.global_max_messages >= 0
                && self.global_messages.load(Ordering::SeqCst) >= self.global_max_messages)
    }

    fn add_global(&self, size: i64, messages: i64) {
        self.global_size.fetch_add(size, Ordering::SeqCst);
        self.global_messages.fetch_add(messages, Ordering::SeqCst);
        if size < 0 || messages < 0 {
            self.check_memory_release();
        }
    }

    /// `checkMemoryRelease`: below the limits, the blocked stores let their
    /// producers go.
    fn check_memory_release(&self) {
        if !self.is_global_full() {
            let blocked = std::mem::take(&mut *lock(&self.blocked));
            for s in blocked.iter().filter_map(Weak::upgrade) {
                s.check_released_memory();
            }
        }
    }

    /// `isDiskFull`.
    pub fn is_disk_full(&self) -> bool {
        self.disk_full.load(Ordering::SeqCst)
    }

    /// `getDiskUsableSpace` and `getDiskTotalSpace`: what the disk monitor
    /// last read, or zeros before it first looks.
    pub fn disk_space(&self) -> (u64, u64) {
        (
            self.disk_usable.load(Ordering::SeqCst),
            self.disk_total.load(Ordering::SeqCst),
        )
    }

    /// `LocalMonitor.tick`: the disk monitor's verdict on the last store it
    /// read.
    pub fn disk_tick(
        &self,
        usable: u64,
        total: u64,
        within_limit: bool,
        limit: crate::disk::Limit,
    ) {
        self.disk_usable.store(usable, Ordering::SeqCst);
        self.disk_total.store(total, Ordering::SeqCst);
        let space = || {
            (
                crate::disk::human_bytes(usable),
                crate::disk::human_bytes(total),
                format!("{:.1}%", crate::disk::usage_of(usable, total) * 100.0),
            )
        };
        if within_limit {
            if self.disk_full.swap(false, Ordering::SeqCst) {
                let (usable, total, usage) = space();
                match limit {
                    crate::disk::Limit::MaxUsage(_) => tracing::info!(
                        "AMQ222211: Free storage space is at {usable} of {total} total. Usage rate is {usage} which is below the configured <max-disk-usage>. System will unblock producers."
                    ),
                    crate::disk::Limit::MinFree(_) => tracing::warn!(
                        "AMQ224129: Free storage space is at {usable} which is above the configured <min-disk-free>. System will unblock producers."
                    ),
                }
            }
            self.check_memory_release();
        } else if !self.disk_full.swap(true, Ordering::SeqCst) {
            let (usable, total, usage) = space();
            match limit {
                crate::disk::Limit::MaxUsage(_) => tracing::warn!(
                    "AMQ222210: Free storage space is at {usable} of {total} total. Usage rate is {usage} which is beyond the configured <max-disk-usage>. System will start blocking producers."
                ),
                crate::disk::Limit::MinFree(_) => tracing::warn!(
                    "AMQ224128: Free storage space is at {usable} which is below the configured <min-disk-free>. System will start blocking producers."
                ),
            }
        }
    }

    pub fn global_size(&self) -> i64 {
        self.global_size.load(Ordering::SeqCst)
    }

    pub fn transaction(&self, tx_id: i64) -> Option<Arc<PageTx>> {
        lock(&self.transactions).get(&tx_id).cloned()
    }

    /// The page transaction of `tx_id`, created on first use
    /// (`installPageTransaction`).
    fn install_transaction(&self, tx_id: i64) -> (Arc<PageTx>, bool) {
        let mut txs = lock(&self.transactions);
        match txs.get(&tx_id) {
            Some(t) => (t.clone(), false),
            None => {
                let t = Arc::new(PageTx::new(tx_id));
                txs.insert(tx_id, t.clone());
                (t, true)
            }
        }
    }

    fn remove_transaction(&self, tx_id: i64) {
        lock(&self.transactions).remove(&tx_id);
    }

    /// Starts replicating page files to a backup: every store's page files
    /// so far are returned, each store writing to a new page from then on,
    /// and what is written to pages afterwards is replicated. No page is
    /// deleted until [`PagingManager::sync_finished`].
    pub(crate) fn start_replication(
        &self,
        replicator: Arc<crate::replication::Replicator>,
    ) -> Vec<(Name, Vec<(i64, PathBuf)>)> {
        self.cleanup_paused.store(true, Ordering::SeqCst);
        self.replication.set(Some(replicator));
        let mut out = Vec::new();
        for store in self.stores() {
            let mut st = lock(&store.state);
            if st.replicated {
                continue;
            }
            st.replicated = true;
            let Some(folder) = st.folder.clone() else {
                continue;
            };
            // getCurrentIds, then forceAnotherPage.
            let mut pages: Vec<(i64, PathBuf)> = fs::read_dir(&folder)
                .map(|d| {
                    d.flatten()
                        .filter_map(|e| {
                            let name = e.file_name();
                            let id = saranga_paging::page_id_of(name.to_str()?)?;
                            Some((id, e.path()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            pages.sort();
            if pages.is_empty() {
                continue;
            }
            if st.current.is_some()
                && let Err(e) = store.open_new_page(&mut st)
            {
                tracing::warn!("opening a new page on {}: {e}", store.address);
            }
            out.push((store.address.clone(), pages));
        }
        out
    }

    /// `resumeCleanup`: the backup has its copies.
    pub(crate) fn sync_finished(&self) {
        self.cleanup_paused.store(false, Ordering::SeqCst);
        for store in self.stores() {
            store.cleanup();
        }
    }

    /// Page files are no longer replicated.
    pub(crate) fn stop_replication(&self) {
        self.replication.set(None);
        for store in self.stores() {
            lock(&store.state).replicated = false;
        }
        if self.cleanup_paused.swap(false, Ordering::SeqCst) {
            for store in self.stores() {
                store.cleanup();
            }
        }
    }

    fn replicator(&self) -> Option<Arc<crate::replication::Replicator>> {
        self.replication.get()
    }

    /// Waits for the backup to have everything replicated so far.
    fn replicated(&self, r: &crate::replication::Replicator) -> Pending {
        let (c, p) = Pending::new();
        r.barrier(c);
        p
    }

    /// Syncs a page file, batched with the other requests waiting
    /// (`PageTimedWriter`), and waits for the backup, when there is one.
    fn sync_file(&self, file: Arc<File>, path: PathBuf) -> Pending {
        let (c, p) = Pending::new();
        let c = match self.replicator() {
            Some(r) => r.join_barrier(c),
            None => c,
        };
        let mut syncer = lock(&self.syncer);
        let tx = syncer.get_or_insert_with(|| {
            let (tx, rx) = mpsc::channel::<SyncRequest>();
            let storage = self.storage.as_ref().map(Arc::downgrade);
            std::thread::Builder::new()
                .name("saranga-page-sync".into())
                .spawn(move || {
                    while let Ok(first) = rx.recv() {
                        let mut batch = vec![first];
                        batch.extend(rx.try_iter());
                        let mut synced: Vec<(*const File, Result<(), String>)> = Vec::new();
                        for (file, path, done) in batch {
                            let key = Arc::as_ptr(&file);
                            let result = match synced.iter().find(|(k, _)| *k == key) {
                                Some((_, r)) => r.clone(),
                                None => {
                                    let r = sync_page(&file, &path);
                                    if let Err(e) = &r
                                        && let Some(s) = storage.as_ref().and_then(|s| s.upgrade())
                                    {
                                        s.critical_io(e, Some(&path));
                                    }
                                    let r = r.map_err(|e| e.to_string());
                                    synced.push((key, r.clone()));
                                    r
                                }
                            };
                            done(result.map_err(|e| {
                                saranga_journal::JournalError::Io(Arc::new(io::Error::other(e)))
                            }));
                        }
                    }
                })
                .expect("spawning the page sync thread");
            tx
        });
        if tx.send((file, path, c)).is_err() {
            *syncer = None;
        }
        p
    }

    /// `onIOError`: a page file could not be written.
    fn critical_io(&self, e: &io::Error, path: &Path) {
        if let Some(s) = &self.storage {
            s.critical_io(e, Some(path));
        }
    }

    /// Loads the paging records of the message journal into the stores'
    /// subscriptions and the page transactions (`loadMessageJournal`).
    pub fn load_records(
        &self,
        cursor_acks: Vec<RecoveredCursorRecord>,
        page_completes: Vec<RecoveredCursorRecord>,
        page_txs: Vec<RecoveredPageTx>,
    ) {
        let storage = self.storage.clone();
        let delete = |id: i64| {
            if let Some(s) = &storage {
                let _ = s.delete_record(id);
            }
        };
        for p in page_txs {
            let tx = Arc::new(PageTx::new(p.tx_id));
            {
                let mut st = lock(&tx.state);
                st.record = p.record;
                st.committed = true;
                st.messages = p.messages;
                st.persistent = p.messages;
            }
            lock(&self.transactions).insert(p.tx_id, tx);
        }
        for a in cursor_acks {
            let Some((store, sub)) = self.locate(a.queue_id) else {
                tracing::warn!(
                    "AMQ222010: Cannot find queue {} while reloading ACKNOWLEDGE_CURSOR, deleting record now",
                    a.queue_id
                );
                delete(a.record);
                continue;
            };
            let (first, current) = store.page_range();
            if a.page >= first && a.page <= current {
                lock(&sub.state)
                    .recovered_acks
                    .push((a.page, a.message_nr, a.record));
            } else {
                tracing::warn!(
                    "AMQ222295: Cannot find page {} on {} while reloading ack, deleting record {}",
                    a.page,
                    store.address,
                    a.record
                );
                delete(a.record);
            }
        }
        for c in page_completes {
            let Some((store, sub)) = self.locate(c.queue_id) else {
                tracing::warn!(
                    "AMQ222277: Cannot find queue {} on page complete, deleting record now",
                    c.queue_id
                );
                delete(c.record);
                continue;
            };
            if !store.reload_page_completion(&sub, c.page, c.message_nr, c.record) {
                delete(c.record);
            }
        }
    }

    fn locate(&self, queue_id: i64) -> Option<(Arc<PagingStore>, Arc<PageSubscription>)> {
        self.stores().into_iter().find_map(|s| {
            let sub = lock(&s.state).subscriptions.get(&queue_id).cloned();
            sub.map(|sub| (s, sub))
        })
    }

    /// A page transaction recovered inside a prepared transaction: its
    /// messages wait for that transaction (`reloadPrepared`).
    pub fn reload_prepared(&self, p: &RecoveredPageTx) -> Arc<PageTx> {
        let tx = Arc::new(PageTx::new(p.tx_id));
        {
            let mut st = lock(&tx.state);
            st.record = p.record;
            st.messages = p.messages;
            st.persistent = p.messages;
        }
        lock(&self.transactions).insert(p.tx_id, tx.clone());
        tx
    }

    /// After the journal is loaded: the cursors take their recovered
    /// acknowledgements, the page counters are rebuilt from the pages
    /// (`PageCounterRebuildManager`), and finished pages are cleaned up.
    /// The IDs of large messages found in pages leave `stored_large`.
    pub fn after_load(&self, stored_large: &mut HashSet<i64>) {
        for store in self.stores() {
            store.process_reload();
            store.rebuild_counters(stored_large);
            store.cleanup();
        }
        self.set_loaded();
    }

    /// Stops the page sync thread.
    pub fn stop(&self) {
        lock(&self.syncer).take();
    }
}

// -------------------------------------------------------------------- store

/// A message in a page.
#[derive(Debug)]
pub struct PageEntry {
    /// Its position in the page.
    pub nr: i32,
    pub tx_id: i64,
    pub queue_ids: Vec<i64>,
    /// `None` for a message of a protocol Saranga cannot deliver yet.
    pub message: Option<Arc<ServerMessage>>,
}

struct LoadedPage {
    id: i64,
    entries: Vec<Arc<PageEntry>>,
}

/// The page being written (`currentPage`).
struct CurrentPage {
    id: i64,
    path: PathBuf,
    file: Arc<File>,
    size: u64,
    entries: Vec<Arc<PageEntry>>,
}

struct StoreState {
    folder: Option<PathBuf>,
    paging: bool,
    /// `firstPageId`; `i64::MAX` when there is none.
    first_page: i64,
    /// `currentPageId`.
    current_page_id: i64,
    /// `numberOfPages`.
    number_of_pages: i64,
    current: Option<CurrentPage>,
    cache: VecDeque<Arc<LoadedPage>>,
    subscriptions: BTreeMap<i64, Arc<PageSubscription>>,
    drop_warned: bool,
    /// What is written to this store's pages is replicated.
    replicated: bool,
}

/// What [`PagingStore::page`] did with a message.
pub enum Paged {
    /// The address is not paging: the message goes to its queues.
    No,
    /// Written to a page; wait for the sync before answering, if any.
    Yes(Option<Pending>),
    /// Dropped (DROP policy).
    Dropped,
}

/// What a producer's credit request gets (`checkMemory`).
pub enum Credits {
    Granted,
    /// Granted later, once memory is freed (BLOCK).
    Blocked,
    /// Refused (FAIL).
    Failed,
}

/// `PagingStoreImpl`: one address's memory accounting and pages.
pub struct PagingStore {
    address: Name,
    manager: Weak<PagingManager>,
    settings: Mutex<Arc<AddressSettings>>,
    size: AtomicI64,
    messages: AtomicI64,
    state: Mutex<StoreState>,
    /// Credit grants waiting for memory (`onMemoryFreedRunnables`).
    blocked: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    blocking: AtomicBool,
    /// Past `page-limit-bytes` or `page-limit-messages` (`pageFull`).
    page_full: AtomicBool,
    /// `blockedViaAddressControl`.
    blocked_via_management: AtomicBool,
    /// `enforceAddressFullMessagePolicy`: the policy whatever the settings
    /// say (a mirror's store-and-forward queue's).
    enforced: Mutex<Option<AddressFullPolicy>>,
    me: Weak<PagingStore>,
}

/// The page limits in force (`applySetting`): a policy without a limit,
/// or a limit without a policy, is ignored.
struct PageLimits {
    /// `estimatedMaxPages`: `page-limit-bytes` / `page-size-bytes`.
    max_pages: Option<i64>,
    max_messages: Option<i64>,
    policy: PageFullPolicy,
}

enum Lookup {
    Entry(Arc<PageEntry>),
    End,
}

impl PagingStore {
    fn new(
        address: &Name,
        manager: Weak<PagingManager>,
        settings: Arc<AddressSettings>,
    ) -> Arc<Self> {
        let store = Arc::new_cyclic(|me| Self {
            address: address.clone(),
            manager,
            settings: Mutex::new(settings.clone()),
            size: AtomicI64::new(0),
            messages: AtomicI64::new(0),
            state: Mutex::new(StoreState {
                folder: None,
                paging: false,
                first_page: i64::MAX,
                current_page_id: 0,
                number_of_pages: 0,
                current: None,
                cache: VecDeque::new(),
                subscriptions: BTreeMap::new(),
                drop_warned: false,
                replicated: false,
            }),
            blocked: Mutex::default(),
            blocking: AtomicBool::new(false),
            blocked_via_management: AtomicBool::new(false),
            page_full: AtomicBool::new(false),
            enforced: Mutex::new(None),
            me: me.clone(),
        });
        store.check_page_settings(&settings);
        store
    }

    /// `applySetting`'s warnings about half-configured page limits.
    fn check_page_settings(&self, s: &AddressSettings) {
        let bytes = s.page_limit_bytes.filter(|b| *b >= 0);
        let messages = s.page_limit_messages.filter(|m| *m >= 0);
        match (s.page_full_policy, bytes.is_some() || messages.is_some()) {
            (Some(p), false) => tracing::warn!(
                "AMQ224124: Address {} has page-full-policy={} but neither page-limit-bytes nor page-limit-messages are set. Page full configuration being ignored on this address.",
                self.address,
                if p == PageFullPolicy::Drop {
                    "DROP"
                } else {
                    "FAIL"
                }
            ),
            (None, true) => tracing::warn!(
                "AMQ224125: Address {} has page-limit-bytes={} and page-limit-messages={} but no page-full-policy set. Page full configuration being ignored on this address",
                self.address,
                bytes.map_or("null".to_owned(), |b| b.to_string()),
                messages.map_or("null".to_owned(), |m| m.to_string())
            ),
            _ => {}
        }
    }

    fn page_limits(&self) -> Option<PageLimits> {
        let s = self.settings();
        let policy = s.page_full_policy?;
        let bytes = s.page_limit_bytes.filter(|b| *b >= 0);
        let max_messages = s.page_limit_messages.filter(|m| *m >= 0);
        if bytes.is_none() && max_messages.is_none() {
            return None;
        }
        Some(PageLimits {
            max_pages: bytes
                .filter(|_| s.page_size_bytes > 0)
                .map(|b| b / s.page_size_bytes),
            max_messages,
            policy,
        })
    }

    /// `checkNumberOfPages`: more pages than `page-limit-bytes` allows.
    fn check_number_of_pages(&self, number_of_pages: i64) {
        if let Some(l) = self.page_limits()
            && let Some(max) = l.max_pages
            && number_of_pages > max
        {
            self.page_full.store(true, Ordering::SeqCst);
            tracing::warn!(
                "AMQ224123: Address {} has more pages than allowed. System currently has {number_of_pages} pages, while the estimated max number of pages is {max} based on the page-limit-bytes ({}) / page-size ({})",
                self.address,
                self.settings().page_limit_bytes.unwrap_or(-1),
                self.settings().page_size_bytes
            );
        }
    }

    /// `checkAdd`: a queue with more paged messages than
    /// `page-limit-messages`.
    fn check_add(&self, queue_id: i64, messages: i64) {
        if self.page_full.load(Ordering::SeqCst) {
            return;
        }
        if let Some(l) = self.page_limits()
            && let Some(max) = l.max_messages
            && messages >= max
        {
            self.page_full.store(true, Ordering::SeqCst);
            tracing::warn!(
                "AMQ224120: Queue {queue_id} on Address {} has more messages than configured page limit. PageLimitMessages={max} while currentValue={messages}",
                self.address
            );
        }
    }

    /// `checkPageLimit`: back under the limits, the address pages again.
    fn check_page_limit(&self, st: &StoreState) {
        if !self.page_full.load(Ordering::SeqCst) {
            return;
        }
        let Some(l) = self.page_limits() else {
            self.page_full.store(false, Ordering::SeqCst);
            return;
        };
        let messages = st
            .subscriptions
            .values()
            .map(|s| lock(&s.state).counter)
            .max()
            .unwrap_or(0);
        let messages_clear = l.max_messages.is_none_or(|m| messages < m);
        let bytes_clear = l.max_pages.is_none_or(|p| st.number_of_pages <= p);
        if messages_clear && bytes_clear {
            self.page_full.store(false, Ordering::SeqCst);
            tracing::info!(
                "AMQ224122: Address {} number of messages is under page limit again, and it should be allowed to page again.",
                self.address
            );
        }
    }

    pub fn address(&self) -> &Name {
        &self.address
    }

    pub fn manager(&self) -> Option<Arc<PagingManager>> {
        self.manager.upgrade()
    }

    fn settings(&self) -> Arc<AddressSettings> {
        let s = lock(&self.settings).clone();
        match *lock(&self.enforced) {
            Some(p) if s.address_full_policy != p => {
                let mut enforced = (*s).clone();
                enforced.address_full_policy = p;
                Arc::new(enforced)
            }
            _ => s,
        }
    }

    /// `enforceAddressFullMessagePolicy`: `policy` from now on, whatever
    /// the address settings say.
    pub fn enforce_policy(&self, policy: AddressFullPolicy) {
        *lock(&self.enforced) = Some(policy);
    }

    /// `applySetting`.
    pub fn apply_settings(&self, settings: Arc<AddressSettings>) {
        *lock(&self.settings) = settings;
    }

    /// `isPaging`, as the routing sees it.
    pub fn is_paging(&self) -> bool {
        lock(&self.state).paging
    }

    pub fn address_size(&self) -> i64 {
        self.size.load(Ordering::SeqCst)
    }

    /// `numberOfPages`.
    pub fn number_of_pages(&self) -> i64 {
        lock(&self.state).number_of_pages
    }

    /// `getPageSizeBytes`.
    pub fn page_size_bytes(&self) -> i64 {
        self.settings().page_size_bytes
    }

    /// The address's settings as the store applies them.
    pub fn address_settings(&self) -> Arc<AddressSettings> {
        self.settings()
    }

    /// `getAddressLimitPercent`: how full the address is, against its own
    /// limit or, without one, the global limit.
    pub fn address_limit_percent(&self) -> i32 {
        let s = self.settings();
        let size = self.size.load(Ordering::SeqCst);
        let max = if s.max_size_bytes > 0 {
            s.max_size_bytes
        } else {
            self.manager.upgrade().map_or(-1, |m| m.global_max_size)
        };
        if max <= 0 {
            0
        } else {
            (size as f64 * 100.0 / max as f64) as i32
        }
    }

    /// `block`: producers wait until [`PagingStore::unblock`].
    pub fn block(&self) {
        self.blocked_via_management.store(true, Ordering::SeqCst);
    }

    /// `unblock`.
    pub fn unblock(&self) {
        self.blocked_via_management.store(false, Ordering::SeqCst);
        self.check_released_memory();
    }

    /// `isBlockedViaManagement`.
    pub fn is_blocked_via_management(&self) -> bool {
        self.blocked_via_management.load(Ordering::SeqCst)
    }

    /// (first page, current page).
    fn page_range(&self) -> (i64, i64) {
        let st = lock(&self.state);
        (st.first_page.min(st.current_page_id), st.current_page_id)
    }

    /// `start`: finds the pages in `folder`; the last is the current page,
    /// written on from where it ends if it is whole. A store with more than
    /// one empty page is paging.
    fn start(&self, folder: PathBuf) -> io::Result<()> {
        let mut st = lock(&self.state);
        let mut pages = Vec::new();
        for entry in fs::read_dir(&folder)? {
            let name = entry?.file_name();
            if let Some(id) = name.to_str().and_then(saranga_paging::page_id_of) {
                pages.push(id);
            }
        }
        st.folder = Some(folder);
        st.number_of_pages = pages.len() as i64;
        self.check_number_of_pages(st.number_of_pages);
        st.first_page = pages.iter().copied().min().unwrap_or(i64::MAX);
        st.current_page_id = pages.iter().copied().max().unwrap_or(0);
        if st.current_page_id != 0 {
            let id = st.current_page_id;
            self.reload_live_page(&mut st, id)?;
        }
        let single_empty =
            st.number_of_pages == 1 && st.current.as_ref().is_some_and(|c| c.entries.is_empty());
        if st.current.is_some() && !single_empty {
            st.paging = true;
        }
        Ok(())
    }

    /// `reloadLivePage`: the current page, read back. A page with a broken
    /// end is left for recovery and a new page opened.
    fn reload_live_page(&self, st: &mut StoreState, id: i64) -> io::Result<()> {
        let path = self.page_path(st, id).expect("folder set");
        let data = fs::read(&path)?;
        let page = saranga_paging::read_page(&data);
        let entries = self.entries_of(page.messages);
        if page.valid_size != data.len() {
            st.cache.push_back(Arc::new(LoadedPage { id, entries }));
            self.open_new_page(st)?;
            return Ok(());
        }
        let file = OpenOptions::new().append(true).open(&path)?;
        st.current = Some(CurrentPage {
            id,
            path,
            file: Arc::new(file),
            size: data.len() as u64,
            entries,
        });
        Ok(())
    }

    fn page_path(&self, st: &StoreState, id: i64) -> Option<PathBuf> {
        st.folder
            .as_ref()
            .map(|f| f.join(saranga_paging::page_file_name(id)))
    }

    /// `newFileFactory`: the address's directory, named by a new UUID, with
    /// `address.txt`.
    fn folder(&self, st: &mut StoreState) -> io::Result<PathBuf> {
        if let Some(f) = &st.folder {
            return Ok(f.clone());
        }
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| io::Error::other("stopped"))?;
        let dir = manager
            .dir
            .as_ref()
            .ok_or_else(|| io::Error::other("no paging directory"))?;
        let folder = dir.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&folder)?;
        fs::write(
            folder.join(saranga_paging::ADDRESS_FILE),
            format!("{}\n", self.address),
        )?;
        st.folder = Some(folder.clone());
        Ok(folder)
    }

    /// `openNewPage`.
    fn open_new_page(&self, st: &mut StoreState) -> io::Result<()> {
        let folder = self.folder(st)?;
        let id = st.current_page_id + 1;
        let path = folder.join(saranga_paging::page_file_name(id));
        let file = saranga_fault::check(saranga_fault::Op::Open, &path)
            .and_then(|()| {
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)
            })
            .inspect_err(|e| {
                if let Some(m) = self.manager.upgrade() {
                    m.critical_io(e, &path);
                }
            })?;
        if st.replicated
            && let Some(old) = &st.current
            && let Some(r) = self.manager.upgrade().and_then(|m| m.replicator())
        {
            // `Page.close`: the backup closes its copy.
            r.send(
                ReplicationPageEventMessage {
                    store_name: (&*self.address).into(),
                    page_number: old.id,
                    is_delete: false,
                },
                None,
            );
        }
        if let Some(old) = st.current.take() {
            st.cache.push_back(Arc::new(LoadedPage {
                id: old.id,
                entries: old.entries,
            }));
            while st.cache.len() > PAGE_CACHE {
                st.cache.pop_front();
            }
        }
        st.number_of_pages += 1;
        self.check_number_of_pages(st.number_of_pages);
        st.current_page_id = id;
        st.first_page = st.first_page.min(id);
        st.current = Some(CurrentPage {
            id,
            path,
            file: Arc::new(file),
            size: 0,
            entries: Vec::new(),
        });
        Ok(())
    }

    fn page_info(&self) -> String {
        let s = self.settings();
        let (gs, gm) = self
            .manager
            .upgrade()
            .map(|m| (m.global_max_size, m.global_max_messages))
            .unwrap_or((-1, -1));
        let (size, msgs) = self
            .manager
            .upgrade()
            .map(|m| {
                (
                    m.global_size.load(Ordering::SeqCst),
                    m.global_messages.load(Ordering::SeqCst),
                )
            })
            .unwrap_or_default();
        format!(
            "size={} bytes ({} messages); maxSize={} bytes ({} messages); globalSize={} bytes ({} messages); globalMaxSize={} bytes ({} messages);",
            self.size.load(Ordering::SeqCst),
            self.messages.load(Ordering::SeqCst),
            s.max_size_bytes,
            s.max_size_messages,
            size,
            msgs,
            gs,
            gm
        )
    }

    /// `startPaging`.
    fn start_paging(&self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        if manager.dir.is_none() {
            return;
        }
        let mut st = lock(&self.state);
        if st.paging {
            return;
        }
        if st.current.is_none()
            && let Err(e) = self.open_new_page(&mut st)
        {
            tracing::warn!("AMQ222037: IO Error, impossible to start paging: {e}");
            return;
        }
        st.paging = true;
        drop(st);
        tracing::info!(
            "AMQ222038: Starting paging on address '{}'; {}",
            self.address,
            self.page_info()
        );
    }

    /// `stopPaging`, with `onPageModeCleared`: the subscriptions' counters
    /// go back to zero.
    fn stop_paging(&self, st: &mut StoreState) {
        if !st.paging {
            return;
        }
        st.paging = false;
        st.drop_warned = false;
        self.page_full.store(false, Ordering::SeqCst);
        for sub in st.subscriptions.values() {
            let mut s = lock(&sub.state);
            s.counter = 0;
            s.counter_size = 0;
            s.empty = true;
        }
        tracing::info!(
            "AMQ224108: Stopped paging on address '{}'; {}",
            self.address,
            self.page_info()
        );
    }

    /// `isFull`: the address or the broker is at its limit.
    pub fn is_full(&self) -> bool {
        self.is_full_with(&self.settings())
    }

    /// [`is_full`](Self::is_full) under settings already read.
    fn is_full_with(&self, s: &AddressSettings) -> bool {
        let own = (s.max_size_bytes >= 0 && self.size.load(Ordering::SeqCst) >= s.max_size_bytes)
            || (s.max_size_messages >= 0
                && self.messages.load(Ordering::SeqCst) >= s.max_size_messages);
        own || self.manager.upgrade().is_some_and(|m| m.is_global_full())
    }

    fn has_limit(&self, s: &AddressSettings) -> bool {
        s.max_size_bytes >= 0
            || s.max_size_messages >= 0
            || self
                .manager
                .upgrade()
                .is_some_and(|m| m.global_max_size >= 0 || m.global_max_messages >= 0)
    }

    /// `addSize`: memory taken (positive) or freed (negative) by the
    /// address's messages; `messages` counts messages, not references.
    pub fn add_size(&self, size: i64, messages: i64) {
        let new = self.size.fetch_add(size, Ordering::SeqCst) + size;
        self.messages.fetch_add(messages, Ordering::SeqCst);
        if new < 0 {
            tracing::warn!(
                "AMQ222214: Destination {} has an inconsistent and negative address size={new}.",
                self.address
            );
        }
        if let Some(m) = self.manager.upgrade() {
            m.add_global(size, messages);
        }
        let s = self.settings();
        match s.address_full_policy {
            AddressFullPolicy::Block | AddressFullPolicy::Fail => {
                if size < 0 || messages < 0 {
                    self.check_released_memory();
                }
            }
            AddressFullPolicy::Page => {
                if size > 0 && self.is_full_with(&s) {
                    self.start_paging();
                }
            }
            AddressFullPolicy::Drop => {}
        }
    }

    /// `checkReleasedMemory`: below the limit, blocked producers get their
    /// credits.
    fn check_released_memory(&self) {
        if self.is_full() || self.blocked_via_management.load(Ordering::SeqCst) {
            return;
        }
        let waiting = std::mem::take(&mut *lock(&self.blocked));
        for grant in waiting {
            grant();
        }
        if self.blocking.swap(false, Ordering::SeqCst) {
            tracing::info!(
                "AMQ221046: Unblocking message production on address '{}'; {}",
                self.address,
                self.page_info()
            );
        }
    }

    /// `checkMemory` for a producer's credit request: `grant` runs now, or
    /// once memory is freed (BLOCK); FAIL refuses the credits.
    pub fn request_credits(&self, grant: Box<dyn FnOnce() + Send>) -> Credits {
        // `blockedViaAddressControl`: producers wait whatever the policy.
        if self.blocked_via_management.load(Ordering::SeqCst) {
            lock(&self.blocked).push(grant);
            if self.blocked_via_management.load(Ordering::SeqCst) {
                return Credits::Blocked;
            }
            self.check_released_memory();
            return Credits::Blocked;
        }
        let s = self.settings();
        if let Some(m) = self.manager.upgrade()
            && m.is_disk_full()
        {
            match s.disk_full_policy {
                DiskFullPolicy::Fail => return Credits::Failed,
                DiskFullPolicy::Block => {
                    lock(&self.blocked).push(grant);
                    // The disk may have room again meanwhile.
                    if !m.is_disk_full() {
                        self.check_released_memory();
                        return Credits::Granted;
                    }
                    lock(&m.blocked).push(self.me.clone());
                    if !self.blocking.swap(true, Ordering::SeqCst) {
                        tracing::warn!(
                            "AMQ222212: Disk Full! Blocking message production on address '{}'. Clients will report blocked.",
                            self.address
                        );
                    }
                    return Credits::Blocked;
                }
                DiskFullPolicy::Drop => {
                    grant();
                    return Credits::Granted;
                }
            }
        }
        match s.address_full_policy {
            AddressFullPolicy::Fail if self.has_limit(&s) && self.is_full() => Credits::Failed,
            AddressFullPolicy::Block if self.has_limit(&s) && self.is_full() => {
                lock(&self.blocked).push(grant);
                if let Some(m) = self.manager.upgrade()
                    && m.is_global_full()
                {
                    lock(&m.blocked).push(self.me.clone());
                }
                // The memory may have been freed meanwhile.
                if !self.is_full() {
                    self.check_released_memory();
                    return Credits::Granted;
                }
                if !self.blocking.swap(true, Ordering::SeqCst) {
                    tracing::warn!(
                        "AMQ222183: Blocking message production on address '{}'; {}",
                        self.address,
                        self.page_info()
                    );
                }
                Credits::Blocked
            }
            _ => {
                grant();
                Credits::Granted
            }
        }
    }

    /// `pageStoreDropMessages`, once until paging stops.
    fn warn_dropping(&self) {
        let mut st = lock(&self.state);
        if !st.drop_warned {
            st.drop_warned = true;
            drop(st);
            tracing::warn!(
                "AMQ222039: Messages sent to address '{}' are being dropped; {}",
                self.address,
                self.page_info()
            );
        }
    }

    /// `page`: under DROP or FAIL a full address drops or refuses the
    /// message; under PAGE a paging address writes it to the current page
    /// for `queue_ids`. `tx` is the transaction sending it, if any.
    pub fn page(
        &self,
        message: &Arc<ServerMessage>,
        tx: Option<&crate::transaction::TxRef>,
        queues: &[crate::queue::Queue],
    ) -> Result<Paged, BrokerError> {
        let s = self.settings();
        // A full disk drops or refuses the message first.
        if matches!(
            s.disk_full_policy,
            DiskFullPolicy::Drop | DiskFullPolicy::Fail
        ) && self.manager.upgrade().is_some_and(|m| m.is_disk_full())
        {
            if s.disk_full_policy == DiskFullPolicy::Fail {
                return Err(BrokerError::new(
                    saranga_core_protocol::ErrorType::AddressFull,
                    format!("AMQ229102: Address \"{}\" is full.", self.address),
                ));
            }
            self.warn_dropping();
            return Ok(Paged::Dropped);
        }
        match s.address_full_policy {
            AddressFullPolicy::Drop | AddressFullPolicy::Fail => {
                if !self.is_full() {
                    return Ok(Paged::No);
                }
                if s.address_full_policy == AddressFullPolicy::Fail {
                    return Err(BrokerError::new(
                        saranga_core_protocol::ErrorType::AddressFull,
                        format!("AMQ229102: Address \"{}\" is full.", self.address),
                    ));
                }
                self.warn_dropping();
                return Ok(Paged::Dropped);
            }
            AddressFullPolicy::Block => return Ok(Paged::No),
            AddressFullPolicy::Page => {}
        }
        let Some(manager) = self.manager.upgrade() else {
            return Ok(Paged::No);
        };
        let mut st = lock(&self.state);
        if !st.paging {
            return Ok(Paged::No);
        }
        // Paging, but past the page limits: the message is dropped or
        // refused (`pageFull`).
        if self.page_full.load(Ordering::SeqCst)
            && let Some(l) = self.page_limits()
        {
            if l.policy == PageFullPolicy::Fail {
                return Err(BrokerError::new(
                    saranga_core_protocol::ErrorType::AddressFull,
                    format!("AMQ229102: Address \"{}\" is full.", self.address),
                ));
            }
            if !st.drop_warned {
                st.drop_warned = true;
                drop(st);
                tracing::warn!(
                    "AMQ222039: Messages sent to address '{}' are being dropped; {}",
                    self.address,
                    self.page_info()
                );
            }
            return Ok(Paged::Dropped);
        }
        let tx_id = tx.map(|t| crate::transaction::lock(t).id).unwrap_or(-1);
        let paged = PagedMessage {
            transaction_id: tx_id,
            body: match (message.large(), message.native()) {
                (Some(_), Some(_)) => PagedBody::OtherLarge(message.persisted()),
                (Some(_), None) => PagedBody::CoreLarge(message.core().clone()),
                (None, _) => PagedBody::Message(message.persisted()),
            },
            queue_ids: queues.iter().map(|q| q.id()).collect(),
        };
        let record = paged.encode_record();
        // directWritePage
        let page_size = s.page_size_bytes.max(1) as u64;
        let roll = match &st.current {
            None => true,
            Some(c) => c.size + record.len() as u64 > page_size && !c.entries.is_empty(),
        };
        let io = |e: io::Error| {
            BrokerError::new(saranga_core_protocol::ErrorType::IoError, e.to_string())
        };
        if roll {
            self.open_new_page(&mut st).map_err(io)?;
        }
        if let Some(storage) = &manager.storage {
            storage.page_write_history(message, &paged.queue_ids);
        }
        let replicated = st.replicated;
        let current = st.current.as_mut().expect("a current page");
        if let Err(e) = write_page(&current.file, &current.path, &record) {
            manager.critical_io(&e, &current.path);
            return Err(io(e));
        }
        let replicator = manager.replicator().filter(|_| replicated);
        if let Some(r) = &replicator {
            replicate_page_write(r, &self.address, current.id, &record);
        }
        let current = st.current.as_mut().expect("a current page");
        current.size += record.len() as u64;
        let nr = current.entries.len() as i32;
        current.entries.push(Arc::new(PageEntry {
            nr,
            tx_id,
            queue_ids: paged.queue_ids.clone(),
            message: Some(message.clone()),
        }));
        let file = current.file.clone();
        let file_path = current.path.clone();
        if let Some(body) = message.large() {
            // The page holds it now: its file goes with the page.
            body.set_keep(true);
        }
        let subs: Vec<Arc<PageSubscription>> = paged
            .queue_ids
            .iter()
            .filter_map(|id| st.subscriptions.get(id).cloned())
            .collect();
        drop(st);
        let size = persistent_size(message);
        for sub in &subs {
            lock(&sub.state).empty = false;
        }
        let pending = match tx {
            None => {
                for sub in &subs {
                    sub.counter_add(1, size);
                }
                let sync = message.is_durable()
                    && manager
                        .storage
                        .as_ref()
                        .is_some_and(|s| s.sync_non_transactional());
                if sync {
                    Some(manager.sync_file(file, file_path))
                } else {
                    replicator
                        .filter(|_| message.is_durable())
                        .map(|r| manager.replicated(&r))
                }
            }
            Some(tx) => {
                let (page_tx, _) = manager.install_transaction(tx_id);
                {
                    let mut p = lock(&page_tx.state);
                    let durable = queues
                        .iter()
                        .filter(|q| q.is_durable_ref_of(message))
                        .count() as i32;
                    p.persistent += durable;
                    p.messages += queues.len() as i32;
                    p.pending_counters
                        .extend(subs.iter().map(|s| (s.clone(), size)));
                }
                let mut t = crate::transaction::lock(tx);
                t.paging.manager.get_or_insert_with(|| manager.clone());
                if !t.paging.page_txs.iter().any(|p| Arc::ptr_eq(p, &page_tx)) {
                    t.paging.page_txs.push(page_tx);
                }
                if let Some(me) = self.me.upgrade()
                    && !t.paging.stores.iter().any(|s| Arc::ptr_eq(s, &me))
                {
                    t.paging.stores.push(me);
                }
                None
            }
        };
        for sub in &subs {
            sub.wake();
        }
        Ok(Paged::Yes(pending))
    }

    /// Syncs the current page (`ioSync`), before a transaction that paged
    /// commits.
    pub fn sync(&self) -> io::Result<()> {
        let current = lock(&self.state)
            .current
            .as_ref()
            .map(|c| (c.file.clone(), c.path.clone()));
        match current {
            Some((f, path)) => sync_page(&f, &path).inspect_err(|e| {
                if let Some(m) = self.manager.upgrade() {
                    m.critical_io(e, &path);
                }
            }),
            None => Ok(()),
        }
    }

    /// Decodes page records into entries.
    fn entries_of(&self, messages: Vec<PagedMessage>) -> Vec<Arc<PageEntry>> {
        let manager = self.manager.upgrade();
        messages
            .into_iter()
            .enumerate()
            .map(|(nr, m)| {
                let message = match (m.body, &manager) {
                    (PagedBody::Message(PersistedMessage::Core(c)), _) => {
                        Some(ServerMessage::new(c.to_message()))
                    }
                    (PagedBody::Message(record), Some(manager)) => {
                        match manager
                            .decoders
                            .get_or_init(MessageDecoders::default)
                            .decode(&record)
                        {
                            Ok(m) => Some(m),
                            Err(e) => {
                                tracing::warn!(
                                    "page {} of {}: message {}: {e}",
                                    self.address,
                                    nr,
                                    record.message_id()
                                );
                                None
                            }
                        }
                    }
                    (PagedBody::OtherLarge(record), Some(manager)) => {
                        let body = manager.large.existing(record.message_id());
                        body.set_keep(true);
                        let decoders = manager.decoders.get_or_init(MessageDecoders::default);
                        match decoders.decode_large(&record, body) {
                            Ok(m) => Some(m),
                            Err(e) => {
                                tracing::warn!("page of {}: {e}", self.address);
                                None
                            }
                        }
                    }
                    (PagedBody::CoreLarge(core), Some(manager)) => {
                        let body = manager.large.existing(core.message_id);
                        body.set_keep(true);
                        Some(ServerMessage::new_large(core, body))
                    }
                    _ => None,
                };
                Arc::new(PageEntry {
                    nr: nr as i32,
                    tx_id: m.transaction_id,
                    queue_ids: m.queue_ids,
                    message,
                })
            })
            .collect()
    }

    /// A page's messages: the current page's from memory, others from the
    /// cache or the file.
    fn load_page(&self, st: &mut StoreState, id: i64) -> Arc<LoadedPage> {
        if let Some(i) = st.cache.iter().position(|p| p.id == id) {
            let page = st.cache.remove(i).expect("position");
            st.cache.push_back(page.clone());
            return page;
        }
        let entries = match self.page_path(st, id).map(fs::read) {
            Some(Ok(data)) => self.entries_of(saranga_paging::read_page(&data).messages),
            _ => Vec::new(),
        };
        let page = Arc::new(LoadedPage { id, entries });
        st.cache.push_back(page.clone());
        while st.cache.len() > PAGE_CACHE {
            st.cache.pop_front();
        }
        page
    }

    fn lookup(&self, st: &mut StoreState, page: i64, nr: i32) -> Lookup {
        if let Some(c) = &st.current
            && c.id == page
        {
            return match c.entries.get(nr as usize) {
                Some(e) => Lookup::Entry(e.clone()),
                None => Lookup::End,
            };
        }
        match self.load_page(st, page).entries.get(nr as usize) {
            Some(e) => Lookup::Entry(e.clone()),
            None => Lookup::End,
        }
    }

    /// The number of messages in a page (`Page.getNumberOfMessages`).
    fn page_messages(&self, st: &mut StoreState, page: i64) -> i64 {
        if let Some(c) = &st.current
            && c.id == page
        {
            return c.entries.len() as i64;
        }
        if let Some(p) = st.cache.iter().find(|p| p.id == page) {
            return p.entries.len() as i64;
        }
        match self.page_path(st, page).map(fs::read) {
            Some(Ok(data)) => saranga_paging::count_messages(&data) as i64,
            _ => 0,
        }
    }

    fn page_exists(&self, st: &StoreState, page: i64) -> bool {
        self.page_path(st, page).is_some_and(|p| p.exists())
    }

    // ------------------------------------------------------- subscriptions

    /// `createSubscription`: the queue's cursor on this address's pages.
    pub fn subscribe(&self, queue_id: i64, persistent: bool) -> Arc<PageSubscription> {
        let sub = Arc::new(PageSubscription {
            queue_id,
            persistent,
            store: self.me.clone(),
            state: Mutex::new(SubState {
                consumed: BTreeMap::new(),
                last_acked: None,
                empty: true,
                counter: 0,
                counter_size: 0,
                recovered_acks: Vec::new(),
                removed: HashSet::new(),
                redeliveries: VecDeque::new(),
                in_memory: 0,
                in_memory_bytes: 0,
            }),
            waker: Mutex::new(None),
        });
        lock(&self.state)
            .subscriptions
            .insert(queue_id, sub.clone());
        sub
    }

    /// The queue is gone (`PageSubscription.destroy`): its cursor records
    /// go, and pages only it held are cleaned up.
    pub fn unsubscribe(&self, sub: &Arc<PageSubscription>) {
        let removed = lock(&self.state).subscriptions.remove(&sub.queue_id);
        if removed.is_none() {
            return;
        }
        let records: Vec<i64> = {
            let st = lock(&sub.state);
            st.consumed
                .values()
                .flat_map(|i| {
                    i.acks
                        .iter()
                        .flat_map(|a| a.values().copied())
                        .chain(i.complete)
                        .collect::<Vec<_>>()
                })
                .filter(|r| *r >= 0)
                .collect()
        };
        if let Some(s) = self.manager.upgrade().and_then(|m| m.storage.clone())
            && !records.is_empty()
        {
            let tx = s.generate_id();
            let written = records
                .iter()
                .try_for_each(|r| s.delete_record_tx(tx, *r))
                .and_then(|()| s.commit(tx).map(|_| ()));
            if let Err(e) = written {
                tracing::warn!("deleting the cursor records of queue {}: {e}", sub.queue_id);
            }
        }
        self.cleanup();
    }

    /// The next paged message for `sub`'s queue, if any
    /// (`CursorIterator.next`). Messages for other queues, already
    /// acknowledged, or of transactions that rolled back are passed over;
    /// those of transactions still open wait for them.
    pub fn next(&self, sub: &Arc<PageSubscription>, it: &mut PageIterator) -> Option<PagedRef> {
        let manager = self.manager.upgrade()?;
        if !manager.loaded.load(Ordering::SeqCst) {
            return None;
        }
        loop {
            if let Some(r) = lock(&sub.state).redeliveries.pop_front() {
                return Some(r);
            }
            let (page, entry) = {
                let mut st = lock(&self.state);
                if !st.paging {
                    return None;
                }
                let first = st.first_page.min(st.current_page_id);
                if !it.started || it.page < first {
                    it.page = first;
                    it.nr = 0;
                    it.started = true;
                }
                match self.lookup(&mut st, it.page, it.nr) {
                    Lookup::Entry(e) => {
                        it.nr += 1;
                        (it.page, e)
                    }
                    Lookup::End => {
                        let mut next = it.page + 1;
                        while next <= st.current_page_id && sub.page_finished(next) {
                            next += 1;
                        }
                        if next > st.current_page_id {
                            return None;
                        }
                        it.page = next;
                        it.nr = 0;
                        continue;
                    }
                }
            };
            let nr = entry.nr;
            let routed = entry.queue_ids.contains(&sub.queue_id);
            let Some(message) = entry.message.clone().filter(|_| routed) else {
                self.position_ignored(sub, page, nr);
                continue;
            };
            if sub.is_acked(page, nr) {
                continue;
            }
            let r = PagedRef {
                position: PagePosition {
                    page,
                    message_nr: nr,
                    tx_id: entry.tx_id,
                    size: persistent_size(&message),
                    ack_record: -1,
                },
                message,
            };
            if entry.tx_id > 0 {
                match manager.transaction(entry.tx_id) {
                    None => {
                        self.position_ignored(sub, page, nr);
                        continue;
                    }
                    Some(tx) => {
                        if tx.deliver_after_commit(self, sub, &r) {
                            continue;
                        }
                    }
                }
            }
            return Some(r);
        }
    }

    /// The next paged message for `sub`'s queue from `it`, for a management
    /// operation's own iterator (`pageSubscription.iterator()` in
    /// `iterQueue`): as [`PagingStore::next`], but the queue's cursor state
    /// is left alone, and messages of transactions not yet committed are
    /// passed over.
    pub fn scan(&self, sub: &Arc<PageSubscription>, it: &mut PageIterator) -> Option<PagedRef> {
        let manager = self.manager.upgrade()?;
        if !manager.loaded.load(Ordering::SeqCst) {
            return None;
        }
        loop {
            let (page, entry) = {
                let mut st = lock(&self.state);
                if !st.paging {
                    return None;
                }
                let first = st.first_page.min(st.current_page_id);
                if !it.started || it.page < first {
                    it.page = first;
                    it.nr = 0;
                    it.started = true;
                }
                match self.lookup(&mut st, it.page, it.nr) {
                    Lookup::Entry(e) => {
                        it.nr += 1;
                        (it.page, e)
                    }
                    Lookup::End => {
                        let mut next = it.page + 1;
                        while next <= st.current_page_id && sub.page_finished(next) {
                            next += 1;
                        }
                        if next > st.current_page_id {
                            return None;
                        }
                        it.page = next;
                        it.nr = 0;
                        continue;
                    }
                }
            };
            let nr = entry.nr;
            if !entry.queue_ids.contains(&sub.queue_id) || sub.is_acked(page, nr) {
                continue;
            }
            let Some(message) = entry.message.clone() else {
                continue;
            };
            if entry.tx_id > 0
                && !manager
                    .transaction(entry.tx_id)
                    .is_some_and(|tx| lock(&tx.state).committed)
            {
                continue;
            }
            return Some(PagedRef {
                position: PagePosition {
                    page,
                    message_nr: nr,
                    tx_id: entry.tx_id,
                    size: persistent_size(&message),
                    ack_record: -1,
                },
                message,
            });
        }
    }

    /// `positionIgnored`: a message the queue will never get counts as
    /// acknowledged for the page's completion.
    fn position_ignored(&self, sub: &Arc<PageSubscription>, page: i64, nr: i32) {
        if self.process_ack(sub, page, nr, -1) {
            self.cleanup();
        }
    }

    /// `processACK`: the position is acknowledged in memory. True when that
    /// finished the page.
    fn process_ack(&self, sub: &PageSubscription, page: i64, nr: i32, record: i64) -> bool {
        let (paging, messages, current) = {
            let mut st = lock(&self.state);
            let current = st.current.as_ref().is_some_and(|c| c.id == page);
            let cached = (!current)
                .then(|| {
                    lock(&sub.state)
                        .consumed
                        .get(&page)
                        .and_then(|i| i.number_of_messages)
                })
                .flatten();
            let messages = cached.unwrap_or_else(|| self.page_messages(&mut st, page));
            (st.paging, messages, current)
        };
        let mut s = lock(&sub.state);
        if s.last_acked.is_none_or(|l| (page, nr) > l) {
            s.last_acked = Some((page, nr));
        }
        s.removed.remove(&(page, nr));
        let info = s.info(page);
        let added = info.add_ack(nr, record);
        let done = added && info.is_done(paging, messages, !current);
        tracing::debug!(
            "{}: queue {} acked {page}/{nr} (record {record}), added={added}, confirmed={}/{messages}, pending_tx={}, done={done}",
            self.address,
            sub.queue_id,
            info.confirmed,
            info.pending_tx
        );
        done
    }

    /// A paged message acknowledged for good (`postAcknowledge` of a paged
    /// reference): the page counter drops, the page transaction is told,
    /// and a finished page is cleaned up.
    pub fn acknowledged(&self, sub: &Arc<PageSubscription>, position: &PagePosition) {
        sub.counter_add(-1, -position.size);
        {
            let mut s = lock(&sub.state);
            s.in_memory -= 1;
            s.in_memory_bytes -= position.size;
        }
        if position.tx_id > 0
            && let Some(m) = self.manager.upgrade()
            && let Some(tx) = m.transaction(position.tx_id)
        {
            tx.on_update(1, &m);
        }
        let done = self.process_ack(sub, position.page, position.message_nr, position.ack_record);
        sub.wake();
        if done {
            self.cleanup();
        }
    }

    /// Journals the acknowledgement of a paged message in the journal
    /// transaction `tx` (`ackTx`): the `ACKNOWLEDGE_CURSOR` record, and the
    /// page transaction's update. Returns the record's ID (-1 for a
    /// non-persistent subscription).
    pub fn store_ack_tx(
        &self,
        sub: &PageSubscription,
        tx: i64,
        position: &PagePosition,
        updates: &mut Vec<(i64, i32)>,
    ) -> StoreResult<i64> {
        let Some(storage) = self.manager.upgrade().and_then(|m| m.storage.clone()) else {
            return Ok(-1);
        };
        lock(&sub.state)
            .removed
            .insert((position.page, position.message_nr));
        let record = if sub.persistent {
            storage.store_cursor_ack_tx(tx, sub.queue_id, position.page, position.message_nr)?
        } else {
            -1
        };
        if position.tx_id > 0 {
            match updates.iter_mut().find(|(t, _)| *t == position.tx_id) {
                Some((_, n)) => *n += 1,
                None => updates.push((position.tx_id, 1)),
            }
        }
        Ok(record)
    }

    // ------------------------------------------------------------- reload

    /// `reloadPageCompletion`: a page the queue had finished. False when the
    /// page is gone, and the record with it.
    fn reload_page_completion(
        &self,
        sub: &PageSubscription,
        page: i64,
        messages: i32,
        record: i64,
    ) -> bool {
        let mut st = lock(&self.state);
        if !self.page_exists(&st, page) {
            return false;
        }
        if st.current.as_ref().is_some_and(|c| c.id == page)
            && let Err(e) = self.open_new_page(&mut st)
        {
            tracing::warn!("opening a new page on {}: {e}", self.address);
        }
        drop(st);
        let mut s = lock(&sub.state);
        let info = s.info(page);
        info.number_of_messages = Some(i64::from(messages));
        info.complete = Some(record);
        info.acks = None;
        true
    }

    /// `processReload` of the cursors: the recovered acknowledgements, in
    /// order, then page information for the pages before every cursor's
    /// first.
    fn process_reload(&self) {
        let subs: Vec<Arc<PageSubscription>> =
            lock(&self.state).subscriptions.values().cloned().collect();
        for sub in &subs {
            let mut s = lock(&sub.state);
            let mut acks = std::mem::take(&mut s.recovered_acks);
            acks.sort();
            for (page, nr, record) in acks {
                s.last_acked = Some((page, nr));
                s.info(page).load_ack(nr, record);
            }
        }
    }

    /// `PageCounterRebuildManager`: counts, per queue, the paged messages
    /// neither acknowledged nor of an unfinished transaction. Large messages
    /// found in the pages leave `stored_large`.
    fn rebuild_counters(&self, stored_large: &mut HashSet<i64>) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let mut st = lock(&self.state);
        if !st.paging {
            return;
        }
        let (first, current) = (st.first_page.min(st.current_page_id), st.current_page_id);
        let subs = st.subscriptions.clone();
        let mut counts: HashMap<i64, (i64, i64)> = HashMap::new();
        for page in first..=current {
            if !self.page_exists(&st, page) {
                continue;
            }
            let loaded = self.load_page(&mut st, page);
            for e in &loaded.entries {
                if let Some(m) = &e.message
                    && m.is_large()
                {
                    stored_large.remove(&m.message_id());
                }
                let Some(m) = &e.message else { continue };
                let size = persistent_size(m);
                let tx = (e.tx_id > 0)
                    .then(|| manager.transaction(e.tx_id))
                    .flatten();
                for qid in &e.queue_ids {
                    let Some(sub) = subs.get(qid) else { continue };
                    if sub.is_acked(page, e.nr) {
                        continue;
                    }
                    match &tx {
                        // A prepared transaction's messages count once it
                        // commits.
                        Some(t) if !lock(&t.state).committed => {
                            lock(&t.state).pending_counters.push((sub.clone(), size));
                        }
                        Some(_) => {
                            let c = counts.entry(*qid).or_default();
                            c.0 += 1;
                            c.1 += size;
                        }
                        None if e.tx_id > 0 => {}
                        None => {
                            let c = counts.entry(*qid).or_default();
                            c.0 += 1;
                            c.1 += size;
                        }
                    }
                }
            }
        }
        drop(st);
        for (qid, (n, size)) in counts {
            if let Some(sub) = subs.get(&qid) {
                let mut s = lock(&sub.state);
                s.counter = n;
                s.counter_size = size;
                s.empty = false;
            }
        }
    }

    // ------------------------------------------------------------ cleanup

    /// `PageCursorProviderImpl.cleanup`: each queue's finished pages get a
    /// `PAGE_CURSOR_COMPLETE` record in place of their acknowledgements;
    /// pages every queue has finished are deleted, with the large messages
    /// in them; with no page left the address stops paging.
    pub fn cleanup(&self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        if manager.cleanup_paused.load(Ordering::SeqCst) {
            return;
        }
        let Some(storage) = manager.storage.clone() else {
            return;
        };
        let mut deleted: Vec<(i64, Option<PathBuf>)> = Vec::new();
        let subs: Vec<Arc<PageSubscription>>;
        {
            let mut st = lock(&self.state);
            if !st.paging {
                return;
            }
            subs = st.subscriptions.values().cloned().collect();
            let current = st.current.as_ref().map(|c| c.id);
            // cleanupEntries, per subscription.
            let tx = storage.generate_id();
            let mut wrote = false;
            for sub in &subs {
                let pages: Vec<i64> = lock(&sub.state).consumed.keys().copied().collect();
                for page in pages {
                    if Some(page) == current {
                        continue;
                    }
                    let messages = self.page_messages(&mut st, page);
                    let exists = self.page_exists(&st, page);
                    let mut s = lock(&sub.state);
                    let Some(info) = s.consumed.get_mut(&page) else {
                        continue;
                    };
                    if !info.is_done(true, messages, true) || info.is_pending_delete() {
                        continue;
                    }
                    if messages <= 0 && !exists {
                        s.consumed.remove(&page);
                        continue;
                    }
                    info.pending_delete = true;
                    if sub.persistent {
                        match storage.store_page_complete_tx(
                            tx,
                            sub.queue_id,
                            page,
                            messages as i32,
                        ) {
                            Ok(r) => {
                                info.complete = Some(r);
                                wrote = true;
                            }
                            Err(e) => tracing::warn!("AMQ222016: Cannot cleanup page: {e}"),
                        }
                    }
                    for r in info.acks.take().into_iter().flat_map(|a| a.into_values()) {
                        if r >= 0 && storage.delete_record_tx(tx, r).is_ok() {
                            wrote = true;
                        }
                    }
                }
            }
            if wrote && let Err(e) = storage.commit(tx) {
                tracing::warn!("AMQ222016: Cannot cleanup page: {e}");
            }
            // The first page some subscription has not finished.
            let min_page = subs
                .iter()
                .filter_map(|s| s.first_page())
                .min()
                .unwrap_or(i64::MAX);
            tracing::debug!(
                "{}: cleanup, pages {}..={} ({}), first unfinished {min_page}",
                self.address,
                st.first_page,
                st.current_page_id,
                st.number_of_pages
            );
            // cleanupRegularStream
            let first = st.first_page;
            let mut page = first;
            while page <= min_page.min(st.current_page_id) {
                let complete = !self.page_exists(&st, page) || {
                    let messages = self.page_messages(&mut st, page);
                    let cache = Some(page) != current;
                    subs.iter().all(|s| s.is_complete(page, messages, cache))
                };
                if !complete {
                    break;
                }
                match self.depage(&mut st) {
                    Some(p) => deleted.push(p),
                    None => break,
                }
                page += 1;
            }
            // cleanupMiddleStream: later pages every queue has finished.
            let later: Vec<i64> = subs
                .iter()
                .flat_map(|s| {
                    lock(&s.state)
                        .consumed
                        .keys()
                        .copied()
                        .filter(|p| *p > min_page && Some(*p) != current)
                        .collect::<Vec<_>>()
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            for p in later {
                if p <= st.first_page || deleted.iter().any(|(d, _)| *d == p) {
                    continue;
                }
                let messages = self.page_messages(&mut st, p);
                if subs.iter().all(|s| s.is_complete(p, messages, true)) && self.page_exists(&st, p)
                {
                    st.number_of_pages -= 1;
                    deleted.push((p, self.page_path(&st, p)));
                }
            }
            for (p, _) in &deleted {
                st.cache.retain(|c| c.id != *p);
            }
            self.check_page_limit(&st);
            let current_empty = st.current.as_ref().is_none_or(|c| c.entries.is_empty());
            if st.number_of_pages == 0 || (st.number_of_pages == 1 && current_empty) {
                self.stop_paging(&mut st);
            }
        }
        // finishCleanup
        for (page, path) in deleted {
            let Some(path) = path else { continue };
            if let Ok(data) = fs::read(&path) {
                let large: Vec<i64> = saranga_paging::read_page(&data)
                    .messages
                    .iter()
                    .filter(|m| m.is_large())
                    .map(PagedMessage::message_id)
                    .collect();
                manager.large.delete_files(large);
            }
            if let Err(e) = fs::remove_file(&path)
                && e.kind() != io::ErrorKind::NotFound
            {
                tracing::warn!("AMQ222003: Error deleting page {}: {e}", path.display());
            }
            if let Some(r) = manager.replicator()
                && lock(&self.state).replicated
            {
                r.send(
                    ReplicationPageEventMessage {
                        store_name: (&*self.address).into(),
                        page_number: page,
                        is_delete: true,
                    },
                    None,
                );
            }
            for sub in &subs {
                sub.on_delete_page(page, &storage);
            }
        }
    }

    /// `depage`: takes the first page out of the store for deletion. The
    /// current page is replaced by a new one, or, empty, ends paging.
    fn depage(&self, st: &mut StoreState) -> Option<(i64, Option<PathBuf>)> {
        if st.number_of_pages == 0 {
            return None;
        }
        st.number_of_pages -= 1;
        if st.first_page >= st.current_page_id {
            let current = st.current.take()?;
            st.first_page = i64::MAX;
            if current.entries.is_empty() {
                self.stop_paging(st);
            } else if let Err(e) = self.open_new_page(st) {
                tracing::warn!("opening a new page on {}: {e}", self.address);
            }
            st.cache.retain(|c| c.id != current.id);
            return Some((current.id, Some(current.path)));
        }
        let page = st.first_page;
        st.first_page += 1;
        Some((page, self.page_path(st, page)))
    }
}

// ------------------------------------------------------------ subscription

/// The read position of a queue in its address's pages (`CursorIterator`).
#[derive(Debug, Default)]
pub struct PageIterator {
    page: i64,
    nr: i32,
    started: bool,
}

/// A paged message for a queue (`PagedReferenceImpl`).
#[derive(Debug, Clone)]
pub struct PagedRef {
    pub position: PagePosition,
    pub message: Arc<ServerMessage>,
}

/// `PageCursorInfo`: what a queue has done with one page.
#[derive(Debug)]
struct PageCursorInfo {
    /// Messages in the page, read on demand.
    number_of_messages: Option<i64>,
    /// Acknowledged message numbers and their records (-1: none); `None`
    /// once the page is complete.
    acks: Option<HashMap<i32, i64>>,
    /// Messages of open transactions waiting on the page.
    pending_tx: i32,
    /// The `PAGE_CURSOR_COMPLETE` record.
    complete: Option<i64>,
    confirmed: i64,
    pending_delete: bool,
}

impl PageCursorInfo {
    fn new() -> Self {
        Self {
            number_of_messages: None,
            acks: Some(HashMap::new()),
            pending_tx: 0,
            complete: None,
            confirmed: 0,
            pending_delete: false,
        }
    }

    fn is_acked(&self, nr: i32) -> bool {
        self.complete.is_some() || self.acks.as_ref().is_some_and(|a| a.contains_key(&nr))
    }

    /// `addACK`: true when newly acknowledged.
    fn add_ack(&mut self, nr: i32, record: i64) -> bool {
        let Some(acks) = &mut self.acks else {
            return false;
        };
        if acks.insert(nr, record).is_some() {
            return false;
        }
        if nr >= 0 {
            self.confirmed += 1;
        }
        true
    }

    fn load_ack(&mut self, nr: i32, record: i64) {
        self.add_ack(nr, record);
    }

    /// `isDone`: every message acknowledged and no transaction pending.
    /// The count of a page no longer written to is kept (`cache`); the
    /// current page's grows.
    fn is_done(&mut self, paging: bool, messages: i64, cache: bool) -> bool {
        if !paging {
            return true;
        }
        let n = match self.number_of_messages {
            Some(n) => n,
            None => {
                if cache {
                    self.number_of_messages = Some(messages);
                }
                messages
            }
        };
        self.complete.is_some() || (self.confirmed >= n && self.pending_tx == 0)
    }

    fn is_pending_delete(&self) -> bool {
        self.pending_delete || self.complete.is_some()
    }
}

struct SubState {
    consumed: BTreeMap<i64, PageCursorInfo>,
    last_acked: Option<(i64, i32)>,
    /// No message was paged for the queue (`empty`).
    empty: bool,
    /// Paged messages not acknowledged (`PageSubscriptionCounterImpl`).
    counter: i64,
    counter_size: i64,
    recovered_acks: Vec<(i64, i32, i64)>,
    /// Positions acknowledged in a transaction not yet complete.
    removed: HashSet<(i64, i32)>,
    /// Messages of transactions that committed after the cursor passed
    /// them (`redeliveries`).
    redeliveries: VecDeque<PagedRef>,
    /// Paged messages the queue holds in memory, and their size.
    in_memory: i64,
    in_memory_bytes: i64,
}

impl SubState {
    fn info(&mut self, page: i64) -> &mut PageCursorInfo {
        self.consumed
            .entry(page)
            .or_insert_with(PageCursorInfo::new)
    }
}

/// `PageSubscriptionImpl`: a queue's cursor on its address's pages.
pub struct PageSubscription {
    queue_id: i64,
    /// Durable queues journal their cursor.
    persistent: bool,
    store: Weak<PagingStore>,
    state: Mutex<SubState>,
    /// Tells the queue there is something to read.
    waker: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl PageSubscription {
    pub fn queue_id(&self) -> i64 {
        self.queue_id
    }

    pub fn store(&self) -> Option<Arc<PagingStore>> {
        self.store.upgrade()
    }

    pub fn set_waker(&self, waker: Box<dyn Fn() + Send + Sync>) {
        *lock(&self.waker) = Some(waker);
    }

    fn wake(&self) {
        if let Some(w) = &*lock(&self.waker) {
            w();
        }
    }

    fn counter_add(&self, n: i64, size: i64) {
        let value = {
            let mut s = lock(&self.state);
            s.counter += n;
            s.counter_size += size;
            s.counter
        };
        if n > 0
            && let Some(store) = self.store.upgrade()
        {
            store.check_add(self.queue_id, value);
        }
    }

    /// `getPersistentSize`: the size of the paged messages not
    /// acknowledged.
    pub fn persistent_size(&self) -> i64 {
        let s = lock(&self.state);
        if s.empty { 0 } else { s.counter_size.max(0) }
    }

    /// `getMessageCount`: paged messages not acknowledged.
    pub fn message_count(&self) -> i64 {
        let s = lock(&self.state);
        if s.empty { 0 } else { s.counter.max(0) }
    }

    /// A paged message read into the queue's memory.
    pub fn read_in(&self, size: i64) {
        let mut s = lock(&self.state);
        s.in_memory += 1;
        s.in_memory_bytes += size;
    }

    /// (messages, bytes) of paged messages the queue holds in memory.
    pub fn in_memory(&self) -> (i64, i64) {
        let s = lock(&self.state);
        (s.in_memory, s.in_memory_bytes)
    }

    fn is_acked(&self, page: i64, nr: i32) -> bool {
        let s = lock(&self.state);
        s.removed.contains(&(page, nr)) || s.consumed.get(&page).is_some_and(|i| i.is_acked(nr))
    }

    /// The page is complete or about to be deleted for this queue.
    fn page_finished(&self, page: i64) -> bool {
        lock(&self.state)
            .consumed
            .get(&page)
            .is_some_and(|i| i.complete.is_some() || i.pending_delete)
    }

    /// `getFirstPage`: the first page the queue has not finished.
    fn first_page(&self) -> Option<i64> {
        let s = lock(&self.state);
        if s.empty && s.consumed.is_empty() {
            return None;
        }
        let mut last = 0;
        for (page, info) in &s.consumed {
            last = *page;
            let done = info.complete.is_some()
                || info
                    .number_of_messages
                    .is_some_and(|n| info.confirmed >= n && info.pending_tx == 0);
            if !done && !info.is_pending_delete() {
                return Some(*page);
            }
        }
        Some(last)
    }

    /// `isComplete`.
    fn is_complete(&self, page: i64, messages: i64, cache: bool) -> bool {
        let mut s = lock(&self.state);
        if s.empty && s.consumed.is_empty() {
            return true;
        }
        let empty = s.empty;
        match s.consumed.get_mut(&page) {
            None => empty,
            Some(info) => info.is_done(true, messages, cache),
        }
    }

    /// `onDeletePage`: the page's records go with it.
    fn on_delete_page(&self, page: i64, storage: &Storage) {
        let info = lock(&self.state).consumed.remove(&page);
        if let Some(info) = info {
            for r in info
                .acks
                .into_iter()
                .flat_map(|a| a.into_values())
                .chain(info.complete)
                .filter(|r| *r >= 0)
            {
                let _ = storage.delete_record(r);
            }
        }
    }

    /// A message of a page transaction that committed after the cursor
    /// passed it (`redeliver`).
    fn redeliver(&self, r: PagedRef) {
        {
            let mut s = lock(&self.state);
            if let Some(info) = s.consumed.get_mut(&r.position.page) {
                info.pending_tx -= 1;
            }
            s.redeliveries.push_back(r);
        }
        self.wake();
    }

    /// A prepared transaction's acknowledgement of a paged message, recovered
    /// (`reloadPreparedACK`): the message is not delivered meanwhile.
    pub fn reload_prepared_ack(&self, page: i64, nr: i32) {
        lock(&self.state).removed.insert((page, nr));
    }

    /// The position of a recovered prepared acknowledgement, if its message
    /// is still in a page.
    pub fn paged_ref(&self, page: i64, nr: i32) -> Option<PagedRef> {
        let store = self.store.upgrade()?;
        let mut st = lock(&store.state);
        let Lookup::Entry(e) = store.lookup(&mut st, page, nr) else {
            return None;
        };
        let message = e.message.clone()?;
        Some(PagedRef {
            position: PagePosition {
                page,
                message_nr: nr,
                tx_id: e.tx_id,
                size: persistent_size(&message),
                ack_record: -1,
            },
            message,
        })
    }

    /// The acknowledgement of a recovered prepared transaction rolled back:
    /// the message is delivered again.
    pub fn unremove(&self, r: PagedRef) {
        lock(&self.state)
            .removed
            .remove(&(r.position.page, r.position.message_nr));
        lock(&self.state).redeliveries.push_back(r);
        self.wake();
    }
}

// ------------------------------------------------------------- page tx

struct PageTxState {
    /// The `PAGE_TRANSACTION` record; -1 until written.
    record: i64,
    committed: bool,
    rolled_back: bool,
    /// Messages times queues not yet consumed (`numberOfMessages`).
    messages: i32,
    /// Messages for durable queues (`numberOfPersistentMessages`).
    persistent: i32,
    /// Messages a cursor reached before the commit (`lateDeliveries`).
    late: Vec<(Arc<PageSubscription>, PagedRef)>,
    /// Counter increments due when the transaction commits.
    pending_counters: Vec<(Arc<PageSubscription>, i64)>,
}

/// `PageTransactionInfoImpl`: the messages a transaction paged.
pub struct PageTx {
    pub tx_id: i64,
    state: Mutex<PageTxState>,
}

impl std::fmt::Debug for PageTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageTx({})", self.tx_id)
    }
}

impl PageTx {
    fn new(tx_id: i64) -> Self {
        Self {
            tx_id,
            state: Mutex::new(PageTxState {
                record: -1,
                committed: false,
                rolled_back: false,
                messages: 0,
                persistent: 0,
                late: Vec::new(),
                pending_counters: Vec::new(),
            }),
        }
    }

    /// `store`: the `PAGE_TRANSACTION` record, in the transaction, once.
    pub fn store(&self, storage: &Storage, tx: i64) -> StoreResult<()> {
        let mut st = lock(&self.state);
        if st.record >= 0 {
            return Ok(());
        }
        st.record = storage.store_page_transaction_tx(tx, self.tx_id, st.persistent)?;
        Ok(())
    }

    /// `commit`: messages reached before the commit are delivered, and the
    /// queues count them.
    pub fn commit(&self) {
        let (late, counters) = {
            let mut st = lock(&self.state);
            st.committed = true;
            (
                std::mem::take(&mut st.late),
                std::mem::take(&mut st.pending_counters),
            )
        };
        for (sub, size) in counters {
            sub.counter_add(1, size);
        }
        for (sub, r) in late {
            sub.redeliver(r);
        }
    }

    /// `rollback`: the transaction's messages are passed over.
    pub fn rollback(&self, manager: &PagingManager) {
        let late = {
            let mut st = lock(&self.state);
            st.rolled_back = true;
            st.committed = false;
            st.pending_counters.clear();
            std::mem::take(&mut st.late)
        };
        for (sub, r) in late {
            if let Some(store) = sub.store() {
                {
                    let mut s = lock(&sub.state);
                    if let Some(info) = s.consumed.get_mut(&r.position.page) {
                        info.pending_tx -= 1;
                    }
                }
                store.position_ignored(&sub, r.position.page, r.position.message_nr);
            }
            self.on_update(1, manager);
        }
        // Nothing to consume: the transaction is forgotten.
        let messages = lock(&self.state).messages;
        if messages <= 0 || lock(&self.state).record < 0 {
            manager.remove_transaction(self.tx_id);
        }
    }

    /// `onUpdate`: messages consumed; with none left the record goes.
    fn on_update(&self, consumed: i32, manager: &PagingManager) {
        let (left, record) = {
            let mut st = lock(&self.state);
            st.messages -= consumed;
            (st.messages, st.record)
        };
        if left <= 0 {
            if record >= 0
                && let Some(s) = &manager.storage
                && let Err(e) = s.delete_record(record)
            {
                tracing::warn!("AMQ222026: Error on deleting page transaction {record}: {e}");
            }
            manager.remove_transaction(self.tx_id);
        }
    }

    /// `deliverAfterCommit`: true when the message is not to be delivered
    /// now: its transaction rolled back, or is still open (the message
    /// waits for the commit).
    fn deliver_after_commit(
        &self,
        store: &PagingStore,
        sub: &Arc<PageSubscription>,
        r: &PagedRef,
    ) -> bool {
        let mut st = lock(&self.state);
        if st.committed {
            return false;
        }
        if st.rolled_back {
            drop(st);
            store.position_ignored(sub, r.position.page, r.position.message_nr);
            if let Some(m) = store.manager.upgrade() {
                self.on_update(1, &m);
            }
            return true;
        }
        lock(&sub.state).info(r.position.page).pending_tx += 1;
        st.late.push((sub.clone(), r.clone()));
        true
    }
}

/// What a transaction did with pages: the page transactions of the
/// messages it paged, the stores to sync before it commits, and the page
/// transactions its acknowledgements consumed messages of.
#[derive(Debug, Default)]
pub struct TxPaging {
    pub manager: Option<Arc<PagingManager>>,
    pub page_txs: Vec<Arc<PageTx>>,
    pub stores: Vec<Arc<PagingStore>>,
    /// (page transaction, messages) its acknowledgements consumed.
    pub updates: Vec<(i64, i32)>,
    /// Written already, at prepare.
    stored: bool,
}

impl std::fmt::Debug for PagingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PagingStore({})", self.address)
    }
}

impl TxPaging {
    /// The paging of a transaction recovered prepared: its records are in
    /// the journal already.
    pub fn recovered(&mut self, manager: Arc<PagingManager>) {
        self.manager = Some(manager);
        self.stored = true;
    }

    /// `beforePrepare` / `beforeCommit`: the page files are synced, the
    /// page transactions and their updates journaled in `tx`.
    pub fn before_commit(&mut self, storage: &Storage, tx: i64) -> StoreResult<()> {
        for s in &self.stores {
            s.sync()
                .map_err(|e| saranga_journal::JournalError::Io(Arc::new(e)))?;
        }
        if std::mem::replace(&mut self.stored, true) {
            return Ok(());
        }
        for p in &self.page_txs {
            p.store(storage, tx)?;
        }
        if let Some(m) = &self.manager {
            for (page_tx, n) in &self.updates {
                if let Some(t) = m.transaction(*page_tx) {
                    let record = lock(&t.state).record;
                    if record >= 0 {
                        storage.update_page_transaction_tx(tx, record, *page_tx, *n)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// `afterCommit`.
    pub fn after_commit(self) {
        for p in self.page_txs {
            p.commit();
        }
    }

    /// `afterRollback`.
    pub fn after_rollback(self) {
        if let Some(m) = &self.manager {
            for p in self.page_txs {
                p.rollback(m);
            }
        }
    }
}

/// `ReplicationManager.pageWrite`: a record just written to a page, for the
/// backup's copy of it. The address goes with it from 2.24.
fn replicate_page_write(
    r: &crate::replication::Replicator,
    address: &Name,
    page_number: i64,
    record: &[u8],
) {
    use saranga_buffer::Codec;
    use saranga_core_protocol::packets::replication::{PagedMessage, ReplicationPageWriteMessage};
    let body = &record[5..record.len() - 1];
    let paged_message = match PagedMessage::read(&mut saranga_buffer::Reader::new(body)) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("a paged message on {address} cannot be replicated: {e}");
            return;
        }
    };
    let address = (r.version() >= saranga_core_protocol::version::ARTEMIS_2_24_0)
        .then(|| Some((&**address).into()));
    r.send(
        ReplicationPageWriteMessage {
            page_number,
            paged_message,
            address,
        },
        None,
    );
}

/// Appends a record to a page file.
fn write_page(file: &File, path: &Path, record: &[u8]) -> io::Result<()> {
    if let Err((n, e)) = saranga_fault::write(path, record.len()) {
        let mut f = file;
        f.write_all(&record[..n])?;
        return Err(e);
    }
    let mut f = file;
    f.write_all(record)
}

/// `fdatasync` on a page file.
fn sync_page(file: &File, path: &Path) -> io::Result<()> {
    saranga_fault::check(saranga_fault::Op::Sync, path)?;
    file.sync_data()
}
