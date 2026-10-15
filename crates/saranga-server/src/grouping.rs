//! Clustered message grouping: one node of the cluster (`LOCAL`) decides
//! which queue each message group goes to, and the others (`REMOTE`) ask it
//! with `PROPOSAL` notifications and hear its answers as
//! `PROPOSAL_RESPONSE`, so that a group's messages go to one consumer
//! wherever they are sent. A group given up (its queue gone, or unused
//! for `group-timeout`) is told as `UNPROPOSAL`. The deciding node
//! journals its groups (`GROUP_RECORD`).
//!
//! Ported from `LocalGroupingHandler`, `RemoteGroupingHandler` and
//! `GroupHandlingAbstract`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_config::GroupingHandlerConfig;
use saranga_storage::codec::{GroupingEncoding, RecordCodec};
use tokio::sync::Notify;

use crate::message::now_millis;
use crate::notifications::{NotificationType, hdr, string};
use crate::postoffice::PostOffice;

/// `BindingsImpl.MAX_GROUP_RETRY`.
pub const MAX_GROUP_RETRY: usize = 10;

/// `BindingType.LOCAL_QUEUE_INDEX`.
const LOCAL_QUEUE_INDEX: i32 = 0;

/// `Response`: where a group goes. Not accepted when the group already
/// went elsewhere (`alternative`).
#[derive(Debug)]
pub struct Response {
    pub group_id: String,
    pub cluster_name: String,
    pub alternative: Option<String>,
    used: AtomicI64,
}

impl Response {
    pub fn new(group_id: &str, cluster_name: &str, alternative: Option<&str>) -> Arc<Self> {
        Arc::new(Self {
            group_id: group_id.to_owned(),
            cluster_name: cluster_name.to_owned(),
            alternative: alternative.map(str::to_owned),
            used: AtomicI64::new(now_millis()),
        })
    }

    pub fn is_accepted(&self) -> bool {
        self.alternative.is_none()
    }

    /// `getChosenClusterName`.
    pub fn chosen(&self) -> &str {
        self.alternative.as_deref().unwrap_or(&self.cluster_name)
    }

    fn touch(&self) {
        self.used.store(now_millis(), Ordering::SeqCst);
    }

    fn time_used(&self) -> i64 {
        self.used.load(Ordering::SeqCst)
    }
}

/// `GroupBinding`: a group and the binding it goes to, journaled under
/// its ID.
#[derive(Debug, Clone)]
struct GroupBinding {
    id: i64,
    group_id: String,
    cluster_name: String,
    used: i64,
}

/// What a `LOCAL` handler keeps.
#[derive(Default)]
struct LocalState {
    /// `map`: by group ID.
    map: HashMap<String, GroupBinding>,
    /// `groupMap`: the group IDs of each binding.
    group_map: HashMap<String, Vec<String>>,
    /// `expectedBindings`: while starting, the bindings of journaled
    /// groups not added yet. `None` once started.
    expected: Option<Vec<String>>,
    waiting: bool,
}

/// What a `REMOTE` handler keeps.
#[derive(Default)]
struct RemoteState {
    /// `responses`: by group ID.
    responses: HashMap<String, Arc<Response>>,
    /// `groupMap`: the group IDs that went to each binding.
    group_map: HashMap<String, Vec<String>>,
    /// `pendingNotifications`: the proposals waiting for an answer, to be
    /// sent again when the deciding node asks (`resendPending`).
    pending: Vec<(u64, String, String)>,
    next_pending: u64,
}

enum Kind {
    Local {
        state: Mutex<LocalState>,
        bindings_added: Notify,
        reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    },
    Remote {
        state: Mutex<RemoteState>,
        answered: Condvar,
    },
}

/// `GroupingHandler`.
pub struct GroupingHandler {
    config: GroupingHandlerConfig,
    post_office: Weak<PostOffice>,
    started: AtomicBool,
    /// Stopped: no longer a notification listener.
    removed: AtomicBool,
    kind: Kind,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl GroupingHandler {
    /// `deployGroupingHandlerConfiguration`.
    pub fn new(config: GroupingHandlerConfig, post_office: &Arc<PostOffice>) -> Arc<Self> {
        let kind = if config.local {
            Kind::Local {
                state: Mutex::new(LocalState {
                    expected: Some(Vec::new()),
                    ..LocalState::default()
                }),
                bindings_added: Notify::new(),
                reaper: Mutex::default(),
            }
        } else {
            Kind::Remote {
                state: Mutex::default(),
                answered: Condvar::new(),
            }
        };
        Arc::new(Self {
            config,
            post_office: Arc::downgrade(post_office),
            started: AtomicBool::new(false),
            removed: AtomicBool::new(false),
            kind,
        })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn config(&self) -> &GroupingHandlerConfig {
        &self.config
    }

    pub fn is_local(&self) -> bool {
        matches!(self.kind, Kind::Local { .. })
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// `start`: a `LOCAL` handler reaps unused groups every
    /// `reaper-period` when `group-timeout` is set.
    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Kind::Local { state, reaper, .. } = &self.kind {
            let mut st = lock(state);
            if st.expected.is_none() {
                st.expected = Some(Vec::new());
            }
            drop(st);
            let (period, timeout) = (self.config.reaper_period, self.config.group_timeout);
            if period > 0 && timeout > 0 {
                let me = Arc::downgrade(self);
                let task = tokio::spawn(async move {
                    let period = Duration::from_millis(period as u64);
                    let mut tick =
                        tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                    loop {
                        tick.tick().await;
                        match me.upgrade() {
                            Some(h) => h.reap(),
                            None => break,
                        }
                    }
                });
                if let Some(old) = lock(reaper).replace(task) {
                    old.abort();
                }
            }
        }
    }

    /// `stop`.
    pub fn stop(&self) {
        self.started.store(false, Ordering::SeqCst);
        self.removed.store(true, Ordering::SeqCst);
        if let Kind::Local { reaper, .. } = &self.kind
            && let Some(task) = lock(reaper).take()
        {
            task.abort();
        }
    }

    fn notify(&self, kind: NotificationType, props: Vec<(&'static str, PropertyValue)>) {
        if let Some(po) = self.post_office.upgrade() {
            po.notify(kind, move |_| props);
        }
    }

    /// The properties of a proposal, its response or an unproposal.
    fn props(
        &self,
        group_id: &str,
        cluster_name: &str,
        alternative: Option<Option<&str>>,
        distance: i32,
    ) -> Vec<(&'static str, PropertyValue)> {
        let mut p = vec![
            (hdr::PROPOSAL_GROUP_ID, string(Some(group_id))),
            (hdr::PROPOSAL_VALUE, string(Some(cluster_name))),
        ];
        if let Some(alt) = alternative {
            p.push((hdr::PROPOSAL_ALT_VALUE, string(alt)));
        }
        p.push((hdr::BINDING_TYPE, PropertyValue::Int(LOCAL_QUEUE_INDEX)));
        p.push((hdr::ADDRESS, string(Some(&self.config.address))));
        p.push((hdr::DISTANCE, PropertyValue::Int(distance)));
        p
    }

    /// `sendUnproposal`.
    fn send_unproposal(&self, group_id: &str, cluster_name: &str, distance: i32) {
        let props = self.props(group_id, cluster_name, None, distance);
        self.notify(NotificationType::Unproposal, props);
    }

    /// `fireUnproposed`: every queue whose groups include it forgets the
    /// group.
    fn fire_unproposed(&self, group_id: &str) {
        if let Some(po) = self.post_office.upgrade() {
            for q in po.queues() {
                q.unproposed(group_id);
            }
        }
    }

    /// `propose`: where the group goes. A `LOCAL` handler decides at once
    /// (a group it knows keeps its binding); a `REMOTE` one asks the
    /// deciding node and waits up to `timeout` for the answer, `None`
    /// when it does not come.
    pub fn propose(
        &self,
        group_id: &str,
        cluster_name: &str,
    ) -> Result<Option<Arc<Response>>, String> {
        match &self.kind {
            Kind::Local { state, .. } => {
                Ok(Some(self.local_propose(state, group_id, cluster_name)))
            }
            Kind::Remote { state, answered } => {
                let mut st = lock(state);
                if let Some(r) = st.responses.get(group_id).cloned() {
                    drop(st);
                    self.check_timeout(&r);
                    return Ok(Some(r));
                }
                if !self.is_started() {
                    return Err(
                        "AMQ229104: Server is stopping. Message grouping not allowed".into(),
                    );
                }
                st.next_pending += 1;
                let key = st.next_pending;
                st.pending
                    .push((key, group_id.to_owned(), cluster_name.to_owned()));
                self.notify(
                    NotificationType::Proposal,
                    self.props(group_id, cluster_name, None, 0),
                );
                let timeout = Duration::from_millis(self.config.timeout.max(0) as u64);
                let deadline = std::time::Instant::now() + timeout;
                let wait = |st: MutexGuard<'_, RemoteState>| {
                    let mut st = st;
                    let mut response = None;
                    loop {
                        let now = std::time::Instant::now();
                        if now >= deadline {
                            break;
                        }
                        st = answered
                            .wait_timeout(st, deadline - now)
                            .unwrap_or_else(|e| e.into_inner())
                            .0;
                        if let Some(r) = st.responses.get(group_id) {
                            response = Some(r.clone());
                            break;
                        }
                    }
                    st.pending.retain(|(k, _, _)| *k != key);
                    response
                };
                // The answer comes on another task: this thread waits for
                // it without holding up the runtime.
                let response = match tokio::runtime::Handle::try_current() {
                    Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                        tokio::task::block_in_place(|| wait(st))
                    }
                    _ => wait(st),
                };
                if response.is_none() {
                    tracing::warn!("AMQ222161: Group Handler timed-out waiting for sendCondition");
                }
                Ok(response)
            }
        }
    }

    /// `LocalGroupingHandler.propose`.
    fn local_propose(
        &self,
        state: &Mutex<LocalState>,
        group_id: &str,
        cluster_name: &str,
    ) -> Arc<Response> {
        let binding = {
            let mut st = lock(state);
            if let Some(b) = st.map.get_mut(group_id) {
                b.used = now_millis();
                // Already grouped: the binding it has is the alternative.
                return Response::new(&b.group_id, cluster_name, Some(&b.cluster_name));
            }
            let id = self.post_office.upgrade().map_or(0, |po| po.ids().next());
            let b = GroupBinding {
                id,
                group_id: group_id.to_owned(),
                cluster_name: cluster_name.to_owned(),
                used: now_millis(),
            };
            st.group_map
                .entry(b.cluster_name.clone())
                .or_default()
                .push(b.group_id.clone());
            st.map.insert(b.group_id.clone(), b.clone());
            b
        };
        // Stored outside the lock.
        if let Some(s) = self
            .post_office
            .upgrade()
            .and_then(|po| po.ids().storage().cloned())
        {
            let body = GroupingEncoding {
                group_id: SimpleString::from(binding.group_id.as_str()),
                cluster_name: SimpleString::from(binding.cluster_name.as_str()),
            }
            .encode();
            if let Err(e) =
                s.add_binding_record_with_id(binding.id, saranga_storage::ids::GROUP_RECORD, body)
            {
                tracing::warn!("storing group {}: {e}", binding.group_id);
            }
        }
        Response::new(&binding.group_id, &binding.cluster_name, None)
    }

    /// `RemoteGroupingHandler.checkTimeout`: a group used again after
    /// `group-timeout` is proposed again, so the deciding node keeps it.
    fn check_timeout(&self, response: &Response) {
        let timeout = self.config.group_timeout;
        if timeout > 0 && response.time_used() + timeout < now_millis() {
            response.touch();
            self.notify(
                NotificationType::Proposal,
                self.props(&response.group_id, &response.cluster_name, None, 0),
            );
        }
    }

    /// `getProposal`: the group's binding, if it has one.
    pub fn get_proposal(&self, full_id: &str, touch: bool) -> Option<Arc<Response>> {
        match &self.kind {
            Kind::Local { state, .. } => {
                let mut st = lock(state);
                let b = st.map.get_mut(full_id)?;
                if touch {
                    b.used = now_millis();
                }
                Some(Response::new(full_id, &b.cluster_name, None))
            }
            Kind::Remote { state, .. } => {
                let r = lock(state).responses.get(full_id).cloned();
                if touch && let Some(r) = &r {
                    self.check_timeout(r);
                }
                r
            }
        }
    }

    /// `proposed`: the deciding node's answer.
    pub fn proposed(&self, response: Arc<Response>) {
        if let Kind::Remote { state, answered } = &self.kind {
            let mut st = lock(state);
            st.group_map
                .entry(response.chosen().to_owned())
                .or_default()
                .push(response.group_id.clone());
            st.responses.insert(response.group_id.clone(), response);
            answered.notify_all();
        }
    }

    /// `sendProposalResponse`: a `LOCAL` handler tells its answer.
    pub fn send_proposal_response(&self, response: &Response, distance: i32) {
        if self.is_local() {
            let props = self.props(
                &response.group_id,
                &response.cluster_name,
                Some(response.alternative.as_deref()),
                distance,
            );
            self.notify(NotificationType::ProposalResponse, props);
        }
    }

    /// `receive`: another node's proposal. A `LOCAL` handler answers it;
    /// a `REMOTE` one passes it on a hop further.
    pub fn receive(
        &self,
        group_id: &str,
        cluster_name: &str,
        distance: i32,
    ) -> Option<Arc<Response>> {
        tracing::trace!("received proposal Proposal:{group_id}:{cluster_name}");
        match &self.kind {
            Kind::Local { state, .. } => Some(self.local_propose(state, group_id, cluster_name)),
            Kind::Remote { .. } => {
                self.notify(
                    NotificationType::Proposal,
                    self.props(group_id, cluster_name, None, distance),
                );
                None
            }
        }
    }

    /// `remove(groupId, clusterName)`.
    pub fn remove(&self, group_id: &str, cluster_name: &str) {
        match &self.kind {
            Kind::Local { state, .. } => {
                let removed = {
                    let mut st = lock(state);
                    let b = st.map.remove(group_id);
                    match (st.group_map.get_mut(cluster_name), b) {
                        (Some(groups), Some(b)) => {
                            groups.retain(|g| *g != b.group_id);
                            Some(b)
                        }
                        _ => None,
                    }
                };
                if let Some(b) = removed {
                    self.delete_records(&[b.id], &b.group_id);
                }
            }
            Kind::Remote { state, .. } => {
                {
                    let mut st = lock(state);
                    if let Some(groups) = st.group_map.get_mut(cluster_name) {
                        groups.retain(|g| g != group_id);
                    }
                    st.responses.remove(group_id);
                }
                self.fire_unproposed(group_id);
            }
        }
    }

    /// `remove(groupId, clusterName, distance)`: an unproposal heard from
    /// another node; a `REMOTE` handler passes it on.
    pub fn remove_at(&self, group_id: &str, cluster_name: &str, distance: i32) {
        self.remove(group_id, cluster_name);
        if !self.is_local() {
            self.send_unproposal(group_id, cluster_name, distance);
        }
    }

    /// `forceRemove`: the group's binding is gone, and every node is told.
    pub fn force_remove(&self, group_id: &str, cluster_name: &str) {
        self.remove(group_id, cluster_name);
        self.send_unproposal(group_id, cluster_name, 0);
    }

    /// `resendPending`: the proposals waiting for an answer go again.
    pub fn resend_pending(&self) {
        if let Kind::Remote { state, .. } = &self.kind {
            let pending = lock(state).pending.clone();
            for (_, group_id, cluster_name) in pending {
                self.notify(
                    NotificationType::Proposal,
                    self.props(&group_id, &cluster_name, None, 0),
                );
            }
        }
    }

    /// `addGroupBinding`: a journaled group, at startup.
    pub fn add_group_binding(&self, id: i64, group_id: &str, cluster_name: &str) {
        if let Kind::Local { state, .. } = &self.kind {
            let mut st = lock(state);
            st.map.insert(
                group_id.to_owned(),
                GroupBinding {
                    id,
                    group_id: group_id.to_owned(),
                    cluster_name: cluster_name.to_owned(),
                    used: now_millis(),
                },
            );
            st.group_map
                .entry(cluster_name.to_owned())
                .or_default()
                .push(group_id.to_owned());
        }
    }

    /// `awaitBindings`: a `LOCAL` handler with journaled groups waits up to
    /// `timeout` for their bindings to be added (`BINDING_ADDED`) before
    /// the broker takes clients.
    pub async fn await_bindings(&self, bound: Vec<String>) {
        let Kind::Local {
            state,
            bindings_added,
            ..
        } = &self.kind
        else {
            return;
        };
        {
            let mut st = lock(state);
            if st.group_map.is_empty() {
                st.expected = None;
                st.waiting = false;
                return;
            }
            st.waiting = true;
            // The queues bound before the broker was active count as added.
            let mut already = st.expected.take().unwrap_or_default();
            already.extend(bound);
            let mut expected: Vec<String> = st
                .group_map
                .keys()
                .filter(|k| !already.contains(k))
                .cloned()
                .collect();
            expected.sort();
            let empty = expected.is_empty();
            st.expected = Some(expected);
            if empty {
                st.expected = None;
                st.waiting = false;
                return;
            }
        }
        tracing::debug!(
            "Waiting remote group bindings to arrive before starting the server. timeout={} milliseconds",
            self.config.timeout
        );
        let timeout = Duration::from_millis(self.config.timeout.max(0) as u64);
        let arrived = tokio::time::timeout(timeout, async {
            loop {
                let notified = bindings_added.notified();
                if lock(state).expected.as_ref().is_none_or(Vec::is_empty) {
                    break;
                }
                notified.await;
            }
        })
        .await;
        if arrived.is_err() {
            tracing::info!("AMQ221040: Remote group coordinators has not started.");
        }
        let mut st = lock(state);
        st.expected = None;
        st.waiting = false;
    }

    /// `onNotification`: a binding removed takes its groups with it; one
    /// added may be what a starting `LOCAL` handler waits for.
    pub fn on_notification(&self, kind: &str, cluster_name: Option<&str>) {
        if self.removed.load(Ordering::SeqCst) {
            return;
        }
        match (&self.kind, kind) {
            (Kind::Local { state, .. }, "BINDING_REMOVED") => {
                if let Some(c) = cluster_name {
                    self.remove_grouping(state, c);
                }
            }
            (
                Kind::Local {
                    state,
                    bindings_added,
                    ..
                },
                "BINDING_ADDED",
            ) => {
                let mut st = lock(state);
                let waiting = st.waiting;
                if let (Some(expected), Some(c)) = (st.expected.as_mut(), cluster_name) {
                    if waiting {
                        expected.retain(|e| e != c);
                    } else {
                        expected.push(c.to_owned());
                    }
                    if waiting && expected.is_empty() {
                        bindings_added.notify_waiters();
                    }
                }
            }
            (Kind::Remote { state, .. }, "BINDING_REMOVED") => {
                if let Some(c) = cluster_name {
                    let mut st = lock(state);
                    if let Some(groups) = st.group_map.remove(c) {
                        for g in groups {
                            st.responses.remove(&g);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// `removeGrouping`: the groups of a binding that went, each told as
    /// unproposed and deleted from the journal.
    fn remove_grouping(&self, state: &Mutex<LocalState>, cluster_name: &str) {
        let removed: Vec<GroupBinding> = {
            let mut st = lock(state);
            let Some(groups) = st.group_map.remove(cluster_name) else {
                return;
            };
            groups.iter().filter_map(|g| st.map.remove(g)).collect()
        };
        let mut ids = Vec::new();
        for b in &removed {
            self.fire_unproposed(&b.group_id);
            self.send_unproposal(&b.group_id, cluster_name, 0);
            ids.push(b.id);
        }
        self.delete_records(&ids, &format!("TX:{cluster_name}"));
    }

    /// `GroupIdReaper`: groups unused for `group-timeout` are given up.
    fn reap(&self) {
        let Kind::Local { state, .. } = &self.kind else {
            return;
        };
        if !self.is_started() {
            return;
        }
        let now = now_millis();
        let timeout = self.config.group_timeout;
        let expired: Vec<GroupBinding> = {
            let mut st = lock(state);
            let gone: Vec<String> = st
                .map
                .values()
                .filter(|b| b.used + timeout < now)
                .map(|b| b.group_id.clone())
                .collect();
            let mut out = Vec::new();
            for g in gone {
                if let Some(b) = st.map.remove(&g) {
                    if let Some(groups) = st.group_map.get_mut(&b.cluster_name) {
                        groups.retain(|x| *x != b.group_id);
                    }
                    out.push(b);
                }
            }
            out
        };
        for chunk in expired.chunks(1000) {
            for b in chunk {
                self.fire_unproposed(&b.group_id);
                self.send_unproposal(&b.group_id, &b.cluster_name, 0);
            }
            let ids: Vec<i64> = chunk.iter().map(|b| b.id).collect();
            self.delete_records(&ids, &chunk[0].group_id);
        }
    }

    /// `deleteGrouping` for each, then `commitBindings`.
    fn delete_records(&self, ids: &[i64], what: &str) {
        if let Some(s) = self
            .post_office
            .upgrade()
            .and_then(|po| po.ids().storage().cloned())
            && let Err(e) = s.delete_bindings(ids)
        {
            tracing::warn!("AMQ222114: Unable to delete group binding info {what}: {e}");
        }
    }
}
