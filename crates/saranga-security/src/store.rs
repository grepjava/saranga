//! The security store: authenticates clients and checks what they may do
//! against the security settings. Ported from `SecurityStoreImpl`.
//!
//! Successful and failed logins are cached for
//! `security-invalidation-interval` under a hash of the credentials, and
//! granted permissions under the user and check type, as Artemis does. The
//! caches are bounded by `authentication-cache-size` and
//! `authorization-cache-size`; when full, the oldest entry goes (Artemis's
//! Caffeine cache evicts by frequency and recency instead).

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use saranga_config::{CheckType, HierarchicalRepository, Role, SecuritySetting};
use sha2::{Digest, Sha256};

use crate::audit;
use crate::jaas::Subject;
use crate::manager::{CERT_SUBJECT_DN_UNAVAILABLE, ConnectionInfo, SecurityManager};

/// `ActiveMQSecurityException`: the message, with its `AMQ` ID, that
/// clients receive.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SecurityError(pub String);

/// Who a session is, for permission checks (`SecurityAuth`).
#[derive(Debug, Clone, Default)]
pub struct SecurityAuth {
    pub username: Option<String>,
    pub password: Option<String>,
    pub connection: Arc<ConnectionInfo>,
    /// The subject the connection authenticated as
    /// (`RemotingConnection.getSubject`), named in error messages when the
    /// client gave no user name.
    pub subject: Option<Arc<Subject>>,
    /// A management caller (`ManagementRemotingConnection`), such as a
    /// web login guarded by `ArtemisRbacMBeanServerBuilder`: without a user
    /// name and password, its subject is authorized as it is.
    pub management: bool,
}

/// A successful authentication.
#[derive(Debug, Clone, Default)]
pub struct Authenticated {
    /// The user the security manager validated, `None` when security is
    /// disabled.
    pub validated_user: Option<String>,
    pub subject: Option<Arc<Subject>>,
}

/// The settings of [`SecurityStore`], from `broker.xml`.
#[derive(Debug, Clone)]
pub struct SecurityStoreConfig {
    pub enabled: bool,
    pub invalidation_interval: Duration,
    pub authentication_cache_size: usize,
    pub authorization_cache_size: usize,
    pub cluster_user: String,
    pub cluster_password: String,
}

impl SecurityStoreConfig {
    pub fn from_broker(config: &saranga_config::BrokerConfig) -> Self {
        Self {
            enabled: config.security_enabled,
            invalidation_interval: Duration::from_millis(
                config.security_invalidation_interval.max(0) as u64,
            ),
            authentication_cache_size: config.authentication_cache_size.max(0) as usize,
            authorization_cache_size: config.authorization_cache_size.max(0) as usize,
            cluster_user: config.cluster_user.clone(),
            cluster_password: config.cluster_password.clone(),
        }
    }
}

enum ClusterCheck {
    Valid,
    Invalid,
    Ignore,
}

/// Counts of successes and failures, as the management API reports them.
#[derive(Debug, Default)]
pub struct SecurityCounters {
    pub authentication_success: AtomicU64,
    pub authentication_failure: AtomicU64,
    pub authorization_success: AtomicU64,
    pub authorization_failure: AtomicU64,
}

/// A refused login or permission, as management notifications report it
/// (`SECURITY_AUTHENTICATION_VIOLATION`, `SECURITY_PERMISSION_VIOLATION`).
#[derive(Debug, Clone)]
pub enum Violation {
    Authentication {
        user: Option<String>,
        cert_subject_dn: String,
        /// As `RemotingConnection.getRemoteAddress` prints it.
        remote_address: String,
    },
    Permission {
        /// Without a queue name.
        address: String,
        check: CheckType,
        /// `getCaller`.
        user: String,
    },
}

/// Told of each [`Violation`].
pub type ViolationListener = Arc<dyn Fn(Violation) + Send + Sync>;

pub struct SecurityStore {
    /// The broker's notifications (`notificationService`).
    violations: RwLock<Option<ViolationListener>>,
    config: SecurityStoreConfig,
    repository: RwLock<HierarchicalRepository<SecuritySetting>>,
    manager: Arc<dyn SecurityManager>,
    /// Credentials hash -> the subject, or `None` for a refused login.
    authentication_cache: Option<TtlCache<String, Option<Arc<Subject>>>>,
    /// `user.CHECK` -> addresses (or FQQNs) granted.
    authorization_cache: Option<TtlCache<String, HashSet<String>>>,
    pub counters: SecurityCounters,
}

impl SecurityStore {
    pub fn new(
        config: SecurityStoreConfig,
        repository: HierarchicalRepository<SecuritySetting>,
        manager: Arc<dyn SecurityManager>,
    ) -> Self {
        let (enabled, ttl) = (config.enabled, config.invalidation_interval);
        Self {
            authentication_cache: (enabled && config.authentication_cache_size > 0)
                .then(|| TtlCache::new(config.authentication_cache_size, ttl)),
            authorization_cache: (enabled && config.authorization_cache_size > 0)
                .then(|| TtlCache::new(config.authorization_cache_size, ttl)),
            config,
            repository: RwLock::new(repository),
            manager,
            counters: SecurityCounters::default(),
            violations: RwLock::default(),
        }
    }

    /// Who hears of refused logins and permissions.
    pub fn set_violation_listener(&self, listener: ViolationListener) {
        *self.violations.write().unwrap_or_else(|e| e.into_inner()) = Some(listener);
    }

    fn violation(&self, v: Violation) {
        let listener = self
            .violations
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(l) = listener {
            l(v);
        }
    }

    /// `authenticationFailed`: the notification.
    fn authentication_failed(&self, user: Option<&str>, connection: &ConnectionInfo) {
        self.violation(Violation::Authentication {
            user: user.map(str::to_owned),
            cert_subject_dn: connection.cert_subject_dn().to_owned(),
            remote_address: connection.remote_address.trim_start_matches('/').to_owned(),
        });
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn manager(&self) -> &Arc<dyn SecurityManager> {
        &self.manager
    }

    /// `HierarchicalRepository.containsExactMatch` on the security settings.
    pub fn contains_exact_match(&self, m: &str) -> bool {
        self.repository
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_exact_match(m)
    }

    /// Replaces the security settings, as a configuration reload does, and
    /// forgets granted permissions (`onChange`).
    pub fn set_repository(&self, repository: HierarchicalRepository<SecuritySetting>) {
        *self.repository.write().unwrap_or_else(|e| e.into_inner()) = repository;
        self.invalidate_authorization_cache();
    }

    /// `addMatch` on the security settings (management's
    /// `addSecuritySettings`): granted permissions are forgotten.
    pub fn add_match(&self, address_match: &str, setting: SecuritySetting) -> Result<(), String> {
        self.repository
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .add_match(address_match, setting)
            .map_err(|e| e.to_string())?;
        self.invalidate_authorization_cache();
        Ok(())
    }

    /// `removeMatch` on the security settings.
    pub fn remove_match(&self, address_match: &str) {
        self.repository
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove_match(address_match);
        self.invalidate_authorization_cache();
    }

    /// `getMatch`: the roles of the most specific setting for `address`.
    pub fn roles(&self, address: &str) -> Vec<Role> {
        self.repository
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get_most_specific(address)
            .map(|s| s.roles.clone())
            .unwrap_or_default()
    }

    /// `getAuthenticationCacheSize`.
    pub fn authentication_cache_size(&self) -> usize {
        self.authentication_cache.as_ref().map_or(0, TtlCache::len)
    }

    /// `getAuthorizationCacheSize`.
    pub fn authorization_cache_size(&self) -> usize {
        self.authorization_cache.as_ref().map_or(0, TtlCache::len)
    }

    pub fn invalidate_authorization_cache(&self) {
        if let Some(c) = &self.authorization_cache {
            c.clear();
        }
    }

    pub fn invalidate_authentication_cache(&self) {
        if let Some(c) = &self.authentication_cache {
            c.clear();
        }
    }

    /// `checkClusterCredentials`: the cluster user logs in with the cluster
    /// password only, and never with the default password.
    fn check_cluster_credentials(
        &self,
        user: Option<&str>,
        password: Option<&str>,
    ) -> ClusterCheck {
        use saranga_config::BrokerConfig as B;
        if user == Some(B::DEFAULT_CLUSTER_USER) && password == Some(B::DEFAULT_CLUSTER_PASSWORD) {
            ClusterCheck::Invalid
        } else if user == Some(self.config.cluster_user.as_str()) {
            if password == Some(self.config.cluster_password.as_str()) {
                ClusterCheck::Valid
            } else {
                ClusterCheck::Invalid
            }
        } else {
            ClusterCheck::Ignore
        }
    }

    /// `authenticate`: the user these credentials validate as. Fails with
    /// `AMQ229031` when they do not.
    pub fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        connection: &ConnectionInfo,
    ) -> Result<Authenticated, SecurityError> {
        if !self.config.enabled {
            return Ok(Authenticated::default());
        }
        match self.check_cluster_credentials(user, password) {
            ClusterCheck::Valid => {
                self.counters
                    .authentication_success
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Authenticated {
                    validated_user: user.map(str::to_owned),
                    subject: None,
                });
            }
            ClusterCheck::Invalid => {
                self.counters
                    .authentication_failure
                    .fetch_add(1, Ordering::Relaxed);
                self.authentication_failed(user, connection);
                let e = unable_to_validate(connection, user, None);
                audit_failed_authentication(connection, &e);
                return Err(e);
            }
            ClusterCheck::Ignore => {}
        }

        let key = authentication_cache_key(user, password, connection);
        let cached = self.authentication_cache.as_ref().and_then(|c| c.get(&key));
        let (subject, validated_user) = match cached {
            Some(subject) => {
                let user = subject
                    .as_deref()
                    .and_then(|s| self.manager.user_from_subject(s));
                (subject, user)
            }
            None => match self.manager.authenticate(user, password, connection) {
                Ok(subject) => {
                    let subject = subject.map(Arc::new);
                    if self.has_user_principal(subject.as_deref()) {
                        if let Some(c) = &self.authentication_cache {
                            c.put(key, subject.clone());
                        }
                        let user = subject
                            .as_deref()
                            .and_then(|s| self.manager.user_from_subject(s));
                        (subject, user)
                    } else {
                        (subject, None)
                    }
                }
                Err(e) => {
                    tracing::debug!("Skipping authentication cache due to exception: {}", e.0);
                    (None, None)
                }
            },
        };
        let Some(validated_user) = validated_user else {
            self.counters
                .authentication_failure
                .fetch_add(1, Ordering::Relaxed);
            self.authentication_failed(user, connection);
            let e = unable_to_validate(connection, user, Some(connection.cert_subject_dn()));
            tracing::warn!("AMQ222216: Security problem while authenticating: {e}");
            audit_failed_authentication(connection, &e);
            return Err(e);
        };
        if audit::resource_enabled() {
            audit::resource(audit::fill(
                "AMQ601715: User {} successfully authenticated on connection {}",
                &[
                    &audit::caller(subject.as_deref(), Some(&connection.remote_address)),
                    connection.connection_id.as_deref().unwrap_or("null"),
                ],
            ));
        }
        self.counters
            .authentication_success
            .fetch_add(1, Ordering::Relaxed);
        Ok(Authenticated {
            validated_user: Some(validated_user),
            subject,
        })
    }

    /// `authenticate` for a management call made with no user and no
    /// password on a management connection: the subject the call runs as
    /// (`SecurityManagerShim.currentSubject()`, the web console's or
    /// Jolokia's user) is taken as authenticated.
    pub fn authenticate_as(
        &self,
        subject: Arc<Subject>,
        connection: &ConnectionInfo,
    ) -> Authenticated {
        if !self.config.enabled {
            return Authenticated::default();
        }
        let validated_user = self.manager.user_from_subject(&subject);
        if audit::resource_enabled() {
            audit::resource(audit::fill(
                "AMQ601715: User {} successfully authenticated on connection {}",
                &[
                    &audit::caller(Some(&subject), Some(&connection.remote_address)),
                    connection.connection_id.as_deref().unwrap_or("null"),
                ],
            ));
        }
        self.counters
            .authentication_success
            .fetch_add(1, Ordering::Relaxed);
        Authenticated {
            validated_user,
            subject: Some(subject),
        }
    }

    /// `validateExpectedUserPrincipal`: a subject must name its user.
    fn has_user_principal(&self, subject: Option<&Subject>) -> bool {
        match subject {
            None => true,
            Some(s) => {
                let ok = s.user().is_some();
                if !ok {
                    tracing::warn!(
                        "AMQ224141: Illegal implementation(s) of java.security.Principal returned from LoginModule:  {:?}",
                        s.principals()
                    );
                }
                ok
            }
        }
    }

    /// `getSubjectForAuthorization`: the cached subject for the session's
    /// credentials, or a fresh login.
    fn subject_for_authorization(&self, auth: &SecurityAuth) -> Option<Arc<Subject>> {
        if auth.management && auth.username.is_none() && auth.password.is_none() {
            return auth.subject.clone();
        }
        let key = authentication_cache_key(
            auth.username.as_deref(),
            auth.password.as_deref(),
            &auth.connection,
        );
        if let Some(cached) = self.authentication_cache.as_ref().and_then(|c| c.get(&key)) {
            return cached;
        }
        match self.manager.authenticate(
            auth.username.as_deref(),
            auth.password.as_deref(),
            &auth.connection,
        ) {
            Ok(subject) => {
                let subject = subject.map(Arc::new);
                if let Some(c) = &self.authentication_cache {
                    c.put(key, subject.clone());
                }
                subject
            }
            Err(e) => {
                tracing::debug!("Skipping authentication cache due to exception: {}", e.0);
                None
            }
        }
    }

    /// `hasPermission`.
    pub fn has_permission(
        &self,
        address: &str,
        queue: Option<&str>,
        check: CheckType,
        auth: &SecurityAuth,
    ) -> bool {
        if !self.config.enabled {
            return true;
        }
        let user = auth.username.as_deref();
        match self.check_cluster_credentials(user, auth.password.as_deref()) {
            ClusterCheck::Valid => return true,
            ClusterCheck::Invalid => return false,
            ClusterCheck::Ignore => {}
        }
        let bare_address = saranga_config::extract_address_name(address);
        let bare_queue = queue.map(extract_queue_name);
        let fqqn = bare_queue.map(|q| format!("{bare_address}::{q}"));
        let target = fqqn.as_deref().unwrap_or(bare_address);

        if let (Some(user), Some(cache)) = (user, &self.authorization_cache)
            && with_cache_key(user, check, |key| {
                cache.test(key, |granted| granted.contains(target))
            })
        {
            return true;
        }

        let roles = {
            let repo = self.repository.read().unwrap_or_else(|e| e.into_inner());
            let setting = match &fqqn {
                Some(f) if repo.contains_exact_match(f) => repo.get_most_specific(f),
                _ => repo.get_most_specific(bare_address),
            };
            setting.map(|s| s.roles.clone()).unwrap_or_default()
        };
        let Some(subject) = self.subject_for_authorization(auth) else {
            return false;
        };
        let validated = self.manager.authorize(&subject, &roles, check, target);
        if validated && let (Some(user), Some(cache)) = (user, &self.authorization_cache) {
            cache.update(authorization_cache_key(user, check), |set| {
                set.get_or_insert_with(HashSet::new)
                    .insert(target.to_owned());
            });
        }
        validated
    }

    /// `check`: `has_permission`, or `AMQ229032`/`AMQ229213`.
    pub fn check(
        &self,
        address: &str,
        queue: Option<&str>,
        check: CheckType,
        auth: &SecurityAuth,
    ) -> Result<(), SecurityError> {
        if !self.config.enabled {
            return Ok(());
        }
        if self.has_permission(address, queue, check, auth) {
            self.counters
                .authorization_success
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.counters
            .authorization_failure
            .fetch_add(1, Ordering::Relaxed);
        let bare_address = saranga_config::extract_address_name(address);
        let caller = self.caller(auth);
        self.violation(Violation::Permission {
            address: bare_address.to_owned(),
            check,
            user: caller.clone(),
        });
        let e = SecurityError(match queue.map(extract_queue_name) {
            None => format!(
                "AMQ229032: User: {caller} does not have permission='{check}' on address {bare_address}"
            ),
            Some(q) => format!(
                "AMQ229213: User: {caller} does not have permission='{check}' for queue {q} on address {bare_address}"
            ),
        });
        if audit::base_enabled() {
            audit::base(audit::fill(
                "AMQ601264: User {} gets security check failure, reason = {}",
                &[
                    &audit::caller(
                        auth.subject.as_deref(),
                        Some(&auth.connection.remote_address),
                    ),
                    &e.0,
                ],
            ));
        }
        Err(e)
    }

    /// `getCaller`: the user name given, or the subject's user.
    fn caller(&self, auth: &SecurityAuth) -> String {
        auth.username
            .clone()
            .or_else(|| {
                auth.subject
                    .as_deref()
                    .and_then(|s| self.manager.user_from_subject(s))
            })
            .unwrap_or_else(|| "null".to_owned())
    }
}

/// `CompositeAddress.extractQueueName`.
fn extract_queue_name(name: &str) -> &str {
    match name.find("::") {
        Some(i) => &name[i + 2..],
        None => name,
    }
}

fn unable_to_validate(
    connection: &ConnectionInfo,
    user: Option<&str>,
    cert_dn: Option<&str>,
) -> SecurityError {
    // RemotingConnection.getRemoteAddress: SocketAddressUtil.toString,
    // without InetSocketAddress's leading slash.
    let remote = if connection.remote_address.is_empty() {
        "null"
    } else {
        connection.remote_address.trim_start_matches('/')
    };
    SecurityError(format!(
        "AMQ229031: Unable to validate user from {remote}. Username: {}; SSL certificate subject DN: {}",
        user.unwrap_or("null"),
        cert_dn.unwrap_or("null")
    ))
}

/// `createAuthenticationCacheKey`: SHA-256 of user, password and
/// certificate subject, in hex.
fn authentication_cache_key(
    user: Option<&str>,
    password: Option<&str>,
    connection: &ConnectionInfo,
) -> String {
    let mut md = Sha256::new();
    if let Some(u) = user {
        md.update(u.as_bytes());
    }
    md.update([0]);
    if let Some(p) = password {
        md.update(p.as_bytes());
    }
    md.update([0]);
    let dn = connection.cert_subject_dn();
    if dn != CERT_SUBJECT_DN_UNAVAILABLE {
        md.update(dn.as_bytes());
    }
    saranga_config::bytes_to_hex(&md.finalize())
}

fn authorization_cache_key(user: &str, check: CheckType) -> String {
    format!("{user}.{}", check.name())
}

/// [`authorization_cache_key`] made in a buffer kept for the thread, for a
/// lookup each message makes.
fn with_cache_key<R>(user: &str, check: CheckType, f: impl FnOnce(&str) -> R) -> R {
    thread_local! {
        static KEY: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
    }
    KEY.with_borrow_mut(|key| {
        key.clear();
        key.push_str(user);
        key.push('.');
        key.push_str(check.name());
        f(key)
    })
}

/// A bounded map whose entries expire a fixed time after they were written.
struct TtlCache<K, V> {
    capacity: usize,
    ttl: Duration,
    entries: Mutex<HashMap<K, (Instant, V)>>,
}

impl<K: Eq + Hash + Clone, V: Clone> TtlCache<K, V> {
    fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity,
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<K, (Instant, V)>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The entries held, expired ones not yet evicted included
    /// (`estimatedSize`).
    fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the live entry for `key` passes `test`, which sees it in place.
    fn test<Q>(&self, key: &Q, test: impl FnOnce(&V) -> bool) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut entries = self.lock();
        match entries.get(key) {
            Some((at, v)) if at.elapsed() < self.ttl => test(v),
            Some(_) => {
                entries.remove(key);
                false
            }
            None => false,
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        let mut entries = self.lock();
        match entries.get(key) {
            Some((at, v)) if at.elapsed() < self.ttl => Some(v.clone()),
            Some(_) => {
                entries.remove(key);
                None
            }
            None => None,
        }
    }

    fn put(&self, key: K, value: V) {
        let mut entries = self.lock();
        self.make_room(&mut entries, &key);
        entries.insert(key, (Instant::now(), value));
    }

    /// Changes the live value under `key` (`None` if absent or expired),
    /// keeping its write time if it was live.
    fn update(&self, key: K, f: impl FnOnce(&mut Option<V>)) {
        let mut entries = self.lock();
        let live = entries
            .get(&key)
            .filter(|(at, _)| at.elapsed() < self.ttl)
            .map(|(at, v)| (*at, v.clone()));
        let (at, mut value) = match live {
            Some((at, v)) => (at, Some(v)),
            None => (Instant::now(), None),
        };
        f(&mut value);
        match value {
            Some(v) => {
                self.make_room(&mut entries, &key);
                entries.insert(key, (at, v));
            }
            None => {
                entries.remove(&key);
            }
        }
    }

    fn make_room(&self, entries: &mut HashMap<K, (Instant, V)>, key: &K) {
        if entries.contains_key(key) || entries.len() < self.capacity {
            return;
        }
        entries.retain(|_, (at, _)| at.elapsed() < self.ttl);
        while entries.len() >= self.capacity {
            let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, (at, _))| *at)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            entries.remove(&oldest);
        }
    }

    fn clear(&self) {
        self.lock().clear();
    }
}

/// `userFailedAuthenticationInAudit`: the failure, for no subject, from the
/// connection's address.
fn audit_failed_authentication(connection: &ConnectionInfo, e: &SecurityError) {
    if audit::resource_enabled() {
        audit::resource(audit::fill(
            "AMQ601716: User {} failed authentication on connection {}, reason: {}",
            &[
                &audit::caller(None, Some(&connection.remote_address)),
                connection.connection_id.as_deref().unwrap_or("null"),
                &e.0,
            ],
        ));
    }
}

#[cfg(test)]
mod tests;
