//! Connection routers (`ConnectionRouterManager`, `ConnectionRouter`): an
//! acceptor with a `router` parameter has each client connection it
//! takes, once authenticated, routed by a key (its client ID, user, role,
//! address or TLS SNI host): kept on this broker, redirected to another,
//! or refused. The protocols redirect natively (Core, AMQP, MQTT,
//! OpenWire); the `ConnectionRouterControl` answers for the others.

pub mod cache;
pub mod policy;
pub mod pool;
pub mod store;
pub mod target;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use saranga_buffer::json::Json;
use saranga_config::configuration::ConnectionRouterConfiguration;

use crate::Broker;
use crate::session::ClientConnection;
use cache::Cache;
use policy::Policy;
use pool::{Pool, Source};
use target::Target;

/// `ConnectionRouter.CLIENT_ID_PREFIX`: a client ID the router keeps on
/// this broker, as remote targets' management sessions have.
pub const CLIENT_ID_PREFIX: &str = "$.artemis.internal.router.client.";

/// `ConnectionRouterManager.CACHE_ID_PREFIX`.
pub const CACHE_ID_PREFIX: &str = "$.BC.";

/// `KeyResolver.NULL_KEY_VALUE`.
pub const NULL_KEY: &str = "NULL";

/// `KeyType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    ClientId,
    SniHost,
    SourceIp,
    UserName,
    RoleName,
}

impl KeyType {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "CLIENT_ID" => Self::ClientId,
            "SNI_HOST" => Self::SniHost,
            "SOURCE_IP" => Self::SourceIp,
            "USER_NAME" => Self::UserName,
            "ROLE_NAME" => Self::RoleName,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::ClientId => "CLIENT_ID",
            Self::SniHost => "SNI_HOST",
            Self::SourceIp => "SOURCE_IP",
            Self::UserName => "USER_NAME",
            Self::RoleName => "ROLE_NAME",
        }
    }
}

/// A regular expression and its source (`Pattern.pattern`).
#[derive(Clone)]
struct Pattern {
    source: String,
    /// `Matcher.find`. Java's patterns have lookarounds and
    /// backreferences, which `fancy-regex` takes too.
    find: fancy_regex::Regex,
    /// `Matcher.matches`: the whole input.
    whole: fancy_regex::Regex,
}

impl Pattern {
    /// `Pattern.compile`; `None` for a blank expression.
    fn compile(source: Option<&str>) -> Result<Option<Self>, String> {
        let Some(source) = source.filter(|s| !s.trim().is_empty()) else {
            return Ok(None);
        };
        let err = |e: fancy_regex::Error| format!("java.util.regex.PatternSyntaxException: {e}");
        Ok(Some(Self {
            source: source.to_owned(),
            find: fancy_regex::Regex::new(source).map_err(err)?,
            whole: fancy_regex::Regex::new(&format!("^(?:{source})$")).map_err(err)?,
        }))
    }
}

/// What a connection is routed by.
pub struct Request<'a> {
    pub connection: &'a ClientConnection,
    pub client_id: Option<&'a str>,
    /// The validated user, else the one the client gave.
    pub username: Option<&'a str>,
    /// The subject the connection authenticated as, for its roles.
    pub subject: Option<&'a saranga_security::jaas::Subject>,
}

/// `TargetResult`.
#[derive(Clone)]
pub enum TargetResult {
    Ok(Arc<Target>),
    /// `REFUSED_UNAVAILABLE`: the pool is not ready, possibly for a while.
    RefusedUnavailable,
    /// `REFUSED_USE_ANOTHER`: go elsewhere.
    RefusedUseAnother,
}

/// `KeyResolver`.
pub struct KeyResolver {
    key: KeyType,
    filter: RwLock<Option<Pattern>>,
}

impl KeyResolver {
    fn filter(&self) -> Option<Pattern> {
        self.filter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `getKeyFilter`.
    pub fn key_filter(&self) -> Option<String> {
        self.filter().map(|p| p.source)
    }

    /// `setKeyFilter`.
    pub fn set_key_filter(&self, source: Option<&str>) -> Result<(), String> {
        let p = Pattern::compile(source)?;
        *self.filter.write().unwrap_or_else(|e| e.into_inner()) = p;
        Ok(())
    }

    /// `resolve`: the key's value, through the filter's first match; `NULL`
    /// for none.
    pub fn resolve(&self, r: &Request<'_>) -> String {
        let filter = self.filter();
        let value: Option<String> = match self.key {
            KeyType::ClientId => r.client_id.map(str::to_owned),
            KeyType::SniHost => r.connection.security_info().sni_host.clone(),
            KeyType::SourceIp => {
                // `getRemoteAddress`, `/127.0.0.1:51234`: the address.
                let address = r.connection.security_info().remote_address.clone();
                let prefixed = address.starts_with('/');
                let delimiter = address.rfind(':').filter(|i| *i > 0);
                if prefixed || delimiter.is_some() {
                    let start = usize::from(prefixed);
                    let end = delimiter.unwrap_or(address.len());
                    Some(address.get(start..end).unwrap_or_default().to_owned())
                } else {
                    Some(address)
                }
            }
            KeyType::UserName => r.username.map(str::to_owned),
            KeyType::RoleName => {
                // The first role the filter finds a match in, or with no
                // filter the first role, taken as it is.
                for role in r.subject.into_iter().flat_map(|s| s.roles()) {
                    match &filter {
                        Some(p) => {
                            if let Ok(Some(m)) = p.find.find(role) {
                                tracing::debug!("role match for {role} via {}", p.source);
                                return m.as_str().to_owned();
                            }
                        }
                        None => {
                            tracing::debug!("first role match: {role}");
                            return role.to_owned();
                        }
                    }
                }
                None
            }
        };
        tracing::debug!("keyValue for {}: {value:?}", self.key.name());
        match (value, filter) {
            (None, _) => NULL_KEY.to_owned(),
            (Some(v), None) => v,
            (Some(v), Some(p)) => match p.find.find(&v) {
                Ok(Some(m)) => m.as_str().to_owned(),
                _ => NULL_KEY.to_owned(),
            },
        }
    }
}

/// `ConnectionRouter`.
pub struct ConnectionRouter {
    name: String,
    resolver: KeyResolver,
    local: Arc<Target>,
    local_filter: RwLock<Option<Pattern>>,
    pool: Option<Arc<Pool>>,
    policy: Option<Arc<Policy>>,
    cache: Option<Cache>,
    started: std::sync::atomic::AtomicBool,
}

impl ConnectionRouter {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_type(&self) -> KeyType {
        self.resolver.key
    }

    pub fn resolver(&self) -> &KeyResolver {
        &self.resolver
    }

    pub fn local_target(&self) -> &Arc<Target> {
        &self.local
    }

    pub fn pool(&self) -> Option<&Arc<Pool>> {
        self.pool.as_ref()
    }

    pub fn policy(&self) -> Option<&Arc<Policy>> {
        self.policy.as_ref()
    }

    pub fn cache(&self) -> Option<&Cache> {
        self.cache.as_ref()
    }

    /// `getLocalTargetFilter`.
    pub fn local_target_filter(&self) -> Option<String> {
        self.local_filter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|p| p.source.clone())
    }

    /// `setLocalTargetFilter`.
    pub fn set_local_target_filter(&self, source: Option<&str>) -> Result<(), String> {
        let p = Pattern::compile(source)?;
        *self.local_filter.write().unwrap_or_else(|e| e.into_inner()) = p;
        Ok(())
    }

    /// `start`: the local target learns the node ID, then the cache and
    /// the pool start.
    fn start(&self) -> Result<(), String> {
        self.local.connect_local();
        if let Some(c) = &self.cache {
            c.start();
        }
        if let Some(p) = &self.pool {
            p.start(self.local.node_id())?;
        }
        self.started
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn stop(&self) {
        self.started
            .store(false, std::sync::atomic::Ordering::Release);
        if let Some(p) = &self.pool {
            p.stop();
        }
        if let Some(c) = &self.cache {
            c.stop();
        }
    }

    /// `getTarget(Connection, clientID, username)`.
    pub async fn route(&self, r: &Request<'_>) -> TargetResult {
        if !self.started.load(std::sync::atomic::Ordering::Acquire) {
            return TargetResult::RefusedUnavailable;
        }
        if r.client_id.is_some_and(|c| c.starts_with(CLIENT_ID_PREFIX)) {
            tracing::debug!(
                "The clientID [{:?}] starts with ConnectionRouter.CLIENT_ID_PREFIX",
                r.client_id
            );
            return TargetResult::Ok(self.local.clone());
        }
        let key = self.resolver.resolve(r);
        self.target_for(key).await
    }

    /// `getTarget(key)`, as management asks it.
    pub async fn target(&self, key: &str) -> TargetResult {
        if !self.started.load(std::sync::atomic::Ordering::Acquire) {
            return TargetResult::RefusedUnavailable;
        }
        self.target_for(key.to_owned()).await
    }

    async fn target_for(&self, mut key: String) -> TargetResult {
        let key_type = self.resolver.key.name();
        if let Some(p) = &self.policy
            && key != NULL_KEY
        {
            key = p.transform_key(&key);
        }
        let local_filter = self
            .local_filter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(f) = local_filter
            && f.whole.is_match(&key).unwrap_or(false)
        {
            tracing::debug!(
                "The {key_type}[{key}] matches the localTargetFilter {}",
                f.source
            );
            return TargetResult::Ok(self.local.clone());
        }
        let (Some(policy), Some(pool)) = (&self.policy, &self.pool) else {
            return TargetResult::RefusedUseAnother;
        };
        if let Some(cache) = &self.cache {
            let node_id = cache.get(&key);
            tracing::debug!("The cache returns target [{node_id:?}] for {key_type}[{key}]");
            if let Some(node_id) = node_id {
                if let Some(t) = pool.ready_target(&node_id).await {
                    tracing::debug!("The target [{node_id}] is ready for {key_type}[{key}]");
                    return TargetResult::Ok(t);
                }
                tracing::debug!("The target [{node_id}] is not ready for {key_type}[{key}]");
            }
        }
        let targets = pool.targets().await;
        let target = policy.select_target(&targets, &key);
        match target {
            Some(t) => {
                if let Some(cache) = &self.cache {
                    tracing::debug!("Caching {key_type}[{key}] for [{t}]");
                    cache.put(&key, &t.node_id().unwrap_or_else(|| "null".to_owned()));
                }
                TargetResult::Ok(t)
            }
            None => TargetResult::RefusedUnavailable,
        }
    }

    /// `ConnectionRouterControl.getTargetAsJSON`.
    pub async fn target_json(&self, key: &str) -> Json {
        match self.target(key).await {
            TargetResult::Ok(t) => Json::from(
                Json::object()
                    .with("nodeID", t.node_id())
                    .with("local", t.is_local())
                    .with("connector", t.connector_json())
                    .to_string(),
            ),
            _ => Json::Null,
        }
    }

    /// `ConnectionRouterControl.getTarget`, as Jolokia shows its
    /// `CompositeData`: the connector's parameters a map of strings.
    pub async fn target_composite(&self, key: &str) -> Json {
        match self.target(key).await {
            TargetResult::Ok(t) => {
                let connector = match t.connector_json() {
                    Json::Object(fields) => {
                        let mut out = Json::object();
                        for (k, v) in fields {
                            match k.as_str() {
                                "name" | "factoryClassName" => out = out.with(&k, v),
                                "params" => {
                                    let params = match v {
                                        Json::Object(ps) => ps
                                            .into_iter()
                                            .map(|(k, v)| {
                                                let v = match v {
                                                    Json::String(s) => Json::String(s),
                                                    Json::Null => Json::Null,
                                                    other => Json::String(other.to_string()),
                                                };
                                                (k, v)
                                            })
                                            .collect(),
                                        _ => Vec::new(),
                                    };
                                    out = out.with("params", Json::Object(params));
                                }
                                _ => {}
                            }
                        }
                        out
                    }
                    _ => Json::Null,
                };
                Json::object()
                    .with("nodeID", t.node_id())
                    .with("local", t.is_local())
                    .with("connector", connector)
            }
            _ => Json::Null,
        }
    }
}

/// `ConnectionRouterManager`.
#[derive(Default)]
pub struct RouterManager {
    state: std::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
    started: bool,
    configurations: HashMap<String, ConnectionRouterConfiguration>,
    routers: HashMap<String, Arc<ConnectionRouter>>,
}

impl RouterManager {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `deploy`: each configured router, not yet started.
    pub fn deploy(
        &self,
        broker: &Arc<Broker>,
        configs: &[ConnectionRouterConfiguration],
    ) -> Result<(), String> {
        let mut state = self.state();
        for c in configs {
            deploy_router(broker, &mut state, c)?;
        }
        Ok(())
    }

    /// `update`, on a configuration reload: a new or changed router is
    /// (re)deployed, one no longer configured stopped.
    pub fn update(
        &self,
        broker: &Arc<Broker>,
        configs: &[ConnectionRouterConfiguration],
    ) -> Result<(), String> {
        let mut state = self.state();
        for c in configs {
            if state.configurations.get(&c.name) != Some(c) {
                if let Some(router) = state.routers.remove(&c.name) {
                    router.stop();
                }
                deploy_router(broker, &mut state, c)?;
            }
        }
        let removed: Vec<String> = state
            .configurations
            .keys()
            .filter(|n| !configs.iter().any(|c| &c.name == *n))
            .cloned()
            .collect();
        for name in removed {
            state.configurations.remove(&name);
            if let Some(router) = state.routers.remove(&name) {
                router.stop();
            }
        }
        Ok(())
    }

    /// `getRouter`.
    pub fn get(&self, name: &str) -> Option<Arc<ConnectionRouter>> {
        self.state().routers.get(name).cloned()
    }

    /// The routers' names, for management.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.state().routers.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn start(&self) -> Result<(), String> {
        let mut state = self.state();
        state.started = true;
        for r in state.routers.values() {
            r.start()?;
        }
        Ok(())
    }

    pub fn stop(&self) {
        let mut state = self.state();
        state.started = false;
        for r in state.routers.values() {
            r.stop();
        }
    }
}

/// `deployConnectionRouter`.
fn deploy_router(
    broker: &Arc<Broker>,
    state: &mut State,
    config: &ConnectionRouterConfiguration,
) -> Result<(), String> {
    tracing::debug!("Deploying ConnectionRouter {}", config.name);
    let key = KeyType::parse(&config.key_type)
        .ok_or_else(|| format!("Invalid RedirectKey:{}", config.key_type))?;
    let local = Target::local(broker);
    let cache = config.cache_configuration.as_ref().map(|c| {
        Cache::new(
            format!("{CACHE_ID_PREFIX}{}", config.name),
            c.persisted,
            c.timeout,
            broker.key_values().clone(),
        )
    });
    let pool = match &config.pool_configuration {
        Some(p) => Some(deploy_pool(broker, p, &local)?),
        None => None,
    };
    let policy = match &config.policy_configuration {
        Some(p) => {
            let properties: Vec<(String, String)> = p
                .properties
                .java_order()
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let policy = Arc::new(Policy::create(&p.name, &properties)?);
            if let Some(pool) = &pool
                && policy.has_probe()
            {
                pool.add_probe(policy.clone());
            }
            Some(policy)
        }
        None => None,
    };
    let router = Arc::new(ConnectionRouter {
        name: config.name.clone(),
        resolver: KeyResolver {
            key,
            filter: RwLock::new(Pattern::compile(config.key_filter.as_deref())?),
        },
        local,
        local_filter: RwLock::new(Pattern::compile(config.local_target_filter.as_deref())?),
        pool,
        policy,
        cache,
        started: std::sync::atomic::AtomicBool::new(false),
    });
    state
        .configurations
        .insert(config.name.clone(), config.clone());
    state.routers.insert(config.name.clone(), router.clone());
    if state.started {
        router.start()?;
    }
    Ok(())
}

/// `deployPool`: a cluster connection's members, else a discovery group's
/// brokers, else static connectors; with the local target first when
/// enabled.
fn deploy_pool(
    broker: &Arc<Broker>,
    config: &saranga_config::configuration::PoolConfiguration,
    local: &Arc<Target>,
) -> Result<Arc<Pool>, String> {
    let source = if let Some(name) = &config.cluster_connection {
        Source::Cluster(broker.cluster_named(name).cloned())
    } else if let Some(name) = &config.discovery_group_name {
        Source::Discovery(discovery_group(&broker.bridges().targets(), name))
    } else if let Some(names) = &config.static_connectors {
        let targets = broker.bridges().targets();
        let mut connectors = Vec::new();
        for name in names {
            match connector(&targets, name) {
                Some(c) => connectors.push(c),
                // As Artemis logs it, with the wrong value.
                None => tracing::warn!(
                    "Static connector not found: {}",
                    config.local_target_enabled
                ),
            }
        }
        Source::Static(connectors)
    } else {
        return Err("java.lang.IllegalStateException: Pool configuration not valid".to_owned());
    };
    let pool = Pool::new(source, config);
    if config.local_target_enabled {
        pool.add_target(local.clone());
    }
    Ok(pool)
}

/// What became of a connection a router routed.
pub enum Routed {
    /// It stays on this broker.
    Local,
    /// The acceptor names a router there is none of (`AMQ224109`).
    NotFound(String),
    /// Refused: `RefusedUnavailable` or `RefusedUseAnother`.
    Refused {
        router: String,
        result: TargetResult,
    },
    /// To go to another broker.
    Redirect { router: String, target: Arc<Target> },
}

/// `RoutingHandler.route` for a connection whose acceptor names a router.
pub async fn route(broker: &Arc<Broker>, router: &str, r: &Request<'_>) -> Routed {
    let Some(cr) = broker.routers().get(router) else {
        tracing::warn!("AMQ224109: ConnectionRouter {router} not found");
        return Routed::NotFound(router.to_owned());
    };
    let connection = crate::audit::transport_string(r.connection);
    match cr.route(r).await {
        TargetResult::Ok(target) => {
            tracing::info!("AMQ221085: Route {connection} to {target}");
            if target.is_local() {
                Routed::Local
            } else {
                Routed::Redirect {
                    router: router.to_owned(),
                    target,
                }
            }
        }
        result => {
            tracing::info!("AMQ221086: Cannot route {connection}");
            Routed::Refused {
                router: router.to_owned(),
                result,
            }
        }
    }
}

/// The connector of this name, of those bridges and pools name.
fn connector(
    targets: &crate::bridge::Targets,
    name: &str,
) -> Option<saranga_core_protocol::transport::TransportConfiguration> {
    targets
        .connectors
        .iter()
        .find(|c| c.name.to_string_lossy() == name)
        .cloned()
}

fn discovery_group(
    targets: &crate::bridge::Targets,
    name: &str,
) -> Option<saranga_client::discovery::DiscoveryConfig> {
    targets
        .discovery_groups
        .iter()
        .find(|g| g.name == name)
        .cloned()
}
