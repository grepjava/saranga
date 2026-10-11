//! OpenWire, the protocol of ActiveMQ 5 clients, as Artemis serves it.

pub mod codec;
pub mod commands;
mod connection;
mod consumer;
pub mod convert;
pub mod destination;
pub mod primitive;

pub use codec::{CodecError, TransactionId, WireFormat};
pub use commands::DataStructure;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use saranga_server::protocol::{Connection, ProtocolManager, ServeFuture};
use saranga_server::{Acceptor, Broker};

use crate::commands::{DestinationInfo, ProducerId};

/// The OpenWire protocol (`OpenWireProtocolManagerFactory`). Artemis makes
/// one `OpenWireProtocolManager` per acceptor; so does Saranga, on an
/// acceptor's first connection.
#[derive(Default)]
pub struct OpenWireProtocol {
    managers: Mutex<HashMap<(String, u16), Arc<Manager>>>,
}

impl OpenWireProtocol {
    pub fn new() -> Self {
        Self::default()
    }

    fn manager(&self, broker: &Broker, acceptor: &Acceptor) -> Arc<Manager> {
        lock(&self.managers)
            .entry((acceptor.host.clone(), acceptor.port))
            .or_insert_with(|| Arc::new(Manager::new(broker, acceptor)))
            .clone()
    }
}

impl ProtocolManager for OpenWireProtocol {
    fn name(&self) -> &'static str {
        "OPENWIRE"
    }

    /// A size prefix, then a `WireFormatInfo` (type 1) and its magic,
    /// `ActiveMQ`.
    fn is_protocol(&self, header: &[u8]) -> bool {
        header.len() >= 5
            && header[4] == 1
            && header[5..].iter().zip(b"ActiveMQ").all(|(a, b)| a == b)
    }

    fn serve(&self, broker: Arc<Broker>, c: Connection) -> ServeFuture {
        let manager = self.manager(&broker, &c.acceptor);
        Box::pin(connection::serve(broker, manager, c))
    }
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The acceptor parameters `OpenWireProtocolManager` takes as bean
/// properties.
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub max_inactivity_duration: i64,
    pub max_inactivity_duration_initial_delay: i64,
    pub use_keep_alive: bool,
    pub support_advisory: bool,
    pub suppress_internal_management_objects: bool,
    pub destination_cache_size: usize,
    pub use_duplicate_detection_on_failover: bool,
}

/// A `virtualTopicConsumerWildcards` entry: consumers on queues the filter
/// matches read from an FQQN of the topic (`VirtualTopicConfig`).
#[derive(Debug, Clone)]
pub(crate) struct VirtualTopic {
    filter: Vec<String>,
    terminus: usize,
    selector_aware: bool,
}

impl VirtualTopic {
    /// `DestinationFilter.parseFilter(..).matches`: `*` is one segment,
    /// `>` the rest.
    fn matches(&self, paths: &[&str]) -> bool {
        let mut i = 0;
        for (n, f) in self.filter.iter().enumerate() {
            if f == ">" && n == self.filter.len() - 1 {
                return true;
            }
            match paths.get(i) {
                Some(p) if f == "*" || f == p => i += 1,
                _ => return false,
            }
        }
        i == paths.len()
    }
}

/// `OpenWireProtocolManager`: what the connections of one acceptor share.
pub(crate) struct Manager {
    pub settings: Settings,
    pub broker_name: String,
    /// The advisories' `originBrokerId`, unlike `BrokerInfo`'s node ID.
    pub advisory_broker_id: String,
    pub advisory_producer: ProducerId,
    advisory_sequence: AtomicI64,
    /// Client ID to (connection, its remote address) (`clientIdSet`).
    client_ids: Mutex<HashMap<String, (u64, String)>>,
    /// The connections in the order they logged in, with their temporary
    /// destinations (`connections`, `getTemporaryDestinations`).
    connections: Mutex<Vec<(u64, TempList)>>,
    next_connection: AtomicU64,
    virtual_topics: Vec<VirtualTopic>,
}

pub(crate) type TempList = Arc<Mutex<Vec<DestinationInfo>>>;

/// `URISupport`: acceptor parameters arrive URL-encoded.
fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Some(v) = std::str::from_utf8(&b[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `InetAddressUtil.getLocalHostName()`.
fn local_host_name() -> Option<String> {
    for k in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(h) = std::env::var(k)
            && !h.is_empty()
        {
            return Some(h);
        }
    }
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
}

/// The ActiveMQ 5 `IdGenerator`: `ID:<host>-<port>-<start>-<n>:<instance>`.
fn generated_id(host: &str, port: u16) -> String {
    static INSTANCE: AtomicU64 = AtomicU64::new(0);
    let start = saranga_server::message::now_millis();
    let n = INSTANCE.fetch_add(1, Ordering::SeqCst) + 1;
    format!("ID:{host}-{port}-{start}-1:{n}:1")
}

impl Manager {
    fn new(broker: &Broker, acceptor: &Acceptor) -> Self {
        let p = |k: &str| acceptor.params.get(k).map(|v| decode(v));
        let long = |k: &str, d: i64| p(k).and_then(|v| v.trim().parse().ok()).unwrap_or(d);
        let flag = |k: &str, d: bool| p(k).map(|v| v.eq_ignore_ascii_case("true")).unwrap_or(d);
        let settings = Settings {
            max_inactivity_duration: long("maxInactivityDuration", 30_000),
            max_inactivity_duration_initial_delay: long("maxInactivityDurationInitalDelay", 10_000),
            use_keep_alive: flag("useKeepAlive", true),
            support_advisory: flag("supportAdvisory", true),
            suppress_internal_management_objects: flag("suppressInternalManagementObjects", true),
            destination_cache_size: long("openWireDestinationCacheSize", 16).max(1) as usize,
            use_duplicate_detection_on_failover: flag(
                "openwireUseDuplicateDetectionOnFailover",
                true,
            ),
        };
        let mut virtual_topics = Vec::new();
        if let Some(v) = p("virtualTopicConsumerWildcards") {
            for filter in v.split(',') {
                let cfg: Vec<&str> = filter.split(';').collect();
                let Some(terminus) = cfg.get(1).and_then(|t| t.trim().parse().ok()) else {
                    tracing::warn!("invalid virtualTopicConsumerWildcards entry {filter}");
                    continue;
                };
                let selector_aware = cfg[2..].iter().any(|o| {
                    o.split_once('=').is_some_and(|(k, v)| {
                        k == "selectorAware" && v.eq_ignore_ascii_case("true")
                    })
                });
                virtual_topics.push(VirtualTopic {
                    filter: cfg[0].split('.').map(str::to_owned).collect(),
                    terminus,
                    selector_aware,
                });
            }
        }
        let host = local_host_name();
        let broker_name = p("brokerName").unwrap_or_else(|| match &host {
            Some(h) => h.to_lowercase(),
            None => broker.node_id().to_owned(),
        });
        let host = host.unwrap_or_else(|| "localhost".into());
        Self {
            settings,
            broker_name,
            advisory_broker_id: generated_id(&host, acceptor.port),
            advisory_producer: ProducerId {
                connection_id: Some(generated_id(&host, acceptor.port)),
                session_id: 0,
                value: 0,
            },
            advisory_sequence: AtomicI64::new(0),
            client_ids: Mutex::default(),
            connections: Mutex::default(),
            next_connection: AtomicU64::new(1),
            virtual_topics,
        }
    }

    pub fn next_connection(&self) -> u64 {
        self.next_connection.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_advisory_sequence(&self) -> i64 {
        self.advisory_sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// `addConnection`'s client ID check: the remote address of the
    /// connection that has it, if another does.
    pub fn claim_client_id(
        &self,
        client_id: &str,
        connection: u64,
        remote: &str,
        failover: bool,
    ) -> Result<(), String> {
        let mut ids = lock(&self.client_ids);
        if let Some((_, from)) = ids.get(client_id)
            && !failover
        {
            return Err(from.clone());
        }
        ids.insert(client_id.to_owned(), (connection, remote.to_owned()));
        Ok(())
    }

    pub fn add_connection(&self, connection: u64, temp: TempList) {
        lock(&self.connections).push((connection, temp));
    }

    /// `removeConnection`: the client ID only if it is still this
    /// connection's.
    pub fn remove_connection(&self, client_id: Option<&str>, connection: u64) {
        if let Some(id) = client_id {
            let mut ids = lock(&self.client_ids);
            if ids.get(id).is_some_and(|(c, _)| *c == connection) {
                ids.remove(id);
            }
        }
        lock(&self.connections).retain(|(c, _)| *c != connection);
    }

    /// Every connection's temporary destinations, in connection order.
    pub fn temporary_destinations(&self) -> Vec<DestinationInfo> {
        let lists: Vec<TempList> = lock(&self.connections)
            .iter()
            .map(|(_, l)| l.clone())
            .collect();
        lists.iter().flat_map(|l| lock(l).clone()).collect()
    }

    /// `virtualTopicConsumerToFQQN`: a queue a consumer wildcard matches
    /// becomes `<topic>::<queue>`.
    pub fn virtual_topic_fqqn(&self, physical_name: &str) -> Option<(String, bool)> {
        let paths: Vec<&str> = physical_name.split('.').collect();
        let vt = self.virtual_topics.iter().find(|v| v.matches(&paths))?;
        let topic = paths.get(vt.terminus..).unwrap_or_default().join(".");
        Some((format!("{topic}::{physical_name}"), vt.selector_aware))
    }
}
