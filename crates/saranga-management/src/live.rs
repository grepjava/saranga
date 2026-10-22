//! The console's live data: a stream of server-sent events, an hour of
//! history of the counters `/metrics` reads, and the last management
//! notifications.
//!
//! - `GET /events?since=N` is the stream:
//!   - `hello` first, `{"stream", "interval", "notifications", "address",
//!     "last"}`: the name `PUT /events/{stream}` takes, the tick in
//!     milliseconds, whether the user may see notifications, the
//!     notification address (which a notification not about an address
//!     names), and the sequence number of the last notification so far;
//!   - `stats` every tick, `{"time", "views"}`: the views the stream
//!     watches whose value changed since the last tick, all of them after
//!     the views change; each is `{"value"}` or `{"error", "status"}`;
//!   - `notification`, whose id is its sequence number: each management
//!     notification, when the user may consume from the notification
//!     address, after those since `N` (or `Last-Event-ID`) still kept;
//!   - `end`, when the session that opened it ends. The stream does not
//!     keep its session from idling out.
//! - `PUT /events/{stream}` `{"views": {"key": view}}` sets what a stream
//!   watches: `{"kind": "broker"}` (what `GET /broker` answers),
//!   `{"kind": "topology"}` (`listNetworkTopology`), `{"kind":
//!   "attributes", "resource", "names"}` (as `GET .../attributes`) or
//!   `{"kind": "listing", "listing", "query"}` (a page of a listing). Each
//!   is read as the user, through the guard and audit, as a request is.
//! - `GET /history?resource=broker&resource=queue.orders`: the samples of
//!   each resource's gauges the guard lets the user read.
//! - `GET /notifications?since=N`: the last notifications, at most
//!   [`NOTIFICATIONS`].
//!
//! The history samples the broker every [`TICK`] for an hour, and the
//! addresses and queues whose address settings enable metrics (as
//! `/metrics` scrapes them) every [`RESOURCE_TICK`], the first
//! [`MAX_SAMPLED`] of each by name.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use saranga_buffer::PropertyValue;
use saranga_config::CheckType;
use saranga_security::manager::ConnectionInfo;
use saranga_security::store::SecurityAuth;
use saranga_server::management::json::Json;
use saranga_server::notifications::NotificationType;
use saranga_server::postoffice::PostOffice;
use saranga_server::{Broker, Current};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::api::{self, ApiState, ListQuery, Problem, User};
use crate::{Peer, percent_decode};

/// How often a stream sends its views, and the history samples the broker.
pub const TICK: Duration = Duration::from_secs(2);
/// How often the history samples the addresses and queues.
pub const RESOURCE_TICK: Duration = Duration::from_secs(30);
/// How long the history reaches back.
pub const HISTORY: Duration = Duration::from_secs(3600);
/// The most addresses, and queues, the history samples.
pub const MAX_SAMPLED: usize = 1000;
/// The notifications kept for `GET /notifications` and for a stream that
/// comes back.
pub const NOTIFICATIONS: usize = 1000;
/// The most views one stream watches.
const MAX_VIEWS: usize = 64;
/// The most streams open at once.
const MAX_STREAMS: usize = 1000;
/// The most resources one `GET /history` asks for.
const MAX_HISTORY: usize = 100;

/// What the history keeps of the broker: the gauges `/metrics` serves
/// that the overview charts, and its message totals.
const BROKER: &[&str] = &[
    "ConnectionCount",
    "SessionCount",
    "TotalConsumerCount",
    "TotalMessageCount",
    "TotalMessagesAdded",
    "TotalMessagesAcknowledged",
    "AddressMemoryUsage",
    "AddressMemoryUsagePercentage",
    "DiskStoreUsage",
];
/// Of each address: its `/metrics` gauges.
const ADDRESS: &[&str] = &[
    "RoutedMessageCount",
    "UnRoutedMessageCount",
    "AddressSize",
    "NumberOfPages",
];
/// Of each queue: the `/metrics` gauges its page charts.
const QUEUE: &[&str] = &[
    "MessageCount",
    "DeliveringCount",
    "ScheduledCount",
    "ConsumerCount",
    "MessagesAdded",
    "MessagesAcknowledged",
    "MessagesExpired",
    "MessagesKilled",
];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Samples taken at the same times: one series per resource and
/// attribute, `NaN` where a sample is missing.
struct Ring {
    interval: Duration,
    cap: usize,
    times: VecDeque<i64>,
    series: HashMap<String, HashMap<&'static str, VecDeque<f64>>>,
}

impl Ring {
    fn new(interval: Duration) -> Self {
        let cap = (HISTORY.as_millis() / interval.as_millis()) as usize;
        Self {
            interval,
            cap,
            times: VecDeque::with_capacity(cap + 1),
            series: HashMap::new(),
        }
    }

    fn push(&mut self, time: i64, samples: Vec<(String, &'static str, f64)>) {
        self.times.push_back(time);
        let len = self.times.len();
        for (resource, attr, v) in samples {
            self.series
                .entry(resource)
                .or_default()
                .entry(attr)
                .or_insert_with(|| std::iter::repeat_n(f64::NAN, len - 1).collect())
                .push_back(v);
        }
        let full = len > self.cap;
        if full {
            self.times.pop_front();
        }
        self.series.retain(|_, attrs| {
            attrs.retain(|_, s| {
                if s.len() < len {
                    s.push_back(f64::NAN);
                }
                if full {
                    s.pop_front();
                }
                s.iter().any(|v| !v.is_nan())
            });
            !attrs.is_empty()
        });
    }
}

/// A notification, as the feed gives it.
pub(crate) struct Note {
    seq: u64,
    json: Json,
}

/// What a stream watches, by the keys the page gave.
type Views = Arc<Vec<(String, View)>>;

/// An open stream.
struct Watcher {
    user: String,
    views: watch::Sender<Views>,
}

pub(crate) struct Live {
    broker: Arc<Current>,
    broker_ring: Mutex<Ring>,
    resource_ring: Mutex<Ring>,
    notes: Mutex<(u64, VecDeque<Arc<Note>>)>,
    feed: broadcast::Sender<Arc<Note>>,
    streams: Mutex<HashMap<String, Watcher>>,
    /// The post office it hears notifications from.
    listening: Mutex<Weak<PostOffice>>,
}

impl Live {
    /// Starts the sampling.
    pub(crate) fn start(broker: Arc<Current>) -> Arc<Self> {
        let live = Arc::new(Self {
            broker,
            broker_ring: Mutex::new(Ring::new(TICK)),
            resource_ring: Mutex::new(Ring::new(RESOURCE_TICK)),
            notes: Mutex::new((0, VecDeque::with_capacity(NOTIFICATIONS))),
            feed: broadcast::channel(256).0,
            streams: Mutex::new(HashMap::new()),
            listening: Mutex::new(Weak::new()),
        });
        tokio::spawn(sample(Arc::downgrade(&live)));
        live
    }

    /// Hears the notifications of the broker now running, once.
    fn listen(self: &Arc<Self>, broker: &Broker) {
        let po = broker.post_office();
        let mut listening = self.listening.lock().unwrap();
        if listening.upgrade().is_some_and(|p| Arc::ptr_eq(&p, po)) {
            return;
        }
        *listening = Arc::downgrade(po);
        let live = Arc::downgrade(self);
        po.add_notification_listener(Arc::new(move |kind, props| {
            if let Some(l) = live.upgrade() {
                l.note(kind, props);
            }
        }));
    }

    fn note(&self, kind: NotificationType, props: &[(&'static str, PropertyValue)]) {
        let mut properties = Json::object();
        for (k, v) in props {
            properties.put(k, property(v));
        }
        let mut notes = self.notes.lock().unwrap();
        notes.0 += 1;
        let seq = notes.0;
        let note = Arc::new(Note {
            seq,
            json: Json::object()
                .with("seq", seq as i64)
                .with("type", kind.name())
                .with("timestamp", now_ms())
                .with("properties", properties),
        });
        if notes.1.len() == NOTIFICATIONS {
            notes.1.pop_front();
        }
        notes.1.push_back(note.clone());
        let _ = self.feed.send(note);
    }

    /// The notifications kept after `seq`.
    fn since(&self, seq: u64) -> Vec<Arc<Note>> {
        let notes = self.notes.lock().unwrap();
        notes.1.iter().filter(|n| n.seq > seq).cloned().collect()
    }
}

/// A notification's property, as JSON.
fn property(v: &PropertyValue) -> Json {
    match v {
        PropertyValue::Null => Json::Null,
        PropertyValue::Bool(b) => Json::from(*b),
        PropertyValue::Byte(b) => Json::Long(i64::from(*b)),
        PropertyValue::Short(s) => Json::Long(i64::from(*s)),
        PropertyValue::Int(i) => Json::Long(i64::from(*i)),
        PropertyValue::Long(l) => Json::Long(*l),
        PropertyValue::Float(f) => Json::Double(f64::from(*f)),
        PropertyValue::Double(d) => Json::Double(*d),
        PropertyValue::Char(c) => Json::from(String::from_utf16_lossy(&[*c])),
        PropertyValue::String(s) => Json::from(s.to_string_lossy()),
        PropertyValue::Bytes(b) => {
            Json::Array(b.iter().map(|x| Json::Long(i64::from(*x as i8))).collect())
        }
    }
}

/// A gauge's value, as `/metrics` reads it.
async fn gauges(
    broker: &Arc<Broker>,
    resource: &str,
    attributes: &[&'static str],
    out: &mut Vec<(String, &'static str, f64)>,
) {
    for a in attributes {
        if let Ok(v) = broker
            .management()
            .attribute(broker, resource, a, Default::default())
            .await
            && let Some(v) = crate::metrics::number(&v)
        {
            out.push((resource.to_owned(), a, v));
        }
    }
}

/// The sampling, for as long as the console's API is served.
async fn sample(live: Weak<Live>) {
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let every = (RESOURCE_TICK.as_millis() / TICK.as_millis()) as u64;
    let mut n = 0u64;
    loop {
        tick.tick().await;
        let Some(live) = live.upgrade() else { return };
        let broker = live.broker.get();
        live.listen(&broker);
        let mut samples = Vec::new();
        gauges(&broker, "broker", BROKER, &mut samples).await;
        live.broker_ring.lock().unwrap().push(now_ms(), samples);
        if n.is_multiple_of(every) {
            let time = now_ms();
            let po = broker.post_office();
            let mut samples = Vec::new();
            let mut addresses: Vec<String> = po
                .addresses()
                .into_iter()
                .filter(|a| po.settings(&a.name).enable_metrics)
                .map(|a| a.name.to_string())
                .collect();
            addresses.sort();
            for a in addresses.iter().take(MAX_SAMPLED) {
                gauges(&broker, &format!("address.{a}"), ADDRESS, &mut samples).await;
            }
            let mut queues: Vec<String> = po
                .queues()
                .into_iter()
                .filter(|q| po.settings(&q.config().address).enable_metrics)
                .map(|q| q.name().to_string())
                .collect();
            queues.sort();
            for q in queues.iter().take(MAX_SAMPLED) {
                gauges(&broker, &format!("queue.{q}"), QUEUE, &mut samples).await;
            }
            live.resource_ring.lock().unwrap().push(time, samples);
        }
        n += 1;
    }
}

/// What a stream may watch.
#[derive(Clone)]
enum View {
    Broker,
    Topology,
    Attributes {
        resource: String,
        names: Vec<String>,
    },
    Listing {
        operation: &'static str,
        query: ListQuery,
    },
}

fn view(j: &Json) -> Result<View, String> {
    let text = |k: &str| {
        j.get(k)
            .and_then(Json::as_str)
            .ok_or_else(|| format!("a view of this kind needs a {k}"))
    };
    match j.get("kind").and_then(Json::as_str) {
        Some("broker") => Ok(View::Broker),
        Some("topology") => Ok(View::Topology),
        Some("attributes") => {
            let resource = text("resource")?.to_owned();
            let Some(Json::Array(names)) = j.get("names") else {
                return Err("an attributes view needs names".into());
            };
            let names = names
                .iter()
                .map(|n| n.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or("names are strings")?;
            Ok(View::Attributes { resource, names })
        }
        Some("listing") => {
            let listing = text("listing")?;
            let operation = api::list_operation(listing)
                .ok_or_else(|| format!("no such listing: {listing}"))?;
            let query = api::list_query(j.get("query").and_then(Json::as_str))?;
            Ok(View::Listing { operation, query })
        }
        Some(k) => Err(format!("no such kind of view: {k}")),
        None => Err("a view needs a kind".into()),
    }
}

/// A view's value now, as the user.
async fn read(state: &ApiState, u: &User, v: &View) -> Json {
    let value = match v {
        View::Broker => api::status(state, u).await,
        View::Topology => api::broker_json(state, u, "listNetworkTopology", Vec::new()).await,
        View::Attributes { resource, names } => match state.agent.bean_of(resource) {
            None => Err(Problem::new(
                StatusCode::NOT_FOUND,
                format!("no such resource: {resource}"),
            )),
            Some((name, bean)) => {
                let mut out = Json::object();
                for a in names {
                    out.put(a, api::attribute_value(state, u, &name, &bean, a).await);
                }
                Ok(out)
            }
        },
        View::Listing { operation, query } => api::list(state, u, operation, query).await,
    };
    match value {
        Ok(v) => Json::object().with("value", v),
        Err(p) => p.json(),
    }
}

/// Whether the user may consume from the notification address, as a
/// client subscribing to it must.
fn may_see_notifications(state: &ApiState, u: &User) -> bool {
    let broker = state.broker.get();
    let Some(store) = broker.security() else {
        return true;
    };
    let auth = SecurityAuth {
        username: None,
        password: None,
        connection: Arc::new(ConnectionInfo {
            remote_address: format!("/{}", u.caller.remote.as_deref().unwrap_or("")),
            certificates: None,
            security_domain: None,
            connection_id: None,
        }),
        subject: u.caller.subject.clone(),
        management: true,
    };
    store.has_permission(
        &broker.management().config().notification_address,
        None,
        CheckType::Consume,
        &auth,
    )
}

fn since(uri: &Uri, headers: &HeaderMap) -> Result<u64, String> {
    let mut since = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    for pair in uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
    {
        match pair.split_once('=') {
            // A browser coming back sends the last id it saw, after the
            // stream's first `since`.
            Some(("since", v)) => {
                since = since.max(v.parse().map_err(|_| "since is a number")?);
            }
            _ => return Err(format!("unknown parameter {pair}")),
        }
    }
    Ok(since)
}

pub(crate) async fn events(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let u = match api::user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let since = match since(&uri, &headers) {
        Ok(s) => s,
        Err(e) => return api::error(StatusCode::BAD_REQUEST, e),
    };
    let session = state.sessions.session(&headers).map(|(id, _)| id);
    let id = format!("{:032x}", rand::random::<u128>());
    let (views, watched) = watch::channel(Views::default());
    {
        let mut streams = state.live.streams.lock().unwrap();
        if streams.len() >= MAX_STREAMS {
            return api::error(StatusCode::SERVICE_UNAVAILABLE, "too many live streams");
        }
        streams.insert(
            id.clone(),
            Watcher {
                user: u.name.clone(),
                views,
            },
        );
    }
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(stream(state.clone(), u, session, id, since, watched, tx));
    let mut r = Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    let h = r.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert("X-Accel-Buffering", HeaderValue::from_static("no"));
    crate::secure(r)
}

type Tx = mpsc::Sender<Result<Event, Infallible>>;

async fn send(tx: &Tx, kind: &str, data: &Json) -> bool {
    tx.send(Ok(Event::default().event(kind).data(data.to_string())))
        .await
        .is_ok()
}

async fn send_note(tx: &Tx, n: &Note) -> bool {
    tx.send(Ok(Event::default()
        .event("notification")
        .id(n.seq.to_string())
        .data(n.json.to_string())))
        .await
        .is_ok()
}

async fn stream(
    state: Arc<ApiState>,
    u: User,
    session: Option<String>,
    id: String,
    since: u64,
    mut watched: watch::Receiver<Views>,
    tx: Tx,
) {
    run(&state, &u, session, &id, since, &mut watched, &tx).await;
    state.live.streams.lock().unwrap().remove(&id);
}

async fn run(
    state: &ApiState,
    u: &User,
    session: Option<String>,
    id: &str,
    mut seq: u64,
    watched: &mut watch::Receiver<Views>,
    tx: &Tx,
) {
    let mut feed = state.live.feed.subscribe();
    let mut allowed = may_see_notifications(state, u);
    let hello = Json::object()
        .with("stream", id)
        .with("interval", TICK.as_millis() as i64)
        .with("notifications", allowed)
        .with(
            "address",
            state
                .broker
                .get()
                .management()
                .config()
                .notification_address
                .as_str(),
        )
        .with("last", state.live.notes.lock().unwrap().0 as i64);
    if !send(tx, "hello", &hello).await {
        return;
    }
    // What was missed, then what comes.
    let replay = |seq: u64| state.live.since(seq);
    if allowed {
        for n in replay(seq) {
            seq = n.seq;
            if !send_note(tx, &n).await {
                return;
            }
        }
    }
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last: HashMap<String, String> = HashMap::new();
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            changed = watched.changed() => {
                if changed.is_err() {
                    return;
                }
                // Everything again, for the new views.
                last.clear();
                tick.reset_immediately();
            }
            _ = tick.tick() => {
                if session.as_deref().is_some_and(|s| !state.sessions.alive(s)) {
                    send(tx, "end", &Json::object().with("reason", "the session ended")).await;
                    return;
                }
                allowed = may_see_notifications(state, u);
                let views = watched.borrow_and_update().clone();
                last.retain(|k, _| views.iter().any(|(key, _)| key == k));
                let mut changed = Json::object();
                for (key, v) in views.iter() {
                    let value = read(state, u, v).await;
                    let text = value.to_string();
                    if last.get(key) != Some(&text) {
                        last.insert(key.clone(), text);
                        changed.put(key, value);
                    }
                }
                let stats = Json::object().with("time", now_ms()).with("views", changed);
                if !send(tx, "stats", &stats).await {
                    return;
                }
            }
            n = feed.recv() => match n {
                Ok(n) => {
                    if allowed && n.seq > seq {
                        seq = n.seq;
                        if !send_note(tx, &n).await {
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if allowed {
                        for n in replay(seq) {
                            seq = n.seq;
                            if !send_note(tx, &n).await {
                                return;
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

/// `PUT /events/{stream}`: what the stream watches from now on.
pub(crate) async fn watch(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    Path(stream): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(r) = api::marked(&headers) {
        return *r;
    }
    let u = match api::user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let Ok(body) = Json::parse(&String::from_utf8_lossy(&body)) else {
        return api::error(StatusCode::BAD_REQUEST, "the body must be JSON");
    };
    let Some(Json::Object(members)) = body.get("views") else {
        return api::error(StatusCode::BAD_REQUEST, "the body needs views, an object");
    };
    if members.len() > MAX_VIEWS {
        return api::error(
            StatusCode::BAD_REQUEST,
            format!("at most {MAX_VIEWS} views a stream"),
        );
    }
    let mut views = Vec::with_capacity(members.len());
    for (key, j) in members {
        match view(j) {
            Ok(v) => views.push((key.clone(), v)),
            Err(e) => return api::error(StatusCode::BAD_REQUEST, format!("{key}: {e}")),
        }
    }
    let n = views.len();
    let streams = state.live.streams.lock().unwrap();
    match streams.get(&stream) {
        Some(w) if w.user == u.name => {
            w.views.send_replace(Arc::new(views));
            api::ok(&Json::object().with("views", n as i64))
        }
        _ => api::error(StatusCode::NOT_FOUND, "no such stream"),
    }
}

fn number(v: f64) -> Json {
    if v.is_nan() {
        Json::Null
    } else if v.fract() == 0.0 && v.abs() < 9.0e15 {
        Json::Long(v as i64)
    } else {
        Json::Double(v)
    }
}

/// `GET /history?resource=...`.
pub(crate) async fn history(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let u = match api::user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let mut wanted = Vec::new();
    for pair in uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
    {
        match pair.split_once('=') {
            Some(("resource", v)) => wanted.push(percent_decode(&v.replace('+', " "))),
            _ => {
                return api::error(StatusCode::BAD_REQUEST, format!("unknown parameter {pair}"));
            }
        }
    }
    if wanted.len() > MAX_HISTORY {
        return api::error(
            StatusCode::BAD_REQUEST,
            format!("at most {MAX_HISTORY} resources at a time"),
        );
    }
    let mut out = Json::object();
    for r in &wanted {
        let Some((name, bean)) = state.agent.bean_of(r) else {
            out.put(
                r,
                Problem::new(StatusCode::NOT_FOUND, format!("no such resource: {r}")).json(),
            );
            continue;
        };
        let (ring, order) = if r == "broker" {
            (&state.live.broker_ring, BROKER)
        } else if r.starts_with("queue.") {
            (&state.live.resource_ring, QUEUE)
        } else {
            (&state.live.resource_ring, ADDRESS)
        };
        let control = bean.control();
        let ring = ring.lock().unwrap();
        let mut attributes = Json::object();
        let mut denied = Vec::new();
        if let Some(series) = ring.series.get(r) {
            for a in order {
                let Some(values) = series.get(a) else {
                    continue;
                };
                let is = control.attr(a).is_some_and(|i| i.is);
                let getter = format!("{}{a}", if is { "is" } else { "get" });
                if state.agent.may(&u.caller, &name, &getter) {
                    attributes.put(a, Json::Array(values.iter().map(|v| number(*v)).collect()));
                } else {
                    denied.push(Json::from(*a));
                }
            }
        }
        let times = if matches!(&attributes, Json::Object(m) if m.is_empty()) {
            Vec::new()
        } else {
            ring.times.iter().map(|t| Json::Long(*t)).collect()
        };
        out.put(
            r,
            Json::object()
                .with("interval", ring.interval.as_millis() as i64)
                .with("times", Json::Array(times))
                .with("attributes", attributes)
                .with("denied", Json::Array(denied)),
        );
    }
    api::ok(&Json::object().with("history", out))
}

/// `GET /notifications?since=N`.
pub(crate) async fn notifications(
    State(state): State<Arc<ApiState>>,
    Extension(Peer(peer)): Extension<Peer>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let u = match api::user(&state, &headers, peer) {
        Ok(u) => u,
        Err(r) => return *r,
    };
    let since = match since(&uri, &HeaderMap::new()) {
        Ok(s) => s,
        Err(e) => return api::error(StatusCode::BAD_REQUEST, e),
    };
    if !may_see_notifications(&state, &u) {
        return api::error(
            StatusCode::FORBIDDEN,
            "the user may not consume from the notification address",
        );
    }
    let notes = state.live.since(since);
    let last = state.live.notes.lock().unwrap().0;
    api::ok(
        &Json::object()
            .with(
                "data",
                Json::Array(notes.iter().map(|n| n.json.clone()).collect()),
            )
            .with("last", last as i64),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ring_keeps_an_hour_aligned() {
        let mut r = Ring::new(Duration::from_secs(1200));
        assert_eq!(r.cap, 3);
        r.push(1, vec![("a".into(), "X", 1.0)]);
        r.push(2, vec![("a".into(), "X", 2.0), ("b".into(), "Y", 5.0)]);
        let b = &r.series["b"]["Y"];
        assert!(b[0].is_nan() && b[1] == 5.0);
        r.push(3, vec![("b".into(), "Y", 6.0)]);
        r.push(4, vec![("b".into(), "Y", 7.0)]);
        assert_eq!(r.times, [2, 3, 4]);
        assert_eq!(r.series["a"]["X"].len(), 3);
        // Gone once it has no sample left.
        r.push(5, vec![]);
        r.push(6, vec![]);
        assert!(!r.series.contains_key("a"));
        assert_eq!(r.series["b"]["Y"].iter().filter(|v| !v.is_nan()).count(), 1);
    }

    #[test]
    fn views_parse() {
        let j = |s: &str| Json::parse(s).unwrap();
        assert!(matches!(view(&j(r#"{"kind":"broker"}"#)), Ok(View::Broker)));
        assert!(matches!(
            view(&j(r#"{"kind":"listing","listing":"queues","query":"page=2&size=5"}"#)),
            Ok(View::Listing { query, .. }) if query.page == 2 && query.size == 5
        ));
        assert!(matches!(
            view(&j(r#"{"kind":"attributes","resource":"queue.q","names":["MessageCount"]}"#)),
            Ok(View::Attributes { names, .. }) if names == ["MessageCount"]
        ));
        assert!(view(&j(r#"{"kind":"listing","listing":"nope"}"#)).is_err());
        assert!(view(&j(r#"{"kind":"attributes","resource":"q"}"#)).is_err());
        assert!(view(&j(r#"{}"#)).is_err());
    }

    #[test]
    fn history_attributes_exist() {
        for a in BROKER {
            assert!(crate::catalog::BROKER.attr(a).is_some(), "{a}");
        }
        for a in ADDRESS {
            assert!(crate::catalog::ADDRESS.attr(a).is_some(), "{a}");
        }
        for a in QUEUE {
            assert!(crate::catalog::QUEUE.attr(a).is_some(), "{a}");
        }
    }
}
