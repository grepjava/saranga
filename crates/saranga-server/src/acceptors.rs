//! The broker's acceptors (`RemotingServiceImpl`'s, as `AcceptorControl`
//! reaches them): each listens under its name until management stops it,
//! and `reload` binds it again with its TLS stores read again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use saranga_config::TransportParams;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::{Acceptor, Broker, notifications};

/// `NettyAcceptorFactory`, which every acceptor is made by.
pub const FACTORY: &str =
    "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory";

/// The acceptors, in the order they were added.
#[derive(Default)]
pub(crate) struct Acceptors {
    running: Mutex<Vec<Arc<Running>>>,
    /// Every configured acceptor's parameters by name, those Saranga does
    /// not open included (`RemotingServiceImpl.acceptorsConfig`).
    configured: Mutex<HashMap<String, TransportParams>>,
}

/// The acceptors of a configuration read again, for
/// `RemotingServiceImpl.updateAcceptors`.
pub struct AcceptorPlan {
    /// Each configured acceptor's name and parameters, in the order
    /// configured.
    pub configured: Vec<(String, TransportParams)>,
    /// `createAcceptor` for the acceptor of that name: `None` when Saranga
    /// does not open it (with a warning), an error when it cannot be made.
    #[allow(clippy::type_complexity)]
    pub create: Box<dyn Fn(&str) -> Result<Option<NewAcceptor>, String> + Send + Sync>,
}

/// An acceptor made from the configuration.
pub struct NewAcceptor {
    /// Where it binds: `host:port`.
    pub address: String,
    pub acceptor: Acceptor,
    /// `autoStart`: started as it is made, else only through management.
    pub auto_start: bool,
}

struct Running {
    name: String,
    /// Where it binds: `host:port`.
    address: String,
    acceptor: Arc<Acceptor>,
    /// The task accepting connections; `None` while stopped.
    listening: Mutex<Option<JoinHandle<()>>>,
    /// Ends the connections not yet given to a protocol.
    closing: Arc<tokio::sync::Notify>,
}

impl Running {
    fn new(name: &str, address: &str, acceptor: Acceptor) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_owned(),
            address: address.to_owned(),
            acceptor: Arc::new(acceptor),
            listening: Mutex::new(None),
            closing: Arc::new(tokio::sync::Notify::new()),
        })
    }

    fn listening(&self) -> std::sync::MutexGuard<'_, Option<JoinHandle<()>>> {
        self.listening.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Acceptors {
    fn find(&self, name: &str) -> Option<Arc<Running>> {
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|r| r.name == name)
            .cloned()
    }

    fn all(&self) -> Vec<Arc<Running>> {
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn remove(&self, name: &str) -> Option<Arc<Running>> {
        let mut running = self.running.lock().unwrap_or_else(|e| e.into_inner());
        let i = running.iter().position(|r| r.name == name)?;
        Some(running.remove(i))
    }

    fn push(&self, running: Arc<Running>) {
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(running);
    }

    fn configured(&self) -> std::sync::MutexGuard<'_, HashMap<String, TransportParams>> {
        self.configured.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Binds the acceptor's host the way Netty does: to the one address
/// `InetAddress.getByName` gives, which is the first IPv4 address when the
/// name has one (`localhost` is 127.0.0.1, not `::1`).
pub async fn bind(address: &str) -> std::io::Result<TcpListener> {
    let mut addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(address).await?.collect();
    addrs.sort_by_key(|a| !a.is_ipv4());
    match addrs.first() {
        Some(a) => TcpListener::bind(a).await,
        None => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("{address} resolves to no address"),
        )),
    }
}

impl Broker {
    /// Adds an acceptor called `name` and starts it, bound to `address`.
    pub async fn add_acceptor(
        self: &Arc<Self>,
        name: &str,
        address: &str,
        acceptor: Acceptor,
    ) -> std::io::Result<()> {
        let running = Running::new(name, address, acceptor);
        self.listen(&running).await?;
        self.acceptors.push(running);
        Ok(())
    }

    /// Adds an acceptor called `name` without starting it, as an acceptor
    /// with `autoStart=false` is: management can start it.
    pub fn add_stopped_acceptor(&self, name: &str, address: &str, acceptor: Acceptor) {
        self.acceptors.push(Running::new(name, address, acceptor));
    }

    /// The parameters of every configured acceptor, which a reload compares
    /// with those it reads.
    pub fn set_configured_acceptors(&self, configured: Vec<(String, TransportParams)>) {
        *self.acceptors.configured() = configured.into_iter().collect();
    }

    /// `RemotingServiceImpl.updateAcceptors`: an acceptor whose parameters
    /// changed is stopped and made again, one no longer configured is
    /// stopped, and a new one is made; those made are started unless
    /// `autoStart` is false. The first that fails to start is the error,
    /// after every one was tried.
    pub(crate) async fn update_acceptors(
        self: &Arc<Self>,
        plan: AcceptorPlan,
    ) -> Result<(), String> {
        // `Collectors.toMap` by name.
        let mut updated: Vec<(String, TransportParams)> = Vec::new();
        for (name, params) in plan.configured {
            match updated.iter_mut().find(|(n, _)| *n == name) {
                Some(entry) => entry.1 = params,
                None => updated.push((name, params)),
            }
        }
        // Walked as the HashMap of them by name is.
        let names: Vec<i32> = updated
            .iter()
            .map(|(n, _)| saranga_buffer::java_hash::string_hash(n))
            .collect();
        let updated: Vec<(String, TransportParams)> =
            saranga_buffer::java_hash::hash_map_order(&names, None)
                .into_iter()
                .map(|i| updated[i].clone())
                .collect();
        let mut to_stop = Vec::new();
        let mut to_create = Vec::new();
        {
            let mut configured = self.acceptors.configured();
            for (name, params) in &updated {
                match configured.get(name) {
                    None => to_create.push(name.clone()),
                    Some(previous) if !previous.same(params) => {
                        to_create.push(name.clone());
                        to_stop.push(name.clone());
                    }
                    Some(_) => {}
                }
            }
            for name in configured.keys() {
                if !updated.iter().any(|(n, _)| n == name) {
                    to_stop.push(name.clone());
                }
            }
            *configured = updated.iter().cloned().collect();
        }
        // acceptorsToCreate is a HashSet of TransportConfigurations.
        let hashes: Vec<i32> = to_create
            .iter()
            .map(|name: &String| {
                updated
                    .iter()
                    .find(|(n, _)| n == name)
                    .map_or(0, |(n, p)| p.hash_code(n))
            })
            .collect();
        let to_create: Vec<String> = saranga_buffer::java_hash::hash_map_order(&hashes, None)
            .into_iter()
            .map(|i| to_create[i].clone())
            .collect();
        for name in &to_stop {
            // Unregistered from management as it leaves the list.
            if let Some(r) = self.acceptors.remove(name) {
                self.stop_running(&r).await;
            }
        }
        let mut to_start = Vec::new();
        for name in &to_create {
            match (plan.create)(name) {
                Ok(Some(n)) => {
                    let running = Running::new(name, &n.address, n.acceptor);
                    self.acceptors.push(running.clone());
                    if n.auto_start {
                        to_start.push(running);
                    }
                }
                Ok(None) => {}
                Err(e) => tracing::warn!("AMQ222080: Error creating acceptor: {name}\n{e}"),
            }
        }
        let mut first_error = None;
        for r in to_start {
            if let Err(e) = self.listen(&r).await {
                let params = updated
                    .iter()
                    .find(|(n, _)| *n == r.name)
                    .map(|(_, p)| p.params_text())
                    .unwrap_or_default();
                tracing::error!("AMQ224104: Error starting the Acceptor {} {params}", r.name);
                // The IllegalStateException NettyAcceptor.start throws.
                first_error.get_or_insert_with(|| {
                    format!(
                        "java.lang.IllegalStateException: {}\nCaused by: {}",
                        failed_to_bind(&r),
                        bind_exception(&e)
                    )
                });
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `NettyAcceptor.internalStart`: binds and accepts, unless already
    /// listening.
    async fn listen(self: &Arc<Self>, running: &Arc<Running>) -> std::io::Result<()> {
        if running.listening().is_some() {
            return Ok(());
        }
        let listener = bind(&running.address).await?;
        // Artemis's log ID, so log-based tooling keeps working (ADR 0003).
        let names: Vec<&str> = self
            .protocols_for(&running.acceptor)
            .iter()
            .map(|p| p.name())
            .collect();
        tracing::info!(
            "AMQ221020: Started NIO Acceptor at {} for protocols [{}]",
            running.address,
            names.join(",")
        );
        let task = tokio::spawn(self.clone().serve_shared(
            listener,
            running.acceptor.clone(),
            running.closing.clone(),
        ));
        *running.listening() = Some(task);
        Ok(())
    }

    /// The names of the acceptors.
    pub fn acceptor_names(&self) -> Vec<String> {
        self.acceptors
            .all()
            .iter()
            .map(|r| r.name.clone())
            .collect()
    }

    /// The acceptor called `name`, with whether it is started.
    pub fn acceptor(&self, name: &str) -> Option<(Arc<Acceptor>, bool)> {
        let r = self.acceptors.find(name)?;
        let started = r.listening().is_some();
        Some((r.acceptor.clone(), started))
    }

    /// `AcceptorControl.start`.
    pub async fn start_acceptor(self: &Arc<Self>, name: &str) -> Result<(), String> {
        let r = self.acceptors.find(name).ok_or_else(|| unknown(name))?;
        self.listen(&r).await.map_err(|_| failed_to_bind(&r))
    }

    /// `AcceptorControl.stop`: ACCEPTOR_STOPPED, then no more connections,
    /// and those it accepted are closed.
    pub async fn stop_acceptor(&self, name: &str) -> Result<(), String> {
        let r = self.acceptors.find(name).ok_or_else(|| unknown(name))?;
        self.stop_running(&r).await;
        Ok(())
    }

    /// `notifyStop`, `pause` and `stop`: ACCEPTOR_STOPPED, then no more
    /// connections, and those it accepted are closed.
    async fn stop_running(&self, r: &Running) {
        let name = r.name.as_str();
        let host = r.acceptor.host.clone();
        let port = i32::from(r.acceptor.port);
        self.post_office.notify(
            notifications::NotificationType::AcceptorStopped,
            move |_| {
                use saranga_buffer::PropertyValue as V;
                vec![
                    ("factory", notifications::string(Some(FACTORY))),
                    ("host", notifications::string(Some(&host))),
                    ("port", V::Int(port)),
                ]
            },
        );
        if !unbind(r).await {
            return;
        }
        r.closing.notify_waiters();
        for c in self.registry().connections() {
            if c.acceptor() == Some(name) {
                c.fail();
            }
        }
    }

    /// `AcceptorControl.reload` (`NettyAcceptor.reload`): unbinds, reads
    /// the TLS stores again and binds again. Its connections stay.
    pub async fn reload_acceptor(self: &Arc<Self>, name: &str) -> Result<(), String> {
        let r = self.acceptors.find(name).ok_or_else(|| unknown(name))?;
        let was_listening = unbind(&r).await;
        if let Some(tls) = &r.acceptor.tls {
            tls.reload().map_err(|e| e.to_string())?;
        }
        if was_listening {
            let listener = bind(&r.address).await.map_err(|_| failed_to_bind(&r))?;
            let task = tokio::spawn(self.clone().serve_shared(
                listener,
                r.acceptor.clone(),
                r.closing.clone(),
            ));
            *r.listening() = Some(task);
        }
        Ok(())
    }

    /// `RemotingService.pauseAcceptors`: no new connections.
    pub async fn pause_acceptors(&self) {
        for r in self.acceptors.all() {
            unbind(&r).await;
        }
    }

    /// The connections no protocol has taken yet (in their TLS handshake
    /// or protocol detection) are closed, as stopping the acceptors closes
    /// every channel (`RemotingServiceImpl.stop`). The others are failed
    /// through the registry.
    pub fn close_unestablished(&self) {
        for r in self.acceptors.all() {
            r.closing.notify_waiters();
        }
    }
}

/// Stops accepting and closes the listener; whether it was listening.
async fn unbind(r: &Running) -> bool {
    let Some(task) = r.listening().take() else {
        return false;
    };
    task.abort();
    let _ = task.await;
    true
}

fn unknown(name: &str) -> String {
    format!("AMQ229067: Cannot find resource with name acceptor.{name}")
}

/// `ActiveMQMessageBundle.failedToBind`: the message, without its cause.
fn failed_to_bind(r: &Running) -> String {
    format!(
        "AMQ229230: Failed to bind acceptor {} to {}",
        r.name, r.address
    )
}

/// The exception Java's `bind` throws for `e`.
fn bind_exception(e: &std::io::Error) -> String {
    // sun.nio.ch.Net names the call on Windows.
    let call = if cfg!(windows) { ": bind" } else { "" };
    match e.kind() {
        std::io::ErrorKind::AddrInUse => {
            format!("java.net.BindException: Address already in use{call}")
        }
        std::io::ErrorKind::AddrNotAvailable => {
            format!("java.net.BindException: Cannot assign requested address{call}")
        }
        std::io::ErrorKind::PermissionDenied => {
            format!("java.net.SocketException: Permission denied{call}")
        }
        _ => format!("java.net.SocketException: {e}"),
    }
}
