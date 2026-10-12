//! The broker's acceptors (`RemotingServiceImpl`'s, as `AcceptorControl`
//! reaches them): each listens under its name until management stops it,
//! and `reload` binds it again with its TLS stores read again.

use std::sync::{Arc, Mutex};

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
        let running = Arc::new(Running {
            name: name.to_owned(),
            address: address.to_owned(),
            acceptor: Arc::new(acceptor),
            listening: Mutex::new(None),
            closing: Arc::new(tokio::sync::Notify::new()),
        });
        self.listen(&running).await?;
        self.acceptors
            .running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(running);
        Ok(())
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
        self.listen(&r).await.map_err(|e| failed_to_bind(&r, &e))
    }

    /// `AcceptorControl.stop`: ACCEPTOR_STOPPED, then no more connections,
    /// and those it accepted are closed.
    pub async fn stop_acceptor(&self, name: &str) -> Result<(), String> {
        let r = self.acceptors.find(name).ok_or_else(|| unknown(name))?;
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
        if !unbind(&r).await {
            return Ok(());
        }
        r.closing.notify_waiters();
        for c in self.registry().connections() {
            if c.acceptor() == Some(name) {
                c.fail();
            }
        }
        Ok(())
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
            let listener = bind(&r.address).await.map_err(|e| failed_to_bind(&r, &e))?;
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

/// `ActiveMQMessageBundle.failedToBind`.
fn failed_to_bind(r: &Running, e: &std::io::Error) -> String {
    format!(
        "AMQ229230: Failed to bind acceptor {} to {}: {e}",
        r.name, r.address
    )
}
