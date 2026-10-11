//! Protocols on an acceptor (`ProtocolHandler`, `ProtocolManager`). The
//! first 8 bytes a client sends pick the protocol among those the acceptor
//! enables; with no match the connection is Core, as in Artemis.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use saranga_security::ConnectionInfo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::{Acceptor, Broker};

/// A connection's byte stream: TCP, or TLS over TCP.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

/// A stream that first gives back the bytes read to detect the protocol.
pub struct Rewind {
    prefix: Vec<u8>,
    pos: usize,
    inner: Box<dyn Stream>,
}

impl Rewind {
    pub fn new(prefix: Vec<u8>, inner: Box<dyn Stream>) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl AsyncRead for Rewind {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.remaining());
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            if self.pos == self.prefix.len() {
                self.prefix = Vec::new();
                self.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Rewind {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// A client connection handed to the protocol that recognised it.
pub struct Connection {
    /// The stream, starting with the bytes the protocol was detected from.
    pub stream: Rewind,
    pub peer: Option<SocketAddr>,
    /// What security sees of the connection.
    pub info: ConnectionInfo,
    pub acceptor: Arc<Acceptor>,
}

pub type ServeFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A protocol the broker serves (`ProtocolManagerFactory` and
/// `ProtocolManager`).
pub trait ProtocolManager: Send + Sync + 'static {
    /// The name acceptors list it under in `protocols`: `CORE`, `AMQP`,
    /// `STOMP`, `MQTT`, `OPENWIRE`, `HORNETQ`.
    fn name(&self) -> &'static str;

    /// `isProtocol`: whether the first 8 bytes a client sent are this
    /// protocol's.
    fn is_protocol(&self, header: &[u8]) -> bool;

    /// `acceptsNoHandshake`: whether a client that matches no protocol is
    /// this one's.
    fn accepts_no_handshake(&self) -> bool {
        false
    }

    /// Serves the connection until it closes.
    fn serve(&self, broker: Arc<Broker>, connection: Connection) -> ServeFuture;

    /// Called once the broker has loaded its journals
    /// (`loadProtocolServices`): the protocol loads its state and starts
    /// its periodic tasks.
    fn start(&self, _broker: &Arc<Broker>) {}
}

/// The Core protocol (`CoreProtocolManager`).
pub struct CoreProtocol;

impl ProtocolManager for CoreProtocol {
    fn name(&self) -> &'static str {
        "CORE"
    }

    /// `isArtemis`: the `ARTEMIS` handshake.
    fn is_protocol(&self, header: &[u8]) -> bool {
        header.starts_with(saranga_core_protocol::HANDSHAKE)
    }

    fn serve(&self, broker: Arc<Broker>, c: Connection) -> ServeFuture {
        Box::pin(crate::core_protocol::serve_stream(
            broker, c.stream, c.peer, c.info, c.acceptor,
        ))
    }
}

/// `ProtocolHandler.ProtocolDecoder.decode`: waits for 8 bytes, then hands
/// the connection to the first enabled protocol that recognises them, or
/// to the one that takes clients without a handshake, or to Core.
pub async fn detect_and_serve(
    broker: Arc<Broker>,
    mut stream: Box<dyn Stream>,
    peer: Option<SocketAddr>,
    info: ConnectionInfo,
    acceptor: Arc<Acceptor>,
) {
    let mut header = Vec::with_capacity(8);
    let read = async {
        while header.len() < 8 {
            let mut chunk = [0u8; 8];
            let want = 8 - header.len();
            match stream.read(&mut chunk[..want]).await {
                Ok(0) | Err(_) => return false,
                Ok(n) => header.extend_from_slice(&chunk[..n]),
            }
        }
        true
    };
    // `handshake-timeout`: a client that sends too little is closed.
    let complete = match acceptor.handshake_timeout {
        Some(t) => tokio::time::timeout(t, read).await.unwrap_or(false),
        None => read.await,
    };
    if !complete {
        tracing::debug!(?peer, "connection closed before its protocol was known");
        return;
    }
    let enabled = broker.protocols_for(&acceptor);
    let manager = enabled
        .iter()
        .find(|p| p.is_protocol(&header))
        .or_else(|| enabled.iter().find(|p| p.accepts_no_handshake()))
        .or_else(|| enabled.iter().find(|p| p.name() == "CORE"))
        .cloned();
    let Some(manager) = manager else {
        let names: Vec<&str> = enabled.iter().map(|p| p.name()).collect();
        tracing::error!(
            "AMQ224096: Error setting up connection from {} to {}:{}; protocol CORE not found in map: [{}]",
            info.remote_address,
            acceptor.host,
            acceptor.port,
            names.join(", ")
        );
        return;
    };
    let connection = Connection {
        stream: Rewind::new(header, stream),
        peer,
        info,
        acceptor,
    };
    manager.serve(broker, connection).await;
}

/// `TransportConstants.DEFAULT_HANDSHAKE_TIMEOUT`, in seconds.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
