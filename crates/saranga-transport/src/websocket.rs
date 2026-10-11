//! WebSockets on an acceptor, as Artemis's `ProtocolHandler` and
//! `WebSocketServerHandler` run them over Netty 4.1: an HTTP upgrade
//! request, Netty's handshake (`WebSocketServerHandshaker13`), then a byte
//! stream made of the payloads of the client's data frames. What the
//! broker writes goes out as binary (or text) frames of at most the
//! maximum payload length (`WebSocketFrameEncoder`).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// `TransportConstants.DEFAULT_WEB_SOCKET_MAX_FRAME_PAYLOAD_LENGTH`.
pub const DEFAULT_MAX_FRAME_PAYLOAD_LENGTH: usize = 65536;

/// The most of a request's head Saranga reads.
const MAX_REQUEST_HEAD: usize = 64 * 1024;

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// `ProtocolDecoder.isHttp`: the first two bytes of an HTTP method.
pub fn is_http(header: &[u8]) -> bool {
    matches!(
        header.get(..2),
        Some(b"GE" | b"PO" | b"PU" | b"HE" | b"OP" | b"PA" | b"DE" | b"TR")
    )
}

/// An HTTP request's head, and the body `Content-Length` gave.
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub uri: String,
    headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    /// The first value of a header, by case-insensitive name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Whether a comma-separated header holds `token` (`containsValue` with
    /// ignore-case).
    fn header_has_token(&self, name: &str, token: &str) -> bool {
        self.headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .any(|(_, v)| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
    }

    /// Whether the client asks to keep the connection (`isKeepAlive`).
    fn keep_alive(&self) -> bool {
        !self.header_has_token("connection", "close")
    }
}

/// Reads an HTTP request (`HttpRequestDecoder` and `HttpObjectAggregator`).
/// Returns it and the bytes read after it.
pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<(Request, Vec<u8>)> {
    let mut buf = Vec::with_capacity(1024);
    let end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > MAX_REQUEST_HEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request head too long",
            ));
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let mut rest = buf[end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let start = lines.next().unwrap_or_default();
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let uri = parts.next().unwrap_or_default().to_owned();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect::<Vec<_>>();
    let mut request = Request {
        method,
        uri,
        headers,
        body: Vec::new(),
    };
    let length = request
        .header("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    while rest.len() < length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        rest.extend_from_slice(&chunk[..n]);
    }
    request.body = rest.drain(..length).collect();
    Ok((request, rest))
}

/// The outcome of a request to an acceptor.
#[derive(Debug)]
pub enum Upgrade {
    /// The handshake went out: the stream now carries WebSocket frames.
    WebSocket,
    /// The request was answered (or refused) and the connection closes.
    Closed,
}

/// `sendHttpResponse` of a refusal: the status is also its body.
async fn refuse<S: AsyncWrite + Unpin>(stream: &mut S, status: &str) -> io::Result<()> {
    let body = status.as_bytes();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

/// `selectSubprotocol`: the first protocol the client asks for that the
/// broker has.
pub fn select_subprotocol<'a>(requested: &'a str, supported: &[&str]) -> Option<&'a str> {
    requested
        .split(',')
        .map(str::trim)
        .find(|r| supported.iter().any(|s| *s == "*" || s == r))
}

/// `Sec-WebSocket-Accept` for a key.
pub fn accept_key(key: &str) -> String {
    let mut h = Sha1::new();
    h.update(key.as_bytes());
    h.update(GUID.as_bytes());
    base64(&h.finalize())
}

fn base64(b: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// `ProtocolDecoder.channelRead` and `WebSocketServerHandler.handleHttpRequest`:
/// answers an HTTP request with the WebSocket handshake, offering
/// `subprotocols`, or refuses it.
pub async fn handshake<S: AsyncWrite + Unpin>(
    stream: &mut S,
    request: &Request,
    subprotocols: &[&str],
) -> io::Result<Upgrade> {
    let upgrade = request.header("upgrade");
    if upgrade.is_some_and(|u| u.eq_ignore_ascii_case("activemq-remoting")) {
        // ProtocolDecoder: an empty 403.
        stream.write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n").await?;
        stream.flush().await?;
        return Ok(Upgrade::Closed);
    }
    if !upgrade.is_some_and(|u| u.eq_ignore_ascii_case("websocket")) {
        // Core over HTTP (`HttpAcceptorHandler`) is not served.
        tracing::debug!(
            "HTTP request {} {} without a WebSocket upgrade",
            request.method,
            request.uri
        );
        refuse(stream, "403 Forbidden").await?;
        return Ok(Upgrade::Closed);
    }
    if request.method != "GET" {
        refuse(stream, "403 Forbidden").await?;
        return Ok(Upgrade::Closed);
    }
    // WebSocketServerHandshakerFactory.newHandshaker: 13, 8 and 7 share a
    // handshake; anything else is unsupported.
    if !matches!(
        request.header("sec-websocket-version"),
        Some("13" | "8" | "7")
    ) {
        stream
            .write_all(
                b"HTTP/1.1 426 Upgrade Required\r\nsec-websocket-version: 13\r\ncontent-length: 0\r\n\r\n",
            )
            .await?;
        stream.flush().await?;
        return Ok(Upgrade::Closed);
    }
    let Some(key) = request.header("sec-websocket-key") else {
        tracing::debug!("not a WebSocket request: missing key");
        return Ok(Upgrade::Closed);
    };
    if !request.header_has_token("connection", "upgrade") {
        tracing::debug!(
            "not a WebSocket request: a |Connection| header must includes a token 'Upgrade'"
        );
        return Ok(Upgrade::Closed);
    }
    let mut response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: upgrade\r\nsec-websocket-accept: {}\r\n",
        accept_key(key)
    );
    if let Some(requested) = request.header("sec-websocket-protocol") {
        match select_subprotocol(requested, subprotocols) {
            Some(p) => response.push_str(&format!("sec-websocket-protocol: {p}\r\n")),
            None => tracing::debug!("Requested subprotocol(s) not supported: {requested}"),
        }
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    let _ = request.keep_alive();
    Ok(Upgrade::WebSocket)
}

/// How data frames go out (`webSocketEncoderType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderType {
    Binary,
    Text,
}

impl EncoderType {
    /// `WebSocketFrameEncoderType.valueOfType`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "binary" => Some(Self::Binary),
            "text" => Some(Self::Text),
            _ => None,
        }
    }
}

/// The byte stream inside a WebSocket: data frames' payloads in, frames
/// out. Control frames are answered here.
pub struct WebSocketStream<S> {
    inner: S,
    max_payload: usize,
    encoder: EncoderType,
    /// Received and not yet framed.
    raw: Vec<u8>,
    /// Payload not yet read by the protocol.
    data: Vec<u8>,
    data_at: usize,
    /// Frames (and control replies) not yet written.
    out: Vec<u8>,
    out_at: usize,
    /// A close frame came: no more data.
    closed: bool,
    eof: bool,
}

impl<S> WebSocketStream<S> {
    /// `leftover` is what was read after the handshake request.
    pub fn new(inner: S, leftover: Vec<u8>, max_payload: usize, encoder: EncoderType) -> Self {
        Self {
            inner,
            max_payload: max_payload.max(1),
            encoder,
            raw: leftover,
            data: Vec::new(),
            data_at: 0,
            out: Vec::new(),
            out_at: 0,
            closed: false,
            eof: false,
        }
    }
}

fn frame_header(out: &mut Vec<u8>, fin: bool, opcode: u8, len: usize) {
    out.push(if fin { 0x80 } else { 0 } | opcode);
    if len < 126 {
        out.push(len as u8);
    } else if len <= 0xFFFF {
        out.push(126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
}

fn close_frame(out: &mut Vec<u8>, code: u16, reason: &str) {
    frame_header(out, true, 0x8, 2 + reason.len());
    out.extend_from_slice(&code.to_be_bytes());
    out.extend_from_slice(reason.as_bytes());
}

impl<S: AsyncRead + AsyncWrite + Unpin> WebSocketStream<S> {
    /// Writes what is queued. Ready once it is all out.
    fn poll_flush_out(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.out_at < self.out.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.out[self.out_at..]) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(n)) => self.out_at += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.out.clear();
        self.out_at = 0;
        Poll::Ready(Ok(()))
    }

    /// Decodes the frames in `raw`. An error means the connection must
    /// close (after the close frame queued for it).
    fn decode(&mut self) -> io::Result<()> {
        loop {
            let b = &self.raw;
            if b.len() < 2 {
                return Ok(());
            }
            let fin_opcode = b[0];
            let opcode = fin_opcode & 0x0F;
            let masked = b[1] & 0x80 != 0;
            let mut at = 2;
            let len = match b[1] & 0x7F {
                126 => {
                    if b.len() < 4 {
                        return Ok(());
                    }
                    at = 4;
                    u64::from(u16::from_be_bytes([b[2], b[3]]))
                }
                127 => {
                    if b.len() < 10 {
                        return Ok(());
                    }
                    at = 10;
                    u64::from_be_bytes(b[2..10].try_into().expect("8 bytes"))
                }
                n => u64::from(n),
            };
            let violation = |s: &mut Self, reason: &str| {
                close_frame(&mut s.out, 1002, reason);
                s.closed = true;
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    reason.to_owned(),
                ))
            };
            if fin_opcode & 0x70 != 0 {
                return violation(self, "RSV != 0 and no extension negotiated");
            }
            if !masked {
                return violation(self, "unmasked client to server frame");
            }
            if len > self.max_payload as u64 {
                let reason = format!(
                    "Max frame length of {} has been exceeded.",
                    self.max_payload
                );
                close_frame(&mut self.out, 1009, &reason);
                self.closed = true;
                return Err(io::Error::new(io::ErrorKind::InvalidData, reason));
            }
            let len = len as usize;
            if self.raw.len() < at + 4 + len {
                return Ok(());
            }
            let mask: [u8; 4] = self.raw[at..at + 4].try_into().expect("4 bytes");
            at += 4;
            let mut payload: Vec<u8> = self.raw[at..at + len].to_vec();
            for (i, p) in payload.iter_mut().enumerate() {
                *p ^= mask[i % 4];
            }
            self.raw.drain(..at + len);
            match opcode {
                0x0..=0x2 => {
                    if self.data_at == self.data.len() {
                        self.data.clear();
                        self.data_at = 0;
                    }
                    self.data.extend_from_slice(&payload);
                }
                0x8 => {
                    // handshaker.close: the close frame goes back.
                    frame_header(&mut self.out, true, 0x8, payload.len());
                    self.out.extend_from_slice(&payload);
                    self.closed = true;
                    return Ok(());
                }
                0x9 => {
                    frame_header(&mut self.out, true, 0xA, payload.len());
                    self.out.extend_from_slice(&payload);
                }
                0xA => {}
                other => {
                    return violation(self, &format!("unsupported frame type {other}"));
                }
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = &mut *self;
        loop {
            // Pongs and close replies go out as soon as they can.
            if !me.out.is_empty() {
                let _ = me.poll_flush_out(cx);
            }
            if me.data_at < me.data.len() {
                let n = (me.data.len() - me.data_at).min(buf.remaining());
                buf.put_slice(&me.data[me.data_at..me.data_at + n]);
                me.data_at += n;
                return Poll::Ready(Ok(()));
            }
            if me.closed || me.eof {
                if !me.out.is_empty() {
                    return me.poll_flush_out(cx).map(|r| r.map(|_| ()));
                }
                return Poll::Ready(Ok(()));
            }
            if let Err(e) = me.decode() {
                tracing::debug!("WebSocket closed: {e}");
                continue;
            }
            if !me.out.is_empty() {
                let _ = me.poll_flush_out(cx);
            }
            if me.data_at < me.data.len() || me.closed {
                continue;
            }
            let mut chunk = [0u8; 16 * 1024];
            let mut rb = ReadBuf::new(&mut chunk);
            match Pin::new(&mut me.inner).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => {
                    if rb.filled().is_empty() {
                        me.eof = true;
                    } else {
                        me.raw.extend_from_slice(rb.filled());
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = &mut *self;
        if !me.out.is_empty() {
            match me.poll_flush_out(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other.map(|r| r.map(|_| 0)),
            }
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // WebSocketFrameEncoder: a data frame, then continuation frames.
        let first = match me.encoder {
            EncoderType::Binary => 0x2,
            EncoderType::Text => 0x1,
        };
        let mut chunks = buf.chunks(me.max_payload).peekable();
        let mut opcode = first;
        while let Some(c) = chunks.next() {
            frame_header(&mut me.out, chunks.peek().is_none(), opcode, c.len());
            me.out.extend_from_slice(c);
            opcode = 0x0;
        }
        let _ = me.poll_flush_out(cx);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = &mut *self;
        match me.poll_flush_out(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut me.inner).poll_flush(cx),
            other => other,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = &mut *self;
        match me.poll_flush_out(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut me.inner).poll_shutdown(cx),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_of_rfc_6455() {
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn subprotocol_is_the_clients_first_known() {
        let supported = ["v10.stomp", "v11.stomp", "v12.stomp", "mqtt"];
        assert_eq!(
            select_subprotocol("foo, v12.stomp, v11.stomp", &supported),
            Some("v12.stomp")
        );
        assert_eq!(select_subprotocol("foo", &supported), None);
    }

    fn masked(opcode: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
        let mut f = Vec::new();
        frame_header(&mut f, fin, opcode, payload.len());
        f[1] |= 0x80;
        let mask = [1, 2, 3, 4];
        f.extend_from_slice(&mask);
        f.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        f
    }

    #[tokio::test]
    async fn payloads_become_a_stream_and_writes_become_frames() {
        let (client, server) = tokio::io::duplex(1 << 20);
        let mut input = masked(0x2, false, b"CONN");
        input.extend(masked(0x9, true, b"hi"));
        input.extend(masked(0x0, true, b"ECT\n"));
        let mut ws = WebSocketStream::new(server, input, 4, EncoderType::Binary);
        let mut got = [0u8; 8];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"CONNECT\n");
        ws.write_all(b"CONNECTED").await.unwrap();
        ws.flush().await.unwrap();
        drop(ws);
        let mut out = Vec::new();
        let mut client = client;
        client.read_to_end(&mut out).await.unwrap();
        // The pong, then the message in frames of 4 bytes at most.
        assert_eq!(&out[..4], &[0x8A, 2, b'h', b'i']);
        assert_eq!(
            &out[4..],
            &[
                0x02, 4, b'C', b'O', b'N', b'N', 0x00, 4, b'E', b'C', b'T', b'E', 0x80, 1, b'D'
            ]
        );
    }
}
