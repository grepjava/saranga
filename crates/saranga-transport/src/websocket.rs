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
/// An upgrade has no use for a body, and this is read before any login.
const MAX_REQUEST_BODY: usize = 64 * 1024;
/// How many times a frame's maximum payload it may inflate to: more than
/// real messages compress by, and well under deflate's limit of about 1,000.
const MAX_INFLATION: usize = 64;

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
    if length > MAX_REQUEST_BODY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "HTTP request body too long",
        ));
    }
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
    /// The handshake went out: the stream now carries WebSocket frames,
    /// compressed as the extension agreed on says.
    WebSocket(Option<Extension>),
    /// The request was answered (or refused) and the connection closes.
    Closed,
}

/// A compression extension agreed on in the handshake, as Netty's
/// `WebSocketServerCompressionHandler` offers them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Extension {
    /// `permessage-deflate` (RFC 7692): a message's frames are one deflate
    /// stream, its first frame marked RSV1.
    PerMessageDeflate,
    /// `deflate-frame` or `x-webkit-deflate-frame`, by the name the client
    /// gave: each frame compressed on its own, all marked RSV1.
    DeflateFrame(String),
}

/// An extension's parameters, in the order given; a bare one's value is
/// `None`.
type Parameters = Vec<(String, Option<String>)>;

/// `WebSocketExtensionUtil.extractExtensions`: each extension's name and
/// parameters.
fn extract_extensions(header: &str) -> Vec<(String, Parameters)> {
    // Java's split drops trailing empty strings.
    fn split(s: &str, sep: char) -> Vec<&str> {
        let mut parts: Vec<&str> = s.split(sep).collect();
        while parts.len() > 1 && parts.last().is_some_and(|p| p.is_empty()) {
            parts.pop();
        }
        parts
    }
    split(header, ',')
        .into_iter()
        .map(|raw| {
            let parts = split(raw, ';');
            let name = parts[0].trim().to_owned();
            let mut params: Parameters = Vec::new();
            for p in &parts[1..] {
                // ^([^=]+)(=["]?([^"]+)["]?)?$, into a HashMap: a key given
                // twice keeps the last value.
                let p = p.trim();
                let (key, value) = match p.split_once('=') {
                    None => (p, None),
                    Some((k, v)) => {
                        let v = v.strip_prefix('"').unwrap_or(v);
                        let v = v.strip_suffix('"').unwrap_or(v);
                        if v.is_empty() || v.contains('"') {
                            continue;
                        }
                        (k, Some(v.to_owned()))
                    }
                };
                if key.is_empty() {
                    continue;
                }
                params.retain(|(k, _)| k != key);
                params.push((key.to_owned(), value));
            }
            (name, params)
        })
        .collect()
}

/// `WebSocketServerExtensionHandler` with the handshakers of
/// `WebSocketServerCompressionHandler`: the first extension the client
/// offers that `PerMessageDeflateServerExtensionHandshaker` (compression
/// level 6, no server window size or no-context, without JZlib) or
/// `DeflateFrameServerExtensionHandshaker` accepts, and the
/// `Sec-WebSocket-Extensions` value answering it. An error for a window
/// size that is not a number, which Netty fails on.
fn negotiate(header: &str) -> io::Result<Option<(Extension, String)>> {
    for (name, params) in extract_extensions(header) {
        if name == "permessage-deflate" {
            let mut accepted = true;
            let mut client_window = 15;
            for (key, value) in &params {
                if key.eq_ignore_ascii_case("client_max_window_bits") {
                    if let Some(v) = value {
                        client_window = v.parse::<i32>().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("For input string: \"{v}\""),
                            )
                        })?;
                        if !(8..=15).contains(&client_window) {
                            accepted = false;
                        }
                    }
                } else if key.eq_ignore_ascii_case("client_no_context_takeover") {
                    // Accepted, though the server does not ask it of the
                    // client.
                } else {
                    // server_max_window_bits, server_no_context_takeover or
                    // anything else.
                    accepted = false;
                }
            }
            if accepted {
                let mut response = name.clone();
                if client_window != 15 {
                    response.push_str(&format!(";client_max_window_bits={client_window}"));
                }
                return Ok(Some((Extension::PerMessageDeflate, response)));
            }
        }
        if (name == "x-webkit-deflate-frame" || name == "deflate-frame") && params.is_empty() {
            return Ok(Some((Extension::DeflateFrame(name.clone()), name)));
        }
    }
    Ok(None)
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
/// `subprotocols`, or refuses it. With `compression`
/// (`webSocketCompressionSupported`), a compression extension the client
/// offers is agreed on, as `WebSocketServerCompressionHandler` does.
pub async fn handshake<S: AsyncWrite + Unpin>(
    stream: &mut S,
    request: &Request,
    subprotocols: &[&str],
    compression: bool,
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
    // The extension handler adds its header to the handshaker's response.
    let mut extension = None;
    if compression
        && let Some(offered) = request.header("sec-websocket-extensions")
        && let Some((agreed, value)) = negotiate(offered)?
    {
        response.push_str(&format!("sec-websocket-extensions: {value}\r\n"));
        extension = Some(agreed);
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    let _ = request.keep_alive();
    Ok(Upgrade::WebSocket(extension))
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

/// `PerMessageDeflateDecoder.FRAME_TAIL`: what a sync flush ends with, left
/// off the wire.
const FRAME_TAIL: [u8; 4] = [0x00, 0x00, 0xff, 0xff];

/// The `DeflateDecoder` of the extension agreed on: raw deflate, its window
/// kept from one message to the next. (Its encoder never sees the broker's
/// data: see `poll_write`.)
struct Deflate {
    per_message: bool,
    /// Made by the first compressed frame, which must start a message.
    decompress: Option<flate2::Decompress>,
    /// `PerMessageDeflateDecoder.compressing`: the message being read is
    /// compressed.
    inflating: bool,
}

impl Deflate {
    fn new(extension: &Extension) -> Self {
        Self {
            per_message: *extension == Extension::PerMessageDeflate,
            decompress: None,
            inflating: false,
        }
    }

    /// `decompressContent`: a frame's payload inflated, the tail added
    /// first when `tail`. `continuation`: whether it continues a message.
    /// More than `limit` bytes of output is a `FileTooLarge` error.
    fn inflate(
        &mut self,
        payload: &[u8],
        tail: bool,
        continuation: bool,
        limit: usize,
    ) -> io::Result<Vec<u8>> {
        let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_owned());
        let d = match &mut self.decompress {
            Some(d) => d,
            None if continuation => {
                return Err(err(
                    "unexpected initial frame type: ContinuationWebSocketFrame",
                ));
            }
            None => self.decompress.insert(flate2::Decompress::new(false)),
        };
        let mut input = payload.to_vec();
        if tail {
            input.extend_from_slice(&FRAME_TAIL);
        }
        let mut out = Vec::with_capacity(input.len() * 3 + 64);
        let start = d.total_in();
        loop {
            let done = (d.total_in() - start) as usize;
            if out.capacity() - out.len() < 64 {
                out.reserve(out.capacity().max(64));
            }
            let before = (d.total_in(), d.total_out());
            let status = d
                .decompress_vec(&input[done..], &mut out, flate2::FlushDecompress::Sync)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if out.len() > limit {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    format!("Max frame length of {limit} has been exceeded."),
                ));
            }
            let all_in = (d.total_in() - start) as usize == input.len();
            if status == flate2::Status::StreamEnd
                || (all_in && out.len() < out.capacity())
                || (d.total_in(), d.total_out()) == before
            {
                break;
            }
        }
        // A frame that held data but gave none (and is not the empty
        // deflate block) cannot start a message.
        if out.is_empty() && !payload.is_empty() && payload != [0x00] && !continuation {
            return Err(err("cannot read uncompressed buffer"));
        }
        Ok(out)
    }
}

/// The byte stream inside a WebSocket: data frames' payloads in, frames
/// out. Control frames are answered here.
pub struct WebSocketStream<S> {
    inner: S,
    max_payload: usize,
    encoder: EncoderType,
    /// `allowExtensions` (`webSocketCompressionSupported`): frames with RSV
    /// bits are taken, as they are unless an extension inflates them.
    allow_extensions: bool,
    deflate: Option<Deflate>,
    /// Received and not yet framed.
    raw: Vec<u8>,
    /// Payload not yet read by the protocol.
    data: Vec<u8>,
    data_at: usize,
    /// Frames (and control replies) not yet written.
    out: Vec<u8>,
    out_at: usize,
    /// The frames of a fragmented message taken so far: 0 outside one
    /// (`fragmentedFramesCount`).
    fragments: u32,
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
            allow_extensions: false,
            deflate: None,
            raw: leftover,
            data: Vec::new(),
            data_at: 0,
            out: Vec::new(),
            out_at: 0,
            fragments: 0,
            closed: false,
            eof: false,
        }
    }

    /// With `webSocketCompressionSupported`: frames may carry RSV bits, and
    /// the extension the handshake agreed on inflates what comes in.
    pub fn with_compression(mut self, extension: Option<&Extension>) -> Self {
        self.allow_extensions = true;
        self.deflate = extension.map(Deflate::new);
        self
    }
}

/// RSV1: a compressed frame.
const RSV1: u8 = 0x40;

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

    /// A data frame's payload, inflated when the extension says it is
    /// compressed: `PerMessageDeflateDecoder` takes a text or binary frame
    /// with RSV1 and the continuations of its message, adding the tail to
    /// the last; `PerFrameDeflateDecoder` takes every data frame with RSV1.
    fn inflated(&mut self, fin_opcode: u8, payload: Vec<u8>) -> io::Result<Vec<u8>> {
        let limit = self.max_payload.saturating_mul(MAX_INFLATION);
        let Some(d) = &mut self.deflate else {
            return Ok(payload);
        };
        let fin = fin_opcode & 0x80 != 0;
        let continuation = fin_opcode & 0x0F == 0;
        let rsv1 = fin_opcode & RSV1 != 0;
        if d.per_message {
            if !continuation {
                d.inflating = rsv1;
            }
            if !d.inflating {
                return Ok(payload);
            }
            let out = d.inflate(&payload, fin, continuation, limit);
            if fin {
                d.inflating = false;
            }
            out
        } else if rsv1 {
            d.inflate(&payload, true, continuation, limit)
        } else {
            Ok(payload)
        }
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
            let len7 = b[1] & 0x7F;
            let fin = fin_opcode & 0x80 != 0;
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
            if fin_opcode & 0x70 != 0 && !self.allow_extensions {
                return violation(self, "RSV != 0 and no extension negotiated");
            }
            if !masked {
                return violation(self, "unmasked client to server frame");
            }
            // WebSocket08FrameDecoder's checks of the opcode against the
            // frame and the message it may be part of.
            if opcode > 7 {
                if !fin {
                    return violation(self, "fragmented control frame");
                }
                if len7 > 125 {
                    return violation(self, "control frame with payload length > 125 octets");
                }
                if !matches!(opcode, 0x8..=0xA) {
                    return violation(
                        self,
                        &format!("control frame using reserved opcode {opcode}"),
                    );
                }
                if opcode == 0x8 && len7 == 1 {
                    return violation(self, "received close control frame with payload len 1");
                }
            } else {
                if opcode > 2 {
                    return violation(self, &format!("data frame using reserved opcode {opcode}"));
                }
                if self.fragments == 0 && opcode == 0 {
                    return violation(
                        self,
                        "received continuation data frame outside fragmented message",
                    );
                }
                if self.fragments != 0 && opcode != 0 {
                    return violation(
                        self,
                        "received non-continuation data frame while inside fragmented message",
                    );
                }
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
                    if fin {
                        self.fragments = 0;
                    } else {
                        self.fragments += 1;
                    }
                    let payload = match self.inflated(fin_opcode, payload) {
                        Ok(p) => p,
                        Err(e) => {
                            // The decoder's exception closes the channel.
                            if e.kind() == io::ErrorKind::FileTooLarge {
                                close_frame(&mut self.out, 1009, &e.to_string());
                            }
                            self.closed = true;
                            return Err(e);
                        }
                    };
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
        // Never compressed, even with an extension agreed on: Artemis adds
        // its websocket-frame-encoder after the handshaker's wsencoder,
        // nearer the head than the extension's encoder, so the broker's
        // data reaches that encoder as bytes, not frames, and passes it by.
        // RFC 7692 lets an endpoint send any message uncompressed.
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

    #[test]
    fn compression_offers_are_answered_as_netty_answers_them() {
        let agreed = |h: &str| negotiate(h).unwrap().map(|(_, v)| v);
        assert_eq!(
            agreed("permessage-deflate; client_max_window_bits").as_deref(),
            Some("permessage-deflate")
        );
        assert_eq!(
            agreed("permessage-deflate; client_max_window_bits=10").as_deref(),
            Some("permessage-deflate;client_max_window_bits=10")
        );
        assert_eq!(
            agreed("permessage-deflate; client_no_context_takeover").as_deref(),
            Some("permessage-deflate")
        );
        for declined in [
            "permessage-deflate; server_no_context_takeover",
            "permessage-deflate; server_max_window_bits=10",
            "permessage-deflate; client_max_window_bits=16",
            "permessage-deflate; foo",
            "deflate-frame; x=1",
            "PERMESSAGE-DEFLATE",
            "",
        ] {
            assert_eq!(agreed(declined), None, "{declined}");
        }
        assert_eq!(
            agreed("permessage-deflate; server_no_context_takeover, x-webkit-deflate-frame")
                .as_deref(),
            Some("x-webkit-deflate-frame")
        );
        assert!(negotiate("permessage-deflate; client_max_window_bits=x").is_err());
    }

    /// A client's raw deflate: sync flushes, the tail left off the last.
    fn client_deflate(c: &mut flate2::Compress, data: &[u8], last: bool) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len() + 64);
        c.compress_vec(data, &mut out, flate2::FlushCompress::Sync)
            .unwrap();
        if last {
            out.truncate(out.len() - 4);
        }
        out
    }

    #[tokio::test]
    async fn compressed_messages_are_inflated_with_their_context() {
        let (_client, server) = tokio::io::duplex(1 << 20);
        let mut c = flate2::Compress::new(flate2::Compression::default(), false);
        // Two messages on one context, the second in two frames; then an
        // uncompressed one.
        let mut input = masked_rsv(0x1, true, RSV1, &client_deflate(&mut c, b"hello ", true));
        input.extend(masked_rsv(
            0x2,
            false,
            RSV1,
            &client_deflate(&mut c, b"hello ", false),
        ));
        input.extend(masked_rsv(
            0x0,
            true,
            0,
            &client_deflate(&mut c, b"world ", true),
        ));
        input.extend(masked(0x1, true, b"plain"));
        let mut ws = WebSocketStream::new(server, input, 1024, EncoderType::Binary)
            .with_compression(Some(&Extension::PerMessageDeflate));
        let mut got = vec![0u8; 23];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello hello world plain");
    }

    #[tokio::test]
    async fn deflate_frame_inflates_each_frame() {
        let (_client, server) = tokio::io::duplex(1 << 20);
        let mut c = flate2::Compress::new(flate2::Compression::default(), false);
        let mut input = masked_rsv(0x2, false, RSV1, &client_deflate(&mut c, b"one ", true));
        input.extend(masked_rsv(
            0x0,
            true,
            RSV1,
            &client_deflate(&mut c, b"two", true),
        ));
        let mut ws = WebSocketStream::new(server, input, 1024, EncoderType::Binary)
            .with_compression(Some(&Extension::DeflateFrame("deflate-frame".to_owned())));
        let mut got = vec![0u8; 7];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"one two");
    }

    #[tokio::test]
    async fn rsv_bits_are_refused_without_compression_and_taken_with_it() {
        let input = masked_rsv(0x1, true, RSV1, b"raw");
        let (_c1, server) = tokio::io::duplex(1 << 16);
        let mut ws = WebSocketStream::new(server, input.clone(), 1024, EncoderType::Binary);
        let mut got = Vec::new();
        ws.read_to_end(&mut got).await.unwrap();
        assert!(got.is_empty());
        // Compression supported, no extension agreed: the payload as it is.
        let (_c2, server) = tokio::io::duplex(1 << 16);
        let mut ws =
            WebSocketStream::new(server, input, 1024, EncoderType::Binary).with_compression(None);
        let mut got = [0u8; 3];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"raw");
    }

    #[tokio::test]
    async fn a_message_that_does_not_inflate_closes() {
        let (_client, server) = tokio::io::duplex(1 << 16);
        let input = masked_rsv(0x1, true, RSV1, &[0xff, 0xff, 0xff, 1, 2, 3]);
        let mut ws = WebSocketStream::new(server, input, 1024, EncoderType::Binary)
            .with_compression(Some(&Extension::PerMessageDeflate));
        let mut got = Vec::new();
        ws.read_to_end(&mut got).await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn a_frame_that_inflates_past_the_limit_closes() {
        let (_client, server) = tokio::io::duplex(1 << 16);
        let mut c = flate2::Compress::new(flate2::Compression::default(), false);
        let zeros = vec![0u8; 1 << 20];
        let compressed = client_deflate(&mut c, &zeros, true);
        assert!(compressed.len() < 4096);
        let input = masked_rsv(0x2, true, RSV1, &compressed);
        let mut ws = WebSocketStream::new(server, input, 4096, EncoderType::Binary)
            .with_compression(Some(&Extension::PerMessageDeflate));
        let mut got = Vec::new();
        let read = ws.read_to_end(&mut got);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), read).await;
        assert!(got.is_empty());
        assert!(ws.closed);
    }

    #[tokio::test]
    async fn a_message_within_the_limit_may_inflate_past_the_frame_size() {
        let (_client, server) = tokio::io::duplex(1 << 16);
        let mut c = flate2::Compress::new(flate2::Compression::default(), false);
        let zeros = vec![0u8; 100_000];
        let input = masked_rsv(0x2, true, RSV1, &client_deflate(&mut c, &zeros, true));
        let mut ws = WebSocketStream::new(server, input, 4096, EncoderType::Binary)
            .with_compression(Some(&Extension::PerMessageDeflate));
        let mut got = vec![1u8; zeros.len()];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(got, zeros);
    }

    /// Frames out of place in the message they would be part of, or
    /// control frames Netty refuses, close the connection with a protocol
    /// error (1002), and nothing after them reaches the protocol.
    #[tokio::test]
    async fn frames_out_of_sequence_close_with_a_protocol_error() {
        let cases: Vec<(&str, Vec<u8>, &[u8])> = vec![
            ("an orphan continuation", masked(0x0, true, b"orphan"), b""),
            (
                "a new message inside a fragmented one",
                [masked(0x1, false, b"a"), masked(0x1, true, b"b")].concat(),
                b"a",
            ),
            ("a fragmented control frame", masked(0x9, false, b"x"), b""),
            (
                "a control frame over 125 bytes",
                masked(0x9, true, &[0; 126]),
                b"",
            ),
            ("a reserved data opcode", masked(0x3, true, b"x"), b""),
            ("a reserved control opcode", masked(0xB, true, b"x"), b""),
            ("a close of one byte", masked(0x8, true, &[1]), b""),
        ];
        for (case, input, delivered) in cases {
            let (mut client, server) = tokio::io::duplex(1 << 16);
            let mut ws = WebSocketStream::new(server, input, 1024, EncoderType::Binary);
            let mut got = Vec::new();
            let read = ws.read_to_end(&mut got);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), read).await;
            assert_eq!(got, delivered, "{case}");
            assert!(ws.closed, "{case}");
            drop(ws);
            let mut sent = Vec::new();
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.read_to_end(&mut sent),
            )
            .await;
            assert!(
                sent.windows(4)
                    .any(|w| w[0] == 0x88 && w[2..4] == [0x03, 0xEA]),
                "{case}: a 1002 close, {sent:?}"
            );
        }
        // A continuation that belongs is taken.
        let (_client, server) = tokio::io::duplex(1 << 16);
        let input = [masked(0x1, false, b"one "), masked(0x0, true, b"two")].concat();
        let mut ws = WebSocketStream::new(server, input, 1024, EncoderType::Binary);
        let mut got = [0u8; 7];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"one two");
    }

    #[tokio::test]
    async fn a_request_body_over_the_limit_is_refused_before_it_is_read() {
        let head = format!(
            "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_REQUEST_BODY + 1
        );
        let e = read_request(&mut head.as_bytes()).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        let head = "POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\nhi";
        let (request, _) = read_request(&mut head.as_bytes()).await.unwrap();
        assert_eq!(request.body, b"hi");
    }

    fn masked_rsv(opcode: u8, fin: bool, rsv: u8, payload: &[u8]) -> Vec<u8> {
        let mut f = masked(opcode, fin, payload);
        f[0] |= rsv;
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
