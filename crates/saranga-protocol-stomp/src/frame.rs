//! STOMP frames as the broker builds and writes them (`StompFrame`,
//! `StompFrameV10`, `StompFrameV11`, `StompFrameV12`).

/// Commands and headers (`Stomp`).
pub mod names {
    pub const CONNECT: &str = "CONNECT";
    pub const STOMP: &str = "STOMP";
    pub const SEND: &str = "SEND";
    pub const SUBSCRIBE: &str = "SUBSCRIBE";
    pub const UNSUBSCRIBE: &str = "UNSUBSCRIBE";
    pub const BEGIN: &str = "BEGIN";
    pub const COMMIT: &str = "COMMIT";
    pub const ABORT: &str = "ABORT";
    pub const ACK: &str = "ACK";
    pub const NACK: &str = "NACK";
    pub const DISCONNECT: &str = "DISCONNECT";

    pub const CONNECTED: &str = "CONNECTED";
    pub const ERROR: &str = "ERROR";
    pub const MESSAGE: &str = "MESSAGE";
    pub const RECEIPT: &str = "RECEIPT";

    pub const RECEIPT_REQUESTED: &str = "receipt";
    pub const RECEIPT_ID: &str = "receipt-id";
    pub const TRANSACTION: &str = "transaction";
    pub const CONTENT_LENGTH: &str = "content-length";
    pub const CONTENT_TYPE: &str = "content-type";
    pub const ACCEPT_VERSION: &str = "accept-version";

    pub const DESTINATION: &str = "destination";
    pub const DESTINATION_TYPE: &str = "destination-type";
    pub const CORRELATION_ID: &str = "correlation-id";
    pub const REPLY_TO: &str = "reply-to";
    pub const EXPIRES: &str = "expires";
    pub const PRIORITY: &str = "priority";
    pub const TYPE: &str = "type";
    pub const PERSISTENT: &str = "persistent";
    pub const AMQ_SCHEDULED_DELAY: &str = "AMQ_SCHEDULED_DELAY";
    pub const AMQ_SCHEDULED_TIME: &str = "AMQ_SCHEDULED_TIME";

    pub const MESSAGE_ID: &str = "message-id";
    pub const REDELIVERED: &str = "redelivered";
    pub const TIMESTAMP: &str = "timestamp";
    pub const SUBSCRIPTION: &str = "subscription";
    pub const ACK_HEADER: &str = "ack";
    pub const VALIDATED_USER: &str = "JMSXUserID";
    pub const INGRESS_TIMESTAMP: &str = "ingress-timestamp";

    pub const ID: &str = "id";
    pub const SELECTOR: &str = "selector";
    pub const DURABLE_SUBSCRIBER_NAME: &str = "durable-subscriber-name";
    pub const DURABLE_SUBSCRIPTION_NAME: &str = "durable-subscription-name";
    pub const ACTIVEMQ_DURABLE_SUBSCRIPTION_NAME: &str = "activemq.subscriptionName";
    pub const ACTIVEMQ_PREFETCH_SIZE: &str = "activemq.prefetchSize";
    pub const CONSUMER_WINDOW_SIZE: &str = "consumer-window-size";
    pub const SUBSCRIPTION_TYPE: &str = "subscription-type";
    pub const NO_LOCAL: &str = "no-local";
    pub const ACTIVEMQ_NO_LOCAL: &str = "activemq.noLocal";

    pub const LOGIN: &str = "login";
    pub const PASSCODE: &str = "passcode";
    pub const CLIENT_ID: &str = "client-id";
    pub const REQUEST_ID: &str = "request-id";
    pub const HEART_BEAT: &str = "heart-beat";

    pub const MESSAGE_HEADER: &str = "message";
    pub const VERSION: &str = "version";
    pub const SESSION: &str = "session";
    pub const RESPONSE_ID: &str = "response-id";
    pub const SERVER: &str = "server";
}

/// The frame classes, which differ in how they keep and write headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `StompFrame` itself: errors raised before a version was chosen.
    Base,
    V10,
    V11,
    V12,
}

/// A frame. Headers keep their insertion order; how a repeated header is
/// kept depends on the kind (`addHeader`).
#[derive(Debug, Clone)]
pub struct Frame {
    pub command: String,
    pub kind: Kind,
    headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Close the connection once this frame is written.
    pub needs_disconnect: bool,
    /// A heart-beat: a lone newline.
    pub ping: bool,
}

impl Frame {
    pub fn new(command: &str, kind: Kind) -> Self {
        Self {
            command: command.to_owned(),
            kind,
            headers: Vec::new(),
            body: Vec::new(),
            // STOMP 1.2 requires disconnecting after an ERROR.
            needs_disconnect: kind == Kind::V12 && command == names::ERROR,
            ping: false,
        }
    }

    /// A decoded frame: its headers as the decoder kept them.
    pub fn decoded(
        command: &str,
        kind: Kind,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            command: command.to_owned(),
            kind,
            headers,
            body,
            needs_disconnect: false,
            ping: false,
        }
    }

    pub fn ping(kind: Kind) -> Self {
        Self {
            ping: true,
            ..Self::new(names::STOMP, kind)
        }
    }

    /// `getHeader`: the first value kept for the name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn has_header(&self, name: &str) -> bool {
        self.header(name).is_some()
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// `addHeader`. 1.0 frames (a `LinkedHashMap`) replace the value in
    /// place; 1.1 and 1.2 frames write a repeated header again, except
    /// `content-length`, and keep the first value for lookups.
    pub fn add_header(&mut self, key: &str, value: &str) {
        let existing = self.headers.iter_mut().find(|(k, _)| k == key);
        match (self.kind, existing) {
            (_, None) => self.headers.push((key.to_owned(), value.to_owned())),
            (Kind::Base | Kind::V10, Some(slot)) => slot.1 = value.to_owned(),
            (Kind::V11 | Kind::V12, Some(_)) if key != names::CONTENT_LENGTH => {
                self.headers.push((key.to_owned(), value.to_owned()))
            }
            (Kind::V11 | Kind::V12, Some(_)) => {}
        }
    }

    pub fn set_body(&mut self, body: impl Into<Vec<u8>>) {
        self.body = body.into();
    }

    /// The body as text (`getBody`).
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// `toActiveMQBuffer`.
    pub fn encode(&self) -> Vec<u8> {
        if self.ping {
            return vec![b'\n'];
        }
        let mut head = String::with_capacity(512);
        head.push_str(&self.command);
        head.push('\n');
        let escaped = matches!(self.kind, Kind::V11 | Kind::V12);
        for (k, v) in &self.headers {
            if escaped {
                escape_into(&mut head, k);
                head.push(':');
                escape_into(&mut head, v);
            } else {
                head.push_str(k);
                head.push(':');
                head.push_str(v);
            }
            head.push('\n');
        }
        if !self.body.is_empty()
            && !self.has_header(names::CONTENT_LENGTH)
            && self.kind != Kind::V10
        {
            head.push_str(names::CONTENT_LENGTH);
            head.push(':');
            head.push_str(&self.body.len().to_string());
            head.push('\n');
        }
        head.push('\n');
        let mut out = Vec::with_capacity(head.len() + self.body.len() + 2);
        out.extend_from_slice(head.as_bytes());
        out.extend_from_slice(&self.body);
        out.extend_from_slice(&[0, b'\n']);
        out
    }
}

/// `StompFrame.encode`: `\n`, `\r`, `\` and `:` escaped.
fn escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            ':' => out.push_str("\\c"),
            c => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_headers() {
        let mut f = Frame::new(names::MESSAGE, Kind::V10);
        f.add_header("a", "1");
        f.add_header("b", "2");
        f.add_header("a", "3");
        assert_eq!(f.encode(), b"MESSAGE\na:3\nb:2\n\n\0\n");
        let mut f = Frame::new(names::MESSAGE, Kind::V12);
        f.add_header("a", "1");
        f.add_header("a", "x:y");
        f.add_header("content-length", "0");
        f.add_header("content-length", "9");
        assert_eq!(f.header("a"), Some("1"));
        assert_eq!(
            f.encode(),
            b"MESSAGE\na:1\na:x\\cy\ncontent-length:0\n\n\0\n"
        );
    }

    #[test]
    fn content_length_except_1_0() {
        let mut f = Frame::new(names::RECEIPT, Kind::V11);
        f.set_body("hi");
        assert_eq!(f.encode(), b"RECEIPT\ncontent-length:2\n\nhi\0\n");
        let mut f = Frame::new(names::RECEIPT, Kind::V10);
        f.set_body("hi");
        assert_eq!(f.encode(), b"RECEIPT\n\nhi\0\n");
        assert_eq!(Frame::ping(Kind::V12).encode(), b"\n");
        assert!(Frame::new(names::ERROR, Kind::V12).needs_disconnect);
        assert!(!Frame::new(names::ERROR, Kind::V11).needs_disconnect);
    }
}
