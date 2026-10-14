//! The ZooKeeper client protocol as Jute serializes it: big-endian ints
//! and longs, buffers and strings prefixed by their length (-1 for null),
//! vectors by their count, every packet by its length.

use std::io;

/// `ZooDefs.OpCode`.
pub mod op {
    pub const CREATE: i32 = 1;
    pub const DELETE: i32 = 2;
    pub const EXISTS: i32 = 3;
    pub const GET_DATA: i32 = 4;
    pub const SET_DATA: i32 = 5;
    pub const GET_CHILDREN: i32 = 8;
    pub const PING: i32 = 11;
    pub const CREATE_CONTAINER: i32 = 19;
    pub const CLOSE_SESSION: i32 = -11;
}

/// The xids the server answers with outside the request order.
pub mod xid {
    pub const NOTIFICATION: i32 = -1;
    pub const PING: i32 = -2;
}

/// `KeeperException.Code`.
pub mod code {
    pub const OK: i32 = 0;
    pub const CONNECTION_LOSS: i32 = -4;
    pub const OPERATION_TIMEOUT: i32 = -7;
    pub const NO_NODE: i32 = -101;
    pub const BAD_VERSION: i32 = -103;
    pub const NO_CHILDREN_FOR_EPHEMERALS: i32 = -108;
    pub const NODE_EXISTS: i32 = -110;
    pub const NOT_EMPTY: i32 = -111;
    pub const SESSION_EXPIRED: i32 = -112;
    pub const SESSION_MOVED: i32 = -118;
}

/// `CreateMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateMode {
    Persistent,
    Ephemeral,
    PersistentSequential,
    EphemeralSequential,
    /// Removed by the server once its last child is.
    Container,
}

impl CreateMode {
    pub fn flag(self) -> i32 {
        match self {
            Self::Persistent => 0,
            Self::Ephemeral => 1,
            Self::PersistentSequential => 2,
            Self::EphemeralSequential => 3,
            Self::Container => 4,
        }
    }
}

/// A znode's `Stat`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stat {
    pub czxid: i64,
    pub mzxid: i64,
    pub ctime: i64,
    pub mtime: i64,
    pub version: i32,
    pub cversion: i32,
    pub aversion: i32,
    pub ephemeral_owner: i64,
    pub data_length: i32,
    pub num_children: i32,
    pub pzxid: i64,
}

#[derive(Default)]
pub struct Out(pub Vec<u8>);

impl Out {
    pub fn int(&mut self, v: i32) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn long(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.0.push(v as u8);
        self
    }
    pub fn buffer(&mut self, v: Option<&[u8]>) -> &mut Self {
        match v {
            Some(b) => {
                self.int(b.len() as i32);
                self.0.extend_from_slice(b);
            }
            None => {
                self.int(-1);
            }
        }
        self
    }
    pub fn string(&mut self, v: &str) -> &mut Self {
        self.buffer(Some(v.as_bytes()))
    }
    /// `ZooDefs.Ids.OPEN_ACL_UNSAFE`: everything allowed to anyone.
    pub fn open_acl(&mut self) -> &mut Self {
        self.int(1).int(31).string("world").string("anyone")
    }
    /// The packet with its length in front.
    pub fn framed(&self) -> Vec<u8> {
        let mut f = Vec::with_capacity(self.0.len() + 4);
        f.extend_from_slice(&(self.0.len() as i32).to_be_bytes());
        f.extend_from_slice(&self.0);
        f
    }
}

pub struct In<'a> {
    data: &'a [u8],
    pos: usize,
}

fn short() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "truncated ZooKeeper packet")
}

impl<'a> In<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let s = self.data.get(self.pos..self.pos + n).ok_or_else(short)?;
        self.pos += n;
        Ok(s)
    }
    pub fn int(&mut self) -> io::Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn long(&mut self) -> io::Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bool(&mut self) -> io::Result<bool> {
        Ok(self.take(1)?[0] != 0)
    }
    pub fn buffer(&mut self) -> io::Result<Option<Vec<u8>>> {
        let n = self.int()?;
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(self.take(n as usize)?.to_vec()))
    }
    pub fn string(&mut self) -> io::Result<String> {
        let b = self.buffer()?.unwrap_or_default();
        String::from_utf8(b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
    pub fn strings(&mut self) -> io::Result<Vec<String>> {
        let n = self.int()?;
        (0..n.max(0)).map(|_| self.string()).collect()
    }
    pub fn stat(&mut self) -> io::Result<Stat> {
        Ok(Stat {
            czxid: self.long()?,
            mzxid: self.long()?,
            ctime: self.long()?,
            mtime: self.long()?,
            version: self.int()?,
            cversion: self.int()?,
            aversion: self.int()?,
            ephemeral_owner: self.long()?,
            data_length: self.int()?,
            num_children: self.int()?,
            pzxid: self.long()?,
        })
    }
}

/// `ConnectRequest`, with the read-only flag 3.4 and later send.
pub fn connect_request(last_zxid: i64, timeout_ms: i32, session_id: i64, passwd: &[u8]) -> Vec<u8> {
    let mut o = Out::default();
    o.int(0)
        .long(last_zxid)
        .int(timeout_ms)
        .long(session_id)
        .buffer(Some(passwd))
        .bool(false);
    o.framed()
}

/// `ConnectResponse`.
#[derive(Debug)]
pub struct ConnectResponse {
    pub timeout_ms: i32,
    pub session_id: i64,
    pub passwd: Vec<u8>,
}

pub fn connect_response(data: &[u8]) -> io::Result<ConnectResponse> {
    let mut i = In::new(data);
    let _protocol_version = i.int()?;
    Ok(ConnectResponse {
        timeout_ms: i.int()?,
        session_id: i.long()?,
        passwd: i.buffer()?.unwrap_or_default(),
    })
}

/// `ReplyHeader`.
#[derive(Debug)]
pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32,
}

pub fn reply_header(i: &mut In<'_>) -> io::Result<ReplyHeader> {
    Ok(ReplyHeader {
        xid: i.int()?,
        zxid: i.long()?,
        err: i.int()?,
    })
}

/// A request: `RequestHeader` and its record.
pub fn request(xid: i32, op: i32, body: &[u8]) -> Vec<u8> {
    let mut o = Out::default();
    o.int(xid).int(op);
    o.0.extend_from_slice(body);
    o.framed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_request_as_jute_writes_it() {
        let r = connect_request(0, 18_000, 0, &[0; 16]);
        // length, protocolVersion, lastZxidSeen, timeOut, sessionId,
        // passwd (16 bytes), readOnly.
        assert_eq!(r.len(), 4 + 4 + 8 + 4 + 8 + 4 + 16 + 1);
        assert_eq!(&r[..4], &(45i32).to_be_bytes());
        assert_eq!(&r[16..20], &18_000i32.to_be_bytes());
    }

    #[test]
    fn create_request_round_trip() {
        let mut o = Out::default();
        o.string("/a/b")
            .buffer(Some(b"xy"))
            .open_acl()
            .int(CreateMode::EphemeralSequential.flag());
        let mut i = In::new(&o.0);
        assert_eq!(i.string().unwrap(), "/a/b");
        assert_eq!(i.buffer().unwrap().unwrap(), b"xy");
        assert_eq!(i.int().unwrap(), 1);
        assert_eq!(i.int().unwrap(), 31);
        assert_eq!(i.string().unwrap(), "world");
        assert_eq!(i.string().unwrap(), "anyone");
        assert_eq!(i.int().unwrap(), 3);
        assert_eq!(i.remaining(), 0);
    }
}
