//! XA transaction packets (`SessionXA*Message`) and the `Xid` encoding
//! (`XidCodecSupport`).

use saranga_buffer::{Codec, DecodeError, JString, Reader, Result, Writer};

use crate::{Context, PacketCodec, types};

/// A `javax.transaction.xa.Xid` as encoded by `XidCodecSupport.encodeXid`:
/// i32 format ID, then the branch qualifier and the global transaction ID,
/// each an i32-length-prefixed byte array (note: branch qualifier first).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Xid {
    pub format_id: i32,
    pub branch_qualifier: Vec<u8>,
    pub global_transaction_id: Vec<u8>,
}

impl Codec for Xid {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            format_id: r.i32()?,
            branch_qualifier: r.byte_array()?,
            global_transaction_id: r.byte_array()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.i32(self.format_id)
            .byte_array(&self.branch_qualifier)
            .byte_array(&self.global_transaction_id);
    }
}

wire_packet! {
    /// `SessionXAAfterFailedMessage`
    SessionXAAfterFailedMessage = types::SESS_XA_FAILED => {
        pub xid: Xid,
    }
}

wire_packet! {
    /// `SessionXACommitMessage`
    SessionXACommitMessage = types::SESS_XA_COMMIT => {
        pub xid: Xid,
        pub one_phase: bool,
    }
}

wire_packet! {
    /// `SessionXAEndMessage`
    SessionXAEndMessage = types::SESS_XA_END => {
        pub xid: Xid,
        pub failed: bool,
    }
}

wire_packet! {
    /// `SessionXAForgetMessage`
    SessionXAForgetMessage = types::SESS_XA_FORGET => {
        pub xid: Xid,
    }
}

/// `SessionXAGetInDoubtXidsResponseMessage`: i32 count, then each Xid.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionXAGetInDoubtXidsResponseMessage {
    pub xids: Vec<Xid>,
}

impl PacketCodec for SessionXAGetInDoubtXidsResponseMessage {
    const TYPE: i8 = types::SESS_XA_INDOUBT_XIDS_RESP;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let count = r.i32()?;
        // Java preallocates `count` and so fails on a negative one. Every Xid
        // takes at least 12 bytes, which bounds the allocation here.
        let count = usize::try_from(count)
            .map_err(|_| DecodeError::Invalid(format!("negative xid count {count}")))?;
        let mut xids = Vec::with_capacity(count.min(r.remaining() / 12));
        for _ in 0..count {
            xids.push(Xid::read(r)?);
        }
        Ok(Self { xids })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i32(self.xids.len() as i32);
        for xid in &self.xids {
            xid.write(w);
        }
    }
}

wire_packet! {
    /// `SessionXAGetTimeoutResponseMessage`
    SessionXAGetTimeoutResponseMessage = types::SESS_XA_GET_TIMEOUT_RESP => {
        pub timeout_seconds: i32,
    }
}

wire_packet! {
    /// `SessionXAJoinMessage`
    SessionXAJoinMessage = types::SESS_XA_JOIN => {
        pub xid: Xid,
    }
}

wire_packet! {
    /// `SessionXAPrepareMessage`
    SessionXAPrepareMessage = types::SESS_XA_PREPARE => {
        pub xid: Xid,
    }
}

wire_packet! {
    /// `SessionXAResponseMessage` (versions before 130). `response_code` is
    /// an `XAResource`/`XAException` code.
    SessionXAResponseMessage = types::SESS_XA_RESP => {
        pub error: bool,
        pub response_code: i32,
        pub message: Option<JString>,
    }
}

/// `SessionXAResponseMessage_V2` (130+): adds the correlation ID, which is
/// read only if present (Java always writes it).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionXAResponseMessageV2 {
    pub error: bool,
    pub response_code: i32,
    pub message: Option<JString>,
    pub correlation_id: Option<i64>,
}

impl PacketCodec for SessionXAResponseMessageV2 {
    const TYPE: i8 = types::SESS_XA_RESP;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            error: r.bool()?,
            response_code: r.i32()?,
            message: r.nullable_string()?,
            correlation_id: if r.remaining() >= 8 {
                Some(r.i64()?)
            } else {
                None
            },
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.error)
            .i32(self.response_code)
            .nullable_string(self.message.as_ref());
        if let Some(id) = self.correlation_id {
            w.i64(id);
        }
    }
}

wire_packet! {
    /// `SessionXAResumeMessage`
    SessionXAResumeMessage = types::SESS_XA_RESUME => {
        pub xid: Xid,
    }
}

wire_packet! {
    /// `SessionXARollbackMessage`
    SessionXARollbackMessage = types::SESS_XA_ROLLBACK => {
        pub xid: Xid,
    }
}

wire_packet! {
    /// `SessionXASetTimeoutMessage`
    SessionXASetTimeoutMessage = types::SESS_XA_SET_TIMEOUT => {
        pub timeout_seconds: i32,
    }
}

wire_packet! {
    /// `SessionXASetTimeoutResponseMessage`
    SessionXASetTimeoutResponseMessage = types::SESS_XA_SET_TIMEOUT_RESP => {
        pub ok: bool,
    }
}

wire_packet! {
    /// `SessionXAStartMessage`
    SessionXAStartMessage = types::SESS_XA_START => {
        pub xid: Xid,
    }
}
