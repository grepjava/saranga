//! The OpenWire wire format: `org.apache.activemq.openwire.OpenWireFormat`,
//! `BooleanStream` and `BaseDataStreamMarshaller` of ActiveMQ 5.19.10, in
//! tight and loose encodings, with the marshal cache.
//!
//! The generated structs ([`crate::commands`]) list their fields; a
//! [`Writer`] or [`Reader`] decides how each kind of field is encoded.

use std::collections::HashMap;
use std::fmt::Write as _;

pub use crate::commands::DataStructure;
use crate::commands::{LocalTransactionId, XaTransactionId};

/// What a Java marshaller would have thrown.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("Unknown data type: {0}")]
    UnknownType(i8),
    #[error("{0}")]
    Io(String),
    #[error("{0} cannot be cast to {1}")]
    ClassCast(&'static str, &'static str),
    #[error("Frame size of {0} is larger than max allowed {1}")]
    FrameSize(i64, i64),
    #[error("unexpected end of frame")]
    Eof,
}

impl CodecError {
    pub fn class_cast(from: &'static str, to: &'static str) -> Self {
        CodecError::ClassCast(from, to)
    }
}

type R<T> = Result<T, CodecError>;

/// A Java `Throwable` as OpenWire carries it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Throwable {
    /// `getClass().getName()`.
    pub class: Option<String>,
    pub message: Option<String>,
    /// Only on the wire when stack traces are enabled.
    pub stack_trace: Vec<StackTraceElement>,
    pub cause: Option<Box<Throwable>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct StackTraceElement {
    pub class_name: Option<String>,
    pub method_name: Option<String>,
    pub file_name: Option<String>,
    pub line_number: i32,
}

impl Throwable {
    pub fn new(class: &str, message: Option<String>) -> Self {
        Throwable {
            class: Some(class.into()),
            message,
            ..Default::default()
        }
    }
}

/// `TransactionId`: a local or an XA transaction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionId {
    Local(LocalTransactionId),
    Xa(XaTransactionId),
}

/// A typed data structure field: how to write it and how to get it back
/// from whatever was read.
pub trait Structure: Sized {
    fn data_type(&self) -> u8;
    fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> R<()>;
    fn from_ds(ds: DataStructure) -> R<Self>;
    fn to_ds(&self) -> DataStructure;
}

impl<T: Structure> Structure for Box<T> {
    fn data_type(&self) -> u8 {
        (**self).data_type()
    }
    fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> R<()> {
        (**self).write_fields(version, w)
    }
    fn from_ds(ds: DataStructure) -> R<Self> {
        T::from_ds(ds).map(Box::new)
    }
    fn to_ds(&self) -> DataStructure {
        (**self).to_ds()
    }
}

impl Structure for DataStructure {
    fn data_type(&self) -> u8 {
        DataStructure::data_type(self)
    }
    fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> R<()> {
        DataStructure::write_fields(self, version, w)
    }
    fn from_ds(ds: DataStructure) -> R<Self> {
        Ok(ds)
    }
    fn to_ds(&self) -> DataStructure {
        self.clone()
    }
}

impl Structure for TransactionId {
    fn data_type(&self) -> u8 {
        match self {
            TransactionId::Local(t) => t.data_type(),
            TransactionId::Xa(t) => t.data_type(),
        }
    }
    fn write_fields<W: Writer>(&self, version: u32, w: &mut W) -> R<()> {
        match self {
            TransactionId::Local(t) => t.write_fields(version, w),
            TransactionId::Xa(t) => t.write_fields(version, w),
        }
    }
    fn from_ds(ds: DataStructure) -> R<Self> {
        match ds {
            DataStructure::LocalTransactionId(t) => Ok(TransactionId::Local(t)),
            DataStructure::XaTransactionId(t) => Ok(TransactionId::Xa(t)),
            other => Err(CodecError::class_cast(other.class_name(), "TransactionId")),
        }
    }
    fn to_ds(&self) -> DataStructure {
        match self {
            TransactionId::Local(t) => DataStructure::LocalTransactionId(t.clone()),
            TransactionId::Xa(t) => DataStructure::XaTransactionId(t.clone()),
        }
    }
}

/// Writes the fields of a structure.
pub trait Writer {
    fn bool(&mut self, name: &'static str, v: bool) -> R<()>;
    fn byte(&mut self, name: &'static str, v: i8) -> R<()>;
    fn short(&mut self, name: &'static str, v: i16) -> R<()>;
    fn int(&mut self, name: &'static str, v: i32) -> R<()>;
    fn long(&mut self, name: &'static str, v: i64) -> R<()>;
    fn string(&mut self, name: &'static str, v: Option<&str>) -> R<()>;
    fn bytes(&mut self, name: &'static str, v: Option<&[u8]>) -> R<()>;
    fn const_bytes(&mut self, name: &'static str, v: &[u8], size: usize) -> R<()>;
    fn throwable(&mut self, name: &'static str, v: Option<&Throwable>) -> R<()>;
    fn nested<T: Structure>(&mut self, name: &'static str, v: Option<&T>) -> R<()>;
    fn cached<T: Structure>(&mut self, name: &'static str, v: Option<&T>) -> R<()>;
    fn array<T: Structure>(&mut self, name: &'static str, v: Option<&[T]>) -> R<()>;
}

/// Reads the fields of a structure.
pub trait Reader {
    fn bool(&mut self) -> R<bool>;
    fn byte(&mut self) -> R<i8>;
    fn short(&mut self) -> R<i16>;
    fn int(&mut self) -> R<i32>;
    fn long(&mut self) -> R<i64>;
    fn string(&mut self) -> R<Option<String>>;
    fn bytes(&mut self) -> R<Option<Vec<u8>>>;
    fn const_bytes(&mut self, size: usize) -> R<Vec<u8>>;
    fn throwable(&mut self) -> R<Option<Box<Throwable>>>;
    fn nested<T: Structure>(&mut self) -> R<Option<T>>;
    fn cached<T: Structure>(&mut self) -> R<Option<T>>;
    fn array<T: Structure>(&mut self) -> R<Option<Vec<T>>>;
}

/// Whether `isMarshallAware()` is true for the type (Message and
/// WireFormatInfo).
fn marshall_aware(data_type: u8) -> bool {
    matches!(data_type, 1 | 23..=29)
}

// ---------------------------------------------------------------- BooleanStream

/// `org.apache.activemq.openwire.BooleanStream`.
#[derive(Debug, Clone)]
pub struct BooleanStream {
    data: Vec<u8>,
    array_limit: usize,
    array_pos: usize,
    byte_pos: u8,
}

impl Default for BooleanStream {
    fn default() -> Self {
        BooleanStream {
            data: vec![0; 48],
            array_limit: 0,
            array_pos: 0,
            byte_pos: 0,
        }
    }
}

impl BooleanStream {
    pub fn write(&mut self, value: bool) {
        if self.byte_pos == 0 {
            self.array_limit += 1;
            if self.array_limit >= self.data.len() {
                let n = self.data.len() * 2;
                self.data.resize(n, 0);
            }
        }
        if value {
            self.data[self.array_pos] |= 1 << self.byte_pos;
        }
        self.byte_pos += 1;
        if self.byte_pos >= 8 {
            self.byte_pos = 0;
            self.array_pos += 1;
        }
    }

    pub fn read(&mut self) -> R<bool> {
        // Java reads past the limit into its (zeroed) buffer.
        let b = *self
            .data
            .get(self.array_pos)
            .ok_or_else(|| CodecError::Io("ArrayIndexOutOfBoundsException".into()))?;
        let rc = (b >> self.byte_pos) & 1 != 0;
        self.byte_pos += 1;
        if self.byte_pos >= 8 {
            self.byte_pos = 0;
            self.array_pos += 1;
        }
        Ok(rc)
    }

    pub fn marshalled_size(&self) -> usize {
        if self.array_limit < 64 {
            1 + self.array_limit
        } else if self.array_limit < 256 {
            2 + self.array_limit
        } else {
            3 + self.array_limit
        }
    }

    /// Writes the stream and rewinds it for reading back.
    pub fn marshal(&mut self, out: &mut Vec<u8>) {
        if self.array_limit < 64 {
            out.push(self.array_limit as u8);
        } else if self.array_limit < 256 {
            out.push(0xc0);
            out.push(self.array_limit as u8);
        } else {
            out.push(0x80);
            out.extend_from_slice(&(self.array_limit as u16).to_be_bytes());
        }
        out.extend_from_slice(&self.data[..self.array_limit]);
        self.array_pos = 0;
        self.byte_pos = 0;
    }

    pub fn unmarshal(input: &mut Input<'_>) -> R<Self> {
        let mut limit = input.u8()? as i32;
        if limit == 192 {
            limit = input.u8()? as i32;
        } else if limit == 128 {
            limit = input.i16()? as i32;
        }
        if limit < 0 {
            return Err(CodecError::Io("NegativeArraySizeException".into()));
        }
        let limit = limit as usize;
        let mut data = vec![0; limit.max(48)];
        data[..limit].copy_from_slice(input.take(limit)?);
        Ok(BooleanStream {
            data,
            array_limit: limit,
            array_pos: 0,
            byte_pos: 0,
        })
    }
}

// ---------------------------------------------------------------- input

/// A `DataInput` over a frame.
pub struct Input<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Input<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Input { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn take(&mut self, n: usize) -> R<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(CodecError::Eof)?;
        if end > self.buf.len() {
            return Err(CodecError::Eof);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    pub fn u8(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn i16(&mut self) -> R<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn i32(&mut self) -> R<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> R<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// `DataInput.readUTF`.
    pub fn utf(&mut self) -> R<String> {
        let len = self.i16()? as u16 as usize;
        decode_modified_utf8(self.take(len)?)
    }
}

/// Java's modified UTF-8, as `DataInputStream.readUTF` decodes it.
pub fn decode_modified_utf8(b: &[u8]) -> R<String> {
    let bad = || CodecError::Io("UTFDataFormatException: malformed input".into());
    let mut units = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i] as u16;
        match c >> 4 {
            0..=7 => {
                units.push(c);
                i += 1;
            }
            12 | 13 => {
                let c2 = *b.get(i + 1).ok_or_else(bad)? as u16;
                if c2 & 0xc0 != 0x80 {
                    return Err(bad());
                }
                units.push(((c & 0x1f) << 6) | (c2 & 0x3f));
                i += 2;
            }
            14 => {
                let c2 = *b.get(i + 1).ok_or_else(bad)? as u16;
                let c3 = *b.get(i + 2).ok_or_else(bad)? as u16;
                if c2 & 0xc0 != 0x80 || c3 & 0xc0 != 0x80 {
                    return Err(bad());
                }
                units.push(((c & 0x0f) << 12) | ((c2 & 0x3f) << 6) | (c3 & 0x3f));
                i += 3;
            }
            _ => return Err(bad()),
        }
    }
    Ok(String::from_utf16_lossy(&units))
}

/// `DataOutput.writeUTF`.
pub fn write_utf(out: &mut Vec<u8>, s: &str) -> R<()> {
    let len: usize = s
        .encode_utf16()
        .map(|c| match c {
            1..=0x7f => 1,
            0 | 0x80..=0x7ff => 2,
            _ => 3,
        })
        .sum();
    if len > 65535 {
        return Err(CodecError::Io(format!(
            "UTFDataFormatException: encoded string too long: {len} bytes"
        )));
    }
    out.extend_from_slice(&(len as u16).to_be_bytes());
    for c in s.encode_utf16() {
        match c {
            1..=0x7f => out.push(c as u8),
            0 | 0x80..=0x7ff => {
                out.push(0xc0 | ((c >> 6) & 0x1f) as u8);
                out.push(0x80 | (c & 0x3f) as u8);
            }
            _ => {
                out.push(0xe0 | ((c >> 12) & 0x0f) as u8);
                out.push(0x80 | ((c >> 6) & 0x3f) as u8);
                out.push(0x80 | (c & 0x3f) as u8);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- the caches

/// What the marshal cache (a Java `HashMap<DataStructure, Short>`) compares
/// an object by: ids, destinations and transaction ids by value, a null as
/// null, anything else by identity (never equal to another object).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    Null,
    Value(Box<DataStructure>),
    Identity(u64),
}

#[derive(Debug, Default)]
struct MarshalCache {
    map: HashMap<CacheKey, i16>,
    slots: Vec<Option<CacheKey>>,
    next: i16,
    next_eviction: i16,
    identities: u64,
}

impl MarshalCache {
    fn new(size: usize) -> Self {
        MarshalCache {
            slots: vec![None; size],
            ..Default::default()
        }
    }

    fn key<T: Structure>(&mut self, v: Option<&T>) -> CacheKey {
        match v {
            None => CacheKey::Null,
            Some(v) => match v.data_type() {
                100..=103 | 111 | 112 | 120..=124 => CacheKey::Value(Box::new(v.to_ds())),
                _ => {
                    self.identities += 1;
                    CacheKey::Identity(self.identities)
                }
            },
        }
    }

    fn sweep(&mut self) {
        while self.map.len() > self.slots.len().saturating_sub(100) {
            let i = self.next_eviction as usize;
            if let Some(k) = self.slots[i].take() {
                self.map.remove(&k);
            } else {
                // Java removes a null key here (the cached null), if any.
                self.map.remove(&CacheKey::Null);
            }
            self.next_eviction += 1;
            if self.next_eviction as usize >= self.slots.len() {
                self.next_eviction = 0;
            }
        }
    }

    fn add(&mut self, key: CacheKey) -> i16 {
        let i = self.next;
        self.next = self.next.wrapping_add(1);
        if self.next as usize >= self.slots.len() {
            self.next = 0;
        }
        if self.map.len() < self.slots.len() {
            self.slots[i as usize] = Some(key.clone());
            self.map.insert(key, i);
            i
        } else {
            -1
        }
    }
}

/// `OpenWireFormat.DEFAULT_WIRE_VERSION`.
pub const DEFAULT_WIRE_VERSION: u32 = 12;
const MARSHAL_CACHE_SIZE: usize = 16383;

/// `org.apache.activemq.openwire.OpenWireFormat`: the negotiated options and
/// the caches of one connection.
#[derive(Debug)]
pub struct WireFormat {
    pub version: u32,
    pub stack_trace_enabled: bool,
    pub tcp_no_delay_enabled: bool,
    cache_enabled: bool,
    pub tight_encoding_enabled: bool,
    pub size_prefix_disabled: bool,
    pub max_frame_size: i64,
    pub max_frame_size_enabled: bool,
    marshal_cache: MarshalCache,
    unmarshal_cache: Vec<Option<DataStructure>>,
}

impl WireFormat {
    pub fn new(version: u32) -> Self {
        WireFormat {
            version,
            stack_trace_enabled: false,
            tcp_no_delay_enabled: false,
            cache_enabled: false,
            tight_encoding_enabled: false,
            size_prefix_disabled: false,
            max_frame_size: i64::MAX,
            max_frame_size_enabled: true,
            marshal_cache: MarshalCache::default(),
            unmarshal_cache: Vec::new(),
        }
    }

    pub fn cache_enabled(&self) -> bool {
        self.cache_enabled
    }

    /// `setCacheEnabled`: turning it on makes caches of the default size.
    pub fn set_cache_enabled(&mut self, enabled: bool) {
        if enabled {
            self.set_cache_size(MARSHAL_CACHE_SIZE);
        }
        self.cache_enabled = enabled;
    }

    /// New, empty caches of a negotiated size (0 for the default).
    pub fn set_cache_size(&mut self, size: usize) {
        let size = if size == 0 { MARSHAL_CACHE_SIZE } else { size };
        self.marshal_cache = MarshalCache::new(size);
        self.unmarshal_cache = vec![None; size];
    }

    /// `marshal(Object)`: a frame for a command (`None` for a null).
    pub fn marshal(&mut self, command: Option<&DataStructure>) -> R<Vec<u8>> {
        if self.cache_enabled {
            self.marshal_cache.sweep();
        }
        let Some(c) = command else {
            return Ok(vec![0, 0, 0, 1, 0]);
        };
        let data_type = c.data_type();
        if !crate::commands::known_type(self.version, data_type) {
            return Err(CodecError::UnknownType(data_type as i8));
        }
        let mut out = Vec::new();
        if self.tight_encoding_enabled {
            let mut bs = BooleanStream::default();
            let mut sizer = TightSizer {
                wf: self,
                bs: &mut bs,
                size: 0,
                indexes: Vec::new(),
            };
            c.write_fields(sizer.wf.version, &mut sizer)?;
            let indexes = sizer.indexes;
            let size = 1 + sizer.size + bs.marshalled_size();
            if self.max_frame_size_enabled && size as i64 > self.max_frame_size {
                return Err(CodecError::FrameSize(size as i64, self.max_frame_size));
            }
            out.reserve(size + 4);
            if !self.size_prefix_disabled {
                out.extend_from_slice(&(size as i32).to_be_bytes());
            }
            out.push(data_type);
            bs.marshal(&mut out);
            let mut w = TightWriter {
                wf: self,
                bs: &mut bs,
                out: &mut out,
                indexes: indexes.into_iter(),
            };
            c.write_fields(w.wf.version, &mut w)?;
        } else {
            if !self.size_prefix_disabled {
                out.extend_from_slice(&[0; 4]);
            }
            out.push(data_type);
            let mut w = LooseWriter {
                wf: self,
                out: &mut out,
            };
            c.write_fields(w.wf.version, &mut w)?;
            if !self.size_prefix_disabled {
                let size = (out.len() - 4) as i32;
                out[..4].copy_from_slice(&size.to_be_bytes());
            }
        }
        Ok(out)
    }

    /// `unmarshal(ByteSequence)`: the command in a frame (with its size
    /// prefix unless that is disabled).
    pub fn unmarshal(&mut self, frame: &[u8]) -> R<Option<DataStructure>> {
        let mut input = Input::new(frame);
        let mut frame_size = None;
        if !self.size_prefix_disabled {
            let size = input.i32()?;
            if self.max_frame_size_enabled && size as i64 > self.max_frame_size {
                return Err(CodecError::FrameSize(size as i64, self.max_frame_size));
            }
            if size < 0 {
                return Err(CodecError::Io(format!(
                    "Frame size {size} can't be negative."
                )));
            }
            frame_size = Some(size);
        }
        let data_type = input.u8()?;
        if data_type == 0 {
            return Ok(None);
        }
        if !crate::commands::known_type(self.version, data_type) {
            return Err(CodecError::UnknownType(data_type as i8));
        }
        let version = self.version;
        let budget = Budget {
            frame_size,
            allocated: 0,
        };
        if self.tight_encoding_enabled {
            let bs = BooleanStream::unmarshal(&mut input)?;
            let mut r = TightReader {
                wf: self,
                bs,
                input,
                budget,
            };
            DataStructure::read_body(data_type, version, &mut r).map(Some)
        } else {
            let mut r = LooseReader {
                wf: self,
                input,
                budget,
            };
            DataStructure::read_body(data_type, version, &mut r).map(Some)
        }
    }

    fn check_buffer_size(&self, budget: &mut Budget, size: i32) -> R<()> {
        if let Some(frame) = budget.frame_size {
            if size < 0 {
                return Err(CodecError::Io(format!("Size {size} can't be negative.")));
            }
            budget.allocated = budget.allocated.checked_add(size).ok_or_else(|| {
                CodecError::Io(format!(
                    "Buffer overflow when incrementing size value: {size}"
                ))
            })?;
            if budget.allocated > frame {
                return Err(CodecError::Io(format!(
                    "Estimated allocated buffer size {} is larger than frame size {frame}",
                    budget.allocated
                )));
            }
        }
        if self.max_frame_size_enabled && size as i64 > self.max_frame_size {
            return Err(CodecError::FrameSize(size as i64, self.max_frame_size));
        }
        Ok(())
    }

    fn cache_get(&self, index: i16) -> R<Option<DataStructure>> {
        self.unmarshal_cache
            .get(index as usize)
            .cloned()
            .filter(|_| index >= 0)
            .ok_or_else(|| CodecError::Io(format!("ArrayIndexOutOfBoundsException: {index}")))
    }

    fn cache_set(&mut self, index: i16, v: Option<DataStructure>) -> R<()> {
        if index == -1 {
            return Ok(());
        }
        match self
            .unmarshal_cache
            .get_mut(index as usize)
            .filter(|_| index >= 0)
        {
            Some(slot) => {
                *slot = v;
                Ok(())
            }
            None => Err(CodecError::Io(format!(
                "ArrayIndexOutOfBoundsException: {index}"
            ))),
        }
    }
}

/// The `MarshallingContext` of an unmarshal: what the byte arrays read so
/// far would allocate, against the frame size.
struct Budget {
    frame_size: Option<i32>,
    allocated: i32,
}

/// The length a string takes in modified UTF-8 and whether it is all ASCII
/// (1 to 127), as `tightMarshalString1` works them out.
fn tight_string_size(s: &str) -> R<(usize, bool)> {
    let mut len = 0;
    let mut ascii = true;
    for c in s.encode_utf16() {
        match c {
            1..=0x7f => len += 1,
            0x800.. => {
                len += 3;
                ascii = false
            }
            _ => {
                len += 2;
                ascii = false
            }
        }
    }
    if len >= 32767 {
        return Err(CodecError::Io(
            "Encountered a String value that is too long to encode.".into(),
        ));
    }
    Ok((len, ascii))
}

/// The message a version writes: v1 and v10 cut it to 1024 UTF-16 units
/// (`cutMessageIfNeeded`, which throws on a null message).
fn throwable_message(version: u32, t: &Throwable) -> R<Option<std::borrow::Cow<'_, str>>> {
    use std::borrow::Cow;
    if !matches!(version, 1 | 10) {
        return Ok(t.message.as_deref().map(Cow::Borrowed));
    }
    let m = t
        .message
        .as_deref()
        .ok_or_else(|| CodecError::Io("NullPointerException: exception message".into()))?;
    let units: Vec<u16> = m.encode_utf16().collect();
    if units.len() <= 1024 {
        return Ok(Some(Cow::Borrowed(m)));
    }
    let mut cut = String::from_utf16_lossy(&units[..1021]);
    cut.push_str("...");
    Ok(Some(Cow::Owned(cut)))
}

fn throwable_class(t: &Throwable) -> R<&str> {
    // Java writes getClass().getName(), which is never null.
    t.class
        .as_deref()
        .ok_or_else(|| CodecError::Io("NullPointerException: throwable class".into()))
}

// ---------------------------------------------------------------- tight

/// `tightMarshal1`: sets the booleans and adds up the size.
struct TightSizer<'a> {
    wf: &'a mut WireFormat,
    bs: &'a mut BooleanStream,
    size: usize,
    /// The cache index of each cached field, for the second pass.
    indexes: Vec<i16>,
}

impl TightSizer<'_> {
    fn nested_ds<T: Structure>(&mut self, v: Option<&T>) -> R<()> {
        self.bs.write(v.is_some());
        if let Some(v) = v {
            if marshall_aware(v.data_type()) {
                self.bs.write(false);
            }
            self.size += 1;
            v.write_fields(self.wf.version, self)?;
        }
        Ok(())
    }
}

impl Writer for TightSizer<'_> {
    fn bool(&mut self, _: &'static str, v: bool) -> R<()> {
        self.bs.write(v);
        Ok(())
    }
    fn byte(&mut self, _: &'static str, _: i8) -> R<()> {
        self.size += 1;
        Ok(())
    }
    fn short(&mut self, _: &'static str, _: i16) -> R<()> {
        self.size += 2;
        Ok(())
    }
    fn int(&mut self, _: &'static str, _: i32) -> R<()> {
        self.size += 4;
        Ok(())
    }
    fn long(&mut self, _: &'static str, o: i64) -> R<()> {
        if o == 0 {
            self.bs.write(false);
            self.bs.write(false);
        } else if o as u64 & 0xffff_ffff_ffff_0000 == 0 {
            self.bs.write(false);
            self.bs.write(true);
            self.size += 2;
        } else if o as u64 & 0xffff_ffff_0000_0000 == 0 {
            self.bs.write(true);
            self.bs.write(false);
            self.size += 4;
        } else {
            self.bs.write(true);
            self.bs.write(true);
            self.size += 8;
        }
        Ok(())
    }
    fn string(&mut self, _: &'static str, v: Option<&str>) -> R<()> {
        self.bs.write(v.is_some());
        if let Some(s) = v {
            let (len, ascii) = tight_string_size(s)?;
            self.bs.write(ascii);
            self.size += len + 2;
        }
        Ok(())
    }
    fn bytes(&mut self, _: &'static str, v: Option<&[u8]>) -> R<()> {
        self.bs.write(v.is_some());
        if let Some(b) = v {
            self.size += b.len() + 4;
        }
        Ok(())
    }
    fn const_bytes(&mut self, _: &'static str, _: &[u8], size: usize) -> R<()> {
        self.size += size;
        Ok(())
    }
    fn throwable(&mut self, n: &'static str, v: Option<&Throwable>) -> R<()> {
        let Some(t) = v else {
            self.bs.write(false);
            return Ok(());
        };
        self.bs.write(true);
        self.string(n, Some(throwable_class(t)?))?;
        self.string(n, throwable_message(self.wf.version, t)?.as_deref())?;
        if self.wf.stack_trace_enabled {
            self.size += 2;
            for e in &t.stack_trace {
                self.string(n, e.class_name.as_deref())?;
                self.string(n, e.method_name.as_deref())?;
                self.string(n, e.file_name.as_deref())?;
                self.size += 4;
            }
            self.throwable(n, t.cause.as_deref())?;
        }
        Ok(())
    }
    fn nested<T: Structure>(&mut self, _: &'static str, v: Option<&T>) -> R<()> {
        self.nested_ds(v)
    }
    fn cached<T: Structure>(&mut self, _: &'static str, v: Option<&T>) -> R<()> {
        if !self.wf.cache_enabled {
            return self.nested_ds(v);
        }
        let key = self.wf.marshal_cache.key(v);
        match self.wf.marshal_cache.map.get(&key).copied() {
            Some(index) => {
                self.bs.write(false);
                self.indexes.push(index);
                self.size += 2;
            }
            None => {
                self.bs.write(true);
                // The second pass writes this index before the nested
                // object's own: keep its place.
                let at = self.indexes.len();
                self.indexes.push(-1);
                self.nested_ds(v)?;
                self.indexes[at] = self.wf.marshal_cache.add(key);
                self.size += 2;
            }
        }
        Ok(())
    }
    fn array<T: Structure>(&mut self, _: &'static str, v: Option<&[T]>) -> R<()> {
        self.bs.write(v.is_some());
        if let Some(a) = v {
            self.size += 2;
            for e in a {
                self.nested_ds(Some(e))?;
            }
        }
        Ok(())
    }
}

/// `tightMarshal2`: writes the bytes, reading the booleans back.
struct TightWriter<'a> {
    wf: &'a mut WireFormat,
    bs: &'a mut BooleanStream,
    out: &'a mut Vec<u8>,
    indexes: std::vec::IntoIter<i16>,
}

impl TightWriter<'_> {
    fn nested_ds<T: Structure>(&mut self, v: Option<&T>) -> R<()> {
        if self.bs.read()? {
            let v = v.expect("set in the first pass");
            let t = v.data_type();
            self.out.push(t);
            if marshall_aware(t) && self.bs.read()? {
                return Err(CodecError::Io("Corrupted stream".into()));
            }
            v.write_fields(self.wf.version, self)?;
        }
        Ok(())
    }
}

impl Writer for TightWriter<'_> {
    fn bool(&mut self, _: &'static str, _: bool) -> R<()> {
        self.bs.read()?;
        Ok(())
    }
    fn byte(&mut self, _: &'static str, v: i8) -> R<()> {
        self.out.push(v as u8);
        Ok(())
    }
    fn short(&mut self, _: &'static str, v: i16) -> R<()> {
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn int(&mut self, _: &'static str, v: i32) -> R<()> {
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn long(&mut self, _: &'static str, o: i64) -> R<()> {
        if self.bs.read()? {
            if self.bs.read()? {
                self.out.extend_from_slice(&o.to_be_bytes());
            } else {
                self.out.extend_from_slice(&(o as i32).to_be_bytes());
            }
        } else if self.bs.read()? {
            self.out.extend_from_slice(&(o as i16).to_be_bytes());
        }
        Ok(())
    }
    fn string(&mut self, _: &'static str, v: Option<&str>) -> R<()> {
        if self.bs.read()? {
            let s = v.expect("set in the first pass");
            if self.bs.read()? {
                let units: Vec<u16> = s.encode_utf16().collect();
                self.out
                    .extend_from_slice(&(units.len() as i16).to_be_bytes());
                self.out.extend(units.iter().map(|&c| c as u8));
            } else {
                write_utf(self.out, s)?;
            }
        }
        Ok(())
    }
    fn bytes(&mut self, _: &'static str, v: Option<&[u8]>) -> R<()> {
        if self.bs.read()? {
            let b = v.expect("set in the first pass");
            self.out.extend_from_slice(&(b.len() as i32).to_be_bytes());
            self.out.extend_from_slice(b);
        }
        Ok(())
    }
    fn const_bytes(&mut self, _: &'static str, v: &[u8], size: usize) -> R<()> {
        let b = v
            .get(..size)
            .ok_or_else(|| CodecError::Io("IndexOutOfBoundsException".into()))?;
        self.out.extend_from_slice(b);
        Ok(())
    }
    fn throwable(&mut self, n: &'static str, v: Option<&Throwable>) -> R<()> {
        if self.bs.read()? {
            let t = v.expect("set in the first pass");
            self.string(n, Some(throwable_class(t)?))?;
            self.string(n, throwable_message(self.wf.version, t)?.as_deref())?;
            if self.wf.stack_trace_enabled {
                self.out
                    .extend_from_slice(&(t.stack_trace.len() as i16).to_be_bytes());
                for e in &t.stack_trace {
                    self.string(n, e.class_name.as_deref())?;
                    self.string(n, e.method_name.as_deref())?;
                    self.string(n, e.file_name.as_deref())?;
                    self.out.extend_from_slice(&e.line_number.to_be_bytes());
                }
                self.throwable(n, t.cause.as_deref())?;
            }
        }
        Ok(())
    }
    fn nested<T: Structure>(&mut self, _: &'static str, v: Option<&T>) -> R<()> {
        self.nested_ds(v)
    }
    fn cached<T: Structure>(&mut self, _: &'static str, v: Option<&T>) -> R<()> {
        if !self.wf.cache_enabled {
            return self.nested_ds(v);
        }
        let index = self.indexes.next().expect("set in the first pass");
        let new = self.bs.read()?;
        self.out.extend_from_slice(&index.to_be_bytes());
        if new {
            self.nested_ds(v)?;
        }
        Ok(())
    }
    fn array<T: Structure>(&mut self, _: &'static str, v: Option<&[T]>) -> R<()> {
        if self.bs.read()? {
            let a = v.expect("set in the first pass");
            self.out.extend_from_slice(&(a.len() as i16).to_be_bytes());
            for e in a {
                self.nested_ds(Some(e))?;
            }
        }
        Ok(())
    }
}

/// `tightUnmarshal`.
struct TightReader<'a, 'b> {
    wf: &'a mut WireFormat,
    bs: BooleanStream,
    input: Input<'b>,
    budget: Budget,
}

impl TightReader<'_, '_> {
    fn nested_ds(&mut self) -> R<Option<DataStructure>> {
        if !self.bs.read()? {
            return Ok(None);
        }
        let t = self.input.u8()?;
        if !crate::commands::known_type(self.wf.version, t) {
            return Err(CodecError::UnknownType(t as i8));
        }
        let version = self.wf.version;
        if marshall_aware(t) && self.bs.read()? {
            // A structure marshalled on its own, with its own size and
            // booleans.
            self.input.i32()?;
            self.input.u8()?;
            let inner = BooleanStream::unmarshal(&mut self.input)?;
            let outer = std::mem::replace(&mut self.bs, inner);
            let r = DataStructure::read_body(t, version, self);
            self.bs = outer;
            r.map(Some)
        } else {
            DataStructure::read_body(t, version, self).map(Some)
        }
    }

    fn string_raw(&mut self) -> R<Option<String>> {
        if !self.bs.read()? {
            return Ok(None);
        }
        if self.bs.read()? {
            let size = self.input.i16()?;
            if size < 0 {
                return Err(CodecError::Io("NegativeArraySizeException".into()));
            }
            // new String(bytes, 0): each byte is a char.
            let b = self.input.take(size as usize)?;
            Ok(Some(b.iter().map(|&c| c as char).collect()))
        } else {
            self.input.utf().map(Some)
        }
    }
}

fn some_ds<T: Structure>(ds: Option<DataStructure>) -> R<Option<T>> {
    ds.map(T::from_ds).transpose()
}

impl Reader for TightReader<'_, '_> {
    fn bool(&mut self) -> R<bool> {
        self.bs.read()
    }
    fn byte(&mut self) -> R<i8> {
        Ok(self.input.u8()? as i8)
    }
    fn short(&mut self) -> R<i16> {
        self.input.i16()
    }
    fn int(&mut self) -> R<i32> {
        self.input.i32()
    }
    fn long(&mut self) -> R<i64> {
        if self.bs.read()? {
            if self.bs.read()? {
                self.input.i64()
            } else {
                Ok(self.input.i32()? as u32 as i64)
            }
        } else if self.bs.read()? {
            Ok(self.input.i16()? as u16 as i64)
        } else {
            Ok(0)
        }
    }
    fn string(&mut self) -> R<Option<String>> {
        self.string_raw()
    }
    fn bytes(&mut self) -> R<Option<Vec<u8>>> {
        if !self.bs.read()? {
            return Ok(None);
        }
        let size = self.input.i32()?;
        self.wf.check_buffer_size(&mut self.budget, size)?;
        Ok(Some(self.input.take(size as usize)?.to_vec()))
    }
    fn const_bytes(&mut self, size: usize) -> R<Vec<u8>> {
        Ok(self.input.take(size)?.to_vec())
    }
    fn throwable(&mut self) -> R<Option<Box<Throwable>>> {
        if !self.bs.read()? {
            return Ok(None);
        }
        let mut t = Throwable {
            class: self.string_raw()?,
            message: self.string_raw()?,
            ..Default::default()
        };
        if self.wf.stack_trace_enabled {
            let n = self.input.i16()?;
            if n < 0 {
                return Err(CodecError::Io("NegativeArraySizeException".into()));
            }
            for _ in 0..n {
                t.stack_trace.push(StackTraceElement {
                    class_name: self.string_raw()?,
                    method_name: self.string_raw()?,
                    file_name: self.string_raw()?,
                    line_number: self.input.i32()?,
                });
            }
            t.cause = self.throwable()?;
        }
        Ok(Some(Box::new(t)))
    }
    fn nested<T: Structure>(&mut self) -> R<Option<T>> {
        let ds = self.nested_ds()?;
        some_ds(ds)
    }
    fn cached<T: Structure>(&mut self) -> R<Option<T>> {
        if !self.wf.cache_enabled {
            return self.nested();
        }
        if self.bs.read()? {
            let index = self.input.i16()?;
            let ds = self.nested_ds()?;
            self.wf.cache_set(index, ds.clone())?;
            some_ds(ds)
        } else {
            let index = self.input.i16()?;
            let ds = self.wf.cache_get(index)?;
            some_ds(ds)
        }
    }
    fn array<T: Structure>(&mut self) -> R<Option<Vec<T>>> {
        if !self.bs.read()? {
            return Ok(None);
        }
        let n = self.input.i16()?;
        if n < 0 {
            return Err(CodecError::Io("NegativeArraySizeException".into()));
        }
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..n {
            // Java keeps a null element; Saranga refuses the array.
            let e = self
                .nested()?
                .ok_or_else(|| CodecError::Io("null array element".into()))?;
            v.push(e);
        }
        Ok(Some(v))
    }
}

// ---------------------------------------------------------------- loose

struct LooseWriter<'a> {
    wf: &'a mut WireFormat,
    out: &'a mut Vec<u8>,
}

impl LooseWriter<'_> {
    fn nested_ds<T: Structure>(&mut self, v: Option<&T>) -> R<()> {
        self.out.push(v.is_some() as u8);
        if let Some(v) = v {
            self.out.push(v.data_type());
            v.write_fields(self.wf.version, self)?;
        }
        Ok(())
    }
}

impl Writer for LooseWriter<'_> {
    fn bool(&mut self, _: &'static str, v: bool) -> R<()> {
        self.out.push(v as u8);
        Ok(())
    }
    fn byte(&mut self, _: &'static str, v: i8) -> R<()> {
        self.out.push(v as u8);
        Ok(())
    }
    fn short(&mut self, _: &'static str, v: i16) -> R<()> {
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn int(&mut self, _: &'static str, v: i32) -> R<()> {
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn long(&mut self, _: &'static str, v: i64) -> R<()> {
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn string(&mut self, _: &'static str, v: Option<&str>) -> R<()> {
        self.out.push(v.is_some() as u8);
        if let Some(s) = v {
            write_utf(self.out, s)?;
        }
        Ok(())
    }
    fn bytes(&mut self, _: &'static str, v: Option<&[u8]>) -> R<()> {
        self.out.push(v.is_some() as u8);
        if let Some(b) = v {
            self.out.extend_from_slice(&(b.len() as i32).to_be_bytes());
            self.out.extend_from_slice(b);
        }
        Ok(())
    }
    fn const_bytes(&mut self, _: &'static str, v: &[u8], size: usize) -> R<()> {
        let b = v
            .get(..size)
            .ok_or_else(|| CodecError::Io("IndexOutOfBoundsException".into()))?;
        self.out.extend_from_slice(b);
        Ok(())
    }
    fn throwable(&mut self, n: &'static str, v: Option<&Throwable>) -> R<()> {
        self.out.push(v.is_some() as u8);
        if let Some(t) = v {
            self.string(n, Some(throwable_class(t)?))?;
            self.string(n, throwable_message(self.wf.version, t)?.as_deref())?;
            if self.wf.stack_trace_enabled {
                self.out
                    .extend_from_slice(&(t.stack_trace.len() as i16).to_be_bytes());
                for e in &t.stack_trace {
                    self.string(n, e.class_name.as_deref())?;
                    self.string(n, e.method_name.as_deref())?;
                    self.string(n, e.file_name.as_deref())?;
                    self.out.extend_from_slice(&e.line_number.to_be_bytes());
                }
                self.throwable(n, t.cause.as_deref())?;
            }
        }
        Ok(())
    }
    fn nested<T: Structure>(&mut self, _: &'static str, v: Option<&T>) -> R<()> {
        self.nested_ds(v)
    }
    fn cached<T: Structure>(&mut self, _: &'static str, v: Option<&T>) -> R<()> {
        if !self.wf.cache_enabled {
            return self.nested_ds(v);
        }
        let key = self.wf.marshal_cache.key(v);
        match self.wf.marshal_cache.map.get(&key).copied() {
            Some(index) => {
                self.out.push(0);
                self.out.extend_from_slice(&index.to_be_bytes());
            }
            None => {
                self.out.push(1);
                let index = self.wf.marshal_cache.add(key);
                self.out.extend_from_slice(&index.to_be_bytes());
                self.nested_ds(v)?;
            }
        }
        Ok(())
    }
    fn array<T: Structure>(&mut self, _: &'static str, v: Option<&[T]>) -> R<()> {
        self.out.push(v.is_some() as u8);
        if let Some(a) = v {
            self.out.extend_from_slice(&(a.len() as i16).to_be_bytes());
            for e in a {
                self.nested_ds(Some(e))?;
            }
        }
        Ok(())
    }
}

struct LooseReader<'a, 'b> {
    wf: &'a mut WireFormat,
    input: Input<'b>,
    budget: Budget,
}

impl LooseReader<'_, '_> {
    fn nested_ds(&mut self) -> R<Option<DataStructure>> {
        if !self.bool()? {
            return Ok(None);
        }
        let t = self.input.u8()?;
        let version = self.wf.version;
        DataStructure::read_body(t, version, self).map(Some)
    }
}

impl Reader for LooseReader<'_, '_> {
    fn bool(&mut self) -> R<bool> {
        Ok(self.input.u8()? != 0)
    }
    fn byte(&mut self) -> R<i8> {
        Ok(self.input.u8()? as i8)
    }
    fn short(&mut self) -> R<i16> {
        self.input.i16()
    }
    fn int(&mut self) -> R<i32> {
        self.input.i32()
    }
    fn long(&mut self) -> R<i64> {
        self.input.i64()
    }
    fn string(&mut self) -> R<Option<String>> {
        if self.bool()? {
            self.input.utf().map(Some)
        } else {
            Ok(None)
        }
    }
    fn bytes(&mut self) -> R<Option<Vec<u8>>> {
        if !self.bool()? {
            return Ok(None);
        }
        let size = self.input.i32()?;
        self.wf.check_buffer_size(&mut self.budget, size)?;
        Ok(Some(self.input.take(size as usize)?.to_vec()))
    }
    fn const_bytes(&mut self, size: usize) -> R<Vec<u8>> {
        Ok(self.input.take(size)?.to_vec())
    }
    fn throwable(&mut self) -> R<Option<Box<Throwable>>> {
        if !self.bool()? {
            return Ok(None);
        }
        let mut t = Throwable {
            class: self.string()?,
            message: self.string()?,
            ..Default::default()
        };
        if self.wf.stack_trace_enabled {
            let n = self.input.i16()?;
            if n < 0 {
                return Err(CodecError::Io("NegativeArraySizeException".into()));
            }
            for _ in 0..n {
                t.stack_trace.push(StackTraceElement {
                    class_name: self.string()?,
                    method_name: self.string()?,
                    file_name: self.string()?,
                    line_number: self.input.i32()?,
                });
            }
            t.cause = self.throwable()?;
        }
        Ok(Some(Box::new(t)))
    }
    fn nested<T: Structure>(&mut self) -> R<Option<T>> {
        let ds = self.nested_ds()?;
        some_ds(ds)
    }
    fn cached<T: Structure>(&mut self) -> R<Option<T>> {
        if !self.wf.cache_enabled {
            return self.nested();
        }
        if self.bool()? {
            let index = self.input.i16()?;
            let ds = self.nested_ds()?;
            self.wf.cache_set(index, ds.clone())?;
            some_ds(ds)
        } else {
            let index = self.input.i16()?;
            let ds = self.wf.cache_get(index)?;
            some_ds(ds)
        }
    }
    fn array<T: Structure>(&mut self) -> R<Option<Vec<T>>> {
        if !self.bool()? {
            return Ok(None);
        }
        let n = self.input.i16()?;
        if n < 0 {
            return Err(CodecError::Io("NegativeArraySizeException".into()));
        }
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let e = self
                .nested()?
                .ok_or_else(|| CodecError::Io("null array element".into()))?;
            v.push(e);
        }
        Ok(Some(v))
    }
}

// ---------------------------------------------------------------- dump

/// A structure as text: `Class{Field=value,...}` with the fields the
/// version marshals, in order. Strings are quoted with every UTF-16 unit
/// outside printable ASCII as `\uXXXX`; byte arrays are hex.
pub fn dump(ds: &DataStructure, version: u32, stack_traces: bool) -> String {
    let mut d = Dump {
        out: String::new(),
        first: true,
        version,
        stack_traces,
    };
    d.structure(ds);
    d.out
}

struct Dump {
    out: String,
    first: bool,
    version: u32,
    stack_traces: bool,
}

fn quote(out: &mut String, s: &str) {
    out.push('"');
    for c in s.encode_utf16() {
        if (0x20..0x7f).contains(&c) && c != b'"' as u16 && c != b'\\' as u16 {
            out.push(c as u8 as char);
        } else {
            let _ = write!(out, "\\u{c:04x}");
        }
    }
    out.push('"');
}

impl Dump {
    fn field(&mut self, name: &str) {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
        self.out.push_str(name);
        self.out.push('=');
    }

    fn structure<T: Structure>(&mut self, v: &T) {
        self.out
            .push_str(crate::commands::class_name(v.data_type()));
        self.out.push('{');
        let first = std::mem::replace(&mut self.first, true);
        let _ = v.write_fields(self.version, self);
        self.first = first;
        self.out.push('}');
    }

    fn opt_str(&mut self, v: Option<&str>) {
        match v {
            Some(s) => quote(&mut self.out, s),
            None => self.out.push_str("null"),
        }
    }

    fn throwable_text(&mut self, t: &Throwable) {
        self.out.push_str("Throwable{class=");
        self.opt_str(t.class.as_deref());
        self.out.push_str(",message=");
        self.opt_str(t.message.as_deref());
        if self.stack_traces {
            self.out.push_str(",stack=[");
            for (i, e) in t.stack_trace.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                self.out.push('(');
                self.opt_str(e.class_name.as_deref());
                self.out.push(',');
                self.opt_str(e.method_name.as_deref());
                self.out.push(',');
                self.opt_str(e.file_name.as_deref());
                let _ = write!(self.out, ",{})", e.line_number);
            }
            self.out.push_str("],cause=");
            match &t.cause {
                Some(c) => self.throwable_text(c),
                None => self.out.push_str("null"),
            }
        }
        self.out.push('}');
    }
}

impl Writer for Dump {
    fn bool(&mut self, n: &'static str, v: bool) -> R<()> {
        self.field(n);
        let _ = write!(self.out, "{v}");
        Ok(())
    }
    fn byte(&mut self, n: &'static str, v: i8) -> R<()> {
        self.field(n);
        let _ = write!(self.out, "{v}");
        Ok(())
    }
    fn short(&mut self, n: &'static str, v: i16) -> R<()> {
        self.field(n);
        let _ = write!(self.out, "{v}");
        Ok(())
    }
    fn int(&mut self, n: &'static str, v: i32) -> R<()> {
        self.field(n);
        let _ = write!(self.out, "{v}");
        Ok(())
    }
    fn long(&mut self, n: &'static str, v: i64) -> R<()> {
        self.field(n);
        let _ = write!(self.out, "{v}");
        Ok(())
    }
    fn string(&mut self, n: &'static str, v: Option<&str>) -> R<()> {
        self.field(n);
        self.opt_str(v);
        Ok(())
    }
    fn bytes(&mut self, n: &'static str, v: Option<&[u8]>) -> R<()> {
        self.field(n);
        match v {
            Some(b) => {
                self.out.push('[');
                for x in b {
                    let _ = write!(self.out, "{x:02x}");
                }
                self.out.push(']');
            }
            None => self.out.push_str("null"),
        }
        Ok(())
    }
    fn const_bytes(&mut self, n: &'static str, v: &[u8], _: usize) -> R<()> {
        self.bytes(n, Some(v))
    }
    fn throwable(&mut self, n: &'static str, v: Option<&Throwable>) -> R<()> {
        self.field(n);
        match v {
            Some(t) => self.throwable_text(t),
            None => self.out.push_str("null"),
        }
        Ok(())
    }
    fn nested<T: Structure>(&mut self, n: &'static str, v: Option<&T>) -> R<()> {
        self.field(n);
        match v {
            Some(v) => self.structure(v),
            None => self.out.push_str("null"),
        }
        Ok(())
    }
    fn cached<T: Structure>(&mut self, n: &'static str, v: Option<&T>) -> R<()> {
        self.nested(n, v)
    }
    fn array<T: Structure>(&mut self, n: &'static str, v: Option<&[T]>) -> R<()> {
        self.field(n);
        match v {
            Some(a) => {
                self.out.push('[');
                for (i, e) in a.iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    self.structure(e);
                }
                self.out.push(']');
            }
            None => self.out.push_str("null"),
        }
        Ok(())
    }
}
