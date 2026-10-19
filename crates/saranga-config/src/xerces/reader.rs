//! The readers an entity's characters come through, each returning the
//! characters its Java original returns from one `read` call: how much an
//! entity loads at a time decides where the scanner's buffer ends, and with
//! it some of the line and column numbers the JDK reports.

use super::{Arg, Stop};

/// `XMLEntityManager.DEFAULT_XMLDECL_BUFFER_SIZE` as `RewindableInputStream`
/// uses it: `Entity.ScannedEntity.DEFAULT_XMLDECL_BUFFER_SIZE`.
const XMLDECL_CHUNK: usize = 28;

/// The flags of the entity a stream belongs to that `RewindableInputStream`
/// reads.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Chunking {
    pub may_read_chunks: bool,
    pub xml_decl_chunk_read: bool,
}

/// A `FileInputStream` over the file's bytes wrapped in `XMLEntityManager`'s
/// `RewindableInputStream`.
#[derive(Debug)]
pub(crate) struct Stream {
    bytes: Vec<u8>,
    /// The underlying stream's position.
    at: usize,
    data: Vec<u8>,
    start_offset: usize,
    end_offset: Option<usize>,
    offset: usize,
    length: usize,
}

impl Stream {
    pub(crate) fn new(bytes: Vec<u8>) -> Stream {
        Stream {
            bytes,
            at: 0,
            data: Vec::new(),
            start_offset: 0,
            end_offset: None,
            offset: 0,
            length: 0,
        }
    }

    fn raw_read(&mut self) -> Option<u8> {
        let b = self.bytes.get(self.at).copied();
        if b.is_some() {
            self.at += 1;
        }
        b
    }

    fn raw_read_into(&mut self, buf: &mut [u8], off: usize, len: usize) -> isize {
        if len == 0 {
            return 0;
        }
        let left = self.bytes.len() - self.at;
        if left == 0 {
            return -1;
        }
        let n = len.min(left);
        buf[off..off + n].copy_from_slice(&self.bytes[self.at..self.at + n]);
        self.at += n;
        n as isize
    }

    pub(crate) fn read_and_buffer(&mut self) -> i32 {
        match self.raw_read() {
            None => {
                self.end_offset = Some(self.offset);
                -1
            }
            Some(b) => {
                if self.length == self.data.len() {
                    self.data.push(b);
                } else {
                    self.data[self.length] = b;
                }
                self.length += 1;
                self.offset += 1;
                b as i32
            }
        }
    }

    pub(crate) fn reset(&mut self) {
        self.offset = self.start_offset;
    }

    pub(crate) fn read_byte(&mut self, chunking: Chunking) -> i32 {
        if self.offset < self.length {
            let b = self.data[self.offset];
            self.offset += 1;
            return b as i32;
        }
        if Some(self.offset) == self.end_offset {
            return -1;
        }
        if chunking.may_read_chunks {
            return self.raw_read().map_or(-1, i32::from);
        }
        self.read_and_buffer()
    }

    pub(crate) fn read(
        &mut self,
        buf: &mut [u8],
        off: usize,
        len: usize,
        chunking: &mut Chunking,
    ) -> isize {
        let left = self.length - self.offset;
        if left == 0 {
            if Some(self.offset) == self.end_offset {
                return -1;
            }
            if chunking.may_read_chunks || !chunking.xml_decl_chunk_read {
                let mut len = len;
                if !chunking.xml_decl_chunk_read {
                    chunking.xml_decl_chunk_read = true;
                    len = XMLDECL_CHUNK;
                }
                return self.raw_read_into(buf, off, len);
            }
            let b = self.read_and_buffer();
            if b == -1 {
                self.end_offset = Some(self.offset);
                return -1;
            }
            buf[off] = b as u8;
            return 1;
        }
        let len = if len < left {
            if len == 0 {
                return 0;
            }
            len
        } else {
            left
        };
        buf[off..off + len].copy_from_slice(&self.data[self.offset..self.offset + len]);
        self.offset += len;
        len as isize
    }

    pub(crate) fn skip(&mut self, n: usize) {
        let left = self.length - self.offset;
        if left == 0 {
            if Some(self.offset) != self.end_offset {
                self.at = (self.at + n).min(self.bytes.len());
            }
            return;
        }
        if n <= left {
            self.offset += n;
            return;
        }
        self.offset += left;
        if Some(self.offset) == self.end_offset {
            return;
        }
        self.at = (self.at + n - left).min(self.bytes.len());
    }
}

/// Where a reader's input comes from.
#[derive(Debug)]
pub(crate) enum Reader {
    /// A `StringReader` (the document `XMLUtil` parses, an internal entity).
    Chars { text: Vec<u16>, at: usize },
    /// Xerces's `UTF8Reader`.
    Utf8 { buffer: Vec<u8>, offset: usize },
    /// Xerces's `ASCIIReader`.
    Ascii { buffer: Vec<u8> },
    /// Xerces's `UTF16Reader`.
    Utf16 { buffer: Vec<u8>, big_endian: bool },
    /// Xerces's `UCSReader`: 4 or 2 bytes a character.
    Ucs {
        buffer: Vec<u8>,
        four: bool,
        big_endian: bool,
    },
    /// An `InputStreamReader` on a Java charset.
    Java(JavaDecoder),
}

impl Reader {
    pub(crate) fn chars(text: Vec<u16>) -> Reader {
        Reader::Chars { text, at: 0 }
    }

    pub(crate) fn utf8(size: usize) -> Reader {
        Reader::Utf8 {
            buffer: vec![0; size],
            offset: 0,
        }
    }

    pub(crate) fn ascii(size: usize) -> Reader {
        Reader::Ascii {
            buffer: vec![0; size],
        }
    }

    pub(crate) fn utf16(size: usize, big_endian: bool) -> Reader {
        Reader::Utf16 {
            buffer: vec![0; size],
            big_endian,
        }
    }

    pub(crate) fn ucs(four: bool, big_endian: bool) -> Reader {
        Reader::Ucs {
            buffer: vec![0; 8192],
            four,
            big_endian,
        }
    }

    /// `Reader.read(ch, offset, length)`: -1 at the end.
    pub(crate) fn read(
        &mut self,
        stream: Option<&mut Stream>,
        chunking: &mut Chunking,
        ch: &mut [u16],
        offset: usize,
        length: usize,
    ) -> Result<isize, Stop> {
        match self {
            Reader::Chars { text, at } => {
                if length == 0 {
                    return Ok(0);
                }
                if *at >= text.len() {
                    return Ok(-1);
                }
                let n = length.min(text.len() - *at);
                ch[offset..offset + n].copy_from_slice(&text[*at..*at + n]);
                *at += n;
                Ok(n as isize)
            }
            Reader::Utf8 {
                buffer,
                offset: saved,
            } => {
                let stream = stream.expect("a UTF-8 entity has a stream");
                utf8_read(buffer, saved, stream, chunking, ch, offset, length)
            }
            Reader::Ascii { buffer } => {
                let stream = stream.expect("an ASCII entity has a stream");
                let length = length.min(buffer.len());
                let count = stream.read(buffer, 0, length, chunking);
                for i in 0..count.max(0) as usize {
                    let b = buffer[i];
                    if b >= 0x80 {
                        return Err(Stop::Malformed {
                            key: "InvalidASCII",
                            args: vec![Arg::Str(b.to_string())],
                        });
                    }
                    ch[offset + i] = b as u16;
                }
                Ok(count)
            }
            Reader::Utf16 { buffer, big_endian } => {
                let stream = stream.expect("a UTF-16 entity has a stream");
                let byte_length = (length << 1).min(buffer.len());
                let mut count = stream.read(buffer, 0, byte_length, chunking);
                if count == -1 {
                    return Ok(-1);
                }
                if count & 1 != 0 {
                    let b = stream.read_byte(*chunking);
                    if b == -1 {
                        return Err(Stop::Malformed {
                            key: "ExpectedByte",
                            args: vec![Arg::Str("2".into()), Arg::Str("2".into())],
                        });
                    }
                    if buffer.len() <= count as usize {
                        buffer.push(0);
                    }
                    buffer[count as usize] = b as u8;
                    count += 1;
                }
                let n = count as usize >> 1;
                for i in 0..n {
                    let (b0, b1) = (buffer[2 * i] as u16, buffer[2 * i + 1] as u16);
                    ch[offset + i] = if *big_endian {
                        b0 << 8 | b1
                    } else {
                        b1 << 8 | b0
                    };
                }
                Ok(n as isize)
            }
            Reader::Ucs {
                buffer,
                four,
                big_endian,
            } => {
                let stream = stream.expect("a UCS entity has a stream");
                let shift = if *four { 2 } else { 1 };
                let byte_length = (length << shift).min(buffer.len());
                let count = stream.read(buffer, 0, byte_length, chunking);
                if count == -1 {
                    return Ok(-1);
                }
                let mut count = count as usize;
                let width = 1 << shift;
                let missing = (width - (count & (width - 1))) & (width - 1);
                if buffer.len() < count + missing {
                    buffer.resize(count + missing, 0);
                }
                let mut i = 0;
                while i < missing {
                    let b = stream.read_byte(*chunking);
                    if b == -1 {
                        for j in i..missing {
                            buffer[count + j] = 0;
                        }
                        break;
                    }
                    buffer[count + i] = b as u8;
                    i += 1;
                }
                count += missing;
                let n = count >> shift;
                for i in 0..n {
                    let b = &buffer[i * width..i * width + width];
                    let c: u32 = if *four {
                        let (b0, b1, b2, b3) = (b[0] as u32, b[1] as u32, b[2] as u32, b[3] as u32);
                        if *big_endian {
                            (b0 << 24) + (b1 << 16) + (b2 << 8) + b3
                        } else {
                            (b3 << 24) + (b2 << 16) + (b1 << 8) + b0
                        }
                    } else if *big_endian {
                        ((b[0] as u32) << 8) + b[1] as u32
                    } else {
                        ((b[1] as u32) << 8) + b[0] as u32
                    };
                    ch[offset + i] = c as u16;
                }
                Ok(n as isize)
            }
            Reader::Java(decoder) => {
                let stream = stream.expect("a Java charset entity has a stream");
                if length == 0 {
                    return Ok(0);
                }
                // StreamDecoder: decodes what it has read, reading on while
                // the stream says more is ready and there is room.
                let mut n = 0usize;
                let mut eof = false;
                loop {
                    n += decoder.decode(&mut ch[offset + n..offset + length], eof);
                    if n == length || eof || (n > 0 && !chunking.may_read_chunks) {
                        break;
                    }
                    let mut buf = vec![0u8; 8192];
                    let got = stream.read(&mut buf, 0, 8192, chunking);
                    if got <= 0 {
                        eof = true;
                        continue;
                    }
                    decoder.pending.extend_from_slice(&buf[..got as usize]);
                }
                Ok(if n == 0 { -1 } else { n as isize })
            }
        }
    }
}

/// What a Java charset decodes with.
#[derive(Debug)]
pub(crate) enum Charset {
    /// A one-byte charset: its 256 characters.
    SingleByte(Vec<u16>),
    /// `UTF-16` and its variants: big-endian unless a byte order mark (read
    /// when `bom`) says otherwise.
    Utf16 { big_endian: bool, bom: bool },
}

/// An `InputStreamReader`'s decoder, replacing what it cannot decode with
/// U+FFFD as Java's does.
#[derive(Debug)]
pub(crate) struct JavaDecoder {
    pub charset: Charset,
    pub pending: Vec<u8>,
}

impl JavaDecoder {
    /// Decodes into `out` what the pending bytes hold, the rest of them too
    /// at the end of the input.
    fn decode(&mut self, out: &mut [u16], eof: bool) -> usize {
        let mut n = 0;
        let mut used = 0;
        match &mut self.charset {
            Charset::SingleByte(table) => {
                while n < out.len() && used < self.pending.len() {
                    out[n] = table[self.pending[used] as usize];
                    n += 1;
                    used += 1;
                }
            }
            Charset::Utf16 { big_endian, bom } => {
                if *bom {
                    if self.pending.len() < 2 && !eof {
                        return 0;
                    }
                    if self.pending.len() >= 2 {
                        match (self.pending[0], self.pending[1]) {
                            (0xFE, 0xFF) => {
                                *big_endian = true;
                                used = 2;
                            }
                            (0xFF, 0xFE) => {
                                *big_endian = false;
                                used = 2;
                            }
                            _ => {}
                        }
                    }
                    *bom = false;
                }
                let unit = |p: &[u8], i: usize, be: bool| -> u16 {
                    if be {
                        (p[i] as u16) << 8 | p[i + 1] as u16
                    } else {
                        (p[i + 1] as u16) << 8 | p[i] as u16
                    }
                };
                while n < out.len() {
                    let left = self.pending.len() - used;
                    if left < 2 {
                        if eof && left == 1 {
                            out[n] = 0xFFFD;
                            n += 1;
                            used += 1;
                        }
                        break;
                    }
                    let c = unit(&self.pending, used, *big_endian);
                    if (0xD800..0xDC00).contains(&c) {
                        if left < 4 {
                            if eof {
                                out[n] = 0xFFFD;
                                n += 1;
                                used += 2;
                                continue;
                            }
                            break;
                        }
                        let d = unit(&self.pending, used + 2, *big_endian);
                        if (0xDC00..0xE000).contains(&d) {
                            if n + 1 >= out.len() {
                                break;
                            }
                            out[n] = c;
                            out[n + 1] = d;
                            n += 2;
                            used += 4;
                        } else {
                            out[n] = 0xFFFD;
                            n += 1;
                            used += 2;
                        }
                    } else if (0xDC00..0xE000).contains(&c) {
                        out[n] = 0xFFFD;
                        n += 1;
                        used += 2;
                    } else {
                        out[n] = c;
                        n += 1;
                        used += 2;
                    }
                }
            }
        }
        self.pending.drain(..used);
        n
    }
}

fn invalid_byte(position: u32, count: u32, _byte: i32) -> Stop {
    Stop::Malformed {
        key: "InvalidByte",
        args: vec![Arg::Str(position.to_string()), Arg::Str(count.to_string())],
    }
}

fn expected_byte(position: u32, count: u32) -> Stop {
    Stop::Malformed {
        key: "ExpectedByte",
        args: vec![Arg::Str(position.to_string()), Arg::Str(count.to_string())],
    }
}

/// `UTF8Reader.read`, with its habit of returning the characters before a
/// bad sequence and failing on the next call.
fn utf8_read(
    buffer: &mut Vec<u8>,
    saved: &mut usize,
    stream: &mut Stream,
    chunking: &mut Chunking,
    ch: &mut [u16],
    offset: usize,
    length: usize,
) -> Result<isize, Stop> {
    let mut out = offset;
    let mut count: isize;
    if *saved == 0 {
        let length = length.min(buffer.len());
        count = stream.read(buffer, 0, length, chunking);
        if count == -1 {
            return Ok(-1);
        }
        count += (out - offset) as isize;
    } else {
        count = *saved as isize;
        *saved = 0;
    }
    let total = count.max(0) as usize;
    let mut i = 0;
    // A byte read past what was buffered goes back into the buffer when a
    // sequence is put back for the next call.
    let next = |i: &mut usize, count: &mut isize, stream: &mut Stream, buffer: &[u8]| -> i32 {
        *i += 1;
        if *i < total {
            buffer[*i] as i32
        } else {
            let b = stream.read_byte(*chunking);
            if b != -1 {
                *count += 1;
            }
            b
        }
    };
    while i < total {
        let b0 = buffer[i] as i32;
        if b0 < 0x80 {
            ch[out] = b0 as u16;
            out += 1;
            i += 1;
            continue;
        }
        let put_back = |buffer: &mut Vec<u8>, saved: &mut usize, bytes: &[i32]| {
            for (k, b) in bytes.iter().enumerate() {
                buffer[k] = *b as u8;
            }
            *saved = bytes.len();
        };
        if (b0 & 0xE0) == 0xC0 && (b0 & 0x1E) != 0 {
            let b1 = next(&mut i, &mut count, stream, buffer);
            if b1 == -1 {
                if out > offset {
                    put_back(buffer, saved, &[b0]);
                    return Ok((out - offset) as isize);
                }
                return Err(expected_byte(2, 2));
            }
            if (b1 & 0xC0) != 0x80 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1]);
                    return Ok((out - offset) as isize);
                }
                return Err(invalid_byte(2, 2, b1));
            }
            ch[out] = (((b0 << 6) & 0x07C0) | (b1 & 0x3F)) as u16;
            out += 1;
            count -= 1;
            i += 1;
            continue;
        }
        if (b0 & 0xF0) == 0xE0 {
            let b1 = next(&mut i, &mut count, stream, buffer);
            if b1 == -1 {
                if out > offset {
                    put_back(buffer, saved, &[b0]);
                    return Ok((out - offset) as isize);
                }
                return Err(expected_byte(2, 3));
            }
            if (b1 & 0xC0) != 0x80
                || (b0 == 0xED && b1 >= 0xA0)
                || ((b0 & 0x0F) == 0 && (b1 & 0x20) == 0)
            {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1]);
                    return Ok((out - offset) as isize);
                }
                return Err(invalid_byte(2, 3, b1));
            }
            let b2 = next(&mut i, &mut count, stream, buffer);
            if b2 == -1 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1]);
                    return Ok((out - offset) as isize);
                }
                return Err(expected_byte(3, 3));
            }
            if (b2 & 0xC0) != 0x80 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1, b2]);
                    return Ok((out - offset) as isize);
                }
                return Err(invalid_byte(3, 3, b2));
            }
            ch[out] = (((b0 << 12) & 0xF000) | ((b1 << 6) & 0x0FC0) | (b2 & 0x3F)) as u16;
            out += 1;
            count -= 2;
            i += 1;
            continue;
        }
        if (b0 & 0xF8) == 0xF0 {
            let b1 = next(&mut i, &mut count, stream, buffer);
            if b1 == -1 {
                if out > offset {
                    put_back(buffer, saved, &[b0]);
                    return Ok((out - offset) as isize);
                }
                return Err(expected_byte(2, 4));
            }
            if (b1 & 0xC0) != 0x80 || ((b1 & 0x30) == 0 && (b0 & 0x07) == 0) {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1]);
                    return Ok((out - offset) as isize);
                }
                return Err(invalid_byte(2, 4, b1));
            }
            let b2 = next(&mut i, &mut count, stream, buffer);
            if b2 == -1 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1]);
                    return Ok((out - offset) as isize);
                }
                return Err(expected_byte(3, 4));
            }
            if (b2 & 0xC0) != 0x80 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1, b2]);
                    return Ok((out - offset) as isize);
                }
                return Err(invalid_byte(3, 4, b2));
            }
            let b3 = next(&mut i, &mut count, stream, buffer);
            if b3 == -1 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1, b2]);
                    return Ok((out - offset) as isize);
                }
                return Err(expected_byte(4, 4));
            }
            if (b3 & 0xC0) != 0x80 {
                if out > offset {
                    put_back(buffer, saved, &[b0, b1, b2, b3]);
                    return Ok((out - offset) as isize);
                }
                return Err(invalid_byte(4, 4, b2));
            }
            if out + 1 >= ch.len() {
                put_back(buffer, saved, &[b0, b1, b2, b3]);
                return Ok((out - offset) as isize);
            }
            let uuuuu = ((b0 << 2) & 0x1C) | ((b1 >> 4) & 0x03);
            if uuuuu > 0x10 {
                return Err(Stop::Malformed {
                    key: "InvalidHighSurrogate",
                    args: vec![Arg::Str(format!("{uuuuu:x}"))],
                });
            }
            let wwww = uuuuu - 1;
            let zzzz = b1 & 0x0F;
            let yyyyyy = b2 & 0x3F;
            let xxxxxx = b3 & 0x3F;
            ch[out] = (0xD800 | ((wwww << 6) & 0x03C0) | (zzzz << 2) | (yyyyyy >> 4)) as u16;
            ch[out + 1] = (0xDC00 | ((yyyyyy << 6) & 0x03C0) | xxxxxx) as u16;
            out += 2;
            count -= 2;
            i += 1;
            continue;
        }
        if out > offset {
            put_back(buffer, saved, &[b0]);
            return Ok((out - offset) as isize);
        }
        return Err(invalid_byte(1, 1, b0));
    }
    Ok(count)
}
