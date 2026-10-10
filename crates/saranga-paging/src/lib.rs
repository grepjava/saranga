//! Artemis page files: the format of `<paging-directory>/<uuid>/<n>.page`
//! (`PageReadWriter`, `PagedMessageImpl`, `PagingStoreFactoryNIO`).
//!
//! Each address that pages has a directory named by a UUID, holding
//! `address.txt` (the address, then a newline) and page files named by
//! their page number, zero-padded to nine digits. A page file is a run of
//! records:
//!
//! ```text
//! '{'  i32 size  PagedMessage (size bytes)  '}'
//! ```
//!
//! A `PagedMessage` is the transaction ID (-1 outside one), a byte saying
//! whether the message is large, the message, then the IDs of the queues it
//! was routed to. A regular message is written by its persister, exactly
//! as an `ADD_MESSAGE_PROTOCOL` record; a Core large message by its headers
//! and properties (its body is the `<id>.msg` file beside the journal).
//!
//! Reading stops at the first record that is cut short or malformed, as
//! Artemis does; what was read up to there is the page.

pub mod print;

use saranga_buffer::{DecodeError, Reader, Result, Writer};
use saranga_message::CoreMessage;
use saranga_storage::RecordCodec;
use saranga_storage::message::PersistedMessage;

/// `PagingStoreFactoryNIO.ADDRESS_FILE`.
pub const ADDRESS_FILE: &str = "address.txt";

/// The page file extension.
pub const PAGE_EXTENSION: &str = "page";

const START_BYTE: u8 = b'{';
const END_BYTE: u8 = b'}';

/// `PageReadWriter.SIZE_RECORD`: the bytes a record adds around its
/// message.
pub const SIZE_RECORD: usize = 1 + 4 + 1;

/// `PagedMessageImpl.LargeMessageType`.
mod large_type {
    pub const NONE: u8 = 0;
    pub const CORE: u8 = 1;
    pub const OLD_CORE: u8 = 0xff;
    pub const NOT_CORE: u8 = 2;
}

/// A paged message's message.
#[derive(Debug, Clone, PartialEq)]
pub enum PagedBody {
    /// A regular message, as its persister writes it.
    Message(PersistedMessage),
    /// A Core large message's headers and properties.
    CoreLarge(CoreMessage),
    /// A large message of another protocol, as its persister writes it.
    OtherLarge(PersistedMessage),
}

/// One page record (`PagedMessageImpl`).
#[derive(Debug, Clone, PartialEq)]
pub struct PagedMessage {
    /// The transaction that paged the message; -1 (or 0) outside one.
    pub transaction_id: i64,
    pub body: PagedBody,
    /// The queues the message was routed to.
    pub queue_ids: Vec<i64>,
}

impl PagedMessage {
    pub fn message_id(&self) -> i64 {
        match &self.body {
            PagedBody::Message(m) | PagedBody::OtherLarge(m) => m.message_id(),
            PagedBody::CoreLarge(m) => m.message_id,
        }
    }

    /// Whether the message is large: its body is in the large-messages
    /// directory.
    pub fn is_large(&self) -> bool {
        !matches!(self.body, PagedBody::Message(_))
    }

    /// Whether it was paged within a transaction.
    pub fn in_transaction(&self) -> bool {
        self.transaction_id > 0
    }

    pub fn write(&self, w: &mut Writer) {
        w.i64(self.transaction_id);
        match &self.body {
            PagedBody::Message(m) => {
                w.u8(large_type::NONE);
                m.write(w);
            }
            PagedBody::CoreLarge(m) => {
                let mut headers = Writer::new();
                m.encode_headers_and_properties(&mut headers);
                w.u8(large_type::CORE)
                    .i32(headers.len() as i32)
                    .bytes(headers.as_slice());
            }
            PagedBody::OtherLarge(m) => {
                w.u8(large_type::NOT_CORE);
                m.write(w);
            }
        }
        w.i32(self.queue_ids.len() as i32);
        for id in &self.queue_ids {
            w.i64(*id);
        }
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self> {
        let transaction_id = r.i64()?;
        let body = match r.u8()? {
            large_type::NONE => PagedBody::Message(PersistedMessage::read(r)?),
            large_type::CORE | large_type::OLD_CORE => {
                let size = r.len("large message headers")?;
                let mut headers = Reader::new(r.bytes(size)?);
                PagedBody::CoreLarge(CoreMessage::decode_headers_and_properties(&mut headers)?)
            }
            large_type::NOT_CORE => PagedBody::OtherLarge(PersistedMessage::read(r)?),
            t => {
                return Err(DecodeError::Invalid(format!(
                    "This largeMessageType isn't supported: {}",
                    t as i8
                )));
            }
        };
        let n = r.len("queue IDs")?;
        let mut queue_ids = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            queue_ids.push(r.i64()?);
        }
        Ok(Self {
            transaction_id,
            body,
            queue_ids,
        })
    }

    /// The record as written to a page file (`PageReadWriter.writeMessage`).
    pub fn encode_record(&self) -> Vec<u8> {
        let mut body = Writer::new();
        self.write(&mut body);
        let mut w = Writer::with_capacity(body.len() + SIZE_RECORD);
        w.u8(START_BYTE)
            .i32(body.len() as i32)
            .bytes(body.as_slice())
            .u8(END_BYTE);
        w.into_vec()
    }
}

/// A page file as read (`PageReadWriter.readFromSequentialFile`).
#[derive(Debug, Default)]
pub struct ReadPage {
    pub messages: Vec<PagedMessage>,
    /// The bytes holding complete records; anything after is ignored.
    pub valid_size: usize,
    /// A record was cut short or malformed: Artemis renames such a page to
    /// `.invalidPage` when it deletes it. Fewer bytes than a record's
    /// header and trailer at the end are ignored without that.
    pub suspect: bool,
}

/// The records of a page file. Reading stops at the first incomplete or
/// malformed record.
pub fn read_page(data: &[u8]) -> ReadPage {
    let mut out = ReadPage::default();
    scan(
        data,
        |m| out.messages.push(m),
        &mut out.valid_size,
        &mut out.suspect,
        true,
    );
    out
}

/// The number of records in a page file, without decoding them
/// (`Page.readNumberOfMessages`).
pub fn count_messages(data: &[u8]) -> usize {
    let (mut n, mut size, mut suspect) = (0, 0, false);
    scan(data, |_| n += 1, &mut size, &mut suspect, false);
    n
}

fn scan(
    data: &[u8],
    mut f: impl FnMut(PagedMessage),
    valid: &mut usize,
    suspect: &mut bool,
    decode: bool,
) {
    let mut at = 0;
    while data.len() - at >= SIZE_RECORD {
        if data[at] != START_BYTE {
            *suspect = true;
            return;
        }
        let size = i32::from_be_bytes(data[at + 1..at + 5].try_into().expect("4 bytes"));
        let Ok(size) = usize::try_from(size) else {
            *suspect = true;
            return;
        };
        let end = at + 5 + size;
        if end >= data.len() {
            // Cut short: the last record was not completely written.
            *suspect = true;
            return;
        }
        if data[end] != END_BYTE {
            *suspect = true;
            return;
        }
        if decode {
            let mut r = Reader::new(&data[at + 5..end]);
            match PagedMessage::read(&mut r) {
                Ok(m) => f(m),
                Err(_) => {
                    *suspect = true;
                    return;
                }
            }
        } else {
            f(PagedMessage {
                transaction_id: 0,
                body: PagedBody::CoreLarge(CoreMessage::default()),
                queue_ids: Vec::new(),
            });
        }
        at = end + 1;
        *valid = at;
    }
}

/// `PagingStoreImpl.createFileName`.
pub fn page_file_name(page_id: i64) -> String {
    format!("{page_id:09}.{PAGE_EXTENSION}")
}

/// The page number of a page file name (`getPageIdFromFileName`).
pub fn page_id_of(file_name: &str) -> Option<i64> {
    let (stem, ext) = file_name.split_once('.')?;
    (ext == PAGE_EXTENSION).then(|| stem.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_buffer::{PropertyValue, SimpleString};
    use saranga_storage::message::CoreMessageRecord;

    fn core(id: i64) -> CoreMessage {
        let mut m = CoreMessage {
            message_id: id,
            address: Some(SimpleString::from("a")),
            durable: true,
            body: vec![1, 2, 3],
            ..Default::default()
        };
        m.properties.insert("k".into(), PropertyValue::Int(5));
        m
    }

    #[test]
    fn records_round_trip() {
        let regular = PagedMessage {
            transaction_id: -1,
            body: PagedBody::Message(PersistedMessage::Core(CoreMessageRecord::new(core(7)))),
            queue_ids: vec![3, 4],
        };
        let mut large_core = core(8);
        large_core.body.clear();
        let large = PagedMessage {
            transaction_id: 99,
            body: PagedBody::CoreLarge(large_core),
            queue_ids: vec![3],
        };
        let mut file = regular.encode_record();
        file.extend(large.encode_record());
        let page = read_page(&file);
        assert_eq!(page.messages, vec![regular.clone(), large.clone()]);
        assert_eq!(page.valid_size, file.len());
        assert!(!page.suspect);
        assert_eq!(count_messages(&file), 2);
        assert_eq!(page.messages[1].message_id(), 8);
        assert!(page.messages[1].is_large());

        // A record cut short ends the page and makes it suspect.
        let cut = &file[..file.len() - 3];
        let page = read_page(cut);
        assert_eq!(page.messages, vec![regular.clone()]);
        assert!(page.suspect);
        // A few stray bytes at the end do not.
        let mut tail = regular.encode_record();
        tail.extend([b'{', 0, 0]);
        let page = read_page(&tail);
        assert_eq!(page.messages, vec![regular]);
        assert!(!page.suspect);
    }

    #[test]
    fn file_names() {
        assert_eq!(page_file_name(15), "000000015.page");
        assert_eq!(page_id_of("000000015.page"), Some(15));
        assert_eq!(page_id_of("address.txt"), None);
    }

    /// Every page Artemis wrote in the persist-paging golden scenario reads
    /// and re-encodes to the same bytes.
    #[test]
    fn golden_pages_round_trip() {
        use std::io::Read;
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/persist-paging/data.tar.gz"
        );
        let Ok(file) = std::fs::File::open(path) else {
            return;
        };
        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
        let mut pages = 0;
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            if !name.ends_with(".page") {
                continue;
            }
            let mut data = Vec::new();
            entry.read_to_end(&mut data).unwrap();
            let page = read_page(&data);
            assert!(!page.suspect, "{name}");
            assert_eq!(page.valid_size, data.len(), "{name}");
            let again: Vec<u8> = page
                .messages
                .iter()
                .flat_map(PagedMessage::encode_record)
                .collect();
            assert_eq!(again, data, "{name}");
            pages += 1;
        }
        assert!(pages > 10, "{pages} pages");
    }
}
