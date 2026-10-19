//! `XMLMessageSerializer`: the messages `consumer --data` writes and
//! `producer --data` sends, in `data exp`'s XML for a message, inside
//! `<messages>`, with a text message's body as its text.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use saranga_client::{CoreMessage, MessageExt, message_type};

use super::export::{Body, Xml, print_message};
use super::import::read_message;
use super::xml::{Event, Reader};

/// `--serializer`'s default (`XMLMessageSerializer`).
pub const XML_SERIALIZER: &str =
    "org.apache.activemq.artemis.cli.factory.serialize.XMLMessageSerializer";

/// The consumer's file (`setOutput`, `write`, `stop`).
pub struct MessageWriter {
    xml: Xml<BufWriter<File>>,
}

impl MessageWriter {
    /// `new FileOutputStream(file)`, failing with the message Java's
    /// `FileNotFoundException` has.
    pub fn create(path: &Path) -> Result<Self, String> {
        let file = File::create(path).map_err(|e| super::dirs::file_not_found(path, &e))?;
        Ok(Self {
            xml: Xml::new(BufWriter::new(file)),
        })
    }

    /// `start`: the declaration and `<messages>`, buffered.
    pub fn start(&mut self) -> std::io::Result<()> {
        self.xml.raw("<?xml version=\"1.0\"?>")?;
        self.xml.start("messages")
    }

    /// `write`: the message as `printSingleMessageAsXML` writes it, with no
    /// queues and a text message's body as its text. A large message's
    /// body is written in Base64 pieces, as `data exp` writes it, where
    /// Artemis 2.57's serializer fails reading it.
    pub fn write(&mut self, m: &CoreMessage, large: bool) -> anyhow::Result<()> {
        let body = if large {
            Body::Large(&m.body)
        } else if m.message_type == message_type::TEXT {
            Body::Text(m.text().ok().flatten())
        } else {
            Body::Base64(&m.body)
        };
        print_message(&mut self.xml, m, true, None, body)
    }

    /// `stop`: the element open ended on a line of its own, the rest by
    /// `writeEndDocument`, then flushed and closed.
    pub fn stop(mut self) -> std::io::Result<()> {
        self.xml.end()?;
        self.xml.end_document()?;
        self.xml.flush()
    }

    /// The file closed with nothing written that was not yet flushed, as a
    /// command returning before its `finally` leaves it.
    pub fn abandon(self) {
        let (_file, _unwritten) = self.xml.into_inner().into_parts();
    }
}

/// The producer's file (`setInput`, `start`, `read`).
pub struct MessageReader {
    reader: Reader<BufReader<File>>,
}

impl MessageReader {
    /// `new FileInputStream(file)`.
    pub fn open(path: &Path) -> Result<Self, String> {
        let file = File::open(path)
            .and_then(|f| {
                // Java opens no directory for reading.
                if f.metadata()?.is_dir() {
                    #[cfg(unix)]
                    return Err(std::io::Error::from_raw_os_error(21));
                    #[cfg(not(unix))]
                    return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
                }
                Ok(f)
            })
            .map_err(|e| super::dirs::file_not_found(path, &e))?;
        Ok(Self {
            reader: Reader::new(BufReader::new(file)),
        })
    }

    /// `start`: to the first tag, `<messages>`.
    pub fn start(&mut self) -> anyhow::Result<()> {
        self.reader.next_tag()?;
        Ok(())
    }

    /// `read`: the next message, none at `messages`.
    pub fn read(&mut self) -> anyhow::Result<Option<CoreMessage>> {
        let attributes = match self.reader.next_tag()? {
            Event::Start { name, .. } | Event::End { name } if name == "messages" => {
                return Ok(None);
            }
            Event::Start { attributes, .. } => attributes,
            _ => anyhow::bail!(
                "java.lang.IllegalStateException: Current state END_ELEMENT is not among the states START_ELEMENT , ATTRIBUTE valid for getAttributeCount()"
            ),
        };
        let info = read_message(&mut self.reader, &attributes, &HashMap::new(), true)?;
        Ok(Some(info.message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_client::PropertyValue;
    use saranga_client::message::{bytes_message, text_message};

    fn written(messages: &[(CoreMessage, bool)]) -> String {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.xml");
        let mut w = MessageWriter::create(&path).unwrap();
        w.start().unwrap();
        for (m, large) in messages {
            w.write(m, *large).unwrap();
        }
        w.stop().unwrap();
        std::fs::read_to_string(&path)
            .unwrap()
            .replace("\r\n", "\n")
    }

    #[test]
    fn no_messages_is_an_empty_messages_element() {
        assert_eq!(
            written(&[]),
            "<?xml version=\"1.0\"?>\n<messages>\n</messages>"
        );
    }

    #[test]
    fn a_text_message_is_written_as_its_text() {
        let mut m = text_message("hi <&> é  ");
        m.message_id = 417;
        m.priority = 4;
        m.timestamp = 1792389254162;
        m.put_property("count", PropertyValue::Long(0));
        let mut b = bytes_message(b"...");
        b.message_id = 430;
        b.timestamp = 1;
        assert_eq!(
            written(&[(m, false), (b, false)]),
            "<?xml version=\"1.0\"?>
<messages>
   <message id=\"417\" priority=\"4\" expiration=\"0\" timestamp=\"1792389254162\" type=\"text\">
      <properties>
         <property name=\"count\" value=\"0\" type=\"long\"/>
      </properties>
      <body>
         <![CDATA[hi <&> é  ]]>
      </body>
   </message>
   <message id=\"430\" priority=\"4\" expiration=\"0\" timestamp=\"1\" type=\"bytes\">
      <properties>
      </properties>
      <body>
         <![CDATA[Li4u]]>
      </body>
   </message>
</messages>"
        );
    }

    #[test]
    fn a_null_text_fails_inside_the_body_and_stop_ends_the_document() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.xml");
        let mut w = MessageWriter::create(&path).unwrap();
        w.start().unwrap();
        let mut m = text_message("x");
        m.set_text(None);
        assert!(w.write(&m, false).is_err());
        w.stop().unwrap();
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("\r\n", "\n");
        assert!(
            text.ends_with("      <body>\n      </body></message></messages>"),
            "{text}"
        );
    }

    #[test]
    fn what_the_consumer_writes_the_producer_reads() {
        let mut m = text_message("  hi <&> é  ");
        m.put_property("i", PropertyValue::Int(7));
        m.put_property("s", PropertyValue::String("a&b<".into()));
        let large = bytes_message(&[7u8; 2500]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.xml");
        let mut w = MessageWriter::create(&path).unwrap();
        w.start().unwrap();
        w.write(&m, false).unwrap();
        w.write(&bytes_message(b"..."), false).unwrap();
        w.write(&large, true).unwrap();
        w.stop().unwrap();

        let mut r = MessageReader::open(&path).unwrap();
        r.start().unwrap();
        let first = r.read().unwrap().unwrap();
        // The text is trimmed, as Artemis reads it.
        assert_eq!(first.text().unwrap().as_deref(), Some("hi <&> é"));
        assert_eq!(first.property("i"), Some(&PropertyValue::Int(7)));
        assert_eq!(
            first.property("s"),
            Some(&PropertyValue::String("a&b<".into()))
        );
        assert!(first.durable);
        assert_eq!(&r.read().unwrap().unwrap().body[..], b"...");
        assert_eq!(&r.read().unwrap().unwrap().body[..], &[7u8; 2500][..]);
        assert!(r.read().unwrap().is_none());
    }

    #[test]
    fn a_missing_file_is_javas_message() {
        let dir = tempfile::tempdir().unwrap();
        let e = MessageReader::open(&dir.path().join("missing.xml"))
            .err()
            .unwrap();
        assert!(e.contains("missing.xml ("), "{e}");
        assert!(!e.contains("os error"), "{e}");
        let e = MessageWriter::create(&dir.path().join("nodir").join("m.xml"))
            .err()
            .unwrap();
        assert!(!e.contains("os error"), "{e}");
    }
}
