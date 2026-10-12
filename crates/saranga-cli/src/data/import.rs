//! `saranga data imp` (`XmlDataImporter`, `XMLMessageImporter`): the
//! addresses, queues and messages of a `data exp` file sent to a running
//! broker over Core. Each message goes to the queues it was on, by their
//! IDs (`_AMQ_ROUTE_TO`), which management gives.

use std::collections::HashMap;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Args;
use saranga_client::{
    CoreMessage, MessageExt, PropertyValue, QueueConfiguration, RoutingType, Session,
    SessionOptions, SimpleString, message_type,
};
use saranga_storage::java;

use super::xml::{Event, Reader};
use crate::client::management::Requestor;

#[derive(Args)]
pub struct ImportArgs {
    /// URL towards the broker.
    #[arg(long)]
    url: Option<String>,
    /// User used to connect.
    #[arg(long)]
    user: Option<String>,
    /// Password used to connect.
    #[arg(long)]
    password: Option<String>,
    /// How often to commit.
    #[arg(long = "commit-interval", default_value_t = 1000, hide = true)]
    commit_interval: u64,
    /// The input file name.
    #[arg(long, required = true)]
    input: PathBuf,
    /// Sort the messages from the input (used for older versions that won't
    /// sort messages).
    #[arg(long)]
    sort: bool,
    /// Do not remove prefixes from legacy imports.
    #[arg(long = "legacy-prefixes")]
    legacy_prefixes: bool,
}

/// `PacketImpl.OLD_QUEUE_PREFIX` and `OLD_TOPIC_PREFIX`.
const OLD_QUEUE_PREFIX: &str = "jms.queue.";
const OLD_TOPIC_PREFIX: &str = "jms.topic.";
/// `FilterImpl.GENERIC_IGNORED_FILTER`.
const GENERIC_IGNORED_FILTER: &str = "__AMQX=-1";
/// `Message.HDR_ROUTE_TO_IDS`.
const HDR_ROUTE_TO_IDS: &str = "_AMQ_ROUTE_TO";

pub async fn run(args: ImportArgs) -> anyhow::Result<()> {
    // XmlDataImporter does not resolve the instance's acceptor.
    let url = args
        .url
        .clone()
        .unwrap_or_else(|| crate::client::DEFAULT_BROKER_URL.to_owned());
    let conn = crate::client::jms::JmsConnection::connect(
        &url,
        args.user.clone(),
        args.password.clone(),
        None,
    )
    .await?;
    let options = |auto_commit_sends| SessionOptions {
        username: args.user.clone(),
        password: args.password.clone(),
        auto_commit_sends,
        auto_commit_acks: true,
        pre_acknowledge: false,
        ack_batch_size: 0,
        ..SessionOptions::default()
    };
    let session = conn.conn.create_session(options(false)).await?;
    let management = conn.conn.create_session(options(true)).await?;
    let file = std::fs::File::open(&args.input)
        .with_context(|| format!("opening {}", args.input.display()))?;
    let mut importer = Importer {
        args: &args,
        reader: Reader::new(BufReader::new(file)),
        session,
        management,
        requestor: Requestor::open(&conn).await?,
        address_map: HashMap::new(),
        queue_ids: HashMap::new(),
        old_prefix_translation: HashMap::new(),
        message_nr: 0,
        sorted: Vec::new(),
    };
    let result = importer.process().await;
    let _ = importer.session.commit().await;
    importer.session.close().await;
    let _ = importer.management.commit().await;
    importer.management.close().await;
    importer.requestor.close().await;
    conn.close().await;
    result
}

/// A message read from the file: its exported ID, its queues.
struct MessageInfo {
    id: i64,
    queues: Vec<String>,
    message: CoreMessage,
}

struct Importer<'a, R> {
    args: &'a ImportArgs,
    reader: Reader<R>,
    session: Session,
    management: Session,
    requestor: Requestor,
    /// Queue to the address its messages are sent to.
    address_map: HashMap<String, String>,
    queue_ids: HashMap<String, i64>,
    old_prefix_translation: HashMap<String, String>,
    message_nr: u64,
    /// With `--sort`.
    sorted: Vec<MessageInfo>,
}

fn attribute<'a>(attributes: &'a [(String, String)], name: &str) -> Option<&'a str> {
    attributes
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

impl<R: std::io::BufRead> Importer<'_, R> {
    /// `processXml`.
    async fn process(&mut self) -> anyhow::Result<()> {
        loop {
            match self.reader.next_event()? {
                Event::Eof => break,
                Event::Start { name, attributes } => match name.as_str() {
                    "binding" => self.old_binding(&attributes).await?,
                    "queue-binding" => self.bind_queue(&attributes).await?,
                    "address-binding" => self.bind_address(&attributes).await?,
                    "message" => self.process_message(&attributes).await?,
                    _ => {}
                },
                _ => {}
            }
        }
        if self.args.sort {
            let mut messages = std::mem::take(&mut self.sorted);
            messages.sort_by_key(|m| m.id);
            for (n, m) in messages.into_iter().enumerate() {
                self.send(&m.queues, m.message).await?;
                if (n as u64 + 1).is_multiple_of(self.args.commit_interval) {
                    self.session.commit().await?;
                }
            }
        }
        self.session.commit().await?;
        Ok(())
    }

    async fn process_message(&mut self, attributes: &[(String, String)]) -> anyhow::Result<()> {
        self.message_nr += 1;
        if self.message_nr.is_multiple_of(self.args.commit_interval) {
            eprintln!("Processed {} messages", self.message_nr);
            self.session.commit().await?;
        }
        let info = self.read_message(attributes)?;
        if self.args.sort {
            self.sorted.push(info);
        } else {
            self.send(&info.queues, info.message).await?;
        }
        Ok(())
    }

    /// `XMLMessageImporter.readMessage`: a durable message of the exported
    /// type, priority, expiration and timestamp, a new user ID if it had
    /// one, its properties, queues and body.
    fn read_message(&mut self, attributes: &[(String, String)]) -> anyhow::Result<MessageInfo> {
        let get = |n| attribute(attributes, n);
        let mut message = CoreMessage {
            message_type: match get("type").unwrap_or("default") {
                "bytes" => message_type::BYTES,
                "map" => message_type::MAP,
                "object" => message_type::OBJECT,
                "stream" => message_type::STREAM,
                "text" => message_type::TEXT,
                _ => message_type::DEFAULT,
            },
            durable: true,
            expiration: get("expiration").map_or(Ok(0), str::parse)?,
            timestamp: get("timestamp").map_or(Ok(0), str::parse)?,
            priority: get("priority").map_or(Ok(0), str::parse::<i8>)? as u8,
            ..CoreMessage::default()
        };
        if get("user-id").is_some() {
            message.set_new_user_id();
        }
        let id = get("id").map_or(Ok(0), str::parse)?;
        let mut queues = Vec::new();
        loop {
            match self.reader.next_event()? {
                Event::Start { name, attributes } => match name.as_str() {
                    "body" => message.body = self.body()?,
                    "property" => property(&mut message, &attributes)?,
                    "queue" => {
                        if let Some(q) = attribute(&attributes, "name") {
                            let q = self
                                .old_prefix_translation
                                .get(q)
                                .cloned()
                                .unwrap_or_else(|| q.to_owned());
                            queues.push(q);
                        }
                    }
                    _ => {}
                },
                Event::End { name } if name == "message" => break,
                Event::Eof => bail!("the input ends inside a message"),
                _ => {}
            }
        }
        Ok(MessageInfo {
            id,
            queues,
            message,
        })
    }

    /// `getMessageBodyBytes`: each chunk of text (CDATA or not, trimmed)
    /// up to whitespace is Base64, decoded and appended.
    fn body(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut body = Vec::new();
        let mut chunk = String::new();
        let flush = |chunk: &mut String, body: &mut Vec<u8>| -> anyhow::Result<()> {
            if !chunk.is_empty() {
                body.extend(
                    java::base64_url_decode(chunk)
                        .map_err(anyhow::Error::msg)
                        .context("a message body")?,
                );
                chunk.clear();
            }
            Ok(())
        };
        loop {
            match self.reader.next_event()? {
                Event::Text(t) if t.trim().is_empty() => flush(&mut chunk, &mut body)?,
                Event::Text(t) | Event::CData(t) => chunk.push_str(t.trim()),
                Event::End { .. } => break,
                Event::Eof => bail!("the input ends inside a message body"),
                Event::Start { .. } => {}
            }
        }
        flush(&mut chunk, &mut body)?;
        Ok(body)
    }

    /// `sendMessage`: to the address of its first queue (created as
    /// `UndefinedQueue` if unknown), routed to its queues by ID.
    async fn send(&mut self, queues: &[String], mut message: CoreMessage) -> anyhow::Result<()> {
        let Some(first) = queues.first() else {
            return Ok(());
        };
        let destination = match self.address_map.get(first) {
            Some(d) => d.clone(),
            None => {
                self.create_undefined_queue(first, &message).await?;
                self.address_map.insert(first.clone(), first.clone());
                first.clone()
            }
        };
        let mut ids = Vec::with_capacity(queues.len() * 8);
        for q in queues {
            let id = match self.queue_ids.get(q) {
                Some(&id) => Some(id),
                None => {
                    let reply = self
                        .requestor
                        .attribute(&format!("queue.{q}"), "ID")
                        .await?;
                    let id = reply
                        .succeeded
                        .then(|| reply.result.as_ref().and_then(|r| r.as_i64()))
                        .flatten();
                    if let Some(id) = id {
                        self.queue_ids.insert(q.clone(), id);
                    }
                    id
                }
            };
            if let Some(id) = id {
                ids.extend(id.to_be_bytes());
            }
        }
        message.put_property(HDR_ROUTE_TO_IDS, PropertyValue::Bytes(ids));
        let producer = self.session.create_producer(None).await?;
        let sent = producer.send_to(&destination, message).await;
        producer.close();
        sent?;
        Ok(())
    }

    /// `createUndefinedQueue`: durable, of the message's routing type.
    async fn create_undefined_queue(
        &self,
        name: &str,
        message: &CoreMessage,
    ) -> anyhow::Result<()> {
        if self.management.queue_query(name).await?.exists {
            return Ok(());
        }
        let routing_type = match message.property("_AMQ_ROUTING_TYPE") {
            Some(PropertyValue::Byte(1)) => RoutingType::Anycast,
            _ => RoutingType::Multicast,
        };
        self.management
            .create_queue(
                QueueConfiguration::new(name)
                    .routing_type(routing_type)
                    .durable(true),
            )
            .await?;
        Ok(())
    }

    /// `bindQueue`.
    async fn bind_queue(&mut self, a: &[(String, String)]) -> anyhow::Result<()> {
        let name = attribute(a, "name").unwrap_or_default().to_owned();
        let address = attribute(a, "address").unwrap_or_default().to_owned();
        let filter = attribute(a, "filter-string").unwrap_or_default();
        let routing_type = match attribute(a, "routing-type").unwrap_or_default() {
            "ANYCAST" => RoutingType::Anycast,
            "MULTICAST" => RoutingType::Multicast,
            other => bail!("No enum constant RoutingType.{other}"),
        };
        if !self.session.queue_query(&name).await?.exists {
            let mut config = QueueConfiguration::new(&name)
                .address(&address)
                .routing_type(routing_type);
            if !filter.is_empty() {
                config = config.filter(filter);
            }
            self.session.create_queue(config).await?;
        }
        self.address_map.insert(name, address);
        Ok(())
    }

    /// `bindAddress`.
    async fn bind_address(&mut self, a: &[(String, String)]) -> anyhow::Result<()> {
        let name = attribute(a, "name").unwrap_or_default();
        let types = attribute(a, "routing-types").unwrap_or_default();
        if !self.session.address_query(name).await?.exists {
            let mut set = Vec::new();
            for t in types.split(',').map(str::trim).filter(|t| !t.is_empty()) {
                set.push(match t {
                    "ANYCAST" => RoutingType::Anycast,
                    "MULTICAST" => RoutingType::Multicast,
                    other => bail!("No enum constant RoutingType.{other}"),
                });
            }
            self.session.create_address(name, &set, false).await?;
        }
        Ok(())
    }

    /// `oldBinding`: a binding of an Artemis 1.x export, its `jms.queue.`
    /// and `jms.topic.` prefixes removed unless `--legacy-prefixes`.
    async fn old_binding(&mut self, a: &[(String, String)]) -> anyhow::Result<()> {
        let mut queue = attribute(a, "queue-name").unwrap_or_default().to_owned();
        let mut address = attribute(a, "address").unwrap_or_default().to_owned();
        let filter = attribute(a, "filter-string").unwrap_or_default().to_owned();
        let mut routing_type = RoutingType::Multicast;
        let keep = self.args.legacy_prefixes;
        if let Some(rest) = address.strip_prefix(OLD_QUEUE_PREFIX) {
            routing_type = RoutingType::Anycast;
            if !keep {
                address = rest.to_owned();
            }
        } else if let Some(rest) = address.strip_prefix(OLD_TOPIC_PREFIX)
            && !keep
        {
            address = rest.to_owned();
        }
        let stripped = queue
            .strip_prefix(OLD_QUEUE_PREFIX)
            .or_else(|| queue.strip_prefix(OLD_TOPIC_PREFIX))
            .map(str::to_owned);
        if let Some(new) = stripped
            && !keep
        {
            self.old_prefix_translation
                .insert(queue.clone(), new.clone());
            queue = new;
        }
        if !self.session.address_query(&address).await?.exists {
            self.session
                .create_address(&address, &[routing_type], true)
                .await?;
        }
        if filter != GENERIC_IGNORED_FILTER && !self.session.queue_query(&queue).await?.exists {
            let mut config = QueueConfiguration::new(&queue)
                .address(&address)
                .routing_type(routing_type);
            if !filter.is_empty() {
                config = config.filter(&filter);
            }
            self.session.create_queue(config).await?;
        }
        self.address_map.insert(queue, address);
        Ok(())
    }
}

/// `processMessageProperties`: a property of the type the file gives, or
/// none for an unknown type.
fn property(message: &mut CoreMessage, a: &[(String, String)]) -> anyhow::Result<()> {
    let key = attribute(a, "name").unwrap_or_default();
    let value = attribute(a, "value").filter(|v| *v != "_AMQ_NULL");
    let typed = attribute(a, "type").unwrap_or_default();
    let need = || value.with_context(|| format!("property {key}: no value"));
    let v = match typed {
        "short" => PropertyValue::Short(need()?.parse()?),
        "boolean" => PropertyValue::Bool(value.is_some_and(|v| v.eq_ignore_ascii_case("true"))),
        "byte" => PropertyValue::Byte(need()?.parse()?),
        "bytes" => match value {
            Some(v) => PropertyValue::Bytes(
                java::base64_url_decode(v)
                    .map_err(anyhow::Error::msg)
                    .context(key.to_owned())?,
            ),
            None => PropertyValue::Null,
        },
        "double" => PropertyValue::Double(need()?.parse()?),
        "float" => PropertyValue::Float(need()?.parse()?),
        "integer" => PropertyValue::Int(need()?.parse()?),
        "long" => PropertyValue::Long(need()?.parse()?),
        "simple-string" | "string" => match value {
            Some(v) => PropertyValue::String(SimpleString::from(v)),
            None => PropertyValue::Null,
        },
        _ => return Ok(()),
    };
    message.put_property(key, v);
    Ok(())
}
