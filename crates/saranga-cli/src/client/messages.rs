//! `producer`, `consumer`, `browser` and `transfer`: JMS clients with the
//! Artemis CLI's options, threads and output (`Producer` /
//! `ProducerThread`, `Consumer` / `Browse` / `ConsumerThread`, `Transfer`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use saranga_client::message::{HDR_GROUP_ID, bytes_message, new_message, text_message};
use saranga_client::{CoreMessage, MessageExt, PropertyValue, message_type};
use saranga_server::management::json::Json;

use super::jms::{Destination, JmsConnection, JmsSession, message, text_or_id};
use super::{Common, ConnectionArgs, UsageError};
use crate::data::serialize::{MessageReader, MessageWriter, XML_SERIALIZER};

/// The text `--text-size` and `--object-size` messages are cut from
/// (`Producer.DEMO_TEXT`).
const DEMO_TEXT: &str = include_str!("demo.txt");

/// `DestAbstract`'s options.
#[derive(clap::Args, Debug)]
pub struct DestArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Destination to be used. It can be prefixed with queue:// or
    /// topic:// and can be an FQQN in the form of <address>::<queue>.
    /// Default: queue://TEST.
    #[arg(long, default_value = "queue://TEST")]
    destination: String,
    /// Number of messages to act on. Default: 1000.
    #[arg(long = "message-count", default_value_t = 1000)]
    message_count: i64,
    /// Time wait between each message.
    #[arg(long, default_value_t = 0)]
    sleep: i32,
    /// Transaction batch size. (deprecated)
    #[arg(long = "txt-size", hide = true, default_value_t = 0)]
    old_batch_size: i64,
    /// Transaction batch size.
    #[arg(long = "commit-interval", default_value_t = 0)]
    tx_batch_size: i64,
    /// Number of threads to use. Default: 1.
    #[arg(long, default_value_t = 1)]
    threads: i32,
    /// The class name of the custom serializer implementation to use
    /// intead of the default.
    #[arg(long)]
    serializer: Option<String>,
}

/// `DestAbstract.getMessageSerializer`: whether the XML serializer serves.
/// No Java class can be loaded, so another named is reported and the XML
/// one used, as Artemis does for a class it cannot load.
fn message_serializer(dest: &DestArgs) -> bool {
    if let Some(name) = dest.serializer.as_deref().filter(|n| *n != XML_SERIALIZER) {
        eprintln!("Error: unable to instantiate serializer class: {name}");
        eprintln!("Defaulting to: {XML_SERIALIZER}");
    }
    if !dest.conn.protocol.eq_ignore_ascii_case("core") {
        eprintln!(
            "Default Serializer does not support: {} protocol",
            dest.conn.protocol.to_uppercase()
        );
        return false;
    }
    true
}

impl DestArgs {
    /// `DestAbstract.execute`.
    fn resolve(&mut self) -> anyhow::Result<()> {
        self.conn.resolve(false)?;
        if self.old_batch_size > 0 && self.tx_batch_size > 0 {
            bail!(UsageError(
                "Either select --txt-size or --commit-interval. Cannot use both!".to_owned()
            ));
        }
        if self.old_batch_size > 0 {
            println!("--txt-size is deprecated, please use --commit-interval");
            self.tx_batch_size = self.old_batch_size;
        }
        Ok(())
    }

    fn verbose(&self) -> bool {
        self.conn.common.verbose
    }

    /// The threads to run: a negative count fails as Artemis's array of
    /// threads does (`NegativeArraySizeException`, whose message is the
    /// count).
    fn threads(&self) -> anyhow::Result<usize> {
        usize::try_from(self.threads).map_err(|_| UsageError(self.threads.to_string()).into())
    }

    /// The wait between messages: none unless positive (`if (sleep > 0)`).
    fn sleep_millis(&self) -> u64 {
        u64::try_from(self.sleep).unwrap_or(0)
    }

    /// The connection, with `Consumer:: clientID = ` first when there is
    /// one (`createCoreConnectionFactory`).
    async fn connect(&self) -> anyhow::Result<JmsConnection> {
        connect(&self.conn).await
    }
}

async fn connect(conn: &ConnectionArgs) -> anyhow::Result<JmsConnection> {
    if let Some(id) = &conn.client_id {
        println!("Consumer:: clientID = {id}");
    }
    // The connection factory's `tryConnect`, then the command's own.
    let mut conn = conn.clone();
    conn.try_connect().await?;
    conn.connect().await
}

fn elapsed_lines(thread: &str, started: Instant) {
    let ms = started.elapsed().as_millis();
    println!("{thread} Elapsed time in second : {} s", ms / 1000);
    println!("{thread} Elapsed time in milli second : {ms} milli seconds");
}

// ---------------------------------------------------------------- producer

#[derive(clap::Args, Debug)]
pub struct ProducerArgs {
    #[command(flatten)]
    dest: DestArgs,
    /// Send messages non persistently.
    #[arg(long = "non-persistent")]
    non_persistent: bool,
    /// Size of each bytesMessage. The producer will use JMS BytesMessage.
    #[arg(long = "message-size", default_value_t = 0)]
    message_size: i32,
    /// Content of each textMessage. The producer will use JMS TextMessage.
    #[arg(long)]
    message: Option<String>,
    /// Size of each textMessage. The producer will use JMS TextMessage.
    #[arg(long = "text-size", default_value_t = 0)]
    text_size: i32,
    /// Size of each ObjectMessage. The producer will use JMS ObjectMessage.
    #[arg(long = "object-size", default_value_t = 0)]
    object_size: i32,
    /// TTL for each message.
    #[arg(long, default_value_t = 0)]
    msgttl: i64,
    /// Message Group to be used.
    #[arg(long)]
    group: Option<String>,
    /// Messages will be read from the specified file.
    #[arg(long)]
    data: Option<String>,
    /// The properties to set on the message in JSON, e.g.:
    /// [{"type":"string","key":"myKey1","value":"myValue1"}]. Valid types
    /// are boolean, byte, short, int, long, float, double, and string.
    #[arg(long)]
    properties: Option<String>,
}

/// What every producer thread sends.
struct ProducerSpec {
    persistent: bool,
    message_size: usize,
    message: Option<String>,
    text_size: usize,
    object_size: usize,
    ttl: i64,
    group: Option<String>,
    properties: Option<Vec<Json>>,
    count: i64,
    sleep: u64,
    batch: i64,
    verbose: bool,
}

pub async fn producer(mut args: ProducerArgs) -> anyhow::Result<()> {
    args.dest.resolve()?;
    if let Some(file) = args.data.clone() {
        let conn = args.dest.connect().await?;
        let r = produce_file(&conn, &args.dest, &file).await;
        conn.close().await;
        return r;
    }
    let properties = match args.properties.as_deref().filter(|p| !p.is_empty()) {
        Some(p) => match Json::parse(p) {
            Ok(Json::Array(items)) => Some(items),
            _ => bail!("Invalid --properties: {p}"),
        },
        None => None,
    };
    let conn = Arc::new(args.dest.connect().await?);
    let spec = Arc::new(ProducerSpec {
        persistent: !args.non_persistent,
        message_size: usize::try_from(args.message_size).unwrap_or(0),
        message: args.message.clone(),
        text_size: usize::try_from(args.text_size).unwrap_or(0),
        object_size: usize::try_from(args.object_size).unwrap_or(0),
        ttl: args.msgttl,
        group: args.group.clone(),
        properties,
        count: args.dest.message_count,
        sleep: args.dest.sleep_millis(),
        batch: args.dest.tx_batch_size,
        verbose: args.dest.verbose(),
    });
    let dest = Destination::parse(&args.dest.destination);
    let mut threads = tokio::task::JoinSet::new();
    for i in 0..args.dest.threads()? {
        let session = conn.session(spec.batch > 0).await?;
        let (spec, dest) = (spec.clone(), dest.clone());
        let name = format!("Producer {dest}, thread={i}");
        threads.spawn(async move {
            if let Err(e) = produce(&session, &dest, &name, &spec).await {
                eprintln!("{e:#}");
            }
            session.close().await;
        });
    }
    while threads.join_next().await.is_some() {}
    if let Ok(conn) = Arc::try_unwrap(conn) {
        conn.close().await;
    }
    Ok(())
}

/// `ProducerThread.run`.
async fn produce(
    session: &JmsSession,
    dest: &Destination,
    thread: &str,
    spec: &ProducerSpec,
) -> anyhow::Result<()> {
    let producer = session.producer(dest).await?;
    let payload = (spec.message_size > 0).then(|| vec![b'.'; spec.message_size]);
    println!("{thread} Started to calculate elapsed time ...\n");
    let started = Instant::now();
    let mut text_cache: Option<String> = None;
    let mut transactions = 0;
    let mut sent: i64 = 0;
    while sent < spec.count {
        let m = create_message(spec, sent, thread, payload.as_deref(), &mut text_cache)?;
        let m = session
            .send(&producer, dest, m, spec.persistent, spec.ttl)
            .await?;
        if spec.verbose {
            println!("{thread} Sent: {}", text_or_id(&m));
        }
        if spec.batch > 0 && sent > 0 && sent % spec.batch == 0 {
            println!("{thread} Committing transaction: {transactions}");
            transactions += 1;
            session.commit().await?;
        }
        if spec.sleep > 0 {
            tokio::time::sleep(Duration::from_millis(spec.sleep)).await;
        }
        sent += 1;
    }
    if session.transacted {
        session.commit().await?;
    }
    println!("{thread} Produced: {sent} messages");
    elapsed_lines(thread, started);
    producer.close();
    Ok(())
}

/// `Producer.execute` with `--data`: the file's messages sent in order in
/// one transaction, persistent, rolled back on the first failure.
async fn produce_file(conn: &JmsConnection, args: &DestArgs, file: &str) -> anyhow::Result<()> {
    let session = conn.session(true).await?;
    let dest = Destination::parse(&args.destination);
    let producer = session.producer(&dest).await?;
    let r = async {
        if !message_serializer(args) {
            eprintln!("Error. Unable to instantiate serializer class: null");
            return Ok(None);
        }
        let mut reader = match MessageReader::open(std::path::Path::new(file)) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: Unable to open file for reading\n{e}");
                return Ok(None);
            }
        };
        reader.start()?;
        let mut count = 0;
        while let Some(m) = reader.read()? {
            session.send(&producer, &dest, m, true, 0).await?;
            count += 1;
        }
        session.commit().await?;
        anyhow::Ok(Some(count))
    }
    .await;
    match r {
        Ok(Some(count)) => println!("Sent {count} Messages."),
        Ok(None) => {}
        Err(e) => {
            eprintln!("Error occurred during import.  Rolling back.");
            session.rollback().await?;
            eprintln!("{e:#}");
        }
    }
    producer.close();
    session.close().await;
    Ok(())
}

/// `ProducerThread.createMessage`.
fn create_message(
    spec: &ProducerSpec,
    i: i64,
    thread: &str,
    payload: Option<&[u8]>,
    text_cache: &mut Option<String>,
) -> anyhow::Result<CoreMessage> {
    let mut m = if let Some(payload) = payload {
        bytes_message(payload)
    } else {
        let text = if spec.text_size > 0 || spec.object_size > 0 {
            let size = if spec.object_size > 0 {
                spec.object_size
            } else {
                spec.text_size
            };
            text_cache.get_or_insert_with(|| demo_text(size)).clone()
        } else if let Some(text) = &spec.message {
            text.clone()
        } else {
            format!("test message: {i}")
        };
        if spec.object_size > 0 {
            object_message(&text)
        } else {
            text_message(&text)
        }
    };
    if let Some(group) = spec.group.as_deref().filter(|g| !g.is_empty()) {
        m.put_string_property(HDR_GROUP_ID, group);
    }
    m.put_property("count", PropertyValue::Long(i));
    m.put_string_property("ThreadSent", thread);
    if let Some(properties) = &spec.properties {
        for p in properties {
            let field = |k: &str| p.get(k).and_then(Json::as_str).unwrap_or("").to_owned();
            let (kind, key, value) = (field("type"), field("key"), field("value"));
            let parsed = match kind.to_lowercase().as_str() {
                "boolean" => Some(PropertyValue::Bool(value.eq_ignore_ascii_case("true"))),
                "int" => Some(PropertyValue::Int(value.parse()?)),
                "long" => Some(PropertyValue::Long(value.parse()?)),
                "byte" => Some(PropertyValue::Byte(value.parse()?)),
                "short" => Some(PropertyValue::Short(value.parse()?)),
                "float" => Some(PropertyValue::Float(value.parse()?)),
                "double" => Some(PropertyValue::Double(value.parse()?)),
                "string" => Some(PropertyValue::String(value.as_str().into())),
                _ => None,
            };
            match parsed {
                Some(v) => {
                    m.put_property(&key, v);
                }
                None => eprintln!(
                    "Unable to set property: {key}. Did not recognize type: {kind}. Supported types are: boolean, int, long, byte, short, float, double, string."
                ),
            }
        }
    }
    Ok(m)
}

/// The first `size` characters of the demo text, repeated whole until
/// there are at least `size` when it is shorter.
fn demo_text(size: usize) -> String {
    let read: String = DEMO_TEXT.chars().take(size).collect();
    let len = read.chars().count();
    if len == size {
        return read;
    }
    let mut out = read.clone();
    while out.chars().count() < size {
        out.push_str(&read);
    }
    out
}

/// An `ObjectMessage` of a `String`: the Java serialization of the string,
/// after its length (`ActiveMQObjectMessage.doBeforeSend`).
fn object_message(text: &str) -> CoreMessage {
    let mut utf = Vec::new();
    for c in text.encode_utf16() {
        // Modified UTF-8, as `DataOutput.writeUTF` writes it.
        match c {
            0x0001..=0x007f => utf.push(c as u8),
            0x0000 | 0x0080..=0x07ff => {
                utf.push((0xc0 | ((c >> 6) & 0x1f)) as u8);
                utf.push((0x80 | (c & 0x3f)) as u8);
            }
            _ => {
                utf.push((0xe0 | ((c >> 12) & 0x0f)) as u8);
                utf.push((0x80 | ((c >> 6) & 0x3f)) as u8);
                utf.push((0x80 | (c & 0x3f)) as u8);
            }
        }
    }
    let mut data = vec![0xac, 0xed, 0x00, 0x05];
    if utf.len() <= 0xffff {
        data.push(0x74);
        data.extend_from_slice(&(utf.len() as u16).to_be_bytes());
    } else {
        data.push(0x7c);
        data.extend_from_slice(&(utf.len() as u64).to_be_bytes());
    }
    data.extend_from_slice(&utf);
    let mut m = new_message(message_type::OBJECT, true);
    let mut body = (data.len() as i32).to_be_bytes().to_vec();
    body.extend_from_slice(&data);
    m.body = body.into();
    m
}

// ---------------------------------------------------------------- consumer

#[derive(clap::Args, Debug)]
pub struct ConsumerArgs {
    #[command(flatten)]
    dest: DestArgs,
    /// Whether the consumer's subscription will be durable.
    #[arg(long)]
    durable: bool,
    /// Stop consuming when a null message is received.
    #[arg(long = "break-on-null")]
    break_on_null: bool,
    /// Timeout for receiving messages (in milliseconds). Specify -1 to wait
    /// forever.
    #[arg(
        long = "receive-timeout",
        default_value_t = 3000,
        allow_negative_numbers = true
    )]
    receive_timeout: i64,
    /// The message filter.
    #[arg(long)]
    filter: Option<String>,
    /// Serialize the messages to the specified file as they are consumed.
    #[arg(long)]
    data: Option<String>,
    /// The subscription name to use for a durable consumer.
    #[arg(long = "subscriptionName")]
    subscription_name: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct BrowseArgs {
    #[command(flatten)]
    dest: DestArgs,
    /// The message filter.
    #[arg(long)]
    filter: Option<String>,
}

/// What every consumer thread does.
struct ConsumerSpec {
    durable: bool,
    break_on_null: bool,
    receive_timeout: i64,
    filter: Option<String>,
    count: i64,
    sleep: u64,
    batch: i64,
    verbose: bool,
    client_id: Option<String>,
    /// `--data`'s file (the `SerialiserMessageListener`), until stopped.
    writer: Option<SharedWriter>,
}

type SharedWriter = Arc<std::sync::Mutex<Option<MessageWriter>>>;

pub async fn consumer(mut args: ConsumerArgs) -> anyhow::Result<()> {
    args.dest.resolve()?;
    println!(
        "Consumer:: filter = {}",
        args.filter.as_deref().unwrap_or("null")
    );
    let mut writer = None;
    if let Some(file) = &args.data {
        if !message_serializer(&args.dest) {
            eprintln!(
                "Error. Unable to instantiate serializer class: {}",
                args.dest.serializer.as_deref().unwrap_or("null")
            );
            return Ok(());
        }
        let mut w = match MessageWriter::create(std::path::Path::new(file)) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("Error: Unable to open file for writing\n{e}");
                return Ok(());
            }
        };
        w.start()?;
        writer = Some(w);
    }
    if let Some(name) = &args.subscription_name {
        if args.dest.threads != 1 {
            eprintln!(
                "Error: Cannot assign a subscription name when multiple threads are also configured."
            );
            // Returned before the `finally` that would stop the serializer.
            if let Some(w) = writer {
                w.abandon();
            }
            return Ok(());
        }
        println!("Consumer:: subscription name = {name}");
    }
    let writer: Option<SharedWriter> = writer.map(|w| Arc::new(std::sync::Mutex::new(Some(w))));
    let r = consume_threads(&args, writer.clone()).await;
    // finally: the serializer stopped, whatever its failure.
    if let Some(w) = writer.and_then(|w| w.lock().ok()?.take()) {
        let _ = w.stop();
    }
    r
}

async fn consume_threads(args: &ConsumerArgs, writer: Option<SharedWriter>) -> anyhow::Result<()> {
    let conn = Arc::new(args.dest.connect().await?);
    let spec = Arc::new(ConsumerSpec {
        durable: args.durable,
        break_on_null: args.break_on_null,
        receive_timeout: args.receive_timeout,
        filter: args.filter.clone(),
        count: args.dest.message_count,
        sleep: args.dest.sleep_millis(),
        batch: args.dest.tx_batch_size,
        verbose: args.dest.verbose(),
        client_id: conn.client_id.clone(),
        writer,
    });
    let dest = Destination::parse(&args.dest.destination);
    let mut threads = tokio::task::JoinSet::new();
    for i in 0..args.dest.threads()? {
        let session = conn.session(spec.batch > 0).await?;
        session.start().await?;
        let (spec, dest) = (spec.clone(), dest.clone());
        let name = args
            .subscription_name
            .clone()
            .unwrap_or_else(|| format!("Consumer {dest}, thread={i}"));
        threads.spawn(async move {
            consume(&session, &dest, &name, &spec).await;
            session.close().await;
        });
    }
    while threads.join_next().await.is_some() {}
    if let Ok(conn) = Arc::try_unwrap(conn) {
        conn.close().await;
    }
    Ok(())
}

/// `ConsumerThread.consume`.
async fn consume(session: &JmsSession, dest: &Destination, thread: &str, spec: &ConsumerSpec) {
    let wait = if spec.receive_timeout == -1 {
        "forever".to_owned()
    } else {
        format!("{}ms", spec.receive_timeout)
    };
    println!(
        "{thread} wait {wait} until {} messages are consumed",
        spec.count
    );
    let mut received: i64 = 0;
    let consumer = match consume_loop(session, dest, thread, spec, &mut received).await {
        Ok(c) => Some(c),
        Err((c, e)) => {
            eprintln!("{e:#}");
            c
        }
    };
    if let Some(c) = consumer {
        println!("{thread} Consumed: {received} messages");
        c.close().await;
    }
    println!("{thread} Consumer thread finished");
}

type LoopError = (Option<saranga_client::Consumer>, anyhow::Error);

async fn consume_loop(
    session: &JmsSession,
    dest: &Destination,
    thread: &str,
    spec: &ConsumerSpec,
    received: &mut i64,
) -> Result<saranga_client::Consumer, LoopError> {
    let subscription =
        (spec.durable && !dest.is_queue()).then_some((thread, spec.client_id.as_deref()));
    let consumer = session
        .consumer(dest, spec.filter.as_deref(), subscription)
        .await
        .map_err(|e| (None, e))?;
    let started = Instant::now();
    let mut count: i64 = 0;
    let mut transactions = 0;
    // `receive(0)`, like `receive()`, waits for ever.
    let timeout =
        (spec.receive_timeout > 0).then(|| Duration::from_millis(spec.receive_timeout as u64));
    let r: anyhow::Result<()> = async {
        while *received < spec.count {
            match session.receive(&consumer, timeout).await? {
                Some(d) => {
                    if spec.verbose {
                        println!("{thread} Received {}", text_or_id(&d.message));
                    } else {
                        count += 1;
                        if count % 1000 == 0 {
                            println!("Received {count}");
                        }
                    }
                    // `handle`: to the file, else the verbose details.
                    match &spec.writer {
                        Some(w) => {
                            if let Some(w) =
                                w.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.as_mut()
                            {
                                w.write(&d.message, d.is_large())?;
                            }
                        }
                        None if spec.verbose => verbose_received(&d.message),
                        None => {}
                    }
                    *received += 1;
                }
                None => {
                    if spec.break_on_null {
                        break;
                    }
                }
            }
            if session.transacted && spec.batch > 0 && *received > 0 && *received % spec.batch == 0
            {
                println!("{thread} Committing transaction: {transactions}");
                transactions += 1;
                session.commit().await?;
            }
            if spec.sleep > 0 {
                tokio::time::sleep(Duration::from_millis(spec.sleep)).await;
            }
        }
        if session.transacted {
            session.commit().await?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = r {
        return Err((Some(consumer), e));
    }
    println!("{thread} Consumed: {} messages", spec.count);
    elapsed_lines(thread, started);
    Ok(consumer)
}

/// `ConsumerThread.handle` for a received message, with `--verbose`.
fn verbose_received(m: &CoreMessage) {
    println!(
        "JMS Message ID:{}",
        m.jms_message_id().unwrap_or_else(|| "null".to_owned())
    );
    match m.message_type {
        message_type::TEXT => {
            let text = m.text().ok().flatten().unwrap_or_default();
            println!("Received text sized at {}", text.encode_utf16().count());
        }
        message_type::OBJECT => {
            if let Some(len) = object_string_len(&m.body) {
                println!("Received object {len}");
            }
        }
        _ => {}
    }
}

/// The length of a serialized `String` object body, in UTF-16 units.
fn object_string_len(body: &[u8]) -> Option<usize> {
    let data = body.get(4..)?;
    let (start, len) = match data.get(4)? {
        0x74 => (
            7,
            u16::from_be_bytes([*data.get(5)?, *data.get(6)?]) as usize,
        ),
        0x7c => (
            13,
            u64::from_be_bytes(data.get(5..13)?.try_into().ok()?) as usize,
        ),
        _ => return None,
    };
    let utf = data.get(start..start + len)?;
    // Modified UTF-8: count the UTF-16 units.
    Some(utf.iter().filter(|b| (**b & 0xc0) != 0x80).count())
}

pub async fn browser(mut args: BrowseArgs) -> anyhow::Result<()> {
    args.dest.resolve()?;
    println!(
        "Consumer:: filter = {}",
        args.filter.as_deref().unwrap_or("null")
    );
    let conn = Arc::new(args.dest.connect().await?);
    let dest = Destination::parse(&args.dest.destination);
    let Destination::Queue(queue) = dest.clone() else {
        bail!(
            "class org.apache.activemq.artemis.jms.client.ActiveMQTopic cannot be cast to class jakarta.jms.Queue"
        );
    };
    let mut threads = tokio::task::JoinSet::new();
    for i in 0..args.dest.threads()? {
        let session = conn.session(args.dest.tx_batch_size > 0).await?;
        session.start().await?;
        let thread = format!("Consumer {dest}, thread={i}");
        let (queue, filter) = (queue.clone(), args.filter.clone());
        let (count, sleep, verbose) = (
            args.dest.message_count,
            args.dest.sleep_millis(),
            args.dest.verbose(),
        );
        threads.spawn(async move {
            browse(
                &session,
                &queue,
                filter.as_deref(),
                &thread,
                count,
                sleep,
                verbose,
            )
            .await;
            session.close().await;
        });
    }
    while threads.join_next().await.is_some() {}
    if let Ok(conn) = Arc::try_unwrap(conn) {
        conn.close().await;
    }
    Ok(())
}

/// `ConsumerThread.browse`.
async fn browse(
    session: &JmsSession,
    queue: &str,
    filter: Option<&str>,
    thread: &str,
    count: i64,
    sleep: u64,
    verbose: bool,
) {
    println!("{thread} trying to browse {count} messages");
    let mut received: i64 = 0;
    let browser = match session.browser(queue, filter).await {
        Ok(b) => Some(b),
        Err(e) => {
            eprintln!("{e:#}");
            None
        }
    };
    if let Some(b) = &browser {
        loop {
            match b.receive_immediate().await {
                Ok(Some(d)) => {
                    println!("{thread} browsing {}", text_or_id(&d.message));
                    if verbose {
                        let m = &d.message;
                        println!(
                            "...ActiveMQMessage[{}]:{}",
                            m.jms_message_id().unwrap_or_else(|| "null".to_owned()),
                            if m.durable {
                                "PERSISTENT"
                            } else {
                                "NON-PERSISTENT"
                            }
                        );
                    }
                    received += 1;
                    if received >= count {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("{}", message(&e));
                    break;
                }
            }
            if sleep > 0 {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
            }
        }
        println!("{thread} browsed: {received} messages");
    }
    if let Some(b) = browser {
        b.close().await;
    }
    println!("{thread} Browser thread finished");
}

// ---------------------------------------------------------------- transfer

#[derive(clap::Args, Debug)]
pub struct TransferArgs {
    #[command(flatten)]
    common: Common,
    /// URL for the source broker. Default: build URL from 'artemis'
    /// acceptor defined in the broker.xml or tcp://localhost:61616 if the
    /// default cannot be parsed.
    #[arg(long = "source-url")]
    source_url: Option<String>,
    /// Acceptor from the local broker.xml used to build URL towards the
    /// broker. Default: 'artemis'.
    #[arg(long = "source-acceptor", default_value = "artemis")]
    source_acceptor: String,
    /// Username for the JMS connection to the source broker.
    #[arg(long = "source-user")]
    source_user: Option<String>,
    /// Password for the JMS connection to the source broker.
    #[arg(long = "source-password")]
    source_password: Option<String>,
    /// URL for the target broker. Default: build URL from 'artemis'
    /// acceptor defined in the broker.xml or tcp://localhost:61616 if the
    /// default cannot be parsed.
    #[arg(long = "target-url")]
    target_url: Option<String>,
    /// Username for the JMS connection to the target broker.
    #[arg(long = "target-user")]
    target_user: Option<String>,
    /// Password for the JMS connection to the target broker.
    #[arg(long = "target-password")]
    target_password: Option<String>,
    /// Amount of time (in milliseconds) to wait before giving up the
    /// receiving loop; 0 means no wait, -1 means wait forever. Default:
    /// 5000.
    #[arg(
        long = "receive-timeout",
        default_value_t = 5000,
        allow_negative_numbers = true
    )]
    receive_timeout: i64,
    /// JMS client ID to be associated with source connection.
    #[arg(long = "source-client-id")]
    source_client_id: Option<String>,
    /// Protocol used. Valid values are amqp or core. Default: core.
    #[arg(long = "source-protocol", default_value = "core")]
    source_protocol: String,
    /// Source JMS queue to transfer messages from. Cannot be used in
    /// conjunction with --source-topic.
    #[arg(long = "source-queue")]
    source_queue: Option<String>,
    /// Name of the JMS shared durable subscription to be used on the
    /// source JMS topic.
    #[arg(long = "shared-durable-subscription")]
    shared_durable_subscription: Option<String>,
    /// Name of the JMS shared non-durable subscription name to be used on
    /// the source JMS topic.
    #[arg(long = "shared-subscription")]
    shared_subscription: Option<String>,
    /// Name of the JMS unshared durable subscription to be used on the
    /// source JMS topic.
    #[arg(long = "durable-consumer")]
    durable_consumer: Option<String>,
    /// Use noLocal when applicable on topic operation
    #[arg(long = "no-Local")]
    no_local: bool,
    /// Source JMS topic to be used. Cannot be used in conjuction with
    /// --source-queue.
    #[arg(long = "source-topic")]
    source_topic: Option<String>,
    /// Filter to be used with the source consumer.
    #[arg(long = "source-filter")]
    filter: Option<String>,
    /// Protocol used. Valid values are amqp or core. Default: core.
    #[arg(long = "target-protocol", default_value = "core")]
    target_protocol: String,
    /// How many messages to transfer before committing the associated
    /// transaction. Default: 1000.
    #[arg(long = "commit-interval", default_value_t = 1000)]
    commit_interval: i64,
    /// Copy messages instead of transferring them.
    #[arg(long)]
    copy: bool,
    /// Target JMS queue to be used. Cannot be set with --target-topic.
    #[arg(long = "target-queue")]
    target_queue: Option<String>,
    /// Target JMS topic to be used. Cannot bet set with --target-queue.
    #[arg(long = "target-topic")]
    target_topic: Option<String>,
    /// Number of messages to transfer.
    #[arg(long = "message-count", default_value_t = i32::MAX as i64)]
    message_count: i64,
}

fn transfer_destination(
    role: &str,
    queue: Option<&String>,
    topic: Option<&String>,
) -> anyhow::Result<Destination> {
    match (queue, topic) {
        (Some(_), Some(_)) => bail!(UsageError(format!(
            "Cannot have both topic and queue passed as {role}"
        ))),
        (Some(q), None) => Ok(Destination::Queue(q.clone())),
        (None, Some(t)) => Ok(Destination::Topic(t.clone())),
        (None, None) => bail!(UsageError(format!(
            "You need to pass either a topic or a queue as {role}"
        ))),
    }
}

async fn transfer_connection(
    common: &Common,
    role: &str,
    protocol: &str,
    url: &str,
    user: Option<String>,
    password: Option<String>,
    client_id: Option<String>,
) -> anyhow::Result<JmsConnection> {
    if protocol != "core" {
        bail!(UsageError(format!("protocol {protocol} not supported")));
    }
    if common.verbose {
        println!("Creating {role} CORE Connection towards {url}");
    }
    let args = ConnectionArgs {
        common: common.clone(),
        url: Some(url.to_owned()),
        user,
        password,
        client_id,
        protocol: "CORE".to_owned(),
        acceptor: None,
    };
    connect(&args).await
}

pub async fn transfer(args: TransferArgs) -> anyhow::Result<()> {
    let source_url = args
        .source_url
        .clone()
        .unwrap_or_else(|| super::instance_url(Some(&args.source_acceptor)));
    let target_url = args
        .target_url
        .clone()
        .unwrap_or_else(|| super::DEFAULT_BROKER_URL.to_owned());
    println!("Connection brokerURL = {source_url}");
    let source_conn = transfer_connection(
        &args.common,
        "source",
        &args.source_protocol,
        &source_url,
        args.source_user.clone(),
        args.source_password.clone(),
        args.source_client_id.clone(),
    )
    .await?;
    let source = source_conn.session(true).await?;
    let source_dest = transfer_destination(
        "source",
        args.source_queue.as_ref(),
        args.source_topic.as_ref(),
    )?;
    let consumer = match &source_dest {
        Destination::Queue(_) => {
            source
                .consumer(&source_dest, args.filter.as_deref(), None)
                .await?
        }
        Destination::Topic(topic) => {
            if let Some(name) = &args.durable_consumer {
                // A filter is dropped here, as `Transfer` drops it.
                let selector = if args.filter.is_some() {
                    None
                } else if args.no_local {
                    Some(format!("__AMQ_CID<>'{}'", source_conn.conn_id))
                } else {
                    None
                };
                source
                    .consumer(
                        &source_dest,
                        selector.as_deref(),
                        Some((name, source_conn.client_id.as_deref())),
                    )
                    .await?
            } else if let Some(name) = args
                .shared_durable_subscription
                .as_ref()
                .or(args.shared_subscription.as_ref())
            {
                let durable = args.shared_durable_subscription.is_some();
                source
                    .shared_consumer(topic, name, durable, args.filter.as_deref())
                    .await?
            } else {
                bail!(UsageError(
                    "you must specify either --durable-consumer, --shared-durable-subscription or --shared-subscription with a JMS topic"
                        .to_owned()
                ));
            }
        }
    };
    let target_conn = transfer_connection(
        &args.common,
        "target",
        &args.target_protocol,
        &target_url,
        args.target_user.clone(),
        args.target_password.clone(),
        None,
    )
    .await?;
    let target = target_conn.session(true).await?;
    let target_dest = transfer_destination(
        "target",
        args.target_queue.as_ref(),
        args.target_topic.as_ref(),
    )?;
    let producer = target.producer(&target_dest).await?;
    if source_url == target_url && source_dest == target_dest {
        println!(
            "You cannot transfer between {source_url}/{source_dest} and {target_url}/{target_dest}.\nThat would create an infinite recursion."
        );
        bail!(UsageError(format!(
            "cannot use {source_dest} == {target_dest}"
        )));
    }
    source.start().await?;
    let timeout = match args.receive_timeout {
        t if t < 0 => None,
        t => Some(Duration::from_millis(t as u64)),
    };
    let (mut pending, mut total) = (0i64, 0i64);
    while total < args.message_count {
        let Some(d) = source.receive(&consumer, timeout).await? else {
            if args.common.verbose {
                println!("could not receive any more messages");
            }
            break;
        };
        let mut m = d.message;
        m.message_id = 0;
        target.send(&producer, &target_dest, m, true, 0).await?;
        pending += 1;
        total += 1;
        if args.common.verbose {
            println!("Received message {total} with {pending} messages pending to be commited");
        }
        if pending > args.commit_interval {
            println!("Transferred {pending} messages of {total}");
            pending = 0;
            target.commit().await?;
            if !args.copy {
                source.commit().await?;
            }
        }
    }
    println!("Transferred a total of {total} messages");
    if pending != 0 {
        target.commit().await?;
        if args.copy {
            source.rollback().await?;
        } else {
            source.commit().await?;
        }
    }
    producer.close();
    consumer.close().await;
    target.close().await;
    source.close().await;
    target_conn.close().await;
    source_conn.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{demo_text, object_message, object_string_len};

    #[test]
    fn demo_text_is_cut_and_repeated() {
        assert_eq!(demo_text(5), "Lorem");
        assert_eq!(demo_text(20), "Lorem ipsum dolor si");
    }

    #[test]
    fn object_messages_hold_a_serialized_string() {
        let m = object_message("héllo");
        assert_eq!(&m.body[4..9], &[0xac, 0xed, 0x00, 0x05, 0x74]);
        assert_eq!(object_string_len(&m.body), Some(5));
    }
}
