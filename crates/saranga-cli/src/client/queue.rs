//! `queue create|delete|update|stat|purge` (`QueueGroup`).

use anyhow::bail;
use saranga_server::management::json::Json;

use super::management::{Reply, perform, perform_simple};
use super::{ConnectionArgs, UsageError};

#[derive(clap::Subcommand, Debug)]
pub enum QueueCommand {
    /// Create a queue.
    Create(QueueArgs),
    /// Delete a queue.
    Delete(DeleteArgs),
    /// Update a queue.
    Update(QueueArgs),
    /// Print basic stats of a queue.
    Stat(StatArgs),
    /// Delete all messages in a queue.
    Purge(PurgeArgs),
}

/// `QueueAbstract`'s options.
#[derive(clap::Args, Debug)]
pub struct QueueArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// The queue's name.
    #[arg(long)]
    name: Option<String>,
    /// The queue's filter string. Default: null.
    #[arg(long)]
    filter: Option<String>,
    /// The queue's address. Default: queue's name.
    #[arg(long)]
    address: Option<String>,
    /// The queue is durable. Default: input.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    durable: Option<bool>,
    /// The queue is not durable. Default: input.
    #[arg(long = "no-durable", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    no_durable: Option<bool>,
    /// Delete the contents of this queue when its last consumer
    /// disconnects. Default: input.
    #[arg(long = "purge-on-no-consumers", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    purge_on_no_consumers: Option<bool>,
    /// Preserve the contents of this queue when its last consumer
    /// disconnects. Default: input.
    #[arg(long = "preserve-on-no-consumers", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    preserve_on_no_consumers: Option<bool>,
    /// The maximum number of concurrent consumers allowed on this queue.
    /// Default: no limit.
    #[arg(long = "max-consumers", allow_negative_numbers = true)]
    max_consumers: Option<i32>,
    /// Automatically create the address (if it doesn't exist) with default
    /// values. Default: input.
    #[arg(long = "auto-create-address", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    auto_create_address: Option<bool>,
    /// Create an anycast queue. Default: input.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    anycast: Option<bool>,
    /// Create a multicast queue. Default: input.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    multicast: Option<bool>,
}

/// `QueueAbstract`'s getters, with its prompts and silent defaults.
struct Queue<'a> {
    args: &'a QueueArgs,
    name: Option<String>,
    anycast: Option<bool>,
    multicast: Option<bool>,
}

impl<'a> Queue<'a> {
    fn new(args: &'a QueueArgs) -> Self {
        Self {
            args,
            name: args.name.clone(),
            anycast: args.anycast,
            multicast: args.multicast,
        }
    }

    fn common(&self) -> &super::Common {
        &self.args.conn.common
    }

    fn name(&mut self) -> anyhow::Result<String> {
        if self.name.is_none() {
            self.name =
                self.common()
                    .input("--name", "What is the name of the queue?", Some(""))?;
        }
        Ok(self.name.clone().unwrap_or_default())
    }

    fn address(&mut self) -> anyhow::Result<String> {
        let name = self.name()?;
        let address = match self.args.address.clone().filter(|a| !a.trim().is_empty()) {
            Some(a) => Some(a),
            None => self.common().input(
                "--address",
                &format!("What is the name of the address? <Enter for {name}>"),
                None,
            )?,
        };
        Ok(address.filter(|a| !a.trim().is_empty()).unwrap_or(name))
    }

    fn is_anycast(&mut self) -> anyhow::Result<bool> {
        if self.anycast.is_none() {
            self.anycast = self.multicast.map(|m| !m);
        }
        if self.anycast.is_none() {
            self.anycast = Some(self.common().input_boolean(
                "--anycast",
                "Is this an anycast queue?",
                false,
            )?);
        }
        Ok(self.anycast == Some(true))
    }

    fn is_multicast(&mut self) -> anyhow::Result<bool> {
        if self.multicast.is_none() {
            self.multicast = self.anycast.map(|a| !a);
        }
        if self.multicast.is_none() {
            self.multicast = Some(self.common().input_boolean(
                "--multicast",
                "Is this a multicast queue?",
                false,
            )?);
        }
        Ok(self.multicast == Some(true))
    }

    fn routing_type(&mut self) -> anyhow::Result<Option<&'static str>> {
        if self.is_anycast()? && self.is_multicast()? {
            bail!(UsageError(
                "--multicast and --anycast are exclusive options for a queue".to_owned()
            ));
        }
        Ok(if self.is_multicast()? {
            Some("MULTICAST")
        } else if self.anycast == Some(true) {
            Some("ANYCAST")
        } else {
            None
        })
    }

    /// `isDurable`: --durable, else the opposite of --no-durable, else
    /// input.
    fn durable(&self) -> anyhow::Result<bool> {
        match self.args.durable.or(self.args.no_durable.map(|no| !no)) {
            Some(durable) => Ok(durable),
            None => self
                .common()
                .input_boolean("--durable", "Is this a durable queue?", false),
        }
    }

    /// `isPurgeOnNoConsumers`: --purge-on-no-consumers, else the opposite
    /// of --preserve-on-no-consumers, else input when `use_input`.
    fn purge_on_no_consumers(&self, use_input: bool) -> anyhow::Result<Option<bool>> {
        let given = self
            .args
            .purge_on_no_consumers
            .or(self.args.preserve_on_no_consumers.map(|preserve| !preserve));
        Ok(match given {
            Some(purge) => Some(purge),
            None if use_input => Some(self.common().input_boolean(
                "--purge-on-no-consumers",
                "Purge the contents of the queue once the last consumer disconnects?",
                false,
            )?),
            None => None,
        })
    }

    /// `isAutoCreateAddress`: as given, else input.
    fn auto_create_address(&self) -> anyhow::Result<bool> {
        match self.args.auto_create_address {
            Some(auto) => Ok(auto),
            None => self.common().input_boolean(
                "--auto-create-address",
                "Automatically create the address if it doesn't exist?",
                false,
            ),
        }
    }
}

fn opt(s: Option<&str>) -> Json {
    s.map(Json::from).unwrap_or(Json::Null)
}

/// Prints a reply as the `ok` and `failed` callbacks do.
fn report(reply: &Reply, ok: impl FnOnce(&Reply) -> String, failed: impl FnOnce(&Reply) -> String) {
    if reply.succeeded {
        println!("{}", ok(reply));
    } else {
        eprintln!("{}", failed(reply));
    }
}

#[derive(clap::Args, Debug)]
pub struct DeleteArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// The queue's name
    #[arg(long)]
    name: Option<String>,
    /// Whether to delete the queue even if it has active consumers.
    /// Default: false.
    #[arg(long = "removeConsumers")]
    remove_consumers: bool,
    /// Whether to delete the address if this is its only queue.
    #[arg(long = "autoDeleteAddress")]
    auto_delete_address: bool,
}

#[derive(clap::Args, Debug)]
pub struct PurgeArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// The queue's name.
    #[arg(long)]
    name: Option<String>,
}

pub async fn run(cmd: QueueCommand) -> anyhow::Result<()> {
    match cmd {
        QueueCommand::Create(mut args) => {
            args.conn.resolve(false)?;
            let mut q = Queue::new(&args);
            let params = vec![
                Json::from(q.address()?),
                opt(q.routing_type()?),
                Json::from(q.name()?),
                opt(args.filter.as_deref()),
                Json::from(q.durable()?),
                Json::from(i64::from(args.max_consumers.unwrap_or(-1))),
                Json::from(q.purge_on_no_consumers(true)?.unwrap_or(false)),
                Json::from(q.auto_create_address()?),
            ];
            let name = q.name()?;
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "createQueue", params).await
            })
            .await?;
            report(
                &reply,
                |r| format!("{} created successfully.", r.string()),
                |r| format!("Failed to create queue {name}. Reason: {}", r.string()),
            );
        }
        QueueCommand::Update(mut args) => {
            args.conn.resolve(false)?;
            let mut q = Queue::new(&args);
            let name = q.name()?;
            let params = vec![
                Json::from(name.clone()),
                opt(q.routing_type()?),
                args.max_consumers
                    .map(|m| Json::from(i64::from(m)))
                    .unwrap_or(Json::Null),
                q.purge_on_no_consumers(false)?
                    .map(Json::from)
                    .unwrap_or(Json::Null),
            ];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "updateQueue", params).await
            })
            .await?;
            report(
                &reply,
                |r| format!("{} updated successfully.", r.string()),
                |r| format!("Failed to update {name}. Reason: {}", r.string()),
            );
        }
        QueueCommand::Delete(mut args) => {
            args.conn.resolve(false)?;
            let name = match args.name.clone() {
                Some(n) => Some(n),
                None => {
                    args.conn
                        .common
                        .input("--name", "What is the name of the queue?", Some(""))?
                }
            }
            .unwrap_or_default();
            let params = vec![
                Json::from(name.clone()),
                Json::from(args.remove_consumers),
                Json::from(args.auto_delete_address),
            ];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "destroyQueue", params).await
            })
            .await?;
            report(
                &reply,
                |_| format!("Queue {name} deleted successfully."),
                |r| format!("Failed to delete queue {name}. Reason: {}", r.string()),
            );
        }
        QueueCommand::Purge(mut args) => {
            args.conn.resolve(false)?;
            let name = match args.name.clone() {
                Some(n) => Some(n),
                None => {
                    args.conn
                        .common
                        .input("--name", "What is the name of the queue?", Some(""))?
                }
            }
            .unwrap_or_default();
            let reply = perform(&args.conn, async |r| {
                r.invoke(&format!("queue.{name}"), "removeAllMessages", Vec::new())
                    .await
            })
            .await?;
            report(
                &reply,
                |_| format!("Queue {name} purged successfully."),
                |r| format!("Failed to purge queue {name}. Reason: {}", r.string()),
            );
        }
        QueueCommand::Stat(args) => stat(args).await?,
    }
    Ok(())
}

// ------------------------------------------------------------------ stat

#[derive(clap::Args, Debug)]
pub struct StatArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Display queue stats for queue(s) with names containing this string.
    #[arg(long = "queueName")]
    queue_name: Option<String>,
    /// The field to filter. Possible values: NAME, ADDRESS, MESSAGE_COUNT,
    /// MESSAGES_ADDED, DELIVERING_COUNT, MESSAGES_ACKED, SCHEDULED_COUNT,
    /// ROUTING_TYPE.
    #[arg(long)]
    field: Option<String>,
    /// The operation to filter. Possible values: CONTAINS, NOT_CONTAINS,
    /// EQUALS, GREATER_THAN, LESS_THAN.
    #[arg(long)]
    operation: Option<String>,
    /// The value to filter.
    #[arg(long)]
    value: Option<String>,
    /// The max number of queues displayed. Default is 50.
    #[arg(long = "maxRows", default_value_t = 50)]
    max_rows: i32,
    /// The max width of data column. Set to -1 for no limit. Default is 25.
    #[arg(
        long = "maxColumnSize",
        default_value_t = 25,
        allow_negative_numbers = true
    )]
    max_column_size: i32,
    /// Expands the report for all nodes on the topology
    #[arg(long)]
    clustered: bool,
    /// Include queues created for notification management in the output
    #[arg(long = "include-management")]
    include_management: bool,
    /// Keep Queue Stat in a forever loop, that you can interrupt with
    /// Ctrl-C, sleeping for --loop-time between each iteration.
    #[arg(long = "loop")]
    use_loop: bool,
    #[arg(long = "loop-sleep", hide = true, default_value_t = -1)]
    old_sleep: i64,
    /// Amount of Milliseconds to sleep before each iteration on queue
    /// stat. Default=60000
    #[arg(long, default_value_t = -1)]
    sleep: i64,
    /// Use a single line on the header titles
    #[arg(long = "single-line-header")]
    single_line_header: bool,
    /// Prints the queue stat information in JSON format, useful for scripts
    #[arg(long)]
    json: bool,
}

/// `StatQueue.FIELD`: the column, its JSON name, and whether it is
/// centred.
const FIELDS: [(&str, &str, bool); 10] = [
    ("NAME", "name", false),
    ("ADDRESS", "address", false),
    ("CONSUMER_COUNT", "consumerCount", true),
    ("MESSAGE_COUNT", "messageCount", true),
    ("MESSAGES_ADDED", "messagesAdded", true),
    ("DELIVERING_COUNT", "deliveringCount", true),
    ("MESSAGES_ACKED", "messagesAcked", true),
    ("SCHEDULED_COUNT", "scheduledCount", true),
    ("ROUTING_TYPE", "routingType", true),
    ("INTERNAL", "internalQueue", true),
];

const OPERATIONS: [&str; 5] = [
    "CONTAINS",
    "NOT_CONTAINS",
    "EQUALS",
    "GREATER_THAN",
    "LESS_THAN",
];

const MANAGEMENT_QUEUE: &str = "activemq.management";

/// `createFilter`: the `listQueues` filter, or `None` after an error.
fn create_filter(args: &StatArgs) -> Option<String> {
    let given = |o: &Option<String>| o.as_deref().is_some_and(|s| !s.trim().is_empty());
    let mut filter = Json::object();
    if given(&args.field) && given(&args.queue_name) {
        eprintln!("'--field' and '--queueName' cannot be specified together.");
        return None;
    }
    let names = || {
        format!(
            "[{}]",
            FIELDS.iter().map(|f| f.0).collect::<Vec<_>>().join(", ")
        )
    };
    if let Some(field) = args.field.as_deref().filter(|f| !f.trim().is_empty()) {
        // By its JSON name, ignoring case, else by its name.
        let found = FIELDS
            .iter()
            .find(|f| f.1.eq_ignore_ascii_case(field))
            .or_else(|| FIELDS.iter().find(|f| f.0 == field));
        let Some(found) = found else {
            eprintln!("'--field' must be set to one of the following {}", names());
            return None;
        };
        filter.put("field", found.0);
        let Some(value) = args.value.as_deref().filter(|v| !v.trim().is_empty()) else {
            eprintln!("'--value' needs to be set when '--field' is specified");
            return None;
        };
        filter.put("value", value);
        let operations = format!("[{}]", OPERATIONS.join(", "));
        let Some(operation) = args.operation.as_deref() else {
            eprintln!("'--operation' must be set when '--field' is specified {operations}");
            return None;
        };
        if !OPERATIONS.contains(&operation) {
            eprintln!("'--operation' must be set to one of the following {operations}");
            return None;
        }
        filter.put("operation", operation);
    } else if let Some(name) = args.queue_name.as_deref().filter(|n| !n.trim().is_empty()) {
        filter.put("field", "NAME");
        filter.put("value", name);
        filter.put("operation", "CONTAINS");
    } else {
        filter.put("field", "");
        filter.put("value", "");
        filter.put("operation", "");
    }
    // `JsonUtil.toJsonObject` of a HashMap, whose order for these keys is
    // the order they are put in.
    Some(filter.to_string())
}

async fn stat(mut args: StatArgs) -> anyhow::Result<()> {
    if let Some(size) = Some(args.max_column_size).filter(|s| *s != -1) {
        let widest = FIELDS.iter().map(|f| f.1.len()).max().unwrap_or(0) as i32;
        if size < widest {
            bail!(UsageError(format!(
                "maxColumnSize must be {widest} or greater or -1 (i.e. no limit)."
            )));
        }
    }
    args.conn.resolve(args.json)?;
    let Some(filter) = create_filter(&args) else {
        return Ok(());
    };
    if args.conn.common.verbose {
        println!("filter is '{filter}'");
        println!("maxRows='{}'", args.max_rows);
    }
    // `createConnectionFactory().close()`: its `tryConnect`, by `--protocol`,
    // the URL of a retry staying with the factory.
    let url = args.conn.url.clone();
    super::jmsapi::Conn::try_connect(&mut args.conn).await?;
    args.conn.url = url;
    let params = || {
        vec![
            Json::from(filter.clone()),
            Json::from(1i64),
            Json::from(i64::from(args.max_rows)),
        ]
    };
    if args.json {
        let reply = perform(&args.conn, async |r| {
            r.invoke("broker", "listQueues", params()).await
        })
        .await?;
        if reply.succeeded {
            println!(
                "{}",
                reply
                    .string()
                    .replace("{\"id\"", "\n   {\"id\"")
                    .replace(']', "\n]")
            );
        } else {
            eprintln!("Failed to get Stats for Queues. Reason: {}", reply.string());
        }
        return Ok(());
    }
    single_execution(&args, params()).await?;
    // -1 is unset; the hidden --loop-sleep wins, and either sleep means
    // --loop.
    let mut sleep = args.sleep;
    if args.old_sleep != -1 {
        sleep = args.old_sleep;
    }
    let use_loop = args.use_loop || sleep != -1;
    if use_loop && sleep == -1 {
        sleep = 60_000;
    }
    if !use_loop {
        return Ok(());
    }
    loop {
        println!("Waiting {sleep} before another queue stat iteration");
        let Ok(millis) = u64::try_from(sleep) else {
            // `Thread.sleep`'s IllegalArgumentException.
            bail!(UsageError("timeout value is negative".to_owned()));
        };
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
        println!(
            "{}>> Queue stat results for {}",
            crate::client::check::java_date_now(),
            crate::client::instance_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_else(|| "null".to_owned())
        );
        if let Err(e) = single_execution(&args, params()).await {
            eprintln!("{e:#}");
        }
    }
}

/// `singleExeuction`: `SimpleManagement`'s node ID and network topology
/// (which matter only to a cluster's report), then the report.
async fn single_execution(args: &StatArgs, params: Vec<Json>) -> anyhow::Result<()> {
    perform_simple(&args.conn, async |r| {
        r.invoke("broker", "getNodeID", Vec::new()).await?;
        r.invoke("broker", "listNetworkTopology", Vec::new()).await
    })
    .await?;
    print_stats(args, params).await
}

async fn print_stats(args: &StatArgs, params: Vec<Json>) -> anyhow::Result<()> {
    let reply = perform(&args.conn, async |r| {
        r.invoke("broker", "listQueues", params).await
    })
    .await?;
    if !reply.succeeded {
        eprintln!("Failed to get Stats for Queues. Reason: {}", reply.string());
        return Ok(());
    }
    let result = Json::parse(&reply.string()).map_err(anyhow::Error::msg)?;
    let count = result.get("count").and_then(Json::as_i64).unwrap_or(0);
    let rows: Vec<&Json> = match result.get("data") {
        Some(Json::Array(rows)) => rows.iter().collect(),
        _ => Vec::new(),
    };
    let cell = |row: &Json, key: &str| -> String {
        match row.get(key) {
            None => String::new(),
            Some(Json::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        }
    };
    let width = |s: &str| s.encode_utf16().count();
    let mut sizes = [0usize; FIELDS.len()];
    let mut titles: Vec<Vec<String>> = Vec::new();
    for (i, f) in FIELDS.iter().enumerate() {
        if args.single_line_header {
            sizes[i] = f.0.len();
        } else {
            let parts: Vec<String> = f.0.split('_').map(str::to_owned).collect();
            sizes[i] = parts.iter().map(|p| p.len()).max().unwrap_or(0);
            titles.push(parts);
        }
    }
    let max = args.max_column_size;
    for row in &rows {
        if !args.include_management && cell(row, "name").starts_with(MANAGEMENT_QUEUE) {
            continue;
        }
        for (i, f) in FIELDS.iter().enumerate() {
            sizes[i] = sizes[i].max(width(&cell(row, f.1)));
            if max != -1 && sizes[i] > max as usize {
                sizes[i] = max as usize;
            }
        }
    }
    // `centralize` is only filled where the titles are split.
    let centre: Vec<bool> = FIELDS
        .iter()
        .map(|f| f.2 && !args.single_line_header)
        .collect();
    let table = TableOut::new("|", 2, sizes.to_vec());
    if args.single_line_header {
        let mut line = String::from("|");
        for (i, f) in FIELDS.iter().enumerate() {
            line.push_str(&padding(f.0, sizes[i], max));
            line.push('|');
        }
        println!("{line}");
    } else {
        // The titles are centred only from the second field on: `centralize`
        // is filled in the loop that splits them, as the data's.
        table.print_lines(&titles, Some(&centre));
    }
    for row in &rows {
        if !args.include_management && cell(row, "name").contains(MANAGEMENT_QUEUE) {
            continue;
        }
        let columns: Vec<String> = FIELDS.iter().map(|f| cell(row, f.1)).collect();
        table.print(&columns, Some(&centre));
    }
    if count > i64::from(args.max_rows) {
        println!(
            "WARNING: the displayed queues are {}/{count}, set maxRows to display more queues.",
            args.max_rows
        );
    }
    Ok(())
}

/// `StatQueue.paddingString`.
fn padding(value: &str, size: usize, max: i32) -> String {
    let mut value = if value.is_empty() {
        "NO DATA".to_owned()
    } else {
        value.to_owned()
    };
    let length = value.encode_utf16().count();
    if length > size && max != -1 {
        let cut: String = value.chars().take(size.saturating_sub(3)).collect();
        return format!("{cut}...");
    }
    for _ in length..size {
        value.push(' ');
    }
    value
}

/// `TableOut`: columns of fixed width, long cells wrapped onto more lines
/// (indented after the first), optionally centred.
pub struct TableOut {
    separator: &'static str,
    indentation: usize,
    sizes: Vec<usize>,
}

impl TableOut {
    pub fn new(separator: &'static str, indentation: usize, sizes: Vec<usize>) -> Self {
        Self {
            separator,
            indentation,
            sizes,
        }
    }

    pub fn print(&self, columns: &[String], centre: Option<&[bool]>) {
        let split: Vec<Vec<String>> = columns
            .iter()
            .zip(&self.sizes)
            .map(|(c, size)| self.split_line(c, *size))
            .collect();
        self.print_lines(&split, centre);
    }

    pub fn print_lines(&self, columns: &[Vec<String>], centre: Option<&[bool]>) {
        let mut line_number = 0;
        loop {
            let mut more = false;
            let mut out = String::from(self.separator);
            for (column, cells) in columns.iter().enumerate() {
                let text = cells.get(line_number).map(String::as_str).unwrap_or("");
                let size = self.sizes[column];
                let len = text.encode_utf16().count();
                let mut cell = String::new();
                if centre.is_some_and(|c| c[column]) && !text.is_empty() {
                    cell.push_str(&" ".repeat(size.saturating_sub(len) / 2));
                }
                cell.push_str(text);
                if line_number + 1 < cells.len() {
                    more = true;
                }
                let mut cell_len = cell.encode_utf16().count();
                while cell_len < size {
                    cell.push(' ');
                    cell_len += 1;
                }
                out.push_str(&cell);
                out.push_str(self.separator);
            }
            println!("{out}");
            line_number += 1;
            if !more {
                break;
            }
        }
    }

    /// `splitLine`.
    fn split_line(&self, column: &str, size: usize) -> Vec<String> {
        let units: Vec<u16> = column.encode_utf16().collect();
        let mut cells = Vec::new();
        let mut position = 0;
        while position < units.len() {
            let indent = if position == 0 { 0 } else { self.indentation };
            let take = size
                .saturating_sub(indent)
                .min(units.len() - position)
                .max(1);
            let text = String::from_utf16_lossy(&units[position..position + take]);
            cells.push(format!("{}{text}", " ".repeat(indent)));
            position += take;
        }
        cells
    }
}

#[cfg(test)]
mod tests {
    use super::TableOut;

    #[test]
    fn long_cells_wrap() {
        let t = TableOut::new("|", 2, vec![5]);
        assert_eq!(t.split_line("abcdefgh", 5), vec!["abcde", "  fgh"]);
        assert_eq!(t.split_line("", 5), Vec::<String>::new());
    }
}
