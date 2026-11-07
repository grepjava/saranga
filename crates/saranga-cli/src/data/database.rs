//! `data print` of the relational store (`DescribeNewDatabase`): its tables,
//! as they are, with what they hold decoded.

use std::collections::BTreeMap;
use std::io::{self, Write};

use saranga_server::storage::{GenericRow, Tables, ids};
use saranga_storage::describe::describe_body;

use crate::client::queue::TableOut;

fn banner(out: &mut dyn Write, title: &str) -> io::Result<()> {
    writeln!(out)?;
    writeln!(out, "********************************************")?;
    writeln!(out, "{title}")?;
    writeln!(out, "********************************************")
}

/// `TableOut`'s border: its left end, the joins of the columns, its right end.
fn border(out: &mut dyn Write, sizes: &[usize], ends: [char; 3]) -> io::Result<()> {
    let columns: Vec<String> = sizes.iter().map(|&n| "\u{2500}".repeat(n)).collect();
    let [left, join, right] = ends;
    writeln!(out, "{left}{}{right}", columns.join(&join.to_string()))
}

/// A table of these column widths and its rows, and how many there were.
fn table(
    out: &mut dyn Write,
    sizes: &[usize],
    header: &[&[&str]],
    rows: impl Iterator<Item = Vec<String>>,
    total: &str,
) -> io::Result<()> {
    let t = TableOut::new("\u{2502}", 2, sizes.to_vec());
    let header: Vec<Vec<String>> = header
        .iter()
        .map(|lines| lines.iter().map(|&l| l.to_owned()).collect())
        .collect();
    border(out, sizes, ['\u{250c}', '\u{252c}', '\u{2510}'])?;
    for line in t.rendered(&header, None) {
        writeln!(out, "{line}")?;
    }
    border(out, sizes, ['\u{251c}', '\u{253c}', '\u{2524}'])?;
    let mut count = 0;
    for row in rows {
        for line in t.lines(&row) {
            writeln!(out, "{line}")?;
        }
        count += 1;
    }
    border(out, sizes, ['\u{2514}', '\u{2534}', '\u{2518}'])?;
    writeln!(out, "{total}: {count}")?;
    writeln!(out)
}

fn flag(b: bool) -> String {
    if b { "Y" } else { "N" }.to_owned()
}

fn queue_types(multicast: bool, anycast: bool) -> &'static str {
    match (multicast, anycast) {
        (true, true) => "Multicast/Anycast",
        (true, false) => "Multicast",
        (false, true) => "Anycast",
        (false, false) => "N/A",
    }
}

/// `JournalRecordIds.recordTypeName`.
fn record_type_name(t: u8) -> String {
    let name = ids::ALL.iter().find(|(id, _)| *id == t).map(|(_, n)| *n);
    match name {
        Some("MQTT_PACKET_ID_CORRELATION") => "MQTT_CORRELATION".to_owned(),
        Some(name) => name.replace("_RECORD", ""),
        None => "UNKNOWN".to_owned(),
    }
}

/// `Message.toString`, out of the journal's description of its record.
fn message_text(described: String) -> String {
    match described.split_once(";msg=") {
        Some((_, message)) => message.strip_suffix(')').unwrap_or(message).to_owned(),
        None => described,
    }
}

fn generic(out: &mut dyn Write, rows: &[GenericRow], safe: bool) -> io::Result<()> {
    table(
        out,
        &[10, 26, 130],
        &[&["ID"], &["Record Type"], &["Decoded"]],
        rows.iter().map(|r| {
            let t = u8::try_from(r.record_type).unwrap_or(0);
            let id = match r.tx {
                Some(tx) => format!("{}(tx={tx})", r.id),
                None => r.id.to_string(),
            };
            vec![
                id,
                format!("{} ({})", r.record_type, record_type_name(t)),
                describe_body(t, r.id, false, &r.data, safe),
            ]
        }),
        "Total generic data records",
    )
}

/// `describeDatabase`.
pub fn print(out: &mut dyn Write, tables: &Tables, safe: bool) -> io::Result<()> {
    banner(out, "A D D R E S S E S")?;
    table(
        out,
        &[10, 50, 10, 10],
        &[&["ID"], &["Name"], &["Multicast"], &["Anycast"]],
        tables.addresses.iter().map(|a| {
            vec![
                a.id.to_string(),
                a.name.clone(),
                flag(a.multicast),
                flag(a.anycast),
            ]
        }),
        "Total addresses",
    )?;

    banner(out, "Q U E U E S")?;
    table(
        out,
        &[10, 10, 30, 15, 20, 90],
        &[
            &["ID"],
            &["Address"],
            &["Name"],
            &["Type"],
            &["Filter"],
            &["Config"],
        ],
        tables.queues.iter().map(|q| {
            vec![
                q.id.to_string(),
                q.address.to_string(),
                q.name.clone(),
                queue_types(q.multicast, q.anycast).to_owned(),
                q.filter.clone().unwrap_or_default(),
                q.config.clone(),
            ]
        }),
        "Total queues",
    )?;

    if !tables.prepared.is_empty() {
        banner(out, "P R E P A R E D   T R A N S A C T I O N S")?;
        table(
            out,
            &[15, 150],
            &[&["TX ID"], &["XID"]],
            tables.prepared.iter().map(|(tx, xid)| {
                let text = saranga_storage::describe::xid_text(xid);
                vec![tx.to_string(), text]
            }),
            "Total prepared transactions",
        )?;
    }

    banner(out, "C O N F I G   D A T A")?;
    generic(out, &tables.config, safe)?;
    banner(out, "B R O K E R   D A T A")?;
    generic(out, &tables.broker, safe)?;

    banner(out, "M E S S A G E S")?;
    let mut queues: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for r in &tables.references {
        let mut q = r.queue.to_string();
        if r.paged {
            q.push('*');
        }
        if let Some(tx) = r.ack_tx {
            q.push_str(&format!("(ackTx={tx})"));
        }
        queues.entry(r.message).or_default().push(q);
    }
    table(
        out,
        &[10, 10, 15, 130],
        &[
            &["ID"],
            &["Memory", "Estimate"],
            &["Queues", "* = Paged"],
            &[if safe { "Size" } else { "Message" }],
        ],
        tables.messages.iter().map(|m| {
            let id = match m.tx {
                Some(tx) => format!("{}(tx={tx})", m.id),
                None => m.id.to_string(),
            };
            let on = queues.get(&m.id).map(|q| q.join(", ")).unwrap_or_default();
            let text = if safe {
                format!("{} bytes", m.record.len())
            } else {
                let kind = if m.large && m.core {
                    ids::ADD_LARGE_MESSAGE
                } else {
                    ids::ADD_MESSAGE_PROTOCOL
                };
                message_text(describe_body(kind, m.id, false, &m.record, false))
            };
            vec![id, m.memory_estimate.to_string(), on, text]
        }),
        "Total messages",
    )
}
