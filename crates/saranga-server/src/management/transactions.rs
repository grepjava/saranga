//! `ActiveMQServerControl`'s XA operations: the prepared branches, and
//! completing them by hand (heuristic commits and rollbacks).

use std::sync::Arc;

use saranga_message::message_type;

use super::Outcome;
use super::args::{Call, Ty::*};
use super::json::Json;
use crate::Broker;
use crate::transaction::{self, TxRef, Xid, xid_base64, xid_string};

/// `DateFormat.getDateTimeInstance(SHORT, MEDIUM)` in the `en_US` locale
/// of JDK 20 and later, in the local time zone: `10/11/26, 11:33:10 AM` (a
/// narrow no-break space before the marker).
pub(super) fn short_medium(millis: i64) -> String {
    short_medium_at(millis + crate::localtime::offset_millis(millis))
}

/// [`short_medium`] of a time already moved to its zone.
fn short_medium_at(local: i64) -> String {
    let (y, m, d, tod) = civil(local);
    let h = tod / 3600;
    let h12 = if h % 12 == 0 { 12 } else { h % 12 };
    format!(
        "{m}/{d}/{:02}, {h12}:{:02}:{:02}\u{202f}{}",
        y.rem_euclid(100),
        tod % 3600 / 60,
        tod % 60,
        if h < 12 { "AM" } else { "PM" }
    )
}

/// `DateFormat.getDateInstance(SHORT)` in the `en_US` locale of a day,
/// counted in days since 1970-01-01 in its zone: `10/11/26`.
pub(super) fn short_day(day: i64) -> String {
    let (y, m, d, _) = civil(day * 86_400_000);
    format!("{m}/{d}/{:02}", y.rem_euclid(100))
}

/// Year, month, day and second of the day of an epoch time, in UTC or as
/// moved to a zone (Howard Hinnant's algorithm).
fn civil(millis: i64) -> (i64, i64, i64, i64) {
    let secs = millis.div_euclid(1000);
    let z = secs.div_euclid(86_400) + 719_468;
    let tod = secs.rem_euclid(86_400);
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d, tod)
}

/// `new String(bytes)`: the platform charset, UTF-8.
fn java_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `CoreTransactionDetail.decodeMessageType`.
fn message_type_name(m: &crate::message::ServerMessage) -> &'static str {
    if m.native().is_some() {
        return "N/A";
    }
    match m.core().message_type {
        message_type::DEFAULT => "Default",
        message_type::OBJECT => "ObjectMessage",
        message_type::TEXT => "TextMessage",
        message_type::BYTES => "ByteMessage",
        message_type::MAP => "MapMessage",
        message_type::STREAM => "StreamMessage",
        _ => "(Unknown Type)",
    }
}

/// `TransactionDetail.toJSON`.
fn detail(xid: &Xid, tx: &TxRef) -> Json {
    let t = transaction::lock(tx);
    let mut messages = Vec::new();
    for (kind, list) in [("(+) send", &t.sends), ("(-) receive", &t.acks)] {
        for (_, r) in list {
            messages.push(
                Json::object()
                    .with("message_operation_type", kind)
                    .with("message_type", message_type_name(&r.message))
                    .with("message_properties", super::messages::to_map(r, 256)),
            );
        }
    }
    Json::object()
        .with("creation_time", short_medium(t.created))
        .with("xid_as_base64", xid_base64(xid))
        .with("xid_format_id", xid.format_id)
        .with("xid_global_txid", java_string(&xid.global_transaction_id))
        .with("xid_branch_qual", java_string(&xid.branch_qualifier))
        .with("tx_related_messages", Json::Array(messages))
}

/// `JsonValue.toString` of a member, as the HTML listing prints it.
fn value_text(v: Option<&Json>) -> String {
    v.map_or_else(|| "null".to_owned(), Json::to_string)
}

/// `listPreparedTransactionDetailsAsHTML`.
fn details_html(details: &[Json]) -> String {
    if details.is_empty() {
        return "<h3>*** Prepared Transaction Details ***</h3><p>No entry.</p>".to_owned();
    }
    let mut html = String::from("<h3>*** Prepared Transaction Details ***</h3>");
    for d in details {
        let g = |k: &str| value_text(d.get(k));
        html.push_str("<table border=\"1\">");
        html.push_str(&format!(
            "<tr><th>creation_time</th><td>{}</td><th>xid_as_base_64</th><td colspan=\"3\">{}</td></tr>",
            g("creation_time"),
            g("xid_as_base64")
        ));
        html.push_str(&format!(
            "<tr><th>xid_format_id</th><td>{}</td><th>xid_global_txid</th><td>{}</td><th>xid_branch_qual</th><td>{}</td></tr>",
            g("xid_format_id"),
            g("xid_global_txid"),
            g("xid_branch_qual")
        ));
        html.push_str("<tr><th colspan=\"6\">Message List</th></tr><tr><td colspan=\"6\">");
        html.push_str("<table border=\"1\" cellspacing=\"0\" cellpadding=\"0\">");
        if let Some(Json::Array(messages)) = d.get("tx_related_messages") {
            for m in messages {
                let mut props = String::new();
                if let Some(Json::Object(p)) = m.get("message_properties") {
                    for (k, v) in p {
                        props.push_str(&format!("{k}={v}, "));
                    }
                }
                html.push_str(&format!(
                    "<th>operation_type</th><td>{}</th><th>message_type</th><td>{}</td></tr><tr><th>properties</th><td colspan=\"3\">{props}</td></tr>",
                    value_text(m.get("message_operation_type")),
                    value_text(m.get("message_type"))
                ));
            }
        }
        html.push_str("</table></td></tr></table><br>");
    }
    html
}

/// `commitPreparedTransaction` / `rollbackPreparedTransaction`: the
/// prepared branch with this base64 Xid is completed and recorded as a
/// heuristic completion. False when there is none.
async fn complete(broker: &Arc<Broker>, base64: &str, commit: bool) -> Outcome {
    let rm = broker.resource_manager();
    let Some((xid, _)) = rm
        .prepared()
        .into_iter()
        .find(|(x, _)| xid_base64(x) == base64)
    else {
        return Ok(Json::from(false));
    };
    let Some(tx) = rm.remove(&xid) else {
        return Ok(Json::from(false));
    };
    let result = {
        let mut t = transaction::lock(&tx);
        if commit {
            t.commit(false).map_err(|f| f.0)
        } else {
            t.rollback()
        }
    };
    let mut outcome = result.map_err(|e| e.message)?;
    let storage = broker.storage().cloned();
    let written = outcome.write(storage.as_deref()).map_err(|e| e.message)?;
    if let Some(p) = written {
        p.wait().await.map_err(|e| e.to_string())?;
    }
    outcome.apply();
    let record = broker.post_office().ids().next();
    if let Some(s) = &storage {
        let p = s
            .store_heuristic_completion(record, &xid, commit)
            .map_err(|e| e.to_string())?;
        p.wait().await.map_err(|e| e.to_string())?;
    }
    rm.put_heuristic(record, xid, commit);
    Ok(Json::from(true))
}

pub(super) async fn invoke(broker: &Arc<Broker>, call: &Call<'_>) -> Option<Outcome> {
    let rm = broker.resource_manager();
    if call.is("listPreparedTransactions", &[]).is_some() {
        let all: Vec<String> = rm
            .prepared()
            .iter()
            .map(|(x, tx)| {
                format!(
                    "{} base64: {} {}",
                    short_medium(transaction::lock(tx).created),
                    xid_base64(x),
                    xid_string(x)
                )
            })
            .collect();
        return Some(Ok(Json::from(all)));
    }
    if call
        .is("listPreparedTransactionDetailsAsJSON", &[])
        .is_some()
    {
        let details: Vec<Json> = rm.prepared().iter().map(|(x, tx)| detail(x, tx)).collect();
        if details.is_empty() {
            return Some(Ok(Json::from("")));
        }
        return Some(Ok(Json::from(Json::Array(details).to_string())));
    }
    if call
        .is("listPreparedTransactionDetailsAsHTML", &[])
        .is_some()
    {
        let details: Vec<Json> = rm.prepared().iter().map(|(x, tx)| detail(x, tx)).collect();
        return Some(Ok(Json::from(details_html(&details))));
    }
    if call.is("listHeuristicCommittedTransactions", &[]).is_some() {
        let all: Vec<String> = rm.heuristics(true).iter().map(xid_base64).collect();
        return Some(Ok(Json::from(all)));
    }
    if call
        .is("listHeuristicRolledBackTransactions", &[])
        .is_some()
    {
        let all: Vec<String> = rm.heuristics(false).iter().map(xid_base64).collect();
        return Some(Ok(Json::from(all)));
    }
    if let Some(a) = call.is("commitPreparedTransaction", &[S]) {
        return Some(complete(broker, &a.s_or_empty(0), true).await);
    }
    if let Some(a) = call.is("rollbackPreparedTransaction", &[S]) {
        return Some(complete(broker, &a.s_or_empty(0), false).await);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{short_day, short_medium_at};

    #[test]
    fn short_medium_dates() {
        assert_eq!(short_medium_at(0), "1/1/70, 12:00:00\u{202f}AM");
        // 2026-10-11 13:05:09
        assert_eq!(
            short_medium_at(1_791_723_909_000),
            "10/11/26, 1:05:09\u{202f}PM"
        );
        assert_eq!(short_day(1_791_723_909_000 / 86_400_000), "10/11/26");
    }
}
