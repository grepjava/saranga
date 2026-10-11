//! The broker's metrics in the Prometheus text format, with the names and
//! tags Artemis registers them under (`ManagementServiceImpl`'s
//! `registerBrokerMeters`, `registerAddressMeters` and
//! `registerQueueMeters`, through `MetricsManager`'s gauges named
//! `artemis.<name>`) as Micrometer's Prometheus registry writes them:
//! dots made underscores, labels sorted, values as Java writes a double.
//!
//! Each gauge is the management attribute Artemis reads it from.

use std::sync::Arc;

use saranga_server::Broker;
use saranga_server::management::json::Json;

use crate::catalog;
use crate::mbean::Control;

/// The scrape's content type (text format 0.0.4).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// A gauge: its name after `artemis.`, the attribute it reads, and its
/// extra tag.
struct Gauge {
    name: &'static str,
    attribute: &'static str,
    tag: Option<(&'static str, &'static str)>,
}

const fn g(name: &'static str, attribute: &'static str) -> Gauge {
    Gauge {
        name,
        attribute,
        tag: None,
    }
}

const fn tagged(
    name: &'static str,
    attribute: &'static str,
    tag: (&'static str, &'static str),
) -> Gauge {
    Gauge {
        name,
        attribute,
        tag: Some(tag),
    }
}

const BROKER: &[Gauge] = &[
    g("connection.count", "ConnectionCount"),
    g("total.connection.count", "TotalConnectionCount"),
    g("session.count", "SessionCount"),
    g("total.session.count", "TotalSessionCount"),
    g("address.memory.usage", "AddressMemoryUsage"),
    g(
        "address.memory.usage.percentage",
        "AddressMemoryUsagePercentage",
    ),
    g("disk.store.usage", "DiskStoreUsage"),
    g("replica.sync", "ReplicaSync"),
    g("active", "Active"),
    tagged(
        "authentication.count",
        "AuthenticationSuccessCount",
        ("result", "success"),
    ),
    tagged(
        "authentication.count",
        "AuthenticationFailureCount",
        ("result", "failure"),
    ),
    tagged(
        "authorization.count",
        "AuthorizationSuccessCount",
        ("result", "success"),
    ),
    tagged(
        "authorization.count",
        "AuthorizationFailureCount",
        ("result", "failure"),
    ),
];

const ADDRESS: &[Gauge] = &[
    g("routed.message.count", "RoutedMessageCount"),
    g("unrouted.message.count", "UnRoutedMessageCount"),
    g("address.size", "AddressSize"),
    g("number.of.pages", "NumberOfPages"),
    g("limit.percent", "AddressLimitPercent"),
];

const QUEUE: &[Gauge] = &[
    g("message.count", "MessageCount"),
    g("durable.message.count", "DurableMessageCount"),
    g("persistent.size", "PersistentSize"),
    g("durable.persistent.size", "DurablePersistentSize"),
    g("delivering.message.count", "DeliveringCount"),
    g("delivering.durable.message.count", "DurableDeliveringCount"),
    g("delivering.persistent_size", "DeliveringSize"),
    g(
        "delivering.durable.persistent.size",
        "DurableDeliveringSize",
    ),
    g("scheduled.message.count", "ScheduledCount"),
    g("scheduled.durable.message.count", "DurableScheduledCount"),
    g("scheduled.persistent.size", "ScheduledSize"),
    g("scheduled.durable.persistent.size", "DurableScheduledSize"),
    g("messages.acknowledged", "MessagesAcknowledged"),
    g("messages.added", "MessagesAdded"),
    g("messages.killed", "MessagesKilled"),
    g("messages.expired", "MessagesExpired"),
    g("consumer.count", "ConsumerCount"),
];

/// A metric family: help, and samples of sorted labels and a value.
struct Family {
    help: String,
    samples: Vec<(Vec<(String, String)>, f64)>,
}

/// `Double.toString`.
pub fn java_double(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_owned();
    }
    if d.is_infinite() {
        return if d > 0.0 { "+Inf" } else { "-Inf" }.to_owned();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0.0" } else { "0.0" }.to_owned();
    }
    let abs = d.abs();
    if (1e-3..1e7).contains(&abs) {
        let s = format!("{d}");
        if s.contains('.') { s } else { format!("{s}.0") }
    } else {
        // d.dddE±n with the shortest digits.
        let s = format!("{d:e}");
        let (mantissa, exp) = s.split_once('e').unwrap_or((&s, "0"));
        let mantissa = if mantissa.contains('.') {
            mantissa.to_owned()
        } else {
            format!("{mantissa}.0")
        };
        format!("{mantissa}E{exp}")
    }
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_help(v: &str) -> String {
    v.replace('\\', "\\\\").replace('\n', "\\n")
}

fn number(v: &Json) -> Option<f64> {
    match v {
        Json::Long(n) => Some(*n as f64),
        Json::Double(d) => Some(*d),
        Json::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

async fn add(
    families: &mut Vec<(String, Family)>,
    broker: &Arc<Broker>,
    control: &Control,
    resource: &str,
    gauges: &[Gauge],
    labels: &[(&str, String)],
) {
    for gauge in gauges {
        let Ok(value) = broker
            .management()
            .attribute(broker, resource, gauge.attribute, Default::default())
            .await
        else {
            continue;
        };
        let Some(value) = number(&value) else {
            continue;
        };
        let name = format!("artemis_{}", gauge.name.replace('.', "_"));
        let mut l: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect();
        if let Some((k, v)) = gauge.tag {
            l.push((k.to_owned(), v.to_owned()));
        }
        l.sort();
        let help = control
            .attr(gauge.attribute)
            .map(|a| a.desc)
            .unwrap_or_default();
        match families.iter_mut().find(|(n, _)| *n == name) {
            Some((_, f)) => f.samples.push((l, value)),
            None => families.push((
                name,
                Family {
                    help: help.to_owned(),
                    samples: vec![(l, value)],
                },
            )),
        }
    }
}

/// The scrape: every gauge of the broker, its addresses and queues whose
/// address settings enable metrics.
pub async fn scrape(broker: &Arc<Broker>) -> String {
    let name = broker.management().config().name.clone();
    let mut families: Vec<(String, Family)> = Vec::new();
    let broker_label = [("broker", name.clone())];
    add(
        &mut families,
        broker,
        &catalog::BROKER,
        "broker",
        BROKER,
        &broker_label,
    )
    .await;
    let po = broker.post_office();
    let mut addresses = po.addresses();
    addresses.sort_by(|a, b| a.name.cmp(&b.name));
    for a in addresses {
        if !po.settings(&a.name).enable_metrics {
            continue;
        }
        let labels = [("address", a.name.to_string()), ("broker", name.clone())];
        add(
            &mut families,
            broker,
            &catalog::ADDRESS,
            &format!("address.{}", a.name),
            ADDRESS,
            &labels,
        )
        .await;
    }
    let mut queues = po.queues();
    queues.sort_by(|a, b| a.name().cmp(b.name()));
    for q in queues {
        let address = q.config().address.to_string();
        if !po.settings(&address).enable_metrics {
            continue;
        }
        let labels = [
            ("address", address),
            ("broker", name.clone()),
            ("queue", q.name().to_string()),
        ];
        add(
            &mut families,
            broker,
            &catalog::QUEUE,
            &format!("queue.{}", q.name()),
            QUEUE,
            &labels,
        )
        .await;
    }
    families.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = String::new();
    for (name, f) in families {
        out.push_str(&format!("# HELP {name} {}\n", escape_help(&f.help)));
        out.push_str(&format!("# TYPE {name} gauge\n"));
        for (labels, value) in f.samples {
            let l: Vec<String> = labels
                .iter()
                .map(|(k, v)| format!("{k}=\"{}\"", escape_label(v)))
                .collect();
            out.push_str(&format!(
                "{name}{{{}}} {}\n",
                l.join(","),
                java_double(value)
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::java_double;

    #[test]
    fn doubles_as_java() {
        assert_eq!(java_double(0.0), "0.0");
        assert_eq!(java_double(3.0), "3.0");
        assert_eq!(java_double(0.5), "0.5");
        assert_eq!(java_double(1.0e7), "1.0E7");
        assert_eq!(java_double(12345678.0), "1.2345678E7");
        assert_eq!(java_double(0.0001), "1.0E-4");
    }
}
