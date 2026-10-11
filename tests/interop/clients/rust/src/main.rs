//! Phase 7 client matrix: AMQP 1.0 with fe2o3-amqp 0.18, the same checks
//! as tests/interop/clients/python/amqp_check.py.
//!   cargo run -- host:port [test-name...]
//! Run through scripts/client-matrix.sh.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use fe2o3_amqp::types::messaging::{
    AmqpValue, ApplicationProperties, Data, Header, Message, Modified, Properties,
};
use fe2o3_amqp::types::primitives::{OrderedMap, SimpleValue, Value};
use fe2o3_amqp::{Connection, Delivery, Receiver, Sender, Session};

type R = Result<(), String>;

struct Ctx {
    address: String,
    run: String,
}

impl Ctx {
    fn queue(&self, name: &str) -> String {
        format!("clients.rust.{name}.{}", self.run)
    }
}

fn check(cond: bool, what: &str) -> R {
    if cond { Ok(()) } else { Err(what.to_owned()) }
}

fn eq<T: PartialEq + std::fmt::Debug>(want: T, got: T, what: &str) -> R {
    if want == got {
        Ok(())
    } else {
        Err(format!("{what}: want <{want:?}> got <{got:?}>"))
    }
}

fn e(err: impl std::fmt::Debug) -> String {
    format!("{err:?}")
}

async fn recv<T>(r: &mut Receiver, secs: u64) -> Result<Delivery<T>, String>
where
    for<'de> T: fe2o3_amqp::types::messaging::FromBody<'de> + Send,
{
    tokio::time::timeout(Duration::from_secs(secs), r.recv::<T>())
        .await
        .map_err(|_| "no message".to_owned())?
        .map_err(e)
}

macro_rules! session {
    ($ctx:expr, $c:ident, $s:ident) => {
        let mut $c = Connection::open("rust-check", format!("amqp://{}", $ctx.address).as_str())
            .await
            .map_err(e)?;
        let mut $s = Session::begin(&mut $c).await.map_err(e)?;
    };
}

/// Bodies, application properties and header fields come back.
async fn round_trip(ctx: &Ctx) -> R {
    session!(ctx, c, s);
    let q = ctx.queue("roundtrip");
    let mut r = Receiver::attach(&mut s, "r", q.as_str()).await.map_err(e)?;
    let mut snd = Sender::attach(&mut s, "s", q.as_str()).await.map_err(e)?;
    for i in 0..3 {
        let m = Message::builder()
            .header(Header::builder().durable(true).priority(7).build())
            .properties(
                Properties::builder()
                    .subject("greeting")
                    .correlation_id(format!("corr-{i}"))
                    .build(),
            )
            .application_properties(
                ApplicationProperties::builder()
                    .insert("n", i as i32)
                    .insert("who", "rust")
                    .build(),
            )
            .value(format!("hello {i}"))
            .build();
        snd.send(m).await.map_err(e)?;
    }
    for i in 0..3 {
        let d: Delivery<AmqpValue<String>> = recv(&mut r, 5).await?;
        let m = d.message();
        eq(format!("hello {i}"), m.body.0.clone(), "body")?;
        let props = &m.application_properties.as_ref().ok_or("no properties")?.0;
        eq(Some(&SimpleValue::Int(i)), props.get("n"), "property n")?;
        eq(
            Some(&SimpleValue::String("rust".into())),
            props.get("who"),
            "property who",
        )?;
        let p = m.properties.as_ref().ok_or("no properties section")?;
        eq(Some("greeting"), p.subject.as_deref(), "subject")?;
        eq(
            Some(format!("corr-{i}")),
            p.correlation_id.as_ref().map(|c| match c {
                fe2o3_amqp::types::messaging::MessageId::String(s) => s.clone(),
                other => format!("{other:?}"),
            }),
            "correlation id",
        )?;
        let h = m.header.as_ref().ok_or("no header")?;
        eq(7u8, h.priority.0, "priority")?;
        check(h.durable, "durable")?;
        r.accept(&d).await.map_err(e)?;
    }
    snd.close().await.map_err(e)?;
    r.close().await.map_err(e)?;
    s.end().await.map_err(e)?;
    c.close().await.map_err(e)
}

/// A message modified as failed comes back, counted once more.
async fn modified_redelivers(ctx: &Ctx) -> R {
    session!(ctx, c, s);
    let q = ctx.queue("modified");
    let mut r = Receiver::attach(&mut s, "r", q.as_str()).await.map_err(e)?;
    let mut snd = Sender::attach(&mut s, "s", q.as_str()).await.map_err(e)?;
    snd.send(Message::builder().value("again").build())
        .await
        .map_err(e)?;
    let d: Delivery<AmqpValue<String>> = recv(&mut r, 5).await?;
    let count = |d: &Delivery<AmqpValue<String>>| {
        d.message().header.as_ref().map_or(0, |h| h.delivery_count)
    };
    eq(0, count(&d), "first delivery count")?;
    r.modify(
        &d,
        Modified {
            delivery_failed: Some(true),
            undeliverable_here: None,
            message_annotations: None,
        },
    )
    .await
    .map_err(e)?;
    let d: Delivery<AmqpValue<String>> = recv(&mut r, 5).await?;
    eq("again", d.message().body.0.as_str(), "redelivered body")?;
    eq(1, count(&d), "delivery count after a failed delivery")?;
    r.accept(&d).await.map_err(e)?;
    snd.close().await.map_err(e)?;
    r.close().await.map_err(e)?;
    s.end().await.map_err(e)?;
    c.close().await.map_err(e)
}

/// A body far over a frame crosses whole.
async fn large_body(ctx: &Ctx) -> R {
    session!(ctx, c, s);
    let q = ctx.queue("large");
    let mut r = Receiver::attach(&mut s, "r", q.as_str()).await.map_err(e)?;
    let mut snd = Sender::attach(&mut s, "s", q.as_str()).await.map_err(e)?;
    let body: Vec<u8> = (0..600_000u32).map(|i| (i * 7 % 251) as u8).collect();
    snd.send(Message::builder().data(body.clone()).build())
        .await
        .map_err(e)?;
    let d: Delivery<Data> = recv(&mut r, 10).await?;
    check(d.message().body.0.as_ref() == body.as_slice(), "body differs")?;
    r.accept(&d).await.map_err(e)?;
    snd.close().await.map_err(e)?;
    r.close().await.map_err(e)?;
    s.end().await.map_err(e)?;
    c.close().await.map_err(e)
}

/// An amqp-value map keeps its entries and their types.
async fn map_body(ctx: &Ctx) -> R {
    session!(ctx, c, s);
    let q = ctx.queue("map");
    let mut r = Receiver::attach(&mut s, "r", q.as_str()).await.map_err(e)?;
    let mut snd = Sender::attach(&mut s, "s", q.as_str()).await.map_err(e)?;
    let mut map: OrderedMap<Value, Value> = OrderedMap::new();
    map.insert("text".into(), "t".into());
    map.insert("number".into(), Value::Int(42));
    map.insert("flag".into(), Value::Bool(true));
    map.insert("float".into(), Value::Double(1.5.into()));
    snd.send(Message::builder().value(map.clone()).build())
        .await
        .map_err(e)?;
    let d: Delivery<AmqpValue<OrderedMap<Value, Value>>> = recv(&mut r, 5).await?;
    let got = &d.message().body.0;
    eq(map.len(), got.len(), "entries")?;
    for (k, v) in map.iter() {
        eq(Some(v), got.get(k), &format!("entry {k:?}"))?;
    }
    r.accept(&d).await.map_err(e)?;
    snd.close().await.map_err(e)?;
    r.close().await.map_err(e)?;
    s.end().await.map_err(e)?;
    c.close().await.map_err(e)
}

/// A rejected message is not delivered again.
async fn rejected_is_gone(ctx: &Ctx) -> R {
    session!(ctx, c, s);
    let q = ctx.queue("reject");
    let mut r = Receiver::attach(&mut s, "r", q.as_str()).await.map_err(e)?;
    let mut snd = Sender::attach(&mut s, "s", q.as_str()).await.map_err(e)?;
    snd.send(Message::builder().value("bad").build())
        .await
        .map_err(e)?;
    snd.send(Message::builder().value("good").build())
        .await
        .map_err(e)?;
    let d: Delivery<AmqpValue<String>> = recv(&mut r, 5).await?;
    eq("bad", d.message().body.0.as_str(), "first")?;
    r.reject(&d, None).await.map_err(e)?;
    let d: Delivery<AmqpValue<String>> = recv(&mut r, 5).await?;
    eq("good", d.message().body.0.as_str(), "second")?;
    r.accept(&d).await.map_err(e)?;
    if let Ok(d) = recv::<AmqpValue<String>>(&mut r, 1).await {
        return Err(format!("delivered again: {}", d.message().body.0));
    }
    snd.close().await.map_err(e)?;
    r.close().await.map_err(e)?;
    s.end().await.map_err(e)?;
    c.close().await.map_err(e)
}

type Test = for<'a> fn(&'a Ctx) -> Pin<Box<dyn Future<Output = R> + 'a>>;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ctx = Ctx {
        address: args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:61616".into()),
        run: format!("{:x}", std::process::id() ^ (Instant::now().elapsed().subsec_nanos())),
    };
    let only: Vec<&String> = args.iter().skip(2).collect();
    let tests: [(&str, Test); 5] = [
        ("large_body", |c| Box::pin(large_body(c))),
        ("map_body", |c| Box::pin(map_body(c))),
        ("modified_redelivers", |c| Box::pin(modified_redelivers(c))),
        ("rejected_is_gone", |c| Box::pin(rejected_is_gone(c))),
        ("round_trip", |c| Box::pin(round_trip(c))),
    ];
    let mut failed = 0;
    for (name, test) in tests {
        if !only.is_empty() && !only.iter().any(|o| *o == name) {
            continue;
        }
        let start = Instant::now();
        let label = format!("rust.{name}");
        match test(&ctx).await {
            Ok(()) => println!("PASS {label:<40} {:5} ms", start.elapsed().as_millis()),
            Err(err) => {
                failed += 1;
                println!("FAIL {label:<40} {err}");
            }
        }
    }
    println!(
        "{}",
        if failed == 0 {
            "ALL PASSED".to_owned()
        } else {
            format!("{failed} FAILED")
        }
    );
    std::process::exit(if failed == 0 { 0 } else { 1 });
}
