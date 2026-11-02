//! Core through saranga-client, as the Artemis perf client's JMS session
//! uses it: an AUTO_ACKNOWLEDGE consumer acknowledging each message (an ack
//! batch of 0), and a producer of BYTES messages with message IDs, sent
//! without blocking. Each side has a runtime of one worker, which runs the
//! client's tasks, while its own loop runs (and the producer waits for its
//! pace and window) on the thread it started on.

use std::io;
use std::time::Duration;

use saranga_client::*;

use crate::{Opts, Pacer, SENT, SIZE, record, stamp, stop};

const QUEUE: &str = "BENCH";

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
}

fn session_options() -> SessionOptions {
    SessionOptions {
        username: Some("admin".into()),
        password: Some("admin".into()),
        ack_batch_size: 0,
        block_on_durable_send: false,
        block_on_non_durable_send: false,
        ..SessionOptions::default()
    }
}

fn err(e: saranga_client::Error) -> io::Error {
    io::Error::other(e.to_string())
}

async fn session(host: &str, port: u16) -> io::Result<(Connection, Session)> {
    let conn = Connection::connect(&format!("{host}:{port}"), ConnectionOptions::default())
        .await
        .map_err(err)?;
    let session = conn.create_session(session_options()).await.map_err(err)?;
    match session
        .create_queue(QueueConfiguration::new(QUEUE).routing_type(RoutingType::Anycast))
        .await
    {
        Err(e) if e.kind() == Some(ErrorType::QueueExists) => {}
        r => r.map_err(err)?,
    }
    Ok((conn, session))
}

pub fn consumer(host: &str, port: u16) -> io::Result<()> {
    runtime()?.block_on(async {
        let (_conn, session) = session(host, port).await?;
        let consumer = session
            .create_consumer(QUEUE, None, false)
            .await
            .map_err(err)?;
        session.start().await.map_err(err)?;
        while !stop() {
            let Some(d) = consumer
                .receive(Duration::from_secs(1))
                .await
                .map_err(err)?
            else {
                continue;
            };
            record(&d.message.body);
            consumer.acknowledge(&d).await.map_err(err)?;
        }
        Ok(())
    })
}

pub fn producer(host: &str, port: u16, rate: u64, o: &Opts) -> io::Result<()> {
    runtime()?.block_on(async {
        let (_conn, session) = session(host, port).await?;
        let producer = session.create_producer(Some(QUEUE)).await.map_err(err)?;
        let mut body = vec![b'x'; SIZE];
        let mut pacer = Pacer::new(rate, o.spin);
        while !stop() {
            stamp(&mut body);
            let mut m = message::bytes_message(&body);
            m.set_durable(o.persistent).set_new_user_id();
            producer.send(m).await.map_err(err)?;
            SENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Sends are written as they are made: nothing to flush.
            pacer.next(&mut || Ok(()))?;
        }
        Ok(())
    })
}
