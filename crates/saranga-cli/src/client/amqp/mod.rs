//! Qpid JMS's AMQP provider, for the CLI's `--protocol AMQP`.

pub mod connection;
pub mod engine;
pub mod jms;
pub mod message;
pub mod properties;
pub mod sasl;

#[cfg(test)]
mod tests {
    use super::connection::Factory;
    use super::jms::ConsumerKind;
    use super::message::AmqpJmsMessage;
    use crate::client::jms::Destination;
    use std::time::Duration;

    /// Against a broker on localhost:61616 with user a/a:
    /// `SARANGA_AMQP_SMOKE=1 cargo test amqp_smoke -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn amqp_smoke() {
        let f = Factory::new(
            Some("a".into()),
            Some("a".into()),
            "amqp://localhost:61616",
            None,
        )
        .unwrap();
        let conn = f.create_connection().await.unwrap();
        let session = conn.session(false).await.unwrap();
        let q = Destination::Queue("amqp.smoke".into());
        let p = session.producer(&q).await.unwrap();
        for i in 0..3 {
            let mut m = AmqpJmsMessage::new_text(Some(&format!("test message: {i}")));
            session
                .send(&p, &mut m, true, 4, 0, true, true)
                .await
                .unwrap();
        }
        p.close().await;
        let b = session.browser(&q, None).await.unwrap();
        let mut browsed = 0;
        while session
            .receive(&b, Some(Duration::ZERO))
            .await
            .unwrap()
            .is_some()
        {
            browsed += 1;
        }
        b.close(&conn).await;
        assert_eq!(browsed, 3);
        let c = session
            .consumer(&q, None, ConsumerKind::Plain)
            .await
            .unwrap();
        for i in 0..3 {
            let m = session
                .receive(&c, Some(Duration::from_secs(3)))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(m.text().unwrap().unwrap(), format!("test message: {i}"));
            println!("{:?}", m.jms_message_id());
        }
        assert!(
            session
                .receive(&c, Some(Duration::from_millis(200)))
                .await
                .unwrap()
                .is_none()
        );
        c.close(&conn).await;
        session.close().await;

        // Transacted: rolled back sends are gone, committed ones arrive.
        let tx = conn.session(true).await.unwrap();
        let p = tx.producer(&q).await.unwrap();
        let mut m = AmqpJmsMessage::new_text(Some("rolled back"));
        tx.send(&p, &mut m, true, 4, 0, true, true).await.unwrap();
        tx.rollback().await.unwrap();
        let mut m = AmqpJmsMessage::new_text(Some("committed"));
        tx.send(&p, &mut m, true, 4, 0, true, true).await.unwrap();
        tx.commit().await.unwrap();
        p.close().await;
        let c = tx.consumer(&q, None, ConsumerKind::Plain).await.unwrap();
        let m = tx
            .receive(&c, Some(Duration::from_secs(3)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.text().unwrap().unwrap(), "committed");
        tx.commit().await.unwrap();
        assert!(
            tx.receive(&c, Some(Duration::from_millis(200)))
                .await
                .unwrap()
                .is_none()
        );
        c.close(&conn).await;
        tx.close().await;
        conn.close().await;

        // A durable subscription, kept across consumers, then removed.
        let f = Factory::new(
            Some("a".into()),
            Some("a".into()),
            "amqp://localhost:61616",
            Some("smoke-client".into()),
        )
        .unwrap();
        let conn = f.create_connection().await.unwrap();
        let s = conn.session(false).await.unwrap();
        let t = Destination::Topic("amqp.smoke.topic".into());
        let kind = ConsumerKind::Durable {
            name: "sub1".into(),
            no_local: false,
        };
        let c = s.consumer(&t, None, kind.clone()).await.unwrap();
        c.close(&conn).await;
        let p = s.producer(&t).await.unwrap();
        let mut m = AmqpJmsMessage::new_text(Some("kept"));
        s.send(&p, &mut m, true, 4, 0, true, true).await.unwrap();
        let c = s.consumer(&t, None, kind).await.unwrap();
        let m = s
            .receive(&c, Some(Duration::from_secs(3)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.text().unwrap().unwrap(), "kept");
        c.close(&conn).await;
        conn.unsubscribe("sub1").await.unwrap();
        let e = conn.unsubscribe("sub1").await.unwrap_err();
        println!("second unsubscribe: {}", e.to_java_string());
        s.close().await;
        conn.close().await;
    }
}
