//! With wildcard routing on, a wildcard address is for consumers only: a
//! send to one fails (`AMQ229260`, as `WildcardAddressManager`), and no
//! queue is auto-created for it to take what is sent to the addresses it
//! matches.

use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

#[tokio::test]
async fn a_send_to_a_wildcard_address_is_refused() {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.address_queue_scan_period = -1;
    let b = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("orders.a", "orders.a");
    spec.auto_create_address = Some(true);
    b.post_office().create_queue(spec, false).unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut s = ServerSession::new(
        b.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    for address in ["orders.#", "orders.*"] {
        let e = s
            .send(
                CoreMessage {
                    address: Some(address.into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err(address);
        assert!(e.to_string().contains("AMQ229260"), "{address}: {e}");
        assert!(b.post_office().queue(address).is_none(), "{address}");
    }
    assert_eq!(
        b.post_office().queue("orders.a").unwrap().message_count(),
        0
    );
    b.stop().unwrap();
}
