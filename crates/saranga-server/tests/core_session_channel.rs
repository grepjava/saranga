//! A CreateSession on a session channel already in use replaces the
//! channel's session, and the session it replaced still ends with the
//! connection (Artemis's replaced `ServerSessionImpl` is a failure listener
//! of the connection), so the connection's teardown finishes.

use std::sync::Arc;
use std::time::Duration;

use saranga_core_protocol::packets::connection::CreateSessionMessage;
use saranga_core_protocol::{Context, Direction, Packet};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

fn create_session(name: &str, channel: i64) -> Vec<u8> {
    Packet::from(CreateSessionMessage {
        name: name.into(),
        session_channel_id: channel,
        version: 136,
        username: None,
        password: None,
        min_large_message_size: 102400,
        xa: false,
        auto_commit_sends: true,
        auto_commit_acks: true,
        window_size: -1,
        pre_acknowledge: false,
        default_address: None,
    })
    .encode_frame(1, &Context::new(Direction::ToServer, 136))
}

async fn settled(broker: &Arc<Broker>, want: (usize, usize)) -> (usize, usize) {
    let mut now = (0, 0);
    for _ in 0..100 {
        now = (
            broker.registry().connections().len(),
            broker.registry().sessions().len(),
        );
        if now == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    now
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_session_ends_with_its_connection() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"ARTEMIS").await.unwrap();
    s.write_all(&create_session("s1", 10)).await.unwrap();
    s.write_all(&create_session("s2", 10)).await.unwrap();
    assert_eq!(settled(&broker, (1, 2)).await, (1, 2));
    drop(s);
    assert_eq!(
        settled(&broker, (0, 0)).await,
        (0, 0),
        "the connection or a session was left behind"
    );
    task.abort();
    broker.stop().unwrap();
}
