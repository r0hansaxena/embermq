use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::timeout;

use embermq::broker::Broker;

async fn start_broker() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(embermq::serve(listener, Arc::new(Broker::new())));
    addr
}

struct TestClient {
    lines: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, writer) = stream.into_split();
        TestClient {
            lines: BufReader::new(read_half).lines(),
            writer,
        }
    }

    async fn send(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> String {
        timeout(Duration::from_secs(2), self.lines.next_line())
            .await
            .expect("timed out waiting for a line")
            .unwrap()
            .expect("connection closed")
    }
}

#[tokio::test]
async fn publish_reaches_subscriber() {
    let addr = start_broker().await;

    let mut sub = TestClient::connect(addr).await;
    sub.send("SUB sensors/temp").await;
    assert!(sub.recv().await.starts_with("+OK"));

    let mut publisher = TestClient::connect(addr).await;
    publisher.send("PUB sensors/temp 42.5").await;

    assert_eq!(sub.recv().await, "MSG sensors/temp 42.5");
}

#[tokio::test]
async fn wildcards_filter_correctly() {
    let addr = start_broker().await;

    let mut sub = TestClient::connect(addr).await;
    sub.send("SUB vehicle/+/engine/#").await;
    assert!(sub.recv().await.starts_with("+OK"));

    let mut publisher = TestClient::connect(addr).await;
    publisher.send("PUB vehicle/7/cabin/temp 22").await;
    publisher.send("PUB vehicle/7/engine/rpm 3000").await;

    assert_eq!(sub.recv().await, "MSG vehicle/7/engine/rpm 3000");
}

#[tokio::test]
async fn retained_message_is_delivered_to_late_subscriber() {
    let addr = start_broker().await;

    let mut publisher = TestClient::connect(addr).await;
    publisher.send("PUBR config/interval 30").await;
    publisher.send("PING").await;
    assert_eq!(publisher.recv().await, "+PONG");

    let mut sub = TestClient::connect(addr).await;
    sub.send("SUB config/+").await;
    assert_eq!(sub.recv().await, "MSG config/interval 30");
    assert_eq!(sub.recv().await, "+OK subscribed config/+ retained=1");
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let addr = start_broker().await;

    let mut sub = TestClient::connect(addr).await;
    sub.send("SUB a/b").await;
    assert!(sub.recv().await.starts_with("+OK"));
    sub.send("UNSUB a/b").await;
    assert!(sub.recv().await.starts_with("+OK unsubscribed"));

    let mut publisher = TestClient::connect(addr).await;
    publisher.send("PUB a/b after-unsub").await;

    sub.send("PING").await;
    assert_eq!(sub.recv().await, "+PONG");
}

#[tokio::test]
async fn bad_input_gets_err_and_connection_survives() {
    let addr = start_broker().await;

    let mut client = TestClient::connect(addr).await;
    client.send("BOGUS hello").await;
    assert!(client.recv().await.starts_with("-ERR"));
    client.send("SUB bad/#/middle").await;
    assert!(client.recv().await.starts_with("-ERR"));
    client.send("PING").await;
    assert_eq!(client.recv().await, "+PONG");
}
