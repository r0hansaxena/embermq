use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;

use crate::broker::{Broker, OUTBOUND_QUEUE};
use crate::protocol::{self, Command};

pub async fn handle(stream: TcpStream, broker: Arc<Broker>) {
    let _ = stream.set_nodelay(true);
    let (read_half, write_half) = stream.into_split();

    let (outbound, outbound_rx) = mpsc::channel::<Arc<str>>(OUTBOUND_QUEUE);
    let id = broker.register(outbound.clone());
    let writer = tokio::spawn(write_loop(write_half, outbound_rx));

    let mut lines = BufReader::new(read_half).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        let reply = match protocol::parse(line) {
            Err(e) => Some(format!("-ERR {e}\n")),
            Ok(Command::Pub {
                topic,
                payload,
                retain,
            }) => {
                broker.publish(&topic, &payload, retain);
                None
            }
            Ok(Command::Sub(pattern)) => {
                let retained = broker.subscribe(id, &pattern);
                Some(format!("+OK subscribed {pattern} retained={retained}\n"))
            }
            Ok(Command::Unsub(pattern)) => {
                broker.unsubscribe(id, &pattern);
                Some(format!("+OK unsubscribed {pattern}\n"))
            }
            Ok(Command::Stats) => Some(broker.stats_line()),
            Ok(Command::Ping) => Some("+PONG\n".to_owned()),
            Ok(Command::Quit) => {
                let _ = outbound.send(Arc::from("+BYE\n")).await;
                break;
            }
        };
        if let Some(reply) = reply {
            if outbound.send(Arc::from(reply)).await.is_err() {
                break;
            }
        }
    }

    broker.unregister(id);
    drop(outbound);
    let _ = writer.await;
}

async fn write_loop(write_half: OwnedWriteHalf, mut queue: mpsc::Receiver<Arc<str>>) {
    let mut sock = BufWriter::new(write_half);
    while let Some(msg) = queue.recv().await {
        if sock.write_all(msg.as_bytes()).await.is_err() {
            return;
        }
        while let Ok(msg) = queue.try_recv() {
            if sock.write_all(msg.as_bytes()).await.is_err() {
                return;
            }
        }
        if sock.flush().await.is_err() {
            return;
        }
    }
}
