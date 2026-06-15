pub mod broker;
pub mod connection;
pub mod metrics;
pub mod protocol;
pub mod topic;

use std::sync::Arc;

use tokio::net::TcpListener;

pub async fn serve(listener: TcpListener, broker: Arc<broker::Broker>) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(connection::handle(stream, broker.clone()));
    }
}
