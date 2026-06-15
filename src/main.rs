use std::sync::Arc;

use tokio::net::TcpListener;

use embermq::broker::Broker;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:4242".to_owned());
    let listener = TcpListener::bind(&addr).await?;
    let broker = Arc::new(Broker::new());
    eprintln!("embermq listening on {addr}");

    tokio::select! {
        result = embermq::serve(listener, broker.clone()) => result,
        _ = tokio::signal::ctrl_c() => {
            eprint!("\nshutting down — {}", broker.stats_line());
            Ok(())
        }
    }
}
