use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, watch};

const LATENCY_SAMPLE: u64 = 16;

struct Config {
    addr: String,
    pubs: usize,
    subs: usize,
    size: usize,
    secs: u64,
}

fn parse_args() -> Config {
    let mut cfg = Config {
        addr: "127.0.0.1:4242".to_owned(),
        pubs: 4,
        subs: 4,
        size: 64,
        secs: 10,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--addr" => cfg.addr = value,
            "--pubs" => cfg.pubs = parse_num(&value, "--pubs"),
            "--subs" => cfg.subs = parse_num(&value, "--subs"),
            "--size" => cfg.size = parse_num(&value, "--size"),
            "--secs" => cfg.secs = parse_num(&value, "--secs") as u64,
            other => die(&format!("unknown flag {other}")),
        }
    }
    cfg
}

fn parse_num(value: &str, flag: &str) -> usize {
    value
        .parse()
        .unwrap_or_else(|_| die(&format!("{flag}: '{value}' is not a number")))
}

fn die(msg: &str) -> ! {
    eprintln!("bench: {msg}");
    eprintln!("usage: bench [--addr HOST:PORT] [--pubs N] [--subs N] [--size BYTES] [--secs N]");
    std::process::exit(1);
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

#[tokio::main]
async fn main() {
    let cfg = parse_args();
    let received = Arc::new(AtomicU64::new(0));
    let published = Arc::new(AtomicU64::new(0));
    let latencies_ns = Arc::new(Mutex::new(Vec::<u64>::new()));
    let (stop_tx, stop_rx) = watch::channel(());

    let mut sub_tasks = Vec::new();
    for _ in 0..cfg.subs {
        let stream = TcpStream::connect(&cfg.addr)
            .await
            .unwrap_or_else(|e| die(&format!("connect {}: {e}", cfg.addr)));
        let _ = stream.set_nodelay(true);
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        write_half.write_all(b"SUB bench/data\n").await.unwrap();
        let ack = lines.next_line().await.unwrap().unwrap_or_default();
        assert!(ack.starts_with("+OK"), "unexpected SUB reply: {ack}");

        let received = received.clone();
        let latencies_ns = latencies_ns.clone();
        let mut stop = stop_rx.clone();
        sub_tasks.push(tokio::spawn(async move {
            let _write_half = write_half;
            let mut count: u64 = 0;
            let mut local_latencies = Vec::new();
            loop {
                tokio::select! {
                    line = lines.next_line() => {
                        let Ok(Some(line)) = line else { break };
                        count += 1;
                        if count % LATENCY_SAMPLE == 0 {
                            if let Some(ts) = line
                                .split(' ')
                                .nth(2)
                                .and_then(|f| f.parse::<u64>().ok())
                            {
                                local_latencies.push(now_nanos().saturating_sub(ts));
                            }
                        }
                    }
                    _ = stop.changed() => break,
                }
            }
            received.fetch_add(count, Relaxed);
            latencies_ns.lock().await.extend(local_latencies);
        }));
    }

    let start = Instant::now();
    let deadline = start + Duration::from_secs(cfg.secs);
    let padding = "x".repeat(cfg.size);

    let mut pub_tasks = Vec::new();
    for _ in 0..cfg.pubs {
        let addr = cfg.addr.clone();
        let padding = padding.clone();
        let published = published.clone();
        pub_tasks.push(tokio::spawn(async move {
            let stream = TcpStream::connect(&addr).await.expect("publisher connect");
            let _ = stream.set_nodelay(true);
            let (_read_half, write_half) = stream.into_split();
            let mut sock = BufWriter::new(write_half);
            let mut count: u64 = 0;
            while Instant::now() < deadline {
                let line = format!("PUB bench/data {} {}\n", now_nanos(), padding);
                if sock.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                count += 1;
                if count % 64 == 0 {
                    let _ = sock.flush().await;
                }
            }
            let _ = sock.flush().await;
            published.fetch_add(count, Relaxed);
        }));
    }

    for task in pub_tasks {
        let _ = task.await;
    }
    let elapsed = start.elapsed().as_secs_f64();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = stop_tx.send(());
    for task in sub_tasks {
        let _ = task.await;
    }

    let pub_total = published.load(Relaxed);
    let recv_total = received.load(Relaxed);
    let mut lat = latencies_ns.lock().await;
    lat.sort_unstable();

    println!("== embermq bench ==");
    println!(
        "config: {} publishers, {} subscribers, {} B payload, {} s",
        cfg.pubs, cfg.subs, cfg.size, cfg.secs
    );
    println!(
        "published  {:>12} msgs   {:>12.0} msg/s",
        pub_total,
        pub_total as f64 / elapsed
    );
    println!(
        "delivered  {:>12} msgs   {:>12.0} msg/s",
        recv_total,
        recv_total as f64 / elapsed
    );
    if lat.is_empty() {
        println!("latency: no samples");
    } else {
        println!(
            "latency    p50={:.0}us  p95={:.0}us  p99={:.0}us   (1/{} sampled)",
            pct(&lat, 0.50),
            pct(&lat, 0.95),
            pct(&lat, 0.99),
            LATENCY_SAMPLE
        );
    }
}

fn pct(sorted_ns: &[u64], q: f64) -> f64 {
    let idx = ((sorted_ns.len() - 1) as f64 * q).round() as usize;
    sorted_ns[idx] as f64 / 1_000.0
}
