# embermq

A tiny, fast pub/sub message broker in Rust — single binary, one dependency
(tokio), ~600 lines. Built to explore the design space of MQTT-style brokers
used in IoT and vehicle telemetry: topic wildcards, retained messages,
slow-consumer handling, and honest benchmarking.

```
publisher ──PUB vehicle/7/engine/rpm 3000──▶ ┌─────────┐
                                             │ embermq │──MSG ...──▶ SUB vehicle/+/engine/#
publisher ──PUB vehicle/9/gps 12.97,77.59──▶ │  :4242  │──MSG ...──▶ SUB vehicle/9/#
                                             └─────────┘──MSG ...──▶ SUB #
```

## Features

- **MQTT-style topics and wildcards** — `vehicle/+/engine/#` (`+` one level,
  `#` any trailing levels), validated on subscribe and matched without
  allocating.
- **Retained messages** — `PUBR` stores the last value per topic; late
  subscribers get it immediately (the classic "last known sensor reading"
  pattern).
- **QoS-0 with an explicit slow-consumer policy** — each connection has a
  bounded outbound queue (1024 frames); when a subscriber can't keep up its
  messages are dropped and counted, so one stuck client can never stall
  publishers or grow broker memory without bound.
- **Write coalescing** — under load, bursts of small frames are merged into a
  few large `write` syscalls per socket.
- **Live metrics** — lock-free atomic counters, queryable in-band with `STATS`.
- **Text protocol you can debug with netcat.**

## Try it (three terminals)

```bash
cargo run --release                      # 1: the broker, on 127.0.0.1:4242

nc 127.0.0.1 4242                        # 2: a subscriber
SUB vehicle/+/engine/#

nc 127.0.0.1 4242                        # 3: a publisher
PUB vehicle/7/engine/rpm 3000
PUBR vehicle/7/config/interval 30        # retained: resubscribe in (2) and see it replayed
STATS
```

## Protocol

| Command | Reply | Meaning |
|---|---|---|
| `SUB <pattern>` | `+OK subscribed <p> retained=<n>` | subscribe; retained matches replayed first |
| `UNSUB <pattern>` | `+OK unsubscribed <p>` | unsubscribe |
| `PUB <topic> <payload>` | *(none — fire-and-forget)* | publish, QoS 0 |
| `PUBR <topic> <payload>` | *(none)* | publish + retain; empty payload clears |
| `STATS` | `+STATS conns=.. in=.. out=.. dropped=..` | live counters |
| `PING` | `+PONG` | liveness / flush barrier |
| `QUIT` | `+BYE` | close |

Subscribers receive `MSG <topic> <payload>`. Errors: `-ERR <reason>`
(connection stays open).

## Architecture

```
                 ┌──────────────────────── Broker (shared, Arc) ───────────────┐
                 │ RwLock<State>: clients, subs: pattern→ids, retained         │
                 │ Metrics: AtomicU64 counters (conns, in, out, dropped)       │
                 └──────────────────────────────────────────────────────────────┘
                        ▲ write lock: (un)register, (un)subscribe
                        ▲ read lock:  publish fan-out (hot path)
accept loop
  └─ per connection: reader task ──parse──▶ broker.publish/subscribe/...
                     writer task ◀──bounded mpsc queue (1024)── try_send from any publisher
                          └─ write-coalescing loop → BufWriter → socket
```

Key decisions, and why:

- **Reader/writer split per connection.** Messages published by *other*
  clients land in this client's outbound queue without touching its reader, so
  a chatty peer can't block another. The queue also gives a single ordered
  stream for both replies and messages.
- **`try_send` + bounded queue on the fan-out path = the broker never awaits a
  slow subscriber.** Dropping (and counting) is the standard QoS-0 trade-off:
  brokers like NATS make the same choice for the same reason.
- **`std::sync::RwLock`, not an async lock.** Critical sections are short and
  never cross an `.await`, so a parking-lot-style lock is cheaper than an async
  mutex, and publishes (read lock) don't serialize against each other.
- **One allocation per publish.** The wire frame is built once as an `Arc<str>`
  and shared by every subscriber's queue by refcount, not by copying.
- **`TCP_NODELAY` plus our own batching.** Nagle would add up to ~40 ms to
  small telemetry frames; instead the write loop coalesces whatever is already
  queued and flushes once per burst.

## Benchmarks

```bash
cargo run --release                  # terminal 1
cargo run --release --bin bench      # terminal 2 (defaults: 4 pubs, 4 subs, 64 B, 10 s)
```

The bench embeds a nanosecond timestamp in each payload; subscribers sample
1/16 messages for end-to-end latency.

Results on my machine (Linux, release build, localhost):

_TODO: run `cargo run --release --bin bench` and paste the output here._

## Tests

```bash
cargo test
```

Unit tests cover topic matching and protocol parsing; integration tests run a
real broker on an ephemeral port and exercise pub/sub, wildcard filtering,
retained replay, unsubscribe, and malformed-input recovery over real sockets.

## Limitations and future work

- Subscription matching is O(patterns) per publish. Fine for thousands of
  patterns; the real fix is a trie keyed by topic level (what production MQTT
  brokers use).
- QoS 0 only — no acks, no redelivery, no persistence.
- No TLS or authentication.
- Shutdown is best-effort: in-flight queues are not drained on Ctrl-C.
