use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct Metrics {
    pub connections: AtomicU64,
    pub msgs_in: AtomicU64,
    pub msgs_out: AtomicU64,
    pub dropped: AtomicU64,
}
