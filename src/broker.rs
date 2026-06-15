use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::metrics::Metrics;
use crate::topic;

pub const OUTBOUND_QUEUE: usize = 1024;

pub type ConnId = u64;
type Outbound = mpsc::Sender<Arc<str>>;

#[derive(Default)]
pub struct Broker {
    state: RwLock<State>,
    pub metrics: Metrics,
}

#[derive(Default)]
struct State {
    next_id: ConnId,
    clients: HashMap<ConnId, Outbound>,
    subs: HashMap<String, HashSet<ConnId>>,
    retained: HashMap<String, String>,
}

impl Broker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, outbound: Outbound) -> ConnId {
        let mut state = self.state.write().unwrap();
        let id = state.next_id;
        state.next_id += 1;
        state.clients.insert(id, outbound);
        self.metrics.connections.fetch_add(1, Relaxed);
        id
    }

    pub fn unregister(&self, id: ConnId) {
        let mut state = self.state.write().unwrap();
        state.clients.remove(&id);
        state.subs.retain(|_, ids| {
            ids.remove(&id);
            !ids.is_empty()
        });
        self.metrics.connections.fetch_sub(1, Relaxed);
    }

    pub fn subscribe(&self, id: ConnId, pattern: &str) -> usize {
        let mut state = self.state.write().unwrap();
        state.subs.entry(pattern.to_owned()).or_default().insert(id);

        let Some(outbound) = state.clients.get(&id) else {
            return 0;
        };
        let mut delivered = 0;
        for (topic_name, payload) in &state.retained {
            if topic::matches(pattern, topic_name)
                && outbound.try_send(frame(topic_name, payload)).is_ok()
            {
                delivered += 1;
            }
        }
        self.metrics.msgs_out.fetch_add(delivered as u64, Relaxed);
        delivered
    }

    pub fn unsubscribe(&self, id: ConnId, pattern: &str) {
        let mut state = self.state.write().unwrap();
        if let Some(ids) = state.subs.get_mut(pattern) {
            ids.remove(&id);
            if ids.is_empty() {
                state.subs.remove(pattern);
            }
        }
    }

    pub fn publish(&self, topic_name: &str, payload: &str, retain: bool) -> (usize, usize) {
        self.metrics.msgs_in.fetch_add(1, Relaxed);
        let msg = frame(topic_name, payload);

        let mut delivered = 0;
        let mut dropped = 0;
        {
            let state = self.state.read().unwrap();
            let mut already_sent = HashSet::new();
            for (pattern, ids) in &state.subs {
                if !topic::matches(pattern, topic_name) {
                    continue;
                }
                for &id in ids {
                    if !already_sent.insert(id) {
                        continue;
                    }
                    let Some(outbound) = state.clients.get(&id) else {
                        continue;
                    };
                    match outbound.try_send(msg.clone()) {
                        Ok(()) => delivered += 1,
                        Err(TrySendError::Full(_)) => dropped += 1,
                        Err(TrySendError::Closed(_)) => {}
                    }
                }
            }
        }

        if retain {
            let mut state = self.state.write().unwrap();
            if payload.is_empty() {
                state.retained.remove(topic_name);
            } else {
                state
                    .retained
                    .insert(topic_name.to_owned(), payload.to_owned());
            }
        }

        self.metrics.msgs_out.fetch_add(delivered as u64, Relaxed);
        self.metrics.dropped.fetch_add(dropped as u64, Relaxed);
        (delivered, dropped)
    }

    pub fn stats_line(&self) -> String {
        let state = self.state.read().unwrap();
        format!(
            "+STATS conns={} in={} out={} dropped={} patterns={} retained={}\n",
            self.metrics.connections.load(Relaxed),
            self.metrics.msgs_in.load(Relaxed),
            self.metrics.msgs_out.load(Relaxed),
            self.metrics.dropped.load(Relaxed),
            state.subs.len(),
            state.retained.len(),
        )
    }
}

fn frame(topic_name: &str, payload: &str) -> Arc<str> {
    Arc::from(format!("MSG {topic_name} {payload}\n"))
}
