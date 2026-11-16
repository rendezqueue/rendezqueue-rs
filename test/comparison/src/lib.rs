//! Clocked benchmark harness reexporting the production ordered core.
use hashlink::LinkedHashMap;
pub use rendezqueue::OrderedServer;
use rendezqueue::{Bytes, Outcome, Request};
use std::collections::{HashMap, hash_map::RandomState};
use std::time::{Duration, Instant};
type Ordered<K, V> = LinkedHashMap<K, V, RandomState>;

fn oversized(capacity: usize, len: usize) -> bool {
    capacity > len.saturating_mul(4).max(32)
}

/// Pure clocked wrapper; no thread/network maintenance is supplied by this harness.
#[derive(Default)]
pub struct Room {
    core: OrderedServer,
    refreshed: Ordered<Bytes, Instant>,
}

impl Room {
    pub fn core(&self) -> &OrderedServer {
        &self.core
    }
    pub fn refreshed_at(&self, self_xid: &[u8]) -> Option<Instant> {
        self.refreshed.get(self_xid).copied()
    }
    pub fn refresh_len(&self) -> usize {
        self.refreshed.len()
    }
    pub fn refresh_capacity(&self) -> usize {
        self.refreshed.capacity()
    }
    pub fn expire(&mut self, now: Instant, ttl: Duration) -> usize {
        let mut count = 0;
        while let Some((_, touched)) = self.refreshed.front() {
            if now.saturating_duration_since(*touched) < ttl {
                break;
            }
            let (self_xid, _) = self.refreshed.pop_front().unwrap();
            self.core.forget(&self_xid);
            count += 1;
        }
        self.core.compact();
        if oversized(self.refreshed.capacity(), self.refreshed.len()) {
            self.refreshed.shrink_to_fit();
        }
        count
    }
    /// Caller must supply nondecreasing time, as serialized Instant::now does.
    pub fn serve(&mut self, request: &Request, now: Instant, ttl: Duration) -> Outcome {
        assert!(!ttl.is_zero());
        if request.self_xid.is_empty() {
            return Outcome::Reject;
        }
        self.expire(now, ttl);
        let out = self.core.serve(request);
        if matches!(out, Outcome::Ok(_)) {
            self.refreshed.insert(request.self_xid.clone(), now);
        }
        out
    }
}

pub struct Rooms {
    ttl: Duration,
    rooms: HashMap<Bytes, Room>,
}
impl Rooms {
    pub fn new(ttl: Duration) -> Self {
        assert!(!ttl.is_zero());
        Self {
            ttl,
            rooms: HashMap::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.rooms.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }
    pub fn serve(&mut self, room: Bytes, request: &Request, now: Instant) -> Outcome {
        if request.self_xid.is_empty() {
            return Outcome::Reject;
        }
        let state = self.rooms.entry(room.clone()).or_default();
        let out = state.serve(request, now, self.ttl);
        if state.core.is_empty() {
            self.rooms.remove(&room);
        }
        out
    }
    pub fn sweep(&mut self, now: Instant) {
        self.rooms.retain(|_, room| {
            room.expire(now, self.ttl);
            !room.core.is_empty()
        });
        if self.rooms.is_empty() || oversized(self.rooms.capacity(), self.rooms.len()) {
            self.rooms.shrink_to_fit();
        }
    }
}
