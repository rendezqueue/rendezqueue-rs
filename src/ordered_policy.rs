//! Physical allocation policy and explicit cache faults, not called by serve.
use crate::OrderedServer;

impl OrderedServer {
    pub fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    /// Check for a cached SID without refreshing it or changing discovery order.
    pub fn contains_sid(&self, sid: &[u8]) -> bool {
        self.rows.contains_key(sid)
    }

    pub fn evict_opening(&mut self) {
        self.rows.retain(|_, row| row.peer.is_some());
    }

    /// Release substantially excess storage without changing protocol rows.
    pub fn compact(&mut self) {
        if self.rows.capacity() > self.rows.len().saturating_mul(4).max(32) {
            self.rows.shrink_to_fit();
        }
    }
}
