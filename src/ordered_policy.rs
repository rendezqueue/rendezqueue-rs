//! Physical allocation policy and explicit cache faults, not called by serve.
use crate::OrderedServer;

impl OrderedServer {
    pub fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    /// Check for a cached XID without refreshing it or changing discovery order.
    pub fn contains_xid(&self, self_xid: &[u8]) -> bool {
        self.rows.contains_key(self_xid)
    }

    pub fn evict_opening(&mut self) {
        self.rows.retain(|_, row| !row.peer_xid.is_empty());
    }

    /// Release substantially excess storage without changing protocol rows.
    pub fn compact(&mut self) {
        if self.rows.capacity() > self.rows.len().saturating_mul(4).max(32) {
            self.rows.shrink_to_fit();
        }
    }
}
