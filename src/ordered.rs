//! XID-indexed PlySwap core with explicit newest-first discovery order.
//! TTL and physical allocation policy are separate from protocol operations.
use crate::{Bytes, Outcome, Record, Reply, Request};
use hashlink::LinkedHashMap;
use std::collections::hash_map::RandomState;

type Ordered<K, V> = LinkedHashMap<K, V, RandomState>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderedServer {
    pub(crate) rows: Ordered<Bytes, Record>,
}

impl OrderedServer {
    pub fn rows(&self) -> impl ExactSizeIterator<Item = &Record> {
        self.rows.values()
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn reset(&mut self) {
        self.rows.clear();
    }
    pub fn forget(&mut self, self_xid: &[u8]) {
        self.rows.remove(self_xid);
    }
    fn lookup(&self, self_xid: &[u8]) -> Option<&Record> {
        self.rows.get(self_xid)
    }
    fn put(&mut self, row: Record) {
        self.rows.insert_front(row.self_xid.clone(), row);
    }
    // W.resolve: reference-first, exact own bound NONEMPTY row and own NEXT.
    fn resolve(&self, wire: &Request) -> Option<Request> {
        if wire.self_xid.is_empty() {
            return None;
        }
        if wire.is_reference() {
            let row = self.lookup(&wire.self_xid)?;
            if row.peer_xid.is_empty() || row.values.is_empty() || row.next_xid != wire.next_xid {
                return None;
            }
            Some(Request {
                self_xid: wire.self_xid.clone(),
                peer_xid: row.peer_xid.clone(),
                next_xid: wire.next_xid.clone(),
                values: row.values.clone(),
            })
        } else if wire.peer_xid.is_empty() {
            (wire.values.is_empty() && wire.next_xid.is_empty()).then(|| wire.clone())
        } else if wire.values.is_empty() {
            Some(wire.clone())
        } else if wire.next_xid.is_empty() {
            None
        } else {
            Some(wire.clone())
        }
    }

    // Base.register, including immutable snapshots and monotone sealing.
    fn register(&mut self, r: &Request, peer_xid: &Bytes) -> bool {
        if r.self_xid == *peer_xid || r.next_xid == r.self_xid || r.next_xid == *peer_xid {
            return false;
        }
        let mut next_xid = r.next_xid.clone();
        let old = self.lookup(&r.self_xid);
        if let Some(old) = old {
            if (!old.peer_xid.is_empty() && old.peer_xid != *peer_xid)
                || ((!old.values.is_empty() || !old.next_xid.is_empty()) && old.values != r.values)
                || (!old.next_xid.is_empty()
                    && !r.next_xid.is_empty()
                    && old.next_xid != r.next_xid)
            {
                return false;
            }
            next_xid = old.next_xid_or(&r.next_xid);
        }
        self.put(Record {
            self_xid: r.self_xid.clone(),
            peer_xid: peer_xid.clone(),
            next_xid,
            values: r.values.clone(),
        });
        true
    }

    // Keep ordered discovery in a shared-borrow context. Inlining this loop
    // in serve's mutable context triggers the pinned extractor's borrow error.
    fn discover(&self, own: &Bytes) -> Bytes {
        for (_, row) in self.rows.iter() {
            if row.self_xid != *own && (row.peer_xid.is_empty() || row.peer_xid == *own) {
                return row.self_xid.clone();
            }
        }
        vec![]
    }

    pub fn serve(&mut self, wire: &Request) -> Outcome {
        let Some(r) = self.resolve(wire) else {
            return Outcome::Reject;
        };
        let mut reply = Reply {
            self_xid: r.self_xid.clone(),
            ..Reply::default()
        };
        if r.peer_xid.is_empty() {
            if self.lookup(&r.self_xid).is_none() {
                self.put(Record {
                    self_xid: r.self_xid.clone(),
                    peer_xid: vec![],
                    next_xid: vec![],
                    values: vec![],
                });
            }
            reply.peer_xid = self
                .lookup(&r.self_xid)
                .map(|row| row.peer_xid.clone())
                .unwrap_or_default();
            if reply.peer_xid.is_empty() {
                reply.peer_xid = self.discover(&r.self_xid);
            }
            return Outcome::Ok(reply);
        };
        let peer_xid = &r.peer_xid;
        if !self.register(&r, peer_xid) {
            return Outcome::Reject;
        }
        // P.boundReply + W.encodeReply: never synthesize a successor row.
        if let Some(row) = self
            .lookup(peer_xid)
            .filter(|row| row.peer_xid == r.self_xid)
        {
            if !row.next_xid.is_empty() {
                reply.next_xid = row.next_xid.clone();
                reply.values = row.values.clone();
            }
        }
        Outcome::Ok(reply)
    }
}
