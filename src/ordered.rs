//! SID-indexed PlySwap core with explicit newest-first discovery order.
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
    pub fn forget(&mut self, sid: &[u8]) {
        self.rows.remove(sid);
    }
    fn lookup(&self, sid: &[u8]) -> Option<&Record> {
        self.rows.get(sid)
    }
    fn put(&mut self, row: Record) {
        self.rows.insert_front(row.sid.clone(), row);
    }
    // W.resolve: reference-first, exact own bound NONEMPTY row and own NXT.
    fn resolve(&self, wire: &Request) -> Option<Request> {
        if wire.is_reference() {
            let row = self.lookup(&wire.sid)?;
            if row.peer.is_none() || row.values.is_empty() || row.nxt != wire.nxt {
                return None;
            }
            Some(Request {
                sid: wire.sid.clone(),
                ack: row.peer.clone(),
                values: row.values.clone(),
                nxt: wire.nxt.clone(),
            })
        } else if wire.ack.is_none() {
            (wire.values.is_empty() && wire.nxt.is_none()).then(|| wire.clone())
        } else {
            (wire.values.is_empty() || wire.nxt.is_some()).then(|| wire.clone())
        }
    }

    // Base.register, including immutable snapshots and monotone sealing.
    fn register(&mut self, r: &Request, peer: &Bytes) -> bool {
        if r.sid == *peer || r.nxt.as_ref() == Some(&r.sid) || r.nxt.as_ref() == Some(peer) {
            return false;
        }
        let old = self.lookup(&r.sid);
        if let Some(old) = old {
            if (old.peer.is_some() && old.peer.as_ref() != Some(peer))
                || ((!old.values.is_empty() || old.nxt.is_some()) && old.values != r.values)
                || (old.nxt.is_some() && r.nxt.is_some() && old.nxt != r.nxt)
            {
                return false;
            }
        }
        let nxt = old
            .and_then(|row| row.nxt.clone())
            .or_else(|| r.nxt.clone());
        self.put(Record {
            sid: r.sid.clone(),
            peer: Some(peer.clone()),
            values: r.values.clone(),
            nxt,
        });
        true
    }

    // Keep ordered discovery in a shared-borrow context. Inlining this loop
    // in serve's mutable context triggers the pinned extractor's borrow error.
    fn discover(&self, own: &Bytes) -> Option<Bytes> {
        for (_, row) in self.rows.iter() {
            if row.sid != *own && (row.peer.is_none() || row.peer.as_ref() == Some(own)) {
                return Some(row.sid.clone());
            }
        }
        None
    }

    pub fn serve(&mut self, wire: &Request) -> Outcome {
        let Some(r) = self.resolve(wire) else {
            return Outcome::Reject;
        };
        let mut reply = Reply {
            sid: r.sid.clone(),
            ..Reply::default()
        };
        let Some(peer) = &r.ack else {
            if self.lookup(&r.sid).is_none() {
                self.put(Record {
                    sid: r.sid.clone(),
                    peer: None,
                    values: vec![],
                    nxt: None,
                });
            }
            reply.ack = self.lookup(&r.sid).and_then(|row| row.peer.clone());
            if reply.ack.is_none() {
                reply.ack = self.discover(&r.sid);
            }
            return Outcome::Ok(reply);
        };
        if !self.register(&r, peer) {
            return Outcome::Reject;
        }
        // P.boundReply + W.encodeReply: never synthesize a successor row.
        if let Some(row) = self
            .lookup(peer)
            .filter(|row| row.peer.as_ref() == Some(&r.sid))
        {
            if row.nxt.is_some() {
                reply.nxt = row.nxt.clone();
                reply.values = row.values.clone();
            }
        }
        Outcome::Ok(reply)
    }
}
