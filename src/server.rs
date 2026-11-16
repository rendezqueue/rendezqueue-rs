use crate::{Bytes, Outcome, Reply, Request};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub self_xid: Bytes,
    pub peer_xid: Bytes,
    pub next_xid: Bytes,
    pub values: Vec<Bytes>,
}

impl Record {
    // Keep this branch in a shared-borrow context for the pinned extractor.
    pub(crate) fn next_xid_or(&self, fallback: &Bytes) -> Bytes {
        if self.next_xid.is_empty() {
            fallback.clone()
        } else {
            self.next_xid.clone()
        }
    }
}

/// Newest-first rows, matching model discovery order. No TTL is implicit.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Server {
    rows: Vec<Record>,
}

impl Server {
    pub fn rows(&self) -> &[Record] {
        &self.rows
    }

    pub fn reset(&mut self) {
        self.rows.clear();
    }

    pub fn forget(&mut self, self_xid: &[u8]) {
        self.rows.retain(|r| r.self_xid != self_xid);
    }

    pub fn evict_opening(&mut self) {
        self.rows.retain(|r| !r.peer_xid.is_empty());
    }

    fn lookup(&self, self_xid: &[u8]) -> Option<&Record> {
        // Indexed traversal avoids Iterator::find's unsupported lifetime
        // constraints in the Rust-to-Lean extractor; first-match order is kept.
        let mut index = 0;
        while index < self.rows.len() {
            let row = &self.rows[index];
            if row.self_xid == self_xid {
                return Some(row);
            }
            index += 1;
        }
        None
    }

    fn put(&mut self, row: Record) {
        self.forget(&row.self_xid);
        self.rows.insert(0, row);
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
                let mut index = 0;
                while index < self.rows.len() {
                    let row = &self.rows[index];
                    if row.self_xid != r.self_xid
                        && (row.peer_xid.is_empty() || row.peer_xid == r.self_xid)
                    {
                        reply.peer_xid = row.self_xid.clone();
                        break;
                    }
                    index += 1;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(self_xid: &[u8], peer_xid: Option<&[u8]>) -> Record {
        Record {
            self_xid: self_xid.to_vec(),
            peer_xid: peer_xid.map(<[u8]>::to_vec).unwrap_or_default(),
            next_xid: vec![],
            values: vec![],
        }
    }

    #[test]
    fn lookup_keeps_first_match_even_with_duplicate_rows() {
        let server = Server {
            rows: vec![
                record(b"other", None),
                record(b"same", None),
                record(b"same", None),
            ],
        };
        assert!(std::ptr::eq(
            server.lookup(b"same").unwrap(),
            &server.rows[1]
        ));
        assert!(server.lookup(b"absent").is_none());
    }

    #[test]
    fn discovery_keeps_first_eligible_row_and_existing_binding() {
        let mut server = Server {
            rows: vec![
                record(b"blocked", Some(b"someone else")),
                record(b"first", None),
                record(b"directed", Some(b"own")),
                record(b"last", None),
            ],
        };
        let request = Request {
            self_xid: b"own".to_vec(),
            ..Request::default()
        };
        assert_eq!(
            server.serve(&request),
            Outcome::Ok(Reply {
                self_xid: b"own".to_vec(),
                peer_xid: b"first".to_vec(),
                ..Reply::default()
            })
        );
        // The first reciprocal candidate wins when it comes before openings.
        server.rows.swap(2, 3);
        assert_eq!(
            server.serve(&request),
            Outcome::Ok(Reply {
                self_xid: b"own".to_vec(),
                peer_xid: b"directed".to_vec(),
                ..Reply::default()
            })
        );
        // A recorded binding overrides discovery, even with earlier openings.
        server.rows[0].peer_xid = b"bound".to_vec();
        assert_eq!(
            server.serve(&request),
            Outcome::Ok(Reply {
                self_xid: b"own".to_vec(),
                peer_xid: b"bound".to_vec(),
                ..Reply::default()
            })
        );
    }
}

// Resource policy is separate from the pure protocol and its extraction.
pub(crate) mod expiry;
