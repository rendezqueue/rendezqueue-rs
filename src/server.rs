use crate::{Bytes, Outcome, Reply, Request};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub sid: Bytes,
    pub peer: Option<Bytes>,
    pub values: Vec<Bytes>,
    pub nxt: Option<Bytes>,
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

    pub fn forget(&mut self, sid: &[u8]) {
        self.rows.retain(|r| r.sid != sid);
    }

    pub fn evict_opening(&mut self) {
        self.rows.retain(|r| r.peer.is_some());
    }

    fn lookup(&self, sid: &[u8]) -> Option<&Record> {
        // Indexed traversal avoids Iterator::find's unsupported lifetime
        // constraints in the Rust-to-Lean extractor; first-match order is kept.
        let mut index = 0;
        while index < self.rows.len() {
            let row = &self.rows[index];
            if row.sid == sid {
                return Some(row);
            }
            index += 1;
        }
        None
    }

    fn put(&mut self, row: Record) {
        self.forget(&row.sid);
        self.rows.insert(0, row);
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
                let mut index = 0;
                while index < self.rows.len() {
                    let row = &self.rows[index];
                    if row.sid != r.sid && (row.peer.is_none() || row.peer.as_ref() == Some(&r.sid))
                    {
                        reply.ack = Some(row.sid.clone());
                        break;
                    }
                    index += 1;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sid: &[u8], peer: Option<&[u8]>) -> Record {
        Record {
            sid: sid.to_vec(),
            peer: peer.map(<[u8]>::to_vec),
            values: vec![],
            nxt: None,
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
            sid: b"own".to_vec(),
            ..Request::default()
        };
        assert_eq!(
            server.serve(&request),
            Outcome::Ok(Reply {
                sid: b"own".to_vec(),
                ack: Some(b"first".to_vec()),
                ..Reply::default()
            })
        );
        // The first reciprocal candidate wins when it comes before openings.
        server.rows.swap(2, 3);
        assert_eq!(
            server.serve(&request),
            Outcome::Ok(Reply {
                sid: b"own".to_vec(),
                ack: Some(b"directed".to_vec()),
                ..Reply::default()
            })
        );
        // A recorded binding overrides discovery, even with earlier openings.
        server.rows[0].peer = Some(b"bound".to_vec());
        assert_eq!(
            server.serve(&request),
            Outcome::Ok(Reply {
                sid: b"own".to_vec(),
                ack: Some(b"bound".to_vec()),
                ..Reply::default()
            })
        );
    }
}

// Resource policy is separate from the pure protocol and its extraction.
pub(crate) mod expiry;
