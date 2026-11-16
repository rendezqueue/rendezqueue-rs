use crate::{Bytes, Outcome, Request};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Current,
    Previous,
}

/// Local transport context, not extra wire fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Packet {
    pub attempt: u64,
    pub request: Request,
}

/// Finite runtime counters must never wrap and reuse an identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Exhausted;
impl fmt::Display for Exhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PlySwap identity or attempt counter exhausted")
    }
}
impl std::error::Error for Exhausted {}

#[derive(Clone, Copy)]
enum Slot {
    Current,
    Previous,
}

/// Retained client state. Distinct fresh namespaces are a caller precondition.
/// Never restart a lost client with a previously used namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Client {
    namespace: Bytes,
    generation: u64,
    next_attempt: u64,
    peer_xid: Bytes,
    batch: Option<Vec<Bytes>>,
    queued: Vec<Bytes>,
    previous: Option<Request>,
    active: Option<Packet>,
    received: Vec<Bytes>,
    current_cached: bool,
    previous_cached: bool,
}

impl Client {
    pub fn new(namespace: Bytes) -> Self {
        Self {
            namespace,
            generation: 0,
            next_attempt: 0,
            peer_xid: vec![],
            batch: None,
            queued: vec![],
            previous: None,
            active: None,
            received: vec![],
            current_cached: false,
            previous_cached: false,
        }
    }

    // Fixed-width suffix makes (namespace, generation) injective.
    fn id(&self, generation: u64) -> Bytes {
        let mut id = self.namespace.clone();
        id.extend_from_slice(&generation.to_be_bytes());
        id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn received(&self) -> &[Bytes] {
        &self.received
    }
    pub fn previous(&self) -> Option<&Request> {
        self.previous.as_ref()
    }
    pub fn active(&self) -> Option<&Packet> {
        self.active.as_ref()
    }
    pub fn queued(&self) -> &[Bytes] {
        &self.queued
    }
    pub fn batch(&self) -> Option<&[Bytes]> {
        self.batch.as_deref()
    }

    /// Real retry storage only: previous + frozen batch + queued tail.
    pub fn stored(&self) -> Vec<Bytes> {
        self.previous
            .iter()
            .flat_map(|p| p.values.iter())
            .chain(self.batch.iter().flatten())
            .chain(self.queued.iter())
            .cloned()
            .collect()
    }

    pub fn enqueue(&mut self, value: Bytes) {
        self.queued.push(value);
    }

    /// Clears correlation only; live network packets can still be processed.
    pub fn timeout(&mut self) {
        self.active = None;
    }

    fn current(&self) -> Result<Request, Exhausted> {
        let values = if !self.peer_xid.is_empty() {
            self.batch.clone().unwrap_or_default()
        } else {
            vec![]
        };
        let next_xid = if values.is_empty() {
            vec![]
        } else {
            self.id(self.generation.checked_add(1).ok_or(Exhausted)?)
        };
        Ok(Request {
            self_xid: self.id(self.generation),
            peer_xid: self.peer_xid.clone(),
            next_xid,
            values,
        })
    }

    fn slot(&self, r: &Request) -> Result<Option<Slot>, Exhausted> {
        if self.previous.as_ref() == Some(r) {
            return Ok(Some(Slot::Previous));
        }
        Ok((!self.peer_xid.is_empty() && *r == self.current()?).then_some(Slot::Current))
    }

    fn hint(&self, slot: Slot) -> bool {
        match slot {
            Slot::Current => self.current_cached,
            Slot::Previous => self.previous_cached,
        }
    }
    fn set_hint(&mut self, slot: Slot, value: bool) {
        match slot {
            Slot::Current => self.current_cached = value,
            Slot::Previous => self.previous_cached = value,
        }
    }

    pub fn send(&mut self, mode: Mode) -> Result<Option<Packet>, Exhausted> {
        if self.active.is_some() {
            return Ok(None);
        }
        if mode == Mode::Previous && self.previous.is_none() {
            return Ok(None);
        }
        // Check finite bounds before mutating retained storage.
        let next_attempt = self.next_attempt.checked_add(1).ok_or(Exhausted)?;
        if mode == Mode::Current {
            self.generation.checked_add(1).ok_or(Exhausted)?;
            if !self.peer_xid.is_empty() && self.batch.is_none() && !self.queued.is_empty() {
                self.batch = Some(std::mem::take(&mut self.queued));
                self.current_cached = false;
            }
        }
        let mut r = match mode {
            Mode::Current => self.current()?,
            Mode::Previous => self.previous.clone().expect("checked previous"),
        };
        if let Some(slot) = self.slot(&r)? {
            if self.hint(slot) && !r.values.is_empty() && !r.peer_xid.is_empty() {
                r.peer_xid.clear();
                r.values.clear();
            }
        }
        let packet = Packet {
            attempt: self.next_attempt,
            request: r,
        };
        self.next_attempt = next_attempt;
        self.active = Some(packet.clone());
        Ok(Some(packet))
    }

    // W.decodeRequest/P.logical, using only real retained local snapshots.
    fn logical(&self, r: &Request) -> Result<Option<Request>, Exhausted> {
        if r.is_reference() {
            let same = |q: &&Request| {
                q.self_xid == r.self_xid
                    && q.next_xid == r.next_xid
                    && !q.peer_xid.is_empty()
                    && !q.values.is_empty()
            };
            if let Some(p) = self.previous.as_ref().filter(same) {
                return Ok(Some(p.clone()));
            }
            return Ok(Some(self.current()?).filter(|q| same(&q)));
        }
        Ok(if r.peer_xid.is_empty() {
            (r.values.is_empty() && r.next_xid.is_empty()).then(|| r.clone())
        } else {
            (r.values.is_empty() || !r.next_xid.is_empty()).then(|| r.clone())
        })
    }

    /// Exact raw ACTIVE equality first. Stale callbacks do not even clear ACTIVE.
    /// Rejected references clear only their selected slot's hint; retries are explicit.
    pub fn accept(&mut self, packet: &Packet, outcome: &Outcome) -> Result<(), Exhausted> {
        if self.active.as_ref() != Some(packet) {
            return Ok(());
        }
        self.active = None;
        let Some(r) = self.logical(&packet.request)? else {
            return Ok(());
        };
        if let Outcome::Ok(reply) = outcome {
            if !reply.is_valid() {
                return Ok(());
            }
        }
        if r.peer_xid.is_empty() {
            if let Outcome::Ok(reply) = outcome {
                if self.previous.is_none()
                    && self.peer_xid.is_empty()
                    && self.generation == 0
                    && r == self.current()?
                    && reply.self_xid == r.self_xid
                    && !reply.peer_xid.is_empty()
                    && reply.next_xid.is_empty()
                    && reply.peer_xid != r.self_xid
                {
                    self.peer_xid = reply.peer_xid.clone();
                }
            }
            return Ok(());
        }
        let Some(slot) = self.slot(&r)? else {
            return Ok(());
        };
        match outcome {
            Outcome::Reject => {
                if packet.request.is_reference() {
                    self.set_hint(slot, false);
                }
            }
            Outcome::Ok(reply) => {
                if reply.self_xid != r.self_xid {
                    return Ok(());
                }
                // A reference HIT never counts as an upload.
                if !packet.request.values.is_empty() {
                    self.set_hint(slot, true);
                }
                if matches!(slot, Slot::Current) && !reply.next_xid.is_empty() {
                    let peer_next = &reply.next_xid;
                    let generation = self.generation.checked_add(1).ok_or(Exhausted)?;
                    let next = self.id(generation);
                    if *peer_next == next {
                        return Ok(());
                    }
                    self.generation = generation;
                    self.peer_xid = peer_next.clone();
                    self.batch = None;
                    self.previous = Some(Request {
                        next_xid: next,
                        ..r
                    });
                    self.received.extend(reply.values.iter().cloned());
                    self.previous_cached = self.current_cached;
                    self.current_cached = false;
                }
            }
        }
        Ok(())
    }
}
