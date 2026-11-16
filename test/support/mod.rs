#![allow(dead_code)]
use rendezqueue::{Bytes, Client, Mode, Outcome, Packet, Server};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    A,
    B,
}
impl Side {
    fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Envelope {
    pub side: Side,
    pub packet: Packet,
    pub outcome: Outcome,
}

/// Test transport: indexed queues, live timed-out packets, no automatic retry.
#[derive(Clone, Debug)]
pub struct World {
    pub clients: [Client; 2],
    pub server: Server,
    pub requests: Vec<(Side, Packet)>,
    pub replies: Vec<Envelope>,
    pub inputs: [Vec<Bytes>; 2],
    pub emissions: Vec<(Side, Packet)>,
}
impl Default for World {
    fn default() -> Self {
        Self {
            // Reversed namespace order deliberately avoids XID rank assumptions.
            clients: [Client::new(b"z".to_vec()), Client::new(b"a".to_vec())],
            server: Server::default(),
            requests: vec![],
            replies: vec![],
            inputs: [vec![], vec![]],
            emissions: vec![],
        }
    }
}
pub fn id(side: Side, generation: u64) -> Bytes {
    let mut v = match side {
        Side::A => b"z".to_vec(),
        Side::B => b"a".to_vec(),
    };
    v.extend(generation.to_be_bytes());
    v
}
impl World {
    pub fn client(&self, s: Side) -> &Client {
        &self.clients[s.index()]
    }
    pub fn enqueue(&mut self, s: Side, v: &[u8]) {
        self.clients[s.index()].enqueue(v.to_vec());
        self.inputs[s.index()].push(v.to_vec());
    }
    pub fn send(&mut self, s: Side, m: Mode) {
        if let Some(p) = self.clients[s.index()].send(m).unwrap() {
            self.emissions.push((s, p.clone()));
            self.requests.push((s, p));
        }
    }
    pub fn process(&mut self, i: usize) {
        if i < self.requests.len() {
            let (s, p) = self.requests.remove(i);
            let outcome = self.server.serve(&p.request);
            self.replies.push(Envelope {
                side: s,
                packet: p,
                outcome,
            });
        }
    }
    pub fn deliver(&mut self, i: usize) {
        if i < self.replies.len() {
            let e = self.replies.remove(i);
            self.clients[e.side.index()]
                .accept(&e.packet, &e.outcome)
                .unwrap();
        }
    }
    pub fn timeout(&mut self, s: Side) {
        self.clients[s.index()].timeout();
    }
    pub fn duplicate_request(&mut self, i: usize) {
        if let Some(p) = self.requests.get(i) {
            self.requests.push(p.clone());
        }
    }
    pub fn duplicate_reply(&mut self, i: usize) {
        if let Some(e) = self.replies.get(i) {
            self.replies.push(e.clone());
        }
    }
    pub fn drop_request(&mut self, i: usize) {
        if i < self.requests.len() {
            self.requests.remove(i);
        }
    }
    pub fn drop_reply(&mut self, i: usize) {
        if i < self.replies.len() {
            self.replies.remove(i);
        }
    }
    pub fn rpc_at(&mut self, s: Side, m: Mode, i: usize) {
        self.send(s, m);
        self.process(i);
        self.deliver(0);
    }
    pub fn rpc(&mut self, s: Side, m: Mode) {
        self.rpc_at(s, m, 0);
    }
    pub fn opening(&mut self) {
        self.rpc(Side::A, Mode::Current);
        self.rpc(Side::B, Mode::Current);
        self.rpc(Side::A, Mode::Current);
    }
    pub fn round(&mut self) {
        self.enqueue(Side::A, b"7");
        self.enqueue(Side::B, b"7");
        self.rpc(Side::A, Mode::Current);
        self.rpc(Side::B, Mode::Current);
        self.rpc(Side::A, Mode::Current);
    }
    pub fn before_miss(&mut self) {
        self.opening();
        self.enqueue(Side::A, b"7");
        self.enqueue(Side::B, b"7");
        self.rpc(Side::A, Mode::Current);
        self.rpc(Side::B, Mode::Current);
    }
    pub fn uploads(&self, s: Side) -> usize {
        self.emissions
            .iter()
            .filter(|(side, p)| *side == s && !p.request.values.is_empty())
            .count()
    }
    pub fn clean(&self) {
        assert!(self.requests.is_empty() && self.replies.is_empty());
        assert!(self.clients.iter().all(|c| c.active().is_none()));
    }
    pub fn safety(&self) {
        for i in 0..2 {
            let out = self.clients[1 - i].received();
            let input = &self.inputs[i];
            assert!(input.starts_with(out), "literal prefix violation: {self:?}");
            let undelivered = &input[out.len()..];
            assert!(
                self.clients[i].stored().ends_with(undelivered),
                "retention violation: {self:?}"
            );
        }
        let received: usize = self.clients.iter().map(|c| c.received().len()).sum();
        assert!(
            self.clients
                .iter()
                .all(|c| c.generation() <= received as u64)
        );
    }
}
