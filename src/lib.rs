//! Volatile PlySwap core; synchronous HTTP adapters add cache expiration.
//! No automatic retry, authentication or durable application effects.
mod client;
pub mod http;
mod keyed;
pub mod scenario;
mod server;
pub mod wire;

pub use client::{Client, Exhausted, Mode, Packet};
pub use keyed::{KeyedOutcome, KeyedReply};
pub use server::{Record, Server};

/// Opaque IDs and payload elements; an empty element is not an empty batch.
pub type Bytes = Vec<u8>;

/// Actual compact wire fields. Omitted and explicit empty values normalize alike.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Request {
    pub self_xid: Bytes,
    pub peer_xid: Bytes,
    pub next_xid: Bytes,
    pub values: Vec<Bytes>,
}

impl Request {
    pub fn is_reference(&self) -> bool {
        self.peer_xid.is_empty() && !self.next_xid.is_empty() && self.values.is_empty()
    }
}

/// NEXT signals completion, including completed EMPTY. PEER is opening-only.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Reply {
    pub self_xid: Bytes,
    pub peer_xid: Bytes,
    pub next_xid: Bytes,
    pub values: Vec<Bytes>,
}

impl Reply {
    pub fn is_valid(&self) -> bool {
        if self.self_xid.is_empty() {
            return false;
        }
        if !self.next_xid.is_empty() {
            self.peer_xid.is_empty()
        } else {
            self.values.is_empty()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Reject,
    Ok(Reply),
}

mod ordered;
mod ordered_policy;
pub use ordered::OrderedServer;
