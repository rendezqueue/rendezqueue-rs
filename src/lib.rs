//! Volatile PlySwap core; synchronous HTTP adapters add cache expiration.
//! No automatic retry, authentication or durable application effects.
mod client;
pub mod http;
pub mod scenario;
mod server;
pub mod wire;

pub use client::{Client, Exhausted, Mode, Packet};
pub use server::{Record, Server};

/// Opaque IDs and payload elements; an empty element is not an empty batch.
pub type Bytes = Vec<u8>;

/// Actual compact wire fields. Omitted and explicit empty values normalize alike.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Request {
    pub sid: Bytes,
    pub ack: Option<Bytes>,
    pub values: Vec<Bytes>,
    pub nxt: Option<Bytes>,
}

impl Request {
    pub fn is_reference(&self) -> bool {
        self.ack.is_none() && self.nxt.is_some() && self.values.is_empty()
    }
}

/// NXT signals completion, including completed EMPTY. ACK is opening-only.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Reply {
    pub sid: Bytes,
    pub ack: Option<Bytes>,
    pub values: Vec<Bytes>,
    pub nxt: Option<Bytes>,
}

impl Reply {
    pub fn is_valid(&self) -> bool {
        if self.nxt.is_some() {
            self.ack.is_none()
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
