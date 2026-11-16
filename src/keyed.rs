//! Keyed service results, outside the single-room protocol state machine.
use crate::{Bytes, Reply};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedReply {
    pub room: Bytes,
    pub reply: Reply,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyedOutcome {
    Reject,
    Ok(KeyedReply),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Request, server::expiry::Rooms};
    use std::time::Duration;

    #[test]
    fn keyed_service_returns_the_key_before_serialization() {
        let rooms = Rooms::new(Duration::from_secs(20));
        let request = Request {
            self_xid: b"a".to_vec(),
            ..Request::default()
        };
        for room in [b"one", b"two"] {
            assert_eq!(
                rooms.serve_now(room.to_vec(), &request).unwrap(),
                KeyedOutcome::Ok(KeyedReply {
                    room: room.to_vec(),
                    reply: Reply {
                        self_xid: b"a".to_vec(),
                        ..Reply::default()
                    }
                })
            );
        }
        assert_eq!(rooms.len(), 2);
    }
}
