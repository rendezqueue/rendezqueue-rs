//! Cache lifecycle: stable expiry/filter followed by the pure protocol.
use super::Server;
use crate::{Bytes, Outcome, Request};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

fn oversized(capacity: usize, len: usize) -> bool {
    capacity > len.saturating_mul(4).max(32)
}

#[derive(Default)]
struct Room {
    core: Server,
    refreshed: HashMap<Bytes, Instant>,
}

impl Room {
    fn expire(&mut self, now: Instant, ttl: Duration) {
        self.refreshed
            .retain(|_, touched| now.saturating_duration_since(*touched) < ttl);
        // One stable batch filter, not one O(n) forget for every expired XID.
        // Missing metadata cannot create an immortal row.
        self.core
            .rows
            .retain(|row| self.refreshed.contains_key(&row.self_xid));
        if oversized(self.core.rows.capacity(), self.core.rows.len()) {
            self.core.rows.shrink_to_fit();
        }
        if oversized(self.refreshed.capacity(), self.refreshed.len()) {
            self.refreshed.shrink_to_fit();
        }
    }
}

pub(crate) struct Rooms {
    ttl: Duration,
    rooms: HashMap<Bytes, Room>,
}

impl Rooms {
    pub(crate) fn new(ttl: Duration) -> Self {
        assert!(!ttl.is_zero());
        Self {
            ttl,
            rooms: HashMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rooms.len()
    }

    #[cfg(test)]
    pub(crate) fn refreshed_at(&self, room: &[u8], self_xid: &[u8]) -> Option<Instant> {
        self.rooms.get(room)?.refreshed.get(self_xid).copied()
    }

    pub(crate) fn serve(&mut self, room: Bytes, request: &Request, now: Instant) -> Outcome {
        if request.self_xid.is_empty() {
            return Outcome::Reject;
        }
        let state = self.rooms.entry(room.clone()).or_default();
        state.expire(now, self.ttl);
        let outcome = state.core.serve(request);
        if matches!(&outcome, Outcome::Ok(_)) {
            // Reading peers or declaring NEXT never refreshes another identity.
            debug_assert!(state.core.rows.iter().any(|r| r.self_xid == request.self_xid));
            state.refreshed.insert(request.self_xid.clone(), now);
        }
        if state.core.rows().is_empty() {
            self.rooms.remove(&room);
        }
        outcome
    }

    pub(crate) fn sweep(&mut self, now: Instant) {
        self.rooms.retain(|_, room| {
            room.expire(now, self.ttl);
            !room.core.rows().is_empty()
        });
        if self.rooms.is_empty() || oversized(self.rooms.capacity(), self.rooms.len()) {
            self.rooms.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Reply;
    use sxpb::{Mesg, Value};

    fn mesg<'a>(v: &'a Value, allowed: &[&str]) -> &'a Mesg {
        let m = v.as_mesg().expect("message");
        for field in m {
            assert!(
                allowed.contains(&field.name.as_str()),
                "unknown field {}",
                field.name
            );
        }
        m
    }

    fn text(v: &Value) -> Bytes {
        v.as_str().expect("byte string").as_bytes().to_vec()
    }

    fn number(v: &Value) -> u64 {
        u64::try_from(
            v.as_number()
                .expect("integer")
                .to_u128()
                .expect("unsigned integer"),
        )
        .unwrap()
    }

    fn packet(v: &Value) -> Request {
        let m = mesg(v, &["self", "peer", "values", "next"]);
        Request {
            self_xid: text(m.get("self").expect("self")),
            peer_xid: m.get("peer").map(text).unwrap_or_default(),
            next_xid: m.get("next").map(text).unwrap_or_default(),
            values: m
                .get("values")
                .map(|v| {
                    v.as_array()
                        .expect("values list")
                        .iter()
                        .map(text)
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    fn run(source: &str) {
        let doc = sxpb::from_str(source).unwrap();
        let doc = mesg(&doc, &["ttl_ms", "steps"]);
        let ttl = Duration::from_millis(number(doc.get("ttl_ms").unwrap()));
        let mut rooms = Rooms::new(ttl);
        let start = Instant::now();
        let mut last_ms = 0;
        let steps = doc.get("steps").unwrap().as_array().unwrap();
        assert!(!steps.is_empty());
        for (index, step) in steps.iter().enumerate() {
            let m = mesg(
                step,
                &[
                    "at_ms", "room", "req", "res", "reject", "sweep", "rows", "rooms",
                ],
            );
            let ms = number(m.get("at_ms").unwrap());
            assert!(ms >= last_ms, "clock reversal");
            last_ms = ms;
            let now = start + Duration::from_millis(ms);
            if let Some(req) = m.get("req") {
                assert!(m.get("sweep").is_none());
                let room = text(m.get("room").unwrap());
                assert_ne!(m.get("res").is_some(), m.get("reject").is_some());
                let expected = if let Some(res) = m.get("res") {
                    let p = packet(res);
                    Outcome::Ok(Reply {
                        self_xid: p.self_xid,
                        peer_xid: p.peer_xid,
                        values: p.values,
                        next_xid: p.next_xid,
                    })
                } else {
                    assert_eq!(m.get("reject").unwrap().as_bool(), Some(true));
                    Outcome::Reject
                };
                let actual = rooms.serve(room.clone(), &packet(req), now);
                assert_eq!(actual, expected, "step {} request/reply", index + 1);
                let actual: Vec<_> = rooms
                    .rooms
                    .get(&room)
                    .map(|r| r.core.rows().iter().map(|r| r.self_xid.clone()).collect())
                    .unwrap_or_default();
                let expected: Vec<_> = m
                    .get("rows")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(text)
                    .collect();
                assert_eq!(actual, expected, "step {} ordered rows", index + 1);
            } else {
                assert_eq!(m.get("sweep").unwrap().as_bool(), Some(true));
                assert!(
                    m.get("res").is_none()
                        && m.get("reject").is_none()
                        && m.get("rows").is_none()
                        && m.get("room").is_none()
                );
                rooms.sweep(now);
            }
            assert_eq!(
                rooms.len() as u64,
                number(m.get("rooms").unwrap()),
                "step {} physical rooms",
                index + 1
            );
            for room in rooms.rooms.values() {
                assert_eq!(
                    room.refreshed.len(),
                    room.core.rows().len(),
                    "no orphan/missing metadata"
                );
                for row in room.core.rows() {
                    assert!(room.refreshed.contains_key(&row.self_xid));
                }
            }
        }
    }

    #[test]
    fn expiry_boundaries_do_not_refresh_peers_and_full_snapshots_recover() {
        run(include_str!("../../test/cache/expiry-recovery.sxpb"));
    }

    #[test]
    fn opening_retry_refreshes_only_own_row_in_place_and_nonempty_xid_expires() {
        run(include_str!("../../test/cache/opening-refresh.sxpb"));
    }

    #[test]
    fn retained_current_and_previous_references_miss_then_retry_exact_full_snapshots() {
        use crate::{Client, Mode};
        let start = Instant::now();
        let mut rooms = Rooms::new(Duration::from_millis(20));
        let mut a = Client::new(b"a".to_vec());
        let mut b = Client::new(b"b".to_vec());
        fn step(
            rooms: &mut Rooms,
            client: &mut Client,
            mode: Mode,
            now: Instant,
        ) -> (Request, Outcome) {
            let packet = client.send(mode).unwrap().unwrap();
            let out = rooms.serve(b"retained".to_vec(), &packet.request, now);
            client.accept(&packet, &out).unwrap();
            (packet.request, out)
        }
        step(&mut rooms, &mut a, Mode::Current, start);
        step(
            &mut rooms,
            &mut b,
            Mode::Current,
            start + Duration::from_millis(1),
        );
        step(
            &mut rooms,
            &mut a,
            Mode::Current,
            start + Duration::from_millis(2),
        );
        a.enqueue(vec![]);
        a.enqueue(vec![255, 0]);
        b.enqueue(b"reply".to_vec());
        let (full, _) = step(
            &mut rooms,
            &mut a,
            Mode::Current,
            start + Duration::from_millis(3),
        );
        assert!(!full.is_reference());
        let (reference, out) = step(
            &mut rooms,
            &mut a,
            Mode::Current,
            start + Duration::from_millis(23),
        );
        assert!(reference.is_reference());
        assert_eq!(out, Outcome::Reject);
        let (retry, _) = step(
            &mut rooms,
            &mut a,
            Mode::Current,
            start + Duration::from_millis(24),
        );
        assert_eq!(retry, full, "FULL restores the same frozen PEER/body/NEXT");
        step(
            &mut rooms,
            &mut b,
            Mode::Current,
            start + Duration::from_millis(25),
        );
        step(
            &mut rooms,
            &mut a,
            Mode::Current,
            start + Duration::from_millis(26),
        );
        assert_eq!(a.received(), [b"reply".to_vec()]);
        assert_eq!(b.received(), [vec![], vec![255, 0]]);
        let previous = a.previous().unwrap().clone();
        let (reference, _) = step(
            &mut rooms,
            &mut a,
            Mode::Previous,
            start + Duration::from_millis(27),
        );
        assert!(reference.is_reference());
        let (reference, out) = step(
            &mut rooms,
            &mut a,
            Mode::Previous,
            start + Duration::from_millis(47),
        );
        assert!(reference.is_reference());
        assert_eq!(out, Outcome::Reject);
        let (retry, _) = step(
            &mut rooms,
            &mut a,
            Mode::Previous,
            start + Duration::from_millis(48),
        );
        assert_eq!(retry, previous);
        assert_eq!(
            a.received(),
            [b"reply".to_vec()],
            "recovery does not redeliver"
        );
    }

    #[test]
    fn rejected_unknown_reference_does_not_accumulate_empty_rooms() {
        let mut rooms = Rooms::new(Duration::from_secs(20));
        let now = Instant::now();
        for self_xid in 0..1000_u32 {
            let request = Request {
                self_xid: self_xid.to_le_bytes().to_vec(),
                next_xid: b"missing-next".to_vec(),
                ..Request::default()
            };
            assert_eq!(
                rooms.serve(self_xid.to_be_bytes().to_vec(), &request, now),
                Outcome::Reject
            );
        }
        assert_eq!(rooms.len(), 0);
    }

    #[test]
    fn large_duration_does_not_overflow_deadline_and_sweep_preserves_survivor_order() {
        let now = Instant::now();
        let mut rooms = Rooms::new(Duration::MAX);
        for self_xid in [vec![0], vec![255, 0], vec![1]] {
            assert!(matches!(
                rooms.serve(
                    vec![],
                    &Request {
                        self_xid,
                        ..Request::default()
                    },
                    now
                ),
                Outcome::Ok(_)
            ));
        }
        rooms.sweep(now + Duration::from_secs(1));
        let room = rooms.rooms.get(&vec![]).unwrap();
        assert_eq!(
            room.core
                .rows()
                .iter()
                .map(|r| r.self_xid.clone())
                .collect::<Vec<_>>(),
            [vec![1], vec![255, 0], vec![0]]
        );
    }

    #[test]
    fn burst_then_one_survivor_reclaims_rows_metadata_and_registry_capacity() {
        let now = Instant::now();
        let ttl = Duration::from_secs(20);
        let mut rooms = Rooms::new(ttl);
        for n in 0..256_u32 {
            let self_xid = n.to_le_bytes().to_vec();
            let input = Request {
                self_xid: self_xid.clone(),
                values: vec![vec![7; 1024]],
                peer_xid: b"peer".to_vec(),
                next_xid: b"next".to_vec(),
            };
            rooms.serve(b"busy".to_vec(), &input, now);
            rooms.serve(self_xid, &Request { self_xid: b"a".to_vec(), ..Request::default() }, now);
        }
        let survivor = Request {
            self_xid: 0_u32.to_le_bytes().to_vec(),
            ..Request::default()
        };
        rooms.serve(b"busy".to_vec(), &survivor, now + Duration::from_secs(19));
        rooms.sweep(now + ttl);
        assert_eq!(rooms.len(), 1);
        let room = rooms.rooms.get(b"busy".as_slice()).unwrap();
        assert_eq!(room.core.rows().len(), 1);
        assert_eq!(room.refreshed.len(), 1);
        assert!(room.core.rows.capacity() <= 32);
        assert!(room.refreshed.capacity() <= 32);
        assert!(rooms.rooms.capacity() <= 32);
        rooms.sweep(now + Duration::from_secs(39));
        assert_eq!(rooms.len(), 0);
        assert_eq!(rooms.rooms.capacity(), 0);
    }
}
