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
        // One stable batch filter, not one O(n) forget for every expired SID.
        // Missing metadata cannot create an immortal row.
        self.core
            .rows
            .retain(|row| self.refreshed.contains_key(&row.sid));
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
    pub(crate) fn refreshed_at(&self, key: &[u8], sid: &[u8]) -> Option<Instant> {
        self.rooms.get(key)?.refreshed.get(sid).copied()
    }

    pub(crate) fn serve(&mut self, key: Bytes, request: &Request, now: Instant) -> Outcome {
        let room = self.rooms.entry(key.clone()).or_default();
        room.expire(now, self.ttl);
        let outcome = room.core.serve(request);
        if matches!(&outcome, Outcome::Ok(_)) {
            // Reading peers or declaring NXT never refreshes another identity.
            debug_assert!(room.core.rows.iter().any(|r| r.sid == request.sid));
            room.refreshed.insert(request.sid.clone(), now);
        }
        if room.core.rows().is_empty() {
            self.rooms.remove(&key);
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
        let m = mesg(v, &["sid", "ack", "values", "nxt"]);
        Request {
            sid: text(m.get("sid").expect("sid")),
            ack: m.get("ack").map(text),
            nxt: m.get("nxt").map(text),
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
                let key = text(m.get("room").unwrap());
                assert_ne!(m.get("res").is_some(), m.get("reject").is_some());
                let expected = if let Some(res) = m.get("res") {
                    let p = packet(res);
                    Outcome::Ok(Reply {
                        sid: p.sid,
                        ack: p.ack,
                        values: p.values,
                        nxt: p.nxt,
                    })
                } else {
                    assert_eq!(m.get("reject").unwrap().as_bool(), Some(true));
                    Outcome::Reject
                };
                let actual = rooms.serve(key.clone(), &packet(req), now);
                assert_eq!(actual, expected, "step {} request/reply", index + 1);
                let actual: Vec<_> = rooms
                    .rooms
                    .get(&key)
                    .map(|r| r.core.rows().iter().map(|r| r.sid.clone()).collect())
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
                    assert!(room.refreshed.contains_key(&row.sid));
                }
            }
        }
    }

    #[test]
    fn expiry_boundaries_do_not_refresh_peers_and_full_snapshots_recover() {
        run(include_str!("../../test/cache/expiry-recovery.sxpb"));
    }

    #[test]
    fn opening_retry_refreshes_only_own_row_in_place_and_empty_sid_expires() {
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
        assert_eq!(retry, full, "FULL restores the same frozen ACK/body/NXT");
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
        for sid in 0..1000_u32 {
            let request = Request {
                sid: sid.to_le_bytes().to_vec(),
                nxt: Some(vec![]),
                ..Request::default()
            };
            assert_eq!(
                rooms.serve(sid.to_be_bytes().to_vec(), &request, now),
                Outcome::Reject
            );
        }
        assert_eq!(rooms.len(), 0);
    }

    #[test]
    fn large_duration_does_not_overflow_deadline_and_sweep_preserves_survivor_order() {
        let now = Instant::now();
        let mut rooms = Rooms::new(Duration::MAX);
        for sid in [vec![], vec![255, 0], vec![1]] {
            assert!(matches!(
                rooms.serve(
                    vec![],
                    &Request {
                        sid,
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
                .map(|r| r.sid.clone())
                .collect::<Vec<_>>(),
            [vec![1], vec![255, 0], vec![]]
        );
    }

    #[test]
    fn burst_then_one_survivor_reclaims_rows_metadata_and_registry_capacity() {
        let now = Instant::now();
        let ttl = Duration::from_secs(20);
        let mut rooms = Rooms::new(ttl);
        for n in 0..256_u32 {
            let sid = n.to_le_bytes().to_vec();
            let input = Request {
                sid: sid.clone(),
                values: vec![vec![7; 1024]],
                ack: Some(b"peer".to_vec()),
                nxt: Some(b"next".to_vec()),
            };
            rooms.serve(b"busy".to_vec(), &input, now);
            rooms.serve(sid, &Request::default(), now);
        }
        let survivor = Request {
            sid: 0_u32.to_le_bytes().to_vec(),
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
