//! Per-room serialized expiry/core, with a briefly locked rotating registry.
use crate::{Bytes, KeyedOutcome, KeyedReply, OrderedServer as Server, Outcome, Request};
use hashlink::LinkedHashMap;
use std::{
    collections::hash_map::RandomState,
    io,
    sync::{Arc, Mutex, TryLockError},
    time::{Duration, Instant},
};

fn oversized(capacity: usize, len: usize) -> bool {
    capacity > len.saturating_mul(4).max(32)
}

#[derive(Default)]
struct Room {
    core: Server,
    refreshed: LinkedHashMap<Bytes, Instant, RandomState>,
}
impl Room {
    fn expire(&mut self, now: Instant, ttl: Duration, budget: usize) {
        let mut removed = 0;
        while removed < budget {
            let Some((_, touched)) = self.refreshed.front() else {
                break;
            };
            if now.saturating_duration_since(*touched) < ttl {
                break;
            }
            let (self_xid, _) = self.refreshed.pop_front().expect("inspected expiry head");
            self.core.forget(&self_xid);
            removed += 1;
        }
        // Physical compaction can be linear, but never holds the registry lock.
        if removed != 0 {
            self.core.compact();
            if oversized(self.refreshed.capacity(), self.refreshed.len()) {
                self.refreshed.shrink_to_fit();
            }
        }
    }
    fn serve(&mut self, request: &Request, now: Instant, ttl: Duration) -> Outcome {
        if request.self_xid.is_empty() {
            return Outcome::Reject;
        }
        // Access expiry is exact; background work alone may be partial.
        self.expire(now, ttl, usize::MAX);
        let outcome = self.core.serve(request);
        if matches!(&outcome, Outcome::Ok(_)) {
            debug_assert!(self.core.contains_xid(&request.self_xid));
            self.refreshed.insert(request.self_xid.clone(), now);
        }
        outcome
    }
}

/// None is an irreversible tombstone. Old handles must retry, never resurrect it.
struct Slot {
    live: Mutex<Option<Room>>,
}
impl Slot {
    fn new() -> Self {
        Self {
            live: Mutex::new(Some(Room::default())),
        }
    }
}
type Registry = LinkedHashMap<Bytes, Arc<Slot>, RandomState>;
fn poisoned() -> io::Error {
    io::Error::other("PlySwap cache lock poisoned")
}

pub(crate) struct Rooms {
    ttl: Duration,
    registry: Mutex<Registry>,
}
impl Rooms {
    pub(crate) fn new(ttl: Duration) -> Self {
        assert!(!ttl.is_zero());
        Self {
            ttl,
            registry: Mutex::new(Registry::with_hasher(RandomState::new())),
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.registry
            .lock()
            .expect("PlySwap registry poisoned")
            .len()
    }
    fn slot(&self, room: &[u8]) -> io::Result<Arc<Slot>> {
        let mut registry = self.registry.lock().map_err(|_| poisoned())?;
        if let Some(slot) = registry.get(room) {
            return Ok(Arc::clone(slot));
        }
        let slot = Arc::new(Slot::new());
        registry.insert(room.to_vec(), Arc::clone(&slot));
        Ok(slot)
    }
    // Caller observed irreversible retirement, and has released the room lock.
    fn unlink(&self, room: &[u8], slot: &Arc<Slot>) -> io::Result<()> {
        let mut registry = self.registry.lock().map_err(|_| poisoned())?;
        if registry
            .get(room)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            registry.remove(room);
        }
        // Empty reset releases even the linked map's free-node pool. Nonempty
        // registry compaction is explicit maintenance, not a request-wide scan.
        let empty_pool = registry
            .is_empty()
            .then(|| std::mem::replace(&mut *registry, Registry::with_hasher(RandomState::new())));
        drop(registry);
        // A historical free-node pool may be large. Destroy it without making
        // requests for other rooms wait for its deallocation.
        drop(empty_pool);
        Ok(())
    }
    fn serve_clock(
        &self,
        room: Bytes,
        request: &Request,
        clock: impl FnOnce() -> Instant,
    ) -> io::Result<Outcome> {
        if request.self_xid.is_empty() {
            return Ok(Outcome::Reject);
        }
        let slot = self.slot(&room)?;
        self.serve_slot_clock(room, request, clock, slot)
    }
    fn serve_slot_clock(
        &self,
        room: Bytes,
        request: &Request,
        clock: impl FnOnce() -> Instant,
        mut slot: Arc<Slot>,
    ) -> io::Result<Outcome> {
        if request.self_xid.is_empty() {
            return Ok(Outcome::Reject);
        }
        let mut clock = Some(clock);
        loop {
            let mut live = slot.live.lock().map_err(|_| poisoned())?;
            if let Some(live_room) = live.as_mut() {
                // Invoke only after acquiring a live room. Concurrent arrivals
                // therefore cannot reverse its refresh timestamps.
                let now = clock.take().expect("clock used once")();
                let outcome = live_room.serve(request, now, self.ttl);
                let retired = live_room.core.is_empty();
                if retired {
                    *live = None;
                }
                drop(live);
                if retired {
                    self.unlink(&room, &slot)?;
                }
                return Ok(outcome);
            }
            drop(live);
            self.unlink(&room, &slot)?;
            slot = self.slot(&room)?;
        }
    }
    pub(crate) fn serve_now(&self, room: Bytes, request: &Request) -> io::Result<KeyedOutcome> {
        Ok(
            match self.serve_clock(room.clone(), request, Instant::now)? {
                Outcome::Reject => KeyedOutcome::Reject,
                Outcome::Ok(reply) => KeyedOutcome::Ok(KeyedReply { room, reply }),
            },
        )
    }
    /// Synthetic-clock entry for sequential fixtures/benchmarks. Production uses
    /// serve_now; callers here must provide nondecreasing time per room.
    #[allow(dead_code)]
    pub(crate) fn serve(&mut self, room: Bytes, request: &Request, now: Instant) -> Outcome {
        self.serve_clock(room, request, || now)
            .expect("cache fixture poisoned")
    }
    fn maintain_clock(
        &self,
        mut clock: impl FnMut() -> Instant,
        room_budget: usize,
        xid_budget: usize,
    ) -> io::Result<usize> {
        let visits = self
            .registry
            .lock()
            .map_err(|_| poisoned())?
            .len()
            .min(room_budget);
        let mut inspected = 0;
        for _ in 0..visits {
            let (room, slot) = {
                let mut registry = self.registry.lock().map_err(|_| poisoned())?;
                let Some((room, slot)) = registry.pop_front() else {
                    break;
                };
                let selected = (room.clone(), Arc::clone(&slot));
                // Rotation is atomic relative to lookup; the live slot is never
                // absent from a registry another request can observe.
                registry.insert(room, slot);
                selected
            };
            let mut live = match slot.live.try_lock() {
                Ok(live) => live,
                Err(TryLockError::WouldBlock) => continue,
                Err(TryLockError::Poisoned(_)) => return Err(poisoned()),
            };
            inspected += 1;
            if let Some(room) = live.as_mut() {
                room.expire(clock(), self.ttl, xid_budget);
                if room.core.is_empty() {
                    *live = None;
                }
            }
            let retired = live.is_none();
            drop(live);
            if retired {
                self.unlink(&room, &slot)?;
            }
        }
        Ok(inspected)
    }
    /// Bounded entry counts, not a wall-clock deadline. Busy rooms are skipped.
    pub(crate) fn maintain(&self, room_budget: usize, xid_budget: usize) -> io::Result<usize> {
        self.maintain_clock(Instant::now, room_budget, xid_budget)
    }
    /// Explicit sequential full maintenance for fixtures/cache-source benchmarks.
    /// This is deliberately not the production reaper's path.
    #[allow(dead_code)]
    pub(crate) fn sweep(&mut self, now: Instant) {
        self.maintain_clock(|| now, usize::MAX, usize::MAX)
            .expect("cache fixture poisoned");
        let mut registry = self.registry.lock().expect("cache fixture poisoned");
        if oversized(registry.capacity(), registry.len()) {
            registry.shrink_to_fit();
        }
    }
    #[cfg(test)]
    pub(crate) fn refreshed_at(&self, room: &[u8], self_xid: &[u8]) -> Option<Instant> {
        let slot = self.registry.lock().unwrap().get(room).cloned()?;
        let live = slot.live.lock().unwrap();
        live.as_ref()?.refreshed.get(self_xid).copied()
    }
}

#[cfg(test)]
#[path = "expiry/concurrency.rs"]
mod concurrency;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Reply;
    use sxpb::{Mesg, Value};

    fn inspect<T>(rooms: &Rooms, room: &[u8], f: impl FnOnce(&Room) -> T) -> T {
        let slot = rooms.registry.lock().unwrap().get(room).cloned().unwrap();
        let live = slot.live.lock().unwrap();
        f(live.as_ref().expect("live fixture room"))
    }

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
                let actual: Vec<_> = if rooms.registry.lock().unwrap().contains_key(&room) {
                    inspect(&rooms, &room, |r| {
                        r.core.rows().map(|r| r.self_xid.clone()).collect()
                    })
                } else {
                    vec![]
                };
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
            let slots: Vec<_> = rooms.registry.lock().unwrap().values().cloned().collect();
            for slot in slots {
                let live = slot.live.lock().unwrap();
                let room = live.as_ref().unwrap();
                assert_eq!(
                    room.refreshed.len(),
                    room.core.len(),
                    "no orphan/missing metadata"
                );
                for row in room.core.rows() {
                    assert!(room.refreshed.contains_key(&row.self_xid));
                }
            }
        }
    }

    #[test]
    fn production_cache_keeps_one_timer_per_sid_and_independent_orders() {
        let start = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut rooms = Rooms::new(ttl);
        let room = b"ordered".to_vec();
        for self_xid in [b"a", b"b"] {
            rooms.serve(
                room.clone(),
                &Request {
                    self_xid: self_xid.to_vec(),
                    ..Request::default()
                },
                start,
            );
        }
        let before: Vec<_> = inspect(&rooms, &room, |r| r.core.rows().cloned().collect());
        for _ in 0..1000 {
            rooms.serve(
                room.clone(),
                &Request {
                    self_xid: b"a".to_vec(),
                    ..Request::default()
                },
                start + Duration::from_secs(5),
            );
        }
        inspect(&rooms, &room, |room| {
            let _: &crate::OrderedServer = &room.core;
            assert_eq!(room.core.rows().cloned().collect::<Vec<_>>(), before);
            assert_eq!(room.refreshed.len(), 2);
            assert_eq!(
                room.refreshed.keys().cloned().collect::<Vec<_>>(),
                [b"b".to_vec(), b"a".to_vec()]
            );
        });
        rooms.sweep(start + ttl);
        inspect(&rooms, &room, |room| {
            assert_eq!(
                room.core
                    .rows()
                    .map(|r| r.self_xid.clone())
                    .collect::<Vec<_>>(),
                [b"a".to_vec()]
            );
            assert_eq!(room.refreshed.len(), 1);
        });
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
    fn empty_self_rejection_does_not_create_expire_or_refresh_a_room() {
        let rooms = Rooms::new(Duration::from_secs(1));
        let now = Instant::now();
        let invalid = Request::default();
        assert_eq!(
            rooms
                .serve_clock(b"absent".to_vec(), &invalid, || panic!("no clock needed"))
                .unwrap(),
            Outcome::Reject
        );
        assert_eq!(rooms.len(), 0);
        let opening = Request {
            self_xid: vec![0],
            ..Request::default()
        };
        rooms
            .serve_clock(b"live".to_vec(), &opening, || now)
            .unwrap();
        assert_eq!(
            rooms
                .serve_clock(b"live".to_vec(), &invalid, || now + Duration::from_secs(2))
                .unwrap(),
            Outcome::Reject
        );
        assert_eq!(rooms.refreshed_at(b"live", &[0]), Some(now));
        inspect(&rooms, b"live", |room| assert_eq!(room.core.len(), 1));
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
        inspect(&rooms, &[], |room| {
            assert_eq!(
                room.core
                    .rows()
                    .map(|r| r.self_xid.clone())
                    .collect::<Vec<_>>(),
                [vec![1], vec![255, 0], vec![0]]
            );
        });
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
            rooms.serve(
                self_xid,
                &Request {
                    self_xid: b"a".to_vec(),
                    ..Request::default()
                },
                now,
            );
        }
        let survivor = Request {
            self_xid: 0_u32.to_le_bytes().to_vec(),
            ..Request::default()
        };
        rooms.serve(b"busy".to_vec(), &survivor, now + Duration::from_secs(19));
        rooms.sweep(now + ttl);
        assert_eq!(rooms.len(), 1);
        inspect(&rooms, b"busy", |room| {
            assert_eq!(room.core.len(), 1);
            assert_eq!(room.refreshed.len(), 1);
            assert!(room.core.capacity() <= 32);
            assert!(room.refreshed.capacity() <= 32);
        });
        assert!(rooms.registry.lock().unwrap().capacity() <= 32);
        rooms.sweep(now + Duration::from_secs(39));
        assert_eq!(rooms.len(), 0);
        assert_eq!(rooms.registry.lock().unwrap().capacity(), 0);
    }
}
