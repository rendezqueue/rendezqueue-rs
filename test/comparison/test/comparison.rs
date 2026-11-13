mod report;

use plyswap_comparison::{OrderedServer, Room, Rooms};
use rendezqueue::{Outcome, Request, Server};
use std::time::{Duration, Instant};

fn check(old: &Server, new: &OrderedServer) {
    assert!(
        old.rows().iter().eq(new.rows()),
        "complete ordered state mismatch"
    );
}
fn step(old: &mut Server, new: &mut OrderedServer, r: &Request) {
    assert_eq!(old.serve(r), new.serve(r), "request {r:?}");
    check(old, new);
}
fn req(sid: &[u8], peer: Option<&[u8]>) -> Request {
    Request {
        sid: sid.to_vec(),
        ack: peer.map(<[u8]>::to_vec),
        ..Request::default()
    }
}
fn full(sid: &[u8], peer: &[u8], nxt: &[u8]) -> Request {
    Request {
        sid: sid.to_vec(),
        ack: Some(peer.to_vec()),
        values: vec![vec![], b"duplicate".to_vec(), b"duplicate".to_vec()],
        nxt: Some(nxt.to_vec()),
    }
}
fn reference(r: &Request) -> Request {
    Request {
        sid: r.sid.clone(),
        nxt: r.nxt.clone(),
        ..Request::default()
    }
}
#[test]
fn fanin_collision_discovery_order_reference_and_empty_ids() {
    let mut old = Server::default();
    let mut new = OrderedServer::default();
    for r in [
        req(b"b", Some(b"a")),
        req(b"c", Some(b"a")),
        req(b"a", None),
        req(b"a", Some(b"b")),
        req(b"c", None),
        req(b"", None),
        req(b"d", Some(b"")),
    ] {
        step(&mut old, &mut new, &r);
    }
    for r in [full(b"b", b"a", b"bn"), full(b"a", b"b", b"an")] {
        step(&mut old, &mut new, &r);
    }
    step(&mut old, &mut new, &reference(&full(b"b", b"a", b"bn")));
    old.forget(b"b");
    new.forget(b"b");
    step(&mut old, &mut new, &reference(&full(b"b", b"a", b"bn")));
    step(&mut old, &mut new, &full(b"b", b"a", b"bn"));
}
struct Rng(u64);
impl Rng {
    fn next(&mut self, n: u64) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n) as usize
    }
}
#[test]
fn seeded_arbitrary_requests_and_faults() {
    let ids = [
        vec![],
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
        vec![0],
        vec![255, 0],
    ];
    let payloads = [
        vec![],
        vec![vec![]],
        vec![vec![0]],
        vec![b"x".to_vec(), b"x".to_vec()],
        vec![vec![255], vec![]],
    ];
    let mut events = 0;
    for seed in 1..=32 {
        let mut rng = Rng(seed * 7919);
        let mut old = Server::default();
        let mut new = OrderedServer::default();
        for _ in 0..4096 {
            let sid = ids[rng.next(ids.len() as u64)].clone();
            match rng.next(32) {
                0 => {
                    old.reset();
                    new.reset();
                }
                1 => {
                    old.forget(&sid);
                    new.forget(&sid);
                }
                2 => {
                    old.evict_opening();
                    new.evict_opening();
                }
                _ => {
                    let ack = match rng.next(7) {
                        6 => None,
                        i => Some(ids[i].clone()),
                    };
                    let nxt = match rng.next(8) {
                        6 | 7 => None,
                        i => Some(ids[i].clone()),
                    };
                    let values = payloads[rng.next(payloads.len() as u64)].clone();
                    step(
                        &mut old,
                        &mut new,
                        &Request {
                            sid,
                            ack,
                            nxt,
                            values,
                        },
                    );
                }
            }
            check(&old, &new);
            events += 1;
        }
    }
    assert_eq!(events, 131072);
}
#[test]
fn comparator_detects_order_corruption() {
    let mut old = Server::default();
    let mut new = OrderedServer::default();
    let a = full(b"a", b"b", b"an");
    for r in [&a, &full(b"b", b"a", b"bn")] {
        step(&mut old, &mut new, r);
    }
    let before = new.clone();
    assert_eq!(new, before);
    // Re-register an unchanged row: only the ordered backend moves it forward.
    new.serve(&a);
    assert!(old.rows().iter().rev().eq(new.rows()));
    assert_ne!(new, before);
    assert!(std::panic::catch_unwind(|| check(&old, &new)).is_err());
}
#[test]
fn expiry_own_refresh_independent_order_and_recovery() {
    let t = Instant::now();
    let ttl = Duration::from_secs(10);
    let mut room = Room::default();
    room.serve(&req(b"a", None), t, ttl);
    room.serve(&req(b"b", None), t, ttl);
    let before: Vec<_> = room.core().rows().cloned().collect();
    for _ in 0..1000 {
        room.serve(&req(b"a", None), t + Duration::from_secs(5), ttl);
    }
    assert_eq!(before, room.core().rows().cloned().collect::<Vec<_>>());
    assert_eq!(room.refresh_len(), 2);
    assert_eq!(room.expire(t + ttl, ttl), 1);
    assert_eq!(room.core().rows().next().unwrap().sid, b"a");
    let a = full(b"a", b"b", b"an");
    let b = full(b"b", b"a", b"bn");
    assert!(matches!(room.serve(&a, t + ttl, ttl), Outcome::Ok(_)));
    room.serve(&b, t + ttl, ttl);
    room.serve(&reference(&a), t + Duration::from_secs(15), ttl);
    assert_eq!(room.refreshed_at(b"b"), Some(t + ttl));
    assert_eq!(
        room.serve(&req(b"a", Some(b"wrong")), t + Duration::from_secs(19), ttl),
        Outcome::Reject
    );
    assert_eq!(room.refreshed_at(b"a"), Some(t + Duration::from_secs(15)));
    assert_eq!(
        room.serve(&reference(&b), t + Duration::from_secs(20), ttl),
        Outcome::Reject
    );
    assert_eq!(room.core().len(), 1);
    assert!(matches!(
        room.serve(&b, t + Duration::from_secs(20), ttl),
        Outcome::Ok(_)
    ));
    assert_eq!(room.refresh_len(), 2);
}
#[test]
fn compaction_empty_rooms_and_huge_ttl() {
    let t = Instant::now();
    let ttl = Duration::from_secs(1);
    let mut room = Room::default();
    for i in 0u32..1024 {
        room.serve(&req(&i.to_le_bytes(), Some(b"peer")), t, ttl);
    }
    room.serve(&req(b"survivor", None), t + Duration::from_millis(500), ttl);
    assert_eq!(room.expire(t + ttl, ttl), 1024);
    assert_eq!(room.core().len(), 1);
    assert!(room.core().capacity() <= 32);
    assert!(room.refresh_capacity() <= 32);
    assert_eq!(room.expire(t + ttl, Duration::MAX), 0);
    let mut rooms = Rooms::new(ttl);
    for i in 0u32..256 {
        rooms.serve(i.to_le_bytes().to_vec(), &req(b"a", Some(b"a")), t);
    }
    assert!(rooms.is_empty());
    rooms.serve(vec![], &req(b"", None), t);
    assert_eq!(rooms.len(), 1);
    rooms.sweep(t + ttl);
    assert!(rooms.is_empty());
}
