//! Compare actual old cache source with the actual production HTTP cache.
pub use rendezqueue::*;
#[allow(dead_code)]
#[rustfmt::skip]
#[path = "../src/bin/legacy_server/mod.rs"]
mod legacy_server;
#[allow(dead_code)]
#[rustfmt::skip]
#[path = "../../../src/server/expiry.rs"]
mod production_cache;
use std::time::{Duration, Instant};

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
fn actual_cache_sources_match_outcomes_refreshes_and_room_counts() {
    let ids = [
        vec![],
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
        vec![0],
        vec![255, 0],
    ];
    let keys = [vec![], b"one".to_vec(), b"two".to_vec(), vec![255, 0]];
    let payloads = [
        vec![],
        vec![vec![]],
        vec![vec![0]],
        vec![b"x".to_vec(), b"x".to_vec()],
        vec![vec![255], vec![]],
    ];
    let ttl = Duration::from_millis(10);
    let start = Instant::now();
    let mut events = 0;
    for seed in 1..=32 {
        let mut rng = Rng(seed * 7919);
        let mut old = legacy_server::expiry::Rooms::new(ttl);
        let mut new = production_cache::Rooms::new(ttl);
        let mut ms = 0;
        for _ in 0..1024 {
            ms += rng.next(4) as u64;
            let now = start + Duration::from_millis(ms);
            if rng.next(8) == 0 {
                old.sweep(now);
                new.sweep(now);
            } else {
                let sid = ids[rng.next(6)].clone();
                let key = keys[rng.next(4)].clone();
                let ack = match rng.next(7) {
                    6 => None,
                    i => Some(ids[i].clone()),
                };
                let nxt = match rng.next(8) {
                    6 | 7 => None,
                    i => Some(ids[i].clone()),
                };
                let request = Request {
                    sid,
                    ack,
                    nxt,
                    values: payloads[rng.next(5)].clone(),
                };
                assert_eq!(
                    old.serve(key.clone(), &request, now),
                    new.serve(key, &request, now),
                    "seed {seed} event {events} request {request:?}"
                );
            }
            assert_eq!(old.len(), new.len(), "seed {seed} event {events} rooms");
            for key in &keys {
                for sid in &ids {
                    assert_eq!(
                        old.refreshed_at(key, sid),
                        new.refreshed_at(key, sid),
                        "seed {seed} event {events} refresh {key:?}/{sid:?}"
                    );
                }
            }
            events += 1;
        }
    }
    assert_eq!(events, 32768);
}
