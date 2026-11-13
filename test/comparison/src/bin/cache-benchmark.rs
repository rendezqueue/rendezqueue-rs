//! White-box benchmark of frozen vector-cache and current HTTP-cache source.
//! Both path imports compile real implementations, not rewritten caches.
pub use rendezqueue::*;
#[allow(dead_code)]
#[rustfmt::skip]
#[path = "legacy_server/mod.rs"]
mod legacy_server;
#[allow(dead_code)]
#[rustfmt::skip]
#[path = "../../../../src/server/expiry.rs"]
mod production_cache;
use production_cache::Rooms as OrderedRooms;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

const OPS: usize = 2000;
const TRIALS: usize = 3;
fn id(i: usize) -> Bytes {
    (i as u64).to_le_bytes().to_vec()
}
fn full(i: usize) -> Request {
    Request {
        sid: id(i),
        ack: Some(id(i ^ 1)),
        values: vec![vec![42; 32]],
        nxt: Some(id(i + 1_000_000)),
    }
}
fn output(kind: &str, size: usize, trial: usize, backend: &str, ns: u128, ops: usize) {
    println!(
        "(() (kind \"{kind}\") (rows {size}) (payload_bytes 32) (trial {trial}) (backend \"{backend}\") (operations {ops}) (elapsed_ns {ns}))"
    );
}
// Runtime guard: this binary must still compile during debug/all-target checks.
#[allow(clippy::assertions_on_constants)]
fn main() {
    assert!(!cfg!(debug_assertions), "use a release build for timing");
    println!("(())");
    println!(
        "(() (environment (os {:?}) (arch {:?}) (seed 0) (operations_per_trial {OPS}) (trials {TRIALS}) (note \"frozen vector-cache (SHA256 57bdce6695b1c73abde5602b7501f02b68b9eae9a8039b9b6a16f7a254b4bf4d) and actual production HTTP-cache source imports; both room registries; fixed synthetic monotonic time; warm release sequential clock/full-maintenance hooks; production includes registry/room locks; no HTTP or concurrent dispatch; not the incremental background path; final outcomes and room counts checked, private legacy rows not inspected\")))",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let key = b"room".to_vec();
    let ttl = Duration::from_secs(10);
    for size in [2, 16, 128, 1024, 8192] {
        for trial in 0..TRIALS {
            let now = Instant::now();
            let mut old = legacy_server::expiry::Rooms::new(ttl);
            let mut new = OrderedRooms::new(ttl);
            for i in 0..size {
                let r = full(i);
                let expected = old.serve(key.clone(), &r, now);
                assert!(matches!(&expected, Outcome::Ok(_)));
                assert_eq!(expected, new.serve(key.clone(), &r, now));
            }
            let r = full(0);
            let mut run_old = || {
                let start = Instant::now();
                for _ in 0..OPS {
                    black_box(old.serve(black_box(key.clone()), black_box(&r), now));
                }
                start.elapsed().as_nanos()
            };
            let mut run_new = || {
                let start = Instant::now();
                for _ in 0..OPS {
                    black_box(new.serve(black_box(key.clone()), black_box(&r), now));
                }
                start.elapsed().as_nanos()
            };
            let (a, b) = if trial % 2 == 0 {
                let a = run_old();
                (a, run_new())
            } else {
                let b = run_new();
                (run_old(), b)
            };
            let expected = old.serve(key.clone(), &r, now);
            assert_eq!(expected, new.serve(key.clone(), &r, now));
            assert_eq!(old.len(), 1);
            assert_eq!(new.len(), 1);
            output("clocked-rooms-hot-no-expired", size, trial, "vec", a, OPS);
            output(
                "clocked-rooms-hot-no-expired",
                size,
                trial,
                "ordered",
                b,
                OPS,
            );
            let mut expire_old = || {
                let start = Instant::now();
                old.sweep(now + ttl);
                start.elapsed().as_nanos()
            };
            let mut expire_new = || {
                let start = Instant::now();
                new.sweep(now + ttl);
                start.elapsed().as_nanos()
            };
            let (a, b) = if trial % 2 == 0 {
                let a = expire_old();
                (a, expire_new())
            } else {
                let b = expire_new();
                (expire_old(), b)
            };
            assert_eq!(old.len(), 0);
            assert_eq!(new.len(), 0);
            output("clocked-rooms-expiry-burst", size, trial, "vec", a, size);
            output(
                "clocked-rooms-expiry-burst",
                size,
                trial,
                "ordered",
                b,
                size,
            );
        }
    }
}
