use plyswap_comparison::{OrderedServer, Room};
use rendezqueue::{Outcome, Request, Server};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

const OPS: usize = 2000;
const TRIALS: usize = 3;
fn id(i: usize) -> Vec<u8> {
    (i as u64).to_le_bytes().to_vec()
}
fn full(i: usize, bytes: usize) -> Request {
    Request {
        self_xid: id(i),
        peer_xid: id(i ^ 1),
        values: vec![vec![42; bytes]],
        next_xid: id(i + 1_000_000),
    }
}
fn same(old: &Server, new: &OrderedServer) {
    assert!(old.rows().iter().eq(new.rows()));
}
fn output(
    kind: &str,
    size: usize,
    payload: usize,
    trial: usize,
    backend: &str,
    nanos: u128,
    ops: usize,
) {
    println!(
        "(() (kind \"{kind}\") (rows {size}) (payload_bytes {payload}) (trial {trial}) (backend \"{backend}\") (operations {ops}) (elapsed_ns {nanos}))"
    );
}
fn main() {
    println!("(())");
    println!(
        "(() (environment (os {:?}) (arch {:?}) (seed 0) (operations_per_trial {OPS}) (trials {TRIALS}) (note \"release; randomized std RandomState; warm cache; separate per-backend runs; no network; times include output construction/destruction\")))",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    for size in [2, 16, 128, 1024, 8192] {
        for bytes in [32, 4096] {
            let mut base_old = Server::default();
            let mut base_new = OrderedServer::default();
            for i in 0..size {
                let r = full(i, bytes);
                assert_eq!(base_old.serve(&r), base_new.serve(&r));
            }
            same(&base_old, &base_new);
            for kind in ["full", "reference"] {
                let requests: Vec<_> = (0..2)
                    .map(|i| {
                        let mut r = full(i, bytes);
                        if kind == "reference" {
                            r.peer_xid.clear();
                            r.values.clear();
                        }
                        r
                    })
                    .collect();
                for trial in 0..TRIALS {
                    let mut old = base_old.clone();
                    let mut new = base_new.clone();
                    // Alternate which backend runs first across trials.
                    let mut run_old = || {
                        let start = Instant::now();
                        for j in 0..OPS {
                            black_box(old.serve(black_box(&requests[j % 2])));
                        }
                        start.elapsed().as_nanos()
                    };
                    let mut run_new = || {
                        let start = Instant::now();
                        for j in 0..OPS {
                            black_box(new.serve(black_box(&requests[j % 2])));
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
                    same(&old, &new);
                    output(kind, size, bytes, trial, "vec", a, OPS);
                    output(kind, size, bytes, trial, "ordered", b, OPS);
                }
            }
        }
        // Existing unbound owner remains oldest; all other rows are ineligible.
        let request = Request {
            self_xid: id(10_000_000),
            ..Request::default()
        };
        let mut old = Server::default();
        let mut new = OrderedServer::default();
        assert_eq!(old.serve(&request), new.serve(&request));
        for i in 0..size - 1 {
            let r = full(i, 32);
            assert_eq!(old.serve(&r), new.serve(&r));
        }
        for trial in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..OPS {
                black_box(old.serve(black_box(&request)));
            }
            let a = start.elapsed().as_nanos();
            let start = Instant::now();
            for _ in 0..OPS {
                black_box(new.serve(black_box(&request)));
            }
            let b = start.elapsed().as_nanos();
            same(&old, &new);
            output("opening-worst", size, 32, trial, "vec", a, OPS);
            output("opening-worst", size, 32, trial, "ordered", b, OPS);
        }
        for trial in 0..TRIALS {
            let t = Instant::now();
            let ttl = Duration::from_secs(10);
            let mut room = Room::default();
            for i in 0..size {
                assert!(matches!(room.serve(&full(i, 32), t, ttl), Outcome::Ok(_)));
            }
            let r = full(0, 32);
            let start = Instant::now();
            for _ in 0..OPS {
                black_box(room.serve(black_box(&r), t, ttl));
            }
            let elapsed = start.elapsed().as_nanos();
            assert_eq!(room.core().len(), size);
            assert_eq!(room.refresh_len(), size);
            output(
                "clocked-hot-no-expired",
                size,
                32,
                trial,
                "ordered-only",
                elapsed,
                OPS,
            );
            let start = Instant::now();
            let removed = room.expire(t + ttl, ttl);
            let elapsed = start.elapsed().as_nanos();
            assert_eq!(removed, size);
            assert!(room.core().is_empty());
            assert_eq!(room.refresh_len(), 0);
            output(
                "expiry-burst",
                size,
                32,
                trial,
                "ordered-only",
                elapsed,
                size,
            );
        }
    }
}
