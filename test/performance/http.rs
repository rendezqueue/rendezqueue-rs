//! Actual loopback HTTP measurements, not a pure-core benchmark.
use rendezqueue::{
    Request, Server,
    http::{HttpServer, HttpTransport},
    wire,
};
use std::{
    io,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
const CALLS: usize = 64;
const TRIALS: usize = 3;
const CLIENTS: [usize; 3] = [1, 4, 16];
fn percentile(samples: &[u128], percent: usize) -> u128 {
    samples[(samples.len() - 1) * percent / 100]
}

fn measure(
    workers: usize,
    clients: usize,
    shared: bool,
    payload: usize,
    trial: usize,
) -> io::Result<()> {
    let mut server = HttpServer::bind_with_ttl("127.0.0.1:0", Duration::MAX)?;
    let addr = server.local_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let owner = thread::spawn(move || server.run_until(workers, &flag));
    // Always stop and join, even if a measurement/check unwinds.
    struct Running(Arc<AtomicBool>, Option<thread::JoinHandle<io::Result<()>>>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            if let Some(owner) = self.1.take() {
                owner.join().unwrap().unwrap();
            }
        }
    }
    let _running = Running(stop, Some(owner));
    let mut inputs = vec![];
    for i in 0..clients {
        let room = if shared { vec![0] } else { vec![1, i as u8] };
        let endpoint = format!("http://{addr}{}", wire::ROUTE);
        let transport = HttpTransport::new(&endpoint, room, Duration::from_secs(20)).unwrap();
        let request = Request {
            self_xid: vec![0x11, i as u8],
            peer_xid: vec![0x22, i as u8],
            next_xid: vec![0x33, i as u8],
            values: vec![vec![7; payload]],
        };
        let mut reference = Server::default();
        assert_eq!(
            transport.exchange(&request).unwrap(),
            reference.serve(&request)
        );
        let expected = reference.serve(&request);
        inputs.push((transport, request, expected));
    }
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut threads = vec![];
    for (transport, request, expected) in inputs {
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            let mut times = Vec::with_capacity(CALLS);
            barrier.wait();
            for _ in 0..CALLS {
                let start = Instant::now();
                assert_eq!(transport.exchange(&request).unwrap(), expected);
                times.push(start.elapsed().as_nanos());
            }
            times
        }));
    }
    let start = Instant::now();
    barrier.wait();
    let mut times: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let elapsed = start.elapsed().as_nanos();
    times.sort_unstable();
    assert_eq!(times.len(), clients * CALLS);
    println!(
        "(() (workers {workers}) (clients {clients}) (distribution \"{}\") (payload_bytes {payload}) (trial {trial}) (requests {}) (elapsed_ns {elapsed}) (p50_ns {}) (p95_ns {}) (p99_ns {}))",
        if shared { "shared" } else { "distinct" },
        times.len(),
        percentile(&times, 50),
        percentile(&times, 95),
        percentile(&times, 99)
    );
    Ok(())
}
fn main() -> io::Result<()> {
    println!(
        "(())\n; Actual loopback HTTP, fresh TCP per RPC, TTL disabled via Duration::MAX.\n; Setup/thread creation before timing; response validation is timed. No CPU/frequency pinning."
    );
    for workers in [1, 4] {
        for clients in CLIENTS {
            for shared in [false, true] {
                for payload in [32, 4096] {
                    for trial in 0..TRIALS {
                        measure(workers, clients, shared, payload, trial)?;
                    }
                }
            }
        }
    }
    Ok(())
}
