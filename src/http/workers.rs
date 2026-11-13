//! Lexically scoped application workers. Transport queues remain library-owned.
use std::{
    io,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

#[derive(Default)]
struct Gate {
    state: Mutex<Option<bool>>,
    changed: Condvar,
}
impl Gate {
    fn wait(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        while state.is_none() {
            state = self.changed.wait(state).unwrap();
        }
        state.unwrap()
    }
    fn release(&self, run: bool) {
        let mut state = self.state.lock().unwrap();
        if state.is_none() {
            *state = Some(run);
            self.changed.notify_all();
        }
    }
}
struct Startup<'a>(&'a Gate);
impl Drop for Startup<'_> {
    fn drop(&mut self) {
        self.0.release(false);
    }
}
struct StopOnExit<'a>(&'a AtomicBool);
impl Drop for StopOnExit<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) fn run(
    count: usize,
    stop: &AtomicBool,
    serve: impl Fn() -> io::Result<()> + Sync,
) -> io::Result<()> {
    run_setup(count, stop, serve, |_| Ok(()))
}

// Startup seam permits deterministic rollback tests without exhausting OS threads.
fn run_setup(
    count: usize,
    stop: &AtomicBool,
    serve: impl Fn() -> io::Result<()> + Sync,
    mut before_spawn: impl FnMut(usize) -> io::Result<()>,
) -> io::Result<()> {
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker count must be positive",
        ));
    }
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }
    let failed = AtomicBool::new(false);
    let gate = Gate::default();
    thread::scope(|scope| {
        // Must live INSIDE the scope closure: abort gated threads before the
        // scope joins them, including on a caller-side panic during startup.
        let _startup = Startup(&gate);
        let mut workers = Vec::new();
        for index in 0..count {
            before_spawn(index)?;
            let (gate, failed, serve) = (&gate, &failed, &serve);
            workers.push(
                thread::Builder::new()
                    .name(format!("plyswap-http-{index}"))
                    .spawn_scoped(scope, move || {
                        if !gate.wait() {
                            return Ok(());
                        }
                        let _exit = StopOnExit(failed);
                        while !stop.load(Ordering::Acquire) && !failed.load(Ordering::Acquire) {
                            // Once received, the request is owned and must finish;
                            // stop is deliberately observed at request boundaries.
                            serve()?;
                        }
                        Ok::<_, io::Error>(())
                    })?,
            );
        }
        gate.release(true);
        let mut error = None;
        for worker in workers {
            let result = worker
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("PlySwap HTTP worker panicked")));
            if let Err(e) = result {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, atomic::AtomicUsize, mpsc},
        time::Duration,
    };
    const WAIT: Duration = Duration::from_secs(3);

    #[test]
    fn startup_failure_and_unwind_release_all_gated_threads_before_join() {
        let calls = AtomicUsize::new(0);
        let stop = AtomicBool::new(false);
        assert!(
            run_setup(
                4,
                &stop,
                || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
                |i| if i == 2 {
                    Err(io::Error::other("injected spawn failure"))
                } else {
                    Ok(())
                }
            )
            .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let unwind = std::panic::catch_unwind(|| {
            run_setup(
                4,
                &stop,
                || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
                |i| {
                    assert_ne!(i, 2, "injected startup panic");
                    Ok(())
                },
            )
        });
        assert!(unwind.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(run(0, &stop, || Ok(())).is_err());
    }

    #[test]
    fn exactly_the_configured_workers_can_own_work_and_all_are_joined() {
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Gate::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entries) = mpsc::channel();
        let cloned = (Arc::clone(&stop), Arc::clone(&gate), Arc::clone(&calls));
        let owner = thread::spawn(move || {
            run(3, &cloned.0, || {
                cloned.2.fetch_add(1, Ordering::Relaxed);
                entered.send(()).unwrap();
                cloned.1.wait();
                Ok(())
            })
            .unwrap();
        });
        for _ in 0..3 {
            entries.recv_timeout(WAIT).unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert!(matches!(entries.try_recv(), Err(mpsc::TryRecvError::Empty)));
        stop.store(true, Ordering::Release);
        gate.release(true);
        owner.join().unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn worker_error_and_panic_stop_peers_and_are_reported() {
        for panic in [false, true] {
            let stop = Arc::new(AtomicBool::new(false));
            let gate = Arc::new(Gate::default());
            let (entered, entries) = mpsc::channel();
            let (done, finished) = mpsc::channel();
            let cloned = (Arc::clone(&stop), Arc::clone(&gate));
            let owner = thread::spawn(move || {
                let calls = AtomicUsize::new(0);
                let result = run(3, &cloned.0, || {
                    let call = calls.fetch_add(1, Ordering::Relaxed);
                    if call < 3 {
                        entered.send(()).unwrap();
                        if !cloned.1.wait() {
                            return Ok(());
                        }
                    }
                    if call == 0 {
                        assert!(!panic, "injected worker panic");
                        return Err(io::Error::other("injected worker failure"));
                    }
                    Ok(())
                });
                done.send(result).unwrap();
            });
            let deadline = std::time::Instant::now() + WAIT;
            let ready = (0..3).all(|_| {
                entries
                    .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                    .is_ok()
            });
            if !ready {
                stop.store(true, Ordering::Release);
            }
            gate.release(ready);
            let result = finished.recv_timeout(WAIT);
            // Rescue the threads on regression, but do not accept a late result.
            stop.store(true, Ordering::Release);
            owner.join().unwrap();
            assert!(ready, "three workers did not enter");
            let error = result.expect("healthy peers did not stop").unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(
                error.to_string(),
                if panic {
                    "PlySwap HTTP worker panicked"
                } else {
                    "injected worker failure"
                }
            );
        }
    }
}
