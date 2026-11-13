use crate::server::expiry::Rooms;
use std::{
    io,
    sync::{Arc, mpsc},
    thread::{self, JoinHandle},
    time::Duration,
};

/// Owns one maintenance worker; dropping the listener wakes and joins it.
pub(super) struct Reaper {
    stop: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl Reaper {
    pub(super) fn start(rooms: Arc<Rooms>, ttl: Duration) -> io::Result<Self> {
        let interval = ttl.clamp(Duration::from_millis(10), Duration::from_secs(1));
        let (stop, commands) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("plyswap-expiry".into())
            .spawn(move || {
                loop {
                    match commands.recv_timeout(interval) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Scheduling budgets, not memory/admission limits or
                            // wall-clock bounds. Never wait on a busy room.
                            if let Err(e) = rooms.maintain(64, 64) {
                                eprintln!("PlySwap expiry worker stopped: {e}");
                                return;
                            }
                        }
                    }
                }
            })?;
        Ok(Self {
            stop: Some(stop),
            worker: Some(worker),
        })
    }

    pub(super) fn check(&self) -> io::Result<()> {
        if self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
            Err(io::Error::other("PlySwap expiry worker stopped"))
        } else {
            Ok(())
        }
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        // Disconnect wakes recv_timeout immediately. Never join under cache lock.
        drop(self.stop.take());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                eprintln!("PlySwap expiry worker panicked");
            }
        }
    }
}
