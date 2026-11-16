use super::*;
use std::{
    sync::{Barrier, mpsc},
    thread,
};

const WAIT: Duration = Duration::from_secs(3);
fn open(self_xid: &[u8]) -> Request {
    Request {
        self_xid: self_xid.to_vec(),
        ..Request::default()
    }
}

#[test]
fn retirement_is_irreversible_and_old_handles_cannot_remove_a_replacement() {
    let rooms = Rooms::new(Duration::MAX);
    let room = b"same".to_vec();
    let old = rooms.slot(&room).unwrap();
    // A request already has an old handle when cleanup retires the empty slot.
    *old.live.lock().unwrap() = None;
    rooms.unlink(&room, &old).unwrap();
    let new = rooms.slot(&room).unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    rooms.serve_now(room.clone(), &open(b"a")).unwrap();
    rooms.unlink(&room, &old).unwrap();
    // Exercise the real retry path with a previously acquired retired handle.
    rooms
        .serve_slot_clock(room.clone(), &open(b"b"), Instant::now, Arc::clone(&old))
        .unwrap();
    assert!(Arc::ptr_eq(&rooms.slot(&room).unwrap(), &new));
    assert!(old.live.lock().unwrap().is_none());
    assert_eq!(new.live.lock().unwrap().as_ref().unwrap().core.len(), 2);
}

#[test]
fn unrelated_rooms_and_maintenance_progress_while_one_room_is_locked() {
    let rooms = Arc::new(Rooms::new(Duration::from_secs(1)));
    let now = Instant::now();
    rooms
        .serve_clock(b"busy".to_vec(), &open(b"a"), || now)
        .unwrap();
    rooms
        .serve_clock(b"idle".to_vec(), &open(b"b"), || now)
        .unwrap();
    let busy = rooms.slot(b"busy").unwrap();
    let held = busy.live.lock().unwrap();
    let cloned = Arc::clone(&rooms);
    let (sent, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        cloned.serve_now(b"other".to_vec(), &open(b"c")).unwrap();
        let visits = cloned
            .maintain_clock(|| now + Duration::from_secs(2), 3, 1)
            .unwrap();
        sent.send(visits).unwrap();
    });
    assert_eq!(received.recv_timeout(WAIT).unwrap(), 2);
    assert!(
        !rooms
            .registry
            .lock()
            .unwrap()
            .contains_key(b"idle".as_slice())
    );
    drop(held);
    worker.join().unwrap();
}

#[test]
fn background_work_is_budgeted_and_access_expiry_is_still_exact() {
    let rooms = Rooms::new(Duration::from_secs(1));
    let now = Instant::now();
    for room in 0..10u8 {
        for self_xid in 0..10u8 {
            rooms
                .serve_clock(vec![room], &open(&[self_xid]), || now)
                .unwrap();
        }
    }
    assert_eq!(
        rooms
            .maintain_clock(|| now + Duration::from_secs(2), 2, 3)
            .unwrap(),
        2
    );
    let slots: Vec<_> = rooms.registry.lock().unwrap().values().cloned().collect();
    let lengths: Vec<_> = slots
        .iter()
        .map(|s| s.live.lock().unwrap().as_ref().unwrap().core.len())
        .collect();
    assert_eq!(lengths.iter().filter(|n| **n == 7).count(), 2);
    assert_eq!(lengths.iter().filter(|n| **n == 10).count(), 8);
    // This room has not been reaped. A reference must nevertheless miss.
    assert_eq!(
        rooms
            .serve_clock(
                vec![9],
                &Request {
                    self_xid: vec![0],
                    next_xid: b"missing-next".to_vec(),
                    ..Request::default()
                },
                || now + Duration::from_secs(2)
            )
            .unwrap(),
        Outcome::Reject
    );
    assert!(!rooms.registry.lock().unwrap().contains_key([9].as_slice()));
    for _ in 0..20 {
        rooms
            .maintain_clock(|| now + Duration::from_secs(2), 2, 3)
            .unwrap();
    }
    assert_eq!(rooms.len(), 0);
}

#[test]
fn concurrent_same_room_updates_preserve_all_rows_and_one_timer_per_sid() {
    let rooms = Arc::new(Rooms::new(Duration::MAX));
    let barrier = Arc::new(Barrier::new(9));
    thread::scope(|scope| {
        for worker in 0..8u8 {
            let rooms = Arc::clone(&rooms);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                for self_xid in 0..128u8 {
                    rooms
                        .serve_now(b"shared".to_vec(), &open(&[worker, self_xid]))
                        .unwrap();
                }
            });
        }
        barrier.wait();
    });
    let slot = rooms.slot(b"shared").unwrap();
    let live = slot.live.lock().unwrap();
    let room = live.as_ref().unwrap();
    assert_eq!(room.core.len(), 1024);
    assert_eq!(room.refreshed.len(), 1024);
    let mut previous = None;
    for (self_xid, time) in &room.refreshed {
        assert!(previous.is_none_or(|p| p <= *time));
        previous = Some(*time);
        assert!(room.core.contains_xid(self_xid));
    }
}

#[test]
fn clock_is_not_sampled_until_the_room_lock_is_acquired() {
    let rooms = Arc::new(Rooms::new(Duration::MAX));
    let slot = rooms.slot(b"room").unwrap();
    let held = slot.live.lock().unwrap();
    let (sampled, samples) = mpsc::channel();
    let (started, starts) = mpsc::channel();
    let cloned = Arc::clone(&rooms);
    let worker = thread::spawn(move || {
        started.send(()).unwrap();
        cloned
            .serve_clock(b"room".to_vec(), &open(b"a"), || {
                sampled.send(()).unwrap();
                Instant::now()
            })
            .unwrap();
    });
    starts.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        samples.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(held);
    samples.recv_timeout(WAIT).unwrap();
    worker.join().unwrap();
}

#[test]
fn poisoned_room_fails_closed_instead_of_creating_a_new_generation() {
    let rooms = Rooms::new(Duration::MAX);
    let slot = rooms.slot(b"room").unwrap();
    let cloned = Arc::clone(&slot);
    let _ = thread::spawn(move || {
        let _held = cloned.live.lock().unwrap();
        panic!("injected room panic");
    })
    .join();
    assert!(rooms.serve_now(b"room".to_vec(), &open(b"a")).is_err());
    assert!(rooms.maintain(1, 1).is_err());
    assert!(Arc::ptr_eq(&rooms.slot(b"room").unwrap(), &slot));
}
