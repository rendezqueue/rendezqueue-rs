mod support;
use rendezqueue::{
    Client,
    Mode::{Current, Previous},
    Outcome, Reply, Request, Server,
};
use support::{
    Side::{A, B},
    World, id,
};

fn full(side: support::Side, generation: u64, values: &[&[u8]], sealed: bool) -> Request {
    let other = if side == A { B } else { A };
    Request {
        self_xid: id(side, generation),
        peer_xid: id(other, generation),
        values: values.iter().map(|v| v.to_vec()).collect(),
        next_xid: if sealed {
            id(side, generation + 1)
        } else {
            vec![]
        },
    }
}

#[test]
fn opening_and_two_literal_rounds() {
    let mut w = World::default();
    w.opening();
    w.clean();
    assert_eq!(w.emissions.len(), 3);
    for (_, p) in &w.emissions {
        assert!(
            p.request.peer_xid.is_empty()
                && p.request.next_xid.is_empty()
                && p.request.values.is_empty()
        );
    }
    for generation in 0..2 {
        w.round();
        w.safety();
        w.clean();
        let base = 3 + generation as usize * 3;
        assert_eq!(
            w.emissions[base].1.request,
            full(A, generation, &[b"7"], true)
        );
        assert_eq!(
            w.emissions[base + 1].1.request,
            full(B, generation, &[b"7"], true)
        );
        let a_ref = &w.emissions[base + 2].1.request;
        assert!(a_ref.is_reference());
        assert_eq!(a_ref.self_xid, id(A, generation));
        assert_eq!(a_ref.next_xid, id(A, generation + 1));
        for side in [A, B] {
            let c = w.client(side);
            assert_eq!(c.generation(), generation + 1);
            assert_eq!(c.previous(), Some(&full(side, generation, &[b"7"], true)));
            assert_eq!(c.received(), vec![b"7".to_vec(); generation as usize + 1]);
            assert!(c.batch().is_none() && c.queued().is_empty());
            assert_eq!(w.uploads(side), generation as usize + 1);
        }
    }
}

#[test]
fn data_empty_pending_then_sealed_completion() {
    let mut w = World::default();
    w.opening();
    w.enqueue(A, b"7");
    w.rpc(A, Current);
    w.rpc(B, Current);
    assert_eq!(
        w.emissions.last().unwrap().1.request,
        full(B, 0, &[], false)
    );
    assert_eq!(w.client(B).generation(), 1);
    w.rpc(A, Current);
    assert_eq!(w.client(A).generation(), 0); // Unsealed EMPTY is pending.
    w.rpc(B, Previous);
    assert_eq!(w.emissions.last().unwrap().1.request, full(B, 0, &[], true));
    w.rpc(A, Current);
    assert_eq!(w.client(A).generation(), 1);
    assert!(w.client(A).received().is_empty());
    assert_eq!(w.client(B).received(), [b"7".to_vec()]);
    assert_eq!(w.client(B).previous(), Some(&full(B, 0, &[], true)));
    assert_eq!((w.uploads(A), w.uploads(B)), (1, 0));
    w.clean();
    w.safety();
}

#[test]
fn reference_miss_restores_exact_frozen_full() {
    let mut w = World::default();
    w.before_miss();
    w.server.forget(&id(A, 0));
    let db = w.server.clone();
    w.send(A, Current);
    assert!(w.requests[0].1.request.is_reference());
    w.process(0);
    assert_eq!(w.replies[0].outcome, Outcome::Reject);
    w.deliver(0);
    assert_eq!(w.server, db);
    assert_eq!(w.client(A).batch(), Some([b"7".to_vec()].as_slice()));
    w.rpc(A, Current);
    assert_eq!(
        w.emissions.last().unwrap().1.request,
        full(A, 0, &[b"7"], true)
    );
    assert_eq!(w.client(A).generation(), 1);
    assert_eq!(w.client(A).received(), [b"7".to_vec()]);
    w.clean();
    w.safety();
}

#[test]
fn lost_full_recovery_reply_keeps_current_and_previous_full() {
    for mode in [Current, Previous] {
        let mut w = World::default();
        w.opening();
        w.round();
        let side = if mode == Current { A } else { B };
        if mode == Current {
            w.enqueue(A, b"7");
            w.rpc(A, Current);
        }
        let generation = if mode == Current { 1 } else { 0 };
        let before_out = w.client(side).received().to_vec();
        w.server.forget(&id(side, generation));
        w.rpc(side, mode); // Reference MISS.
        w.send(side, mode);
        assert_eq!(
            w.requests[0].1.request,
            full(side, generation, &[b"7"], true)
        );
        w.process(0);
        w.drop_reply(0);
        w.timeout(side);
        w.send(side, mode);
        assert_eq!(
            w.requests[0].1.request,
            full(side, generation, &[b"7"], true)
        );
        assert_eq!(w.client(side).received(), before_out);
        w.safety();
    }
}

#[test]
fn both_rows_recover_after_reset() {
    let mut w = World::default();
    w.before_miss();
    w.server.reset();
    let start = w.emissions.len();
    w.rpc(A, Current);
    w.rpc(A, Current);
    w.rpc(B, Previous);
    w.rpc(B, Previous);
    w.rpc(A, Current);
    let emitted = &w.emissions[start..];
    assert!(emitted[0].1.request.is_reference());
    assert_eq!(emitted[1].1.request, full(A, 0, &[b"7"], true));
    assert!(emitted[2].1.request.is_reference());
    assert_eq!(emitted[3].1.request, full(B, 0, &[b"7"], true));
    assert!(emitted[4].1.request.is_reference());
    for side in [A, B] {
        assert_eq!(w.client(side).generation(), 1);
        assert_eq!(w.client(side).received(), [b"7".to_vec()]);
        assert_eq!(
            w.client(side).previous(),
            Some(&full(side, 0, &[b"7"], true))
        );
    }
    w.clean();
    w.safety();
}

#[test]
fn frozen_batch_queued_tail_and_duplicate_completion() {
    let mut w = World::default();
    w.opening();
    w.enqueue(A, b"7");
    w.send(A, Current);
    w.enqueue(A, b"7");
    w.process(0);
    w.deliver(0);
    w.enqueue(B, b"7");
    w.send(B, Current);
    w.process(0);
    w.duplicate_reply(0);
    w.deliver(0);
    let completed = w.client(B).clone();
    w.deliver(0);
    assert_eq!(w.client(B), &completed);
    w.rpc(A, Current);
    assert_eq!(w.client(A).queued(), [b"7".to_vec()]);
    assert_eq!(w.client(A).stored(), [b"7".to_vec(), b"7".to_vec()]);
    assert_eq!(w.client(A).previous(), Some(&full(A, 0, &[b"7"], true)));
    w.safety();
}

#[test]
fn timeout_live_original_cannot_clear_or_confirm_new_attempt() {
    let mut w = World::default();
    w.opening();
    w.enqueue(A, b"7");
    w.send(A, Current);
    w.duplicate_request(0);
    w.timeout(A);
    w.send(A, Current);
    let active = w.client(A).clone();
    w.process(0);
    w.deliver(0);
    assert_eq!(w.client(A), &active);
    assert_eq!(w.requests.len(), 2);
    w.process(1);
    w.deliver(0);
    w.send(A, Current);
    assert!(w.requests.last().unwrap().1.request.is_reference());
    w.safety();
}

#[test]
fn stale_empty_does_not_erase_seal_after_reset() {
    let mut w = World::default();
    w.opening();
    w.send(B, Current);
    w.duplicate_request(0);
    w.timeout(B);
    w.enqueue(A, b"7");
    w.rpc_at(A, Current, 2);
    w.rpc_at(B, Current, 2);
    w.rpc_at(B, Previous, 2);
    w.rpc_at(A, Current, 2);
    w.server.reset();
    w.process(0);
    w.deliver(0);
    w.rpc_at(B, Previous, 1);
    w.process(0);
    w.deliver(0);
    let row = w
        .server
        .rows()
        .iter()
        .find(|r| r.self_xid == id(B, 0))
        .unwrap();
    assert_eq!(row.next_xid, id(B, 1));
    assert!(row.values.is_empty());
    assert_eq!(w.client(B).previous(), Some(&full(B, 0, &[], true)));
    assert_eq!(w.client(B).received(), [b"7".to_vec()]);
    assert_eq!(w.client(B).generation(), 1);
    w.safety();
}

#[test]
fn previous_reference_does_not_deliver_again() {
    let mut w = World::default();
    w.opening();
    w.round();
    let before = w.client(B).clone();
    w.rpc(B, Previous);
    assert!(w.emissions.last().unwrap().1.request.is_reference());
    // Transport attempt changes; application/storage/generation do not.
    assert_eq!(w.client(B).generation(), before.generation());
    assert_eq!(w.client(B).received(), before.received());
    assert_eq!(w.client(B).previous(), before.previous());
    assert_eq!(w.client(B).stored(), before.stored());
    w.safety();
}

#[test]
fn malformed_reply_clears_only_exact_active_and_retains_storage() {
    let mut w = World::default();
    w.before_miss();
    w.send(A, Current);
    let p = w.requests.last().unwrap().1.clone();
    let storage = w.client(A).stored();
    let out = Outcome::Ok(Reply {
        self_xid: p.request.self_xid.clone(),
        peer_xid: b"illegal".to_vec(),
        next_xid: b"also illegal".to_vec(),
        values: vec![],
    });
    w.clients[0].accept(&p, &out).unwrap();
    assert!(w.client(A).active().is_none());
    assert_eq!(w.client(A).stored(), storage);
    assert!(w.client(A).received().is_empty());
    w.send(A, Current);
    assert!(w.requests.last().unwrap().1.request.is_reference()); // Hint was not changed.
}

#[test]
fn binary_payloads_and_empty_element_are_preserved() {
    let mut w = World::default();
    w.opening();
    for v in [b"".as_slice(), &[0, 255, 128], b"".as_slice()] {
        w.enqueue(A, v);
    }
    w.enqueue(B, &[255, 0]);
    w.rpc(A, Current);
    w.rpc(B, Current);
    w.rpc(A, Current);
    assert_eq!(w.client(B).received(), [vec![], vec![0, 255, 128], vec![]]);
    assert_eq!(w.client(A).received(), [vec![255, 0]]);
    w.safety();
}

#[test]
fn disabled_send_and_same_attempt_changed_request_are_noops() {
    let mut c = Client::new(b"local".to_vec());
    let initial = c.clone();
    assert!(c.send(Previous).unwrap().is_none());
    assert_eq!(c, initial);
    let p = c.send(Current).unwrap().unwrap();
    let active = c.clone();
    assert!(c.send(Current).unwrap().is_none());
    assert_eq!(c, active);
    let mut altered = p.clone();
    altered.request.values.push(b"forged".to_vec());
    c.accept(&altered, &Outcome::Reject).unwrap();
    assert_eq!(c, active);
}

#[test]
fn reference_miss_is_not_discovery_or_successor_lookup() {
    let mut db = Server::default();
    let root = Request {
        self_xid: b"root".to_vec(),
        next_xid: b"next".to_vec(),
        ..Request::default()
    };
    let original = db.clone();
    assert_eq!(db.serve(&root), Outcome::Reject);
    assert_eq!(db, original);
    let later = Request {
        self_xid: b"next".to_vec(),
        peer_xid: b"other".to_vec(),
        values: vec![b"data".to_vec()],
        next_xid: b"later".to_vec(),
    };
    db.serve(&later);
    let original = db.clone();
    assert_eq!(db.serve(&root), Outcome::Reject);
    assert_eq!(db, original);
}

#[test]
fn empty_xids_are_absent_and_cannot_complete_or_erase_a_seal() {
    let mut vector = Server::default();
    let mut ordered = rendezqueue::OrderedServer::default();
    let mut serve = |request: &Request| {
        let outcome = vector.serve(request);
        assert_eq!(ordered.serve(request), outcome);
        assert!(vector.rows().iter().eq(ordered.rows()));
        outcome
    };
    assert_eq!(serve(&Request::default()), Outcome::Reject);
    let pending = Request {
        self_xid: vec![0],
        peer_xid: b"b".to_vec(),
        ..Request::default()
    };
    assert_eq!(
        serve(&pending),
        Outcome::Ok(Reply {
            self_xid: vec![0],
            ..Reply::default()
        })
    );
    let sealed = Request {
        next_xid: b"a1".to_vec(),
        ..pending.clone()
    };
    serve(&sealed);
    serve(&pending); // An empty successor cannot erase the seal.
    let data = Request {
        self_xid: b"b".to_vec(),
        peer_xid: vec![0],
        next_xid: b"b1".to_vec(),
        values: vec![vec![]],
    };
    assert_eq!(
        serve(&data),
        Outcome::Ok(Reply {
            self_xid: b"b".to_vec(),
            next_xid: b"a1".to_vec(),
            ..Reply::default()
        })
    );
    assert_eq!(
        serve(&sealed),
        Outcome::Ok(Reply {
            self_xid: vec![0],
            next_xid: b"b1".to_vec(),
            values: vec![vec![]],
            ..Reply::default()
        })
    );

    let mut w = World::default();
    w.opening();
    w.enqueue(A, b"keep");
    w.send(A, Current);
    let packet = w.requests.last().unwrap().1.clone();
    let stored = w.client(A).stored();
    w.clients[0]
        .accept(
            &packet,
            &Outcome::Ok(Reply {
                self_xid: packet.request.self_xid.clone(),
                ..Reply::default()
            }),
        )
        .unwrap();
    assert_eq!(w.client(A).generation(), 0);
    assert_eq!(w.client(A).stored(), stored);
    assert!(w.client(A).received().is_empty());
}

/// Bounded adversarial exploration, not a proof or a fairness claim.
#[test]
fn seeded_fault_traces_preserve_literal_prefix_and_real_storage() {
    for seed in 1..=32u64 {
        let mut random = seed;
        let mut w = World::default();
        for _ in 0..1000 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let side = if random & 256 == 0 { A } else { B };
            let index = ((random >> 32) % 7) as usize;
            match (random >> 16) % 16 {
                0 | 1 => w.enqueue(side, &[((random >> 48) % 3) as u8]),
                2 | 3 => w.send(side, Current),
                4 => w.send(side, Previous),
                5 => w.process(index),
                6 => w.deliver(index),
                7 => w.duplicate_request(index),
                8 => w.duplicate_reply(index),
                9 => w.drop_request(index),
                10 => w.drop_reply(index),
                11 => w.timeout(side),
                12 => w.server.reset(),
                13 => w.server.forget(&id(side, w.client(side).generation())),
                14 => w.server.evict_opening(),
                _ => {
                    w.process(0);
                    w.deliver(0);
                }
            }
            w.safety();
        }
        // Faults stop, pending traffic is lost, then explicit healthy retries.
        w.requests.clear();
        w.replies.clear();
        w.timeout(A);
        w.timeout(B);
        for _ in 0..30 {
            for side in [A, B] {
                w.rpc(side, Previous);
                w.rpc(side, Current);
                w.safety();
            }
        }
        assert_eq!(w.client(A).received(), w.inputs[1], "seed {seed}");
        assert_eq!(w.client(B).received(), w.inputs[0], "seed {seed}");
    }
}
