use rendezqueue::{
    Client,
    Mode::{Current, Previous},
    Outcome, Server,
};
use sxpb::{Field, List, Mesg, Value};

/// Human-readable renaming only; the library still uses opaque byte identities.
fn label(id: &[u8]) -> String {
    let (namespace, suffix) = id.split_at(id.len().checked_sub(8).unwrap());
    assert!(namespace == b"Alice-" || namespace == b"Bob-");
    let generation = u64::from_be_bytes(suffix.try_into().unwrap());
    format!("{}{generation}", std::str::from_utf8(namespace).unwrap())
}

fn message(sid: &[u8], ack: Option<&[u8]>, values: &[Vec<u8>], nxt: Option<&[u8]>) -> Value {
    let mut fields = vec![Field::new("sid", label(sid))];
    if let Some(ack) = ack {
        fields.push(Field::new("ack", label(ack)));
    }
    if !values.is_empty() {
        fields.push(Field::new(
            "values",
            List::from(
                values
                    .iter()
                    .map(|v| Value::from(std::str::from_utf8(v).unwrap()))
                    .collect::<Vec<_>>(),
            ),
        ));
    }
    if let Some(nxt) = nxt {
        fields.push(Field::new("nxt", label(nxt)));
    }
    Mesg::from(fields).into()
}

#[test]
fn idle_then_data_transcript_matches_real_client_emissions_and_receipts() {
    let doc = sxpb::from_str(include_str!("scenario/idle-then-data.sxpb")).unwrap();
    let steps = doc.as_array().unwrap();
    // Opening 3 + first duplex 3 + two idle cycles × 4 + resumed DATA/EMPTY 5.
    let schedule = [
        (0, Current),
        (1, Current),
        (0, Current),
        (0, Current),
        (1, Current),
        (0, Current),
        (0, Previous),
        (0, Current),
        (1, Previous),
        (1, Current),
        (0, Previous),
        (0, Current),
        (1, Previous),
        (1, Current),
        (0, Current),
        (1, Current),
        (0, Current),
        (1, Previous),
        (0, Current),
    ];
    assert_eq!(steps.len(), schedule.len());
    let mut clients = [
        Client::new(b"Alice-".to_vec()),
        Client::new(b"Bob-".to_vec()),
    ];
    let mut server = Server::default();
    let mut idle_snapshot = None;
    let mut uploads = [0, 0];
    for (i, ((side, mode), expected)) in schedule.into_iter().zip(steps).enumerate() {
        if i == 3 {
            clients[0].enqueue(b"first from Alice".to_vec());
            clients[1].enqueue(b"first from Bob".to_vec());
        }
        if i == 14 {
            clients[0].enqueue(b"something new".to_vec());
        }
        let packet = clients[side].send(mode).unwrap().unwrap();
        let r = &packet.request;
        assert_eq!(
            Some(&message(
                &r.sid,
                r.ack.as_deref(),
                &r.values,
                r.nxt.as_deref()
            )),
            expected.get("req"),
            "request at step {}",
            i + 1
        );
        uploads[side] += usize::from(!r.values.is_empty());
        let outcome = server.serve(r);
        let Outcome::Ok(reply) = &outcome else {
            panic!("unexpected rejection at step {}", i + 1)
        };
        assert_eq!(
            Some(&message(
                &reply.sid,
                reply.ack.as_deref(),
                &reply.values,
                reply.nxt.as_deref()
            )),
            expected.get("res"),
            "reply at step {}",
            i + 1
        );
        clients[side].accept(&packet, &outcome).unwrap();
        if i == 5 {
            idle_snapshot = Some(clients.clone());
        }
        if (6..14).contains(&i) {
            let before = idle_snapshot.as_ref().unwrap();
            for side in 0..2 {
                // Attempts advance, but every actual storage/delivery field
                // observable through the public API remains unchanged.
                assert_eq!(clients[side].generation(), 1);
                assert_eq!(clients[side].received(), before[side].received());
                assert_eq!(clients[side].previous(), before[side].previous());
                assert_eq!(clients[side].stored(), before[side].stored());
                assert!(clients[side].queued().is_empty() && clients[side].batch().is_none());
                assert!(clients[side].active().is_none());
            }
            assert_eq!(uploads, [1, 1]);
        }
        if i == 16 {
            assert_eq!(clients[0].generation(), 1); // Bob has not published his EMPTY seal.
            assert_eq!(clients[1].generation(), 2);
        }
    }
    assert_eq!(clients[0].generation(), 2);
    assert_eq!(clients[1].generation(), 2);
    assert_eq!(clients[0].received(), [b"first from Bob".to_vec()]);
    assert_eq!(
        clients[1].received(),
        [b"first from Alice".to_vec(), b"something new".to_vec()]
    );
    assert_eq!(uploads, [2, 1]); // New DATA uploaded exactly once; Bob uploads no new DATA.
}
