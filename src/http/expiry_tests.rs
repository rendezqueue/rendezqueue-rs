use super::*;
use std::{
    sync::{Weak, mpsc},
    thread,
    time::Instant,
};

const WAIT: Duration = Duration::from_secs(5);
const TTL: Duration = Duration::from_secs(1);

fn endpoint(server: &HttpServer) -> String {
    format!("http://{}{}", server.local_addr(), wire::ROUTE)
}

fn rpc(server: &mut HttpServer, url: String, room: Bytes, request: Request) -> Outcome {
    let caller = thread::spawn(move || {
        HttpTransport::new(&url, room, WAIT)
            .unwrap()
            .exchange(&request)
            .unwrap()
    });
    assert!(server.serve_next(WAIT).unwrap());
    caller.join().unwrap()
}

fn wait_empty(rooms: &Rooms) {
    let deadline = Instant::now() + WAIT;
    while rooms.len() != 0 {
        assert!(
            Instant::now() < deadline,
            "idle reaper did not remove expired room"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn idle_rooms_are_physically_reclaimed_without_another_dispatch() {
    let mut server = HttpServer::bind_with_ttl("127.0.0.1:0", TTL).unwrap();
    let url = endpoint(&server);
    assert!(matches!(
        rpc(
            &mut server,
            url,
            b"idle".to_vec(),
            Request {
                self_xid: b"a".to_vec(),
                ..Request::default()
            }
        ),
        Outcome::Ok(_)
    ));
    assert_eq!(server.room_count(), 1);
    // Inspection only: no serve_next/sweep request can manufacture cleanup.
    wait_empty(&server.rooms);
    assert_eq!(server.room_count(), 0);
}

#[test]
fn blocked_body_read_does_not_block_unrelated_idle_expiry() {
    let mut server = HttpServer::bind_with_ttl("127.0.0.1:0", TTL).unwrap();
    let url = endpoint(&server);
    rpc(
        &mut server,
        url,
        b"old".to_vec(),
        Request {
            self_xid: b"a".to_vec(),
            ..Request::default()
        },
    );
    assert_eq!(server.room_count(), 1);
    let rooms = Arc::clone(&server.rooms);
    let addr = server.local_addr();
    let (done, finished) = mpsc::channel();
    let worker = thread::spawn(move || {
        let request = server
            .listener
            .recv_timeout(WAIT)
            .unwrap()
            .expect("slow request");
        server.handle(request);
        done.send(()).unwrap();
        server
    });
    let stream = super::concurrency_tests::slow(addr);
    super::concurrency_tests::wait_for_body_reader(&stream);
    wait_empty(&rooms);
    assert!(
        matches!(finished.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "body read should still be blocked"
    );
    drop(stream);
    finished.recv_timeout(WAIT).unwrap();
    let server = worker.join().unwrap();
    assert_eq!(
        server.room_count(),
        0,
        "incomplete body cannot create a room"
    );
}

#[test]
fn malformed_admission_does_not_renew_an_existing_row() {
    let mut server = HttpServer::bind("127.0.0.1:0").unwrap();
    let url = endpoint(&server);
    rpc(
        &mut server,
        url.clone(),
        b"admission".to_vec(),
        Request {
            self_xid: b"a".to_vec(),
            ..Request::default()
        },
    );
    let before = server.rooms.refreshed_at(b"admission", b"a").unwrap();
    for body in [
        b"not JSON".as_slice(),
        br#"{"room":"admission","self":"a","extra":true}"#,
        br#"{"room":"admission","self":"a","ttl":20}"#,
    ] {
        let url = url.clone();
        let caller = thread::spawn(move || {
            let agent: ureq::Agent = ureq::Agent::config_builder()
                .http_status_as_error(false)
                .proxy(None)
                .timeout_global(Some(WAIT))
                .build()
                .into();
            agent
                .post(&url)
                .header("Content-Type", "application/json")
                .send(body)
                .unwrap()
                .status()
                .as_u16()
        });
        assert!(server.serve_next(WAIT).unwrap());
        assert_eq!(caller.join().unwrap(), 400);
        assert_eq!(server.rooms.refreshed_at(b"admission", b"a"), Some(before));
    }
}

#[test]
fn shutdown_wakes_and_joins_reaper_and_zero_ttl_is_rejected() {
    assert!(
        matches!(HttpServer::bind_with_ttl("127.0.0.1:0", Duration::ZERO), Err(e) if e.kind() == io::ErrorKind::InvalidInput)
    );
    let server = HttpServer::bind_with_ttl("127.0.0.1:0", Duration::MAX).unwrap();
    let rooms: Weak<Rooms> = Arc::downgrade(&server.rooms);
    let (done, finished) = mpsc::channel();
    let worker = thread::spawn(move || {
        drop(server);
        done.send(()).unwrap();
    });
    finished
        .recv_timeout(WAIT)
        .expect("drop failed to wake sleeping reaper");
    worker.join().unwrap();
    assert!(
        rooms.upgrade().is_none(),
        "maintenance retained its cache after shutdown"
    );
}
