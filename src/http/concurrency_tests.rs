use super::*;
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::{Mutex, atomic::Ordering, mpsc},
    thread,
    time::Instant,
};
const WAIT: Duration = Duration::from_secs(5);
pub(super) fn slow(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    write!(stream, "POST {} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n{{", wire::ROUTE).unwrap();
    stream
}

pub(super) fn wait_for_body_reader(stream: &TcpStream) {
    // tiny_http sends 100 Continue only when the handler calls as_reader().
    // This establishes actual worker ownership before sending the healthy RPC.
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("HTTP/1.1 100 Continue"), "{line:?}");
    loop {
        line.clear();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        if line == "\r\n" {
            break;
        }
    }
}

#[test]
fn pooled_requests_progress_while_another_body_is_stalled() {
    let mut server = HttpServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    // Make the slow request available before starting the pool.
    let stream = slow(addr);
    let stop = Arc::new(AtomicBool::new(false));
    let cloned = Arc::clone(&stop);
    let owner = thread::spawn(move || {
        server.run_until(2, &cloned).unwrap();
        server
    });
    wait_for_body_reader(&stream);
    let url = format!("http://{addr}{}", wire::ROUTE);
    let transport = HttpTransport::new(&url, b"healthy".to_vec(), WAIT).unwrap();
    assert!(matches!(
        transport
            .exchange(&Request {
                self_xid: b"a".to_vec(),
                ..Request::default()
            })
            .unwrap(),
        Outcome::Ok(_)
    ));
    // The other worker is still blocked; the incomplete body created no state.
    drop(stream);
    stop.store(true, Ordering::Release);
    let server = owner.join().unwrap();
    assert_eq!(server.room_count(), 1);
}

#[test]
fn cooperative_stop_waits_for_worker_owned_io_then_joins() {
    let server = HttpServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let stream = slow(addr);
    // Explicitly receive the request to deterministically establish ownership.
    let request = server.listener.recv_timeout(WAIT).unwrap().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let cloned = Arc::clone(&stop);
    let (entered, entries) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let owner = thread::spawn(move || {
        let request = Mutex::new(Some(request));
        workers::run(1, &cloned, || {
            let request = request.lock().unwrap().take().unwrap();
            entered.send(()).unwrap();
            server.handle(request);
            Ok(())
        })
        .unwrap();
        done.send(()).unwrap();
    });
    entries.recv_timeout(WAIT).unwrap();
    wait_for_body_reader(&stream);
    stop.store(true, Ordering::Release);
    assert!(matches!(
        finished.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(stream);
    finished.recv_timeout(WAIT).unwrap();
    owner.join().unwrap();
}

#[test]
fn idle_pool_stop_is_joined_and_zero_workers_are_rejected() {
    let mut server = HttpServer::bind("127.0.0.1:0").unwrap();
    assert!(
        matches!(server.run_until(0, &AtomicBool::new(false)), Err(e) if e.kind() == io::ErrorKind::InvalidInput)
    );
    let addr = server.local_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let cloned = Arc::clone(&stop);
    let (done, finished) = mpsc::channel();
    let owner = thread::spawn(move || {
        server.run_until(4, &cloned).unwrap();
        drop(server);
        done.send(()).unwrap();
    });
    // A completed real RPC proves the pool passed startup; immediate stop
    // without this could test only the already-stopped early-return path.
    let url = format!("http://{addr}{}", wire::ROUTE);
    assert!(matches!(
        HttpTransport::new(&url, b"ready".to_vec(), WAIT)
            .unwrap()
            .exchange(&Request {
                self_xid: b"a".to_vec(),
                ..Request::default()
            })
            .unwrap(),
        Outcome::Ok(_)
    ));
    let start = Instant::now();
    stop.store(true, Ordering::Release);
    finished.recv_timeout(WAIT).unwrap();
    owner.join().unwrap();
    assert!(start.elapsed() < WAIT);
}
