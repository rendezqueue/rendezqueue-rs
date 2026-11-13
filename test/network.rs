use rendezqueue::{
    Client, Mode, Outcome, Reply, Request, Server,
    http::{HttpClient, HttpServer, HttpTransport},
    wire,
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use tiny_http::{Header, Response, StatusCode};

const WAIT: Duration = Duration::from_secs(3);

fn serve(count: usize) -> (String, thread::JoinHandle<HttpServer>) {
    let mut server = HttpServer::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", server.local_addr());
    let worker = thread::spawn(move || {
        for _ in 0..count {
            assert!(server.serve_next(WAIT).unwrap(), "missing RPC");
        }
        server
    });
    (origin, worker)
}
fn endpoint(origin: &str) -> String {
    format!("{origin}{}", wire::ROUTE)
}
fn client(url: &str, who: &[u8], key: &[u8]) -> HttpClient {
    HttpClient::new(who.to_vec(), url, key.to_vec(), WAIT).unwrap()
}
fn open(a: &mut HttpClient, b: &mut HttpClient) {
    a.step(Mode::Current).unwrap();
    b.step(Mode::Current).unwrap();
    a.step(Mode::Current).unwrap();
}

#[test]
fn codec_losslessly_normalizes_all_bytes_and_empty_contributions() {
    let r = Request {
        sid: (0..=255).collect(),
        ack: Some(vec![]),
        values: vec![vec![], (0..=255).rev().collect(), vec![]],
        nxt: Some(vec![0, 255]),
    };
    assert_eq!(wire::decode_request(&wire::encode_request(&r)).unwrap(), r);
    let omitted = wire::decode_request(br#"{"sid":""}"#).unwrap();
    assert_eq!(
        omitted,
        wire::decode_request(br#"{"sid":"","values":[]}"#).unwrap()
    );
    assert_eq!(
        wire::decode_request(br#"{"sid":"","values":[""]}"#)
            .unwrap()
            .values,
        [Vec::<u8>::new()]
    );
    assert_eq!(wire::encode_request(&omitted), br#"{"sid":""}"#);
    assert!(
        wire::decode_reply(br#"{"sid":"","nxt":""}"#)
            .unwrap()
            .nxt
            .is_some()
    );
    for body in [
        r#"{}"#,
        r#"{"sid":null}"#,
        r#"{"sid":1}"#,
        r#"{"sid":"","ack":null}"#,
        r#"{"sid":"","nxt":null}"#,
        r#"{"sid":"","values":null}"#,
        r#"{"sid":"","values":[1]}"#,
        r#"{"sid":"","sid":""}"#,
        r#"{"sid":"","ack":"","ack":""}"#,
        r#"{"sid":"","extra":1}"#,
        r#"{"sid":"AA===","b64":2}"#,
        r#"{"sid":"AB","b64":2}"#,
        r#"{"sid":"@w","b64":2}"#,
        r#"{"sid":"a","b64":2}"#,
        r#"{"sid":""} {}"#,
    ] {
        assert!(wire::decode_request(body.as_bytes()).is_err(), "{body}");
    }
    for body in [
        br#"{"sid":"","values":["AA"]}"#.as_slice(),
        br#"{"sid":"","ack":"","nxt":""}"#,
    ] {
        assert!(wire::decode_reply(body).is_err());
    }
    for path in [
        "/tryswap",
        "/plyswap/v1/AA",
        "/v1/plyswap/",
        "/v1/plyswap/AA==",
        "/v1/plyswap/AB",
        "/v1/plyswap/AA?x=1",
        "/v1/plyswap/AA/",
        "/v1/plyswap/%41A",
    ] {
        assert!(!wire::is_route(path), "{path}");
    }
}

#[test]
fn real_http_duplex_binary_payload_and_completed_empty() {
    let (origin, worker) = serve(13);
    let url = endpoint(&origin);
    let (mut a, mut b) = (client(&url, b"a", b"binary"), client(&url, b"b", b"binary"));
    open(&mut a, &mut b);
    a.enqueue((0..=255).collect());
    a.enqueue(vec![]);
    b.enqueue(vec![255, 0]);
    a.step(Mode::Current).unwrap();
    b.step(Mode::Current).unwrap();
    a.step(Mode::Current).unwrap();
    assert_eq!(b.core().received(), [(0..=255).collect::<Vec<_>>(), vec![]]);
    assert_eq!(a.core().received(), [vec![255, 0]]);
    assert_eq!((a.core().generation(), b.core().generation()), (1, 1));
    let url = endpoint(&origin);
    let (mut a, mut b) = (client(&url, b"a", b"mixed"), client(&url, b"b", b"mixed"));
    open(&mut a, &mut b);
    a.enqueue(b"data".to_vec());
    a.step(Mode::Current).unwrap();
    b.step(Mode::Current).unwrap();
    b.step(Mode::Previous).unwrap();
    a.step(Mode::Current).unwrap();
    assert_eq!(b.core().received(), [b"data".to_vec()]);
    assert!(a.core().received().is_empty());
    assert_eq!(a.core().generation(), 1);
    let empty = b.core().previous().unwrap();
    assert!(empty.values.is_empty() && empty.ack.is_some() && empty.nxt.is_some());
    assert_eq!(worker.join().unwrap().room_count(), 2);
}

#[test]
fn rooms_isolate_discovery_payload_and_reference_cache() {
    let (origin, worker) = serve(10);
    let url1 = endpoint(&origin);
    let url2 = url1.clone();
    let (mut a, mut b) = (client(&url1, b"a", b"one"), client(&url1, b"b", b"one"));
    open(&mut a, &mut b);
    a.enqueue(b"secret".to_vec());
    b.enqueue(b"answer".to_vec());
    a.step(Mode::Current).unwrap();
    b.step(Mode::Current).unwrap();
    a.step(Mode::Current).unwrap();
    let p = a.core().previous().unwrap();
    let reference = Request {
        sid: p.sid.clone(),
        nxt: p.nxt.clone(),
        ..Request::default()
    };
    assert_eq!(
        HttpTransport::new(&url2, b"two".to_vec(), WAIT)
            .unwrap()
            .exchange(&reference)
            .unwrap(),
        Outcome::Reject
    );
    let mut c = client(&url2, b"a", b"two");
    let opening = c.step(Mode::Current).unwrap().unwrap();
    assert!(matches!(
        opening,
        Outcome::Ok(Reply {
            ack: None,
            nxt: None,
            ..
        })
    ));
    let mut d = client(&url2, b"b", b"two");
    d.step(Mode::Current).unwrap();
    c.step(Mode::Current).unwrap();
    assert!(c.core().received().is_empty() && d.core().received().is_empty());
    worker.join().unwrap();
}

fn post(url: &str, body: &[u8], content_type: &str) -> u16 {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .proxy(None)
        .timeout_global(Some(WAIT))
        .build()
        .into();
    agent
        .post(url)
        .header("Content-Type", content_type)
        .send(body)
        .unwrap()
        .status()
        .as_u16()
}

#[test]
fn admission_errors_do_not_create_rooms() {
    let (origin, worker) = serve(9);
    let url = endpoint(&origin);
    assert_eq!(
        post(
            &format!("{origin}/tryswap"),
            br#"{"sid":""}"#,
            "application/json"
        ),
        404
    );
    assert_eq!(
        post(
            &format!("{origin}/plyswap/v1/cm9vbQ"),
            br#"{"sid":""}"#,
            "application/json"
        ),
        404
    );
    assert_eq!(
        post(&format!("{url}?x=1"), br#"{"sid":""}"#, "application/json"),
        400
    );
    assert_eq!(post(&url, b"not JSON", "application/json"), 400);
    assert_eq!(
        post(&url, br#"{"sid":"","oops":1}"#, "application/json"),
        400
    );
    assert_eq!(post(&url, br#"{"sid":""}"#, "text/plain"), 415);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .proxy(None)
        .timeout_global(Some(WAIT))
        .build()
        .into();
    assert_eq!(agent.get(&url).call().unwrap().status().as_u16(), 405);
    // Reject oversized declared lengths before waiting for a body. Sending a
    // full upload here can race early socket closure and produce BrokenPipe.
    assert_eq!(declared_post(&url, wire::REQUEST_LIMIT + 1), 413);
    // The adapter deliberately rejects streaming/chunked request framing.
    let host = origin.strip_prefix("http://").unwrap();
    let mut stream = TcpStream::connect(host).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    write!(stream, "POST {} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\n{{}}\r\n0\r\n\r\n", url.strip_prefix(&origin).unwrap()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 411"), "{response}");
    assert_eq!(worker.join().unwrap().room_count(), 0);
}

fn fake(
    count: usize,
    handler: impl Fn(usize, tiny_http::Request) + Send + 'static,
) -> (String, thread::JoinHandle<()>) {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let url = endpoint(&format!("http://{}", server.server_addr()));
    let worker = thread::spawn(move || {
        for i in 0..count {
            let request = server
                .recv_timeout(WAIT)
                .unwrap()
                .expect("missing HTTP call");
            handler(i, request);
        }
        // An adapter must not invent another HTTP attempt.
        assert!(
            server
                .recv_timeout(Duration::from_millis(30))
                .unwrap()
                .is_none()
        );
    });
    (url, worker)
}
fn read_request(r: &mut tiny_http::Request) -> Request {
    let mut data = vec![];
    r.as_reader().read_to_end(&mut data).unwrap();
    wire::decode_envelope(&data, None).unwrap().request
}
fn respond(r: tiny_http::Request, status: u16, body: &[u8]) {
    let _ = r.respond(
        Response::from_data(body)
            .with_status_code(StatusCode(status))
            .with_header(Header::from_bytes("Content-Type", "application/json").unwrap()),
    );
}
fn opening(request: &Request) -> Vec<u8> {
    wire::encode_reply(&Reply {
        sid: request.sid.clone(),
        ack: Some(b"peer".to_vec()),
        ..Reply::default()
    })
}

#[test]
fn uncertain_http_errors_preserve_reference_hint_and_retry_storage() {
    let cases: Vec<(u16, Vec<u8>)> = vec![
        (404, vec![]),
        (500, vec![]),
        (302, vec![]),
        (409, b"proxy conflict".to_vec()),
        (200, b"not JSON".to_vec()),
        (200, br#"{"sid":"d3Jvbmc"}"#.to_vec()),
        (200, br#"{"sid":"","ack":"","nxt":""}"#.to_vec()),
        (200, vec![b' '; wire::RESPONSE_LIMIT + 1]),
    ];
    for (status, bad_body) in cases {
        let (url, worker) = fake(4, move |i, mut raw| {
            let r = read_request(&mut raw);
            if i == 0 {
                respond(raw, 200, &opening(&r));
            } else if i == 2 {
                assert!(r.is_reference());
                respond(raw, status, &bad_body);
            } else {
                assert_eq!(r.is_reference(), i == 3);
                assert_eq!(r.values.is_empty(), i == 3);
                respond(
                    raw,
                    200,
                    &wire::encode_reply(&Reply {
                        sid: r.sid,
                        ..Reply::default()
                    }),
                );
            }
        });
        let mut c = client(&url, b"local", b"test");
        c.enqueue(b"retained".to_vec());
        c.step(Mode::Current).unwrap();
        c.step(Mode::Current).unwrap();
        let stored = c.core().stored();
        assert!(c.step(Mode::Current).is_err(), "HTTP {status}");
        assert!(c.core().active().is_none());
        assert_eq!(c.core().stored(), stored);
        c.step(Mode::Current).unwrap(); // Still a reference, not a fabricated MISS.
        worker.join().unwrap();
    }
}

#[test]
fn only_documented_protocol_rejection_clears_hint() {
    let (url, worker) = fake(4, |i, mut raw| {
        let r = read_request(&mut raw);
        match i {
            0 => respond(raw, 200, &opening(&r)),
            2 => {
                assert!(r.is_reference());
                respond(raw, 409, wire::REJECTION);
            }
            _ => {
                assert_eq!(r.values, [b"payload".to_vec()]);
                assert!(r.ack.is_some() && r.nxt.is_some());
                respond(
                    raw,
                    200,
                    &wire::encode_reply(&Reply {
                        sid: r.sid,
                        ..Reply::default()
                    }),
                );
            }
        }
    });
    let mut c = client(&url, b"local", b"test");
    c.enqueue(b"payload".to_vec());
    c.step(Mode::Current).unwrap();
    c.step(Mode::Current).unwrap();
    assert_eq!(c.step(Mode::Current).unwrap(), Some(Outcome::Reject));
    c.step(Mode::Current).unwrap();
    worker.join().unwrap();
}

#[test]
fn response_loss_after_processing_keeps_full_retry_and_no_auto_replay() {
    let db = std::sync::Mutex::new(Server::default());
    let (url, worker) = fake(3, move |i, mut raw| {
        let r = read_request(&mut raw);
        if i == 0 {
            respond(raw, 200, &opening(&r));
            return;
        }
        assert_eq!(r.values, [b"payload".to_vec()]);
        assert!(r.ack.is_some() && r.nxt.is_some());
        let result = db.lock().unwrap().serve(&r);
        let Outcome::Ok(reply) = result else {
            panic!("not processed")
        };
        if i == 1 {
            // Advertise a response, but close with a truncated body after mutation.
            let mut writer = raw.into_writer();
            writer.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 999\r\nConnection: close\r\n\r\n{").unwrap();
            writer.flush().unwrap();
        } else {
            respond(raw, 200, &wire::encode_reply(&reply));
        }
    });
    let mut c = HttpClient::new(
        b"local".to_vec(),
        &url,
        b"test".to_vec(),
        Duration::from_millis(200),
    )
    .unwrap();
    c.enqueue(b"payload".to_vec());
    c.step(Mode::Current).unwrap();
    assert!(c.step(Mode::Current).is_err());
    assert!(c.core().active().is_none());
    assert_eq!(c.core().stored(), [b"payload".to_vec()]);
    c.step(Mode::Current).unwrap(); // Must remain FULL because the upload OK was lost.
    assert!(c.core().received().is_empty());
    worker.join().unwrap();
}

#[test]
fn response_body_deadline_bounds_wait_without_retry() {
    let (release, held) = mpsc::channel();
    let (url, worker) = fake(1, move |_, mut raw| {
        let _r = read_request(&mut raw);
        let mut writer = raw.into_writer();
        writer.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").unwrap();
        writer.flush().unwrap();
        held.recv_timeout(WAIT).unwrap(); // Keep body open until the client times out.
    });
    let mut c = HttpClient::new(
        b"local".to_vec(),
        &url,
        b"test".to_vec(),
        Duration::from_millis(100),
    )
    .unwrap();
    c.enqueue(b"unsent-body".to_vec());
    let start = Instant::now();
    assert!(c.step(Mode::Current).is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(c.core().active().is_none());
    assert_eq!(c.core().stored(), [b"unsent-body".to_vec()]);
    release.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
fn oversize_frozen_batch_is_not_sent_or_discarded() {
    let (url, worker) = fake(1, |_, mut raw| {
        let r = read_request(&mut raw);
        respond(raw, 200, &opening(&r));
    });
    let mut c = client(&url, b"local", b"test");
    c.step(Mode::Current).unwrap();
    let payload = vec![255; wire::REQUEST_LIMIT];
    c.enqueue(payload.clone());
    assert!(matches!(
        c.step(Mode::Current),
        Err(rendezqueue::http::ExchangeError::Transport(
            rendezqueue::http::TransportError::RequestTooLarge
        ))
    ));
    assert_eq!(c.core().batch(), Some([payload.clone()].as_slice()));
    assert_eq!(c.core().stored(), [payload]);
    assert!(c.core().active().is_none());
    worker.join().unwrap();
}

#[test]
fn redirects_and_non_json_or_encoded_responses_are_not_accepted() {
    let destination = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let location = endpoint(&format!("http://{}", destination.server_addr()));
    let (url, worker) = fake(3, move |i, mut raw| {
        let r = read_request(&mut raw);
        let response = Response::from_data(wire::encode_reply(&Reply {
            sid: r.sid,
            ..Reply::default()
        }));
        let response = match i {
            0 => response
                .with_status_code(StatusCode(307))
                .with_header(Header::from_bytes("Location", location.as_bytes()).unwrap()),
            1 => response.with_header(Header::from_bytes("Content-Type", "text/plain").unwrap()),
            _ => response
                .with_header(Header::from_bytes("Content-Type", "application/json").unwrap())
                .with_header(Header::from_bytes("Content-Encoding", "gzip").unwrap()),
        };
        raw.respond(response).unwrap();
    });
    let transport = HttpTransport::new(&url, b"test".to_vec(), WAIT).unwrap();
    for _ in 0..3 {
        assert!(transport.exchange(&Request::default()).is_err());
    }
    assert!(
        destination
            .recv_timeout(Duration::from_millis(30))
            .unwrap()
            .is_none()
    );
    worker.join().unwrap();
}

#[test]
fn positional_arrays_are_not_packets_and_cannot_create_rooms() {
    let array = br#"["YQ","Yg",["aGk"],"YTE"]"#;
    assert!(wire::decode_request(array).is_err());
    assert!(wire::decode_reply(br#"["YQ"]"#).is_err());
    // Leading JSON whitespace remains legal for an actual object.
    assert!(wire::decode_request(b" \r\n\t{\"sid\":\"YQ\"}").is_ok());
    let (origin, worker) = serve(1);
    assert_eq!(post(&endpoint(&origin), array, "application/json"), 400);
    assert_eq!(worker.join().unwrap().room_count(), 0);
}

#[test]
fn truncated_content_length_body_cannot_create_protocol_state() {
    let (origin, worker) = serve(1);
    let url = endpoint(&origin);
    let host = origin.strip_prefix("http://").unwrap();
    let mut stream = TcpStream::connect(host).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    // tiny_http prebuffers <=1024 bytes; exercise its larger streaming reader.
    write!(stream, "POST {} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: 2000\r\nConnection: close\r\n\r\n", url.strip_prefix(&origin).unwrap()).unwrap();
    stream
        .write_all(br#"{"key":"truncated","sid":"a","ack":"b","values":["hi"],"nxt":"a1"}"#)
        .unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert_eq!(worker.join().unwrap().room_count(), 0);
}

#[test]
fn incomplete_chunked_body_is_rejected_without_mutation() {
    let (origin, worker) = serve(1);
    let url = endpoint(&origin);
    let host = origin.strip_prefix("http://").unwrap();
    let mut stream = TcpStream::connect(host).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    write!(stream, "POST {} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1000\r\n", url.strip_prefix(&origin).unwrap()).unwrap();
    stream
        .write_all(br#"{"key":"truncated","sid":"a","ack":"b","values":["hi"],"nxt":"a1"}"#)
        .unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 411"), "{response}");
    assert_eq!(worker.join().unwrap().room_count(), 0);
}

#[test]
fn endpoint_configuration_and_disabled_send_are_explicit() {
    for url in [
        "/v1/plyswap",
        "http://example.test/v1/plyswap/AA?x=1",
        "",
        "ftp://example.test/v1/plyswap/AA",
    ] {
        assert!(
            HttpTransport::new(url, b"test".to_vec(), WAIT).is_err(),
            "{url}"
        );
    }
    assert!(
        HttpTransport::new(
            "http://example.test/v1/plyswap",
            b"test".to_vec(),
            Duration::ZERO
        )
        .is_err()
    );
    assert!(HttpTransport::new("http://example.test/v1/plyswap", vec![], WAIT).is_err());
    // Disabled previous cannot make any connection.
    let mut c = client("http://127.0.0.1:1/v1/plyswap", b"local", b"test");
    assert_eq!(c.step(Mode::Previous).unwrap(), None);
    assert!(c.step(Mode::Current).is_err());
    assert!(c.core().active().is_none());
    // Transport-free core remains independently usable.
    assert!(Client::new(vec![]).send(Mode::Current).unwrap().is_some());
}

fn declared_post(url: &str, length: usize) -> u16 {
    let target = url.strip_prefix("http://").unwrap();
    let (host, path) = target.split_once('/').unwrap();
    let mut stream = TcpStream::connect(host).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    write!(stream, "POST /{path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response.split_whitespace().nth(1).unwrap().parse().unwrap()
}

fn json_exchange(url: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .proxy(None)
        .timeout_global(Some(WAIT))
        .build()
        .into();
    let mut response = agent
        .post(url)
        .header("Content-Type", "application/json")
        .send(body)
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response
        .body_mut()
        .with_config()
        .limit(wire::RESPONSE_LIMIT as u64)
        .read_to_vec()
        .unwrap();
    (status, bytes)
}

#[test]
fn json_and_query_key_encodings_share_immutable_snapshots() {
    let (origin, worker) = serve(4);
    let url = endpoint(&origin);
    let full = Request {
        sid: b"a".to_vec(),
        ack: Some(b"b".to_vec()),
        values: vec![vec![0, 255], vec![]],
        nxt: Some(b"a1".to_vec()),
    };
    let (status, body) = json_exchange(&url, &wire::encode_envelope(&full, &[255]));
    assert_eq!(status, 200);
    assert_eq!(wire::decode_reply(&body).unwrap().sid, b"a");
    let raw = serde_json::json!({"key":"ÿ","sid":"a","ack":"b","values":["\0ÿ",""],"nxt":"a1"});
    assert_eq!(
        json_exchange(&url, &serde_json::to_vec(&raw).unwrap()).0,
        200
    );
    let (status, body) = json_exchange(
        &format!("{url}?key=%C3%BF"),
        br#"{"sid":"b","ack":"a","nxt":"b1"}"#,
    );
    assert_eq!(status, 200);
    assert_eq!(
        wire::decode_reply(&body).unwrap(),
        Reply {
            sid: b"b".to_vec(),
            values: full.values.clone(),
            nxt: Some(b"a1".to_vec()),
            ..Reply::default()
        }
    );
    let raw_reply: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(raw_reply.get("b64").is_none());
    let (status, body) = json_exchange(
        &format!("{url}?key=_w"),
        br#"{"sid":"Yg","ack":"YQ","nxt":"YjE","b64":6}"#,
    );
    assert_eq!(status, 200);
    let mixed_reply: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(mixed_reply["b64"], 2);
    assert_eq!(mixed_reply["values"], raw_reply["values"]);
    assert_eq!(worker.join().unwrap().room_count(), 1);
}

#[test]
fn key_admission_errors_and_query_size_do_not_create_rooms() {
    let cases: &[(&str, &[u8])] = &[
        ("", br#"{"sid":"a"}"#),
        ("", br#"{"key":"","sid":"a"}"#),
        ("", br#"{"key":null,"sid":"a"}"#),
        ("", br#"{"key":"x","key":"x","sid":"a"}"#),
        ("?key=room", br#"{"key":"room","sid":"a"}"#),
        ("?key=", br#"{"sid":"a"}"#),
        ("?key=x&key=x", br#"{"sid":"a"}"#),
        ("?key=x&other=y", br#"{"sid":"a"}"#),
        ("?other=x", br#"{"sid":"a"}"#),
        ("?key=%FF", br#"{"sid":"a"}"#),
        ("?key=%ZZ", br#"{"sid":"a"}"#),
        ("?key=%", br#"{"sid":"a"}"#),
        ("?key=%C4%80", br#"{"sid":"a"}"#),
        ("", br#"{"key":"room","sid":"a","b64":true}"#),
        ("", br#"{"key":"room","sid":"a","b64":8}"#),
    ];
    let (origin, worker) = serve(cases.len() + 1);
    let url = endpoint(&origin);
    for (query, body) in cases {
        assert_eq!(
            post(&format!("{url}{query}"), body, "application/json"),
            400,
            "{query}"
        );
    }
    assert_eq!(
        declared_post(&format!("{url}?key=room"), wire::REQUEST_LIMIT),
        413
    );
    assert_eq!(worker.join().unwrap().room_count(), 0);
}

#[test]
fn raw_reply_expansion_exceeds_old_limit_but_preserves_binary_data() {
    let (origin, worker) = serve(2);
    let url = endpoint(&origin);
    let payload = vec![0; 400_000];
    let full = Request {
        sid: b"a".to_vec(),
        ack: Some(b"b".to_vec()),
        values: vec![payload.clone()],
        nxt: Some(b"a1".to_vec()),
    };
    assert_eq!(
        json_exchange(&url, &wire::encode_envelope(&full, b"room")).0,
        200
    );
    let (status, body) = json_exchange(&url, br#"{"key":"room","sid":"b","ack":"a","nxt":"b1"}"#);
    assert_eq!(status, 200);
    assert!(body.len() > 2 * wire::REQUEST_LIMIT + 1024);
    assert!(body.len() < wire::RESPONSE_LIMIT);
    assert_eq!(wire::decode_reply(&body).unwrap().values, [payload]);
    assert_eq!(worker.join().unwrap().room_count(), 1);
}

#[test]
fn retained_transport_accepts_a_large_valid_unflagged_reply() {
    let payload = vec![0; 400_000];
    let expected = payload.clone();
    let (url, worker) = fake(1, move |_, mut raw| {
        let request = read_request(&mut raw);
        let body = wire::encode_reply(&Reply {
            sid: request.sid,
            values: vec![payload.clone()],
            nxt: Some(b"next".to_vec()),
            ..Reply::default()
        });
        assert!(body.len() > 2 * wire::REQUEST_LIMIT + 1024);
        respond(raw, 200, &body);
    });
    let transport = HttpTransport::new(&url, b"room".to_vec(), WAIT).unwrap();
    let outcome = transport.exchange(&Request::default()).unwrap();
    assert!(matches!(outcome, Outcome::Ok(reply) if reply.values == [expected]));
    worker.join().unwrap();
}
