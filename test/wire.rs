use rendezqueue::{KeyedReply, Reply, Request, wire};

#[test]
fn emitted_fields_follow_the_readable_protocol_order() {
    let request = Request {
        self_xid: b"a".to_vec(),
        peer_xid: b"b".to_vec(),
        values: vec![b"hi".to_vec()],
        next_xid: b"a1".to_vec(),
    };
    assert_eq!(
        wire::encode_request(&request),
        br#"{"self":"a","peer":"b","next":"a1","values":["hi"]}"#
    );
    assert_eq!(
        wire::encode_envelope(&request, b"room"),
        br#"{"b64":7,"room":"cm9vbQ","self":"YQ","peer":"Yg","next":"YTE","values":["aGk"]}"#
    );
    assert_eq!(
        wire::encode_reply(&Reply {
            self_xid: b"a".to_vec(),
            values: vec![b"hi".to_vec()],
            next_xid: b"b1".to_vec(),
            ..Reply::default()
        }),
        br#"{"self":"a","next":"b1","values":["hi"]}"#
    );
    assert_eq!(
        wire::encode_reply_with_flags(
            &Reply {
                self_xid: b"a".to_vec(),
                next_xid: b"b1".to_vec(),
                values: vec![b"hi".to_vec()],
                ..Reply::default()
            },
            3
        ),
        br#"{"b64":3,"self":"YQ","next":"YjE","values":["aGk"]}"#
    );
}

#[test]
fn every_bitmap_preserves_all_bytes_and_presence() {
    let request = Request {
        self_xid: (0..=255).collect(),
        peer_xid: vec![],
        values: vec![vec![], (0..=255).rev().collect()],
        next_xid: vec![0, 255],
    };
    for flags in 0..=7 {
        let bytes = wire::encode_request_with_flags(&request, flags);
        if flags != 0 {
            assert!(bytes.starts_with(br#"{"b64":"#));
        }
        assert_eq!(wire::decode_request(&bytes).unwrap(), request);
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json.get("b64").and_then(|v| v.as_u64()).unwrap_or(0),
            flags as u64
        );
        assert!(json.get("peer").is_none());
        let mut explicit_empty = json.clone();
        explicit_empty["peer"] = "".into();
        assert_eq!(
            wire::decode_request(&serde_json::to_vec(&explicit_empty).unwrap()).unwrap(),
            request
        );
        assert_eq!(
            json["self"],
            if flags & 2 != 0 {
                wire::encode_bytes(&request.self_xid)
            } else {
                (0..=255).map(char::from).collect::<String>()
            }
        );
    }
    let raw =
        wire::decode_request(br#"{"self":"YQ","peer":"b","values":["hello"],"next":"c"}"#).unwrap();
    assert_eq!(raw.self_xid, b"YQ"); // Never guess that a plain string is encoded.
    assert_eq!(raw.values, [b"hello".to_vec()]);
    let encoded =
        wire::decode_request(br#"{"self":"YQ","peer":"Yg","values":["aGk"],"next":"Yw","b64":3}"#)
            .unwrap();
    assert_eq!(encoded.self_xid, b"a");
    assert_eq!(encoded.peer_xid, b"b");
    assert_eq!(encoded.next_xid, b"c");
    assert_eq!(encoded.values, [b"hi".to_vec()]);
}

#[test]
fn base64_accepts_valid_alphabets_and_padding_only_when_flagged() {
    for value in ["/w==", "/w", "_w==", "_w"] {
        let body = serde_json::json!({"self":value,"b64":2});
        assert_eq!(
            wire::decode_request(&serde_json::to_vec(&body).unwrap())
                .unwrap()
                .self_xid,
            [255]
        );
    }
    for value in ["_x", "_x==", "A", "YQ=", "YQ===", "YQ\n", "Y=Q=", "@w"] {
        assert!(wire::decode_bytes(value).is_err(), "{value}");
    }
    for body in [
        r#"{"self":"a","b64":true}"#,
        r#"{"self":"a","b64":-1}"#,
        r#"{"self":"a","b64":8}"#,
        r#"{"self":"a","b64":1.0}"#,
        r#"{"self":"a","b64":null}"#,
        r#"{"self":"a","b64":0,"b64":0}"#,
        r#"{"self":"\u0100"}"#,
        r#"{"self":"\ud800"}"#,
        r#"{"self":"a","room":"room"}"#,
    ] {
        assert!(wire::decode_request(body.as_bytes()).is_err(), "{body}");
    }
}

#[test]
fn room_selection_is_explicit_and_equivalent_across_encodings() {
    let raw = wire::decode_envelope(br#"{"room":"\u00ff","self":"a"}"#, None).unwrap();
    let encoded = wire::decode_envelope(br#"{"room":"/w==","self":"YQ","b64":6}"#, None).unwrap();
    let query = wire::query_room("/?room=%C3%BF").unwrap();
    let queried = wire::decode_envelope(br#"{"self":"a"}"#, query.as_deref()).unwrap();
    assert_eq!(raw.room, [255]);
    assert_eq!(raw.room, encoded.room);
    assert_eq!(raw.room, queried.room);
    assert_eq!(raw.request, encoded.request);
    assert_eq!(raw.request, queried.request);
    let encoded_query = wire::query_room("/?room=%2Fw%3D%3D").unwrap();
    assert_eq!(
        wire::decode_envelope(br#"{"self":"a","b64":4}"#, encoded_query.as_deref())
            .unwrap()
            .room,
        [255]
    );
    for (body, query) in [
        (r#"{"self":"a"}"#, None),
        (r#"{"room":"","self":"a"}"#, None),
        (r#"{"room":null,"self":"a"}"#, None),
        (r#"{"room":"room","room":"room","self":"a"}"#, None),
        (r#"{"room":"room","self":"a"}"#, Some("room")),
        (r#"{"self":"a"}"#, Some("")),
        (r#"{"room":"room","self":"a","extra":1}"#, None),
    ] {
        assert!(
            wire::decode_envelope(body.as_bytes(), query).is_err(),
            "{body}"
        );
    }
    assert!(wire::decode_reply(br#"{"room":"room","self":"a"}"#).is_err());
}

#[test]
fn keyed_responses_echo_all_bitmap_bits_and_binary_keys() {
    let response = KeyedReply {
        room: (0..=255).collect(),
        reply: Reply {
            self_xid: (0..=255).rev().collect(),
            next_xid: vec![0, 255],
            values: vec![vec![], (0..=255).collect()],
            ..Reply::default()
        },
    };
    for flags in 0..=7 {
        let bytes = wire::encode_response(&response, flags);
        assert_eq!(wire::decode_response(&bytes).unwrap(), response);
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json.get("b64").and_then(|v| v.as_u64()).unwrap_or(0),
            flags as u64
        );
        assert_eq!(
            json["room"],
            if flags & 4 != 0 {
                wire::encode_bytes(&response.room)
            } else {
                response
                    .room
                    .iter()
                    .map(|b| char::from(*b))
                    .collect::<String>()
            }
        );
        assert!(bytes.starts_with(if flags == 0 {
            br#"{"room":"#
        } else {
            br#"{"b64":"#
        }));
    }
}

#[test]
fn response_keys_are_required_and_reply_shapes_are_checked() {
    for body in [
        r#"{"self":"a"}"#,
        r#"{"room":"","self":"a"}"#,
        r#"{"room":null,"self":"a"}"#,
        r#"{"room":1,"self":"a"}"#,
        r#"{"room":"room","room":"room","self":"a"}"#,
        r#"{"room":"!","self":"a","b64":4}"#,
        r#"{"room":"room","self":"a","b64":true}"#,
        r#"{"room":"room","self":"a","b64":8}"#,
        r#"{"room":"room","self":"a","peer":"b","next":"c"}"#,
        r#"{"room":"room","self":"a","values":["unsealed"]}"#,
        r#"{"room":"room","self":"a","extra":1}"#,
    ] {
        assert!(wire::decode_response(body.as_bytes()).is_err(), "{body}");
    }
}

#[test]
fn query_form_decode_is_strict_and_once_only() {
    assert_eq!(
        wire::query_room("/?room=a+b%2Bc%2520").unwrap(),
        Some("a b+c%20".into())
    );
    for target in [
        "/?",
        "/?room",
        "/?room=x&room=x",
        "/?room=x&extra=y",
        "/?extra=x",
        "/?room=%",
        "/?room=%0G",
        "/?room=%FF",
        "/?room=ÿ",
    ] {
        assert!(wire::query_room(target).is_err(), "{target}");
    }
    for target in [
        "/plyswap",
        "/plyswap/room",
        "/plyswap/",
        "/plyswap#fragment",
    ] {
        assert!(!wire::is_route(target));
    }
    assert!(wire::is_route("/"));
    assert!(wire::is_route("/?room=room"));
}
