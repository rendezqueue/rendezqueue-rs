use rendezqueue::{Request, wire};

#[test]
fn every_bitmap_preserves_all_bytes_and_presence() {
    let request = Request {
        sid: (0..=255).collect(),
        ack: Some(vec![]),
        values: vec![vec![], (0..=255).rev().collect()],
        nxt: Some(vec![0, 255]),
    };
    for flags in 0..=7 {
        let bytes = wire::encode_request_with_flags(&request, flags);
        assert_eq!(wire::decode_request(&bytes).unwrap(), request);
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json.get("b64").and_then(|v| v.as_u64()).unwrap_or(0),
            flags as u64
        );
        assert_eq!(
            json["sid"],
            if flags & 2 != 0 {
                wire::encode_bytes(&request.sid)
            } else {
                (0..=255).map(char::from).collect::<String>()
            }
        );
    }
    let raw =
        wire::decode_request(br#"{"sid":"YQ","ack":"b","values":["hello"],"nxt":"c"}"#).unwrap();
    assert_eq!(raw.sid, b"YQ"); // Never guess that a plain string is encoded.
    assert_eq!(raw.values, [b"hello".to_vec()]);
    let encoded =
        wire::decode_request(br#"{"sid":"YQ","ack":"Yg","values":["aGk"],"nxt":"Yw","b64":3}"#)
            .unwrap();
    assert_eq!(encoded.sid, b"a");
    assert_eq!(encoded.ack, Some(b"b".to_vec()));
    assert_eq!(encoded.nxt, Some(b"c".to_vec()));
    assert_eq!(encoded.values, [b"hi".to_vec()]);
}

#[test]
fn base64_accepts_valid_alphabets_and_padding_only_when_flagged() {
    for value in ["/w==", "/w", "_w==", "_w"] {
        let body = serde_json::json!({"sid":value,"b64":2});
        assert_eq!(
            wire::decode_request(&serde_json::to_vec(&body).unwrap())
                .unwrap()
                .sid,
            [255]
        );
    }
    for value in ["_x", "_x==", "A", "YQ=", "YQ===", "YQ\n", "Y=Q=", "@w"] {
        assert!(wire::decode_bytes(value).is_err(), "{value}");
    }
    for body in [
        r#"{"sid":"a","b64":true}"#,
        r#"{"sid":"a","b64":-1}"#,
        r#"{"sid":"a","b64":8}"#,
        r#"{"sid":"a","b64":1.0}"#,
        r#"{"sid":"a","b64":null}"#,
        r#"{"sid":"a","b64":0,"b64":0}"#,
        r#"{"sid":"\u0100"}"#,
        r#"{"sid":"\ud800"}"#,
        r#"{"sid":"a","key":"room"}"#,
    ] {
        assert!(wire::decode_request(body.as_bytes()).is_err(), "{body}");
    }
}

#[test]
fn room_selection_is_explicit_and_equivalent_across_encodings() {
    let raw = wire::decode_envelope(br#"{"key":"\u00ff","sid":"a"}"#, None).unwrap();
    let encoded = wire::decode_envelope(br#"{"key":"/w==","sid":"YQ","b64":6}"#, None).unwrap();
    let query = wire::query_key("/v1/plyswap?key=%C3%BF").unwrap();
    let queried = wire::decode_envelope(br#"{"sid":"a"}"#, query.as_deref()).unwrap();
    assert_eq!(raw.key, [255]);
    assert_eq!(raw.key, encoded.key);
    assert_eq!(raw.key, queried.key);
    assert_eq!(raw.request, encoded.request);
    assert_eq!(raw.request, queried.request);
    let encoded_query = wire::query_key("/v1/plyswap?key=%2Fw%3D%3D").unwrap();
    assert_eq!(
        wire::decode_envelope(br#"{"sid":"a","b64":4}"#, encoded_query.as_deref())
            .unwrap()
            .key,
        [255]
    );
    for (body, query) in [
        (r#"{"sid":"a"}"#, None),
        (r#"{"key":"","sid":"a"}"#, None),
        (r#"{"key":null,"sid":"a"}"#, None),
        (r#"{"key":"room","key":"room","sid":"a"}"#, None),
        (r#"{"key":"room","sid":"a"}"#, Some("room")),
        (r#"{"sid":"a"}"#, Some("")),
        (r#"{"key":"room","sid":"a","extra":1}"#, None),
    ] {
        assert!(
            wire::decode_envelope(body.as_bytes(), query).is_err(),
            "{body}"
        );
    }
    assert!(wire::decode_reply(br#"{"key":"room","sid":"a"}"#).is_err());
}

#[test]
fn query_form_decode_is_strict_and_once_only() {
    assert_eq!(
        wire::query_key("/v1/plyswap?key=a+b%2Bc%2520").unwrap(),
        Some("a b+c%20".into())
    );
    for target in [
        "/v1/plyswap?",
        "/v1/plyswap?key",
        "/v1/plyswap?key=x&key=x",
        "/v1/plyswap?key=x&extra=y",
        "/v1/plyswap?extra=x",
        "/v1/plyswap?key=%",
        "/v1/plyswap?key=%0G",
        "/v1/plyswap?key=%FF",
        "/v1/plyswap?key=ÿ",
    ] {
        assert!(wire::query_key(target).is_err(), "{target}");
    }
    for target in [
        "/v1/plyswap/room",
        "/plyswap/v1/room",
        "/v1/plyswap/",
        "/v1/plyswap#fragment",
    ] {
        assert!(!wire::is_route(target));
    }
    assert!(wire::is_route("/v1/plyswap"));
    assert!(wire::is_route("/v1/plyswap?key=room"));
}
