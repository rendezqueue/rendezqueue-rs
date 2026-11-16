//! PlySwap JSON codec. b64 bits: values=1, XID/PEER/NEXT=2, room=4.
use crate::{Bytes, KeyedReply, Reply, Request};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

pub const ROUTE: &str = "/";
pub const REQUEST_LIMIT: usize = 1024 * 1024;
// Own room + XID share one admitted envelope budget; peer_xid fields share another.
// A raw-byte JSON string can expand each byte to six characters (\u0000).
pub const RESPONSE_LIMIT: usize = 12 * REQUEST_LIMIT + 1024;
pub const REJECTION: &[u8] = br#"{"error":"plyswap-rejected"}"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidWire(pub String);
impl fmt::Display for InvalidWire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for InvalidWire {}

fn invalid(message: impl Into<String>) -> InvalidWire {
    InvalidWire(message.into())
}

pub fn encode_bytes(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn decode_bytes(s: &str) -> Result<Bytes, InvalidWire> {
    let normalized = s.replace('+', "-").replace('/', "_");
    let unpadded = normalized.trim_end_matches('=');
    let padding = normalized.len() - unpadded.len();
    if padding > 2
        || (padding != 0 && (normalized.len() % 4 != 0 || padding != (4 - unpadded.len() % 4) % 4))
    {
        return Err(invalid("invalid base64 padding"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(unpadded)
        .map_err(|e| invalid(e.to_string()))?;
    if encode_bytes(&bytes) != unpadded {
        return Err(invalid("nonzero base64 trailing bits"));
    }
    Ok(bytes)
}

fn encode_field(bytes: &[u8], encoded: bool) -> String {
    if encoded {
        encode_bytes(bytes)
    } else {
        bytes.iter().map(|b| char::from(*b)).collect()
    }
}
fn decode_field(s: &str, encoded: bool) -> Result<Bytes, InvalidWire> {
    if encoded {
        decode_bytes(s)
    } else {
        s.chars()
            .map(|c| {
                u8::try_from(u32::from(c)).map_err(|_| invalid("expected Latin-1 byte string"))
            })
            .collect()
    }
}

// Omitted is None; a present field must be a string, never null.
fn present_string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    String::deserialize(d).map(Some)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    #[serde(default, skip_serializing_if = "is_zero")]
    b64: u8,
    #[serde(rename = "self")]
    self_xid: String,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "peer")]
    peer_xid: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "next")]
    next_xid: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<String>,
}
fn is_zero(value: &u8) -> bool {
    *value == 0
}
impl Message {
    fn from_request(r: &Request, flags: u8) -> Self {
        assert!(flags <= 7, "invalid b64 bitmap");
        assert!(!r.self_xid.is_empty(), "self_xid must not be empty");
        Self {
            self_xid: encode_field(&r.self_xid, flags & 2 != 0),
            peer_xid: (!r.peer_xid.is_empty()).then(|| encode_field(&r.peer_xid, flags & 2 != 0)),
            next_xid: (!r.next_xid.is_empty()).then(|| encode_field(&r.next_xid, flags & 2 != 0)),
            values: r
                .values
                .iter()
                .map(|v| encode_field(v, flags & 1 != 0))
                .collect(),

            b64: flags,
        }
    }
    fn into_request(self) -> Result<Request, InvalidWire> {
        if self.b64 > 7 {
            return Err(invalid("invalid b64 bitmap"));
        }
        let self_xid = decode_field(&self.self_xid, self.b64 & 2 != 0)?;
        if self_xid.is_empty() {
            return Err(invalid("self must not be empty"));
        }
        Ok(Request {
            self_xid,
            peer_xid: self
                .peer_xid
                .as_deref()
                .map(|v| decode_field(v, self.b64 & 2 != 0))
                .transpose()?
                .unwrap_or_default(),
            values: self
                .values
                .iter()
                .map(|v| decode_field(v, self.b64 & 1 != 0))
                .collect::<Result<_, _>>()?,
            next_xid: self
                .next_xid
                .as_deref()
                .map(|v| decode_field(v, self.b64 & 2 != 0))
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

fn object(bytes: &[u8]) -> Result<(), InvalidWire> {
    if bytes
        .iter()
        .find(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        != Some(&b'{')
    {
        return Err(invalid("expected JSON object"));
    }
    Ok(())
}

pub fn encode_request(r: &Request) -> Vec<u8> {
    encode_request_with_flags(r, 0)
}
pub fn encode_request_with_flags(r: &Request, flags: u8) -> Vec<u8> {
    serde_json::to_vec(&Message::from_request(r, flags)).expect("string-only JSON")
}
pub fn decode_request(bytes: &[u8]) -> Result<Request, InvalidWire> {
    object(bytes)?;
    serde_json::from_slice::<Message>(bytes)
        .map_err(|e| invalid(e.to_string()))?
        .into_request()
}
pub fn encode_reply(r: &Reply) -> Vec<u8> {
    encode_reply_with_flags(r, 0)
}
pub fn encode_reply_with_flags(r: &Reply, flags: u8) -> Vec<u8> {
    encode_request_with_flags(
        &Request {
            self_xid: r.self_xid.clone(),
            peer_xid: r.peer_xid.clone(),
            next_xid: r.next_xid.clone(),
            values: r.values.clone(),
        },
        flags & 3,
    )
}
pub fn decode_reply(bytes: &[u8]) -> Result<Reply, InvalidWire> {
    let r = decode_request(bytes)?;
    let reply = Reply {
        self_xid: r.self_xid,
        peer_xid: r.peer_xid,
        next_xid: r.next_xid,
        values: r.values,
    };
    if !reply.is_valid() {
        return Err(invalid("invalid reply shape"));
    }
    Ok(reply)
}

// Do not flatten Message here: serde flatten can obscure duplicate fields.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpMessage {
    #[serde(default, skip_serializing_if = "is_zero")]
    b64: u8,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    room: Option<String>,
    #[serde(rename = "self")]
    self_xid: String,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "peer")]
    peer_xid: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "next")]
    next_xid: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<String>,
}

pub struct Envelope {
    pub room: Bytes,
    pub request: Request,
    pub b64: u8,
}

pub fn encode_envelope(r: &Request, room: &[u8]) -> Vec<u8> {
    encode_keyed_message(Message::from_request(r, 7), room)
}
fn encode_keyed_message(m: Message, room: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&HttpMessage {
        room: Some(encode_field(room, m.b64 & 4 != 0)),
        self_xid: m.self_xid,
        peer_xid: m.peer_xid,
        next_xid: m.next_xid,
        values: m.values,
        b64: m.b64,
    })
    .expect("string-only JSON")
}
pub fn decode_envelope(bytes: &[u8], query_room: Option<&str>) -> Result<Envelope, InvalidWire> {
    object(bytes)?;
    let m: HttpMessage = serde_json::from_slice(bytes).map_err(|e| invalid(e.to_string()))?;
    let room = match (m.room.as_deref(), query_room) {
        (Some(room), None) | (None, Some(room)) => decode_field(room, m.b64 & 4 != 0)?,
        _ => return Err(invalid("require room in JSON or query, not both")),
    };
    if room.is_empty() {
        return Err(invalid("room must not be empty"));
    }
    let b64 = m.b64;
    let request = Message {
        self_xid: m.self_xid,
        peer_xid: m.peer_xid,
        next_xid: m.next_xid,
        values: m.values,
        b64,
    }
    .into_request()?;
    Ok(Envelope { room, request, b64 })
}

/// Successful keyed service response; every bitmap bit is preserved.
pub fn encode_response(response: &KeyedReply, flags: u8) -> Vec<u8> {
    let r = &response.reply;
    encode_keyed_message(
        Message::from_request(
            &Request {
                self_xid: r.self_xid.clone(),
                peer_xid: r.peer_xid.clone(),
                next_xid: r.next_xid.clone(),
                values: r.values.clone(),
            },
            flags,
        ),
        &response.room,
    )
}
pub fn decode_response(bytes: &[u8]) -> Result<KeyedReply, InvalidWire> {
    let envelope = decode_envelope(bytes, None)?;
    let r = envelope.request;
    let reply = Reply {
        self_xid: r.self_xid,
        peer_xid: r.peer_xid,
        next_xid: r.next_xid,
        values: r.values,
    };
    if !reply.is_valid() {
        return Err(invalid("invalid reply shape"));
    }
    Ok(KeyedReply {
        room: envelope.room,
        reply,
    })
}

/// Exact endpoint; the optional query is admitted separately, never a path room.
pub fn is_route(target: &str) -> bool {
    target.split('?').next() == Some(ROUTE) && !target.contains('#')
}

fn form_decode(value: &str) -> Result<String, InvalidWire> {
    let mut result = Vec::new();
    let mut input = value.bytes();
    while let Some(byte) = input.next() {
        match byte {
            b'+' => result.push(b' '),
            b'%' => {
                let a = input.next().and_then(|b| char::from(b).to_digit(16));
                let b = input.next().and_then(|b| char::from(b).to_digit(16));
                match (a, b) {
                    (Some(a), Some(b)) => result.push((a * 16 + b) as u8),
                    _ => return Err(invalid("invalid percent escape")),
                }
            }
            _ if byte.is_ascii() => result.push(byte),
            _ => return Err(invalid("query target must be ASCII")),
        }
    }
    String::from_utf8(result).map_err(|e| invalid(e.to_string()))
}

pub fn query_room(target: &str) -> Result<Option<String>, InvalidWire> {
    let Some((_, query)) = target.split_once('?') else {
        return Ok(None);
    };
    if query.is_empty() || query.contains('&') {
        return Err(invalid("expected one query room"));
    }
    let (name, value) = query
        .split_once('=')
        .ok_or_else(|| invalid("expected query room"))?;
    if form_decode(name)? != "room" {
        return Err(invalid("unknown query parameter"));
    }
    Ok(Some(form_decode(value)?))
}
