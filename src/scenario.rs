//! Text-only SxPB server scenarios. This is a test format, not a network codec.
use crate::{Bytes, Outcome, Reply, Request, Server};
use sxpb::{Mesg, Value};

fn message(value: &Value) -> Result<&Mesg, String> {
    value.as_mesg().ok_or_else(|| "expected message".into())
}

fn fields(m: &Mesg, allowed: &[&str]) -> Result<(), String> {
    for f in m {
        if !allowed.contains(&f.name.as_str()) {
            return Err(format!("unknown field: {}", f.name));
        }
    }
    Ok(())
}

fn bytes(value: &Value) -> Result<Bytes, String> {
    value
        .as_str()
        .map(|s| s.as_bytes().to_vec())
        .ok_or_else(|| "expected string".into())
}

fn packet(value: &Value) -> Result<Request, String> {
    let m = message(value)?;
    fields(m, &["sid", "ack", "values", "nxt"])?;
    let sid = bytes(m.get("sid").ok_or("missing sid")?)?;
    let ack = m.get("ack").map(bytes).transpose()?;
    let nxt = m.get("nxt").map(bytes).transpose()?;
    let values = m
        .get("values")
        .map(|v| {
            v.as_array()
                .ok_or_else(|| "expected values list".to_string())?
                .iter()
                .map(bytes)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(Request {
        sid,
        ack,
        values,
        nxt,
    })
}

fn step(server: &mut Server, value: &Value) -> Result<(), String> {
    let m = message(value)?;
    fields(
        m,
        &["req", "res", "reject", "reset", "forget", "evict_opening"],
    )?;
    let operations = ["req", "reset", "forget", "evict_opening"]
        .iter()
        .filter(|key| m.get(key).is_some())
        .count();
    if operations != 1 {
        return Err("expected exactly one operation".into());
    }
    if let Some(req) = m.get("req") {
        if m.get("res").is_some() == m.get("reject").is_some() {
            return Err("request needs exactly one of res or reject".into());
        }
        let req = packet(req)?;
        let expected = if let Some(res) = m.get("res") {
            let res = packet(res)?;
            Outcome::Ok(Reply {
                sid: res.sid,
                ack: res.ack,
                values: res.values,
                nxt: res.nxt,
            })
        } else {
            if m.get("reject").and_then(Value::as_bool) != Some(true) {
                return Err("reject must be +true".into());
            }
            Outcome::Reject
        };
        let before = server.clone();
        let actual = server.serve(&req);
        if actual != expected {
            return Err(format!(
                "request {req:?}\nexpected {expected:?}\nactual   {actual:?}"
            ));
        }
        if actual == Outcome::Reject && *server != before {
            return Err("rejection changed server state".into());
        }
    } else {
        if m.get("res").is_some() || m.get("reject").is_some() {
            return Err("fault operation cannot have a response".into());
        }
        if let Some(sid) = m.get("forget") {
            server.forget(&bytes(sid)?);
        } else if let Some(flag) = m.get("reset") {
            if flag.as_bool() != Some(true) {
                return Err("reset must be +true".into());
            }
            server.reset();
        } else {
            if m.get("evict_opening").and_then(Value::as_bool) != Some(true) {
                return Err("evict_opening must be +true".into());
            }
            server.evict_opening();
        }
    }
    Ok(())
}

/// Run all steps from an empty channel, failing on syntax or the first mismatch.
pub fn run(source: &str) -> Result<usize, String> {
    let doc = sxpb::from_str(source).map_err(|e| e.to_string())?;
    let steps = doc.as_array().ok_or("expected scenario list")?;
    let mut server = Server::default();
    for (i, value) in steps.iter().enumerate() {
        step(&mut server, value).map_err(|e| format!("step {}: {e}", i + 1))?;
    }
    Ok(steps.len())
}
