//! Synchronous HTTP adapter. Explicit attempts only; no redirects or automatic retry.
#[cfg(test)]
mod concurrency_tests;
#[cfg(test)]
mod expiry_tests;
mod reaper;
mod workers;

use crate::{Bytes, Client, Exhausted, Mode, Outcome, Request, server::expiry::Rooms, wire};
use reaper::Reaper;
use std::{
    fmt,
    io::{self, Read},
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tiny_http::{Header, Method, Response, StatusCode};

fn json_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
}

/// Volatile per-room server. Inbound TLS, deadlines and public-service hardening
/// belong to a separately configured reverse proxy; the CLI binds loopback.
pub struct HttpServer {
    // Drop maintenance first, without owning the cache lock.
    reaper: Reaper,
    listener: tiny_http::Server,
    rooms: Arc<Rooms>,
}

/// Inactivity lifetime for each successfully requested SID, not an entire room.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(20);
/// Application workers, not a bound on tiny_http's internal connections/queue.
pub const DEFAULT_WORKERS: usize = 4;
impl HttpServer {
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::bind_with_ttl(addr, DEFAULT_CACHE_TTL)
    }

    /// Configure a nonzero inactivity TTL; wire fields and the core are unchanged.
    pub fn bind_with_ttl(addr: impl ToSocketAddrs, ttl: Duration) -> io::Result<Self> {
        if ttl.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache TTL must be nonzero",
            ));
        }
        let listener = tiny_http::Server::http(addr).map_err(io::Error::other)?;
        let rooms = Arc::new(Rooms::new(ttl));
        let reaper = Reaper::start(Arc::clone(&rooms), ttl)?;
        Ok(Self {
            reaper,
            listener,
            rooms,
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.server_addr().to_ip().expect("TCP listener")
    }
    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    fn handle(&self, mut request: tiny_http::Request) {
        let (status, body) = self.dispatch(&mut request);
        let response = Response::from_data(body)
            .with_status_code(StatusCode(status))
            .with_header(Header::from_bytes("Content-Type", "application/json").unwrap())
            .with_header(Header::from_bytes("Cache-Control", "no-store").unwrap());
        // A lost response does not roll back a processed exchange.
        if let Err(e) = request.respond(response) {
            eprintln!("PlySwap response write failed: {e}");
        }
    }

    fn dispatch(&self, request: &mut tiny_http::Request) -> (u16, Vec<u8>) {
        fn error(status: u16, message: &str) -> (u16, Vec<u8>) {
            (
                status,
                serde_json::to_vec(&serde_json::json!({"error": message})).unwrap(),
            )
        }
        if !wire::is_route(request.url()) {
            return error(404, "unknown endpoint");
        }
        let query_key = match wire::query_key(request.url()) {
            Ok(key) => key,
            Err(_) => return error(400, "invalid query key"),
        };
        if request.method() != &Method::Post {
            return error(405, "POST required");
        }
        let content_types: Vec<_> = request
            .headers()
            .iter()
            .filter(|h| h.field.equiv("Content-Type"))
            .collect();
        if content_types.len() != 1 || !json_content_type(content_types[0].value.as_str()) {
            return error(415, "application/json required");
        }
        if request
            .headers()
            .iter()
            .any(|h| h.field.equiv("Content-Encoding"))
        {
            return error(415, "content encoding not supported");
        }
        // tiny_http's chunk decoder can hide premature EOF as a clean end of
        // body. Require one explicit fixed length instead of guessing completion.
        let lengths = request
            .headers()
            .iter()
            .filter(|h| h.field.equiv("Content-Length"))
            .count();
        if request
            .headers()
            .iter()
            .any(|h| h.field.equiv("Transfer-Encoding"))
            || lengths != 1
        {
            return error(
                411,
                "one Content-Length required; transfer encoding unsupported",
            );
        }
        if request.headers().iter().any(|h| {
            h.field.equiv("Upgrade")
                || (h.field.equiv("Connection")
                    && h.value
                        .as_str()
                        .split(',')
                        .any(|v| v.trim().eq_ignore_ascii_case("upgrade")))
        }) {
            return error(400, "connection upgrade unsupported");
        }
        let Some(declared_length) = request.body_length() else {
            return error(400, "invalid Content-Length");
        };
        let query_length = request
            .url()
            .split_once('?')
            .map_or(0, |(_, query)| query.len());
        if query_length > wire::REQUEST_LIMIT
            || declared_length > wire::REQUEST_LIMIT.saturating_sub(query_length)
        {
            return error(413, "request too large");
        }
        let mut bytes = vec![];
        if request
            .as_reader()
            .take((wire::REQUEST_LIMIT + 1) as u64)
            .read_to_end(&mut bytes)
            .is_err()
        {
            return error(400, "body read failed");
        }
        if bytes.len() > wire::REQUEST_LIMIT {
            return error(413, "request too large");
        }
        // tiny_http's streaming fixed-length reader can report clean EOF on
        // truncation. Never process a complete JSON prefix of an incomplete body.
        if bytes.len() != declared_length {
            return error(400, "incomplete request body");
        }
        let input = match wire::decode_envelope(&bytes, query_key.as_deref()) {
            Ok(r) => r,
            Err(_) => return error(400, "invalid PlySwap JSON"),
        };
        // Admission/socket reads precede cache access. Registry lookup releases
        // its lock before room locking; expiry/core/refresh and time sampling
        // are atomic under that room's lock, not the whole registry's lock.
        let outcome = match self.rooms.serve_now(input.key, &input.request) {
            Ok(outcome) => outcome,
            Err(_) => return error(503, "cache unavailable"),
        };
        // No cache lock is held during serialization, response I/O or request drop.
        match outcome {
            Outcome::Reject => (409, wire::REJECTION.to_vec()),
            Outcome::Ok(reply) => (200, wire::encode_reply_with_flags(&reply, input.b64)),
        }
    }

    /// Process at most one request; false means no request arrived in the wait.
    /// This wait is not a body-read deadline.
    pub fn serve_next(&mut self, wait: Duration) -> io::Result<bool> {
        self.serve_one(wait)
    }
    fn serve_one(&self, wait: Duration) -> io::Result<bool> {
        self.reaper.check()?;
        let request = self.listener.recv_timeout(wait)?;
        self.reaper.check()?;
        if let Some(request) = request {
            self.handle(request);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Run a fixed application-worker pool. Each worker receives directly from
    /// tiny_http; no extra application request queue or per-request thread.
    pub fn run(&mut self) -> io::Result<()> {
        self.run_until(DEFAULT_WORKERS, &AtomicBool::new(false))
    }
    /// Cooperative stop: finish worker-owned requests, then join all workers.
    /// Idle workers poll within 100ms. This does NOT cancel active socket I/O,
    /// drain tiny_http's entire queue, or impose a shutdown deadline. A request
    /// racing with stop and already received is also finished. Scoped workers
    /// cannot outlive this call/server; stalled I/O can postpone return.
    pub fn run_until(&mut self, worker_count: usize, stop: &AtomicBool) -> io::Result<()> {
        workers::run(worker_count, stop, || {
            self.serve_one(Duration::from_millis(100)).map(|_| ())
        })
    }
}

#[derive(Debug)]
pub enum TransportError {
    Configuration(String),
    RequestTooLarge,
    Network(ureq::Error),
    Status(u16),
    InvalidResponse(String),
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(e) => write!(f, "invalid HTTP configuration: {e}"),
            Self::RequestTooLarge => write!(
                f,
                "not sent: JSON request exceeds {} bytes; retained batch is not split",
                wire::REQUEST_LIMIT
            ),
            Self::Network(e) => write!(f, "HTTP exchange failed; delivery may have occurred: {e}"),
            Self::Status(s) => write!(f, "unexpected HTTP {s}; delivery is uncertain"),
            Self::InvalidResponse(e) => {
                write!(f, "invalid PlySwap response; delivery is uncertain: {e}")
            }
        }
    }
}
impl std::error::Error for TransportError {}

/// Fixed endpoint and key, finite overall deadline, verified TLS on HTTPS. Environment
/// proxies and redirects are disabled so they cannot silently move channel state.
pub struct HttpTransport {
    url: String,
    key: Bytes,
    agent: ureq::Agent,
}
impl HttpTransport {
    pub fn new(url: &str, key: Bytes, timeout: Duration) -> Result<Self, TransportError> {
        let uri: ureq::http::Uri = url
            .parse()
            .map_err(|e| TransportError::Configuration(format!("{e}")))?;
        if !matches!(uri.scheme_str(), Some("http" | "https"))
            || uri.authority().is_none()
            || uri.query().is_some()
            || timeout.is_zero()
            || key.is_empty()
        {
            return Err(TransportError::Configuration(
                "absolute HTTP(S) endpoint, nonempty key and nonzero timeout required".into(),
            ));
        }
        if wire::encode_envelope(&Request::default(), &key).len() > wire::REQUEST_LIMIT {
            return Err(TransportError::Configuration(
                "key exceeds request envelope limit".into(),
            ));
        }
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .max_idle_connections(0)
            .max_idle_connections_per_host(0)
            .proxy(None)
            .timeout_global(Some(timeout))
            .build();
        Ok(Self {
            url: url.into(),
            key,
            agent: config.into(),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.url
    }

    pub fn exchange(&self, request: &Request) -> Result<Outcome, TransportError> {
        let body = wire::encode_envelope(request, &self.key);
        if body.len() > wire::REQUEST_LIMIT {
            return Err(TransportError::RequestTooLarge);
        }
        let mut response = self
            .agent
            .post(&self.url)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-store")
            .send(body.as_slice())
            .map_err(TransportError::Network)?;
        let status = response.status().as_u16();
        if status != 200 && status != 409 {
            return Err(TransportError::Status(status));
        }
        if !response
            .headers()
            .get("Content-Type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(json_content_type)
            || response.headers().contains_key("Content-Encoding")
        {
            return Err(TransportError::InvalidResponse(
                "content type or encoding".into(),
            ));
        }
        let body = response
            .body_mut()
            .with_config()
            .limit(wire::RESPONSE_LIMIT as u64)
            .read_to_vec()
            .map_err(TransportError::Network)?;
        if status == 409 {
            return if body == wire::REJECTION {
                Ok(Outcome::Reject)
            } else {
                Err(TransportError::InvalidResponse(
                    "not a protocol rejection".into(),
                ))
            };
        }
        let reply = wire::decode_reply(&body)
            .map_err(|e| TransportError::InvalidResponse(e.to_string()))?;
        if reply.sid != request.sid {
            return Err(TransportError::InvalidResponse("reply SID mismatch".into()));
        }
        Ok(Outcome::Ok(reply))
    }
}

#[derive(Debug)]
pub enum ExchangeError {
    Exhausted(Exhausted),
    Transport(TransportError),
}
impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted(e) => e.fmt(f),
            Self::Transport(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ExchangeError {}

/// Retained client pinned to one endpoint/key. No state-changing channel setter,
/// mutable core escape hatch, automatic retry, or background scheduler.
pub struct HttpClient {
    core: Client,
    transport: HttpTransport,
}
impl HttpClient {
    pub fn new(
        namespace: Bytes,
        endpoint: &str,
        key: Bytes,
        timeout: Duration,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            core: Client::new(namespace),
            transport: HttpTransport::new(endpoint, key, timeout)?,
        })
    }
    pub fn core(&self) -> &Client {
        &self.core
    }
    pub fn enqueue(&mut self, value: Bytes) {
        self.core.enqueue(value);
    }

    /// Exactly one caller-requested attempt. None is a disabled send, not a queued
    /// future action. All errors retain input; uncertain failures are not Reject.
    pub fn step(&mut self, mode: Mode) -> Result<Option<Outcome>, ExchangeError> {
        let Some(packet) = self.core.send(mode).map_err(ExchangeError::Exhausted)? else {
            return Ok(None);
        };
        match self.transport.exchange(&packet.request) {
            Ok(outcome) => {
                self.core
                    .accept(&packet, &outcome)
                    .map_err(ExchangeError::Exhausted)?;
                Ok(Some(outcome))
            }
            Err(error) => {
                self.core.timeout();
                Err(ExchangeError::Transport(error))
            }
        }
    }
}
