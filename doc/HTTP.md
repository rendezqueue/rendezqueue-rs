# PlySwap HTTP v1

## Library use

`http::HttpServer::bind(address)` creates a volatile HTTP listener with a 20-second per-SID inactivity TTL. `bind_with_ttl(address, Duration)` configures a nonzero TTL. `run()` uses four fixed application workers receiving directly from the existing listener; `serve_next(wait)` still synchronously completes one request for embedded/test use. `run_until(worker_count, &AtomicBool)` selects a positive worker count and cooperative stop for embedding/measurements. The receive wait bounds waiting for a request, not reading its body. Independent maintenance continues while request workers wait or block on socket I/O.

Worker threads are scoped to `run`/`run_until`, never detached or spawned per RPC. Startup is gated: partial thread-start failure (or startup unwinding) releases/joins already-created workers before they can receive requests. Stop finishes worker-owned requests and joins workers; idle workers check within100ms, but **active body reads, writes and request destruction can postpone return indefinitely**. A receipt racing with stop is also completed. This is not socket cancellation, complete draining of tiny_http's queue, or a shutdown deadline. No transport fork or new request-deadline policy was introduced.

`http::HttpClient::new(namespace, endpoint, key, timeout)` pins retained client state to a full endpoint URL **and an immutable byte key**. `HttpTransport::new(endpoint, key, timeout)` does the same for a caller-owned core. Endpoints can use a custom full Python path but cannot include a query; these clients send key in JSON with `b64=7`. It exposes `enqueue(bytes)`, `step(Mode)` and read-only `core()`. Each step emits at most one RPC; `Ok(None)` means no request was enabled. Only this exact emitted packet is used to accept the response. No redirects, environment proxies, compression, automatic retries or background scheduling are enabled. HTTPS uses certificate verification through ureq/rustls; the included listener itself serves plain HTTP.

Applications owning their own retained core can instead use `HttpTransport::exchange(&Request)`. They are responsible for exact packet correlation and timeout handling. The wrapper does not expose a state-changing endpoint setter: do not move retained exchange state to another key or URL.

Callers must explicitly schedule both current attempts and retained previous attempts when enabled, including after input stops. An empty current contribution is not the same as sealing a previous EMPTY. There is no built-in fairness or progress guarantee for an arbitrary caller loop.

See [HTTP_CONCURRENCY.md](HTTP_CONCURRENCY.md) for retirement/locking invariants,
worker startup/stop behavior, tests and measurement scope.

## Route and encoding

`POST /v1/plyswap`, with `Content-Type: application/json` and exactly one `Content-Length`. Transfer-Encoding/chunked requests and connection upgrades are unsupported. There is no room path suffix, trailing-slash alias or default room. Naive `/tryswap` is not implemented.

Supply exactly one nonempty room key in JSON `"key":"room"` or query `?key=room`. Both sources, even if equal, duplicate query keys, unknown query parameters and malformed percent escapes are rejected. Query text is strict form-decoded once (plus becomes space; escape a literal plus as `%2B`), then decoded according to the JSON `b64` bitmap. Each decoded key selects an isolated room with its own SID-indexed `OrderedServer`.

Requests have required string `sid`, optional string `ack`/`nxt`, optional string array `values` (default EMPTY), optional integer `b64` (default 0), and `key` when not in the query. Only flagged groups are base64:

- Bit `1`: values.
- Bit `2`: SID, ACK and NXT.
- Bit `4`: key, whether in JSON or query.

An unflagged string is a Latin-1 binary string: U+0000–U+00FF maps directly to bytes; larger code points and lone surrogates are rejected. JSON itself is UTF-8. This follows the legacy binary-string/bitmap convention, not its permissive validation quirks. Flagged strings accept standard or URL-safe base64, correctly padded or unpadded. Whitespace, garbage, bad padding and nonzero unused bits are rejected; output is canonical unpadded base64url.

For example, `{"key":"room","sid":"a","ack":"b","values":["hi"],"nxt":"a1"}` and `{"key":"room","sid":"YQ","ack":"Yg","values":["aGk"],"nxt":"YTE","b64":3}` are the same decoded request. Flags/key stay in the HTTP envelope, not the pure protocol `Request`.

Replies omit key and honor the caller's bits `1|2`, dropping bit `4`. Zero `b64`, absent optional fields and empty values are omitted. Unflagged replies preserve arbitrary peer bytes through Latin-1 strings. Empty byte strings are legal identifiers and payloads: omitted values and `[]` are EMPTY, but `[""]` is one data element. NXT signals completed replies even when EMPTY; ACK is opening-only.

Unknown/duplicate JSON fields, nulls, wrong types and invalid bitmaps (including booleans) are rejected. `decode_reply` rejects key and invalid reply shapes.

## Outcomes and uncertainty

- HTTP200: a valid PlySwap reply with matching request SID.
- HTTP409 with JSON body exactly `{"error":"plyswap-rejected"}`: protocol rejection. This is the only HTTP result mapped to `Outcome::Reject`.
- HTTP400/404/405/411/413/415 and other statuses: adapter/status errors, not protocol rejection. Wrong-route, incomplete or malformed/oversized input cannot create/refresh protocol state or a room. Independent scheduled expiry can still remove stale state.
- Invalid reply JSON, content type/encoding, SID, shape, truncation, size or deadline: errors, not protocol rejection. All responses have `Cache-Control: no-store`.

After a transport error, the wrapper clears ACTIVE but retains whole previous, frozen batch, queued tail and cache hints. A later retry is another explicit caller action. A valid reference rejection instead clears its matching cache hint so its next retry restores the real full snapshot. Losing a FULL response does not confirm the upload.

**An error or timeout does not prove nondelivery.** The server may have processed the request; a failed response write does not undo it. No application callback/external-effect exactly-once claim follows.

## Cache expiration

- TTL is server-side configuration, not a new request/reply field. Run `rendezqueue serve [address] --ttl 20` (positive integer seconds); the default remains20.
- After complete admission, the adapter briefly locks the registry to find/create a room handle, releases it, then takes that room's lock and samples monotonic time, expires **every stale row in that room**, then performs the core transition atomically. Elapsed time **greater than or equal to TTL** expires a row before any lookup/discovery. A reference at its deadline rejects; its next explicit retained-client attempt sends FULL.
- An OK refreshes only the request's own SID, including opening polls, unchanged FULL and successful references. Reject/error refreshes nothing. Reading a peer, naming ACK/NXT, or polling another generation/room does not renew it or manufacture successor rows. This is inactivity expiry: intentionally polling an old SID keeps that particular row alive.
- Each room keeps separate newest-first protocol rows and oldest-refresh-first timers, both SID-indexed ordered maps. An opening poll refreshes its own timer without changing discovery order. There is one timer entry per live SID, not an accumulating queue of stale timer versions.
- A background worker rotates at most64 registered rooms per tick, with at most64 expired SID removals per visited room, normally once per second (TTL clamped to10ms–1s). It releases the registry lock before trying a room lock, skips busy rooms, and caps visits by the initial room count. These are scheduling budgets, not admission limits or wall-clock bounds. Request-side expiry remains complete. Per-room geometric compaction and dropping large payloads can still cost proportional work.
- Idle physical reclamation now depends on rotation length, expired-entry backlog and busy rooms, not merely TTL plus one tick. There is no global registry scan in production maintenance. Room payload/timer allocations are dropped or geometrically compacted under their room lock. Registry metadata capacity may persist while rooms remain; an empty registry releases its node pool. Explicit sequential full-maintenance hooks used in fixtures/cache benchmarks also compact the registry; production does not call those hooks.
- Request bodies, JSON work and response I/O run outside both locks. Same-room expiry/core/refresh is serialized; unrelated rooms can execute concurrently. A retired empty slot becomes an irreversible tombstone before unlinking; requests holding old handles retry, and pointer-identity checks prevent old cleanup handles removing replacements. Registry and room locks are never held together. A slow socket cannot itself hold either lock or stop idle cleanup. The worker is stopped/joined on listener drop; failed startup is returned, and worker failure is reported rather than silently disabling expiry. Scheduling and core processing can delay sweeps; this is not a hard real-time reclamation deadline.
- Retained clients must schedule current/previous attempts frequently enough for the chosen TTL. Expiration permits cache-MISS/FULL recovery; it does not add a delivery guarantee. Both transport-free cores remain clock-free; these rules belong to the HTTP cache adapter.

## Limits and deployment boundary

- Incoming JSON plus raw query text, and locally encoded request envelopes, are limited to 1 MiB. Reply reads are bounded at 12 MiB +1024 bytes: two admitted envelopes can contribute own SID and peer body/NXT, and raw JSON escaping can expand one byte to six characters (`\u0000`). This conservative bound covers mixed base64/raw traffic; the former 2 MiB bound would reject valid raw replies. Keys cannot bypass request limits via the query.
- Request admission checks both the actual bytes read and equality to the declared Content-Length. Streaming/chunked request bodies and compressed bodies are unsupported. Reply reads are bounded even without a response Content-Length; client deadlines cover response-body consumption.
- Enqueue batches must fit the request limit after base64/JSON expansion. Current preparation freezes the entire queued tail. An oversized frozen batch is retained but remains unsendable through this transport; it is not silently split or truncated, and eventual delivery is not claimed for it. The local oversize error explicitly says not sent.
- Namespaces must be fresh within their room; never reuse one after state loss. The network adapter does not provision random identities or implement authenticated membership.
- Cache state is volatile and inactive rows/rooms expire. TTL is **not a hard memory cap** under arbitrary admission rates; explicit admission/capacity limits, authentication and persistence are not implemented. Room keys select participation; they are not authenticated identities.
- The CLI defaults to `127.0.0.1:15050`; an explicit address can be supplied as `rendezqueue serve <address>`. This is **not a hardened public listener**. Application request execution is bounded by the selected worker count, but tiny_http's internal connection tasks and request queue are **not** bounded by it. Slow bodies/writes can occupy all application workers; no per-room fairness or absolute I/O deadlines are claimed. Public exposure needs separately selected TLS, body/header deadlines and resource controls, typically at a reverse proxy. None is deployed by this project.
