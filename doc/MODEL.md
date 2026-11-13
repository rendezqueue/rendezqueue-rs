# Protocol boundary

This library implements PlySwap with volatile server state and retained clients.
Formal models, proofs and cross-language verification are maintained separately
in `rendezqueue-lean`; no command in this repository requires that checkout.

- `Server` is one implicit channel with newest-first rows and explicit reset/eviction.
  HTTP uses `OrderedServer`, per-SID inactivity expiry and independent idle cleanup.
  See [HTTP.md](HTTP.md) for refresh, reclamation and recovery rules. No hard
  memory/admission cap or persistence is implemented.
- IDs and payload elements are opaque bytes. Equality, list order and emptiness
  matter; an empty byte element is real data, not an empty batch.
- Each client namespace must be globally fresh for its channel and never reused
  after state loss. IDs append an eight-byte big-endian generation. There is no
  random allocator, authentication or restart recovery. Do not run clones of one
  client as independent peers.
- Generations and local attempt counters are checked u64 values. Exhaustion
  returns `Exhausted`, never wraps. Attempts are correlation state, not wire fields.
- One ACTIVE request per client is allowed. Timeout clears ACTIVE, not traffic.
  Callers drive sends, retries and callback delivery explicitly.
- Previous requests, frozen batches and queued tails are real retained memory.
  Callback reference resolution uses only that memory. Previous callbacks cannot
  append output or advance generation.
- SxPB scenarios use UTF-8 strings as byte labels. Omitted values and empty lists
  normalize alike; invalid fixture fields/types fail loudly. This test adapter is
  separate from the JSON network codec in `src/wire.rs`.
- HTTP validates admission before mutation. Expiry/core/refresh serialize under
  one room lock, with time sampled there. A failed reply write does not roll back
  processing. Transport errors retain input and clear only ACTIVE; they are not
  synthetic protocol rejections. See [HTTP_CONCURRENCY.md](HTTP_CONCURRENCY.md).

No naive TrySwap compatibility, automatic scheduling, production identity
provisioning, restart-safe SDK lifecycle or external-effect exactly-once guarantee
is included. Routing uses `/v1/plyswap` with a JSON/query key and isolated room
state, never protocol sniffing. The HTTP envelope's `b64` bitmap selects byte-string
encoding; key and flags are outside the pure protocol request. Inbound TLS, authentication and slow-client protection
are outside this adapter. Finite JSON limits restrict network-admissible inputs.
