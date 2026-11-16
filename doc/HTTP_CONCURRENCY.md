# HTTP concurrency and maintenance invariants

The adapter uses the same actually extracted/proved `OrderedServer`. Its
kernel-checked theorem is a pure one-call result; **these locking, timer and
worker-lifecycle invariants are source-reviewed and tested, not a new Lean proof**.

## Room lifecycle

The registry is a mutex-protected `LinkedHashMap<room bytes, Arc<Slot>>` with std
RandomState. Each slot has a separate mutex over `Option<Room>`. Registry order
is housekeeping order, not protocol discovery or per-XID expiry order.

- Lookup/create clones a handle under the registry lock, then releases it before
  acquiring the room lock. No path holds both locks simultaneously.
- Under the room lock, a request first checks the slot is live, then samples
  monotonic time, expires **all** due rows, executes the actual core, and refreshes
  its own XID only on protocol success. Wire admission happened beforehand;
  reply encoding and I/O happen afterward.
- Empty state is set to `None` under the room lock, irreversibly. Unlink follows
  after releasing that lock and only if the registry's handle is pointer-equal.
- A request already holding an old retired handle retries lookup; it never
  revives it. Replacement is possible only after retirement. Old cleanup handles
  cannot remove replacement slots, so one room cannot have two live instances.
- Poisoning fails closed rather than constructing a fresh generation silently.
  Same-room requests serialize; other rooms can execute independently when the
  application pool has available workers. Neither room nor worker fairness is
  guaranteed.

Maintenance atomically rotates at most64 registry entries under brief registry
locks, then uses `try_lock` on each room. Busy rooms are skipped. Each visit removes
at most64 expired XIDs from its due prefix. Access expiry remains complete and
cannot use stale records even when background housekeeping lags. Visits are
capped by the initial registry length; no whole-registry snapshot/scan happens.
The interval remains TTL clamped to10ms–1s. Rotation/backlog/skipped rooms affect
physical reclamation delay, which is not bounded by TTL plus one tick.

Per-room compaction can still be linear, and large payload destruction costs
bytes, not merely entries. Nonempty registry metadata capacity is retained rather
than globally compacted in a maintenance tick. Empty-registry replacement releases
its historical free pool **after** releasing the registry lock. Explicit synthetic
clock/full-maintenance fixture and benchmark methods call the same production
helpers; full-maintenance registry compaction is not a production reaper path.

## Worker lifetime

`serve_next` preserves its synchronous one-request contract. `run` uses four
fixed, scoped workers; `run_until(count, stop)` allows another positive count.
Workers directly receive from tiny_http. No application queue, per-request
thread, detached worker or transport fork is added.

Startup is gated until every fallible thread spawn succeeds. An abort-on-drop
guard lives **inside** the thread-scope closure, so failure or startup unwinding
releases waiting workers before the scope joins them. No request is received on
partial startup. Worker error/panic sets a shared stop flag before peers are
joined, and failure is returned. All scoped threads are joined before returning.

Stop is **cooperative draining of worker-owned requests**. Idle workers poll at
100ms; a request already returned by receive, including one racing with stop,
is finished. Active reads, response writes or tiny_http request destruction can
block indefinitely. Stop is not cancellation or a shutdown deadline, and does
not drain tiny_http's entire internal queue. The borrowed server cannot be
destroyed until its run call finishes. Maintenance continues during draining.

Only application request execution is bounded by the worker count. tiny_http's
internal connection tasks and queued requests remain outside that bound. All
workers can be occupied by slow clients or one hot room. Body/header/write
deadlines, admission/shape/capacity policies and public deployment remain separate.

## Evidence and performance

Tests cover stale-handle retry/replacement identity, simultaneous same-room
updates with exact row/timer cardinality, monotonic sampling after locking,
unrelated-room progress, busy-room skipping, maintenance budgets, poisoning,
startup failure and startup unwind rollback, exact worker occupancy, worker
failure/panic, live HTTP progress beside stalled body I/O (a `100 Continue`
handshake establishes that a worker has entered body reading), idle pool stop
only after a completed RPC establishes startup, and cooperative stop waiting
until a test-owned slow socket is closed. Existing expiry/recovery and
wire tests remain in force.

[Actual HTTP measurements](../test/performance/README.md) retain all72 trials and
regressions. More workers improve isolation, not universally throughput. The
small/fresh-TCP benchmark is not a cleanup-burst, deployment, fairness, DoS or
worst-case guarantee. The Rust-only core/cache comparison tests are independent
of these HTTP measurements.
