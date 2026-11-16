# Actual HTTP measurements

`http.rs` runs the real server and retained transport on ephemeral loopback TCP.
Run `cargo run --release --example http_benchmark > /local/cache/http.sxpb`, then
copy the completed report here deliberately. `python3 summarize.py > summary.md`
reproduces the bullet summary; normal Rust tests parse the report with the real
pinned SxPB library and require every trial/field in the complete grid.

- One/four application workers; one/four/sixteen concurrent clients; shared room
  or distinct rooms; 32/4096-byte repeated FULL bodies; three trials,64 calls per
  client. Every response is checked against the actual vector reference.
- Setup, initial uploads and client-thread creation precede timing. The timed
  interval includes barrier release, request/response encoding, fresh TCP per
  call, protocol work, response validation, output allocation/destruction, and
  joining completed clients. Per-call percentiles include response validation.
- The server uses `Duration::MAX` TTL, so these are hot steady-state measurements,
  not expiry-burst timings. Shared-room clients use different XIDs and absent,
  distinct peer_xid IDs; this is not an honest exchange/convergence scenario.
- All cases use the new per-room adapter. One versus four workers does not
  isolate lock costs and is not an old-release baseline. Busy-room isolation,
  stalled-client progress and cooperative-stop behavior have separate tests.

[summary.md](summary.md) retains regressions: four workers are **not** a universal
throughput improvement on this small/fresh-connection workload. They permit
independent progress while another request waits on I/O or another room lock.
Four is a configurable library default, not a measured optimum. Same-room work
is still serialized; a hot room or enough slow connections can occupy the pool.

Captured on atomman AMD Ryzen9 7945HX, Linux x86_64, rootless container, pinned
nightly2026-08-18 rustc1.100.0-nightly / LLVM23, nice/ionice. No CPU/frequency
isolation; warm builds, randomized hashing. Compilations/downloads and initial
reports used persistent atomman/local NVMe caches, not the shared source tree.
No production/RSS/adversarial ingress or hard-latency claim follows. tiny_http's
internal connection tasks/queue are outside the application-worker bound.
