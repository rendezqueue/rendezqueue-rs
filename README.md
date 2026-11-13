# rendezqueue-rs

A Rust PlySwap rendezvous exchange library and HTTP server. It includes a volatile server, retained clients and a synchronous network transport—not naive TrySwap compatibility.

```sh
cargo run -- serve                     # loopback, port 15050; 20s cache TTL
cargo run -- serve --ttl 60    # optional server-side inactivity TTL
# In another terminal:
cargo run --example http_exchange      # two clients exchange greetings over HTTP
```

The library exposes transport-free `Client`, vector-reference `Server`, and
SID-indexed `OrderedServer`. `http::HttpServer` uses `OrderedServer` with a
separate expiry queue and per-room locks; the public vector API is unchanged.
`run()` uses four scoped request workers; incremental background cleanup skips
busy rooms without locking the whole registry through protocol execution. Callers drive
attempts explicitly; there is no automatic retry loop. See [HTTP.md](doc/HTTP.md) for the wire contract and network limits.

## Development

Development uses Cargo, rustfmt, and Clippy. SxPB is pinned to an exact upstream Git revision.

```sh
make tidytest     # format, lint, test, in order
make test         # Rust-only tests, with concise output
```

Tests and SxPB scenarios live in `test/`. Replay a fixture with `cargo run -- test/scenario/duplex.sxpb`; `cargo run --example exchange` runs the transport-free cache-loss demo. Network tests use ephemeral loopback listeners. Build output defaults to Cargo's normal target directory; a local ignored `.cargo/config.toml` can move it off network storage.

All builds, tests and benchmarks run without Lean or a companion checkout. Separately maintained formal proofs,
extraction controls and Rust-versus-model checks belong to `rendezqueue-lean`.
See [MODEL.md](doc/MODEL.md) for the runtime's identity assumptions and scope.

The [ordered comparison harness](test/comparison/README.md) reexports the
production ordered core and includes benchmark reports plus actual cache-source
comparisons. Actual loopback throughput/tail
latency measurements are in [test/performance/README.md](test/performance/README.md);
more workers are not an across-the-board throughput win.

## License

0BSD; see [LICENSE](LICENSE).

---
*Maintained by [paprika-bot](https://github.com/sockpaprika).*
