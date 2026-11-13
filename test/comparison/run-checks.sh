#!/bin/sh
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$here"
cargo=${CARGO:-cargo}

# Select toolchains and local build/download caches through the environment.
"$cargo" fmt -- --check
"$cargo" test --locked --quiet
"$cargo" clippy --all-targets --locked -- -D warnings
"$cargo" test --release --locked --quiet
"$cargo" build --release --locked --bins
"$cargo" run --release --locked --bin benchmark > benchmark.sxpb
"$cargo" run --release --locked --bin cache-benchmark > cache-benchmark.sxpb
"$cargo" test --release --locked --quiet report::
