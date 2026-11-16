#!/usr/bin/env python3
"""Summarize generated timings (SxPB syntax is independently checked by Rust)."""
from pathlib import Path
import re
import statistics

records = {}
for line in Path(__file__).with_name("benchmark.sxpb").read_text().splitlines():
    match = re.search(r'\(kind "([^"]+)"\).*\(rows (\d+)\).*\(payload_bytes (\d+)\).*\(backend "([^"]+)"\).*\(operations (\d+)\).*\(elapsed_ns (\d+)\)', line)
    if match:
        kind, size, payload, backend, ops, ns = match.groups()
        records.setdefault((kind, int(size), int(payload), backend), []).append(int(ns) / int(ops))
assert sum(map(len, records.values())) == 180
print("# Core microbenchmark timings\n")
print("Median nanoseconds per operation; three trials, with raw min–max.\n")
for (kind, size, payload, backend), samples in records.items():
    assert len(samples) == 3
    print(f"- {kind}; {size} rows; {payload} B; {backend}: "
          f"{statistics.median(samples):.1f} ns/op "
          f"(range {min(samples):.1f}–{max(samples):.1f}).")
