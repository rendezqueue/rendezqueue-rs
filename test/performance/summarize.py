#!/usr/bin/env python3
"""Summarize actual HTTP records; Rust/SxPB tests enforce the complete grid."""
from pathlib import Path
import re
from statistics import median

rows = {}
for line in Path(__file__).with_name('http.sxpb').read_text().splitlines():
    fields = dict(re.findall(r'\((\w+) ([^()]+)\)', line))
    if 'workers' not in fields:
        continue
    k = (int(fields['clients']), fields['distribution'].strip('"'),
         int(fields['payload_bytes']), int(fields['workers']))
    rows.setdefault(k, []).append((int(fields['requests']) * 1e9 / int(fields['elapsed_ns']),
                                  int(fields['p95_ns']) / 1e6))
assert len(rows) == 24 and all(len(v) == 3 for v in rows.values())
print('# Loopback HTTP: one versus four application workers\n')
print('Median of three trials; requests/s and per-trial p95 milliseconds.\n')
for clients in [1, 4, 16]:
    for distribution in ['distinct', 'shared']:
        for payload in [32, 4096]:
            values = []
            for workers in [1, 4]:
                samples = rows[clients, distribution, payload, workers]
                values.append((median(v[0] for v in samples), median(v[1] for v in samples)))
            a, b = values
            print(f'- {clients} clients, {distribution} rooms, {payload}B: '
                  f'{a[0]:.0f} → {b[0]:.0f} requests/s; p95 {a[1]:.3f} → {b[1]:.3f}ms.')
print('\nMixed results, including regressions. This compares worker counts in the new adapter, '
      'not old-versus-new releases or an isolated lock optimization. '
      'Fresh TCP per RPC; TTL effectively disabled; no CPU/frequency isolation. '
      'No production-load, cleanup-burst, adversarial-client or worst-case guarantee.')
