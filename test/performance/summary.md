# Loopback HTTP: one versus four application workers

Median of three trials; requests/s and per-trial p95 milliseconds.

- 1 clients, distinct rooms, 32B: 5801 → 4684 requests/s; p95 0.199 → 0.290ms.
- 1 clients, distinct rooms, 4096B: 5059 → 4444 requests/s; p95 0.228 → 0.267ms.
- 1 clients, shared rooms, 32B: 5572 → 4500 requests/s; p95 0.208 → 0.258ms.
- 1 clients, shared rooms, 4096B: 4949 → 4152 requests/s; p95 0.236 → 0.324ms.
- 4 clients, distinct rooms, 32B: 19721 → 18910 requests/s; p95 0.273 → 0.268ms.
- 4 clients, distinct rooms, 4096B: 12976 → 11902 requests/s; p95 0.354 → 0.406ms.
- 4 clients, shared rooms, 32B: 19437 → 18767 requests/s; p95 0.254 → 0.283ms.
- 4 clients, shared rooms, 4096B: 12921 → 17481 requests/s; p95 0.351 → 0.294ms.
- 16 clients, distinct rooms, 32B: 18971 → 20156 requests/s; p95 1.770 → 1.685ms.
- 16 clients, distinct rooms, 4096B: 21422 → 19585 requests/s; p95 0.896 → 1.573ms.
- 16 clients, shared rooms, 32B: 21715 → 21365 requests/s; p95 1.554 → 1.581ms.
- 16 clients, shared rooms, 4096B: 22907 → 19838 requests/s; p95 0.889 → 1.632ms.

Mixed results, including regressions. This compares worker counts in the new adapter, not old-versus-new releases or an isolated lock optimization. Fresh TCP per RPC; TTL effectively disabled; no CPU/frequency isolation. No production-load, cleanup-burst, adversarial-client or worst-case guarantee.
