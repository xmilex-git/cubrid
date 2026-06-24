# G005 — R6 remediation findings (env-A regression). PARTIAL: I/O component fixed; CPU/wait component OPEN.

## Attempt 1: port the as-built P6 temp-page LRU-ignore-unfix (commit 69d78cac4) -> PARTIAL
Cherry-picked the +29-line `pgbuf_unlatch_void_zone_bcb` temp-ignore-unfix hunk onto the redesign (src/storage/page_buffer.c). Rebuilt redesign RELEASE, re-measured env A (data_buffer_size=512M, parallelism=8) on tpch_sf10.

Result (env A, redesign+fix vs baseline 222ms):
- **I/O component FIXED**: env-A `ioread` dropped from **6818 -> 0** (the temp/cache pages no longer evict hot data/index pages from the 512M buffer). This confirms the LRU-pollution part of the root cause and that the as-built P6 fix is the correct remedy for it.
- **CPU/wait component STILL OPEN**: env-A total time is still ~2300-2600ms (baseline ~222ms). The dominant cost is now pure CPU/wait, NOT I/O:
  - inner orders covered-pk-index scan SELECT: redesign **2467ms (btree time 1304ms), ioread 0** vs baseline **121ms (btree 75ms), ioread 0** — a ~17x CPU regression in the parallel-HJ inner btree scan with everything cached.
  - present BEFORE the LRU fix too (pre-fix btree time 1039ms), so it is independent of LRU pollution.

## Refined root cause (hypothesis, NOT yet pinned — perf is not installed on this box)
A ~17x CPU regression localized to the parallel-HJ inner index-scan hot path, ioread 0. Likely redesign instrumentation in a per-page/per-row hot path (candidates to profile: the global sharded work-mem accountant reserve/release, the per-worker work_mem tier bookkeeping, or the px_scan mergeable-list / segment path). Needs a profiler (perf not present -> gdb thread sampling or installing perf) to pin the hot function, since the slow path runs in parallel worker threads.

## Status / decision needed (leader)
- Committed: the LRU-ignore-unfix fix (I/O component) — a correct, standalone improvement matching the as-built P6 design.
- OPEN: the parallel-scan CPU/wait regression. Options: (a) profile + fix the hot path (needs perf/gdb; open-ended); (b) accept a documented scope-limit; (c) defer. The redesign remains NOT mergeable to develop until env A is within the baseline band.
- Re-run full R6 (N=10, env A/B/C + positioned, incl as-built leg) after the CPU component is resolved.
