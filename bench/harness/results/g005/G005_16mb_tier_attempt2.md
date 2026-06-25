# G005 — proper attempt to enable a 16MB per-worker work_mem tier: NOT viable as a targeted change

## Goal (leader request)
Raise the per-worker work_mem RAM tier to ~16MB (from the legacy temp_file_memory_size_in_pages cap of <=20
pages) so big-table sort/hash-agg keep more in RAM and spill less, then re-measure.

## What was implemented (single source of truth + admission consistency + guards)
- temp_page_store::workmem_tier_pages() = ~16MB/page, used by BOTH the runtime membuf request
  (qmgr_create_new_temp_file) AND the parallel-degree admission estimate (reservation_bytes_for_degree default),
  so admission and runtime use the same size.
- Coarse geometric back-off in reserve_membuf_budget (so a large request doesn't loop O(requested)).
- Defensive NULL-tl guard in px_scan write_finalize (MERGEABLE_LIST) so a worker that fails write_start
  accounts for itself instead of crashing/hanging.

## Result: THREE layered couplings; not viable + no benefit
1. CRASH (fixed): admission estimated the small tier but runtime requested 16MB -> over-cap workers failed
   write_start (tl.xasl=NULL) -> write_finalize SIGSEGV (px_scan_result_handler.cpp:314). The shared
   workmem_tier_pages() + guard removed this crash.
2. HANG (exposed next): once admission clamps the degree (16MB est. vs 64MB cap @256M buffer -> ~4 of 8), the
   result handler's m_.active_results still expects the original parallelism -> coordinator waits forever for
   finalizes that never come. The degree is NOT reconciled between the accountant clamp and the result handler.
3. OVER-ALLOCATION (fundamental): a parallel HASH JOIN creates MANY temp files per worker (one per hash
   partition). 16MB PER TEMP FILE x (partitions x workers) >> the accountant cap -> reservation failures ->
   PHJ/HASHAGG fail even at data_buffer=2G (cap 256MB). Measured: redesign PHJ 26ms / HASHAGG 20ms = failed
   (bogus-fast = errored); PSCAN unstable.

## And even where it runs, it does NOT help
PSORT (single-stream sort) ran with the 16MB tier: 3528ms vs develop baseline 3226ms -- STILL SLOWER. A 4M-row
sort's working set is hundreds of MB; 16MB (or any feasible per-worker tier; 8 workers x 256MB = 2GB) is far
too small to avoid spilling. So enlarging the RAM tier cannot close the gap -- the data spills regardless.

## Conclusion: the per-worker tier size is the WRONG lever
- Enlarging the work_mem tier is not a targeted change: it needs (a) a per-worker/per-query work_mem BUDGET
  shared across that worker's many temp files (NOT 16MB per file), and (b) the parallel degree reconciled across
  the accountant clamp + the result-handler active_results + list sizing. That is a work_mem-architecture
  rework, multi-day, with real correctness risk.
- AND it would still not help big-table sort/agg (working set >> any feasible tier -> spill inevitable).
- The redesign's REAL win (parallel hash join 4x faster on big spilling tables) comes from the efficient SPILL
  PATH, not the RAM tier. The path to improving big sort/agg is to extend that efficient spill/reassembly path
  to the external sort + hash-agg spill (which still use the legacy sort_run_merge / agg-partial spill), NOT to
  grow the RAM tier.

## State
All experimental changes REVERTED; branch source is clean (small tier; PHJ 4x faster, sort/agg ~parity-to-
slightly-slower; non-TDE correct + fast per the G005 gate fix). The installed release test binary is the
experimental (unstable) one; rebuild from clean source before further testing.
