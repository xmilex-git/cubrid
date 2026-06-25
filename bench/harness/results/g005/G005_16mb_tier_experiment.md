# G005 — 16MB per-worker work_mem tier experiment: result = CRASH (latent parallel-query bug)

## What was tried
Per leader request, raise the per-worker work_mem RAM tier cap to ~16MB (1024 x 16KB pages), decoupled from the
legacy temp_file_memory_size_in_pages cap (max 20 pages). Change: qmgr_create_new_temp_file requests
max(param, 16MB/DB_PAGESIZE) for NORMAL temp files + a coarse back-off in reserve_membuf_budget so a large
request doesn't loop O(requested) under accountant pressure. data_buffer=256M (cap=64MB), 4M-row non-TDE table.

## Result: SERVER CRASH (SIGSEGV)
Parallel hash join (which is 4x FASTER at the small tier) CRASHES the cub_server with the 16MB tier.
Backtrace (gdb, RelWithDebInfo): SIGSEGV in worker threads at
  parallel_scan::result_handler<(RESULT_TYPE)1>::write_finalize  (px_scan_result_handler.cpp:310)
The 4-operator bench reported bogus ~15ms for all redesign ops = the server had crashed (instant connection
failures). Not OOM (193GB box, ~90GB free at crash).

## Root cause (exact)
px_scan_result_handler.cpp:100  `m_.list_id_headers.resize (parallelism);`  -- the result-writer header array
is sized to EXACTLY `parallelism`. Line 300-301 `index = list_id_header_index.fetch_add(1);
tl.list_id_header_p = &m_.list_id_headers[index];` assumes at most `parallelism` result writers. With the small
legacy membuf each parallel worker yields one result list (index < parallelism, OK). The large 16MB tier changes
the parallel HJ's spill/writer behavior so more writers are created than `parallelism` -> std::vector operator[]
out-of-bounds -> garbage list_id_header_p -> SIGSEGV in write_finalize.

## Conclusion
The redesign's per-worker work_mem tier is effectively HARD-LOCKED to the small legacy size: not just by config,
but because the parallel-query result handler has a brittle assumption (list_id_headers sized to parallelism,
and likely other membuf-coupled assumptions) that breaks once the tier is enlarged. So "set the cap to 16MB" is
not a config/one-line change -- it requires making the parallel-query result path tier-agnostic first.

## Proper fix scope (follow-up, not a quick tweak)
1. Make px_scan result handling independent of the per-worker buffer size: size m_.list_id_headers by the actual
   number of result writers (or guard index < size and grow), and audit other membuf-size assumptions in the
   parallel HJ/agg spill + reassembly.
2. THEN raise the per-worker tier (16MB) governed by the accountant cap, and re-measure.
3. Expected payoff: large sort/agg working sets stay in RAM -> the current sort 1.23x regression and agg parity
   turn into wins, and the PHJ 4x win grows. But it is gated on (1).

## State
Experimental change REVERTED; branch is clean at the prior commit (HJ 4x faster, sort/agg ~parity at small tier).
The installed release test binary is the experimental (crashing) one; rebuild from the clean source before any
further testing.
