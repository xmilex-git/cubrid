# G005 — develop vs redesign perf analysis (big non-TDE table) + VTune diff + improvement plan

## Benchmark (non-TDE wmnt, 4,194,304 rows, data_buffer_size=256M, parallelism=8, spill forced)
| operator | develop (baseline) | redesign | result |
|---|---|---|---|
| parallel hash join | 2392 ms | **593 ms** | **redesign 4.0x FASTER** |
| parallel sort      | 3617 ms | 4442 ms | redesign 1.23x slower |
| parallel scan      | 24507 ms | 24202 ms | parity |
| hash aggregate     | 2502 ms | 2707 ms | parity (1.08x) |

So YES there IS a real improvement -- a large one on parallel hash join (the hottest spill operator). Parallel scan + hash-agg are parity; parallel sort is modestly slower.
(At 1M rows / 512M buffer where the HJ does NOT spill, redesign HJ was ~1.4x slower -- the redesign overhead only pays off once the operator actually spills. The win scales with table size.)

## VTune diff (PSORT, develop vs redesign) -- nearly identical
Top self-time functions on BOTH legs: pread64, pwrite, __memmove_evex, qfile_compare_all_sort_record,
pthread_cond_timedwait, __pthread_mutex_lock, sort_exphase_merge, sort_spage_find_free, sort_run_merge,
pgbuf_bcb_get_zone, epoll_wait. The redesign additionally shows heap_attrinfo_read_dbvalues +
qfile_generate_tuple_into_list (tuple re-materialization); baseline shows sort_spage_insert.
=> The sort path is the SAME external-sort + spill code in both. Both are I/O-bound (pread/pwrite dominate).
The 1.23x is NOT a new hotspot or extra I/O -- it is slight extra tuple regeneration on the redesign sort spill.

## Answers to the specific questions
- "I/O가 더 많나?" (more I/O?): For SORT, NO -- both legs spill heavily and pread/pwrite dominate both about equally.
  For HASH JOIN, the redesign does LESS effective I/O work (4x faster = fewer temp-volume round-trips). The
  redesign is not generally "more I/O"; it is path-dependent.
- "워커 메모리 버퍼가 좁아서?" (worker memory buffer too narrow?): YES -- this is the core limiter. The per-worker
  work_mem RAM tier is sized from `temp_file_memory_size_in_pages` (== PRM_ID_TEMP_MEM_BUFFER_PAGES), whose range
  is [4, 20] pages -> at most 20 pages = 320KB, default 4 pages = 64KB (system_parameter.c). And the accountant
  (`reserve_membuf_budget` -> `derive_query_membuf_pages`) can shrink it FURTHER under reserved pressure
  (qmgr_create_new_temp_file:3733-3736). So for a multi-million-row sort/agg the RAM tier holds essentially
  nothing -> everything spills, on BOTH develop and redesign.
- "버퍼가 아예 없어?" (no buffer at all?): No, a per-worker membuf IS allocated (qmgr_allocate_tempfile_with_buffer),
  but it is tiny -- capped at the legacy 20-page limit. The redesign did NOT introduce a genuinely large work_mem
  RAM tier; it kept the legacy cap, so the "work_mem-first, overflow-rare" thesis does not actually kick in for
  big operators.

## Why the redesign is slower on sort/agg but faster on HJ
- The 4x HJ win comes from the redesign's SPILL PATH (work_mem-first + the new overflow path) avoiding develop's
  temp-volume round-trip on the hash-join partition re-emit. That path was specifically reworked.
- Sort + hash-agg still go through the legacy external-sort / hash-agg spill (sort_run_merge etc.), now with a
  tiny RAM tier (<=320KB) + slight tuple-regen overhead -> no win, small loss.

## How to improve (recommended, in priority order)
1. DECOUPLE the per-worker work_mem RAM tier from `temp_file_memory_size_in_pages` (max 20). Size it from the
   accountant's internal cap (a fraction of data_buffer_size / system RAM, already designed as g_workmem_cap_bytes)
   so each worker gets an MB-scale RAM tier. This is the redesign's STATED design; the implementation capped it at
   the legacy 20 pages, defeating "work_mem-first". With an MB-scale tier, big sort/agg working sets stay in RAM (or
   spill far less) -> the sort/agg regression turns into a win and the HJ win grows. (This is the single highest-value
   change; it directly addresses the narrow-buffer root cause.)
2. Remove the redesign sort-spill tuple regeneration (heap_attrinfo_read_dbvalues + qfile_generate_tuple_into_list
   appearing only on the redesign sort path) -> route sort spill through the same copy-light path the HJ uses.
3. Keep the accountant from shrinking the tier toward zero under light load (only shrink under genuine global pressure).

## Caveats
- temp_file_memory_size_in_pages=4 (min) was set to FORCE spill for these tests; it equally penalizes both legs,
  so it is fair for the redesign-vs-develop delta but is not a production buffer setting.
- Numbers are non-TDE (raw-fd off -> develop spill path). TDE adds the accepted encryption cost on top.
