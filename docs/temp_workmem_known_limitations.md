# temp work_mem redesign — Known performance limitation: parallel SORT / DISTINCT heavy spill

Status: **accepted known limitation (future work).** Functional correctness is intact;
this is a performance-only regression confined to one operator family.

## Summary

The raw-fd single-overflow temp-spill redesign is **correct** for all operators
(parallel == serial md5 parity, orphan-zero on crash, and TDE crypto all pass), and the
original mutex-contention regression has been **fixed** (VTune `pthread_mutex` self-CPU
356s -> 26.4s via the registry mutex split + O(1) secondary index + 64-shard partitioning).

However, parallel **SORT** and **DISTINCT** (sort-based) remain slower than the `develop`
pgbuf baseline on heavy-spill workloads. Hash operations (hash-agg, hash-join) are
unaffected or improved.

## Measured (clean host, tpch_sf10 @ data_buffer_size=512M, matched 8MB sort memory, RELEASE, 3x median)

| workload                 | develop | redesign | delta   |
|--------------------------|---------|----------|---------|
| HEAVY DISTINCT (tpch)    | 42.5s   | 64.0s    | +50%    |
| HEAVY HASHAGG (tpch)     | 25.3s   | 29.6s    | +17%    |
| LIGHT SORT (wmloc 4.19M) | 4.0s    | 4.5s     | +14%    |
| LIGHT HASHAGG (wmloc)    | ~3.5s   | ~3.5s    | ~flat   |

The residual gap is pure I/O, not lock contention (the mutex regression was already fixed).

## Root cause

`base Phase A` (raw-fd activation, commit `f2384bd92`) routes the parallel-sort **worker
output lists** through raw-fd workmem — an extra **15.13 GiB** of raw-fd I/O across the 7
worker output lists — where `develop` kept them on the pgbuf VFID temp volume. Specifically
base Phase A:

1. **Repurposed the `QFILE_NOT_USE_MEMBUF` bit (0x0800)** as `QFILE_FLAG_PRIVATE_SPILL`,
   removing develop's "bypass membuf onto the pgbuf-VFID temp volume" capability for these
   transient lists.
2. **Split the qfile read/scan dispatch by backing type**:
   - `RAW_FD_OVERFLOW` -> uniform `NULL_VOLID` contiguous-pageid scanner (handles
     membuf-base + overflow as one space; correct but slow).
   - `PRIVATE_SPILL_FALLBACK` -> `next_vpid` chain walk, contract requires `membuf == NULL`.
   This **lost develop's unified `membuf-base + real-VPID-overflow` chain walk**, so the
   redesign cannot read a mixed-backing sort list correctly.

Consequence: the redesign's only **correct** sort backings are both slow — raw-fd (correct,
+15 GiB I/O) or membuf-null `PRIVATE_SPILL` (correct, but no in-memory tier so everything
spills to pgbuf). Any **fast** mixed backing (membuf base + real-VPID overflow) **truncates
output** because the split read path follows only one backing.

## Surgical fixes attempted (all empirically failed; all reverted)

1. Restore zero-copy `qfile_connect_list` worker merge -> garbled order (raw-fd `NULL_VOLID`
   pageids are ambiguous across files; the chain relink resolves to the wrong file).
2. Route worker lists via `QFILE_FLAG_PRIVATE_SPILL` -> `qmgr_create_result_file`
   (`FILE_QUERY_AREA`) -> catastrophic ~1571s (membuf=NULL forces 100% through pgbuf + a
   serial copy-merge re-encrypting the whole spill + TDE global-nonce mutex).
3. Coordinated real-VPID backing (`no_raw_fd_spill` divert) + zero-copy connect -> output
   truncated ~60% (mixed-backing list, split read path drops the real-VPID overflow region).
4. Real-VPID backing only, copy-merge unchanged -> **byte-identical truncation** to (3),
   proving the **backing divert** (not the merge) is the cause.

## Recommended fix (future work — not implemented)

Scope raw-fd to hash operations only and **restore `develop`'s sort/distinct path**: either
re-establish a unified read/scan dispatch that traverses a `membuf-base + real-VPID-overflow`
list (develop's behavior), or surgically revert base Phase A's sort-specific raw-fd
activation while keeping the hash-agg raw-fd win. Estimated medium-large effort. The
`PRIVATE_SPILL_FALLBACK` backing must only ever be the **creation-time** backing of a
`membuf == NULL` list — never bolted onto a membuf-base list at overflow (add a release
assert `backing == PRIVATE_SPILL_FALLBACK => membuf == NULL` to convert silent truncation
into a hard failure).

## Evidence

`~/.claude/scratch/remote-codex/{perf_reckoning,perf_memiso,perf_bisect,sortprobe,coordfix_qa,backingonly_qa}/`
and the ultragoal ledger `.gjc/.../ultragoal/ledger.jsonl`.
