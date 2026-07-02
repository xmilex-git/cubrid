# Tapeset/BufFile Measurement Hooks (redesign G003, issue #68)

Purpose: track what the G003 measurement hooks (`buffile_metrics`, `tapeset_scan_metrics`, the
4 in-server selftests) actually prove today, so a future reviewer doesn't have to re-derive it
from scratch. Issue #93 found several of these hooks were tautological (see "Fixed by #93"
below) and re-wired them; this checklist is the record of what changed and how to re-verify it.

## Hook inventory

| Hook | Where | What it measures |
|---|---|---|
| `buffile_metrics.pages_appended/pages_flushed/flush_calls/bytes_written` | `qfile_buffile.hpp` | producer write-path bookkeeping (never tautological -- always incremented by `append_page`/`flush`) |
| `buffile_metrics.pages_read` | `qfile_buffile.hpp` | `read_page()` disk reads |
| `buffile_metrics.pgbuf_fixes` | `qfile_buffile.hpp` | producer-side pgbuf-bypass gate: MUST be 0 across `append_page`/`flush` |
| `tapeset_scan_metrics.{page_reads,tuple_reads,tape_advances,jumps,copies,peeks}` | `qfile_tape.hpp` | scan-side operation counters (never tautological) |
| `tapeset_scan_metrics.pgbuf_fixes` | `qfile_tape.hpp` | scan-side pgbuf-bypass gate: MUST be 0 across a `tapeset_scan`/`tapeset_reader`'s page touches |
| `CUBRID_BUFFILE_SELFTEST` | `qfile_buffile_selftest` (`qfile_buffile.cpp`), gated in `qmgr_initialize` | in-server BufFile write/flush/read round-trip incl. TDE |
| `CUBRID_HELDTAPE_SELFTEST` | `qfile_heldtape_selftest` (`qfile_tape.cpp`) | held/frozen Tape round-trip |
| `CUBRID_TAPEREAD_SELFTEST` | `qfile_taperead_selftest` (`qfile_tape.cpp`) | N-reader concurrent `tapeset_reader` read (ADR 0005) |
| `CUBRID_PRODUCER_SELFTEST` | `qfile_producer_selftest` (`qfile_tape.cpp`) | producer freeze path |

## Fixed by #93 (previously tautological or silent)

1. **`pgbuf_fixes` never incremented anywhere in `src/`** -- both fields were permanently 0 by
   construction, so every `!= 0` check (including unit gates G7/G8/G10/G18) could never fail
   regardless of whether the pgbuf-bypass invariant actually held. Fixed by a boot-independent
   debug counter (`pgbuf_get_fix_debug_count()`, `src/storage/page_buffer.h`) bumped unconditionally
   at the top of the real `pgbuf_fix_debug()` entry point, snapshot-diffed across the
   producer/scan region (`buffile::refresh_pgbuf_fixes`, `qfile_tape.cpp`'s free-function
   `refresh_pgbuf_fixes`). Anti-tautology proof: unit gate **G20**
   (`run_pgbuf_fixes_anti_tautology`, `test_tapeset_scan.cpp`) injects one simulated fix via the
   TEST-ONLY `pgbuf_test_bump_fix_debug_count()` hook and asserts the gate would now see it.
2. **`pages_read` never incremented** -- `read_page()` only bumped a separate, never-read
   `m_reads` atomic. Consolidated: `m_reads` removed, `buffile_metrics.pages_read` is now the
   atomic counter `read_page()` updates directly (kept atomic because reads are re-entrant/
   concurrent post-freeze, ADR 0005; `pgbuf_fixes` stays a plain `long` since it's only ever
   touched from the single-writer producer path). **G8** (`run_file_parity`) asserts
   `pages_read == wr.file_pages` right after a single forward pass over the fixture's 4 spilled
   pages, via `dynamic_cast<buffile_tape *>(...)->backing()->metrics().pages_read`.
3. **selftest return codes discarded** -- `qmgr_initialize` logged `SELFTEST result=%d` but never
   checked it; a FAIL was indistinguishable from a PASS short of reading the number by hand. Now
   each of the 4 selftests additionally logs a `<NAME>_SELFTEST FAIL result=%d` marker and asserts
   (debug-only, matching the existing `#if !defined (NDEBUG)` scope) when `rc != NO_ERROR`.
   `gate_tapeset_scan.sh` (below) greps the server's er_log for this.
4. **Unit scratch escaped `/tmp`** -- `test_tapeset_scan.cpp`'s `test_scratch_dir()` hardcoded
   `/tmp/cubrid_buffile_ut_<pid>`. Now resolves `$CUBRID_TMP` first, `$TMP` second, `/tmp` only as
   a last resort -- mirroring `qfile::buffile::default_scratch_dir`'s own fallback chain.

## Re-verifying

- **Unit gates (bootless, no server)**: build `test_tapeset_scan` (see `unit_tests/tapeset/`,
  requires `-DUNIT_TEST_TAPESET=ON`) and run it; G1-G20 must all report PASS, including the new
  G20 anti-tautology case.
- **In-server selftests + gating**: `./gate_tapeset_scan.sh` (this directory). Restarts the
  harness DB with all 4 `CUBRID_*_SELFTEST` env vars set on the **server process's** environment
  and greps the resulting er_log for `result=0` per selftest and for the absence of any
  `SELFTEST FAIL` marker. Run `./preflight.sh` first (or let the gate script call it) to catch a
  stale/missing build or a PL-boot-failure-prone conf before spending a restart on it.

## Fresh-clone execution

```sh
cd bench/harness
./preflight.sh                 # read-only [OK]/[MISS]/[WARN] report; fix any [MISS] before continuing
./gate_tapeset_scan.sh         # restarts $DB_NAME (default tpch_sf10; export DB_NAME=... to override)
```

Both scripts source `lib.sh` for `$CUBRID`/`$DB_NAME`/the `cubrid-server-control` wrapper, so the
usual harness env vars (`BUILD_NAME`, `DB_NAME`, `CUBRID_DATABASES`, ...) apply unchanged.
