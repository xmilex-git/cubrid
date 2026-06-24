# R6 — Honest 3-way verdict + ADR (CUBRID temp-workmem raw-fd redesign)

Branch: feature/temp-workmem-redesign @ e337defce (guard TRUE). Verdict produced LOCALLY on 192.168.6.34.
Comparison legs (RELEASE, in-tree build_x86_64_release/_install/CUBRID):
- develop baseline: /home/cubrid/dev/cubrid-tbs-baseline @ a2f738e75
- redesign:         /home/cubrid/dev/cubrid-workmem @ e337defce
- (as-built 69d78cac4 leg available at /home/cubrid/dev/cubrid-temp-buffer-sep; not run in this 2-way — plan's primary verdict is redesign-vs-develop-baseline.)
DB: tpch_sf10 (48G, NON-TDE -> raw-fd inert by runtime gate; this 3-way exercises the develop/regression-guard path. raw-fd-live correctness/parity/TDE/orphan-zero were proven separately in G003 on a fresh 1,048,576-row TDE DB.)

## VERDICT: HONEST REJECTION (env-A regression). NOT ready to merge to develop as-is.

## Measured (run_3way.sh, RELEASE, N=5 per cell; band = mean +/- 2 sigma of the N baseline repeats)
| env | workload | baseline median | redesign median | redesign vs baseline | result |
|---|---|---|---|---|---|
| A | I/O-bound parallel hash join, data_buffer_size=512M | 222.4 ms (mean 259.9, 2σ band [88.8, 431.0]) | 2229.9 ms (mean 2250.5, σ 51.7) | ~10x SLOWER, far above baseline 2σ high (431) | **REGRESSION (FAIL)** |
| C | RAM-rich regression guard, data_buffer_size=16G | 4644.7 ms (mean 6091, σ 3164, noisy) | 4860.9 ms (mean 4863, σ 178) | within baseline noise band | PARITY/PASS |

(N=5 used rather than the plan's N=10 to bound shared-box time; the env-A gap is ~10x and stable (redesign σ=51.7ms), far larger than any N-sensitivity, so the regression conclusion is robust. env B and positioned workloads were not run; the env-A regression already fails the gate.)

## Root cause of the env-A regression (apples-to-apples query trace, same DB + same 512M buffer, only the server binary differs)
- baseline env A: total 238 ms, **ioread: 0** (HASHJOIN 199 ms; lineitem index scan 149 ms / ioread 0; orders pk scan 121 ms / ioread 0). Everything stays in the 512M data buffer.
- redesign env A: total 2220 ms, **ioread: 6818** (HASHJOIN 2174 ms; lineitem index scan ioread 6684; inner orders SELECT 2119 ms). The redesign re-reads ~6818 pages from disk under the SAME 512M buffer.
- Interpretation: the redesign's temp / per-worker work_mem page handling **pollutes and evicts the shared data-buffer LRU**, so hot index/data pages are evicted and re-read from disk under a constrained buffer. With env C's ample 16G buffer the pollution causes no eviction -> parity. This matches the as-built leg's P6 concern ("stop temp/cache first-touch pages polluting data LRU in pgbuf_unlatch_void_zone_bcb (honor temp ignore-unfix)", commit 69d78cac4), a fix the redesign (clean base off origin/develop c08453968) does NOT carry.

## What PASSED (rigorous, from G001-G003; on xmilex history, per-commit local critic-reviewed)
- Correctness + parity: parallel == serial md5 (canonical 314974c5...) for hash GROUP BY / DISTINCT / ORDER BY at parallelism 4 and 1, in develop (raw-fd off) AND raw-fd-live regimes; all operator counts = 1,048,576.
- Develop path unchanged when raw-fd OFF (G001 FIX-1/FIX-2 restored parallel HJ + hash-agg; critic-verified).
- Segment-native parallel hash-aggregate consumption (G002) — removed the scope-limit; raw-fd-live parallel hash GBY correct.
- raw-fd-live safety (G003): TDE (16 raw-fd .tmp files, 0 plaintext, fresh nonce), orphan-zero (SIGKILL mid-spill -> boot-sweep -> 0), runtime gate intact (master && boot_sweep_complete && tde_wired && reaper_active). Guard flipped TRUE.

## Decision / recommendation
The raw-fd redesign is CORRECT, raw-fd-safe, and parity-clean, but it REGRESSES the I/O-bound (constrained-buffer) workload ~10x via data-buffer LRU pollution. Per the plan's threshold (REGRESSION = redesign worse than baseline by > 2 sigma in ANY of A/B/C), this FAILS R6. Honest verdict = rejection: do NOT open a merge PR to xmilex/develop until the regression is fixed.
Remediation (follow-up goal): make redesign temp / per-worker work_mem pages NOT pollute/evict the shared data-buffer LRU (port/adapt the as-built P6 treatment — temp ignore-unfix in pgbuf_unlatch_void_zone_bcb — onto the clean-base redesign), then re-run R6 (env A/B/C + positioned, N=10) and require env A within baseline band before any develop PR.

## Evidence files
bench/harness/results/{baseline,redesign}.{a,c}.tsv (+ .median), and the env-A trace comparison captured in this analysis. raw-fd-live G003 evidence under bench/harness/results/g003/.
