# Baseline Runs

R0 base-skew pin:

- Baseline: `develop` at `c08453968`.
- Redesign R0: `feature/temp-workmem-redesign` at `c08453968` plus `bench/harness/**` only.
- As-built comparison: `xmilex/feature/temp-buffer-separation` at `69d78cac4`; full N=10 3-way is deferred to R6.

Build command selected by repository contents: this tree has `build.sh` and no `justfile`; use
`./build.sh -m release -g ninja -p <leg-install> build` for measurement legs.

R0 builds used:

- Baseline worktree: `/home/cubrid/dev/cubrid-wm-baseline`.
- Baseline install: `/home/cubrid/dev/cubrid-wm-baseline/build_x86_64_release/_install/CUBRID`.
- Redesign worktree: `/home/cubrid/dev/cubrid-workmem`.
- Redesign install: `/home/cubrid/dev/cubrid-workmem/build_x86_64_release/_install/CUBRID`.
- As-built worktree: `/home/cubrid/dev/cubrid-wm-asbuilt`.
- As-built install: `/home/cubrid/dev/cubrid-wm-asbuilt/build_x86_64_release/_install/CUBRID`.

The harness raises the selected install's `conf/cubrid.conf` `max_clients` to
`HARNESS_MAX_CLIENTS` (default `200`) before server restart. This is required for
the K=128 concurrency cell; the stock `max_clients=100` rejects the last clients
and measures connection failure rather than query throughput.

R0 fix round config pins:

- Env A remains the I/O-bound small-buffer cell: `data_buffer_size=512M`.
- Env C is the RAM-rich guard: `data_buffer_size=16G`, `parallelism=8`,
  `max_parallel_workers=8`, and `HARNESS_C_CONCURRENCY=8`.
- Positioned baselines use distinct SQL files:
  `queries/pos_hj_probe.sql`, `queries/pos_connect_by.sql`, and
  `queries/pos_mutation.sql`.
- Env B high-concurrency bands use `queries/B_concurrency_sort.sql`; the
  heavier `queries/B_parallel_sort.sql` is the real parallel-sort plan-gate
  proof query.
- The C plan gate records that the LRU mutex wait timers remain zero on this
  build under the RAM-rich workload, and gates on the exposed
  `Num_data_page_victim_candidate` runtime counter instead.

Harness conventions:

- N=10 timed repeats per env/workload unless `HARNESS_N` is set for smoke.
- Noise band is mean +/- 2 sigma from the N baseline repeats. Env B records
  separate `qps`, `p50_ms`, `p95_ms`, and `rc_failures` rows for K=64 and K=128.
- CSQL commands run with `</dev/null`.
- Server start/stop/restart uses `.agents/skills/cubrid-server-control/scripts/cubrid-server-ctl.sh`.

`baseline.bands.tsv` is generated from `bench/harness/results/baseline.*.tsv`.
The as-built smoke leg may use `HARNESS_N=2`; a full as-built N=10 comparison is not a P1a/R0 gate.
R0 as-built smoke passed with `HARNESS_N=2 bash bench/harness/run_3way.sh c asbuilt`;
see `bench/harness/results/asbuilt.c.tsv`.
