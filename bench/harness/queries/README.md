# Forced Query Set

Database: `tpch_sf10`, `CUBRID_DATABASES=~/databases`.

The plan gate uses `;plan detail` plus `;trace on` so `bench/harness/results/plan_check.txt`
contains both optimizer plan trees and execution trace statistics.

## A_parallel_hj.sql

Hash-join target over bounded `lineitem`/`orders`, with `USE_HASH(o,l) PARALLEL(8)`.
R0 FIX verification requires real trace evidence:

- `Query Plan` contains `HASH JOIN`.
- `Trace Statistics` contains `HASHJOIN`, `BUILD`, and `PROBE`.
- Runtime trace contains `parallel workers` scan/subquery markers.

## B_parallel_sort.sql

RAM-resident but real parallel sort target over `orders`, with `PARALLEL(8)`.
The corrected query uses a heap scan so the trace shows actual parallel workers.
R0 FIX verification requires:

- `Query Plan` contains `SORT (order by)` and `SORT (group by)`.
- `Trace Statistics` contains `parallel workers: 8`.
- `GROUPBY`/`ORDERBY` trace entries show `sort: true`.

`B_concurrency_sort.sql` is the bounded high-concurrency env-B workload used by
`env_b_concurrency.sh` for K=64/K=128 baselines. It is kept separate because the
real heap-scan parallel-sort proof query is intentionally heavier.

## C_driver.sql

RAM-rich guard over `orders` with `PARALLEL(8)`. `run_3way.sh c ...` configures
`data_buffer_size=16G`, `parallelism=8`, and `max_parallel_workers=8` before
restart; `env_c_ramrich_guard.sh` proves those values with `paramdump` and runs
eight concurrent clients.

For the plan gate, this build keeps the LRU mutex wait timers at zero under the
RAM-rich C workload. The R0 FIX gate therefore records that limitation and gates
on the exposed buffer victim-candidate runtime counter:
`Num_data_page_victim_candidate_delta > 0`.

## Positioned Workloads

These are distinct from the A/B/C forced query set.

- `pos_hj_probe.sql`: self-join on `orders` with `USE_HASH(o1,o2) PARALLEL(8)`.
  The trace shows `HASHJOIN`, `BUILD`, `PROBE`, and parallel worker scan markers,
  exercising repeated hash probe/lookups.
- `pos_connect_by.sql`: real `CONNECT BY` generated hierarchy over `db_root`,
  exercising hierarchical parent-position save/restore.
- `pos_mutation.sql`: real `CONNECT BY NOCYCLE` with `CONNECT_BY_ISLEAF` and
  `CONNECT_BY_ISCYCLE` projection. This is the pure-SQL path for
  `qfile_set_tuple_column_value` pseudocolumn mutation in `query_executor.c`.

Confirmed plan/trace output is written by `bench/harness/plan_check.sh` to
`bench/harness/results/plan_check.txt`; positioned verification traces are in
`bench/harness/results/positioned_plan_check.txt`; positioned timings are
written by `bench/harness/run_3way.sh positioned baseline`.
