# ctp-parallel

One-click tool to run the CUBRID **CTP SQL regression suite in N parallel shards**
on a single host, mirroring CircleCI's `test_sql` job (`parallelism: 10`). Each
shard runs the real `ctp.sh sql` inside an **isolated rootless-podman container**
against a **private, pristine copy** of the build / scenario / CTP-conf. The suite
is partitioned across shards via per-shard `exclusions.txt`, and results merge into
one pass/fail summary. Wall-clock time drops to ~1/N.

## Deliverables

| File | Role |
|------|------|
| `scripts/Containerfile` | **#1** Per-shard runtime image. `FROM rockylinux:8` + JDK 8 + gcc + en_US locale + `entrypoint.sh`. glibc matches a modern host build; the CI build image (CentOS 6 / glibc 2.12) is too old to run one. State is mounted at run time, not baked in. |
| `scripts/ctp_parallel.sh` | **#2** Host-side orchestrator (main entry point): split → validate → launch → aggregate. |
| `scripts/entrypoint.sh` | In-container runner: preflight (D5 relocation guard) + `exec ctp.sh sql`. |
| `scripts/harvest_weights.sh` | Derive a per-`.sql` time table from run logs (refreshes `baseline_weights.tsv`). |
| `baseline_weights.tsv` | Bundled per-`.sql` times (real green-run seconds); auto-loaded for time balancing. |
| `colocate.tsv` | Order-sensitivity registry (used by `--by-case`): dirs kept whole / co-located. |
| `SKILL.md` | **#3** One-click skill wrapper. |
| `test/run_tests.sh` | Static + logic self-tests (run **without** podman). |

## Why containers (isolation)

Running N CTP processes directly on one host collides on TCP ports (1822/33120),
SysV SHM IDs (33122/33120), the rewritten `$CUBRID/conf`, DB names, and concurrent
`.result` writes into one scenario tree. Rootless podman isolates each shard:

- **net namespace** (podman default; **no `--network=host`**, no published ports) —
  each shard's `localhost` is private, so the fixed broker/server ports never clash.
- **IPC namespace** (`--ipc=private`) — private SysV SHM space, so the fixed
  `MASTER_SHM_ID` / `APPL_SERVER_SHM_ID` never collide. `/dev/shm` is sized with
  `--shm-size`.
- **mount namespace** — per-shard writable copies of build / scenario / CTP-conf.

**Key simplification:** every shard reuses the *same* ports/SHM IDs. The only
per-shard differences are its `exclusions.txt` and its output directories.

## How the split works

- **Split unit = top-level `_*` directory ("bulk"), exactly CircleCI's sql unit.**
  `.circleci/config.yml` globs `cubrid-testcases/sql/_*` and ships each match WHOLE to
  one node; this tree has **35** such bulks holding all **17,420** `.sql`. A bulk is
  **atomic — never split across shards** — so every test inside a `_*` dir stays
  co-located in canonical order, *exactly* as CI groups them. That co-location is what
  keeps the suite green: finer splits move tests apart and expose cross-test/shared-DB
  interference (each shard runs ONE database for all its cases, and not every test
  self-isolates), producing failures that depend on which tests share a shard.
  Reproducing CI's grouping avoids that whole class. **Tradeoff:** the heaviest single
  bulk bounds the slowest shard (here `_01_object` ≈ 560 s vs a ~305 s ideal), since a
  bulk cannot be split. Finer opt-ins exist: **`--by-dir`** (outermost `cases/` dir,
  ~1157 units, better balance but co-locates fewer related tests) and **`--by-case`**
  (per-`.sql`, NOT order-safe).
- **Base exclusions merged first (D4):** the existing `$CTP_HOME/conf/exclusions.txt`
  is applied with exact CTP semantics (`CommonUtils.containPath`: trim, `\`→`/`,
  append `/` unless the entry ends in `/` or `.sql`, then substring `indexOf`) to
  pre-remove globally-excluded `.sql` from the pool. Each shard's exclusion file =
  the base list (verbatim) **+** every unit not assigned to that shard.
- **Balance (greedy-LPT, D2):** bulks are packed descending into the least-loaded
  shard (deterministic; name tie-break). By **count** the bulks are lumpy (one bulk
  may hold 3000+ cases); the right metric is **time** (below).
- **Time balancing (automatic, D7):** counts ≠ wall-time, and bulks vary widely (here
  `_04_operator_function` has 1618 cases in ~48 s while `_05_plcsql` has 1249 cases in
  ~323 s). A bundled `baseline_weights.tsv` (real per-case seconds) is loaded by default,
  so each bulk's weight = the sum of its cases' seconds and LPT packs bulks by measured
  time with no flags. Refresh it with `scripts/harvest_weights.sh` (parses the
  `[HH:MM:SS] Testing … .sql` log lines); `--weights <file>` overrides, `--no-weights`
  reverts to count. **Floor:** the slowest shard can't beat the heaviest single bulk's
  total time (a bulk is atomic by design).
- **Order-sensitivity registry (`colocate.tsv`, D6):** applies to **`--by-case` only**
  — it keeps listed `cases/` dirs whole there (in bulk/`--by-dir` they are already
  atomic). A line with 2+ dirs also pins them to the SAME shard (all modes). Override
  with `--colocate <file>`, disable with `--no-colocate`.
- **Webconsole merge (default ON):** at the end of every run the per-shard CTP result
  dirs are merged into ONE schedule under `$CTP_HOME/sql/result`, so the whole parallel
  run shows as a single browsable entry in `ctp.sh webconsole start` (`--no-webconsole`
  to skip). To merge an already-finished run (e.g. one done with `--no-webconsole`), use
  `--merge-only <out-dir> [--label <tag>]` — it merges that out dir and exits (no podman /
  build / testcases needed). `--label` tags the run in the webconsole 'machine' field.
- **Offline split-validator:** before launching anything, it replays `containPath`
  to prove every surviving `.sql` is alive in **exactly one** shard (0 duplicates,
  0 orphans), and aborts otherwise.

## Usage

# A bare run uses the fixed optimal config: 7 shards, bulk(_*) split, auto time-weights.
```bash
# Real parallel run (needs podman) — no flags needed beyond build + testcases:
scripts/ctp_parallel.sh --build "$CUBRID" --testcases ~/cubrid-testcases   # = 7 shards, bulk, time-balanced

# Plan + validate only, no podman, no --build needed:
scripts/ctp_parallel.sh --dry-run --testcases ~/cubrid-testcases --out ./plan
```

Run `scripts/ctp_parallel.sh --help` for all flags (`--ctp`, `--image`, `--out`,
`--overlay`, `--by-category`, `--by-dir`, `--by-case`, `--weights`, `--no-weights`,
`--colocate`, `--no-colocate`, `--keep`).

**Time balancing is automatic.** A bundled `baseline_weights.tsv` (real per-case seconds
from a green run) is loaded by default, so bulks are packed by measured time with no extra
flags. To refresh it after the suite changes, harvest a new table and replace the bundle (or
pass `--weights`); use `--no-weights` to fall back to case-count balancing:

```bash
# refresh the bundled time table from a prior run's per-shard logs:
scripts/harvest_weights.sh --out baseline_weights.tsv ./ctp-parallel-out/shard_*/console.log
# (or use a one-off table for a single run:)
scripts/ctp_parallel.sh --build "$CUBRID" --testcases ~/cubrid-testcases --weights my.tsv
```

Output lands in `--out` (default `./ctp-parallel-out`): `assignment.tsv`,
`units.tsv`, `plan.tsv`, and `shard_<i>/{exclusions.txt,sql.conf,console.log,out/}`.
The orchestrator exits non-zero if any shard fails, crashes, or an invariant breaks.

## Self-tests (no podman)

```bash
cd ~/dev/cubrid
bash -n .claude/skills/ctp-parallel/scripts/ctp_parallel.sh
bash -n .claude/skills/ctp-parallel/scripts/entrypoint.sh
bash    .claude/skills/ctp-parallel/test/run_tests.sh
```

`run_tests.sh` asserts (against the real trees): unit discovery vs direct `find`,
partition disjointness/coverage for N∈{1,4,10}, the offline validator on the real
tree **and** on a synthetic ambiguous fixture (must be flagged), the surviving-sql
invariant, balance (max-shard ≤ 1.5×mean), config generation (scenario =
`/home/cubrid-testcases/sql`, F2 ports/SHM verbatim, no host paths), and the
podman-missing preflight. It prints `ALL TESTS PASSED (k checks)` on success.

## Manual e2e QA (requires a podman host)

These cannot be verified on a host without podman (this dev box has none), so run
them on a podman-capable host. They are the real acceptance checks for the
container path.

```bash
cd ~/dev/cubrid/.claude/skills/ctp-parallel/scripts

# 0. Build the per-shard image (or just use the base image directly).
podman build -t ctp-parallel:local -f Containerfile .
#    Override the base if needed:
#    podman build --build-arg BASE_IMAGE=<ref> -t ctp-parallel:local -f Containerfile .

# 1. Real 4-shard run against a built engine + testcases.
./ctp_parallel.sh \
  --build "$CUBRID" \
  --testcases ~/cubrid-testcases \
  --image ctp-parallel:local \
  --shards 4 \
  --out ./ctp-parallel-out \
  --keep

# 2. Confirm port/SHM NON-collision with 2+ live shards: while the run is in
#    flight, every shard's broker/server is up on the SAME ports inside its own
#    net/IPC namespace with no EADDRINUSE / shmget collisions:
podman ps --filter "name=ctp_shard_" --format '{{.Names}} {{.Status}}'
for c in $(podman ps -q --filter "name=ctp_shard_"); do
  echo "== $c =="; podman exec "$c" sh -lc 'cubrid broker status 2>/dev/null | head; ipcs -m | head'
done

# 3. Aggregate equivalence: a sharded run's combined pass/fail must equal a single
#    full CTP run. Compare totals:
#    a) single run (one shard == whole suite):
./ctp_parallel.sh --build "$CUBRID" --testcases ~/cubrid-testcases \
   --image ctp-parallel:local --shards 1 --out ./ctp-1shard
#    b) parallel run (e.g. 10 shards):
./ctp_parallel.sh --build "$CUBRID" --testcases ~/cubrid-testcases \
   --image ctp-parallel:local --shards 10 --out ./ctp-10shard
#    Then diff the AGGREGATE 'ALL' rows: total and fail counts must match, and the
#    set of failing cases must be identical (modulo shard grouping).

# 4. Inspect a shard's artifacts:
ls ./ctp-parallel-out/shard_0/           # console.log, exclusions.txt, sql.conf, out/
cat ./ctp-parallel-out/shard_0/console.log
```

**Deferred (no podman on this dev host):** real container launch, port/SHM
non-collision with 2 live shards, and "aggregate pass/fail == single-CTP-run
pass/fail". Everything else (split, validator, exclusions merge, invariants,
config generation, lint) is fully verified by `test/run_tests.sh`.

## Decision log

- **D1** default = per-shard `cp -a` of the build (overlay `:O` reliability is
  unproven rootless); `--overlay` opts into the overlay mount.
- **D2** split unit = top-level `_*` "bulk" (= CircleCI's sql unit), atomic per shard,
  packed greedy-LPT (by time with `--weights`, else count). Mirroring CI's grouping
  avoids the cross-test interference finer splits expose. `--by-dir` / `--by-case` are
  finer opt-ins (better balance, weaker isolation).
- **D3** scenario isolation = pristine per-shard copy (rsync, `*.result`/`*.log`
  excluded); never a shared writable scenario.
- **D4** base `exclusions.txt` is merged into every shard list and pre-removed from
  the pool — an invariant, no escape hatch.
- **D5** copied builds: `export CUBRID` before sourcing `.cubrid.sh`, assert it
  wasn't relocated afterward.
- **D6** isolation comes from matching CI's bulk grouping (default), so `colocate.tsv`
  is a narrow aid for `--by-case` only: keep listed `cases/` dirs whole there, and
  optionally pin co-dependent dirs to one shard. Opt-out via `--no-colocate`.
- **D7** balance by **measured time, on by default** — a bundled `baseline_weights.tsv`
  (real per-case seconds from a green run) auto-loads, no flag needed; `--weights`
  overrides, `--no-weights` reverts to count. With atomic bulks the slowest shard is
  bounded by the heaviest single bulk — accepted, in exchange for CI-parity / no
  isolation-induced failures. `--by-dir` trades that isolation for finer balance.
