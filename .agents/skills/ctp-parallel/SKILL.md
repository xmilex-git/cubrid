---
name: ctp-parallel
description: >-
  Run the CUBRID CTP SQL regression suite in N parallel rootless-podman shards on
  one host, finishing in ~1/N wall-clock time. Use when asked to "run ctp in
  parallel", "parallel sql regression", "shard the sql suite", "split CTP across
  containers", "ctp 병렬 실행", or "딸깍 ctp". Splits the suite via per-shard
  exclusions.txt, runs each shard in an isolated container, and merges one
  pass/fail summary. Requires podman; without it, use --dry-run to validate the split.
---

# ctp-parallel — one-click parallel CTP SQL runner

Runs the full CTP SQL suite split across **N isolated rootless-podman containers**
(one per shard) and merges the results into a single pass/fail summary. Isolation
is by podman namespaces (net + IPC + mount), so every shard safely reuses the same
ports/SHM IDs; only each shard's `exclusions.txt` and output dirs differ.

## One-click recipe

1. **Find the two required inputs:**
   - `--build <dir>` — a built `$CUBRID` directory (the engine under test).
   - `--testcases <root>` — the testcases repo root (scenario is `<root>/sql`,
     normally `~/cubrid-testcases`).
   If the user has a built tree (e.g. `$CUBRID`) and `~/cubrid-testcases`, use those.

2. **Run the orchestrator** — no tuning needed; the defaults are the optimal setting:

   ```bash
   .claude/skills/ctp-parallel/scripts/ctp_parallel.sh \
     --build "$CUBRID" \
     --testcases ~/cubrid-testcases
   ```

   A bare run uses the **fixed optimal configuration**:
   - **7 shards** (`DEFAULT_SHARDS`) — the workload knee: the heaviest single bulk
     (~323 s) bounds the slowest shard, so `N* = ceil(total/heaviest) = 7`; more shards
     just sit idle. RAM-capped down only if 7 won't fit.
   - **Split unit = top-level `_*` directory ("bulk")** — exactly CircleCI's sql unit;
     each bulk runs WHOLE on one shard (never split), so tests stay co-located as CI
     groups them, avoiding the cross-test/shared-DB interference finer splits expose.
   - **Time-balanced automatically** — bundled `baseline_weights.tsv` (real per-case
     seconds) auto-loads; each bulk's weight = sum of its cases' seconds. Refresh via
     `scripts/harvest_weights.sh`; `--no-weights` reverts to case count.

   This default config ran the full suite **17,420 pass / 0 fail / 0 core**. Common
   overrides: `--shards <N>` (e.g. `10` for CircleCI parity),
   `--out <dir>`, `--image <ref>`, `--keep`, `--by-dir` (finer: outermost `cases/` dir —
   better balance, weaker isolation), `--by-case` (finest: per-`.sql`, NOT order-safe),
   `--overlay` (overlay the build instead of copying — experimental). `colocate.tsv`
   (`--colocate`/`--no-colocate`) aids `--by-case` only.

3. **Validate the split without running anything** (no podman needed):

   ```bash
   .claude/skills/ctp-parallel/scripts/ctp_parallel.sh \
     --dry-run --testcases ~/cubrid-testcases
   ```

   This discovers split units, balances them (greedy-LPT), writes per-shard
   `exclusions.txt` + `sql.conf` + `assignment.tsv` under `--out`, and runs the
   offline split-validator (proves every `.sql` runs in exactly one shard).

## Reading the result

The run ends with an `AGGREGATE` table: per-shard `rc / fail / success / total`
plus an `ALL` row. It **exits non-zero** if any shard has `fail>0`, a shard
crashed, or a split invariant is violated (`Σ total != surviving .sql`).
Per-shard artifacts (console log, CTP results/logs, generated conf, exclusions)
are preserved under `<out>/shard_<i>/`.

By default every run is also **merged into webconsole**: the per-shard results are
combined into one schedule under `$CTP_HOME/sql/result`, viewable as a single entry
via `$CTP_HOME/bin/ctp.sh webconsole start` (open `http://<host>:8888`). Skip with
`--no-webconsole`. To make an *already-finished* run viewable (e.g. one run with
`--no-webconsole`), merge it after the fact without re-running:

```bash
.claude/skills/ctp-parallel/scripts/ctp_parallel.sh \
  --merge-only <out-dir> --label <tag>     # merges <out-dir> into webconsole, then exits
```

## Requirements

- **podman** (rootless). If absent the orchestrator prints a clear error and exits
  non-zero — use `--dry-run` to still validate the split logic.
- **Image**: by default the orchestrator builds `ctp-parallel:local` on demand from
  the bundled `scripts/Containerfile` (a Rocky 8 runtime with JDK 8 + gcc + en_US
  locale). The image's glibc must match the `--build` you mount: a CUBRID built on a
  modern host (e.g. Rocky 8 / glibc 2.28) will NOT run on the CI build image
  `cubridci/cubridci:develop` (CentOS 6 / glibc 2.12). Override with `--image <ref>`
  only if you have a glibc-compatible image.
- On **cgroup v1** rootless hosts, containers are launched with `--cgroupns=private`
  automatically (the default ns fails to mount the systemd cgroup hierarchy).
- `java` is only needed inside the image, not on the host.

## More

- `scripts/Containerfile` — the per-shard image (build with `podman build`).
- `scripts/entrypoint.sh` — in-container runner (preflight + `ctp.sh sql`).
- `scripts/harvest_weights.sh` — per-`.sql` time table from run logs, for `--weights`.
- `colocate.tsv` — order-sensitivity registry (keep-whole / co-locate on one shard).
- `test/run_tests.sh` — static + logic self-tests (run without podman).
- `README.md` — design rationale and the **Manual e2e QA** steps for a podman host.
