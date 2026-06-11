---
name: cubrid-shell-run
description: "Run one CUBRID CTP shell test (or a narrow subtree) against the local build. Use when the user wants to debug, reproduce, or iterate on a specific shell test (or a small bucket of them) without running the full CI suite. For CTP shell tests only — not for unit tests (cubrid-build), SQL regression tests, or .ctl isolation tests (cubrid-isolation-test). Triggers on phrases like 'run one shell test', 'debug a shell test', 'just shell-debug', 'ctp shell single test', 'run shell test for bug_XXXX', 'run itrack_XXXXX', 'reproduce CBRD-XXXXX shell test', 'rerun a CI shell failure locally', 'reproduce a CTP shell failure locally', 'rerun shell_ci.conf for one test', or 'ctp.sh shell'."
argument-hint: "<test-dir-or-subtree>"
---

# Run a Single CUBRID CTP Shell Test

CUBRID's CTP (`~/cubrid-testtools/CTP/bin/ctp.sh shell`) is the standard runner for `cubrid-testcases-private-ex/shell/**/cases/*.sh`. The stock conf runs *everything* under `scenario=`. For debugging a single test (or a narrow subtree) the playbook is: copy the conf to `/tmp`, rewrite `scenario=` to point at one directory, disable the git auto-update, then invoke `ctp.sh shell -c <temp.conf>`. The `just shell-debug` recipe in the project justfile bakes this in.

## When to Use

- User asks to run one shell test (e.g. `bug_1638`, `cbrd_26517`, `itrack_10005`)
- User asks to reproduce a single CTP shell failure from CI
- User asks to run all shell tests under one bucket (e.g. `_06_issues/_10_1h`)
- User mentions `ctp.sh shell`, `shell_ci.conf`, or `--interactive`
- User says "just shell-debug" or otherwise references the justfile recipe

## Prerequisites

- `~/cubrid-testtools/CTP/bin/ctp.sh` exists (CUBRID Test Platform installed)
- `~/cubrid-testtools/CTP/conf/shell_ci.conf` exists (default conf, used as template)
- `~/cubrid-testcases-private-ex/shell/` cloned (the testcase repo)
- `cubrid` on PATH — verify with `which cubrid && cubrid_rel | head -1`. CTP uses the **currently-active install**; a stale build will give misleading results.
- Project justfile contains `shell-debug`, `shell-debug-many`, `shell-debug-interactive`. If it does not, see Step 5 (add recipes).

## Step 1: Confirm CUBRID Install Is Current

```bash
which cubrid
cubrid_rel | head -1
```

If the user just rebuilt, make sure the install they want to test is the one on PATH. CTP does not rebuild — it only runs.

## Step 2: Locate the Test Directory

The `TEST_DIR` argument (passed as `$ARGUMENTS` to this skill) must be the directory that **contains `cases/<name>.sh`**, not the `.sh` file itself, and not the `cases/` directory.

```bash
# Yes
~/cubrid-testcases-private-ex/shell/_06_issues/_10_1h/bug_1638/

# No
~/cubrid-testcases-private-ex/shell/_06_issues/_10_1h/bug_1638/cases/bug_1638.sh
~/cubrid-testcases-private-ex/shell/_06_issues/_10_1h/bug_1638/cases/
```

If `$ARGUMENTS` is empty, ask the user which test to run, or fall back to `just shell-debug-interactive` for an stdin-driven picker.

Discovery commands:

```bash
ls ~/cubrid-testcases-private-ex/shell                          # top-level buckets
# depth 4 lands on the <test_name> dir, which is the TEST_DIR shape we want:
#   shell/<bucket>/<sub-bucket>/<test_name>/cases/<name>.sh
fd -t d -d 4 . ~/cubrid-testcases-private-ex/shell | fzf        # pick interactively
rg -l "CBRD-26517" ~/cubrid-testcases-private-ex/shell           # find tests by ticket
fd -t d "bug_1638|cbrd_26517|itrack_10005" ~/cubrid-testcases-private-ex/shell  # by id
```

(`fd` takes the search pattern as a positional argument — there is no `-name` flag.)

To run a wider subtree (e.g. one entire bucket), pass that ancestor directory — CTP recurses, so the same recipe runs 1 test or 1000.

## Step 3: Run via `just shell-debug`

Preferred path — the justfile recipe handles the conf munging:

```bash
just shell-debug ~/cubrid-testcases-private-ex/shell/_06_issues/_10_1h/bug_1638
```

For a subtree:

```bash
just shell-debug-many ~/cubrid-testcases-private-ex/shell/_06_issues/_10_1h
```

`shell-debug-many` is a pure semantic alias for `shell-debug` — same recipe body, same behavior. The separate name just signals intent at the call site (a single test vs. a bucket); there is no parallelism or per-test isolation difference.

For one-off poking when you don't want to commit to a directory:

```bash
just shell-debug-interactive
```

This launches CTP's `--interactive` mode against the **unmodified** `~/cubrid-testtools/CTP/conf/shell_ci.conf`. That means:

- `testcase_update_yn=true` is still active — CTP will `git pull` `~/cubrid-testcases-private-ex` before running.
- `testcase_exclude_from_file` is still applied — if the test you want is on the exclude list, it will be silently skipped.
- Reads from stdin — avoid in automated / non-tty contexts (it blocks waiting for input).

If either of the above is a problem, use `just shell-debug <TEST_DIR>` instead, which copies the conf and overrides both.

## Step 4: Run Manually (if justfile recipe is missing)

```bash
SRC=~/cubrid-testtools/CTP/conf/shell_ci.conf
CONF=$(mktemp /home/cubrid/dev/cubrid/.claude/scratch/shell_single.XXXXXX.conf)
cp "$SRC" "$CONF"
sed -i "s|^scenario=.*|scenario=<TEST_DIR>|"                "$CONF"
sed -i "s|^testcase_update_yn=.*|testcase_update_yn=false|" "$CONF"
sed -i "s|^testcase_exclude_from_file=.*|#&|"               "$CONF"
~/cubrid-testtools/CTP/bin/ctp.sh shell -c "$CONF"
```

Why each line:

| Override | Why |
|----------|-----|
| `scenario=<TEST_DIR>` | Narrows discovery to just this directory tree. |
| `testcase_update_yn=false` | Skips `git pull` on `~/cubrid-testcases-private-ex` — debug runs should not change the testcase repo under you. |
| `testcase_exclude_from_file` commented | The default excludes list (`config/daily_regression_test_excluded_list_linux.conf`) lives in the upstream tree and may skip the very test you are trying to debug. |

## Step 5: Add the Recipes to a New Project's justfile

If the project does not yet have `shell-debug`, drop this block into its justfile. The header comment is intentionally long so the recipe is self-documenting:

```just
# Run one or a limited range of CTP shell tests against the local build.
#
# ARG SHAPE
#   TEST_DIR must be the directory that *contains* `cases/<name>.sh`, NOT the .sh
#   itself. Pass any ancestor directory to run a wider subtree.
#
# Usage:
#   just shell-debug ~/cubrid-testcases-private-ex/shell/_06_issues/_10_1h/bug_1638
shell-debug TEST_DIR:
    #!/usr/bin/env bash
    set -euo pipefail
    SRC=~/cubrid-testtools/CTP/conf/shell_ci.conf
    CONF=$(mktemp /home/cubrid/dev/cubrid/.claude/scratch/shell_single.XXXXXX.conf)
    cp "$SRC" "$CONF"
    sed -i "s|^scenario=.*|scenario={{TEST_DIR}}|"              "$CONF"
    sed -i "s|^testcase_update_yn=.*|testcase_update_yn=false|" "$CONF"
    sed -i "s|^testcase_exclude_from_file=.*|#&|"               "$CONF"
    echo "[shell-debug] scenario={{TEST_DIR}}"
    echo "[shell-debug] conf=$CONF"
    ~/cubrid-testtools/CTP/bin/ctp.sh shell -c "$CONF"

shell-debug-many SUBTREE: (shell-debug SUBTREE)

shell-debug-interactive:
    ~/cubrid-testtools/CTP/bin/ctp.sh shell --interactive -c ~/cubrid-testtools/CTP/conf/shell_ci.conf
```

## Step 6: Read the Results

CTP writes to stdout and to `~/cubrid-testtools/CTP/result/shell/<timestamp>/`. On stdout, look for:

```
[TESTCASE] cubrid-testcases-private-ex/shell/.../bug_1638.sh EnvId=local [OK]
============= PRINT SUMMARY ==================
Test Category:shell
Total Case:1
Total Execution Case:1
Total Success Case:1
Total Fail Case:0
```

For a failing test, drill into the per-case log directory:

```bash
ls -lt ~/cubrid-testtools/CTP/result/shell/ | head -3                    # most recent run
ls ~/cubrid-testtools/CTP/result/shell/<timestamp>/                       # all artifacts
cat ~/cubrid-testtools/CTP/result/shell/<timestamp>/<test_name>.log       # stdout/stderr of the .sh
```

The `.sh` itself uses `write_ok` / `write_nok` from `$init_path/init.sh` to signal pass/fail. Search the log for `write_nok` messages to see the assertion that failed.

## Step 7: Iterate

Typical debug loop:

1. `just shell-debug <test-dir>` — run
2. Read result; if NOK, open `~/cubrid-testtools/CTP/result/shell/<latest>/<test>.log`
3. Edit source under `src/` (and rebuild — CTP does **not** rebuild)
4. Repeat

For a build-then-test one-liner: `just build && just shell-debug <test-dir>`.

## Troubleshooting

### Test runs but always passes — even when source is broken
CTP runs against the install on PATH, not the build tree. Rebuild and reinstall: `just build` (or whatever the project uses), then check `cubrid_rel | head -1` matches the commit you expect.

### `[NOK]` with no obvious assertion in stdout
Look at `~/cubrid-testtools/CTP/result/shell/<timestamp>/<test>.log` — the on-stdout summary truncates per-case detail.

### `--interactive` mode hangs
It is reading from stdin. Do not background it or pipe it. Run in a real terminal, or use `shell-debug` with an explicit `TEST_DIR` instead.

### Test wants a testcase that was excluded by the default exclude list
This is exactly what `sed -i "s|^testcase_exclude_from_file=.*|#&|" "$CONF"` fixes. If you copied the manual command, make sure that line ran.

### `git pull` happens despite `testcase_update_yn=false`
Most likely cause: you ran `shell-debug-interactive`, which uses the unmodified conf (see Step 3). Second cause: the sed pattern only matches `^testcase_update_yn=` (anchored) — if the line is commented in your local conf, uncomment it first or rewrite with a more lenient pattern.

### CTP reports `Total Case:0` or "no testcase found"
`TEST_DIR` is wrong. Pass the directory that **contains** `cases/<name>.sh`, not the `.sh` file itself and not the `cases/` directory. See Step 2 for the exact shape.

### Test fails with "address already in use" / "broker port in use"
A previous `cub_server` or broker is still holding the port (often left over from a prior aborted run). Clean up before retrying:

```bash
cubrid service stop || true
cubrid broker stop || true
# fallback if those hang:
pkill -9 cub_server; pkill -9 cub_broker
```

### `cubrid: command not found` inside the test
CTP inherits the parent shell's PATH. Source the CUBRID env (`. ~/.cubrid.sh` or your equivalent) before running `just shell-debug`.

### Need to run multiple buckets but not the whole suite
Run `shell-debug` multiple times with different `TEST_DIR` values, or temporarily make `scenario=` point at a common parent.
