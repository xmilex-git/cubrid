#!/usr/bin/env bash
#
# run_tests.sh — static + logic self-tests for ctp-parallel. Runs WITHOUT podman.
#
# Drives the orchestrator's pure-logic paths (--dry-run / --validate-only) against
# the REAL testtools + testcases trees on this machine and asserts every property
# from SPEC.md "Verification". Real container launch is out of scope here (no
# podman); see README.md "Manual e2e QA".
#
# Usage:
#   bash run_tests.sh [--testcases <root>] [--ctp <CTP_HOME>]
# Defaults: --testcases $HOME/cubrid-testcases  --ctp $HOME/cubrid-testtools/CTP

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SKILL="$(cd "$HERE/.." && pwd)"
ORCH="$SKILL/scripts/ctp_parallel.sh"
ENTRY="$SKILL/scripts/entrypoint.sh"

TC="${HOME}/cubrid-testcases"
CTP="${HOME}/cubrid-testtools/CTP"
while [ $# -gt 0 ]; do
  case "$1" in
    --testcases) TC="$2"; shift 2 ;;
    --ctp)       CTP="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
SCN="$TC/sql"

# Keep all scratch on disk-backed storage (NOT /tmp).
SCRATCH="$SKILL/test/.scratch"
rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
export TMPDIR="$SCRATCH"
trap 'rm -rf "$SCRATCH"' EXIT

PASS=0
FAILED=0
note() { printf '  - %s\n' "$*"; }
ok()   { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad()  { FAILED=$((FAILED+1)); printf '[FAIL] %s\n' "$*" >&2; }
require_files() {
  [ -r "$ORCH" ]  || { echo "missing orchestrator: $ORCH" >&2; exit 2; }
  [ -d "$SCN" ]   || { echo "missing scenario tree: $SCN" >&2; exit 2; }
  [ -r "$CTP/conf/sql.conf" ] || { echo "missing CTP template: $CTP/conf/sql.conf" >&2; exit 2; }
}
require_files

echo "=================================================================="
echo " ctp-parallel self-tests"
echo "   orchestrator : $ORCH"
echo "   testcases    : $SCN"
echo "   CTP_HOME     : $CTP"
echo "=================================================================="

#-------------------------------------------------------------------
# (a) Static lint — bash -n on every script (+ shellcheck if available)
#-------------------------------------------------------------------
echo; echo "## (a) static lint"
if bash -n "$ORCH"; then ok "bash -n clean: ctp_parallel.sh"; else bad "bash -n FAILED: ctp_parallel.sh"; fi
if bash -n "$ENTRY"; then ok "bash -n clean: entrypoint.sh"; else bad "bash -n FAILED: entrypoint.sh"; fi
if command -v shellcheck >/dev/null 2>&1; then
  if shellcheck -S warning "$ORCH" "$ENTRY"; then ok "shellcheck clean"; else bad "shellcheck reported issues"; fi
else
  note "shellcheck absent — bash -n only"
fi

#-------------------------------------------------------------------
# Helper: run a dry-run for a given N, capture stdout + artifacts.
#-------------------------------------------------------------------
dryrun() { # <N> <outdir>  -> writes log to <outdir>.log
  # --no-weights so the structural / invariant / count-balance checks are stable and
  # independent of the bundled time table (auto-weights is covered separately in (j)).
  local n="$1" out="$2"
  rm -rf "$out"
  bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --shards "$n" --no-weights --out "$out" >"$out.log" 2>&1
  return $?
}

#-------------------------------------------------------------------
# (b) Leaf/unit discovery == independent direct find
#-------------------------------------------------------------------
echo; echo "## (b) unit discovery"
OUT10="$SCRATCH/out10"
if dryrun 10 "$OUT10"; then
  units_plan="$(wc -l < "$OUT10/units.tsv")"
  # DEFAULT unit = top-level _* "bulk" (CI's sql unit). Recompute independently:
  # the first path component of every */cases/*.sql.
  units_find="$(find "$SCN" -type f -name '*.sql' -path '*/cases/*' \
                  | sed "s#^${SCN}/##" | sed -E 's#/.*##' | sort -u | wc -l)"
  glob_bulks="$(ls -d "$SCN"/_*/ 2>/dev/null | wc -l)"
  note "bulk(_*) units (plan)=$units_plan  (direct find)=$units_find  ; sql/_* dirs=$glob_bulks"
  if [ "$units_plan" -eq "$units_find" ]; then ok "unit discovery matches direct find ($units_plan bulks)"; else bad "unit discovery $units_plan != find $units_find"; fi
  if [ "$units_plan" -ge 20 ] && [ "$units_plan" -le 60 ]; then ok "bulk count in expected ~35 range ($units_plan)"; else bad "bulk count $units_plan out of range"; fi
else
  bad "dry-run N=10 failed"; cat "$OUT10.log" >&2
fi

#-------------------------------------------------------------------
# (c) Partition correctness for N in {1,4,10}: disjoint + union == all leaves
#-------------------------------------------------------------------
echo; echo "## (c) partition correctness N in {1,4,10}"
ALL_UNITS="$SCRATCH/all_units.txt"
awk -F'\t' '{print $1}' "$OUT10/units.tsv" | sort > "$ALL_UNITS"
total_units="$(wc -l < "$ALL_UNITS")"
for N in 1 4 10; do
  OUT="$SCRATCH/out$N"
  if ! dryrun "$N" "$OUT"; then bad "dry-run N=$N failed"; cat "$OUT.log" >&2; continue; fi
  asg="$OUT/assignment.tsv"
  rows="$(wc -l < "$asg")"
  uniq_units="$(awk -F'\t' '{print $1}' "$asg" | sort -u | wc -l)"
  union_match="$(awk -F'\t' '{print $1}' "$asg" | sort | comm -3 - "$ALL_UNITS" | wc -l)"
  # disjoint: no unit assigned to >1 shard  => rows == uniq_units
  # union   : assigned set == all units      => union_match == 0 AND uniq_units == total_units
  # shard range: every shard idx in [0,N)
  badidx="$(awk -F'\t' -v n="$N" '$2<0 || $2>=n' "$asg" | wc -l)"
  if [ "$rows" -eq "$uniq_units" ] && [ "$uniq_units" -eq "$total_units" ] && [ "$union_match" -eq 0 ] && [ "$badidx" -eq 0 ]; then
    ok "N=$N: leaf-sets disjoint & union==all leaves ($uniq_units units, shards in range)"
  else
    bad "N=$N: rows=$rows uniq=$uniq_units total=$total_units union_diff=$union_match badidx=$badidx"
  fi
done

#-------------------------------------------------------------------
# (d) Offline split-validator passes on the real tree (already runs inside dry-run)
#     + synthetic ambiguous fixture must be FLAGGED (proves not a no-op)
#-------------------------------------------------------------------
echo; echo "## (d) split-validator"
if grep -q "split valid: all .* surviving .sql alive in exactly one shard" "$OUT10.log"; then
  ok "validator passes on real tree (0 duplicates, 0 orphans)"
else
  bad "validator did not confirm clean split on real tree"; grep -i 'validat\|orphan\|duplicate' "$OUT10.log" >&2
fi
# Synthetic fixture: an inner .sql under a NESTED cases dir, with the outer and
# inner units assigned to DIFFERENT shards -> ambiguous substring match -> orphan.
FX_ASG="$SCRATCH/fx_assign.tsv"; FX_SQL="$SCRATCH/fx_sql.txt"
printf '/a/cases\t0\n/a/cases/sub/cases\t1\n' > "$FX_ASG"
printf '/a/cases/x.sql\n/a/cases/sub/cases/y.sql\n' > "$FX_SQL"
if bash "$ORCH" --validate-only "$FX_ASG" "$FX_SQL" >"$SCRATCH/fx.log" 2>&1; then
  bad "validator did NOT flag the ambiguous fixture (false negative)"; cat "$SCRATCH/fx.log" >&2
else
  if grep -q 'ORPHAN' "$SCRATCH/fx.log"; then ok "validator FLAGS ambiguous substring fixture (orphan, non-zero exit)"; else bad "validator exited non-zero but without an ORPHAN diagnosis"; fi
fi
# Control: a clean disjoint fixture must PASS.
printf '/a/cases\t0\n/b/cases\t1\n' > "$SCRATCH/fxok_a.tsv"
printf '/a/cases/x.sql\n/b/cases/y.sql\n' > "$SCRATCH/fxok_s.txt"
if bash "$ORCH" --validate-only "$SCRATCH/fxok_a.tsv" "$SCRATCH/fxok_s.txt" >/dev/null 2>&1; then
  ok "validator passes a clean disjoint fixture (control)"
else
  bad "validator wrongly flagged a clean disjoint fixture"
fi

#-------------------------------------------------------------------
# (e) Invariant: sum surviving(shard) == global - base_excluded
#-------------------------------------------------------------------
echo; echo "## (e) split invariant"
g="$(awk '/global \.sql:/{print $NF}'        "$OUT10.log")"
b="$(awk '/base-excluded \.sql:/{print $NF}' "$OUT10.log")"
s="$(awk '/surviving \.sql:/{print $NF}'     "$OUT10.log")"
sum_shard="$(awk -F'\t' 'NR>1{s+=$2} END{print s+0}' "$OUT10/plan.tsv")"
sum_units="$(awk -F'\t' '{s+=$2} END{print s+0}' "$OUT10/units.tsv")"
note "global=$g base_excluded=$b surviving=$s ; sum(shard sql)=$sum_shard sum(unit sql)=$sum_units"
if [ "$s" -eq "$((g - b))" ] && [ "$sum_shard" -eq "$s" ] && [ "$sum_units" -eq "$s" ]; then
  ok "invariant holds: surviving==global-base==Σshard==Σunit ($s)"
else
  bad "invariant violated: g=$g b=$b s=$s Σshard=$sum_shard Σunit=$sum_units"
fi

#-------------------------------------------------------------------
# (f) Balance sanity (--by-dir: fine units balance well) + bulk atomicity (default)
#-------------------------------------------------------------------
echo; echo "## (f) balance (--by-dir, N=10) + bulk atomicity (default)"
# Balance is measured with --by-dir: the default bulk(_*) unit is intentionally coarse
# (a bulk is atomic, so the heaviest bulk bounds the slowest shard); fine units show the
# greedy-LPT quality. Real bulk runs balance by TIME via --weights.
OUTF="$SCRATCH/outf_bydir"
bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --shards 10 --by-dir --no-weights --out "$OUTF" >"$OUTF.log" 2>&1
read -r mx mean ratio_ok < <(awk -F'\t' '
  NR>1 { c[n++]=$2; sum+=$2; if($2>mx) mx=$2 }
  END { mean=sum/n; printf "%d %.1f %d\n", mx, mean, (mx <= 1.5*mean) }
' "$OUTF/plan.tsv")
note "by-dir max-shard=$mx mean=$mean  (max <= 1.5*mean ? $ratio_ok)"
if [ "$ratio_ok" -eq 1 ]; then ok "balanced (--by-dir): max-shard $mx <= 1.5 x mean $mean"; else bad "imbalanced (--by-dir): max-shard $mx > 1.5 x mean $mean"; fi
# Bulk atomicity: in the default plan, each top-level _* bulk lands on exactly ONE shard.
splitn="$(awk 'FNR==1{ match(FILENAME,/shard_([0-9]+)/,m); sid=m[1] }
               { b=$0; sub(/\/.*/,"",b); seen[b"\t"sid]=1; bulks[b]=1 }
               END{ for(k in seen){split(k,a,"\t"); c[a[1]]++} s=0; for(b in bulks) if(c[b]>1) s++; print s+0 }' \
             "$OUT10"/shard_*/assigned_sql.txt 2>/dev/null)"
if [ "${splitn:-1}" -eq 0 ]; then ok "bulk atomicity: no _* bulk split across shards"; else bad "bulk atomicity: $splitn bulk(s) split across shards"; fi

#-------------------------------------------------------------------
# (g) Config generation: scenario path, ports/SHM verbatim, no host paths
#-------------------------------------------------------------------
echo; echo "## (g) generated sql.conf"
CONF="$OUT10/shard_0/sql.conf"
cfg_ok=1
grep -qx 'scenario=/home/cubrid-testcases/sql' "$CONF" || { cfg_ok=0; note "scenario line wrong"; }
for kv in 'cubrid_port_id=1822' 'BROKER_PORT=33120' 'APPL_SERVER_SHM_ID=33120' 'MASTER_SHM_ID=33122' 'ha_port_id=59901' 'ha_mode=yes'; do
  grep -qx "$kv" "$CONF" || { cfg_ok=0; note "missing/changed F2 line: $kv"; }
done
grep -qx 'testcase_exclude_from_file=${CTP_HOME}/conf/exclusions.txt' "$CONF" || { cfg_ok=0; note "exclude-file line wrong"; }
# No HOST filesystem paths may leak. Host paths begin with "$HOME/" (e.g. the real
# testcases/ctp/out dirs). The container target /home/cubrid-testcases (no slash
# after 'cubrid') is NOT a host path and must NOT trip this.
leak="$(grep -nF "$HOME/" "$CONF" || true)"
leak2="$(grep -nF "$CTP" "$CONF" || true)"
leak3="$(grep -nF "$OUT10" "$CONF" || true)"
if [ "$cfg_ok" -eq 1 ] && [ -z "$leak$leak2$leak3" ]; then
  ok "sql.conf: scenario=container path, F2 ports/SHM verbatim, no host paths leak"
else
  bad "sql.conf check failed (cfg_ok=$cfg_ok leaks: $leak $leak2 $leak3)"
fi

#-------------------------------------------------------------------
# (h) podman-missing preflight: clear error, non-zero, no partial work dirs
#-------------------------------------------------------------------
echo; echo "## (h) podman-missing preflight"
if command -v podman >/dev/null 2>&1; then
  note "podman IS present on this host — skipping the absence assertion (environment-dependent)."
  ok "podman-missing path: not applicable here (podman present)"
else
  PM_OUT="$SCRATCH/pm_out_should_not_exist"
  rm -rf "$PM_OUT"
  # Use an existing dir as a stand-in build so arg-validation passes and we reach
  # the podman preflight (which must fail fast, before any work dirs are made).
  bash "$ORCH" --build "$CTP" --testcases "$TC" --ctp "$CTP" --out "$PM_OUT" >"$SCRATCH/pm.log" 2>&1
  rc=$?
  if [ "$rc" -ne 0 ] && grep -qi 'podman is not installed' "$SCRATCH/pm.log" && [ ! -e "$PM_OUT" ]; then
    ok "podman-missing: clear error, exit=$rc, no work dir left behind"
  else
    bad "podman-missing path wrong (rc=$rc, out-exists=$([ -e "$PM_OUT" ] && echo yes || echo no))"; cat "$SCRATCH/pm.log" >&2
  fi
fi

#-------------------------------------------------------------------
# (i) colocate registry: keep-whole (survives --by-case) + co-locate (one shard)
#-------------------------------------------------------------------
echo; echo "## (i) colocate registry"
COLO="$SKILL/colocate.tsv"
if [ -r "$COLO" ]; then
  # (i1) keep-whole: a registered dir stays ONE unit even under --by-case.
  reg_dir="$(awk '!/^[[:space:]]*#/ && NF {print $1; exit}' "$COLO")"
  OUTBC="$SCRATCH/outbc"; rm -rf "$OUTBC"
  bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --shards 10 --by-case --out "$OUTBC" >"$OUTBC.log" 2>&1
  asg="$OUTBC/assignment.tsv"
  asunit="$(awk -F'\t' -v d="$reg_dir" '$1==d' "$asg" | wc -l)"
  undersplit="$(awk -F'\t' -v d="$reg_dir/" 'index($1,d)==1' "$asg" | wc -l)"
  if [ "$asunit" -eq 1 ] && [ "$undersplit" -eq 0 ]; then
    ok "keep-whole: registered dir is one unit under --by-case ($reg_dir)"
  else
    bad "keep-whole failed: as-unit=$asunit per-case-under=$undersplit for $reg_dir"
  fi
  # (i2) co-locate: two dirs grouped on one registry line must share a shard.
  d1="$(awk -F'\t' 'NR==1{print $1}' "$OUT10/units.tsv")"
  d2="$(awk -F'\t' 'NR==2{print $1}' "$OUT10/units.tsv")"
  GRP="$SCRATCH/grp.tsv"; printf '%s %s\n' "$d1" "$d2" > "$GRP"
  OUTG="$SCRATCH/outg"; rm -rf "$OUTG"
  bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --shards 10 --colocate "$GRP" --out "$OUTG" >"$OUTG.log" 2>&1
  s1="$(awk -F'\t' -v d="$d1" '$1==d{print $2}' "$OUTG/assignment.tsv")"
  s2="$(awk -F'\t' -v d="$d2" '$1==d{print $2}' "$OUTG/assignment.tsv")"
  if [ -n "$s1" ] && [ "$s1" = "$s2" ]; then
    ok "co-locate: grouped dirs share shard $s1"
  else
    bad "co-locate failed: shards differ ($d1->$s1, $d2->$s2)"
  fi
  # (i3) --no-colocate ignores the registry: --by-case is pure per-.sql again.
  OUTN="$SCRATCH/outn"; rm -rf "$OUTN"
  bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --shards 10 --by-case --no-colocate --out "$OUTN" >"$OUTN.log" 2>&1; rcN=$?
  un="$(wc -l < "$OUTN/units.tsv" 2>/dev/null || echo -1)"
  if [ "$rcN" -eq 0 ] && [ "$un" -eq "$s" ]; then
    ok "--no-colocate: dry-run rc=0 and --by-case units == surviving .sql ($un, registry ignored)"
  else
    bad "--no-colocate failed: rc=$rcN by-case units=$un != surviving=$s"
  fi
else
  note "no bundled colocate.tsv — skipping registry checks"
fi

#-------------------------------------------------------------------
# (j) auto-weights: default run (no --weights) loads the bundled time table
#-------------------------------------------------------------------
echo; echo "## (j) auto-weights (bundled time table, default)"
BW="$SKILL/baseline_weights.tsv"
if [ -r "$BW" ]; then
  OUTW="$SCRATCH/outw"; rm -rf "$OUTW"
  bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --shards 10 --out "$OUTW" >"$OUTW.log" 2>&1; rcW=$?
  # default should report time-based from the bundled table, and unit weights should sum
  # to the measured seconds (~ table total), NOT the case count (17420).
  bw_total="$(awk -F'\t' '{s+=$2} END{print s+0}' "$BW")"
  uw_sum="$(awk -F'\t' '{s+=$2} END{print s+0}' "$OUTW/units.tsv" 2>/dev/null)"
  if [ "$rcW" -eq 0 ] && grep -q 'weights: time-based from bundled' "$OUTW.log" \
       && [ "$uw_sum" -gt 0 ] && [ "$uw_sum" -lt 5000 ] && [ "$uw_sum" -ne "$s" ]; then
    ok "auto-weights: default is time-based (Σunit-weight=${uw_sum}s ~ table ${bw_total}s, != count $s)"
  else
    bad "auto-weights failed: rc=$rcW Σunit-weight=$uw_sum table=$bw_total count=$s"; grep -i weights "$OUTW.log" >&2
  fi
else
  note "no bundled baseline_weights.tsv — skipping auto-weights check"
fi

#-------------------------------------------------------------------
# (k) default shard count = 7 (workload-optimal; no --shards given)
#-------------------------------------------------------------------
echo; echo "## (k) default shard count"
OUTK="$SCRATCH/outk"; rm -rf "$OUTK"
bash "$ORCH" --dry-run --testcases "$TC" --ctp "$CTP" --out "$OUTK" >"$OUTK.log" 2>&1
def_n="$(awk -F'\t' 'NR>1{n++} END{print n+0}' "$OUTK/plan.tsv" 2>/dev/null)"
if [ "$def_n" -eq 7 ]; then
  ok "default shard count = 7 (no --shards)"
else
  note "default shards=$def_n (7 unless RAM-capped on this host)"
  grep -q 'capping to' "$OUTK.log" && ok "default 7 capped by RAM guard (expected on low-memory host)" || bad "default shards=$def_n, expected 7"
fi

#-------------------------------------------------------------------
# Summary
#-------------------------------------------------------------------
echo
echo "=================================================================="
if [ "$FAILED" -eq 0 ]; then
  echo "ALL TESTS PASSED ($PASS checks)"
  echo "=================================================================="
  exit 0
else
  echo "TESTS FAILED: $FAILED failed, $PASS passed"
  echo "=================================================================="
  exit 1
fi
