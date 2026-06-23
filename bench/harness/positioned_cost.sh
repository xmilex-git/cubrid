#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

declare -A queries=(
  [hj_probe_heavy]="${HARNESS_DIR}/queries/pos_hj_probe.sql"
  [connect_by]="${HARNESS_DIR}/queries/pos_connect_by.sql"
  [positional_mutation]="${HARNESS_DIR}/queries/pos_mutation.sql"
)

echo -e "workload\twall_ms\trc\tout_file"
for name in hj_probe_heavy connect_by positional_mutation; do
  out="${RESULTS_DIR}/positioned.${name}.$(date +%s).out"
  timing=$(run_timed_sql "${queries[$name]}" "${out}" || true)
  echo -e "${name}\t${timing}"
done
