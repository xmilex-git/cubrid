#!/usr/bin/env bash
set -euo pipefail

env_name=${1:?usage: run_3way.sh <a|b|c|positioned> <baseline|asbuilt|redesign>}
build=${2:?usage: run_3way.sh <a|b|c|positioned> <baseline|asbuilt|redesign>}
export BUILD_NAME="${build}"
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

n=${HARNESS_N:-10}
result="${RESULTS_DIR}/${build}.${env_name}.tsv"
tmp="${result}.tmp"
: >"${tmp}"

case "${env_name}" in
  a) runner="${HARNESS_DIR}/env_a_iobound.sh" ;;
  b) runner="${HARNESS_DIR}/env_b_concurrency.sh" ;;
  c) runner="${HARNESS_DIR}/env_c_ramrich_guard.sh" ;;
  positioned) runner="${HARNESS_DIR}/positioned_cost.sh" ;;
  *) echo "unknown env: ${env_name}" >&2; exit 2 ;;
esac

echo "# build=${build} env=${env_name} n=${n} worktree=${BUILD_WORKTREE} cubrid=${CUBRID}" >"${result}"
configure_env_server_conf "${env_name}"
restart_server
for i in $(seq 1 "${n}"); do
  echo "# repeat=${i}" | tee -a "${result}" >/dev/null
  if "${runner}" "${build}.${env_name}.${i}" >"${tmp}" 2>>"${RESULTS_DIR}/${build}.${env_name}.err"; then
    sed "s/^/${i}\t/" "${tmp}" >>"${result}"
  else
    rc=$?
    sed "s/^/${i}\t/" "${tmp}" >>"${result}" || true
    echo -e "${i}\tERROR\trc=${rc}" >>"${result}"
    exit "${rc}"
  fi
done

if [[ "${env_name}" != "b" ]] \
  && awk -F'\t' '$3 ~ /^[0-9.]+$/ { print $3 }' "${result}" >"${result}.latencies" \
  && [[ -s "${result}.latencies" ]]; then
  med=$(median <"${result}.latencies")
  band=$(mean_sigma2 <"${result}.latencies")
  echo -e "# median_ms\t${med}" >>"${result}"
  echo -e "# mean_ms\tsigma_ms\tlow_2sigma_ms\thigh_2sigma_ms\n# ${band}" >>"${result}"
fi
