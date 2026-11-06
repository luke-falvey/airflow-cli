#!/usr/bin/env bash
# Trigger one run per date, wait for all, retry failures once, print a summary.
#
#   backfill.sh DAG START END [EXTRA_CONF_JSON] [airflow-cli options...]
#   backfill.sh etl 2026-10-01 2026-10-31 '{"full_refresh": true}' -p staging
#
# Each run gets run id bf-<date> and conf {"date": "<date>", ...EXTRA_CONF_JSON}.
# Dates that already have a bf-<date> run are skipped, so it is safe to re-run.
# Exit code: 0 if every date succeeded, 3 if any date is still failing.
set -uo pipefail

if [[ $# -lt 3 ]]; then
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
DAG=$1 START=$2 END=$3
shift 3
EXTRA='{}'
if [[ $# -gt 0 && $1 == \{* ]]; then EXTRA=$1; shift; fi
A=(airflow-cli "$@")

# Dates from START to END inclusive (jq does the date maths portably).
mapfile -t DATES < <(jq -nr --arg s "$START" --arg e "$END" \
  '[range(($s + "T00:00:00Z" | fromdate); ($e + "T00:00:00Z" | fromdate) + 1; 86400) | strftime("%Y-%m-%d")] | .[]')
[[ ${#DATES[@]} -gt 0 ]] || { echo "no dates between $START and $END" >&2; exit 2; }

was_paused=$("${A[@]}" dags get "$DAG" --compact | jq -r '.is_paused') || exit 1
if [[ $was_paused == true ]]; then "${A[@]}" dags unpause "$DAG" > /dev/null || exit 1; fi

for d in "${DATES[@]}"; do
  run="bf-$d"
  if "${A[@]}" runs get "$DAG" "$run" > /dev/null 2>&1; then echo "skip $run (exists)" >&2; continue; fi
  conf=$(jq -nc --arg d "$d" --argjson extra "$EXTRA" '$extra + {date: $d}')
  "${A[@]}" runs trigger "$DAG" --run-id "$run" --conf "$conf" --id-only > /dev/null || exit 1
  echo "triggered $run" >&2
done

failed=()
for d in "${DATES[@]}"; do
  "${A[@]}" runs wait "$DAG" "bf-$d" -q --timeout 3600 > /dev/null 2>&1
  case $? in
    0) ;;
    3) failed+=("$d") ;;
    4) echo "bf-$d timed out" >&2; failed+=("$d") ;;
    *) echo "bf-$d: error while waiting" >&2; failed+=("$d") ;;
  esac
done

still=()
for d in "${failed[@]}"; do
  echo "retrying bf-$d" >&2
  "${A[@]}" tasks clear "$DAG" "bf-$d" --only-failed --downstream > /dev/null
  "${A[@]}" runs wait "$DAG" "bf-$d" -q --timeout 3600 > /dev/null 2>&1 || still+=("$d")
done

[[ $was_paused == true ]] && "${A[@]}" dags pause "$DAG" > /dev/null

"${A[@]}" runs list "$DAG" --order-by dag_run_id --all --compact \
  | jq -r --arg s "bf-$START" --arg e "bf-$END" \
      '.dag_runs[] | select(.dag_run_id >= $s and .dag_run_id <= $e) | "\(.dag_run_id)\t\(.state)"'

if [[ ${#still[@]} -gt 0 ]]; then
  echo "still failing after one retry: ${still[*]}" >&2
  exit 3
fi
