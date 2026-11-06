#!/usr/bin/env bash
# Worked example: trigger a DAG run, poll until it finishes, report, clean up.
#
#   ./examples/run-dag.sh [dag_id] [conf.json] [--profile NAME]
#
# Needs: airflow-cli and jq on PATH, plus a profile in ~/.airflow.toml.
# The default DAG is examples/dags/cli_demo.py (copy it into your dags folder first).
set -euo pipefail

DAG=${1:-cli_demo}
CONF=${2:-"$(dirname "$0")/conf/cli_demo.json"}
shift 2 || shift $# || true
A=(airflow-cli "$@")                       # e.g. pass --profile staging
RUN_ID="cli-$(date +%s)-$$"                # our own id, so we know exactly what to clean up

# Remember whether the DAG was paused, so cleanup can put it back.
was_paused=$("${A[@]}" dags get "$DAG" --compact | jq -r '.is_paused')

cleanup() {
  "${A[@]}" runs delete "$DAG" "$RUN_ID" >/dev/null 2>&1 && echo "deleted run $RUN_ID" >&2
  if [[ $was_paused == true ]]; then "${A[@]}" dags pause "$DAG" >/dev/null && echo "re-paused $DAG" >&2; fi
}
trap cleanup EXIT

# 1. Trigger. Runs of a paused DAG stay queued, so unpause it for the duration.
[[ $was_paused == true ]] && "${A[@]}" dags unpause "$DAG" >/dev/null
"${A[@]}" runs trigger "$DAG" --run-id "$RUN_ID" --conf "@$CONF" --note "triggered by run-dag.sh" >/dev/null
echo "triggered $DAG $RUN_ID" >&2

# 2. Poll until the run leaves queued/running.
deadline=$((SECONDS + 900))
while :; do
  state=$("${A[@]}" runs get "$DAG" "$RUN_ID" --compact | jq -r '.state')
  tasks=$("${A[@]}" tasks list "$DAG" "$RUN_ID" --compact | jq -r '[.task_instances[] | "\(.task_id)=\(.state // "none")"] | join(" ")')
  echo "state=$state  $tasks" >&2
  [[ $state != queued && $state != running ]] && break
  (( SECONDS < deadline )) || { echo "timed out" >&2; "${A[@]}" runs set-state "$DAG" "$RUN_ID" failed >/dev/null; exit 4; }
  sleep 5
done

# 3. Report.
if [[ $state == success ]]; then
  echo "result of transform:" >&2
  "${A[@]}" xcom get "$DAG" "$RUN_ID" transform --value || true
  exit 0
fi

echo "run $state; logs of failed tasks:" >&2
"${A[@]}" tasks list "$DAG" "$RUN_ID" --state failed --compact | jq -r '.task_instances[].task_id' |
  while read -r task; do
    echo "----- $task -----" >&2
    "${A[@]}" tasks logs "$DAG" "$RUN_ID" "$task" | tail -20 >&2
  done
exit 3
