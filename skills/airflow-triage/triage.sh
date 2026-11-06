#!/usr/bin/env bash
# Read-only health report for an Airflow environment.
#
#   triage.sh [airflow-cli options...]      e.g. triage.sh -p prod
#
# Runs every call with --read-only, so it never changes anything.
# Exit code: 1 if a component is unhealthy, a DAG file fails to parse, or a run
# is stuck on a paused DAG; otherwise 0. Failed runs are listed but don't change
# the exit code (old failures would otherwise fail every check).
set -uo pipefail

A=(airflow-cli --read-only "$@")
problems=0

section() { printf '\n== %s\n' "$1"; }

section "health"
health=$("${A[@]}" health --compact) || exit 1
jq -r 'to_entries[] | "\(.key)\t\(.value.status // "n/a")"' <<<"$health"
jq -e '[.metadatabase.status, .scheduler.status, .triggerer.status] | all(. == "healthy")' <<<"$health" > /dev/null || problems=1

section "DAG files that fail to parse"
errors=$("${A[@]}" import-errors list --all --compact) || exit 1
jq -r '.import_errors[] | "\(.filename)\n    \(.stack_trace | split("\n") | map(select(length > 0)) | last)"' <<<"$errors"
[[ $(jq '.total_entries' <<<"$errors") -eq 0 ]] || problems=1

section "DAG warnings"
"${A[@]}" dags warnings --all --compact | jq -r '.dag_warnings[] | "\(.dag_id): \(.message)"'

section "failed runs (latest 10)"
failed=$("${A[@]}" runs list '~' --state failed --order-by -start_date --limit 10 --compact) || exit 1
jq -r '.dag_runs[] | "\(.dag_id)\t\(.dag_run_id)\t\(.end_date)"' <<<"$failed"

section "queued or running runs"
paused=$("${A[@]}" dags list --paused --all --compact | jq -c '[.dags[].dag_id]')
active=$("${A[@]}" runs list '~' --state queued --state running --all --compact) || exit 1
jq -r --argjson paused "$paused" '.dag_runs[] |
  "\(.dag_id)\t\(.dag_run_id)\t\(.state)\(if (.dag_id | IN($paused[])) then "\t<- DAG is paused, will not run" else "" end)"' <<<"$active"
jq -e --argjson paused "$paused" '[.dag_runs[] | select(.dag_id | IN($paused[]))] | length == 0' <<<"$active" > /dev/null || problems=1

section "pools"
"${A[@]}" pools list --all --compact \
  | jq -r '.pools[] | "\(.name)\t\(.occupied_slots)/\(.slots) used, \(.queued_slots) queued\(if .open_slots == 0 and .queued_slots > 0 then "\t<- full" else "" end)"'

exit $problems
