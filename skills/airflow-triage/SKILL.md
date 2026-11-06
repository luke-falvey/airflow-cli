---
name: airflow-triage
description: Produce a read-only health report for an Apache Airflow 2.x environment with airflow-cli. Covers scheduler/triggerer health, DAG files that fail to parse, DAG warnings, recent failed runs, runs stuck in queued (e.g. on paused DAGs) and pool usage. Use when asked "is Airflow OK?", "what's broken?", "why isn't my DAG running?", for an on-call check, or before and after a deployment.
---

# Triage an Airflow environment

The whole report only reads, and runs with `--read-only`, so it's safe against production. Needs
`airflow-cli` and `jq`. The report is packaged as [`triage.sh`](triage.sh) next to this file:

```bash
skills/airflow-triage/triage.sh              # default profile
skills/airflow-triage/triage.sh -p prod      # any airflow-cli options are passed through
```

The script exits 1 if a component is unhealthy, a DAG file fails to parse, or a run is stuck on a
paused DAG, so it can gate a deploy or alert from cron. Failed runs are listed, but don't change the
exit code.

## Steps

1. **Health.** The `scheduler`, `triggerer` and `metadatabase` statuses should all be `healthy`.
   `dag_processor` shows `n/a` unless a standalone DAG processor runs.
2. **Parse errors.** Files that fail to import don't show up as DAGs at all. This is the usual answer
   to "my new DAG doesn't appear".
3. **DAG warnings**, such as non-existent pools.
4. **Recent failed runs** across all DAGs (`~` means all).
5. **Queued and running runs**, flagging any that sit on a paused DAG: those never start.
6. **Pools.** A full pool, with queued slots, explains tasks that wait without running.

## Worked example

Against an environment with a broken DAG file, a failed run, and a run triggered on a paused DAG:

```bash
A=(airflow-cli --read-only)

echo "== health"
"${A[@]}" health --compact | jq -r 'to_entries[] | "\(.key)\t\(.value.status // "n/a")"'

echo "== DAG files that fail to parse"
"${A[@]}" import-errors list --all --compact \
  | jq -r '.import_errors[] | "\(.filename)\n    \(.stack_trace | split("\n") | map(select(length > 0)) | last)"'

echo "== DAG warnings"
"${A[@]}" dags warnings --all --compact | jq -r '.dag_warnings[] | "\(.dag_id): \(.message)"'

echo "== failed runs (latest 10)"
"${A[@]}" runs list '~' --state failed --order-by -start_date --limit 10 --compact \
  | jq -r '.dag_runs[] | "\(.dag_id)\t\(.dag_run_id)\t\(.end_date)"'

echo "== queued or running runs"
paused=$("${A[@]}" dags list --paused --all --compact | jq -c '[.dags[].dag_id]')
"${A[@]}" runs list '~' --state queued --state running --all --compact \
  | jq -r --argjson paused "$paused" '.dag_runs[] |
      "\(.dag_id)\t\(.dag_run_id)\t\(.state)\(if (.dag_id | IN($paused[])) then "\t<- DAG is paused, will not run" else "" end)"'

echo "== pools"
"${A[@]}" pools list --all --compact | jq -r '.pools[] | "\(.name)\t\(.occupied_slots)/\(.slots) used, \(.queued_slots) queued"'
```

```
== health
dag_processor	n/a
metadatabase	healthy
scheduler	healthy
triggerer	healthy
== DAG files that fail to parse
/opt/airflow/dags/broken_demo.py
    ModuleNotFoundError: No module named 'not_a_real_module'
== DAG warnings
== failed runs (latest 10)
cli_demo	triage-failed	2026-11-06T23:02:53.791922+00:00
== queued or running runs
cli_demo	triage-stuck	queued	<- DAG is paused, will not run
== pools
default_pool	0/128 used, 0 queued
warehouse	0/8 used, 0 queued
```

Reading it: the scheduler is fine. `broken_demo.py` needs its dependency installed (or the import
fixed). `triage-failed` should be looked at with the `airflow-debug-failed-run` skill. `triage-stuck`
will start as soon as `cli_demo` is unpaused, if that was intended.

## Going further

| Question | Command |
|---|---|
| Is the scheduler heartbeat recent? | `airflow-cli health --compact \| jq .scheduler.latest_scheduler_heartbeat` |
| Run counts per state for a DAG | `airflow-cli dags stats my_dag` |
| Has this DAG run recently? | `airflow-cli runs list my_dag --order-by -start_date --limit 5` |
| What's a task waiting for? | `airflow-cli api GET /dags/my_dag/dagRuns/RUN/taskInstances/TASK/dependencies` |
| Recent user actions (who paused it?) | `airflow-cli api GET /eventLogs -q dag_id=my_dag -q order_by=-when -q limit=20` |
| Same report on every environment | `for p in staging prod; do echo "### $p"; skills/airflow-triage/triage.sh -p "$p"; done` |

## Pitfalls

- A new DAG file that doesn't appear **and** has no parse error is usually skipped by Airflow's
  "safe mode". Airflow only parses `.py` files that contain both the words `airflow` and `dag`
  (`[core] dag_discovery_safe_mode`). It can also be listed in `.airflowignore`.
- `version` and `health` don't need a login, so a clean health check doesn't prove your credentials work. `airflow-cli ping` does.
- `runs list '~'` covers only DAGs the user can read. A `Viewer` may see fewer than an admin.
- A healthy scheduler with nothing running can still mean every DAG is paused. Check with `dags list --unpaused`.
