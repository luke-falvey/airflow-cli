---
name: airflow-debug-failed-run
description: Diagnose and repair a failed Apache Airflow 2.x DAG run with airflow-cli. Find the failed run and task, read the exception from the task log, fix the cause, then re-run only the failed tasks and what's downstream, and confirm. Use when a DAG run failed, a task is red, or someone asks "why did X fail" or "re-run the failed tasks".
---

# Debug and repair a failed run

Needs `airflow-cli` and `jq`, and a profile in `~/.airflow.toml`. Steps 1–3 only read, so on a
production profile you can run them with `--read-only` for safety.

## Steps

1. **Find the run.** Use the latest failed run, or the run id you were given.
2. **Find the failed task.** `failed` tasks have the error. `upstream_failed` tasks never ran;
   they're a consequence, not a cause.
3. **Read the exception** at the end of the failed task's log. Check `try_number`: a task that has
   already retried and failed the same way is unlikely to be transient.
4. **Fix the cause.** Depending on the error, that means a variable or connection, an upstream
   system, the DAG code, or the input conf. Some errors can't be fixed by re-running, such as bad
   conf or a code bug that hasn't been deployed. Say so rather than clearing.
5. **Preview, then clear** the failed task and everything downstream. Clearing also sets the run back
   to `queued`, so the scheduler picks the tasks up again.
6. **Wait and confirm**, then leave a note on the run explaining what happened.

## Worked example

A run of `cli_demo` failed overnight.

```bash
DAG=cli_demo

# 1. The most recent failed run
RUN=$(airflow-cli runs list "$DAG" --state failed --order-by -start_date --limit 1 --compact \
      | jq -r '.dag_runs[0].dag_run_id')
echo "RUN=$RUN"

# 2. Task states and tries
airflow-cli tasks list "$DAG" "$RUN" --compact \
  | jq -r '.task_instances[] | "\(.task_id)\t\(.state)\ttry=\(.try_number)"'
```

```
RUN=nightly-2026-11-06
extract	success	try=1
transform	failed	try=1
load	upstream_failed	try=0
```

```bash
# 3. The exception
airflow-cli tasks logs "$DAG" "$RUN" transform | grep -E 'Error|Exception' | tail -3
```

```
RuntimeError: upstream service unavailable (variable cli_demo_break is 'true')
    raise RuntimeError("upstream service unavailable (variable cli_demo_break is 'true')")
RuntimeError: upstream service unavailable (variable cli_demo_break is 'true')
```

The cause is outside the DAG: the variable that simulates the outage is set. Fix it, then re-run.

```bash
# 4. Fix the cause
airflow-cli variables delete cli_demo_break

# 5. Preview, then clear failed tasks + downstream
airflow-cli tasks clear "$DAG" "$RUN" --only-failed --downstream --dry-run --compact | jq -c '[.task_instances[].task_id]'
airflow-cli tasks clear "$DAG" "$RUN" --only-failed --downstream > /dev/null

# 6. Wait, confirm, and leave a note
airflow-cli runs wait "$DAG" "$RUN" --interval 3 --compact | jq -c '{dag_run_id, state}'
airflow-cli tasks list "$DAG" "$RUN" --compact | jq -r '.task_instances[] | "\(.task_id)\t\(.state)\ttry=\(.try_number)"'
airflow-cli runs note "$DAG" "$RUN" "transform failed: upstream outage; cleared after fix" --compact | jq -c '{state, note}'
```

```
["transform","load"]
[    0s] cli_demo nightly-2026-11-06: queued — 1 success, 2 none
[    3s] cli_demo nightly-2026-11-06: running — 2 success, 1 queued
[    7s] cli_demo nightly-2026-11-06: success — 3 success
{"dag_run_id":"nightly-2026-11-06","state":"success"}
extract	success	try=1
transform	success	try=2
load	success	try=1
{"state":"success","note":"transform failed: upstream outage; cleared after fix"}
```

`transform` is on try 2 and succeeded. `extract` wasn't re-run.

## Other repairs

| Situation | Command |
|---|---|
| Re-run the whole run | `airflow-cli runs clear "$DAG" "$RUN"` (preview with `--dry-run`) |
| Re-run specific tasks, whatever their state | `airflow-cli tasks clear "$DAG" "$RUN" --task a,b --downstream` |
| A task did its job but was marked failed | `airflow-cli tasks set-state "$DAG" "$RUN" task success` |
| Give up on a stuck run | `airflow-cli runs set-state "$DAG" "$RUN" failed` |
| An earlier try's log | `airflow-cli tasks tries ...`, then `tasks logs ... --try 1` |
| A mapped task | add `--map-index N` to `tasks get` / `tasks logs` / `tasks set-state` |
| The failure was bad conf | trigger a new run with corrected conf (see `airflow-run-dag`); clearing re-uses the old conf |

## Pitfalls

- Don't clear before reading the log. Clearing starts a new try, and you'll have to pass `--try` to find the original error.
- `tasks clear` without `--only-failed` re-runs every task in the run, including the successful ones.
- `--no-reset-dag-run` keeps a failed run `failed`. The scheduler then never runs the cleared tasks,
  so only use it if you'll reset the run yourself.
- A task log that says `ModuleNotFoundError` or similar at import time usually means a broken
  deployment. Check `airflow-cli import-errors list` (see `airflow-triage`).
