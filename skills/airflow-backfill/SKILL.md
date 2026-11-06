---
name: airflow-backfill
description: Re-process a range of dates in Apache Airflow 2.x with airflow-cli by triggering one DAG run per date with conf, waiting for all of them, retrying failures once, and reporting which dates succeeded. Safe to re-run. Use when asked to backfill, re-run a date range, reprocess historical data, or fan out many runs of one DAG.
---

# Backfill a date range

The REST API has no "backfill" command. Instead, trigger one run per date, passing the date in conf,
which is what the DAG must read. Needs `airflow-cli` and `jq`.

The steps below are packaged as [`backfill.sh`](backfill.sh) next to this file:

```bash
skills/airflow-backfill/backfill.sh cli_demo 2026-11-01 2026-11-03          # DAG START END
skills/airflow-backfill/backfill.sh etl 2026-10-01 2026-10-31 '{"full_refresh": true}' -p staging
```

## Steps

1. **Pick deterministic run ids**, e.g. `bf-<date>`. Re-running the backfill then skips dates that
   already have a run, instead of duplicating work. Don't use Airflow's reserved prefixes:
   `scheduled__`, `manual__`, `backfill__` and `dataset_triggered__`.
2. **Trigger one run per date** with the date in conf. Airflow runs up to the DAG's
   `max_active_runs` at once and queues the rest.
3. **Wait for each run** and record which ones failed. Use the exit code to tell failed (3) from
   timed out (4) from errors (1).
4. **Retry failures once** by clearing only the failed tasks and what's downstream. Anything still
   failing needs a person (see `airflow-debug-failed-run`).
5. **Report** a per-date summary.

## Worked example

In this example, `2026-11-02` gets bad input (`"fail": true`), so the retry path really runs.

```bash
DAG=cli_demo
DATES=(2026-11-01 2026-11-02 2026-11-03)
airflow-cli dags unpause "$DAG" > /dev/null

# 1+2. One run per date; skip dates that already have a run
for d in "${DATES[@]}"; do
  run="bf-$d"
  if airflow-cli runs get "$DAG" "$run" > /dev/null 2>&1; then echo "skip $run (exists)"; continue; fi
  conf=$(jq -nc --arg d "$d" '{date: $d}')
  [[ $d == 2026-11-02 ]] && conf=$(jq -nc --arg d "$d" '{date: $d, fail: true}')   # simulate bad input
  airflow-cli runs trigger "$DAG" --run-id "$run" --conf "$conf" --id-only
done

# 3. Wait for all of them
failed=()
for d in "${DATES[@]}"; do
  airflow-cli runs wait "$DAG" "bf-$d" -q --timeout 900 > /dev/null 2>&1; rc=$?
  case $rc in
    0) echo "$d success" ;;
    3) echo "$d FAILED"; failed+=("$d") ;;
    4) echo "$d timed out" ;;
    *) echo "$d error (exit $rc)" ;;
  esac
done

# 4. Retry each failure once
for d in "${failed[@]}"; do
  airflow-cli tasks clear "$DAG" "bf-$d" --only-failed --downstream > /dev/null
  airflow-cli runs wait "$DAG" "bf-$d" -q > /dev/null 2>&1 && echo "$d success after retry" || echo "$d still failing"
done

# 5. Summary
airflow-cli runs list "$DAG" --order-by dag_run_id --all --compact \
  | jq -r '.dag_runs[] | select(.dag_run_id | startswith("bf-")) | "\(.dag_run_id)\t\(.state)\t\(.conf | tostring)"'
```

```
bf-2026-11-01
bf-2026-11-02
bf-2026-11-03
2026-11-01 success
2026-11-02 FAILED
2026-11-03 success
2026-11-02 still failing
bf-2026-11-01	success	{"date":"2026-11-01"}
bf-2026-11-02	failed	{"date":"2026-11-02","fail":true}
bf-2026-11-03	success	{"date":"2026-11-03"}
```

`2026-11-02` failed again on retry, because its conf is bad and a clear re-uses the run's conf. That
date needs a corrected run, not more retries. Running the script again skips every date:

```
skip bf-2026-11-01 (exists)
skip bf-2026-11-02 (exists)
skip bf-2026-11-03 (exists)
```

To redo a date, delete its run first: `airflow-cli runs delete cli_demo bf-2026-11-02`.

## Pitfalls

- The DAG has to read the date from `dag_run.conf`, e.g. `{{ dag_run.conf.get('date', ds) }}`.
  Otherwise every run processes its own logical date, which is the trigger time.
- If the DAG is paused, every run waits in `queued`. Unpause it for the backfill, and re-pause after if it was paused.
- Hundreds of runs at once can starve the scheduled runs. Trigger in batches, or rely on `max_active_runs`.
- A `read_only` profile refuses to trigger (exit 5). Use a write profile deliberately.
