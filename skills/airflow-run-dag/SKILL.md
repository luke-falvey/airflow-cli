---
name: airflow-run-dag
description: Trigger an Apache Airflow 2.x DAG run with airflow-cli, pass it conf, wait for it to finish, read its result (XCom) and logs, and clean up. Use when asked to run, kick off, test or execute a DAG, or to "run X with these parameters and tell me the result".
---

# Run a DAG and get its result

Uses [`airflow-cli`](https://github.com/luke-falvey/airflow-cli) against the server in the current
profile. Needs `airflow-cli` and `jq` on `PATH`, and a profile in `~/.airflow.toml`. Check both with
`airflow-cli ping`. For the full command reference, see the repo's `SKILLS.md`.

## Steps

1. **Check the DAG before triggering.** Confirm it exists, has no import errors, and whether it's
   paused. Read its `params` to learn which conf keys it accepts.
2. **Trigger with an explicit `--run-id`** so you know exactly which run is yours. Add `--unpause`,
   because runs of a paused DAG sit in `queued` forever. Add `--wait` to poll until the run finishes.
3. **Read the outcome.** Exit code 0 means success, 3 means the run failed, 4 means `--timeout` was hit.
   Read the return value with `xcom get`, and output with `tasks logs`.
4. **Clean up** test runs, and re-pause the DAG if it was paused before.

## Worked example

The `cli_demo` DAG (`examples/dags/cli_demo.py`) runs extract → transform → load.

```bash
DAG=cli_demo
RUN_ID="adhoc-$(date -u +%Y%m%dT%H%M%S)"

# 1. Check it
airflow-cli dags get "$DAG" --compact | jq -c '{dag_id, is_paused, has_import_errors}'
airflow-cli dags details "$DAG" --compact | jq -c '.params | map_values(.value)'
```

```
{"dag_id":"cli_demo","is_paused":true,"has_import_errors":false}
{"fail":false,"rows":5}
```

```bash
# 2. Trigger with conf and wait (progress goes to stderr, the final run to stdout)
airflow-cli runs trigger "$DAG" --unpause --run-id "$RUN_ID" \
  --conf '{"rows": 12}' --note "ad-hoc run from airflow-cli" \
  --wait --interval 3 --timeout 600 --compact > run.json
echo "exit=$?  state=$(jq -r .state run.json)"
```

```
triggered cli_demo adhoc-20261106T225752
[    0s] cli_demo adhoc-20261106T225752: queued — 3 none
[    3s] cli_demo adhoc-20261106T225752: running — 1 queued, 2 none
[    7s] cli_demo adhoc-20261106T225752: running — 2 success, 1 none
[   10s] cli_demo adhoc-20261106T225752: success — 3 success
exit=0  state=success
```

```bash
# 3. Read the result
airflow-cli xcom get "$DAG" "$RUN_ID" transform --value
airflow-cli tasks logs "$DAG" "$RUN_ID" extract | grep -m1 'extracting'
```

```
{'rows': 12, 'sum_of_squares': 506}
[2026-11-06T22:57:57.170+0000] {subprocess.py:88} INFO - Running command: ['/usr/bin/bash', '-c', 'echo extracting 12 rows for 2026-11-06']
```

```bash
# 4. Clean up (the DAG was paused before, so put it back)
airflow-cli runs delete "$DAG" "$RUN_ID"
airflow-cli dags pause "$DAG" > /dev/null
```

## Variations

- **Conf from a file or generated:** `--conf @conf.json`, or `jq -n '{date: "2026-11-01"}' | airflow-cli runs trigger etl --conf -`.
- **Don't block:** drop `--wait`, keep the id (`--id-only`), and check later with `airflow-cli runs wait "$DAG" "$RUN_ID" --timeout 1800`.
- **Another environment:** add `-p staging`. A profile with `read_only = true` refuses to trigger (exit 5), which is intended.
- **Script that works whatever happens:** `examples/run-dag.sh` wraps these steps with a `trap` that always cleans up.

## Pitfalls

- With `set -e`, call `runs trigger --wait` in an `if`, or the script stops on exit 3 or 4 before you can report.
- An XCom value is whatever the task returned. Python dicts come back as Python repr text, as shown above.
  Ask with `--deserialize` only if the server enables it.
- Run ids can't start with `scheduled__`, `manual__`, `backfill__` or `dataset_triggered__`; Airflow reserves them.
- The DAG must already be deployed. The API can't upload DAG files.
