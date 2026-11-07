---
name: airflow-cli
description: Drive Apache Airflow 2.11 from the shell with the `airflow-cli` tool (stable REST API, multiple server profiles). Covers listing and inspecting DAGs, triggering runs with conf, polling for completion, reading task logs and XComs, retrying failures, and managing variables, connections and pools. Use it when asked to run, automate, test or inspect Airflow DAGs.
---

# Using `airflow-cli`

`airflow-cli` wraps the [Airflow 2.11 stable REST API](https://airflow.apache.org/docs/apache-airflow/2.11.0/stable-rest-api-ref.html) (`/api/v1`).
It talks to the webserver over HTTP, so it works from any machine that can reach the Airflow UI.
It doesn't replace Airflow's own `airflow` command, which needs a local Airflow install.

This file is the general reference. Task-focused skills, each with a worked example, are in
[`skills/`](skills/):

| Skill | Use it to |
|---|---|
| [`airflow-run-dag`](skills/airflow-run-dag/SKILL.md) | trigger a run with conf, wait, read the result, clean up |
| [`airflow-debug-failed-run`](skills/airflow-debug-failed-run/SKILL.md) | find why a run failed, fix it, re-run only what failed |
| [`airflow-backfill`](skills/airflow-backfill/SKILL.md) | one run per date over a range, with retry and a summary ([script](skills/airflow-backfill/backfill.sh)) |
| [`airflow-promote-config`](skills/airflow-promote-config/SKILL.md) | diff and copy variables, pools and connections between environments |
| [`airflow-triage`](skills/airflow-triage/SKILL.md) | read-only health report: parse errors, stuck runs, pools ([script](skills/airflow-triage/triage.sh)) |

## 1. Configure profiles

Connection details live in `~/.airflow.toml`. Run `airflow-cli config example` to print a template:

```toml
# Profile used when --profile / AIRFLOW_CLI_PROFILE are not given.
default_profile = "local"

# Default credentials, used by any profile that doesn't set its own.
username = "admin"
password = "admin"

# Refuse anything that changes server state. Default for every profile.
read_only = false

[profiles.local]
base_url = "http://localhost:8080"

[profiles.staging]
base_url = "https://airflow.staging.example.com"

[profiles.prod]
base_url = "https://airflow.example.com"
username = "svc-airflow"         # overrides the top-level username/password
password = "change-me"
read_only = true                 # look but don't touch: overrides the top-level read_only

[profiles.astro]
base_url = "https://<deployment-url>"   # Astro: the Deployment URL
token = "<deployment API token>"        # sent as "Authorization: Bearer <token>"

[profiles.software]
base_url = "https://deployments.<base-domain>/<release-name>/airflow"   # Astronomer Software
token = "<service account API key>"
token_prefix = ""                       # sent as "Authorization: <key>", no "Bearer"
```

How the settings are chosen:

| Setting | Taken from (first match wins) |
|---|---|
| profile | `--profile/-p`, `$AIRFLOW_CLI_PROFILE`, `default_profile`, a profile named `default`, the only profile |
| base URL | `--url`/`$AIRFLOW_CLI_URL`, then the profile's `base_url` (`/api/v1` is added for you) |
| credentials | `--username`/`--password`/`--token` (or `$AIRFLOW_CLI_USERNAME`, `$AIRFLOW_CLI_PASSWORD`, `$AIRFLOW_CLI_TOKEN`), then the profile, then the top-level `username`/`password` |

### Read-only profiles

With `read_only = true`, the CLI refuses any request that would change server state. It fails before
sending anything and exits with code 5:

```
$ airflow-cli -p prod runs trigger etl
error: profile 'prod' is read-only: refusing POST /api/v1/dags/etl/dagRuns because it would change server state
```

- **Where it's set:** at the top level as the default, or per profile, where the profile's value wins.
- **Forcing it on:** `--read-only` or `AIRFLOW_CLI_READ_ONLY=true` force it on for one command or shell. That's useful
  when handing the CLI to an automated agent. Nothing on the command line can turn it off; only the config file can.
- **Still allowed:**
  - every GET (listing, logs, XComs, variables, DAG source and so on)
  - the batch list endpoints, which use POST but only read
  - `connections test`
  - dry runs: `tasks clear --dry-run`, `runs clear --dry-run` and `tasks set-state --dry-run`
- **Refused:** everything else, including the `api` passthrough. That covers triggering, pausing and
  unpausing, clearing, setting states, notes, deletes, re-parsing, and creating or changing variables,
  connections and pools.

This is a client-side guard against mistakes. For real access control, give the profile an Airflow
user with the `Viewer` role as well.

A profile with a `token` sends `Authorization: Bearer <token>`. Change the word with `token_prefix`, or
with `--token-prefix`/`AIRFLOW_CLI_TOKEN_PREFIX`. `token_prefix = ""` sends the token alone, which is
what Astronomer Software service account keys need. Otherwise the CLI uses HTTP basic auth,
which needs `airflow.api.auth.backend.basic_auth` in the server's `[api] auth_backends`.

Check the setup:

```bash
airflow-cli config profiles     # list profiles (which one is default)
airflow-cli config show         # resolved URL, auth and read_only for this invocation (secrets hidden)
airflow-cli ping                # reachable + credentials accepted
airflow-cli -p prod ping        # same, against another profile
airflow-cli health              # scheduler / metadatabase / triggerer health
```

`version` and `health` don't need auth, so use `ping` to test credentials.

## 2. Conventions to rely on

- **stdout carries data and stderr carries progress and warnings.**
  - Responses are printed as Airflow returns them, e.g. `{"dags": [...], "total_entries": N}`.
  - Use `--compact` for single-line JSON, which is handy with `jq`.
  - Logs, DAG source and `--value` print plain text.
- **Pagination:** list commands take `--limit` (default 100), `--offset` and `--order-by` (prefix `-` for descending). Use `--all` to fetch every page into one response.
- **JSON arguments** (`--conf`, `--data`, connection `--json`) take inline JSON, `@file.json`, or `-` for stdin.
- **Exit codes:**

  | Code | Meaning |
  |---|---|
  | 0 | success |
  | 1 | error (HTTP failure, bad input, auth) |
  | 2 | invalid command-line usage |
  | 3 | the DAG run failed (`runs wait`, `runs trigger --wait`) |
  | 4 | `wait` timed out |
  | 5 | refused because the profile is read-only |

- Quote run ids. Airflow's generated ids look like `manual__2026-11-06T09:23:41.773322+00:00`, and the CLI encodes them for you.

## 3. Worked example: trigger → poll → inspect → clean up

This is the full lifecycle in plain shell, polling by hand so every step is visible. It's also in
[`examples/run-dag.sh`](examples/run-dag.sh). It uses the demo DAG in
[`examples/dags/cli_demo.py`](examples/dags/cli_demo.py) (extract → transform → load), which takes
`{"rows": N, "fail": bool}` as conf.

```bash
#!/usr/bin/env bash
set -euo pipefail

DAG=cli_demo
RUN_ID="cli-$(date +%s)-$$"        # choose our own id so we know exactly what to clean up

# Runs of a paused DAG stay queued forever, so unpause for the duration and restore it afterwards.
was_paused=$(airflow-cli dags get "$DAG" --compact | jq -r '.is_paused')

cleanup() {
  airflow-cli runs delete "$DAG" "$RUN_ID" >/dev/null 2>&1 && echo "deleted run $RUN_ID" >&2
  [[ $was_paused == true ]] && airflow-cli dags pause "$DAG" >/dev/null && echo "re-paused $DAG" >&2
}
trap cleanup EXIT

# 1. Trigger a run with conf.
[[ $was_paused == true ]] && airflow-cli dags unpause "$DAG" >/dev/null
airflow-cli runs trigger "$DAG" --run-id "$RUN_ID" --conf '{"rows": 10}' --note "from my script" >/dev/null

# 2. Poll the run until it leaves queued/running.
deadline=$((SECONDS + 900))
while :; do
  state=$(airflow-cli runs get "$DAG" "$RUN_ID" --compact | jq -r '.state')
  echo "state=$state $(airflow-cli tasks list "$DAG" "$RUN_ID" --compact \
        | jq -r '[.task_instances[] | "\(.task_id)=\(.state // "none")"] | join(" ")')" >&2
  [[ $state != queued && $state != running ]] && break
  (( SECONDS < deadline )) || { airflow-cli runs set-state "$DAG" "$RUN_ID" failed; exit 4; }
  sleep 5
done

# 3. Inspect the outcome.
if [[ $state == success ]]; then
  airflow-cli xcom get "$DAG" "$RUN_ID" transform --value      # the task's return value
else
  for task in $(airflow-cli tasks list "$DAG" "$RUN_ID" --state failed --compact | jq -r '.task_instances[].task_id'); do
    echo "----- $task -----" >&2
    airflow-cli tasks logs "$DAG" "$RUN_ID" "$task" | tail -20 >&2
  done
  exit 3
fi
```

Output:

```
state=queued  extract=none transform=none load=none
state=running  extract=success transform=queued load=none
state=running  extract=success transform=success load=queued
state=success  extract=success transform=success load=success
{'rows': 10, 'sum_of_squares': 285}
deleted run cli-1793957165-15858
re-paused cli_demo
```

Run states are `queued`, `running`, `success` and `failed`. Task states include `none`, `scheduled`,
`queued`, `running`, `success`, `failed`, `upstream_failed`, `skipped` and `up_for_retry`.

### The same thing with the built-in poller

`runs trigger --wait` triggers, then polls until the run finishes. It prints task-state changes to
stderr and the final run to stdout:

```bash
if airflow-cli runs trigger cli_demo --unpause --conf @examples/conf/cli_demo.json --wait --timeout 900 > run.json; then
  echo "succeeded: $(jq -r .dag_run_id run.json)"
else
  rc=$?    # 3 = run failed, 4 = timed out
fi
```

```
triggered cli_demo manual__2026-11-06T09:23:41.773322+00:00
[    0s] cli_demo manual__...: queued — 3 none
[    5s] cli_demo manual__...: running — 1 success, 1 scheduled, 1 none
[    9s] cli_demo manual__...: running — 1 success, 1 failed, 1 upstream_failed
[   11s] cli_demo manual__...: failed — 1 success, 1 failed, 1 upstream_failed
task transform failed (try 1). Logs: airflow-cli tasks logs cli_demo 'manual__...' transform
task load upstream_failed
```

To wait on a run that something else started:

```bash
airflow-cli runs wait my_dag 'scheduled__2026-11-06T00:00:00+00:00' --interval 10
```

## 4. Feeding inputs into the CLI

### Run conf: inline, from a file, from stdin, or generated

```bash
airflow-cli runs trigger etl --conf '{"date": "2026-11-01"}'
airflow-cli runs trigger etl --conf @examples/conf/cli_demo.json
jq -n --arg d "$(date -u +%F)" '{date: $d, full_refresh: false}' | airflow-cli runs trigger etl --conf -
```

### Backfill-style fan-out: one run per input

```bash
for day in 2026-11-01 2026-11-02 2026-11-03; do
  airflow-cli runs trigger etl --run-id "backfill-$day" --conf "{\"date\": \"$day\"}" --id-only
done | while read -r run; do
  airflow-cli runs wait etl "$run" -q > /dev/null && echo "$run ok" || echo "$run FAILED"
done
```

### Variables from JSON

`variables import` takes `{"key": value}`. Non-string values are stored as JSON text, the same way
`airflow variables import` stores them. `variables export` writes the same shape back out:

```bash
airflow-cli variables import examples/variables.json
airflow-cli variables set api_base_url https://api.example.com --description "used by etl"
cat secrets/token.txt | airflow-cli variables set api_token --value-file -
airflow-cli variables get cli_demo_settings --value | jq .batch_size

# copy every variable from staging to prod
airflow-cli -p staging variables export | airflow-cli -p prod variables import -
```

### Connections from JSON or flags

```bash
airflow-cli connections create warehouse --json @examples/connection.json --conn-password "$PGPASS"
airflow-cli connections create slack --conn-type http --host hooks.slack.com --conn-password "$TOKEN"
airflow-cli connections update warehouse --host warehouse-2.internal     # only the fields you pass change
airflow-cli connections get warehouse                                     # passwords are never returned
```

### Deploying DAG files

The REST API can't upload DAG files. Ship them the way your deployment does: git-sync, an image
build, or copying to the dags folder. Then have the scheduler pick them up and check for parse errors:

```bash
airflow-cli dags reparse my_dag          # re-parse an existing DAG's file now
airflow-cli import-errors list           # any file that failed to parse
airflow-cli dags get my_dag              # appears once parsed
```

## 5. More recipes

### Find and inspect DAGs

```bash
airflow-cli dags list --compact | jq -r '.dags[] | "\(.dag_id)\tpaused=\(.is_paused)"'
airflow-cli dags list --pattern etl --tag team-data --unpaused
airflow-cli dags list --all --compact | jq '.total_entries'
airflow-cli dags details my_dag | jq '{schedule_interval, params}'
airflow-cli dags tasks my_dag --compact | jq -r '.tasks[].task_id'
airflow-cli dags source my_dag | less
airflow-cli dags stats my_dag other_dag
```

### Runs history

```bash
airflow-cli runs list my_dag --order-by -start_date --limit 5
airflow-cli runs list my_dag --state failed --all --compact | jq -r '.dag_runs[].dag_run_id'
airflow-cli runs list '~' --state running                # running runs across all DAGs
```

### Retry and repair

```bash
airflow-cli tasks clear my_dag "$RUN" --only-failed --dry-run        # preview
airflow-cli tasks clear my_dag "$RUN" --only-failed --downstream     # re-run failed tasks and what follows
airflow-cli runs wait my_dag "$RUN"

airflow-cli runs clear my_dag "$RUN"                     # re-run the whole run
airflow-cli tasks set-state my_dag "$RUN" flaky_task success
airflow-cli runs set-state my_dag "$RUN" failed          # give up on a stuck run
airflow-cli runs note my_dag "$RUN" "re-ran after upstream fix"
```

### Task logs, tries and XComs

```bash
airflow-cli tasks logs my_dag "$RUN" load                  # latest try
airflow-cli tasks logs my_dag "$RUN" load --try 1          # an earlier try
airflow-cli tasks logs my_dag "$RUN" process --map-index 3 # one mapped task instance
airflow-cli tasks tries my_dag "$RUN" load
airflow-cli xcom list my_dag "$RUN" transform
airflow-cli xcom get my_dag "$RUN" transform               # key defaults to return_value
```

### Pools

```bash
airflow-cli pools set warehouse 8 --description "max concurrent warehouse queries"
airflow-cli pools list --compact | jq -r '.pools[] | "\(.name) \(.occupied_slots)/\(.slots)"'
airflow-cli pools delete warehouse
```

### Anything else: the `api` passthrough

Every other endpoint (users, roles, datasets, event logs, plugins, providers and so on) is available
through `api`. Paths are relative to `/api/v1`:

```bash
airflow-cli api GET /users --query limit=50
airflow-cli api GET /eventLogs -q dag_id=my_dag -q limit=20
airflow-cli api POST /datasets/events --data '{"dataset_uri": "s3://bucket/key"}'
airflow-cli api GET '/datasets?uri_pattern=s3'
```

### Clean up after tests

```bash
airflow-cli runs list cli_demo --all --compact | jq -r '.dag_runs[] | select(.dag_run_id | startswith("cli-")) | .dag_run_id' \
  | while read -r r; do airflow-cli runs delete cli_demo "$r"; done
```

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `HTTP 401 Unauthorized` | Wrong credentials, or the server doesn't accept basic auth. Add `airflow.api.auth.backend.basic_auth` to `[api] auth_backends`, or use a `token` profile. |
| `HTTP 401`/`403` on Astronomer Software with a valid key | Software expects the key without `Bearer`. Set `token_prefix = ""` on the profile. |
| `HTTP 403 Forbidden` | The user's role lacks that permission. For example, `Viewer` can't change variables. |
| `... is read-only: refusing ...` (exit 5) | The profile has `read_only = true`, or `--read-only`/`AIRFLOW_CLI_READ_ONLY` is set. Switch profile, or change `read_only` in `~/.airflow.toml`. |
| Run stays `queued` | The DAG is paused (`trigger` warns about this; use `--unpause`), or the scheduler is down (check `airflow-cli health`). |
| `HTTP 404 DAG not found` right after adding a file | The file isn't parsed yet. Wait for the scheduler, check `import-errors list`, or use `dags reparse` for an existing DAG. |
| `server-config` gives `HTTP 403` | The server needs `[webserver] expose_config = True`. |
| `connections test` gives `HTTP 403` | The server needs `[core] test_connection = Enabled`. |
| `an id argument is empty` | A shell variable like `$RUN` was unset. The CLI refuses rather than calling a different endpoint. |
| `non-JSON (HTML) response` | `base_url` points at the wrong place, e.g. a proxy login page, or is missing a sub-path like `/airflow`. |
