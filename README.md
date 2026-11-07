# airflow-cli

A Rust command-line client for the [Apache Airflow 2.11 stable REST API](https://airflow.apache.org/docs/apache-airflow/2.11.0/stable-rest-api-ref.html). It supports multiple server profiles.

## Install

Download a prebuilt binary from the [latest release](https://github.com/luke-falvey/airflow-cli/releases/latest):

| Platform | Archive |
|---|---|
| Linux x86_64 (static) | `airflow-cli-x86_64-unknown-linux-musl.tar.gz` |
| Linux ARM64 (static) | `airflow-cli-aarch64-unknown-linux-musl.tar.gz` |
| macOS Apple Silicon | `airflow-cli-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `airflow-cli-x86_64-apple-darwin.tar.gz` |
| Windows x86_64 | `airflow-cli-x86_64-pc-windows-msvc.zip` |
| Windows ARM64 | `airflow-cli-aarch64-pc-windows-msvc.zip` |

```bash
curl -fsSL https://github.com/luke-falvey/airflow-cli/releases/latest/download/airflow-cli-x86_64-unknown-linux-musl.tar.gz | tar xz
sudo mv airflow-cli-x86_64-unknown-linux-musl/airflow-cli /usr/local/bin/
```

Each release includes a `SHA256SUMS` file. To build from source instead, run `cargo install --path .`.

## Quick start

```bash
airflow-cli config example > ~/.airflow.toml    # then edit URLs and credentials
airflow-cli ping
airflow-cli runs trigger my_dag --conf '{"date": "2026-11-01"}' --wait
```

- **Usage guide and worked examples:** [SKILLS.md](SKILLS.md). It has YAML front matter, so it can be dropped into a skills directory as-is.
- **Configuration:** profiles in `~/.airflow.toml`, with optional read-only mode; see [Configuration](#configuration).
- **Command coverage:**
  - DAGs: list, get, details, pause, unpause, delete, source, tasks, reparse, stats, warnings.
  - DAG runs: list, get, trigger, wait, clear, set state, note, delete.
  - Task instances: list, get, logs, tries, clear, set state.
  - XComs, variables (with import/export), connections, pools, import errors, health, version, server config.
  - Everything else in the API through `airflow-cli api METHOD PATH`.

## Configuration

Connection settings live in `~/.airflow.toml`. Each profile is one Airflow webserver. Username,
password and `read_only` at the top level are the defaults for every profile that doesn't set its own.

```toml
# ~/.airflow.toml

# Profile used when neither --profile nor AIRFLOW_CLI_PROFILE is given.
default_profile = "local"

# Defaults for every profile below.
username = "admin"
password = "admin"
read_only = false          # true = refuse anything that changes server state

[profiles.local]
base_url = "http://localhost:8080"          # /api/v1 is appended automatically

[profiles.staging]
base_url = "https://airflow.staging.example.com"
# no credentials here, so it uses the top-level ones

# Production: a read-only profile for everyday use...
[profiles.prod]
base_url = "https://airflow.example.com"
username = "svc-airflow"                    # overrides the top-level username
password = "change-me"
read_only = true

# ...and a separate one for deliberate changes.
[profiles.prod-write]
base_url = "https://airflow.example.com"
username = "svc-airflow"
password = "change-me"

# Astro, or any token gateway: a bearer token instead of basic auth.
[profiles.astro]
base_url = "https://<deployment-url>"        # the Deployment URL shown in Astro
token = "<deployment API token>"

# Astronomer Software (on-premise): the service account key goes in the header as-is.
[profiles.software]
base_url = "https://deployments.mycompany.example.com/galactic-stars-1234/airflow"   # sub-paths work
token = "<service account API key>"
token_prefix = ""                           # Authorization: <key>   (no "Bearer")
```

How each setting is chosen (first match wins):

| Setting | Order |
|---|---|
| Profile | `--profile`/`-p`, `AIRFLOW_CLI_PROFILE`, `default_profile`, a profile named `default`, the only profile if there's just one |
| Server URL | `--url`/`AIRFLOW_CLI_URL`, then the profile's `base_url` |
| Auth | the profile's `token` (unless `--username` is given), otherwise basic auth with `--username`/`--password` (or `AIRFLOW_CLI_USERNAME`/`AIRFLOW_CLI_PASSWORD`), then the profile, then the top level. `--token`/`AIRFLOW_CLI_TOKEN` supply a token. |
| Token header | `Authorization: <token_prefix> <token>`. The prefix comes from `--token-prefix`/`AIRFLOW_CLI_TOKEN_PREFIX`, then the profile's `token_prefix`, default `Bearer`; `""` sends the token alone. |
| `read_only` | on if `--read-only` or `AIRFLOW_CLI_READ_ONLY` is set, otherwise the profile's value, then the top level, then off |

Basic auth needs `airflow.api.auth.backend.basic_auth` in the server's `[api] auth_backends`. Read-only
mode is checked before anything is sent. It allows reads and dry runs, refuses the rest with exit code 5,
and can only be switched off in this file, never from the command line. Unknown keys are rejected, so a
typo like `baseurl` is an error rather than silently ignored. Use `--config PATH` or `AIRFLOW_CLI_CONFIG`
to read a different file.

```bash
airflow-cli config example        # print a starter file
airflow-cli config profiles       # list profiles: default, auth type, read_only
airflow-cli -p prod config show   # the settings a command would use (secrets hidden)
airflow-cli -p prod ping          # check the server is reachable and the credentials work
```

The file holds passwords and tokens, so make it readable only by you: `chmod 600 ~/.airflow.toml`.

### Astronomer

| Platform | `base_url` | Credentials |
|---|---|---|
| **Astro** (hosted), Airflow 2 deployments | the Deployment URL from the Astro UI | a Deployment or Workspace API token: `token = "..."` (sent as `Bearer`) |
| **Astronomer Software** (on-premise) | `https://deployments.<base-domain>/<release-name>/airflow` | a Deployment service account API key: `token = "..."` plus `token_prefix = ""` |

The CLI adds `/api/v1` to `base_url`, so give the URL up to `.../airflow` for Software, not the `api/v1` part.
For a quick session on Astro without saving a token, borrow the Astro CLI's login token, which expires:

```bash
AIRFLOW_CLI_TOKEN=$(astro auth token) airflow-cli -p astro ping
```

Not supported:
- Airflow 3 deployments, which serve `/api/v2`. This CLI speaks Airflow 2's `/api/v1`.
- Logging in through a browser like `astro login` does. Use an API token or service account key instead.

## Claude skills

[`skills/`](skills/) has task-focused skills, each with a worked example that was run against a real
Airflow 2.11 server:
- **`airflow-run-dag`:** trigger, wait, read the result, clean up.
- **`airflow-debug-failed-run`:** read the failing task's log, fix the cause, re-run only what failed.
- **`airflow-backfill`:** one run per date over a range, retry once, summary. Includes a script.
- **`airflow-promote-config`:** diff and copy variables, pools and connections between profiles.
- **`airflow-triage`:** read-only health report. Includes a script that exits 1 when something is wrong.

To make them available to Claude Code in every project, copy them into your personal skills folder.
For one project only, use that project's `.claude/skills/` instead.

```bash
mkdir -p ~/.claude/skills && cp -r skills/* ~/.claude/skills/
```

They're also included in each release archive.

## Development

CI (`.github/workflows/ci.yml`) runs on every push and pull request:
- `cargo fmt` and `clippy` checks.
- Tests on Linux, macOS and Windows.
- Release builds for all six platforms above, uploaded as workflow artifacts.

To publish a release, set `version` in `Cargo.toml`, commit, then push a matching tag. CI checks that the tag matches the version, then attaches the archives and checksums to a GitHub release. Tags with a hyphen (`v0.2.0-rc1`) become pre-releases.

```bash
git tag v0.1.0
git push origin v0.1.0
```

Local checks:

```bash
cargo test        # unit tests + end-to-end tests against a mock server
cargo clippy --all-targets
```

To try it against a real server, run Airflow standalone with basic auth. The admin password is printed in the container log.

```bash
docker run -d --name airflow -p 8080:8080 \
  -e AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth \
  apache/airflow:2.11.0 standalone
docker cp examples/dags/cli_demo.py airflow:/opt/airflow/dags/
docker logs airflow 2>&1 | grep 'Login with username'
```
