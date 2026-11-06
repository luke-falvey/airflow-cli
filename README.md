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
- **Configuration:** `~/.airflow.toml` holds named profiles (each with `base_url` plus `username`/`password` or `token`) and top-level default credentials. Pick a profile with `-p NAME` or `AIRFLOW_CLI_PROFILE`.
- **Command coverage:**
  - DAGs: list, get, details, pause, unpause, delete, source, tasks, reparse, stats, warnings.
  - DAG runs: list, get, trigger, wait, clear, set state, note, delete.
  - Task instances: list, get, logs, tries, clear, set state.
  - XComs, variables (with import/export), connections, pools, import errors, health, version, server config.
  - Everything else in the API through `airflow-cli api METHOD PATH`.

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
