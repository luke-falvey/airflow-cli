mod cli;
mod client;
mod config;
mod input;
mod output;
mod wait;

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use reqwest::Method;
use serde_json::{Map, Value, json};

use cli::*;
use client::{Client, Page, Query, ReadOnlyError, is_not_found};
use config::Auth;
use output::Output;

const EXAMPLE_CONFIG: &str = r#"# ~/.airflow.toml

# Profile used when --profile / AIRFLOW_CLI_PROFILE are not given.
default_profile = "local"

# Default credentials, used by any profile that doesn't set its own.
username = "admin"
password = "admin"

# Set read_only = true to refuse anything that changes server state (triggering
# runs, pausing DAGs, editing variables...). A profile's own read_only wins.
# read_only = false

[profiles.local]
base_url = "http://localhost:8080"

[profiles.staging]
base_url = "https://airflow.staging.example.com"

[profiles.prod]
base_url = "https://airflow.example.com"
username = "svc-airflow"
password = "change-me"
read_only = true

# Deployments behind a token-based gateway can use a bearer token instead.
# [profiles.managed]
# base_url = "https://airflow.managed.example.com"
# token = "eyJ..."
"#;

fn main() {
    let cli = Cli::parse();
    let code = match run(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            if e.downcast_ref::<ReadOnlyError>().is_some() {
                5
            } else {
                1
            }
        }
    };
    std::process::exit(code);
}

fn run(cli: Cli) -> Result<i32> {
    let out = Output {
        compact: cli.compact,
    };
    let config_path = match &cli.config {
        Some(p) => p.clone(),
        None => config::default_path()?,
    };
    let file = config::load(&config_path)?;
    let overrides = config::Overrides {
        profile: cli.profile.clone(),
        url: cli.url.clone(),
        username: cli.username.clone(),
        password: cli.password.clone(),
        token: cli.token.clone(),
        read_only: cli.read_only,
    };

    if let Command::Config(cmd) = &cli.command {
        match cmd {
            ConfigCmd::Path => println!("{}", config_path.display()),
            ConfigCmd::Example => print!("{EXAMPLE_CONFIG}"),
            ConfigCmd::Profiles => {
                let default = file.profile_name(None).ok().flatten();
                let list: Vec<Value> = file
                    .profiles
                    .iter()
                    .map(|(name, p)| {
                        json!({
                            "name": name,
                            "base_url": p.base_url,
                            "auth": if p.token.is_some() { "token" } else { "basic" },
                            "username": p.username.clone().or_else(|| file.username.clone()),
                            "read_only": p.read_only.or(file.read_only).unwrap_or(false),
                            "default": Some(name) == default.as_ref(),
                        })
                    })
                    .collect();
                out.json(&Value::Array(list));
            }
            ConfigCmd::Show => {
                let r = file.resolve(&overrides)?;
                let (auth, username) = match &r.auth {
                    Auth::None => ("none", None),
                    Auth::Basic { username, .. } => ("basic", Some(username.clone())),
                    Auth::Bearer(_) => ("token", None),
                };
                out.json(&json!({
                    "config_file": config_path.display().to_string(),
                    "profile": r.profile,
                    "base_url": r.base_url,
                    "api_root": config::api_root(&r.base_url),
                    "auth": auth,
                    "username": username,
                    "read_only": r.read_only,
                }));
            }
        }
        return Ok(0);
    }

    let resolved = file.resolve(&overrides)?;
    let client = Client::new(&resolved, Duration::from_secs(cli.http_timeout))?;
    dispatch(&client, out, cli.command)
}

fn q(pairs: &[(&str, String)]) -> Query {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn show(out: Output, v: Result<Value>) -> Result<i32> {
    out.json(&v?);
    Ok(0)
}

fn dispatch(client: &Client, out: Output, command: Command) -> Result<i32> {
    match command {
        Command::Config(_) => unreachable!("handled before connecting"),
        Command::Health => show(out, client.get(&["health"], &vec![])),
        Command::Version => show(out, client.get(&["version"], &vec![])),
        Command::Ping => {
            let version = client.get(&["version"], &vec![])?;
            // /version needs no auth, so make one authenticated call too.
            client
                .get(&["dags"], &q(&[("limit", "1".into())]))
                .context("server is reachable but the authenticated request failed")?;
            out.json(&json!({
                "ok": true,
                "version": version.get("version"),
                "api_root": client.url(&[]).as_str().trim_end_matches('/'),
            }));
            Ok(0)
        }
        Command::ServerConfig { section, option } => match (section, option) {
            (Some(s), Some(o)) => show(
                out,
                client.get(&["config", "section", &s, "option", &o], &vec![]),
            ),
            (s, None) => {
                let query = s.map(|s| q(&[("section", s)])).unwrap_or_default();
                show(out, client.get(&["config"], &query))
            }
            (None, Some(_)) => bail!("OPTION needs SECTION"),
        },
        Command::Api {
            method,
            path,
            query,
            data,
        } => {
            let method = Method::from_bytes(method.to_uppercase().as_bytes())
                .map_err(|_| anyhow!("invalid HTTP method {method}"))?;
            let url = reqwest::Url::parse(&format!(
                "{}/{}",
                client.url(&[]).as_str().trim_end_matches('/'),
                path.trim_start_matches('/')
            ))
            .with_context(|| format!("invalid path {path}"))?;
            let query: Query = query
                .iter()
                .map(|kv| {
                    kv.split_once('=')
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .ok_or_else(|| anyhow!("--query expects KEY=VALUE, got {kv}"))
                })
                .collect::<Result<_>>()?;
            let body = data.map(|d| input::json_arg(&d)).transpose()?;
            show(out, client.json_url(method, url, &query, body.as_ref()))
        }
        Command::Dags(cmd) => dags(client, out, cmd),
        Command::Runs(cmd) => runs(client, out, cmd),
        Command::Tasks(cmd) => tasks(client, out, cmd),
        Command::Xcom(cmd) => xcom(client, out, cmd),
        Command::Variables(cmd) => variables(client, out, cmd),
        Command::Connections(cmd) => connections(client, out, cmd),
        Command::Pools(cmd) => pools(client, out, cmd),
        Command::ImportErrors(cmd) => match cmd {
            ImportErrorsCmd::List { page } => show(
                out,
                client.list(&["importErrors"], "import_errors", vec![], &page),
            ),
            ImportErrorsCmd::Get { id } => {
                show(out, client.get(&["importErrors", &id.to_string()], &vec![]))
            }
        },
    }
}

fn set_paused(client: &Client, dag_id: &str, paused: bool) -> Result<Value> {
    client.json(
        Method::PATCH,
        &["dags", dag_id],
        &q(&[("update_mask", "is_paused".into())]),
        Some(&json!({ "is_paused": paused })),
    )
}

fn file_token(client: &Client, dag_id: &str) -> Result<String> {
    let dag = client.get(&["dags", dag_id], &vec![])?;
    dag.get("file_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("DAG {dag_id} has no file_token"))
}

fn dags(client: &Client, out: Output, cmd: DagsCmd) -> Result<i32> {
    match cmd {
        DagsCmd::List {
            pattern,
            tags,
            paused,
            unpaused,
            include_inactive,
            page,
        } => {
            let mut query = Query::new();
            if let Some(p) = pattern {
                query.push(("dag_id_pattern".into(), p));
            }
            for t in tags {
                query.push(("tags".into(), t));
            }
            if paused || unpaused {
                query.push(("paused".into(), paused.to_string()));
            }
            if include_inactive {
                query.push(("only_active".into(), "false".into()));
            }
            show(out, client.list(&["dags"], "dags", query, &page))
        }
        DagsCmd::Get { dag_id } => show(out, client.get(&["dags", &dag_id], &vec![])),
        DagsCmd::Details { dag_id } => {
            show(out, client.get(&["dags", &dag_id, "details"], &vec![]))
        }
        DagsCmd::Pause { dag_id } => show(out, set_paused(client, &dag_id, true)),
        DagsCmd::Unpause { dag_id } => show(out, set_paused(client, &dag_id, false)),
        DagsCmd::Delete { dag_id } => show(
            out,
            client.json(Method::DELETE, &["dags", &dag_id], &vec![], None),
        ),
        DagsCmd::Source { dag_id } => {
            let token = file_token(client, &dag_id)?;
            out.text(&client.text(&["dagSources", &token], &vec![])?);
            Ok(0)
        }
        DagsCmd::Tasks { dag_id, order_by } => {
            let query = order_by.map(|o| q(&[("order_by", o)])).unwrap_or_default();
            show(out, client.get(&["dags", &dag_id, "tasks"], &query))
        }
        DagsCmd::Task { dag_id, task_id } => show(
            out,
            client.get(&["dags", &dag_id, "tasks", &task_id], &vec![]),
        ),
        DagsCmd::Reparse { dag_id } => {
            let token = file_token(client, &dag_id)?;
            client.json(Method::PUT, &["parseDagFile", &token], &vec![], None)?;
            eprintln!("requested re-parse of {dag_id}'s file");
            Ok(0)
        }
        DagsCmd::Stats { dag_ids } => show(
            out,
            client.get(&["dagStats"], &q(&[("dag_ids", dag_ids.join(","))])),
        ),
        DagsCmd::Warnings { dag_id, page } => {
            let query = dag_id.map(|d| q(&[("dag_id", d)])).unwrap_or_default();
            show(
                out,
                client.list(&["dagWarnings"], "dag_warnings", query, &page),
            )
        }
    }
}

fn state_query(states: Vec<String>) -> Query {
    states
        .into_iter()
        .map(|s| ("state".to_string(), s))
        .collect()
}

fn runs(client: &Client, out: Output, cmd: RunsCmd) -> Result<i32> {
    match cmd {
        RunsCmd::List {
            dag_id,
            states,
            page,
        } => show(
            out,
            client.list(
                &["dags", &dag_id, "dagRuns"],
                "dag_runs",
                state_query(states),
                &page,
            ),
        ),
        RunsCmd::Get { dag_id, run_id } => show(
            out,
            client.get(&["dags", &dag_id, "dagRuns", &run_id], &vec![]),
        ),
        RunsCmd::Trigger {
            dag_id,
            conf,
            run_id,
            logical_date,
            note,
            unpause,
            id_only,
            wait,
            wait_args,
        } => {
            if unpause {
                set_paused(client, &dag_id, false)?;
            } else {
                let dag = client.get(&["dags", &dag_id], &vec![])?;
                if dag.get("is_paused").and_then(Value::as_bool) == Some(true) {
                    eprintln!(
                        "warning: DAG {dag_id} is paused, so this run will stay queued; pass --unpause or run `airflow-cli dags unpause {dag_id}`"
                    );
                }
            }
            let mut body = Map::new();
            if let Some(c) = conf {
                let c = input::json_arg(&c)?;
                if !c.is_object() {
                    bail!("--conf must be a JSON object");
                }
                body.insert("conf".into(), c);
            }
            if let Some(r) = run_id {
                body.insert("dag_run_id".into(), Value::String(r));
            }
            if let Some(d) = logical_date {
                body.insert("logical_date".into(), Value::String(d));
            }
            if let Some(n) = note {
                body.insert("note".into(), Value::String(n));
            }
            let dag_run = client.json(
                Method::POST,
                &["dags", &dag_id, "dagRuns"],
                &vec![],
                Some(&Value::Object(body)),
            )?;
            let new_id = dag_run
                .get("dag_run_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("response has no dag_run_id"))?
                .to_string();
            if wait {
                if !wait_args.quiet {
                    eprintln!("triggered {dag_id} {new_id}");
                }
                return wait::run(client, out, &dag_id, &new_id, &wait_args);
            }
            if id_only {
                println!("{new_id}");
            } else {
                out.json(&dag_run);
            }
            Ok(0)
        }
        RunsCmd::Wait {
            dag_id,
            run_id,
            wait_args,
        } => wait::run(client, out, &dag_id, &run_id, &wait_args),
        RunsCmd::Delete { dag_id, run_id } => show(
            out,
            client.json(
                Method::DELETE,
                &["dags", &dag_id, "dagRuns", &run_id],
                &vec![],
                None,
            ),
        ),
        RunsCmd::Clear {
            dag_id,
            run_id,
            dry_run,
        } => show(
            out,
            client.json(
                Method::POST,
                &["dags", &dag_id, "dagRuns", &run_id, "clear"],
                &vec![],
                Some(&json!({ "dry_run": dry_run })),
            ),
        ),
        RunsCmd::SetState {
            dag_id,
            run_id,
            state,
        } => {
            let state = match state {
                RunState::Success => "success",
                RunState::Failed => "failed",
                RunState::Queued => "queued",
            };
            show(
                out,
                client.json(
                    Method::PATCH,
                    &["dags", &dag_id, "dagRuns", &run_id],
                    &vec![],
                    Some(&json!({ "state": state })),
                ),
            )
        }
        RunsCmd::Note {
            dag_id,
            run_id,
            note,
        } => show(
            out,
            client.json(
                Method::PATCH,
                &["dags", &dag_id, "dagRuns", &run_id, "setNote"],
                &vec![],
                Some(&json!({ "note": note })),
            ),
        ),
    }
}

fn ti_path<'a>(
    dag_id: &'a str,
    run_id: &'a str,
    task_id: &'a str,
    map_index: &'a Option<String>,
) -> Vec<&'a str> {
    let mut p = vec!["dags", dag_id, "dagRuns", run_id, "taskInstances", task_id];
    if let Some(m) = map_index {
        p.push(m);
    }
    p
}

fn tasks(client: &Client, out: Output, cmd: TasksCmd) -> Result<i32> {
    match cmd {
        TasksCmd::List {
            dag_id,
            run_id,
            states,
            page,
        } => show(
            out,
            client.list(
                &["dags", &dag_id, "dagRuns", &run_id, "taskInstances"],
                "task_instances",
                state_query(states),
                &page,
            ),
        ),
        TasksCmd::Get {
            dag_id,
            run_id,
            task_id,
            map_index,
        } => {
            let m = map_index.map(|m| m.to_string());
            show(
                out,
                client.get(&ti_path(&dag_id, &run_id, &task_id, &m), &vec![]),
            )
        }
        TasksCmd::Logs {
            dag_id,
            run_id,
            task_id,
            try_number,
            map_index,
        } => {
            let try_number = match try_number {
                Some(t) => t,
                None => {
                    let m = map_index.map(|m| m.to_string());
                    let ti = client.get(&ti_path(&dag_id, &run_id, &task_id, &m), &vec![])?;
                    match ti.get("try_number").and_then(Value::as_u64) {
                        Some(t) if t > 0 => t,
                        _ => bail!(
                            "task {task_id} has not run yet (state: {})",
                            ti.get("state").and_then(Value::as_str).unwrap_or("none")
                        ),
                    }
                }
            };
            let mut query = q(&[("full_content", "true".into())]);
            if let Some(m) = map_index {
                query.push(("map_index".into(), m.to_string()));
            }
            let try_str = try_number.to_string();
            let text = client.text(
                &[
                    "dags",
                    &dag_id,
                    "dagRuns",
                    &run_id,
                    "taskInstances",
                    &task_id,
                    "logs",
                    &try_str,
                ],
                &query,
            )?;
            out.text(&text);
            Ok(0)
        }
        TasksCmd::Tries {
            dag_id,
            run_id,
            task_id,
        } => show(
            out,
            client.get(
                &[
                    "dags",
                    &dag_id,
                    "dagRuns",
                    &run_id,
                    "taskInstances",
                    &task_id,
                    "tries",
                ],
                &vec![],
            ),
        ),
        TasksCmd::Clear {
            dag_id,
            run_id,
            tasks,
            only_failed,
            downstream,
            upstream,
            reset_dag_run,
            dry_run,
        } => {
            // Airflow's dry_run defaults to true for this endpoint, so always send it.
            let mut body = json!({
                "dag_run_id": run_id,
                "dry_run": dry_run,
                "only_failed": only_failed,
                "include_downstream": downstream,
                "include_upstream": upstream,
                "reset_dag_runs": reset_dag_run,
            });
            if !tasks.is_empty() {
                body["task_ids"] = json!(tasks);
            }
            show(
                out,
                client.json(
                    Method::POST,
                    &["dags", &dag_id, "clearTaskInstances"],
                    &vec![],
                    Some(&body),
                ),
            )
        }
        TasksCmd::SetState {
            dag_id,
            run_id,
            task_id,
            state,
            map_index,
            dry_run,
        } => {
            let state = match state {
                TaskState::Success => "success",
                TaskState::Failed => "failed",
                TaskState::Skipped => "skipped",
            };
            let m = map_index.map(|m| m.to_string());
            show(
                out,
                client.json(
                    Method::PATCH,
                    &ti_path(&dag_id, &run_id, &task_id, &m),
                    &vec![],
                    Some(&json!({ "new_state": state, "dry_run": dry_run })),
                ),
            )
        }
    }
}

fn xcom(client: &Client, out: Output, cmd: XcomCmd) -> Result<i32> {
    match cmd {
        XcomCmd::List {
            dag_id,
            run_id,
            task_id,
            map_index,
        } => {
            let query = map_index
                .map(|m| q(&[("map_index", m.to_string())]))
                .unwrap_or_default();
            show(
                out,
                client.list(
                    &[
                        "dags",
                        &dag_id,
                        "dagRuns",
                        &run_id,
                        "taskInstances",
                        &task_id,
                        "xcomEntries",
                    ],
                    "xcom_entries",
                    query,
                    &Page::everything(),
                ),
            )
        }
        XcomCmd::Get {
            dag_id,
            run_id,
            task_id,
            key,
            map_index,
            deserialize,
            value,
        } => {
            let mut query = Query::new();
            if let Some(m) = map_index {
                query.push(("map_index".into(), m.to_string()));
            }
            if deserialize {
                query.push(("deserialize".into(), "true".into()));
            }
            let entry = client.get(
                &[
                    "dags",
                    &dag_id,
                    "dagRuns",
                    &run_id,
                    "taskInstances",
                    &task_id,
                    "xcomEntries",
                    &key,
                ],
                &query,
            )?;
            if value {
                out.json(entry.get("value").unwrap_or(&Value::Null));
            } else {
                out.json(&entry);
            }
            Ok(0)
        }
    }
}

/// PATCH a resource, or POST it to the collection when it doesn't exist yet.
fn upsert(
    client: &Client,
    item: &[&str],
    collection: &[&str],
    update_mask: Option<&str>,
    body: &Value,
) -> Result<Value> {
    let query = update_mask
        .map(|m| q(&[("update_mask", m.to_string())]))
        .unwrap_or_default();
    match client.json(Method::PATCH, item, &query, Some(body)) {
        Err(e) if is_not_found(&e) => client.json(Method::POST, collection, &vec![], Some(body)),
        other => other,
    }
}

fn set_variable(
    client: &Client,
    key: &str,
    value: String,
    description: Option<String>,
) -> Result<Value> {
    let mut body = json!({ "key": key, "value": value });
    if let Some(d) = description {
        body["description"] = Value::String(d);
    }
    match client.get(&["variables", key], &vec![]) {
        Err(e) if is_not_found(&e) => {
            client.json(Method::POST, &["variables"], &vec![], Some(&body))
        }
        Err(e) => Err(e),
        Ok(existing) => {
            // A PATCH without update_mask replaces every field, so carry the
            // existing description over when none is given. (Airflow 2.11
            // rejects update_mask=value.)
            if body.get("description").is_none() {
                body["description"] = existing.get("description").cloned().unwrap_or(Value::Null);
            }
            client.json(Method::PATCH, &["variables", key], &vec![], Some(&body))
        }
    }
}

fn variables(client: &Client, out: Output, cmd: VariablesCmd) -> Result<i32> {
    match cmd {
        VariablesCmd::List { page } => {
            show(out, client.list(&["variables"], "variables", vec![], &page))
        }
        VariablesCmd::Get { key, value } => {
            let v = client.get(&["variables", &key], &vec![])?;
            if value {
                out.text(v.get("value").and_then(Value::as_str).unwrap_or(""));
            } else {
                out.json(&v);
            }
            Ok(0)
        }
        VariablesCmd::Set {
            key,
            value,
            value_file,
            description,
        } => {
            let value = match (value, value_file) {
                (Some(v), _) => v,
                (None, Some(f)) => input::read_source(&f)?,
                (None, None) => unreachable!("clap requires one"),
            };
            show(out, set_variable(client, &key, value, description))
        }
        VariablesCmd::Delete { key } => show(
            out,
            client.json(Method::DELETE, &["variables", &key], &vec![], None),
        ),
        VariablesCmd::Import { file } => {
            let doc = input::json_source(&file)?;
            let map = doc
                .as_object()
                .ok_or_else(|| anyhow!("expected a JSON object of {{\"key\": value}}"))?;
            for (k, v) in map {
                let value = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                set_variable(client, k, value, None).with_context(|| format!("setting {k}"))?;
            }
            eprintln!("imported {} variables", map.len());
            Ok(0)
        }
        VariablesCmd::Export => {
            let all = client.list(&["variables"], "variables", vec![], &Page::everything())?;
            let mut map = Map::new();
            for v in all
                .get("variables")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(k) = v.get("key").and_then(Value::as_str) {
                    map.insert(k.into(), v.get("value").cloned().unwrap_or(Value::Null));
                }
            }
            out.json(&Value::Object(map));
            Ok(0)
        }
    }
}

/// Connection fields from --json plus individual flags (flags win).
fn connection_body(id: &str, f: ConnectionFields) -> Result<Map<String, Value>> {
    let mut body = match f.json {
        Some(j) => match input::json_arg(&j)? {
            Value::Object(m) => m,
            _ => bail!("--json must be a JSON object"),
        },
        None => Map::new(),
    };
    let mut put = |k: &str, v: Option<Value>| {
        if let Some(v) = v {
            body.insert(k.into(), v);
        }
    };
    put("conn_type", f.conn_type.map(Value::String));
    put("host", f.host.map(Value::String));
    put("login", f.login.map(Value::String));
    put("password", f.conn_password.map(Value::String));
    put("port", f.port.map(|p| json!(p)));
    put("schema", f.schema.map(Value::String));
    put("extra", f.extra.map(Value::String));
    put("description", f.description.map(Value::String));
    body.insert("connection_id".into(), Value::String(id.into()));
    Ok(body)
}

fn connections(client: &Client, out: Output, cmd: ConnectionsCmd) -> Result<i32> {
    match cmd {
        ConnectionsCmd::List { page } => show(
            out,
            client.list(&["connections"], "connections", vec![], &page),
        ),
        ConnectionsCmd::Get { connection_id } => {
            show(out, client.get(&["connections", &connection_id], &vec![]))
        }
        ConnectionsCmd::Create {
            connection_id,
            fields,
        } => {
            let body = connection_body(&connection_id, fields)?;
            if !body.contains_key("conn_type") {
                bail!("--conn-type is required (or set conn_type in --json)");
            }
            show(
                out,
                client.json(
                    Method::POST,
                    &["connections"],
                    &vec![],
                    Some(&Value::Object(body)),
                ),
            )
        }
        ConnectionsCmd::Update {
            connection_id,
            fields,
        } => {
            let changes = connection_body(&connection_id, fields)?;
            let mask: Vec<&str> = changes
                .keys()
                .map(String::as_str)
                .filter(|k| *k != "connection_id")
                .collect();
            if mask.is_empty() {
                bail!("nothing to update: pass --json or field flags");
            }
            // The server validates the whole object, so start from the current one.
            let mut body = client.get(&["connections", &connection_id], &vec![])?;
            let obj = body
                .as_object_mut()
                .ok_or_else(|| anyhow!("unexpected connection response"))?;
            for (k, v) in &changes {
                obj.insert(k.clone(), v.clone());
            }
            show(
                out,
                client.json(
                    Method::PATCH,
                    &["connections", &connection_id],
                    &q(&[("update_mask", mask.join(","))]),
                    Some(&body),
                ),
            )
        }
        ConnectionsCmd::Delete { connection_id } => show(
            out,
            client.json(
                Method::DELETE,
                &["connections", &connection_id],
                &vec![],
                None,
            ),
        ),
        ConnectionsCmd::Test {
            connection_id,
            fields,
        } => {
            let body = connection_body(&connection_id, fields)?;
            let resp = client.json(
                Method::POST,
                &["connections", "test"],
                &vec![],
                Some(&Value::Object(body)),
            )?;
            out.json(&resp);
            Ok(
                if resp.get("status").and_then(Value::as_bool) == Some(false) {
                    1
                } else {
                    0
                },
            )
        }
    }
}

fn pools(client: &Client, out: Output, cmd: PoolsCmd) -> Result<i32> {
    match cmd {
        PoolsCmd::List { page } => show(out, client.list(&["pools"], "pools", vec![], &page)),
        PoolsCmd::Get { name } => show(out, client.get(&["pools", &name], &vec![])),
        PoolsCmd::Set {
            name,
            slots,
            description,
            include_deferred,
        } => {
            let mut body =
                json!({ "name": name, "slots": slots, "include_deferred": include_deferred });
            if let Some(d) = description {
                body["description"] = Value::String(d);
            }
            show(
                out,
                upsert(client, &["pools", &name], &["pools"], None, &body),
            )
        }
        PoolsCmd::Delete { name } => show(
            out,
            client.json(Method::DELETE, &["pools", &name], &vec![], None),
        ),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cli_definition_is_consistent() {
        use clap::CommandFactory;
        super::Cli::command().debug_assert();
    }
}
