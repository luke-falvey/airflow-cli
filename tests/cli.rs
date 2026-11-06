//! End-to-end tests: run the binary against a mock Airflow server.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use assert_cmd::Command;
use mockito::{Matcher, Server};
use predicates::prelude::*;
use serde_json::json;
use tempfile::TempDir;

// base64("admin:secret")
const BASIC: &str = "Basic YWRtaW46c2VjcmV0";

struct Env {
    dir: TempDir,
}

impl Env {
    fn new(config: &str) -> Env {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(".airflow.toml"), config).unwrap();
        Env { dir }
    }

    fn for_server(server: &Server) -> Env {
        Env::new(&format!(
            "username = \"admin\"\npassword = \"secret\"\n[profiles.local]\nbase_url = \"{}\"\n",
            server.url()
        ))
    }

    fn cmd(&self) -> Command {
        let mut c = Command::cargo_bin("airflow-cli").unwrap();
        // Point at the temp config explicitly (HOME isn't used on Windows) and keep
        // any HTTP proxy out of calls to the local mock server.
        c.env("AIRFLOW_CLI_CONFIG", self.dir.path().join(".airflow.toml"))
            .env("HOME", self.dir.path())
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env_remove("AIRFLOW_CLI_PROFILE")
            .env_remove("AIRFLOW_CLI_URL")
            .env_remove("AIRFLOW_CLI_USERNAME")
            .env_remove("AIRFLOW_CLI_PASSWORD")
            .env_remove("AIRFLOW_CLI_TOKEN");
        c
    }
}

#[test]
fn sends_basic_auth_from_top_level_defaults() {
    let mut server = Server::new();
    let m = server
        .mock("GET", "/api/v1/dags")
        .match_header("authorization", BASIC)
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("dag_id_pattern".into(), "etl".into()),
            Matcher::UrlEncoded("tags".into(), "team".into()),
            Matcher::UrlEncoded("limit".into(), "100".into()),
        ]))
        .with_body(r#"{"dags":[{"dag_id":"etl_daily"}],"total_entries":1}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args([
            "dags",
            "list",
            "--pattern",
            "etl",
            "--tag",
            "team",
            "--compact",
        ])
        .assert()
        .success()
        .stdout("{\"dags\":[{\"dag_id\":\"etl_daily\"}],\"total_entries\":1}\n");
    m.assert();
}

#[test]
fn token_profile_sends_bearer() {
    let mut server = Server::new();
    let m = server
        .mock("GET", "/api/v1/version")
        .match_header("authorization", "Bearer tok123")
        .with_body(r#"{"version":"2.11.0"}"#)
        .create();
    let env = Env::new(&format!(
        "username = \"admin\"\npassword = \"secret\"\n[profiles.local]\nbase_url = \"http://127.0.0.1:1\"\n[profiles.managed]\nbase_url = \"{}\"\ntoken = \"tok123\"\n",
        server.url()
    ));
    env.cmd()
        .args(["-p", "managed", "version", "--compact"])
        .assert()
        .success();
    m.assert();
}

#[test]
fn list_all_follows_pages() {
    let mut server = Server::new();
    for (offset, body) in [
        (
            "0",
            r#"{"variables":[{"key":"a"},{"key":"b"}],"total_entries":3}"#,
        ),
        ("2", r#"{"variables":[{"key":"c"}],"total_entries":3}"#),
    ] {
        server
            .mock("GET", "/api/v1/variables")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("limit".into(), "2".into()),
                Matcher::UrlEncoded("offset".into(), offset.into()),
            ]))
            .with_body(body)
            .create();
    }
    Env::for_server(&server)
        .cmd()
        .args(["variables", "list", "--all", "--limit", "2", "--compact"])
        .assert()
        .success()
        .stdout(
            "{\"total_entries\":3,\"variables\":[{\"key\":\"a\"},{\"key\":\"b\"},{\"key\":\"c\"}]}\n",
        );
}

#[test]
fn trigger_and_wait_until_success() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/dags/etl")
        .with_body(r#"{"dag_id":"etl","is_paused":false}"#)
        .create();
    let trigger = server
        .mock("POST", "/api/v1/dags/etl/dagRuns")
        .match_body(Matcher::Json(
            json!({"conf": {"day": "2026-01-01"}, "dag_run_id": "r1"}),
        ))
        .with_body(r#"{"dag_run_id":"r1","state":"queued"}"#)
        .create();
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = polls.clone();
    server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1")
        .with_body_from_request(move |_| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let state = ["queued", "running", "success"][n.min(2)];
            json!({"dag_run_id": "r1", "state": state})
                .to_string()
                .into_bytes()
        })
        .create();
    server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1/taskInstances")
        .match_query(Matcher::Any)
        .with_body(r#"{"task_instances":[{"task_id":"t","state":"running"}],"total_entries":1}"#)
        .create();

    Env::for_server(&server)
        .cmd()
        .args([
            "runs",
            "trigger",
            "etl",
            "--run-id",
            "r1",
            "--conf",
            r#"{"day":"2026-01-01"}"#,
            "--wait",
            "--interval",
            "0.1",
            "--compact",
        ])
        .assert()
        .success()
        .stdout("{\"dag_run_id\":\"r1\",\"state\":\"success\"}\n")
        .stderr(predicate::str::contains("running — 1 running"));
    trigger.assert();
    assert!(polls.load(Ordering::SeqCst) >= 3);
}

#[test]
fn trigger_warns_when_paused() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/dags/etl")
        .with_body(r#"{"dag_id":"etl","is_paused":true}"#)
        .create();
    server
        .mock("POST", "/api/v1/dags/etl/dagRuns")
        .with_body(r#"{"dag_run_id":"manual__x","state":"queued"}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["runs", "trigger", "etl", "--id-only"])
        .assert()
        .success()
        .stdout("manual__x\n")
        .stderr(predicate::str::contains("is paused"));
}

#[test]
fn failed_run_exits_3_and_names_failed_tasks() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1")
        .with_body(r#"{"dag_run_id":"r1","state":"failed"}"#)
        .create();
    server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1/taskInstances")
        .match_query(Matcher::Any)
        .with_body(
            r#"{"task_instances":[{"task_id":"a","state":"failed","try_number":2},{"task_id":"b","state":"upstream_failed","try_number":0}],"total_entries":2}"#,
        )
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["runs", "wait", "etl", "r1"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("task a failed (try 2)"))
        .stderr(predicate::str::contains("task b upstream_failed"));
}

#[test]
fn wait_times_out_with_exit_4() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1")
        .with_body(r#"{"dag_run_id":"r1","state":"running"}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args([
            "runs",
            "wait",
            "etl",
            "r1",
            "-q",
            "--interval",
            "0.1",
            "--timeout",
            "0.3",
        ])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("timed out"));
}

#[test]
fn run_ids_with_special_characters_are_encoded() {
    let mut server = Server::new();
    let m = server
        .mock(
            "GET",
            "/api/v1/dags/etl/dagRuns/manual__2026-01-01T00:00:00+00:00",
        )
        .with_body(r#"{"state":"success"}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["runs", "get", "etl", "manual__2026-01-01T00:00:00+00:00"])
        .assert()
        .success();
    m.assert();
}

#[test]
fn logs_default_to_current_try() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1/taskInstances/t")
        .with_body(r#"{"task_id":"t","try_number":2,"state":"success"}"#)
        .create();
    let logs = server
        .mock("GET", "/api/v1/dags/etl/dagRuns/r1/taskInstances/t/logs/2")
        .match_header("accept", "text/plain")
        .match_query(Matcher::UrlEncoded("full_content".into(), "true".into()))
        .with_body("line one\nline two")
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["tasks", "logs", "etl", "r1", "t"])
        .assert()
        .success()
        .stdout("line one\nline two\n");
    logs.assert();
}

#[test]
fn clear_tasks_always_sends_dry_run() {
    let mut server = Server::new();
    let m = server
        .mock("POST", "/api/v1/dags/etl/clearTaskInstances")
        .match_body(Matcher::PartialJson(json!({
            "dag_run_id": "r1", "dry_run": false, "only_failed": true, "task_ids": ["a", "b"]
        })))
        .with_body(r#"{"task_instances":[]}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args([
            "tasks",
            "clear",
            "etl",
            "r1",
            "--task",
            "a,b",
            "--only-failed",
        ])
        .assert()
        .success();
    m.assert();
}

#[test]
fn variable_set_creates_when_missing() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/variables/new_key")
        .with_status(404)
        .with_body(r#"{"detail":"Variable not found","status":404,"title":"Variable not found"}"#)
        .create();
    let post = server
        .mock("POST", "/api/v1/variables")
        .match_body(Matcher::Json(
            json!({"key": "new_key", "value": "{\"a\":1}"}),
        ))
        .with_body(r#"{"key":"new_key","value":"{\"a\":1}"}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["variables", "import", "-"])
        .write_stdin(r#"{"new_key": {"a": 1}}"#)
        .assert()
        .success()
        .stderr(predicate::str::contains("imported 1 variables"));
    post.assert();
}

#[test]
fn variable_set_keeps_existing_description() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/variables/k")
        .with_body(r#"{"key":"k","value":"old","description":"keep me"}"#)
        .create();
    let patch = server
        .mock("PATCH", "/api/v1/variables/k")
        .match_body(Matcher::Json(
            json!({"key": "k", "value": "new", "description": "keep me"}),
        ))
        .with_body(r#"{"key":"k","value":"new","description":"keep me"}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["variables", "set", "k", "new"])
        .assert()
        .success();
    patch.assert();
}

#[test]
fn api_errors_are_reported() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/dags/nope")
        .with_status(404)
        .with_body(r#"{"detail":"The DAG with dag_id: nope was not found","status":404,"title":"DAG not found","type":"x"}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args(["dags", "get", "nope"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "HTTP 404 DAG not found: The DAG with dag_id: nope was not found",
        ));
}

#[test]
fn ping_fails_on_bad_credentials() {
    let mut server = Server::new();
    server
        .mock("GET", "/api/v1/version")
        .with_body(r#"{"version":"2.11.0"}"#)
        .create();
    server
        .mock("GET", "/api/v1/dags")
        .match_query(Matcher::Any)
        .with_status(401)
        .with_body(r#"{"title":"Unauthorized","status":401}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .arg("ping")
        .assert()
        .code(1)
        .stderr(predicate::str::contains("HTTP 401 Unauthorized"));
}

#[test]
fn empty_ids_are_rejected() {
    let env = Env::new("[profiles.x]\nbase_url = \"http://127.0.0.1:1\"\n");
    env.cmd()
        .args(["runs", "get", "etl", ""])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("id argument is empty"));
}

#[test]
fn api_passthrough_sends_body_and_query() {
    let mut server = Server::new();
    let m = server
        .mock("POST", "/api/v1/datasets/events")
        .match_query(Matcher::UrlEncoded("x".into(), "1".into()))
        .match_body(Matcher::Json(json!({"dataset_uri": "s3://b/k"})))
        .with_body(r#"{"id":7}"#)
        .create();
    Env::for_server(&server)
        .cmd()
        .args([
            "api",
            "post",
            "/datasets/events",
            "-q",
            "x=1",
            "-d",
            r#"{"dataset_uri":"s3://b/k"}"#,
            "--compact",
        ])
        .assert()
        .success()
        .stdout("{\"id\":7}\n");
    m.assert();
}

#[test]
fn config_show_hides_secrets() {
    let env = Env::new(
        "username = \"admin\"\npassword = \"hunter2\"\n[profiles.x]\nbase_url = \"http://af:8080/\"\n",
    );
    env.cmd()
        .args(["config", "show"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"api_root\": \"http://af:8080/api/v1\"",
        ))
        .stdout(predicate::str::contains("hunter2").not());
}
