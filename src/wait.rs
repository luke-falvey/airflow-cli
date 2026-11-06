//! Polling a DAG run until it finishes.

use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::Value;

use crate::cli::WaitArgs;
use crate::client::{Client, Page};
use crate::output::Output;

pub const EXIT_FAILED: i32 = 3;
pub const EXIT_TIMEOUT: i32 = 4;

pub fn run_is_active(state: &str) -> bool {
    matches!(state, "queued" | "running")
}

fn task_state(ti: &Value) -> &str {
    ti.get("state").and_then(Value::as_str).unwrap_or("none")
}

pub fn task_failed(state: &str) -> bool {
    matches!(state, "failed" | "upstream_failed")
}

/// "3 success, 1 running, 2 scheduled" in first-seen order.
fn summarise(tis: &[Value]) -> String {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for s in tis.iter().map(task_state) {
        match counts.iter_mut().find(|(k, _)| k == s) {
            Some((_, n)) => *n += 1,
            None => counts.push((s.to_string(), 1)),
        }
    }
    if counts.is_empty() {
        return "no task instances yet".into();
    }
    counts
        .iter()
        .map(|(k, n)| format!("{n} {k}"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn task_instances(client: &Client, dag_id: &str, run_id: &str) -> Result<Vec<Value>> {
    let resp = client.list(
        &["dags", dag_id, "dagRuns", run_id, "taskInstances"],
        "task_instances",
        vec![],
        &Page::everything(),
    )?;
    Ok(resp
        .get("task_instances")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

/// Poll `GET /dags/{dag_id}/dagRuns/{run_id}`. Prints the final run on stdout
/// and returns the exit code.
pub fn run(
    client: &Client,
    out: Output,
    dag_id: &str,
    run_id: &str,
    args: &WaitArgs,
) -> Result<i32> {
    let start = Instant::now();
    let mut last_line = String::new();
    loop {
        let dag_run = client.get(&["dags", dag_id, "dagRuns", run_id], &vec![])?;
        let state = dag_run
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let tis = if args.quiet && run_is_active(&state) {
            Vec::new()
        } else {
            task_instances(client, dag_id, run_id)?
        };

        if !args.quiet {
            let line = format!("{state} — {}", summarise(&tis));
            if line != last_line {
                eprintln!(
                    "[{:>5.0}s] {dag_id} {run_id}: {line}",
                    start.elapsed().as_secs_f64()
                );
                last_line = line;
            }
        }

        if !run_is_active(&state) {
            out.json(&dag_run);
            if state == "success" {
                return Ok(0);
            }
            for ti in tis.iter().filter(|t| task_failed(task_state(t))) {
                let task = ti.get("task_id").and_then(Value::as_str).unwrap_or("?");
                if task_state(ti) == "failed" {
                    eprintln!(
                        "task {task} failed (try {}). Logs: airflow-cli tasks logs {dag_id} '{run_id}' {task}",
                        ti.get("try_number").and_then(Value::as_u64).unwrap_or(0)
                    );
                } else {
                    eprintln!("task {task} {}", task_state(ti));
                }
            }
            return Ok(EXIT_FAILED);
        }

        if args.timeout > 0.0 && start.elapsed().as_secs_f64() >= args.timeout {
            out.json(&dag_run);
            eprintln!(
                "timed out after {}s waiting for {dag_id} {run_id}",
                args.timeout
            );
            return Ok(EXIT_TIMEOUT);
        }
        std::thread::sleep(Duration::from_secs_f64(args.interval.max(0.1)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn summary_counts_states() {
        let tis = vec![
            json!({"state": "success"}),
            json!({"state": null}),
            json!({"state": "success"}),
        ];
        assert_eq!(summarise(&tis), "2 success, 1 none");
        assert_eq!(summarise(&[]), "no task instances yet");
    }
}
