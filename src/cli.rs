//! Command-line definitions.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::client::Page;

#[derive(Parser)]
#[command(
    name = "airflow-cli",
    version,
    about = "Command-line client for the Apache Airflow 2.11 stable REST API",
    after_help = "Connection settings come from ~/.airflow.toml (see `airflow-cli config example`).\n\
                  JSON arguments accept inline JSON, @file, or - for stdin.\n\n\
                  Exit codes: 0 success, 1 error, 2 usage error, 3 a DAG run or task failed, 4 wait timed out,\n\
                  5 refused because the profile is read-only."
)]
pub struct Cli {
    /// Profile from the config file to use.
    #[arg(short, long, global = true, env = "AIRFLOW_CLI_PROFILE")]
    pub profile: Option<String>,

    /// Path to the config file.
    #[arg(long, global = true, env = "AIRFLOW_CLI_CONFIG", value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Webserver URL, overriding the profile's base_url (e.g. http://localhost:8080).
    #[arg(long, global = true, env = "AIRFLOW_CLI_URL")]
    pub url: Option<String>,

    /// Username, overriding the config file.
    #[arg(long, global = true, env = "AIRFLOW_CLI_USERNAME")]
    pub username: Option<String>,

    /// Password, overriding the config file.
    #[arg(
        long,
        global = true,
        env = "AIRFLOW_CLI_PASSWORD",
        hide_env_values = true
    )]
    pub password: Option<String>,

    /// Bearer token, overriding the config file.
    #[arg(long, global = true, env = "AIRFLOW_CLI_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Refuse requests that change server state, whatever the config says.
    #[arg(long, global = true, env = "AIRFLOW_CLI_READ_ONLY")]
    pub read_only: bool,

    /// Print JSON on a single line.
    #[arg(long, global = true)]
    pub compact: bool,

    /// HTTP request timeout in seconds.
    #[arg(long, global = true, default_value_t = 120, value_name = "SECS")]
    pub http_timeout: u64,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// DAGs: list, inspect, pause/unpause, source, tasks.
    #[command(subcommand, visible_alias = "dag")]
    Dags(DagsCmd),
    /// DAG runs: trigger, poll for completion, clear, set state.
    #[command(subcommand, visible_alias = "run")]
    Runs(RunsCmd),
    /// Task instances: list, logs, clear, set state.
    #[command(subcommand, visible_alias = "task")]
    Tasks(TasksCmd),
    /// XCom entries of a task instance.
    #[command(subcommand)]
    Xcom(XcomCmd),
    /// Airflow variables.
    #[command(subcommand, visible_alias = "vars")]
    Variables(VariablesCmd),
    /// Airflow connections.
    #[command(subcommand, visible_alias = "conns")]
    Connections(ConnectionsCmd),
    /// Pools.
    #[command(subcommand)]
    Pools(PoolsCmd),
    /// DAG import (parse) errors.
    #[command(subcommand)]
    ImportErrors(ImportErrorsCmd),
    /// Scheduler/metadatabase health (GET /health, no auth needed).
    Health,
    /// Server version (GET /version; Airflow serves this without auth).
    Version,
    /// Check that the server is reachable and the credentials work.
    Ping,
    /// Read the server's airflow.cfg (GET /config; needs [webserver] expose_config).
    ServerConfig {
        /// Only this section.
        section: Option<String>,
        /// Only this option (needs SECTION).
        option: Option<String>,
    },
    /// Call any endpoint under /api/v1, e.g. `api GET /users` or `api POST /datasets/events --data @e.json`.
    Api {
        /// HTTP method.
        method: String,
        /// Path relative to /api/v1 (e.g. /dags/my_dag/details). May include ?query.
        path: String,
        /// Query parameter KEY=VALUE (repeatable).
        #[arg(short, long = "query", value_name = "KEY=VALUE")]
        query: Vec<String>,
        /// JSON request body (inline, @file or -).
        #[arg(short, long, value_name = "JSON")]
        data: Option<String>,
    },
    /// Inspect this CLI's configuration and profiles.
    #[command(subcommand)]
    Config(ConfigCmd),
}

#[derive(Args, Clone)]
pub struct WaitArgs {
    /// Seconds between status polls.
    #[arg(long, default_value_t = 5.0, value_name = "SECS")]
    pub interval: f64,
    /// Give up after this many seconds (0 = wait forever). Exit code 4 on timeout.
    #[arg(long, default_value_t = 0.0, value_name = "SECS")]
    pub timeout: f64,
    /// Don't print progress to stderr.
    #[arg(short, long)]
    pub quiet: bool,
}

#[derive(Subcommand)]
pub enum DagsCmd {
    /// List DAGs (GET /dags).
    #[command(visible_alias = "ls")]
    List {
        /// Only DAG ids containing this text.
        #[arg(long)]
        pattern: Option<String>,
        /// Only DAGs with this tag (repeatable; any match).
        #[arg(long = "tag")]
        tags: Vec<String>,
        /// Only paused DAGs.
        #[arg(long, conflicts_with = "unpaused")]
        paused: bool,
        /// Only unpaused DAGs.
        #[arg(long)]
        unpaused: bool,
        /// Include DAGs whose files were removed (only_active=false).
        #[arg(long)]
        include_inactive: bool,
        #[command(flatten)]
        page: Page,
    },
    /// Get a DAG (GET /dags/{dag_id}).
    Get { dag_id: String },
    /// Full DAG details (GET /dags/{dag_id}/details).
    Details { dag_id: String },
    /// Pause a DAG.
    Pause { dag_id: String },
    /// Unpause a DAG.
    Unpause { dag_id: String },
    /// Delete a DAG and its metadata (DELETE /dags/{dag_id}).
    #[command(visible_alias = "rm")]
    Delete { dag_id: String },
    /// Print the DAG's Python source.
    Source { dag_id: String },
    /// List a DAG's tasks (GET /dags/{dag_id}/tasks).
    Tasks {
        dag_id: String,
        /// Sort field, e.g. task_id (prefix with - for descending).
        #[arg(long, allow_hyphen_values = true)]
        order_by: Option<String>,
    },
    /// Get one task definition (GET /dags/{dag_id}/tasks/{task_id}).
    Task { dag_id: String, task_id: String },
    /// Ask the scheduler to re-parse the DAG's file (PUT /parseDagFile/{file_token}).
    Reparse { dag_id: String },
    /// Run counts per state (GET /dagStats).
    Stats {
        #[arg(required = true)]
        dag_ids: Vec<String>,
    },
    /// DAG warnings (GET /dagWarnings).
    Warnings {
        #[arg(long)]
        dag_id: Option<String>,
        #[command(flatten)]
        page: Page,
    },
}

#[derive(Copy, Clone, ValueEnum)]
pub enum RunState {
    Success,
    Failed,
    Queued,
}

#[derive(Subcommand)]
pub enum RunsCmd {
    /// List DAG runs (GET /dags/{dag_id}/dagRuns). Use ~ for all DAGs.
    #[command(visible_alias = "ls")]
    List {
        dag_id: String,
        /// Only runs in this state (repeatable): queued, running, success, failed.
        #[arg(long = "state")]
        states: Vec<String>,
        #[command(flatten)]
        page: Page,
    },
    /// Get a DAG run (GET /dags/{dag_id}/dagRuns/{dag_run_id}).
    Get { dag_id: String, run_id: String },
    /// Trigger a DAG run (POST /dags/{dag_id}/dagRuns). Prints the run; `-q`-friendly id with --id-only.
    Trigger {
        dag_id: String,
        /// Run configuration as JSON object (inline, @file or -).
        #[arg(long, value_name = "JSON")]
        conf: Option<String>,
        /// Custom run id (default: generated by Airflow, manual__<date>).
        #[arg(long)]
        run_id: Option<String>,
        /// Logical date, RFC 3339 (e.g. 2026-01-01T00:00:00Z).
        #[arg(long)]
        logical_date: Option<String>,
        /// Note shown in the UI.
        #[arg(long)]
        note: Option<String>,
        /// Unpause the DAG first (runs of paused DAGs stay queued).
        #[arg(long)]
        unpause: bool,
        /// Print only the new run id.
        #[arg(long)]
        id_only: bool,
        /// After triggering, poll until the run finishes (like `runs wait`).
        #[arg(short, long)]
        wait: bool,
        #[command(flatten)]
        wait_args: WaitArgs,
    },
    /// Poll a DAG run until it is no longer queued/running.
    ///
    /// Exits 0 on success, 3 on failure, 4 on timeout. Prints the final run.
    Wait {
        dag_id: String,
        run_id: String,
        #[command(flatten)]
        wait_args: WaitArgs,
    },
    /// Delete a DAG run (DELETE /dags/{dag_id}/dagRuns/{dag_run_id}).
    #[command(visible_alias = "rm")]
    Delete { dag_id: String, run_id: String },
    /// Clear a DAG run so its tasks re-run (POST .../clear).
    Clear {
        dag_id: String,
        run_id: String,
        /// Only show what would be cleared.
        #[arg(long)]
        dry_run: bool,
    },
    /// Set a DAG run's state (PATCH /dags/{dag_id}/dagRuns/{dag_run_id}).
    SetState {
        dag_id: String,
        run_id: String,
        #[arg(value_enum)]
        state: RunState,
    },
    /// Set a DAG run's note (PATCH .../setNote).
    Note {
        dag_id: String,
        run_id: String,
        note: String,
    },
}

#[derive(Copy, Clone, ValueEnum)]
pub enum TaskState {
    Success,
    Failed,
    Skipped,
}

#[derive(Subcommand)]
pub enum TasksCmd {
    /// List task instances of a run (GET .../taskInstances). Use ~ for all DAGs/runs.
    #[command(visible_alias = "ls")]
    List {
        dag_id: String,
        run_id: String,
        /// Only tasks in this state (repeatable), e.g. failed, success, running.
        #[arg(long = "state")]
        states: Vec<String>,
        #[command(flatten)]
        page: Page,
    },
    /// Get a task instance.
    Get {
        dag_id: String,
        run_id: String,
        task_id: String,
        /// Map index of a mapped task.
        #[arg(long)]
        map_index: Option<i64>,
    },
    /// Print a task instance's log (defaults to the latest try).
    Logs {
        dag_id: String,
        run_id: String,
        task_id: String,
        /// Try number (default: the task's current try).
        #[arg(long = "try")]
        try_number: Option<u64>,
        /// Map index of a mapped task.
        #[arg(long)]
        map_index: Option<i64>,
    },
    /// List the tries of a task instance (GET .../tries).
    Tries {
        dag_id: String,
        run_id: String,
        task_id: String,
    },
    /// Clear task instances in a run so they re-run (POST /dags/{dag_id}/clearTaskInstances).
    Clear {
        dag_id: String,
        run_id: String,
        /// Only these tasks (repeatable or comma-separated; default: all).
        #[arg(long = "task", value_delimiter = ',')]
        tasks: Vec<String>,
        /// Only failed tasks.
        #[arg(long)]
        only_failed: bool,
        /// Also clear downstream tasks.
        #[arg(long)]
        downstream: bool,
        /// Also clear upstream tasks.
        #[arg(long)]
        upstream: bool,
        /// Also reset the DAG run's state.
        #[arg(long)]
        reset_dag_run: bool,
        /// Only show what would be cleared.
        #[arg(long)]
        dry_run: bool,
    },
    /// Set a task instance's state (PATCH .../taskInstances/{task_id}).
    SetState {
        dag_id: String,
        run_id: String,
        task_id: String,
        #[arg(value_enum)]
        state: TaskState,
        #[arg(long)]
        map_index: Option<i64>,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub enum XcomCmd {
    /// List XCom entries of a task instance.
    #[command(visible_alias = "ls")]
    List {
        dag_id: String,
        run_id: String,
        task_id: String,
        #[arg(long)]
        map_index: Option<i64>,
    },
    /// Get one XCom entry. Prints the value only with --value.
    Get {
        dag_id: String,
        run_id: String,
        task_id: String,
        /// XCom key (default return_value).
        #[arg(default_value = "return_value")]
        key: String,
        #[arg(long)]
        map_index: Option<i64>,
        /// Ask the server to deserialize the value.
        #[arg(long)]
        deserialize: bool,
        /// Print only the value.
        #[arg(long)]
        value: bool,
    },
}

#[derive(Subcommand)]
pub enum VariablesCmd {
    /// List variables.
    #[command(visible_alias = "ls")]
    List {
        #[command(flatten)]
        page: Page,
    },
    /// Get a variable. With --value, prints only its value.
    Get {
        key: String,
        #[arg(long)]
        value: bool,
    },
    /// Create or update a variable.
    Set {
        key: String,
        /// The value (or use --value-file).
        #[arg(required_unless_present = "value_file")]
        value: Option<String>,
        /// Read the value from a file (- for stdin).
        #[arg(long, conflicts_with = "value", value_name = "FILE|-")]
        value_file: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a variable.
    #[command(visible_alias = "rm")]
    Delete { key: String },
    /// Create/update variables from a JSON object {"key": value, ...} (file or -).
    /// Non-string values are stored as JSON text, like `airflow variables import`.
    Import {
        #[arg(default_value = "-", value_name = "FILE|-")]
        file: String,
    },
    /// Print all variables as a JSON object {"key": "value", ...}.
    Export,
}

#[derive(Args, Default)]
pub struct ConnectionFields {
    /// Connection fields as JSON (inline, @file or -); flags override its fields.
    #[arg(long, value_name = "JSON")]
    pub json: Option<String>,
    #[arg(long)]
    pub conn_type: Option<String>,
    #[arg(long)]
    pub host: Option<String>,
    #[arg(long)]
    pub login: Option<String>,
    #[arg(long = "conn-password")]
    pub conn_password: Option<String>,
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long)]
    pub schema: Option<String>,
    /// Extra field as JSON text.
    #[arg(long)]
    pub extra: Option<String>,
    #[arg(long)]
    pub description: Option<String>,
}

#[derive(Subcommand)]
pub enum ConnectionsCmd {
    /// List connections.
    #[command(visible_alias = "ls")]
    List {
        #[command(flatten)]
        page: Page,
    },
    /// Get a connection (password is never returned).
    Get { connection_id: String },
    /// Create a connection.
    Create {
        connection_id: String,
        #[command(flatten)]
        fields: ConnectionFields,
    },
    /// Update fields of a connection (only the fields you pass change).
    Update {
        connection_id: String,
        #[command(flatten)]
        fields: ConnectionFields,
    },
    /// Delete a connection.
    #[command(visible_alias = "rm")]
    Delete { connection_id: String },
    /// Test connection settings without saving them (needs [core] test_connection = Enabled).
    Test {
        connection_id: String,
        #[command(flatten)]
        fields: ConnectionFields,
    },
}

#[derive(Subcommand)]
pub enum PoolsCmd {
    /// List pools.
    #[command(visible_alias = "ls")]
    List {
        #[command(flatten)]
        page: Page,
    },
    /// Get a pool.
    Get { name: String },
    /// Create or update a pool.
    Set {
        name: String,
        slots: i64,
        #[arg(long)]
        description: Option<String>,
        /// Count deferred tasks against the pool.
        #[arg(long)]
        include_deferred: bool,
    },
    /// Delete a pool.
    #[command(visible_alias = "rm")]
    Delete { name: String },
}

#[derive(Subcommand)]
pub enum ImportErrorsCmd {
    /// List import errors.
    #[command(visible_alias = "ls")]
    List {
        #[command(flatten)]
        page: Page,
    },
    /// Get one import error.
    Get { id: u64 },
}

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// Show the resolved connection settings (secrets hidden).
    Show,
    /// List the profiles in the config file.
    Profiles,
    /// Print the config file path.
    Path,
    /// Print an example ~/.airflow.toml.
    Example,
}
