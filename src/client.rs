//! Thin HTTP client for the Airflow stable REST API (`/api/v1`).

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Method;
use reqwest::blocking::{Client as Http, RequestBuilder, Response};
use serde_json::Value;

use crate::config::{Auth, Resolved, api_root};

pub type Query = Vec<(String, String)>;

pub struct Client {
    http: Http,
    api: reqwest::Url,
    auth: Auth,
    /// Set when the profile is read-only; holds a label for error messages.
    read_only: Option<String>,
}

/// A request refused because the profile is read-only. Exits with code 5.
#[derive(Debug)]
pub struct ReadOnlyError {
    pub mode: String,
    pub method: Method,
    pub path: String,
}

impl std::fmt::Display for ReadOnlyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: refusing {} {} because it would change server state\n\
             hint: read-only comes from `read_only` in ~/.airflow.toml (top level or the profile), --read-only, or AIRFLOW_CLI_READ_ONLY",
            self.mode, self.method, self.path
        )
    }
}

impl std::error::Error for ReadOnlyError {}

/// Whether a request leaves server state unchanged. GETs always do; a few
/// POST/PATCH endpoints only read, or only preview when `dry_run` is true.
/// `segments` is the path below `/api/v1`.
pub fn is_read_only_request(method: &Method, segments: &[&str], body: Option<&Value>) -> bool {
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return true;
    }
    let dry_run = body.and_then(|b| b.get("dry_run")).and_then(Value::as_bool) == Some(true);
    match (method.as_str(), segments) {
        // Batch list endpoints use POST but only read.
        ("POST", ["dags", "~", "dagRuns", "list"]) => true,
        ("POST", ["dags", "~", "dagRuns", "~", "taskInstances", "list"]) => true,
        // Tests connection settings without saving them.
        ("POST", ["connections", "test"]) => true,
        // Previews: these endpoints change nothing when dry_run is true.
        ("POST", ["dags", _, "clearTaskInstances"])
        | ("POST", ["dags", _, "updateTaskInstancesState"])
        | ("POST", ["dags", _, "dagRuns", _, "clear"])
        | ("PATCH", ["dags", _, "dagRuns", _, "taskInstances", _])
        | ("PATCH", ["dags", _, "dagRuns", _, "taskInstances", _, _]) => dry_run,
        _ => false,
    }
}

/// An error response from the server. Airflow returns RFC 7807 problem JSON:
/// `{"detail", "status", "title", "type"}`.
#[derive(Debug)]
pub struct ApiError {
    pub http_status: u16,
    pub body: Value,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}", self.http_status)?;
        if let Some(t) = self.body.get("title").and_then(Value::as_str) {
            write!(f, " {t}")?;
        }
        match self.body.get("detail") {
            Some(Value::String(d)) if !d.is_empty() => write!(f, ": {d}")?,
            Some(d) if !d.is_null() => write!(f, ": {d}")?,
            _ => {}
        }
        if let Some(t) = self.body.as_str().map(str::trim).filter(|t| !t.is_empty()) {
            if t.starts_with('<') {
                write!(
                    f,
                    ": non-JSON (HTML) response; check base_url and that the REST API is enabled"
                )?;
            } else {
                let short: String = t.chars().take(500).collect();
                write!(f, ": {short}")?;
            }
        }
        if self.http_status == 401 {
            write!(
                f,
                "\nhint: check credentials, and that [api] auth_backends includes basic_auth"
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

impl Client {
    pub fn new(conf: &Resolved, timeout: Duration) -> Result<Self> {
        let http = Http::builder()
            .timeout(timeout)
            .user_agent(concat!("airflow-cli/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("could not build HTTP client")?;
        let api = reqwest::Url::parse(&api_root(&conf.base_url))
            .with_context(|| format!("invalid base_url '{}'", conf.base_url))?;
        let read_only = conf.read_only.then(|| match &conf.profile {
            Some(p) => format!("profile '{p}' is read-only"),
            None => "read-only mode is on".to_string(),
        });
        Ok(Client {
            http,
            api,
            auth: conf.auth.clone(),
            read_only,
        })
    }

    /// Build a URL from path segments (each one is percent-encoded).
    /// Segments must be non-empty; see [`check_segments`].
    pub fn url(&self, segments: &[&str]) -> reqwest::Url {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .expect("http(s) URL")
            .pop_if_empty()
            .extend(segments);
        url
    }

    fn request(&self, method: Method, url: reqwest::Url) -> RequestBuilder {
        let req = self.http.request(method, url);
        match &self.auth {
            Auth::None => req,
            Auth::Basic { username, password } => req.basic_auth(username, Some(password)),
            Auth::Token { token, prefix } => {
                let value = if prefix.is_empty() {
                    token.clone()
                } else {
                    format!("{prefix} {token}")
                };
                match reqwest::header::HeaderValue::from_str(&value) {
                    Ok(mut v) => {
                        v.set_sensitive(true);
                        req.header(reqwest::header::AUTHORIZATION, v)
                    }
                    // Invalid header characters: let reqwest report it when sending.
                    Err(_) => req.header(reqwest::header::AUTHORIZATION, value),
                }
            }
        }
    }

    fn send(
        &self,
        method: Method,
        url: reqwest::Url,
        query: &Query,
        body: Option<&Value>,
        accept: &str,
    ) -> Result<Response> {
        if let Some(mode) = &self.read_only {
            let rel = url
                .path()
                .strip_prefix(self.api.path().trim_end_matches('/'))
                .unwrap_or(url.path());
            let segments: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
            if !is_read_only_request(&method, &segments, body) {
                return Err(ReadOnlyError {
                    mode: mode.clone(),
                    method,
                    path: url.path().to_string(),
                }
                .into());
            }
        }
        let mut req = self
            .request(method.clone(), url.clone())
            .header("Accept", accept);
        if !query.is_empty() {
            req = req.query(query);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req
            .send()
            .with_context(|| format!("{method} {url} failed"))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let text = resp.text().unwrap_or_default();
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Err(ApiError {
            http_status: status.as_u16(),
            body,
        }
        .into())
    }

    /// Send a request to `/api/v1/<segments>` and parse the JSON response (Null when empty).
    pub fn json(
        &self,
        method: Method,
        segments: &[&str],
        query: &Query,
        body: Option<&Value>,
    ) -> Result<Value> {
        check_segments(segments)?;
        self.json_url(method, self.url(segments), query, body)
    }

    pub fn json_url(
        &self,
        method: Method,
        url: reqwest::Url,
        query: &Query,
        body: Option<&Value>,
    ) -> Result<Value> {
        let text = self
            .send(method, url, query, body, "application/json")?
            .text()
            .context("could not read response body")?;
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    /// GET a plain-text resource (task logs, DAG source).
    pub fn text(&self, segments: &[&str], query: &Query) -> Result<String> {
        check_segments(segments)?;
        self.send(Method::GET, self.url(segments), query, None, "text/plain")?
            .text()
            .context("could not read response body")
    }

    pub fn get(&self, segments: &[&str], query: &Query) -> Result<Value> {
        self.json(Method::GET, segments, query, None)
    }

    /// GET a list endpoint. With `all`, follow `offset` until `total_entries` items
    /// have been collected and return one combined response.
    pub fn list(
        &self,
        segments: &[&str],
        key: &str,
        mut query: Query,
        page: &Page,
    ) -> Result<Value> {
        if let Some(o) = &page.order_by {
            query.push(("order_by".into(), o.clone()));
        }
        if !page.all {
            query.push(("limit".into(), page.limit.to_string()));
            query.push(("offset".into(), page.offset.to_string()));
            return self.get(segments, &query);
        }
        let mut items: Vec<Value> = Vec::new();
        let mut offset = page.offset;
        loop {
            let mut q = query.clone();
            q.push(("limit".into(), page.limit.to_string()));
            q.push(("offset".into(), offset.to_string()));
            let resp = self.get(segments, &q)?;
            let batch = resp
                .get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let total = resp.get("total_entries").and_then(Value::as_u64);
            let n = batch.len() as u64;
            items.extend(batch);
            offset += n;
            let done = n == 0 || total.is_some_and(|t| offset >= t);
            if done {
                let total = total.unwrap_or(items.len() as u64);
                return Ok(serde_json::json!({ key: items, "total_entries": total }));
            }
        }
    }
}

/// Pagination options shared by list commands.
#[derive(clap::Args, Clone, Debug)]
pub struct Page {
    /// Maximum items to return (per page with --all).
    #[arg(long, default_value_t = 100)]
    pub limit: u64,
    /// Items to skip.
    #[arg(long, default_value_t = 0)]
    pub offset: u64,
    /// Fetch every page and combine them.
    #[arg(long)]
    pub all: bool,
    /// Sort field; prefix with - for descending (e.g. -start_date).
    #[arg(long, allow_hyphen_values = true)]
    pub order_by: Option<String>,
}

impl Page {
    pub fn everything() -> Page {
        Page {
            limit: 100,
            offset: 0,
            all: true,
            order_by: None,
        }
    }
}

/// An empty id (e.g. from an unset shell variable) would silently change which
/// endpoint is called, so refuse it.
fn check_segments(segments: &[&str]) -> Result<()> {
    if segments.iter().any(|s| s.trim().is_empty()) {
        anyhow::bail!("an id argument is empty (is a shell variable unset?)");
    }
    Ok(())
}

pub fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ApiError>()
        .is_some_and(|a| a.http_status == 404)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ok(m: Method, path: &str, body: Option<Value>) -> bool {
        let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        is_read_only_request(&m, &segs, body.as_ref())
    }

    #[test]
    fn classifies_requests() {
        assert!(ok(Method::GET, "/dags", None));
        assert!(ok(Method::POST, "/dags/~/dagRuns/list", Some(json!({}))));
        assert!(ok(Method::POST, "/connections/test", Some(json!({}))));
        assert!(ok(
            Method::POST,
            "/dags/d/clearTaskInstances",
            Some(json!({"dry_run": true}))
        ));
        assert!(ok(
            Method::POST,
            "/dags/d/dagRuns/r/clear",
            Some(json!({"dry_run": true}))
        ));
        assert!(ok(
            Method::PATCH,
            "/dags/d/dagRuns/r/taskInstances/t",
            Some(json!({"dry_run": true}))
        ));

        assert!(!ok(
            Method::POST,
            "/dags/d/clearTaskInstances",
            Some(json!({"dry_run": false}))
        ));
        assert!(!ok(Method::POST, "/dags/d/clearTaskInstances", None));
        assert!(!ok(
            Method::POST,
            "/dags/d/dagRuns",
            Some(json!({"dry_run": true}))
        ));
        assert!(!ok(
            Method::POST,
            "/variables",
            Some(json!({"dry_run": true}))
        ));
        assert!(!ok(Method::PATCH, "/dags/d", None));
        assert!(!ok(Method::DELETE, "/dags/d/dagRuns/r", None));
        assert!(!ok(Method::PUT, "/parseDagFile/x", None));
    }
}
