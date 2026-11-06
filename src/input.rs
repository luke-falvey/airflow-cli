//! Reading notebooks, paragraph text and JSON arguments from files, stdin or inline.

use std::io::Read;

use anyhow::{Context, Result};
use serde_json::Value;

/// Read a whole file, or stdin when `source` is `-`.
pub fn read_source(source: &str) -> Result<String> {
    if source == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("could not read stdin")?;
        Ok(buf)
    } else {
        std::fs::read_to_string(source).with_context(|| format!("could not read {source}"))
    }
}

/// Parse a JSON argument, curl-style: `@path` reads a file, `-` reads stdin,
/// anything else is parsed as inline JSON.
pub fn json_arg(arg: &str) -> Result<Value> {
    let (text, origin) = if arg == "-" {
        (read_source("-")?, "stdin".to_string())
    } else if let Some(path) = arg.strip_prefix('@') {
        (read_source(path)?, path.to_string())
    } else {
        (arg.to_string(), "argument".to_string())
    };
    serde_json::from_str(&text).with_context(|| format!("invalid JSON in {origin}"))
}

/// Parse a JSON document from a file or stdin (`-`).
pub fn json_source(source: &str) -> Result<Value> {
    let text = read_source(source)?;
    serde_json::from_str(&text).with_context(|| {
        format!(
            "invalid JSON in {}",
            if source == "-" { "stdin" } else { source }
        )
    })
}
