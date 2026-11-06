//! Printing responses.

use serde_json::Value;

#[derive(Clone, Copy)]
pub struct Output {
    /// Single-line JSON.
    pub compact: bool,
}

impl Output {
    /// Print a response. Null (an empty 204 response) prints nothing;
    /// a bare string prints without quotes.
    pub fn json(&self, v: &Value) {
        match v {
            Value::Null => {}
            Value::String(s) => println!("{s}"),
            _ => {
                let s = if self.compact {
                    serde_json::to_string(v)
                } else {
                    serde_json::to_string_pretty(v)
                };
                println!("{}", s.expect("serialisable JSON"));
            }
        }
    }

    pub fn text(&self, s: &str) {
        print!("{s}");
        if !s.ends_with('\n') {
            println!();
        }
    }
}
