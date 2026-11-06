//! Loading `~/.airflow.toml` and resolving which profile/credentials to use.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

/// The on-disk config file.
///
/// ```toml
/// default_profile = "dev"
/// username = "alice"        # default for every profile
/// password = "secret"
///
/// [profiles.dev]
/// base_url = "http://localhost:8080"
///
/// [profiles.prod]
/// base_url = "https://airflow.example.com"
/// username = "svc-airflow"   # overrides the top-level default
/// password = "other-secret"
///
/// [profiles.managed]
/// base_url = "https://managed.example.com"
/// token = "eyJ..."           # bearer token instead of username/password
/// read_only = true           # refuse anything that changes server state
/// ```
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub default_profile: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Default for profiles that don't set `read_only` themselves.
    pub read_only: Option<bool>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub base_url: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Bearer token. When set, it is sent instead of username/password.
    pub token: Option<String>,
    /// Refuse requests that change server state. Overrides the top-level value.
    pub read_only: Option<bool>,
}

/// Values given on the command line or through environment variables.
#[derive(Debug, Default)]
pub struct Overrides {
    pub profile: Option<String>,
    pub url: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    /// `--read-only`: forces read-only on. There is deliberately no flag to
    /// turn it off; that has to be done in the config file.
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Auth {
    None,
    Basic { username: String, password: String },
    Bearer(String),
}

/// The final connection settings.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub profile: Option<String>,
    pub base_url: String,
    pub auth: Auth,
    pub read_only: bool,
}

pub fn default_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("could not determine home directory"))?;
    Ok(home.join(".airflow.toml"))
}

/// Load the config file. A missing file is not an error (returns an empty config),
/// so the CLI can still be used with `--url` alone.
pub fn load(path: &Path) -> Result<FileConfig> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            toml::from_str(&text).with_context(|| format!("invalid config file {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(FileConfig::default()),
        Err(e) => Err(e).with_context(|| format!("could not read {}", path.display())),
    }
}

impl FileConfig {
    /// Pick the profile name: explicit choice, then `default_profile`,
    /// then a profile literally named "default", then the only profile if there is one.
    pub fn profile_name(&self, explicit: Option<&str>) -> Result<Option<String>> {
        if let Some(name) = explicit {
            if !self.profiles.contains_key(name) {
                let known: Vec<_> = self.profiles.keys().cloned().collect();
                bail!(
                    "profile '{name}' not found in config (known profiles: {})",
                    if known.is_empty() {
                        "none".to_string()
                    } else {
                        known.join(", ")
                    }
                );
            }
            return Ok(Some(name.to_string()));
        }
        if let Some(name) = &self.default_profile {
            if !self.profiles.contains_key(name) {
                bail!("default_profile '{name}' is not defined under [profiles]");
            }
            return Ok(Some(name.clone()));
        }
        if self.profiles.contains_key("default") {
            return Ok(Some("default".to_string()));
        }
        if self.profiles.len() == 1 {
            return Ok(self.profiles.keys().next().cloned());
        }
        Ok(None)
    }

    pub fn resolve(&self, ov: &Overrides) -> Result<Resolved> {
        let name = self.profile_name(ov.profile.as_deref())?;
        let profile = name.as_ref().and_then(|n| self.profiles.get(n));

        let base_url = ov
            .url
            .clone()
            .or_else(|| profile.and_then(|p| p.base_url.clone()))
            .ok_or_else(|| match &name {
                Some(n) => anyhow!("profile '{n}' has no base_url"),
                None if self.profiles.len() > 1 => anyhow!(
                    "several profiles are defined; choose one with --profile, AIRFLOW_CLI_PROFILE or default_profile"
                ),
                None => anyhow!(
                    "no Airflow server configured: add a profile to ~/.airflow.toml or pass --url"
                ),
            })?;

        let token = ov
            .token
            .clone()
            .or_else(|| profile.and_then(|p| p.token.clone()));
        let username = ov
            .username
            .clone()
            .or_else(|| profile.and_then(|p| p.username.clone()))
            .or_else(|| self.username.clone());
        let password = ov
            .password
            .clone()
            .or_else(|| profile.and_then(|p| p.password.clone()))
            .or_else(|| self.password.clone());

        // An explicit --username beats a profile token; otherwise a token wins.
        let auth = match (token, username) {
            (Some(t), _) if ov.username.is_none() => Auth::Bearer(t),
            (_, Some(u)) => Auth::Basic {
                username: u,
                password: password.unwrap_or_default(),
            },
            (Some(t), None) => Auth::Bearer(t),
            (None, None) => Auth::None,
        };

        let read_only = ov.read_only
            || profile
                .and_then(|p| p.read_only)
                .or(self.read_only)
                .unwrap_or(false);

        Ok(Resolved {
            profile: name,
            base_url,
            auth,
            read_only,
        })
    }
}

/// Normalise a base URL into the `/api/v1` root. Accepts `http://host:8080`,
/// `http://host:8080/`, `http://host/airflow` or `http://host:8080/api/v1`.
pub fn api_root(base_url: &str) -> String {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/api/v1") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/api/v1")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(text: &str) -> FileConfig {
        toml::from_str(text).unwrap()
    }

    const SAMPLE: &str = r#"
        default_profile = "dev"
        username = "top"
        password = "toppw"

        [profiles.dev]
        base_url = "http://dev:8080"

        [profiles.prod]
        base_url = "https://prod/"
        username = "produser"
        password = "prodpw"

        [profiles.managed]
        base_url = "https://managed"
        token = "tok"
    "#;

    fn basic(u: &str, p: &str) -> Auth {
        Auth::Basic {
            username: u.into(),
            password: p.into(),
        }
    }

    #[test]
    fn top_level_credentials_are_defaults() {
        let r = cfg(SAMPLE).resolve(&Overrides::default()).unwrap();
        assert_eq!(r.profile.as_deref(), Some("dev"));
        assert_eq!(r.base_url, "http://dev:8080");
        assert_eq!(r.auth, basic("top", "toppw"));
    }

    #[test]
    fn profile_credentials_override_defaults() {
        let ov = Overrides {
            profile: Some("prod".into()),
            ..Default::default()
        };
        assert_eq!(
            cfg(SAMPLE).resolve(&ov).unwrap().auth,
            basic("produser", "prodpw")
        );
    }

    #[test]
    fn token_profile_uses_bearer() {
        let ov = Overrides {
            profile: Some("managed".into()),
            ..Default::default()
        };
        assert_eq!(
            cfg(SAMPLE).resolve(&ov).unwrap().auth,
            Auth::Bearer("tok".into())
        );
        let ov = Overrides {
            profile: Some("managed".into()),
            username: Some("me".into()),
            ..Default::default()
        };
        assert_eq!(cfg(SAMPLE).resolve(&ov).unwrap().auth, basic("me", "toppw"));
    }

    #[test]
    fn cli_overrides_win() {
        let ov = Overrides {
            profile: Some("prod".into()),
            url: Some("http://other".into()),
            username: Some("cli".into()),
            ..Default::default()
        };
        let r = cfg(SAMPLE).resolve(&ov).unwrap();
        assert_eq!(r.base_url, "http://other");
        assert_eq!(r.auth, basic("cli", "prodpw"));
    }

    #[test]
    fn unknown_profile_errors() {
        let ov = Overrides {
            profile: Some("nope".into()),
            ..Default::default()
        };
        let err = cfg(SAMPLE).resolve(&ov).unwrap_err().to_string();
        assert!(
            err.contains("nope") && err.contains("dev, managed, prod"),
            "{err}"
        );
    }

    #[test]
    fn single_profile_is_implicit_and_anonymous() {
        let r = cfg("[profiles.only]\nbase_url = \"http://x\"")
            .resolve(&Overrides::default())
            .unwrap();
        assert_eq!(r.base_url, "http://x");
        assert_eq!(r.auth, Auth::None);
    }

    #[test]
    fn ambiguous_profiles_error() {
        let c = cfg("[profiles.a]\nbase_url = \"http://a\"\n[profiles.b]\nbase_url = \"http://b\"");
        assert!(c.resolve(&Overrides::default()).is_err());
    }

    #[test]
    fn read_only_profile_overrides_top_level() {
        let c = cfg(r#"
            read_only = true
            [profiles.prod]
            base_url = "http://prod"
            [profiles.dev]
            base_url = "http://dev"
            read_only = false
        "#);
        let pick = |p: &str, force: bool| {
            c.resolve(&Overrides {
                profile: Some(p.into()),
                read_only: force,
                ..Default::default()
            })
            .unwrap()
            .read_only
        };
        assert!(pick("prod", false), "inherits top-level read_only");
        assert!(!pick("dev", false), "profile can opt out");
        assert!(pick("dev", true), "--read-only forces it on");
        assert!(
            !cfg(SAMPLE)
                .resolve(&Overrides::default())
                .unwrap()
                .read_only,
            "off by default"
        );
    }

    #[test]
    fn unknown_keys_rejected() {
        assert!(toml::from_str::<FileConfig>("[profiles.a]\nbaseurl = \"x\"").is_err());
    }

    #[test]
    fn api_root_normalises() {
        assert_eq!(api_root("http://h:8080"), "http://h:8080/api/v1");
        assert_eq!(api_root("http://h:8080/"), "http://h:8080/api/v1");
        assert_eq!(api_root("http://h:8080/api/v1/"), "http://h:8080/api/v1");
        assert_eq!(api_root("https://h/airflow"), "https://h/airflow/api/v1");
    }
}
