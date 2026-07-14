//! Pushover notification library.
//!
//! Sends a Pushover notification with a single function call.  Credentials are
//! read from a `.env` file in the **current working directory** of the calling
//! process, or from environment variables (which take precedence).
//!
//! ## .env keys
//!
//! - `PUSHOVER_TOKEN` — Application API token (create at <https://pushover.net/apps/build>)
//! - `PUSHOVER_USER`  — Your user key (find at <https://pushover.net>)

use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::path::PathBuf;
use std::sync::OnceLock;

const API_URL: &str = "https://api.pushover.net/1/messages.json";

static DOTENV_LOADED: OnceLock<()> = OnceLock::new();

/// Parse `$CWD/.env` and load key-value pairs into the process environment
/// (does **not** overwrite already-set environment variables).
fn load_dotenv() {
    DOTENV_LOADED.get_or_init(|| {
        let env_path = match env::current_dir() {
            Ok(cwd) => cwd.join(".env"),
            Err(_) => return,
        };
        let text = match std::fs::read_to_string(&env_path) {
            Ok(t) => t,
            Err(_) => return,
        };
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, val)) = line.split_once('=') else {
                continue;
            };
            let key = key.trim();
            let val = val.trim().trim_matches('"').trim_matches('\'');
            if !key.is_empty() && env::var(key).is_err() {
                // SAFETY: key and val are valid UTF-8 (we read from a String)
                unsafe {
                    env::set_var(key, val);
                }
            }
        }
    });
}

/// Error returned by [`push_notification`].
#[derive(Debug)]
pub enum PushoverError {
    /// PUSHOVER_TOKEN is missing from both environment and .env.
    MissingToken(PathBuf),
    /// PUSHOVER_USER is missing from both environment and .env.
    MissingUser(PathBuf),
    /// HTTP-level failure (network, TLS, timeout).
    Http(Box<ureq::Error>),
    /// API returned a non-1 status.
    Api { errors: Vec<String> },
}

impl std::fmt::Display for PushoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushoverError::MissingToken(env_path) => write!(
                f,
                "PUSHOVER_TOKEN not set. Add it to {} or set the environment variable.",
                env_path.display()
            ),
            PushoverError::MissingUser(env_path) => write!(
                f,
                "PUSHOVER_USER not set. Add it to {} or set the environment variable.",
                env_path.display()
            ),
            PushoverError::Http(e) => write!(f, "Pushover HTTP error: {e}"),
            PushoverError::Api { errors } => {
                write!(f, "Pushover API error: {errors:?}")
            }
        }
    }
}

impl std::error::Error for PushoverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PushoverError::Http(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

/// Send a Pushover notification.
///
/// Reads `PUSHOVER_TOKEN` and `PUSHOVER_USER` from the environment (or the
/// `.env` file in the current working directory, loaded automatically on the
/// first call).
///
/// Returns the `request` identifier string on success.
pub fn push_notification(message: &str) -> Result<String, PushoverError> {
    push_notification_with(message, &PushoverOptions::default())
}

/// Optional parameters for [`push_notification_with`].
#[derive(Default)]
pub struct PushoverOptions<'a> {
    pub title: &'a str,
    pub priority: i32,
    pub device: &'a str,
    pub sound: &'a str,
    pub url: &'a str,
    pub url_title: &'a str,
}

/// Send a Pushover notification with the given options.
///
/// See [`push_notification`] for details. Use [`PushoverOptions`] to set
/// optional fields.
pub fn push_notification_with(
    message: &str,
    options: &PushoverOptions<'_>,
) -> Result<String, PushoverError> {
    load_dotenv();

    let env_path = env::current_dir()
        .map(|cwd| cwd.join(".env"))
        .unwrap_or_else(|_| PathBuf::from(".env"));

    let token = env::var("PUSHOVER_TOKEN").unwrap_or_default();
    let user = env::var("PUSHOVER_USER").unwrap_or_default();

    if token.is_empty() {
        return Err(PushoverError::MissingToken(env_path));
    }
    if user.is_empty() {
        return Err(PushoverError::MissingUser(env_path));
    }

    let mut params: Vec<(&str, &str)> =
        vec![("token", &token), ("user", &user), ("message", message)];

    // Helper to push optional string params
    let mut push_if = |key: &'static str, val: &str| {
        if !val.is_empty() {
            params.push((key, val));
        }
    };
    push_if("title", options.title);
    push_if("device", options.device);
    push_if("sound", options.sound);
    push_if("url", options.url);
    push_if("url_title", options.url_title);

    let priority_str;
    if options.priority != 0 {
        priority_str = options.priority.to_string();
        params.push(("priority", &priority_str));
    }

    let body = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish();

    let resp = ureq::post(API_URL)
        .set("Content-Type", "application/x-www-form-urlencoded")
        .timeout(std::time::Duration::from_secs(15))
        .send_string(&body)
        .map_err(|e| PushoverError::Http(Box::new(e)))?;

    let raw: serde_json::Value = resp
        .into_json()
        .map_err(|e| PushoverError::Http(Box::new(e.into())))?;

    let status = raw.get("status").and_then(|v| v.as_i64()).unwrap_or(0);
    if status != 1 {
        let errors: Vec<String> = raw
            .get("errors")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        return Err(PushoverError::Api { errors });
    }

    Ok(raw
        .get("request")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string())
}
