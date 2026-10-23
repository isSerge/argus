//! POSIX-style environment variable interpolation for configuration values.
//!
//! Supports `${VAR}`, `${VAR:-default}` (default when unset or empty) and
//! `${VAR-default}` (default when unset only), plus `$$` escaping — the same
//! syntax and semantics as Docker Compose. Interpolation applies to every
//! string value in the configuration tree, recursively, after all
//! configuration sources are merged.

use config::ConfigError;
use serde_json::Value;

/// Recursively expands environment variable references in all string values
/// of a configuration tree.
pub fn expand_env_vars(value: Value) -> Result<Value, ConfigError> {
    expand_value(value, "").map_err(ConfigError::Message)
}

fn expand_value(value: Value, path: &str) -> Result<Value, String> {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (key, val) in map {
                let child_path = format!("{path}.{key}");
                out.insert(key, expand_value(val, &child_path)?);
            }
            Ok(Value::Object(out))
        }
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (index, item) in items.into_iter().enumerate() {
                out.push(expand_value(item, &format!("{path}[{index}]"))?);
            }
            Ok(Value::Array(out))
        }
        Value::String(s) => Ok(Value::String(expand_string(&s, path)?)),
        other => Ok(other),
    }
}

/// A parsed `${...}` reference: variable name plus optional default with its
/// applicability (empty-vs-unset).
struct Reference<'a> {
    name: &'a str,
    default: Option<(&'a str, DefaultWhen)>,
}

enum DefaultWhen {
    /// `${VAR:-default}` — default when unset or empty.
    UnsetOrEmpty,
    /// `${VAR-default}` — default when unset only.
    Unset,
}

pub(crate) fn expand_string(input: &str, path: &str) -> Result<String, String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        let after = &rest[dollar + 1..];
        if let Some(body) = after.strip_prefix('{') {
            let end = body.find('}').ok_or_else(|| {
                format!("unterminated environment variable reference at '{path}': '{input}'")
            })?;
            let reference = parse_reference(&body[..end], path)?;
            out.push_str(&resolve(&reference, path)?);
            rest = &body[end + 1..];
        } else if let Some(literal) = after.strip_prefix('$') {
            out.push('$');
            rest = literal;
        } else {
            out.push('$');
            rest = after;
        }
    }
    out.push_str(rest);
    Ok(out)
}

fn parse_reference<'a>(body: &'a str, path: &str) -> Result<Reference<'a>, String> {
    let name_end =
        body.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(body.len());
    let name = &body[..name_end];
    let valid_name =
        !name.is_empty() && body[..1].chars().all(|c| c.is_ascii_alphabetic() || c == '_');
    if !valid_name {
        return Err(format!(
            "invalid environment variable reference '{{{body}}}' at '{path}': variable names must match [A-Za-z_][A-Za-z0-9_]*"
        ));
    }
    let remainder = &body[name_end..];
    let default = match remainder.strip_prefix(":-") {
        Some(default) => Some((default, DefaultWhen::UnsetOrEmpty)),
        None => match remainder.strip_prefix('-') {
            Some(default) => Some((default, DefaultWhen::Unset)),
            None if remainder.is_empty() => None,
            None => {
                return Err(format!(
                    "invalid environment variable reference '{{{body}}}' at '{path}': expected ':-default', '-default' or '}}'"
                ));
            }
        },
    };
    Ok(Reference { name, default })
}

fn resolve(reference: &Reference<'_>, path: &str) -> Result<String, String> {
    match std::env::var(reference.name) {
        Ok(value) => match reference.default {
            Some((default, DefaultWhen::UnsetOrEmpty)) if value.is_empty() => {
                Ok(default.to_string())
            }
            _ => Ok(value),
        },
        Err(_) => match reference.default {
            Some((default, _)) => Ok(default.to_string()),
            None => Err(format!(
                "environment variable '{}' is not set and no default is provided (at '{}')",
                reference.name, path
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EnvVarGuard(&'static str);
    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            unsafe { std::env::set_var(name, value) };
            Self(name)
        }
    }
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe { std::env::remove_var(self.0) };
        }
    }

    #[test]
    fn plain_values_are_untouched() {
        assert_eq!(expand_string("http://localhost:8545", "p").unwrap(), "http://localhost:8545");
    }

    #[test]
    fn substitutes_set_variable() {
        let _g = EnvVarGuard::set("ARGUS_TEST_VAR", "value");
        assert_eq!(expand_string("${ARGUS_TEST_VAR}", "p").unwrap(), "value");
        assert_eq!(expand_string("pre-${ARGUS_TEST_VAR}-post", "p").unwrap(), "pre-value-post");
        assert_eq!(expand_string("${ARGUS_TEST_VAR}${ARGUS_TEST_VAR}", "p").unwrap(), "valuevalue");
    }

    #[test]
    fn unset_variable_without_default_is_an_error() {
        let err = expand_string("${ARGUS_TEST_UNSET}", "rpc_urls[0]").unwrap_err();
        assert!(err.contains("ARGUS_TEST_UNSET"), "unexpected error: {err}");
    }

    #[test]
    fn default_applies_when_unset_or_empty() {
        unsafe { std::env::remove_var("ARGUS_TEST_UNSET") };
        assert_eq!(expand_string("${ARGUS_TEST_UNSET:-fallback}", "p").unwrap(), "fallback");
        let _g = EnvVarGuard::set("ARGUS_TEST_EMPTY", "");
        assert_eq!(expand_string("${ARGUS_TEST_EMPTY:-fallback}", "p").unwrap(), "fallback");
    }

    #[test]
    fn hyphen_default_applies_when_unset_only() {
        unsafe { std::env::remove_var("ARGUS_TEST_UNSET") };
        assert_eq!(expand_string("${ARGUS_TEST_UNSET-fallback}", "p").unwrap(), "fallback");
        let _g = EnvVarGuard::set("ARGUS_TEST_EMPTY", "");
        assert_eq!(expand_string("${ARGUS_TEST_EMPTY-fallback}", "p").unwrap(), "");
    }

    #[test]
    fn set_variable_wins_over_default() {
        let _g = EnvVarGuard::set("ARGUS_TEST_VAR", "value");
        assert_eq!(expand_string("${ARGUS_TEST_VAR:-fallback}", "p").unwrap(), "value");
        assert_eq!(expand_string("${ARGUS_TEST_VAR-fallback}", "p").unwrap(), "value");
    }

    #[test]
    fn empty_default_is_allowed() {
        unsafe { std::env::remove_var("ARGUS_TEST_UNSET") };
        assert_eq!(expand_string("${ARGUS_TEST_UNSET:-}", "p").unwrap(), "");
    }

    #[test]
    fn dollar_is_escaped_with_double_dollar() {
        assert_eq!(expand_string("a$$b", "p").unwrap(), "a$b");
        assert_eq!(expand_string("$$", "p").unwrap(), "$");
    }

    #[test]
    fn lone_dollar_is_literal() {
        assert_eq!(expand_string("a$b", "p").unwrap(), "a$b");
        assert_eq!(expand_string("$", "p").unwrap(), "$");
    }

    #[test]
    fn malformed_references_are_errors() {
        assert!(expand_string("${UNCLOSED", "p").is_err());
        assert!(expand_string("${1BAD}", "p").is_err());
        assert!(expand_string("${VAR:bad}", "p").is_err());
    }

    #[test]
    fn expands_recursively_with_paths_in_errors() {
        let _g = EnvVarGuard::set("ARGUS_TEST_NESTED", "expanded");
        let tree: Value = serde_json::json!({
            "rpc_urls": ["${ARGUS_TEST_NESTED}", "${ARGUS_TEST_MISSING}"],
            "server": { "listen_address": "${ARGUS_TEST_NESTED}:8080" }
        });
        let err = expand_env_vars(tree).unwrap_err();
        assert!(err.to_string().contains("ARGUS_TEST_MISSING"), "unexpected: {err}");
        assert!(err.to_string().contains("rpc_urls[1]"), "unexpected: {err}");
    }
}
