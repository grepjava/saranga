//! `${name}` and `${name:default}` substitution. Ported from
//! `XMLUtil.replaceSystemPropsInString`.
//!
//! Artemis resolves a name against Java system properties first and then
//! against environment variables. Rust has no system properties, so
//! [`replace_system_props_from_env`] (what `BrokerConfig::parse` uses)
//! consults environment variables only. A caller that wants a `-D`-style
//! override layer passes its own lookup to [`replace_system_props`].

/// Bound on the number of substitutions. Java loops forever when a value
/// expands to itself (directly or through a cycle); Saranga stops instead.
const MAX_SUBSTITUTIONS: usize = 10_000;

/// Replaces every `${...}` placeholder in `input`, exactly as Artemis does:
///
/// - The placeholder runs from `${` to the next `}`. A `${` with no `}` after
///   it stops processing, leaving that and any later placeholders as they are.
/// - The name and the default (after the first `:`) are trimmed.
/// - An unresolved name without a default becomes the empty string.
/// - Every occurrence of the same placeholder text is replaced at once, and
///   a replacement that itself contains `${` is expanded again.
///
/// Artemis applies this to the raw file text before XML parsing, so it also
/// reaches comments and attribute values.
pub fn replace_system_props(input: &str, lookup: impl Fn(&str) -> Option<String>) -> String {
    let mut text = input.to_owned();
    for _ in 0..MAX_SUBSTITUTIONS {
        let Some(start) = text.find("${") else {
            break;
        };
        let Some(close) = text[start..].find('}') else {
            break;
        };
        let placeholder = text[start..start + close + 1].to_owned();
        let inner = crate::java_trim(&placeholder[2..placeholder.len() - 1]);
        let (name, default) = match inner.split_once(':') {
            Some((name, default)) => (crate::java_trim(name), crate::java_trim(default)),
            None => (inner, ""),
        };
        let value = lookup(name).unwrap_or_else(|| default.to_owned());
        text = text.replace(&placeholder, &value);
        if value.contains(&placeholder) {
            break;
        }
    }
    text
}

/// [`replace_system_props`] with environment variables as the only source.
pub fn replace_system_props_from_env(input: &str) -> String {
    replace_system_props(input, env_lookup)
}

pub(crate) fn env_lookup(name: &str) -> Option<String> {
    if name.is_empty() || name.contains(['=', '\0']) {
        return None;
    }
    std::env::var(name).ok()
}
