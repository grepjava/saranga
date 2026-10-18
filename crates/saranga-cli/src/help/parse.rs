//! Artemis's command line parsed as picocli 4.7 parses it
//! (`CommandLine.parseArgs`), from the model of [artemis-cli.json]: the
//! command each word names, the options and their values converted to
//! their Java types, the positional parameters, and on a mistake the
//! `ParameterException` picocli throws, whose message `Artemis.execute`
//! prints. `tests/golden/cli/parse.txt` (from Artemis's own command line)
//! holds what picocli makes of thousands of command lines; `parse` must
//! make the same of each.
//!
//! The arguments are taken in order. `--` ends the options. A word naming a
//! subcommand of the current command enters it. An option is its exact
//! name, or `name=value`; a flag takes no value, but `=true` or `=false`;
//! any other option takes the next word, unless that is `--` or another of
//! the command's options. The rest are positional parameters while the
//! command takes them, and unmatched otherwise, as are words that resemble
//! options. Converting a value, a missing value and an option given twice
//! fail at once; after the last word, a required option or parameter left
//! out fails, and then the unmatched words.

use super::{Arg, Java, Spec, artemis};

/// A converted value, written as Java's `toString` writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Boolean(bool),
    Int(i32),
    Long(i64),
    /// A string, a file or an enum constant.
    Text(String),
    List(Vec<Value>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Long(l) => write!(f, "{l}"),
            Value::Text(s) => f.write_str(s),
            Value::List(items) => {
                f.write_str("[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str("]")
            }
        }
    }
}

/// What a command line names: its commands from the root, and the options
/// and positional parameters given to the last, each once, in the order
/// first given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
    pub path: Vec<String>,
    pub options: Vec<Given>,
    /// Each positional parameter by its index (`0`, `0..*`).
    pub positionals: Vec<(String, Value)>,
}

/// An option given, and its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Given {
    /// Its longest name, as picocli names it.
    pub name: String,
    pub names: Vec<String>,
    /// A `Boolean`, unset unless given.
    pub boxed: bool,
    pub value: Value,
}

impl Parsed {
    /// Whether the command named is a group (`queue`, `data`...), whose
    /// own action prints its help (`QueueGroup.run`).
    pub fn is_group(&self) -> bool {
        let mut spec = saranga();
        for name in &self.path {
            match spec.commands.iter().find(|c| &c.name == name) {
                Some(c) => spec = c,
                None => return false,
            }
        }
        !self.path.is_empty() && !spec.commands.is_empty()
    }

    /// The command line as saranga's clap command line reads it, with the
    /// program's name: the commands, each option as `--name=value` (by the
    /// name `command` gives it), a flag as `--name` when true and not at
    /// all when false, unless it is a `Boolean` (`--name=false`), a short
    /// option with its value attached, then `--` and the positional
    /// parameters. The values are converted already: a number in decimal,
    /// a protocol in upper case.
    pub fn clap_args(&self, program: &str, command: &clap::Command) -> Vec<String> {
        let mut out = vec![program.to_owned()];
        let mut command = command;
        for name in &self.path {
            out.push(name.clone());
            command = command
                .find_subcommand(name)
                .expect("saranga has every command of the model");
        }
        for given in &self.options {
            let arg = command
                .get_arguments()
                .find(|a| {
                    given.names.iter().any(|n| {
                        let bare = n.trim_start_matches('-');
                        a.get_long() == Some(bare)
                            || (n.len() == 2
                                && a.get_short().map(String::from).as_deref() == Some(bare))
                    })
                })
                .expect("saranga takes every option of the model");
            let flag = |value: Option<String>| match (arg.get_long(), value) {
                (Some(long), None) => format!("--{long}"),
                (Some(long), Some(v)) => format!("--{long}={v}"),
                (None, Some(v)) => format!("-{}{v}", arg.get_short().unwrap_or('?')),
                (None, None) => format!("-{}", arg.get_short().unwrap_or('?')),
            };
            match &given.value {
                Value::Boolean(b) if given.boxed => out.push(flag(Some(b.to_string()))),
                Value::Boolean(true) => out.push(flag(None)),
                Value::Boolean(false) => {}
                Value::List(items) => out.extend(items.iter().map(|v| flag(Some(v.to_string())))),
                v => out.push(flag(Some(v.to_string()))),
            }
        }
        if !self.positionals.is_empty() {
            out.push("--".to_owned());
            for (_, value) in &self.positionals {
                match value {
                    Value::List(items) => out.extend(items.iter().map(ToString::to_string)),
                    v => out.push(v.to_string()),
                }
            }
        }
        out
    }
}

/// A `ParameterException`: its message, the command whose parsing threw
/// it (`getCommandLine`), and for an `UnmatchedArgumentException` the
/// words left unmatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    /// The command's path from the root (empty: the root).
    pub path: Vec<String>,
    pub unmatched: Vec<String>,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseError {}

impl ParseError {
    /// What `UnmatchedArgumentException.printSuggestions` prints, which the
    /// shell prints in place of the command's usage: for an unknown option,
    /// the visible option names starting as it does (`Possible solutions:
    /// --user, --url`), otherwise up to three of the command's visible
    /// subcommands most like the word (`Did you mean: queue stat?`); none
    /// when nothing is like it. Below the root only, as the root's command
    /// names depend on where `buildCommand` was run.
    pub fn suggestions(&self) -> Option<String> {
        let word = self.unmatched.first()?;
        if self.path.is_empty() {
            return None;
        }
        let mut spec = saranga();
        for name in &self.path {
            spec = spec.commands.iter().find(|c| &c.name == name)?;
        }
        if resembles_option(spec, word) {
            let stripped = strip_prefix(word);
            let prefix: String = stripped.chars().take(2).collect();
            let names: Vec<&str> = spec
                .options
                .iter()
                .filter(|o| !o.hidden)
                .flat_map(|o| o.names.iter())
                .filter(|n| strip_prefix(n).starts_with(&prefix))
                .map(String::as_str)
                .collect();
            return (!names.is_empty())
                .then(|| format!("Possible solutions: {}", names.join(", ")));
        }
        let visible: Vec<&str> = spec
            .commands
            .iter()
            .filter(|c| !c.hidden)
            .map(|c| c.name.as_str())
            .collect();
        let similar = most_similar(word, &visible);
        if similar.is_empty() {
            return None;
        }
        let name = self.path.last().map_or("", String::as_str);
        let named: Vec<String> = similar
            .iter()
            .take(3)
            .map(|s| format!("{name} {s}"))
            .collect();
        Some(format!("Did you mean: {}?", named.join(" or ")))
    }
}

/// `CommandSpec.stripPrefix`: the word from its first character that may
/// be part of a Java identifier.
fn strip_prefix(word: &str) -> &str {
    match word.char_indices().find(|(_, c)| java_identifier_part(*c)) {
        Some((at, _)) => &word[at..],
        None => word,
    }
}

/// `Character.isJavaIdentifierPart`, for the characters of a command line.
fn java_identifier_part(c: char) -> bool {
    c.is_alphanumeric()
        || c == '_'
        || c == '$'
        || ('\u{0}'..='\u{8}').contains(&c)
        || ('\u{e}'..='\u{1b}').contains(&c)
        || ('\u{7f}'..='\u{9f}').contains(&c)
        || matches!(
            unicode_category(c),
            Some("Pc" | "Sc" | "Mn" | "Mc" | "Nl" | "Cf")
        )
}

/// The few general categories `isJavaIdentifierPart` adds beyond letters
/// and digits, for the characters likely on a command line.
fn unicode_category(c: char) -> Option<&'static str> {
    match c {
        '\u{a2}'..='\u{a5}' | '\u{20a0}'..='\u{20c0}' => Some("Sc"),
        '\u{203f}'
        | '\u{2040}'
        | '\u{2054}'
        | '\u{fe33}'
        | '\u{fe34}'
        | '\u{fe4d}'..='\u{fe4f}'
        | '\u{ff3f}' => Some("Pc"),
        '\u{300}'..='\u{36f}' => Some("Mn"),
        '\u{ad}'
        | '\u{200b}'..='\u{200f}'
        | '\u{202a}'..='\u{202e}'
        | '\u{2060}'..='\u{2064}'
        | '\u{feff}' => Some("Cf"),
        _ => None,
    }
}

/// `CosineSimilarity.mostSimilar`: the candidates with a bigram cosine
/// similarity to the pattern above zero, ignoring case, most similar
/// first; of candidates as similar, the last one given.
fn most_similar<'c>(pattern: &str, candidates: &[&'c str]) -> Vec<&'c str> {
    let pattern = pattern.to_lowercase();
    // A TreeMap from the score: a later candidate replaces an equal one.
    let mut sorted: Vec<(f64, &str)> = Vec::new();
    for candidate in candidates {
        let score = similarity(&pattern, &candidate.to_lowercase());
        if score > 0.0 {
            match sorted.iter_mut().find(|(s, _)| *s == score) {
                Some(slot) => slot.1 = candidate,
                None => sorted.push((score, candidate)),
            }
        }
    }
    sorted.sort_by(|a, b| b.0.total_cmp(&a.0));
    sorted.into_iter().map(|(_, c)| c).collect()
}

/// The cosine similarity of the two words' bigram frequencies (NaN when
/// either has none).
fn similarity(a: &str, b: &str) -> f64 {
    let grams = |s: &str| {
        let chars: Vec<u16> = s.encode_utf16().collect();
        let mut m: std::collections::HashMap<[u16; 2], u32> = std::collections::HashMap::new();
        for w in chars.windows(2) {
            *m.entry([w[0], w[1]]).or_default() += 1;
        }
        m
    };
    let dot = |x: &std::collections::HashMap<[u16; 2], u32>,
               y: &std::collections::HashMap<[u16; 2], u32>| {
        x.iter()
            .map(|(k, v)| f64::from(*v) * f64::from(y.get(k).copied().unwrap_or(0)))
            .sum::<f64>()
    };
    let (ma, mb) = (grams(a), grams(b));
    dot(&ma, &mb) / (dot(&ma, &ma) * dot(&mb, &mb)).sqrt()
}

/// A mistake found within a command, before it is known as the
/// `ParameterException` of that command.
struct Mistake(String);

impl Mistake {
    /// The `ParameterException` of the command at `path`.
    fn at(self, path: &[String]) -> ParseError {
        ParseError {
            message: self.0,
            path: path.to_vec(),
            unmatched: Vec::new(),
        }
    }
}

/// The commands of the root: `buildCommand`'s for an instance and for the
/// home both, as saranga takes each of them in either; in the shell,
/// `connect` and `disconnect` and not `run` or `shell`.
fn root_command(word: &str, in_shell: bool) -> bool {
    match word {
        "connect" | "disconnect" => in_shell,
        "run" | "shell" => !in_shell,
        _ => true,
    }
}

/// Whether `word` names a command of the shell, which JLine's registry
/// runs (picocli's subcommands of the root, hidden ones too, and `help`);
/// it answers any other first word with `Unknown command`.
pub fn shell_command(word: &str) -> bool {
    saranga()
        .commands
        .iter()
        .any(|c| c.name == word && root_command(word, true))
}

/// The command line saranga parses: Artemis's, with saranga's own options
/// of `run` (a broker.xml or an etc directory to run, a listening address
/// and system properties) and of `mask` (the broker.xml of
/// `--password-codec`).
pub fn saranga() -> &'static Spec {
    static SPEC: std::sync::OnceLock<Spec> = std::sync::OnceLock::new();
    SPEC.get_or_init(|| {
        let mut root = artemis().clone();
        let own = |names: &[&str], label: &str, list: bool| Arg {
            names: names.iter().map(|n| (*n).to_owned()).collect(),
            label: label.to_owned(),
            arity: (1, Some(1)),
            index: (0, Some(0)),
            multi: list,
            hidden: false,
            required: false,
            description: Vec::new(),
            candidates: Vec::new(),
            completion: None,
            kind: None,
            java: Java::String,
            list,
        };
        for c in &mut root.commands {
            match c.name.as_str() {
                "run" => c.options.extend([
                    own(&["--config"], "<config>", false),
                    own(&["--etc"], "<etc>", false),
                    own(&["--bind"], "<bind>", false),
                    own(&["-D"], "<NAME=VALUE>", true),
                ]),
                "mask" => c.options.push(own(&["--config"], "<config>", false)),
                _ => {}
            }
        }
        root
    })
}

/// The command line `words`, outside the shell or in it. saranga takes
/// every command from an instance and from the home (COMPAT.md).
pub fn parse(words: &[String], in_shell: bool) -> Result<Parsed, ParseError> {
    Parser::new(in_shell).run(words)
}

struct Parser {
    in_shell: bool,
}

/// What the current command was given.
struct Matched<'a> {
    spec: &'a Spec,
    options: Vec<(&'a Arg, Value)>,
    positionals: Vec<(&'a Arg, Value)>,
    /// How many positional words the command has taken.
    taken: usize,
}

impl<'a> Matched<'a> {
    fn new(spec: &'a Spec) -> Self {
        Matched {
            spec,
            options: Vec::new(),
            positionals: Vec::new(),
            taken: 0,
        }
    }
}

impl Parser {
    fn new(in_shell: bool) -> Self {
        Parser { in_shell }
    }

    fn run(&self, words: &[String]) -> Result<Parsed, ParseError> {
        let root = saranga();
        let mut path = Vec::new();
        let mut current = Matched::new(root);
        let mut unmatched: Vec<(usize, &str)> = Vec::new();
        let mut end_of_options = false;
        let mut i = 0;
        while i < words.len() {
            let word = words[i].as_str();
            if !end_of_options {
                if word == "--" {
                    end_of_options = true;
                    i += 1;
                    continue;
                }
                if let Some(sub) = self.subcommand(&current, path.is_empty(), word) {
                    check_required(&current).map_err(|m| m.at(&path))?;
                    path.push(sub.name.clone());
                    current = Matched::new(sub);
                    i += 1;
                    continue;
                }
                if let Some((arg, attached)) = option(current.spec, word) {
                    i = self
                        .option_value(&mut current, arg, attached, words, i)
                        .map_err(|m| m.at(&path))?;
                    continue;
                }
                if resembles_option(current.spec, word) {
                    unmatched.push((i, word));
                    i += 1;
                    continue;
                }
            }
            if !positional(&mut current, word) {
                unmatched.push((i, word));
            }
            i += 1;
        }
        check_required(&current).map_err(|m| m.at(&path))?;
        if let Some(&(index, first)) = unmatched.first() {
            let words: Vec<String> = unmatched.iter().map(|(_, w)| format!("'{w}'")).collect();
            let plural = if unmatched.len() == 1 { "" } else { "s" };
            let message = if resembles_option(current.spec, first) {
                format!("Unknown option{plural}: {}", words.join(", "))
            } else {
                let at = if unmatched.len() == 1 { "at" } else { "from" };
                format!(
                    "Unmatched argument{plural} {at} index {index}: {}",
                    words.join(", ")
                )
            };
            return Err(ParseError {
                message,
                path,
                unmatched: unmatched.iter().map(|(_, w)| (*w).to_owned()).collect(),
            });
        }
        Ok(Parsed {
            path,
            options: current
                .options
                .into_iter()
                .map(|(a, v)| Given {
                    name: longest_name(a).to_owned(),
                    names: a.names.clone(),
                    boxed: matches!(a.java, Java::Boolean { boxed: true }),
                    value: v,
                })
                .collect(),
            positionals: current
                .positionals
                .into_iter()
                .map(|(a, v)| (index_text(a), v))
                .collect(),
        })
    }

    /// The subcommand `word` names, if any.
    fn subcommand<'a>(&self, current: &Matched<'a>, root: bool, word: &str) -> Option<&'a Spec> {
        current
            .spec
            .commands
            .iter()
            .find(|c| c.name == word && (!root || root_command(word, self.in_shell)))
    }

    /// Takes the option at `words[i]` and its value; the index of the word
    /// after them.
    fn option_value<'a>(
        &self,
        current: &mut Matched<'a>,
        arg: &'a Arg,
        attached: Option<&str>,
        words: &[String],
        i: usize,
    ) -> Result<usize, Mistake> {
        let name = longest_name(arg);
        let (value, next) = if arg.boolean() {
            let value = match attached {
                None => true,
                Some(v) if v.eq_ignore_ascii_case("true") => true,
                Some(v) if v.is_empty() || v.eq_ignore_ascii_case("false") => false,
                Some(v) => {
                    return Err(Mistake(format!(
                        "Invalid value for option '{name}': '{v}' is not a boolean"
                    )));
                }
            };
            (Value::Boolean(value), i + 1)
        } else {
            let (text, next) = match attached {
                Some(v) => (v, i + 1),
                None => match words.get(i + 1) {
                    None => {
                        return Err(Mistake(format!(
                            "Missing required parameter for option '{name}' ({})",
                            arg.label
                        )));
                    }
                    Some(w) if w == "--" || option(current.spec, w).is_some() => {
                        return Err(Mistake(format!(
                            "Expected parameter for option '{name}' but found '{w}'"
                        )));
                    }
                    Some(w) => (w.as_str(), i + 2),
                },
            };
            let value = convert(arg, text)
                .map_err(|e| Mistake(format!("Invalid value for option '{name}': {e}")))?;
            (value, next)
        };
        if let Some(slot) = current
            .options
            .iter_mut()
            .find(|(a, _)| std::ptr::eq(*a, arg))
        {
            match &mut slot.1 {
                Value::List(items) if arg.list => items.push(value),
                _ => {
                    return Err(Mistake(if arg.boolean() {
                        format!("option '{name}' should be specified only once")
                    } else {
                        format!(
                            "option '{name}' ({}) should be specified only once",
                            arg.label
                        )
                    }));
                }
            }
        } else if arg.list {
            current.options.push((arg, Value::List(vec![value])));
        } else {
            current.options.push((arg, value));
        }
        Ok(next)
    }
}

/// The option `word` names, exactly or as `name=value` (with the value),
/// or a short one (saranga's `-D`) with its value attached.
fn option<'a, 'w>(spec: &'a Spec, word: &'w str) -> Option<(&'a Arg, Option<&'w str>)> {
    if let Some(arg) = spec
        .options
        .iter()
        .find(|o| o.names.iter().any(|n| n == word))
    {
        return Some((arg, None));
    }
    if let Some((name, value)) = word.split_once('=')
        && let Some(arg) = spec
            .options
            .iter()
            .find(|o| o.names.iter().any(|n| n == name))
    {
        return Some((arg, Some(value)));
    }
    spec.options.iter().find_map(|o| {
        o.names
            .iter()
            .find(|n| {
                n.len() == 2
                    && n.starts_with('-')
                    && n != &"--"
                    && word.len() > 2
                    && word.starts_with(n.as_str())
            })
            .map(|n| (o, Some(&word[n.len()..])))
    })
}

/// Gives `word` to the command's positional parameter that takes it next;
/// false when none does.
fn positional<'a>(current: &mut Matched<'a>, word: &str) -> bool {
    let position = current.taken;
    let Some(arg) = current
        .spec
        .parameters
        .iter()
        .find(|p| p.index.0 <= position && p.index.1.is_none_or(|last| position <= last))
    else {
        return false;
    };
    current.taken += 1;
    let value = Value::Text(word.to_owned());
    if let Some(slot) = current
        .positionals
        .iter_mut()
        .find(|(a, _)| std::ptr::eq(*a, arg))
    {
        if let Value::List(items) = &mut slot.1 {
            items.push(value);
        }
    } else if arg.multi {
        current.positionals.push((arg, Value::List(vec![value])));
    } else {
        current.positionals.push((arg, value));
    }
    true
}

/// `MissingParameterException` for a required option or positional
/// parameter the command was not given.
fn check_required(current: &Matched<'_>) -> Result<(), Mistake> {
    let given = |a: &Arg, list: &[(&Arg, Value)]| list.iter().any(|(x, _)| std::ptr::eq(*x, a));
    let missing: Vec<String> = current
        .spec
        .options
        .iter()
        .filter(|o| o.required && !given(o, &current.options))
        .map(|o| format!("'{}={}'", longest_name(o), o.label))
        .collect();
    if !missing.is_empty() {
        let plural = if missing.len() == 1 { "" } else { "s" };
        return Err(Mistake(format!(
            "Missing required option{plural}: {}",
            missing.join(", ")
        )));
    }
    let missing: Vec<String> = current
        .spec
        .parameters
        .iter()
        .filter(|p| p.required && !given(p, &current.positionals))
        .map(|p| format!("'{}'", p.label))
        .collect();
    if !missing.is_empty() {
        let plural = if missing.len() == 1 { "" } else { "s" };
        return Err(Mistake(format!(
            "Missing required parameter{plural}: {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

/// The value converted as picocli converts it to the argument's type: the
/// converter's message when it cannot be.
fn convert(arg: &Arg, text: &str) -> Result<Value, String> {
    match &arg.java {
        Java::Boolean { .. } => match text.to_ascii_lowercase().as_str() {
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
            _ => Err(format!("'{text}' is not a boolean")),
        },
        Java::Int => java_integer(text)
            .and_then(|n| i32::try_from(n).ok())
            .map(Value::Int)
            .ok_or_else(|| format!("'{text}' is not an int")),
        Java::Long => java_integer(text)
            .and_then(|n| i64::try_from(n).ok())
            .map(Value::Long)
            .ok_or_else(|| format!("'{text}' is not a long")),
        Java::String | Java::File => Ok(Value::Text(text.to_owned())),
        Java::Enum(constants) => {
            if constants.iter().any(|c| c == text) {
                Ok(Value::Text(text.to_owned()))
            } else {
                Err(format!(
                    "expected one of [{}] (case-sensitive) but was '{text}'",
                    constants.join(", ")
                ))
            }
        }
        Java::UpperEnum { class, constants } => {
            let upper = text.to_uppercase();
            if constants.contains(&upper) {
                Ok(Value::Text(upper))
            } else {
                let simple = class.rsplit(['.', '$']).next().unwrap_or(class);
                Err(format!(
                    "cannot convert '{text}' to {simple} (java.lang.IllegalArgumentException: No enum constant {}.{upper})",
                    class.replace('$', ".")
                ))
            }
        }
    }
}

/// `Long.valueOf` and `Integer.valueOf`'s syntax: an optional sign and
/// decimal digits, with no spaces.
fn java_integer(text: &str) -> Option<i128> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || digits.len() > 30 {
        return None;
    }
    text.parse::<i128>().ok()
}

/// picocli's `resemblesOption`: not a number, and sharing its first
/// characters with the command's option names (most of them), or starting
/// with `-` when it has none.
fn resembles_option(spec: &Spec, word: &str) -> bool {
    if word.chars().count() <= 1 || java_decode(word) || java_double(word) {
        return false;
    }
    let names: Vec<&str> = spec
        .options
        .iter()
        .flat_map(|o| o.names.iter().map(String::as_str))
        .collect();
    if names.is_empty() {
        return word.starts_with('-');
    }
    let count: usize = names
        .iter()
        .map(|name| {
            word.chars()
                .zip(name.chars())
                .take_while(|(a, b)| a == b)
                .count()
        })
        .sum();
    count > 0 && count * 10 >= names.len() * 9
}

/// Whether `Long.decode` takes the word: a sign, then decimal digits, or
/// hexadecimal after `0x`, `0X` or `#`, or octal after `0`.
fn java_decode(word: &str) -> bool {
    let rest = word.strip_prefix(['+', '-']).unwrap_or(word);
    let (digits, radix) = if let Some(h) = rest
        .strip_prefix("0x")
        .or_else(|| rest.strip_prefix("0X"))
        .or_else(|| rest.strip_prefix('#'))
    {
        (h, 16)
    } else if rest.len() > 1 && rest.starts_with('0') {
        (&rest[1..], 8)
    } else {
        (rest, 10)
    };
    !digits.is_empty()
        && !digits.starts_with(['+', '-'])
        && i128::from_str_radix(digits, radix).is_ok_and(|n| {
            let n = if word.starts_with('-') { -n } else { n };
            i64::try_from(n).is_ok()
        })
}

/// Whether `Double.parseDouble` takes the word (after trimming spaces):
/// a decimal or hexadecimal floating-point literal, `NaN` or `Infinity`.
fn java_double(word: &str) -> bool {
    let w = word.trim_matches(|c: char| c <= ' ');
    let body = w.strip_prefix(['+', '-']).unwrap_or(w);
    if body == "NaN" || body == "Infinity" {
        return true;
    }
    // A type suffix (f, F, d, D) is allowed after a number.
    let body = body.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(body);
    if body.starts_with("0x") || body.starts_with("0X") {
        return java_hex_double(&body[2..]);
    }
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if int.is_empty() && frac.is_empty() || !digits(int) || !digits(frac) {
        return false;
    }
    match exponent {
        None => true,
        Some(e) => {
            let e = e.strip_prefix(['+', '-']).unwrap_or(e);
            !e.is_empty() && digits(e)
        }
    }
}

/// A hexadecimal floating-point literal after its `0x`: hex digits with
/// an optional point, then a binary exponent `p` that Java requires.
fn java_hex_double(body: &str) -> bool {
    let Some(at) = body.find(['p', 'P']) else {
        return false;
    };
    let (mantissa, exponent) = (&body[..at], &body[at + 1..]);
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let hex = |s: &str| s.bytes().all(|b| b.is_ascii_hexdigit());
    let e = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    !(int.is_empty() && frac.is_empty())
        && hex(int)
        && hex(frac)
        && !e.is_empty()
        && e.bytes().all(|b| b.is_ascii_digit())
}

/// `OptionSpec.longestName`: the first of the longest names.
fn longest_name(arg: &Arg) -> &str {
    let mut longest = arg.names[0].as_str();
    for n in &arg.names[1..] {
        if n.len() > longest.len() {
            longest = n;
        }
    }
    longest
}

/// A positional parameter's `index`, as picocli writes its range.
fn index_text(arg: &Arg) -> String {
    match arg.index {
        (first, Some(last)) if first == last => first.to_string(),
        (first, Some(last)) => format!("{first}..{last}"),
        (first, None) => format!("{first}..*"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every command line picocli takes, saranga's clap command line takes
    /// as `clap_args` writes it: the options and values Artemis's commands
    /// take, saranga's take.
    #[test]
    fn clap_takes_what_picocli_takes() {
        use clap::FromArgMatches;
        let text = include_str!("../../../../tests/golden/cli/parse.txt");
        let missing: Vec<Vec<&str>> = super::super::MISSING
            .iter()
            .map(|m| m.split(' ').collect())
            .collect();
        let commands = [crate::command_line(false), crate::command_line(true)];
        let mut failures = Vec::new();
        let mut checked = 0;
        for line in text.lines() {
            let (case, outcome) = line.split_once("  =>  ").expect("a case");
            if !outcome.starts_with("ok ") {
                continue;
            }
            let in_shell = case.starts_with("shell");
            let command = &commands[usize::from(in_shell)];
            let words: Vec<String> = match case.split_once(' ') {
                Some((_, args)) => args.split('\u{1f}').map(str::to_owned).collect(),
                None => Vec::new(),
            };
            if missing
                .iter()
                .any(|m| words.len() >= m.len() && words[..m.len()] == m[..])
            {
                continue;
            }
            let parsed = parse(&words, in_shell).expect("parsed as picocli parses it");
            // A group prints its help; clap is not asked.
            if parsed.is_group() {
                continue;
            }
            let args = parsed.clap_args("saranga", command);
            checked += 1;
            if let Err(e) = command
                .clone()
                .try_get_matches_from(crate::artemis_spellings(args.clone()))
                .and_then(|m| crate::Cli::from_arg_matches(&m))
            {
                failures.push(format!("{words:?} as {args:?}: {}", e.kind()));
            }
        }
        failures.sort();
        failures.dedup();
        assert!(
            failures.is_empty(),
            "{} of {checked} refused:\n{}",
            failures.len(),
            failures
                .iter()
                .take(400)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    /// The command lines of `tests/golden/cli/parse.txt`, parsed by
    /// Artemis's picocli command line: saranga's parser must make the same
    /// of each. A command only the other mode's command line has is taken
    /// as the other mode takes it, as saranga takes every command in both
    /// (COMPAT.md); the lines through commands saranga does not have are
    /// left out.
    #[test]
    fn parses_as_picocli_parses() {
        let text = include_str!("../../../../tests/golden/cli/parse.txt");
        let mut golden = std::collections::HashMap::new();
        let mut lines = Vec::new();
        for line in text.lines() {
            let (case, outcome) = line.split_once("  =>  ").expect("a case");
            // The mode alone: no arguments.
            let (mode, args) = match case.split_once(' ') {
                Some((mode, args)) => (mode, Some(args)),
                None => (case, None),
            };
            golden.insert((mode, args), outcome);
            lines.push((mode, args, outcome));
        }
        let missing: Vec<Vec<&str>> = super::super::MISSING
            .iter()
            .map(|m| m.split(' ').collect())
            .collect();
        let mut failures = Vec::new();
        let mut checked = 0;
        for (mode, args, outcome) in lines {
            let words: Vec<String> = match args {
                None => Vec::new(),
                Some(args) => args.split('\u{1f}').map(str::to_owned).collect(),
            };
            if missing
                .iter()
                .any(|m| words.len() >= m.len() && words[..m.len()] == m[..])
            {
                continue;
            }
            let in_shell = mode.starts_with("shell");
            let other = match mode {
                "instance" => "home",
                "home" => "instance",
                "shell" => "shell-home",
                _ => "shell",
            };
            let expected = if outcome.starts_with("error Unmatched argument")
                && outcome.contains(" index 0: ")
                && !words.is_empty()
                && artemis().commands.iter().any(|c| c.name == words[0])
            {
                golden.get(&(other, args)).copied().unwrap_or(outcome)
            } else {
                outcome
            };
            let got = match parse(&words, in_shell) {
                Ok(p) => {
                    let mut s = format!(
                        "ok {}",
                        if p.path.is_empty() {
                            "-".to_owned()
                        } else {
                            p.path.join(" ")
                        }
                    );
                    for g in &p.options {
                        s.push_str(&format!(" {}={}", g.name, g.value));
                    }
                    for (i, v) in &p.positionals {
                        s.push_str(&format!(" [{i}]={v}"));
                    }
                    s
                }
                Err(e) => format!(
                    "error {}  @  {}  ?  {}",
                    e.message,
                    if e.path.is_empty() {
                        "-".to_owned()
                    } else {
                        e.path.join(" ")
                    },
                    e.suggestions().unwrap_or_default()
                ),
            };
            // The root's suggestions name the commands of where Artemis
            // ran; saranga suggests none there.
            let root = |s: &str| s.split_once("  @  -  ?  ").map(|(m, _)| m.to_owned());
            let (expected, got) = match (root(expected), root(&got)) {
                (Some(e), Some(g)) => (e, g),
                _ => (expected.to_owned(), got),
            };
            // Nor does saranga suggest a command it does not have, and it
            // may suggest options of its own (`run --etc`).
            let suggests = |s: &str, names: &[&str]| {
                s.split_once("  ?  ")
                    .is_some_and(|(_, x)| names.iter().any(|n| x.contains(n)))
            };
            let (expected, got) = if suggests(&expected, &super::super::MISSING)
                || suggests(&got, &["--config", "--etc", "--bind", "-D"])
            {
                let message = |s: &str| {
                    s.split_once("  ?  ")
                        .map_or(s.to_owned(), |(m, _)| m.to_owned())
                };
                (message(&expected), message(&got))
            } else {
                (expected, got)
            };
            checked += 1;
            if got != expected {
                failures.push(format!(
                    "{mode:?} {words:?}\n  expected {expected}\n  got      {got}"
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {checked} differ:\n{}",
            failures.len(),
            failures
                .iter()
                .take(40)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}
