//! `artemis help` (`HelpAction`): picocli's usage message of a command, as
//! Artemis's command line declares it. The commands, their options and
//! descriptions are Artemis's ([artemis-cli.json], which
//! `scripts/CliHelp.java` writes from `Artemis.buildCommand`), laid out as
//! picocli 4.7 lays them out at its default width of 80 columns, without
//! its colours. The commands saranga does not have are left out.

use std::sync::OnceLock;

use saranga_buffer::json::Json;

mod breaks;
pub mod parse;

/// picocli's `usageHelpWidth`.
const WIDTH: usize = 80;
/// picocli's `longOptionsMaxWidth`: longer options do not widen the
/// option column.
const LONG_OPTIONS_MAX_WIDTH: usize = 20;

/// The commands of Artemis's command line that saranga does not have (the
/// lock coordinators, `properties`, `check cluster` with clustering), and
/// the deprecated `auto-complete`.
const MISSING: [&str; 4] = ["lock", "properties", "check cluster", "auto-complete"];

/// Where the command line is: `buildCommand`'s choice of commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Run from an instance (`artemis.instance` set): its commands, and no
    /// `create` or `upgrade`.
    Instance,
    /// Run from the home: `data`, `create` and `upgrade` beside the
    /// client commands.
    Home,
}

#[derive(Debug, Clone)]
pub struct Spec {
    pub name: String,
    pub hidden: bool,
    description: Vec<String>,
    pub options: Vec<Arg>,
    pub parameters: Vec<Arg>,
    pub commands: Vec<Spec>,
}

#[derive(Debug, Clone)]
pub struct Arg {
    pub names: Vec<String>,
    pub label: String,
    /// `arity`: its minimum and maximum (`None`: unbounded).
    arity: (usize, Option<usize>),
    /// A positional parameter's `index`: its first and last position
    /// (`None`: unbounded).
    pub index: (usize, Option<usize>),
    /// Whether it takes more than one value (`isMultiValue`).
    multi: bool,
    pub hidden: bool,
    required: bool,
    description: Vec<String>,
    candidates: Vec<String>,
    /// `completionCandidates`, which picocli's completion script offers
    /// (an enum's constants, or those declared).
    pub completion: Option<Vec<String>>,
    /// Its type, where the completion script cares: `boolean` (a flag),
    /// `file` (`File` or `Path`, completed with file names) or `host`
    /// (`InetAddress`, completed with host names).
    pub kind: Option<String>,
    /// How picocli converts its values.
    pub java: Java,
    /// Whether its values are collected in a list (or an array).
    pub list: bool,
}

/// The type an argument's values are converted to, as picocli converts
/// them (`scripts/CliHelp.java`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Java {
    /// `boolean`, or `Boolean` (`boxed`), whose commands tell a value left
    /// unset from false.
    Boolean {
        boxed: bool,
    },
    Int,
    Long,
    String,
    File,
    /// An enum, by picocli's converter: the constant of that name.
    Enum(Vec<String>),
    /// An enum by Artemis's `ConnectionProtocol.ProtocolConverter`: the
    /// constant named by the value in upper case.
    UpperEnum {
        class: String,
        constants: Vec<String>,
    },
}

impl Arg {
    fn boolean(&self) -> bool {
        self.arity.1 == Some(0)
    }

    /// The option's names, shortest first, as the option list shows them.
    fn names_by_length(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.names.iter().map(String::as_str).collect();
        names.sort_by_key(|n| n.len());
        names
    }

    fn shortest_name(&self) -> &str {
        self.names_by_length()[0]
    }

    /// A positional parameter's label: `<label>`, `[<label>]` when it may
    /// be left out, with `...` when it repeats.
    fn parameter_label(&self) -> String {
        let label = if self.multi {
            format!("{}...", self.label)
        } else {
            self.label.clone()
        };
        if self.arity.0 == 0 {
            format!("[{label}]")
        } else {
            label
        }
    }
}

/// Artemis's command line, as `buildCommand(true, true, true)` makes it
/// with the shell's `connect` and `disconnect`.
pub fn artemis() -> &'static Spec {
    static SPEC: OnceLock<Spec> = OnceLock::new();
    SPEC.get_or_init(|| {
        let json = Json::parse(include_str!("artemis-cli.json")).expect("artemis-cli.json");
        let mut root = spec(&json);
        prune(&mut root, "");
        root
    })
}

fn prune(spec: &mut Spec, path: &str) {
    spec.commands.retain(|c| {
        let p = if path.is_empty() {
            c.name.clone()
        } else {
            format!("{path} {}", c.name)
        };
        !MISSING.contains(&p.as_str())
    });
    for c in &mut spec.commands {
        let p = if path.is_empty() {
            c.name.clone()
        } else {
            format!("{path} {}", c.name)
        };
        prune(c, &p);
    }
}

fn strings(json: Option<&Json>) -> Vec<String> {
    match json {
        Some(Json::Array(items)) => items
            .iter()
            .filter_map(|i| i.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

fn spec(json: &Json) -> Spec {
    let list = |key: &str| match json.get(key) {
        Some(Json::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    Spec {
        name: json
            .get("name")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned(),
        hidden: json.get("hidden").and_then(Json::as_bool).unwrap_or(false),
        description: strings(json.get("description")),
        options: list("options").iter().map(arg).collect(),
        parameters: list("parameters").iter().map(arg).collect(),
        commands: list("commands").iter().map(spec).collect(),
    }
}

fn arg(json: &Json) -> Arg {
    let text = |key: &str| json.get(key).and_then(Json::as_str).unwrap_or_default();
    let bound = |s: &str| s.parse::<usize>().ok();
    let range = |key: &str, default: usize| match text(key).split_once("..") {
        Some((min, max)) => (bound(min).unwrap_or(0), bound(max)),
        None => {
            let n = bound(text(key)).unwrap_or(default);
            (n, Some(n))
        }
    };
    let arity = range("arity", 1);
    Arg {
        names: strings(json.get("names")),
        label: text("label").to_owned(),
        arity,
        index: range("index", 0),
        multi: json.get("multi").and_then(Json::as_bool).unwrap_or(false),
        hidden: json.get("hidden").and_then(Json::as_bool).unwrap_or(false),
        required: json
            .get("required")
            .and_then(Json::as_bool)
            .unwrap_or(false),
        description: strings(json.get("description")),
        candidates: strings(json.get("candidates")),
        completion: json.get("completion").map(|c| strings(Some(c))),
        kind: json.get("type").and_then(Json::as_str).map(str::to_owned),
        java: match text("java") {
            "boolean" => Java::Boolean { boxed: false },
            "Boolean" => Java::Boolean { boxed: true },
            "int" => Java::Int,
            "long" => Java::Long,
            "File" => Java::File,
            "enum" => Java::Enum(strings(json.get("constants"))),
            "upper-enum" => Java::UpperEnum {
                class: text("class").to_owned(),
                constants: strings(json.get("constants")),
            },
            _ => Java::String,
        },
        list: json.get("list").and_then(Json::as_bool).unwrap_or(false),
    }
}

impl Spec {
    /// The subcommands this command line offers: `buildCommand`'s choice
    /// at the root, all of them below it.
    fn subcommands(&self, root: bool, mode: Mode, in_shell: bool) -> Vec<&Spec> {
        self.commands
            .iter()
            .filter(|c| !c.hidden)
            .filter(|c| {
                !root
                    || match c.name.as_str() {
                        "shell" => !in_shell,
                        "connect" | "disconnect" => in_shell,
                        "run" => mode == Mode::Instance && !in_shell,
                        "activation" | "user" | "stop" | "kill" | "perf-journal" => {
                            mode == Mode::Instance
                        }
                        "create" | "upgrade" => mode == Mode::Home,
                        _ => true,
                    }
            })
            .collect()
    }
}

/// `HelpAction.help`: the usage of the command the words name, walking
/// down from the root; a word that names no subcommand prints the root's
/// usage first, as Artemis's does.
pub fn help(root_name: &str, words: &[String], mode: Mode, in_shell: bool) -> String {
    let root = artemis();
    let mut out = String::new();
    let mut at = root;
    let mut path = vec![root_name.to_owned()];
    for word in words {
        let sub = at
            .subcommands(std::ptr::eq(at, root), mode, in_shell)
            .into_iter()
            .find(|c| &c.name == word);
        match sub {
            Some(sub) => {
                at = sub;
                path.push(sub.name.clone());
            }
            None => out.push_str(&usage(root, &[root_name.to_owned()], mode, in_shell)),
        }
    }
    out.push_str(&usage(at, &path, mode, in_shell));
    out
}

/// `CommandLine.usage`: the synopsis, the description, the parameters and
/// options, and the subcommands.
fn usage(spec: &Spec, path: &[String], mode: Mode, in_shell: bool) -> String {
    let root = path.len() == 1;
    let commands = spec.subcommands(root, mode, in_shell);
    let mut out = synopsis(spec, &path.join(" "), !commands.is_empty());
    for d in &spec.description {
        for line in wrap(&interpolate(d, &[]), WIDTH, 0, 0) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out.push_str(&arguments(spec));
    if !commands.is_empty() {
        out.push_str("Commands:\n");
        let width = commands.iter().map(|c| c.name.len()).max().unwrap_or(0);
        for c in commands {
            let column = 2 + width + 2;
            let description = c.description.first().cloned().unwrap_or_default();
            let lines = wrap(&interpolate(&description, &[]), WIDTH - column, 0, 2);
            let mut first = format!("  {:width$}  ", c.name);
            match lines.first() {
                Some(l) => first.push_str(l),
                None => first.truncate(first.trim_end().len()),
            }
            out.push_str(&first);
            out.push('\n');
            for l in lines.iter().skip(1) {
                out.push_str(&" ".repeat(column));
                out.push_str(l);
                out.push('\n');
            }
        }
    }
    out
}

/// The synopsis: `Usage: <command>`, its boolean options, its options with
/// values (each group sorted by name, optional ones in brackets), its
/// positional parameters and `[COMMAND]`, wrapped at spaces under the
/// first option.
fn synopsis(spec: &Spec, name: &str, commands: bool) -> String {
    let mut options: Vec<&Arg> = spec.options.iter().filter(|o| !o.hidden).collect();
    // `SortByOptionArityAndNameAlphabetically`.
    options.sort_by(|a, b| {
        let max = |o: &Arg| o.arity.1.unwrap_or(usize::MAX);
        max(a)
            .cmp(&max(b))
            .then_with(|| a.arity.0.cmp(&b.arity.0))
            .then_with(|| a.multi.cmp(&b.multi))
            .then_with(|| compare_names(a.shortest_name(), b.shortest_name()))
    });
    let mut items: Vec<String> = options
        .iter()
        .map(|o| {
            let text = if o.boolean() {
                o.shortest_name().to_owned()
            } else {
                format!("{}={}", o.shortest_name(), o.label)
            };
            let text = if o.required {
                text
            } else {
                format!("[{text}]")
            };
            if o.multi { format!("{text}...") } else { text }
        })
        .collect();
    items.extend(
        spec.parameters
            .iter()
            .filter(|p| !p.hidden)
            .map(Arg::parameter_label),
    );
    if commands {
        items.push("[COMMAND]".to_owned());
    }
    let text = format!("Usage: {name} {}", items.join(" "));
    // Breaks at the spaces between the items only.
    let breaks: Vec<usize> = text
        .char_indices()
        .filter(|&(i, c)| c != ' ' && i > 0 && text.as_bytes()[i - 1] == b' ')
        .map(|(i, _)| i)
        .chain(std::iter::once(text.len()))
        .collect();
    let indent = "Usage: ".len() + name.len() + 1;
    let mut out = String::new();
    for (i, line) in wrap_at(&text, &breaks, WIDTH, indent)
        .into_iter()
        .enumerate()
    {
        if i > 0 {
            out.push_str(&" ".repeat(indent));
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// `SortByShortestOptionNameAlphabetically`: by the shortest names
/// without their prefix (`log-interval` among `--large-messages` and
/// `--output`), ignoring case, lower case first.
fn compare_names(a: &str, b: &str) -> std::cmp::Ordering {
    let (a, b) = (a.trim_start_matches('-'), b.trim_start_matches('-'));
    a.to_uppercase()
        .cmp(&b.to_uppercase())
        .then_with(|| b.cmp(a))
}

/// The positional parameters, then the options sorted by name: each in
/// the option column (six columns in, as wide as the longest of them up to
/// `longOptionsMaxWidth`), its description beside it, or below it when it
/// is wider.
fn arguments(spec: &Spec) -> String {
    let mut options: Vec<&Arg> = spec.options.iter().filter(|o| !o.hidden).collect();
    options.sort_by(|a, b| compare_names(a.shortest_name(), b.shortest_name()));
    let rows: Vec<(String, &Arg)> = spec
        .parameters
        .iter()
        .filter(|p| !p.hidden)
        .map(|p| (p.parameter_label(), p))
        .chain(options.into_iter().map(|o| {
            let names = o.names_by_length().join(", ");
            let text = if o.boolean() {
                names
            } else {
                format!("{names}={}", o.label)
            };
            (text, o)
        }))
        .collect();
    let widest = rows
        .iter()
        .map(|(t, _)| t.len())
        .filter(|&l| l <= LONG_OPTIONS_MAX_WIDTH)
        .max()
        .unwrap_or(0);
    let column = 6 + widest + 3;
    let mut out = String::new();
    for (text, arg) in rows {
        let mut line = format!("      {text}");
        let mut descriptions = arg
            .description
            .iter()
            .flat_map(|d| {
                let d = interpolate(d, &arg.candidates);
                let mut lines = wrap(&d, WIDTH - column, 0, 2);
                if lines.is_empty() {
                    lines.push(String::new());
                }
                lines
            })
            .peekable();
        if text.len() > widest + 2 && descriptions.peek().is_some() {
            out.push_str(&line);
            out.push('\n');
            line = String::new();
        }
        if let Some(d) = descriptions.next() {
            line = format!("{line:column$}{d}");
        }
        out.push_str(line.trim_end());
        out.push('\n');
        for d in descriptions {
            out.push_str(format!("{}{d}", " ".repeat(column)).trim_end());
            out.push('\n');
        }
    }
    out
}

/// picocli's variables in a description: `${COMPLETION-CANDIDATES}` (an
/// enum option's values), and others from the environment, `null` when it
/// has none.
fn interpolate(text: &str, candidates: &[String]) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        let Some(end) = rest[start..].find('}') else {
            break;
        };
        out.push_str(&rest[..start]);
        let name = &rest[start + 2..start + end];
        match name {
            "COMPLETION-CANDIDATES" => out.push_str(&candidates.join(", ")),
            _ => out.push_str(&std::env::var(name).unwrap_or_else(|_| "null".to_owned())),
        }
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    out
}

/// picocli's `TextTable` wrapping a column `width` wide: words as Java's
/// line `BreakIterator` finds them, the first line indented by `indent`
/// and the others by `indent + wrapped`.
fn wrap(text: &str, width: usize, indent: usize, wrapped: usize) -> Vec<String> {
    let lines = wrap_at(text, &breaks::line_breaks(text), width - indent, wrapped);
    lines
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let pad = if i == 0 { indent } else { indent + wrapped };
            format!("{}{l}", " ".repeat(pad))
        })
        .collect()
}

/// The lines `text` wraps into at the given break offsets: a word fits on
/// a line with the spaces after it, the first line is `width` wide and the
/// others `width - wrapped`, and a word wider than a line is cut where the
/// line ends. The lines come without their indentation or trailing spaces.
fn wrap_at(text: &str, breaks: &[usize], width: usize, wrapped: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut line = String::new();
    let mut room = width;
    let mut start = 0;
    for &end in breaks {
        let mut word = &text[start..end];
        start = end;
        if !line.is_empty() && line.len() + word.len() > room {
            lines.push(line.trim_end().to_owned());
            line = String::new();
            room = width - wrapped;
        }
        while line.len() + word.trim_end_matches(' ').len() > room {
            let (head, tail) = word.split_at(room - line.len());
            line.push_str(head);
            lines.push(line.trim_end().to_owned());
            line = String::new();
            room = width - wrapped;
            word = tail;
        }
        line.push_str(word);
    }
    if !line.trim().is_empty() {
        lines.push(line.trim_end().to_owned());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_manual_s_help() {
        let help = help("artemis", &[], Mode::Home, false);
        assert!(help.starts_with("Usage: artemis [COMMAND]\nApache Artemis Command Line\nCommands:\n  help        use 'help <command>' for more information\n"));
        assert!(help.contains("  upgrade     Update a broker instance to the current artemis.home, keeping all\n                the data and broker.xml. Warning: backup your instance before\n"));
        let help = help_of(&["help"]);
        assert_eq!(
            help,
            "Usage: artemis help [<args>...]\nuse 'help <command>' for more information\n      [<args>...]\n"
        );
    }

    fn help_of(words: &[&str]) -> String {
        let words: Vec<String> = words.iter().map(|w| (*w).to_owned()).collect();
        help("artemis", &words, Mode::Instance, false)
    }

    /// Every option the help shows is one saranga takes.
    #[test]
    fn saranga_takes_the_options_shown() {
        fn walk(spec: &Spec, command: &clap::Command, path: &str, missing: &mut Vec<String>) {
            for o in spec.options.iter().filter(|o| !o.hidden) {
                for name in &o.names {
                    let long = name.trim_start_matches('-');
                    let taken = command.get_arguments().any(|a| {
                        a.get_long() == Some(long)
                            || a.get_all_aliases().into_iter().flatten().any(|x| x == long)
                    });
                    if !taken {
                        missing.push(format!("{path} {name}"));
                    }
                }
            }
            for sub in &spec.commands {
                match command.find_subcommand(&sub.name) {
                    Some(c) => walk(sub, c, &format!("{path} {}", sub.name), missing),
                    None if sub.hidden => {}
                    None => missing.push(format!("{path} {}", sub.name)),
                }
            }
        }
        let mut missing = Vec::new();
        walk(
            artemis(),
            &crate::command_line(false),
            "artemis",
            &mut missing,
        );
        assert!(missing.is_empty(), "{missing:#?}");
    }

    #[test]
    fn a_long_option_under_the_column() {
        let help = help_of(&["kill"]);
        assert!(help.contains("      --broker=<brokerConfig>\n                        Override the broker configuration from the bootstrap.\n                          xml.\n"), "{help}");
    }
}
