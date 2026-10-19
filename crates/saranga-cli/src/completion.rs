//! `saranga completion`: the bash (or zsh, through `bashcompinit`)
//! completion script `artemis completion` prints
//! (`source <(./artemis completion)`), which picocli 4.7's
//! `AutoComplete.bash` generates from Artemis's command line. Here it is
//! generated the same way from that command line as `help` knows it
//! ([`crate::help::artemis`]), so that TAB offers what it offers for
//! Artemis: the subcommands, a word starting with `-` the options, and
//! after an option that takes a value, that value's candidates (or file or
//! host names).

use std::fmt::Write;

use crate::help::{self, Arg, Spec};

/// The script's helper functions, after its header comment; `{name}` is
/// the script's name.
const HEADER: &str = include_str!("completion-header.bash");

/// picocli's `CommandDescriptor`: a command, its bash function and the one
/// of its parent.
struct Descriptor<'a> {
    function: String,
    parent_function: String,
    /// Its name; the script's for the top-level command.
    name: String,
    /// The subcommand names leading to it, separated by spaces.
    path: String,
    spec: &'a Spec,
}

/// `AutoComplete.bash(startScript, Artemis.buildCommand(true, true,
/// true))`: every command but the shell's `connect` and `disconnect`.
pub fn script(start_script: &str) -> String {
    let mut root = help::artemis().clone();
    root.commands
        .retain(|c| c.name != "connect" && c.name != "disconnect");
    generate(start_script, &root)
}

/// `AutoComplete.bash`: the script for the command line `root`, started
/// as `start_script`.
fn generate(start_script: &str, root: &Spec) -> String {
    let name = start_script
        .replace(".sh", "")
        .replace(".bash", "")
        .replace("./", "");
    let mut hierarchy = vec![Descriptor {
        function: format!("_picocli_{name}"),
        parent_function: String::new(),
        name: name.clone(),
        path: String::new(),
        spec: root,
    }];
    sub_hierarchy(&name, "", root, &mut hierarchy);

    let mut s = HEADER.replace("{name}", &name);
    s.push_str(&entry_point(&name, &hierarchy));
    for d in &hierarchy {
        s.push_str(&function(&d.function, &d.name, d.spec));
    }
    let _ = write!(
        s,
        "\n# Define a completion specification (a compspec) for the\n\
         # `{name}`, `{name}.sh`, and `{name}.bash` commands.\n\
         # Uses the bash `complete` builtin (see [6]) to specify that shell function\n\
         # `_complete_{name}` is responsible for generating possible completions for the\n\
         # current word on the command line.\n\
         # The `-o default` option means that if the function generated no matches, the\n\
         # default Bash completions and the Readline default filename completions are performed.\n\
         complete -F _complete_{name} -o default {name} {name}.sh {name}.bash\n"
    );
    s
}

/// `createSubHierarchy`: a command's visible subcommands, then theirs.
fn sub_hierarchy<'a>(
    script_name: &str,
    parent_path: &str,
    spec: &'a Spec,
    out: &mut Vec<Descriptor<'a>>,
) {
    let parent_function = function_name(script_name, parent_path);
    let visible = || spec.commands.iter().filter(|c| !c.hidden);
    for sub in visible() {
        let path = join(parent_path, &sub.name);
        out.push(Descriptor {
            function: function_name(script_name, &path),
            parent_function: parent_function.clone(),
            name: sub.name.clone(),
            path,
            spec: sub,
        });
    }
    for sub in visible() {
        sub_hierarchy(script_name, &join(parent_path, &sub.name), sub, out);
    }
}

fn join(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_owned()
    } else {
        format!("{path} {name}")
    }
}

fn function_name(script_name: &str, path: &str) -> String {
    if path.is_empty() {
        format!("_picocli_{script_name}")
    } else {
        format!(
            "_picocli_{script_name}_{}",
            bashify(&path.replace(' ', "_"))
        )
    }
}

/// `bashify`: letters, digits and `_`, spaces as `_`, and a `_` before a
/// leading digit.
fn bashify(s: &str) -> String {
    let mut out: String = s
        .chars()
        .filter_map(|c| {
            if c.is_alphanumeric() || c == '_' {
                Some(c)
            } else if c == ' ' {
                Some('_')
            } else {
                None
            }
        })
        .collect();
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// `generateEntryPointFunction`: `_complete_<name>`, which calls the
/// function of the longest sequence of subcommands on the line.
fn entry_point(name: &str, hierarchy: &[Descriptor]) -> String {
    let mut s = format!(
        "# Bash completion entry point function.\n\
         # _complete_{name} finds which commands and subcommands have been specified\n\
         # on the command line and delegates to the appropriate function\n\
         # to generate possible options and subcommands for the last specified subcommand.\n\
         function _complete_{name}() {{\n"
    );
    s.push_str(
        "  # Edge case: if command line has no space after subcommand, then don't assume this subcommand is selected (remkop/picocli#1468).\n",
    );
    for d in &hierarchy[1..] {
        let _ = writeln!(
            s,
            "  if [ \"${{COMP_LINE}}\" = \"${{COMP_WORDS[0]}} {}\" ];    then {}; return $?; fi",
            d.path, d.parent_function
        );
    }
    s.push('\n');
    s.push_str(
        "  # Find the longest sequence of subcommands and call the bash function for that subcommand.\n",
    );
    for (i, d) in hierarchy[1..].iter().enumerate() {
        let _ = writeln!(s, "  local cmds{i}=({})", d.path);
    }
    s.push('\n');
    for (i, d) in hierarchy[1..].iter().enumerate().rev() {
        let _ = writeln!(
            s,
            "  if CompWordsContainsArray \"${{cmds{i}[@]}}\"; then {}; return $?; fi",
            d.function
        );
    }
    let _ = write!(
        s,
        "\n  # No subcommands were specified; generate completions for the top-level command.\n  \
         _picocli_{name}; return $?;\n}}\n"
    );
    s
}

fn boolean(a: &Arg) -> bool {
    a.kind.as_deref() == Some("boolean")
}

fn quoted_names<'a>(options: impl Iterator<Item = &'a Arg>) -> String {
    options
        .filter(|o| !o.hidden)
        .flat_map(|o| o.names.iter().map(|n| format!("'{n}'")))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `generateFunctionForCommand`: a command's function, which completes its
/// options, its options' values, its subcommands and its positional
/// parameters.
fn function(function: &str, name: &str, spec: &Spec) -> String {
    let flags = quoted_names(spec.options.iter().filter(|o| boolean(o)));
    let arg_options: Vec<&Arg> = spec.options.iter().filter(|o| !boolean(o)).collect();
    let args = quoted_names(arg_options.iter().copied());
    let commands = spec
        .commands
        .iter()
        .filter(|c| !c.hidden)
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let sub = if *function == format!("_picocli_{name}") {
        ""
    } else {
        "sub"
    };
    let prev = if arg_options.is_empty() {
        ""
    } else {
        "  local prev_word=${COMP_WORDS[COMP_CWORD-1]}\n"
    };
    let mut s = format!(
        "\n# Generates completions for the options and subcommands of the `{name}` {sub}command.\n\
         function {function}() {{\n  \
         # Get completion data\n  \
         local curr_word=${{COMP_WORDS[COMP_CWORD]}}\n\
         {prev}\n  \
         local commands=\"{commands}\"\n  \
         local flag_opts=\"{flags}\"\n  \
         local arg_opts=\"{args}\"\n"
    );
    for o in spec.options.iter().filter(|o| !o.hidden) {
        if let Some(values) = &o.completion {
            let _ = writeln!(
                s,
                "  local {}_option_args=(\"{}\") # {} values",
                bashify(&o.label),
                values.join("\" \""),
                longest_name(o)
            );
        }
    }
    s.push_str(&options_switch(&arg_options));
    for p in &spec.parameters {
        if let Some(values) = &p.completion {
            let _ = writeln!(
                s,
                "  local {}_pos_param_args=(\"{}\") # {}-{} values",
                bashify(&p.label),
                values.join("\" \""),
                p.index.0,
                index_max(p)
            );
        }
    }
    let cases = positional_cases(&spec.parameters);
    let positionals = if cases.is_empty() {
        String::new()
    } else {
        format!(
            "    local currIndex\n    \
             currIndex=$(currentPositionalIndex \"{name}\" \"${{arg_opts}}\" \"${{flag_opts}}\")\n{cases}"
        )
    };
    let _ = write!(
        s,
        "\n  if [[ \"${{curr_word}}\" == -* ]]; then\n    \
         COMPREPLY=( $(compgen -W \"${{flag_opts}} ${{arg_opts}}\" -- \"${{curr_word}}\") )\n  \
         else\n    \
         local positionals=\"\"\n\
         {positionals}    \
         local IFS=$'\\n'\n    \
         COMPREPLY=( $(compgen -W \"${{commands// /$'\\n'}}${{IFS}}${{positionals}}\" -- \"${{curr_word}}\") )\n  \
         fi\n}}\n"
    );
    s
}

/// `longestName`: the first of the longest names.
fn longest_name(o: &Arg) -> &str {
    let mut longest = o.names[0].as_str();
    for n in &o.names {
        if n.len() > longest.len() {
            longest = n;
        }
    }
    longest
}

/// `Range.max`, `Integer.MAX_VALUE` when unbounded.
fn index_max(p: &Arg) -> usize {
    p.index.1.unwrap_or(i32::MAX as usize)
}

/// `generateOptionsSwitch`: what follows an option that takes a value.
fn options_switch(arg_options: &[&Arg]) -> String {
    let mut cases = String::new();
    for o in arg_options.iter().filter(|o| !o.hidden) {
        let names = o
            .names
            .iter()
            .map(|n| format!("'{n}'"))
            .collect::<Vec<_>>()
            .join("|");
        let _ = writeln!(cases, "    {names})");
        if o.completion.is_some() {
            cases.push_str("      local IFS=$'\\n'\n");
            let _ = writeln!(
                cases,
                "      COMPREPLY=( $( compReplyArray \"${{{}_option_args[@]}}\" ) )",
                bashify(&o.label)
            );
            cases.push_str("      return $?\n");
        } else if o.kind.as_deref() == Some("file") {
            cases.push_str("      local IFS=$'\\n'\n");
            cases.push_str("      type compopt &>/dev/null && compopt -o filenames\n");
            cases.push_str("      COMPREPLY=( $( compgen -f -- \"${curr_word}\" ) ) # files\n");
            cases.push_str("      return $?\n");
        } else if o.kind.as_deref() == Some("host") {
            cases.push_str("      type compopt &>/dev/null && compopt -o filenames\n");
            cases.push_str("      COMPREPLY=( $( compgen -A hostname -- \"${curr_word}\" ) )\n");
            cases.push_str("      return $?\n");
        } else {
            cases.push_str("      return\n");
        }
        cases.push_str("      ;;\n");
    }
    if cases.is_empty() {
        return String::new();
    }
    format!(
        "\n  type compopt &>/dev/null && compopt +o default\n\n  case ${{prev_word}} in\n{cases}  esac\n"
    )
}

/// `generatePositionalParamsCases`: what completes the positional
/// parameter at the current index.
fn positional_cases(parameters: &[Arg]) -> String {
    let mut s = String::new();
    for p in parameters.iter().filter(|p| !p.hidden) {
        let keyword = if s.is_empty() { "if" } else { "elif" };
        let test = format!(
            "    {keyword} (( currIndex >= {} && currIndex <= {} )); then\n",
            p.index.0,
            index_max(p)
        );
        if p.completion.is_some() {
            s.push_str(&test);
            let _ = writeln!(
                s,
                "      positionals=$( compReplyArray \"${{{}_pos_param_args[@]}}\" )",
                bashify(&p.label)
            );
        } else if p.kind.as_deref() == Some("file") {
            s.push_str(&test);
            s.push_str("      local IFS=$'\\n'\n");
            s.push_str("      type compopt &>/dev/null && compopt -o filenames\n");
            s.push_str("      positionals=$( compgen -f -- \"${curr_word}\" ) # files\n");
        } else if p.kind.as_deref() == Some("host") {
            s.push_str(&test);
            s.push_str("      type compopt &>/dev/null && compopt -o filenames\n");
            s.push_str("      positionals=$( compgen -A hostname -- \"${curr_word}\" )\n");
        }
    }
    if !s.is_empty() {
        s.push_str("    fi\n");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::script;

    #[test]
    fn completes_as_picocli_does() {
        let s = script("./artemis");
        assert!(s.starts_with("#!/usr/bin/env bash\n#\n# artemis Bash Completion\n"));
        // The commands saranga has, and no shell commands.
        assert!(s.contains("  local cmds0=(help)\n"));
        assert!(s.contains("  local cmds1=(pwd)\n"));
        assert!(s.contains("_picocli_artemis_activation_list()"));
        assert!(s.contains("_picocli_artemis_lock_list()"));
        assert!(!s.contains("_picocli_artemis_connect"));
        assert!(s.contains("_picocli_artemis_perfjournal()"));
        // Option values: candidates, file names, nothing.
        assert!(
            s.contains("  local protocol_option_args=(\"AMQP\" \"CORE\") # --protocol values\n")
        );
        assert!(s.contains("    '--history')\n      local IFS=$'\\n'\n      type compopt &>/dev/null && compopt -o filenames\n"));
        assert!(s.contains("    '--staticCluster'|'--static-cluster')\n      return\n      ;;\n"));
        // create's directory.
        assert!(s.contains(
            "    currIndex=$(currentPositionalIndex \"create\" \"${arg_opts}\" \"${flag_opts}\")\n    if (( currIndex >= 0 && currIndex <= 0 )); then\n"
        ));
        assert!(s.ends_with(
            "complete -F _complete_artemis -o default artemis artemis.sh artemis.bash\n"
        ));
    }
}
