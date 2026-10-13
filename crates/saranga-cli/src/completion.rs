//! `saranga completion`: a bash (or zsh, through `bashcompinit`) completion
//! script for the commands and their options, as `artemis completion`
//! prints picocli's (`source <(./artemis completion)`).

use std::fmt::Write;

/// The script for `start_script`, completing `command`'s subcommands and
/// their long options.
pub fn script(command: &clap::Command, start_script: &str) -> String {
    let name: String = start_script
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(start_script)
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let function = format!("_complete_{name}");
    let mut paths = Vec::new();
    collect(command, String::new(), &mut paths);

    let mut s = String::new();
    let _ = writeln!(s, "#!/usr/bin/env bash");
    let _ = writeln!(s, "#");
    let _ = writeln!(
        s,
        "# {start_script} Bash Completion: source this file, or source <({start_script} completion)"
    );
    let _ = writeln!(s, "#");
    let _ = writeln!(
        s,
        "if [ -n \"$ZSH_VERSION\" ]; then autoload -U +X compinit && compinit; autoload -U +X bashcompinit && bashcompinit; fi"
    );
    let _ = writeln!(s);
    let _ = writeln!(s, "{function}() {{");
    let _ = writeln!(
        s,
        "  local cur=\"${{COMP_WORDS[COMP_CWORD]}}\" path=\"\" i w"
    );
    let _ = writeln!(s, "  for ((i = 1; i < COMP_CWORD; i++)); do");
    let _ = writeln!(s, "    w=\"${{COMP_WORDS[i]}}\"");
    let _ = writeln!(s, "    case \"$path/$w\" in");
    for (path, _) in paths.iter().filter(|(p, _)| !p.is_empty()) {
        let _ = writeln!(s, "      \"{path}\") path=\"{path}\" ;;");
    }
    let _ = writeln!(s, "    esac");
    let _ = writeln!(s, "  done");
    let _ = writeln!(s, "  local words");
    let _ = writeln!(s, "  case \"$path\" in");
    for (path, words) in &paths {
        let _ = writeln!(s, "    \"{path}\") words=\"{}\" ;;", words.join(" "));
    }
    let _ = writeln!(s, "  esac");
    let _ = writeln!(s, "  COMPREPLY=( $(compgen -W \"$words\" -- \"$cur\") )");
    let _ = writeln!(s, "}}");
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "complete -F {function} -o default {start_script} {start_script}.sh {start_script}.bash"
    );
    s
}

/// Each command's path (`/queue/create`) and the words that complete after
/// it: its subcommands, then its long options.
fn collect(command: &clap::Command, path: String, out: &mut Vec<(String, Vec<String>)>) {
    let mut words: Vec<String> = subcommands(command)
        .map(|c| c.get_name().to_owned())
        .collect();
    if command.has_subcommands() {
        words.push("help".to_owned());
    }
    for arg in command.get_arguments().filter(|a| !a.is_hide_set()) {
        if let Some(long) = arg.get_long() {
            words.push(format!("--{long}"));
        }
        for alias in arg.get_visible_aliases().into_iter().flatten() {
            words.push(format!("--{alias}"));
        }
    }
    if !command.is_disable_help_flag_set() {
        words.push("--help".to_owned());
    }
    out.push((path.clone(), words));
    for sub in subcommands(command) {
        collect(sub, format!("{path}/{}", sub.get_name()), out);
    }
}

/// The subcommands shown, but `help`, which completes as clap's own help
/// subcommand does.
fn subcommands(command: &clap::Command) -> impl Iterator<Item = &clap::Command> {
    command
        .get_subcommands()
        .filter(|c| !c.is_hide_set() && c.get_name() != "help")
}

#[cfg(test)]
mod tests {
    use super::script;

    #[test]
    fn completes_subcommands_and_options() {
        let cmd = clap::Command::new("x")
            .subcommand(
                clap::Command::new("queue")
                    .subcommand(clap::Command::new("stat").arg(clap::Arg::new("url").long("url"))),
            )
            .subcommand(clap::Command::new("run"));
        let s = script(&cmd, "./artemis");
        assert!(s.contains("\"\") words=\"queue run help --help\""));
        assert!(s.contains("\"/queue/stat\") path=\"/queue/stat\""));
        assert!(s.contains("\"/queue/stat\") words=\"--url --help\""));
        assert!(s.contains("complete -F _complete_artemis -o default ./artemis"));
    }
}
