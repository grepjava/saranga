//! `saranga upgrade`: an instance's scripts and profiles brought to this
//! `saranga`, as `artemis upgrade` brings them to its `artemis.home`
//! (`Upgrade`): its configuration and data kept, each changed file backed
//! up first. An Artemis instance upgraded so runs on Saranga, and a
//! Saranga instance on Artemis after `artemis upgrade`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

use crate::client::UsageError;
use crate::create::{
    Filters, InstallArgs, ScriptDirs, add_script_filters, canonical, make_exec, saranga_home,
    template_text,
};

#[derive(clap::Args, Debug)]
pub struct UpgradeArgs {
    #[command(flatten)]
    install: InstallArgs,
}

/// The JVM arguments kept between upgrades, and their old names.
const KEEPING_JVM_ARGUMENTS: &[&str] = &[
    "-Xmx",
    "-Djava.security.auth.login.config",
    "-Dhawtio.roles=",
    "-Dartemis.discovery.enabled=",
];
const KEEPING_JVM_ARGUMENTS_ALTERNATES: &[(&str, &str)] = &[("-Dhawtio.roles=", "-Dhawtio.role=")];
const JDK_PREFIX_WINDOWS: &str = "IF \"%JAVA_ARGS%\"==\"\" (set JAVA_ARGS=";
const JDK_PREFIX_LINUX: &str = "JAVA_ARGS=";

pub fn run(args: &UpgradeArgs) -> anyhow::Result<()> {
    let install = &args.install;
    let directory = java_file(&install.directory);
    // `checkDirectory`.
    if !directory.exists() {
        bail!(UsageError(format!(
            "Could not find path '{}' to upgrade.",
            directory.display()
        )));
    } else if std::fs::metadata(&directory)?.permissions().readonly() {
        bail!(UsageError(format!(
            "The path '{}' is not writable.",
            directory.display()
        )));
    }
    if !install.encoding.eq_ignore_ascii_case("UTF-8") {
        bail!("saranga upgrade writes UTF-8 files only");
    }
    let windows = install.is_windows();
    let nix = !windows;
    let broker_home = saranga_home()?;
    let stars = "*".repeat(127);
    println!("{stars}");
    println!(
        "Upgrading broker instance {} to use artemis.home={}",
        directory.display(),
        broker_home.display()
    );

    let bkp = find_backup(&directory)?;
    let bin_bkp = bkp.join("bin");
    let etc_bkp = bkp.join("etc");
    let tmp = bkp.join("tmp");
    for d in [&bin_bkp, &etc_bkp, &tmp] {
        std::fs::create_dir(d)?;
    }

    let bin = directory.join("bin");
    let mut etc = directory.join(&install.etc);
    let mut data = directory.join(&install.data);
    let oome = directory.join("log").join("oom_dump.hprof");
    let artemis_cmd = bin.join("artemis.cmd");
    let artemis_script = bin.join("artemis");

    if install.etc == "etc" {
        etc = if windows {
            path_from_file(etc, &artemis_cmd, "set ARTEMIS_INSTANCE_ETC=", "ETC")?
        } else {
            path_from_file(etc, &artemis_script, "ARTEMIS_INSTANCE_ETC=", "ETC")?
        };
    }
    if !bin.exists() {
        bail!("{} does not exist for binary", bin.display());
    }
    if !etc.exists() {
        bail!("{} does not exist for etc", etc.display());
    }

    let home = match &install.home {
        Some(h) => canonical(h)?,
        None => broker_home,
    };
    let filters = |data: &Path| -> anyhow::Result<Filters> {
        let mut f = Filters::default();
        add_script_filters(
            &mut f,
            install,
            &ScriptDirs {
                home: &home,
                instance: &absolute(&directory),
                etc: &absolute(&etc),
                data: &absolute(data),
                oome: &absolute(&oome),
            },
            "NA",
        )?;
        Ok(f)
    };
    let mut f = filters(&data)?;
    let w = TmpWriter { nix };

    if windows {
        // Saranga has no Windows service wrapper: an Artemis instance's
        // bin/artemis-service.exe and its XML are left as they are.
        let cmd_tmp = tmp.join("artemis.cmd");
        w.write("bin/instance.cmd", &cmd_tmp, &f, false)?;
        upgrade(
            &cmd_tmp,
            &artemis_cmd,
            &bin_bkp.join("artemis.cmd"),
            &["set ARTEMIS_INSTANCE_ETC="],
            &[],
        )?;
        saranga_script(&artemis_cmd, &bin.join("saranga.cmd"), &bin_bkp)?;

        let profile = etc.join("artemis.profile.cmd");
        let profile_tmp = tmp.join("artemis.profile.cmd");
        w.write("etc/artemis.profile.cmd", &profile_tmp, &f, false)?;
        upgrade_jdk(
            JDK_PREFIX_WINDOWS,
            "",
            &profile_tmp,
            &profile,
            &etc_bkp.join("artemis.profile.cmd"),
            &[],
            &[
                "set ARTEMIS_INSTANCE=\"",
                "set ARTEMIS_DATA_DIR=",
                "set ARTEMIS_OOME_DUMP=",
            ],
        )?;

        let utility = etc.join("artemis-utility.profile.cmd");
        if utility.exists() {
            let utility_tmp = tmp.join("artemis-utility.profile.cmd");
            w.write("etc/artemis-utility.profile.cmd", &utility_tmp, &f, false)?;
            upgrade_jdk(
                JDK_PREFIX_WINDOWS,
                "",
                &utility_tmp,
                &utility,
                &etc_bkp.join("artemis-utility.profile.cmd"),
                &[],
                &["set ARTEMIS_INSTANCE=\"", "set ARTEMIS_DATA_DIR="],
            )?;
        } else {
            if install.data == "data" {
                data = path_from_file(data, &profile, "set ARTEMIS_DATA_DIR=", "DATA")?;
                f = filters(&data)?;
            }
            println!("Creating {}", utility.display());
            w.write("etc/artemis-utility.profile.cmd", &utility, &f, false)?;
        }
    }

    if nix {
        let script_tmp = tmp.join("artemis");
        w.write("bin/instance.sh", &script_tmp, &f, true)?;
        upgrade(
            &script_tmp,
            &artemis_script,
            &bin_bkp.join("artemis"),
            &["ARTEMIS_INSTANCE_ETC="],
            &[],
        )?;
        saranga_script(&artemis_script, &bin.join("saranga"), &bin_bkp)?;

        let service = bin.join("artemis-service");
        let service_tmp = tmp.join("artemis-service");
        w.write("bin/artemis-service", &service_tmp, &f, true)?;
        upgrade(
            &service_tmp,
            &service,
            &bin_bkp.join("artemis-service"),
            &[],
            &[],
        )?;

        let hawtio = &[("HAWTIO_ROLES=", "HAWTIO_ROLE=")];
        let profile = etc.join("artemis.profile");
        let profile_tmp = tmp.join("artemis.profile");
        w.write("etc/artemis.profile", &profile_tmp, &f, true)?;
        upgrade_jdk(
            JDK_PREFIX_LINUX,
            "\"",
            &profile_tmp,
            &profile,
            &etc_bkp.join("artemis.profile"),
            hawtio,
            &[
                "ARTEMIS_INSTANCE=",
                "ARTEMIS_DATA_DIR=",
                "ARTEMIS_OOME_DUMP=",
                "HAWTIO_ROLES=",
            ],
        )?;

        let utility = etc.join("artemis-utility.profile");
        if utility.exists() {
            let utility_tmp = tmp.join("artemis-utility.profile");
            w.write("etc/artemis-utility.profile", &utility_tmp, &f, true)?;
            upgrade_jdk(
                JDK_PREFIX_LINUX,
                "\"",
                &utility_tmp,
                &utility,
                &etc_bkp.join("artemis-utility.profile"),
                hawtio,
                &["ARTEMIS_INSTANCE=", "ARTEMIS_DATA_DIR="],
            )?;
        } else {
            if install.data == "data" {
                data = path_from_file(data, &profile, "ARTEMIS_DATA_DIR=", "DATA")?;
                f = filters(&data)?;
            }
            println!("Creating {}", utility.display());
            w.write("etc/artemis-utility.profile", &utility, &f, true)?;
        }
    }

    let bootstrap = etc.join("bootstrap.xml");
    let bootstrap_tmp = tmp.join("bootstrap.xml");
    std::fs::copy(&bootstrap, &bootstrap_tmp)
        .with_context(|| format!("copying {}", bootstrap.display()))?;
    do_upgrade(
        &bootstrap_tmp,
        &bootstrap,
        &etc_bkp.join("bootstrap.xml"),
        |line| Some(replace_bootstrap_line(line)),
    )?;
    remove_wars(&bootstrap)?;
    upgrade_logging(&etc, &etc_bkp)?;

    println!();
    println!("{stars}");
    Ok(())
}

/// What Java's `File` makes of a path: the platform's separators.
fn java_file(p: &Path) -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(p.to_string_lossy().replace('/', "\\"))
    } else {
        p.to_path_buf()
    }
}

/// `File.getAbsolutePath`.
fn absolute(p: &Path) -> PathBuf {
    std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf())
}

/// The platform's line separator, as `PrintStream.println` writes it.
const LINE_SEPARATOR: &str = if cfg!(windows) { "\r\n" } else { "\n" };

/// `InstallAbstract.write` to a file of the backup's `tmp/`.
struct TmpWriter {
    nix: bool,
}

impl TmpWriter {
    fn write(
        &self,
        source: &str,
        target: &Path,
        f: &Filters,
        unix_target: bool,
    ) -> anyhow::Result<()> {
        let content = f.apply(template_text(source)?);
        let separator = if unix_target && self.nix {
            "\n"
        } else {
            LINE_SEPARATOR
        };
        let content = content.replace("\r\n", "\n").replace('\n', separator);
        std::fs::write(target, content).with_context(|| format!("writing {}", target.display()))
    }
}

/// `findBackup`: the first free `old-config-bkp.<n>`.
fn find_backup(directory: &Path) -> anyhow::Result<PathBuf> {
    for n in 0..10 {
        let dir = directory.join(format!("old-config-bkp.{n}"));
        if !dir.exists() {
            std::fs::create_dir(&dir)?;
            println!(
                "Using {} as a backup folder for the modified files",
                absolute(&dir).display()
            );
            return Ok(dir);
        }
    }
    bail!(UsageError(
        "Too many backup folders in place already. Please remove some of the old-config-bkp.* folders"
            .to_owned()
    ))
}

/// `getPathFromFile`: the path a script's `prefix` line sets, its quotes
/// dropped, else `default`.
fn path_from_file(
    default: PathBuf,
    file: &Path,
    prefix: &str,
    name: &str,
) -> anyhow::Result<PathBuf> {
    let Ok(text) = std::fs::read_to_string(file) else {
        return Ok(default);
    };
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with(prefix) && t.len() > prefix.len() + 1 {
            let path = java_file(Path::new(&t[prefix.len() + 1..t.len() - 1]));
            println!("{name} found as {}", path.display());
            return Ok(path);
        }
    }
    Ok(default)
}

/// The lines of `target`, which `doUpgrade` shows its caller first.
fn original_lines(target: &Path) -> anyhow::Result<Vec<String>> {
    let text =
        std::fs::read_to_string(target).with_context(|| format!("reading {}", target.display()))?;
    Ok(text.lines().map(str::to_owned).collect())
}

/// `doUpgrade`: `target` backed up to `bkp`, then rewritten from `tmp`'s
/// lines through `update` (`None` drops one).
fn do_upgrade(
    tmp: &Path,
    target: &Path,
    bkp: &Path,
    mut update: impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<()> {
    println!("Copying {} to {}", target.display(), bkp.display());
    std::fs::copy(target, bkp).with_context(|| format!("copying {}", target.display()))?;
    println!("Updating {}", target.display());
    let mut out = String::new();
    for line in std::fs::read_to_string(tmp)?.lines() {
        if let Some(l) = update(line) {
            out.push_str(&l);
            out.push_str(LINE_SEPARATOR);
        }
    }
    std::fs::write(target, out).with_context(|| format!("writing {}", target.display()))
}

/// The line of `keeping` whose prefix `line` (trimmed) starts with, under
/// its current name: an old name in `alternates` is renamed.
fn kept<'a>(
    line: &str,
    keeping: &[&'a str],
    alternates: &[(&str, &str)],
    mut keep: impl FnMut(&'a str, String),
) {
    let t = line.trim();
    for prefix in keeping {
        if t.starts_with(prefix) {
            keep(prefix, line.to_owned());
        } else if let Some((_, old)) = alternates.iter().find(|(k, _)| k == prefix)
            && t.starts_with(old)
        {
            keep(prefix, line.replacen(old, prefix, 1));
        }
    }
}

/// `upgrade`: the new file, with the old file's lines for `keeping` (a
/// prefix the old file lacks drops its line).
fn upgrade(
    tmp: &Path,
    target: &Path,
    bkp: &Path,
    keeping: &[&str],
    alternates: &[(&str, &str)],
) -> anyhow::Result<()> {
    let mut old: HashMap<&str, String> = HashMap::new();
    for line in original_lines(target)? {
        kept(&line, keeping, alternates, |p, l| {
            old.insert(p, l);
        });
    }
    do_upgrade(tmp, target, bkp, |line| {
        match keeping.iter().find(|p| line.trim().starts_with(**p)) {
            Some(p) => old.get(p).cloned(),
            None => Some(line.to_owned()),
        }
    })
}

/// `upgradeJDK`: `upgrade`, with the JVM arguments of
/// `KEEPING_JVM_ARGUMENTS` kept from the old `JAVA_ARGS` line.
fn upgrade_jdk(
    jdk_prefix: &str,
    end_of_line: &str,
    tmp: &Path,
    target: &Path,
    bkp: &Path,
    alternates: &[(&str, &str)],
    keeping: &[&str],
) -> anyhow::Result<()> {
    let mut old: HashMap<&str, String> = HashMap::new();
    let mut args: HashMap<&str, String> = HashMap::new();
    for line in original_lines(target)? {
        if line.trim().starts_with(jdk_prefix) {
            parse_original_args(jdk_prefix, end_of_line, &line, &mut args);
        } else {
            kept(&line, keeping, alternates, |p, l| {
                old.insert(p, l);
            });
        }
    }
    do_upgrade(tmp, target, bkp, |line| {
        if line.trim().starts_with(jdk_prefix) {
            Some(parse_new_line(jdk_prefix, end_of_line, line, &args))
        } else {
            match keeping.iter().find(|p| line.trim().starts_with(**p)) {
                Some(p) => old.get(p).cloned(),
                None => Some(line.to_owned()),
            }
        }
    })
}

/// The arguments between `prefix` and `end` of a trimmed line, split on
/// spaces as Java's `split(" ")` splits: trailing empty strings dropped.
fn jvm_args<'a>(prefix: &str, end: &str, line: &'a str) -> Vec<&'a str> {
    let t = line.trim();
    let mut args: Vec<&str> = t
        .get(prefix.len()..t.len().saturating_sub(end.len()))
        .unwrap_or_default()
        .split(' ')
        .collect();
    while args.last() == Some(&"") {
        args.pop();
    }
    args
}

/// `JVMArgumentParser.parseOriginalArgs`.
fn parse_original_args(
    prefix: &str,
    end: &str,
    line: &str,
    args: &mut HashMap<&'static str, String>,
) {
    for s in jvm_args(prefix, end, line) {
        for k in KEEPING_JVM_ARGUMENTS {
            if s.starts_with(k) {
                args.insert(k, s.to_owned());
            } else if let Some((_, old)) = KEEPING_JVM_ARGUMENTS_ALTERNATES
                .iter()
                .find(|(n, _)| n == k)
                && s.starts_with(old)
            {
                args.insert(k, s.replacen(old, k, 1));
            }
        }
    }
}

/// `JVMArgumentParser.parseNewLine`: the new line's arguments, those kept
/// taken from the old line, each followed by a space.
fn parse_new_line(prefix: &str, end: &str, line: &str, args: &HashMap<&str, String>) -> String {
    let indent = &line[..line.find(prefix).unwrap_or(0)];
    let mut out = String::new();
    for s in jvm_args(prefix, end, line) {
        let value = KEEPING_JVM_ARGUMENTS
            .iter()
            .find(|k| s.starts_with(**k))
            .and_then(|k| args.get(k));
        out.push_str(value.map_or(s, String::as_str));
        out.push(' ');
    }
    format!("{indent}{prefix}{out}{end}")
}

/// `bin/saranga` (or `saranga.cmd`), as `create` writes it beside
/// `bin/artemis`: the upgraded `artemis` script, an older one backed up.
fn saranga_script(artemis: &Path, saranga: &Path, bin_bkp: &Path) -> anyhow::Result<()> {
    if saranga.exists() {
        let bkp = bin_bkp.join(saranga.file_name().context("no file name")?);
        println!("Copying {} to {}", saranga.display(), bkp.display());
        std::fs::copy(saranga, &bkp)?;
        println!("Updating {}", saranga.display());
    } else {
        println!("Creating {}", saranga.display());
    }
    std::fs::copy(artemis, saranga)?;
    make_exec(saranga)
}

/// `^(.*)<needle(rest)$`, `(.*)` greedy: the last `needle` whose rest
/// passes `rest`, split around it.
fn split_last<'a>(
    line: &'a str,
    needle: &str,
    rest: impl Fn(&str) -> bool,
) -> Option<(&'a str, &'a str)> {
    line.rmatch_indices(needle)
        .map(|(i, _)| (&line[..i], &line[i + needle.len()..]))
        .find(|(_, r)| rest(r))
}

/// The `bootstrap.xml` rewrites of `replaceLines`, applied in turn.
fn replace_bootstrap_line(line: &str) -> String {
    let mut l = line.to_owned();
    if let Some((head, _)) = split_last(&l, "<web path", |_| true) {
        l = format!("{head}<web path=\"web\" rootRedirectLocation=\"console\">");
    }
    if let Some((head, tail)) = split_last(&l, "<binding uri=\"http://localhost:8161\"", |_| true) {
        l = format!("{head}<binding name=\"artemis\" uri=\"http://localhost:8161\"{tail}");
    }
    for app in ["branding", "plugin"] {
        if let Some((head, tail)) = split_last(&l, "<app url=", |r| r.contains(app)) {
            l = format!("{head}<app name=\"{app}\" url={tail}");
        }
    }
    let quoted = |r: &str| r.find('"').is_some_and(|i| i > 0);
    if let Some((head, tail)) = split_last(&l, "<app url=\"", quoted) {
        let end = tail.find('"').unwrap_or_default();
        let (url, rest) = (&tail[..end], &tail[end + 1..]);
        l = format!("{head}<app name=\"{url}\" url=\"{url}\"{rest}");
    }
    l
}

/// `removeWars`: the branding and plugin apps dropped.
fn remove_wars(bootstrap: &Path) -> anyhow::Result<()> {
    let mut out = String::new();
    let mut remove = false;
    for line in std::fs::read_to_string(bootstrap)?.lines() {
        if split_last(line, "<app name=", |r| r.contains("branding")).is_some() {
            println!("removing branding war as no longer needed");
            remove = true;
        } else if split_last(line, "<app name=", |r| r.contains("plugin")).is_some() {
            println!("removing plugin war as no longer needed");
            remove = true;
        } else {
            out.push_str(line);
            out.push_str(LINE_SEPARATOR);
        }
    }
    if remove {
        std::fs::write(bootstrap, out)?;
    }
    Ok(())
}

/// `upgradeLogging`: an old `logging.properties` replaced by
/// `log4j2.properties`, and `log4j2-utility.properties` added if missing.
fn upgrade_logging(etc: &Path, etc_bkp: &Path) -> anyhow::Result<()> {
    let old = etc.join("logging.properties");
    if old.exists() {
        let copy = etc_bkp.join("logging.properties");
        println!("Copying {} to {}", old.display(), copy.display());
        std::fs::copy(&old, &copy)?;
        println!("Removing {}", old.display());
        if std::fs::remove_file(&old).is_err() {
            println!("{} could not be removed!", old.display());
        }
        let new = etc.join("log4j2.properties");
        if !new.exists() {
            println!("Creating {}", new.display());
            std::fs::write(&new, template_text("etc/log4j2.properties")?)?;
        }
    }
    let utility = etc.join("log4j2-utility.properties");
    if !utility.exists() {
        println!("Creating {}", utility.display());
        std::fs::write(&utility, template_text("etc/log4j2-utility.properties")?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{parse_new_line, parse_original_args, replace_bootstrap_line};

    #[test]
    fn bootstrap_lines() {
        assert_eq!(
            replace_bootstrap_line(r#"   <web path="web">"#),
            r#"   <web path="web" rootRedirectLocation="console">"#
        );
        assert_eq!(
            replace_bootstrap_line(r#"       <binding uri="http://localhost:8161">"#),
            r#"       <binding name="artemis" uri="http://localhost:8161">"#
        );
        assert_eq!(
            replace_bootstrap_line(
                r#"  <app url="activemq-branding" war="activemq-branding.war"/>"#
            ),
            r#"  <app name="branding" url="activemq-branding" war="activemq-branding.war"/>"#
        );
        assert_eq!(
            replace_bootstrap_line(r#"  <app url="console" war="console.war"/>"#),
            r#"  <app name="console" url="console" war="console.war"/>"#
        );
        let current = r#"  <app name="console" url="console" war="console.war"/>"#;
        assert_eq!(replace_bootstrap_line(current), current);
    }

    #[test]
    fn jvm_arguments_kept() {
        let mut args2 = HashMap::new();
        parse_original_args(
            "JAVA_ARGS=",
            "\"",
            "JAVA_ARGS=\"-Xms1G -Xmx4G -Dhawtio.role=amq\"",
            &mut args2,
        );
        assert_eq!(args2.get("-Xmx").map(String::as_str), Some("-Xmx4G"));
        assert_eq!(
            args2.get("-Dhawtio.roles=").map(String::as_str),
            Some("-Dhawtio.roles=amq")
        );
        assert_eq!(
            parse_new_line(
                "JAVA_ARGS=",
                "\"",
                "    JAVA_ARGS=\"-Xms512M -Xmx2G -Dy=2\"",
                &args2
            ),
            "    JAVA_ARGS=\"-Xms512M -Xmx4G -Dy=2 \""
        );
    }
}
