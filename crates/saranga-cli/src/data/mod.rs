//! `saranga data`: tools on a broker's data directory (`artemis data`).

use std::io::Write;

mod compact;
pub mod dirs;
mod export;
mod import;
mod journal;
mod recover;
mod xml;

use clap::{Args, Subcommand};
use saranga_storage::describe::{PrintDataOptions, print_data};

#[derive(Subcommand)]
pub enum DataCommand {
    /// Prints the bindings and message journals, as `artemis data print`
    /// does. Don't use it on the data of a running broker.
    Print(PrintArgs),
    /// Compact the journal of a non running server.
    Compact(dirs::DataDirArgs),
    /// Export all message-data using an XML that could be interpreted by
    /// any system.
    Exp(export::ExportArgs),
    /// Import all message-data using an XML that could be interpreted by
    /// any system.
    Imp(import::ImportArgs),
    /// Encode a set of journal files into an internal encoded data format.
    Encode(journal::JournalArgs),
    /// Decode a journal's internal format into a new set of journal files.
    Decode(journal::DecodeArgs),
    /// Recover (undelete) every message on the journal by creating a new
    /// output journal. Rolled back and acked messages will be sent out to
    /// the output as much as possible.
    Recover(recover::RecoverArgs),
}

#[derive(Args)]
pub struct PrintArgs {
    #[command(flatten)]
    dirs: dirs::DataDirArgs,
    /// Print the data structure without showing your data.
    #[arg(long)]
    safe: bool,
    /// Try to print as many records as possible from reclaimed files.
    #[arg(long)]
    reclaimed: bool,
    /// Maximum number of pages to read; -1 is unlimited.
    #[arg(long, default_value_t = -1, allow_negative_numbers = true)]
    max_pages: i64,
    /// Do not print data from the bindings journal.
    #[arg(long)]
    skip_bindings: bool,
    /// Do not print data from the messages journal.
    #[arg(long)]
    skip_journal: bool,
}

pub async fn run(cmd: DataCommand) -> anyhow::Result<()> {
    match cmd {
        DataCommand::Compact(a) => treat_error(compact::run(&a)),
        DataCommand::Exp(a) => treat_error(export::run(a)),
        DataCommand::Imp(a) => import::run(a).await,
        DataCommand::Encode(a) => treat_error(journal::encode(&a)),
        DataCommand::Decode(a) => treat_error(journal::decode(&a)),
        DataCommand::Recover(a) => treat_error(recover::run(&a)),
        DataCommand::Print(a) => treat_error(print(&a)),
    }
}

/// `Configurable.treatError`: what fails in the tool is reported on stderr
/// between blank lines, and the command still succeeds. A held lock (a
/// `CLIException`) and the directories `getParameterConfiguration` finds
/// missing (a `RuntimeException`) are thrown before the tool runs, so they
/// end the command.
fn treat_error(result: anyhow::Result<()>) -> anyhow::Result<()> {
    match result {
        Err(e)
            if e.downcast_ref::<crate::client::CliError>().is_none()
                && e.downcast_ref::<crate::client::UsageError>().is_none() =>
        {
            eprintln!();
            eprintln!("Error:{e:#}");
            eprintln!();
            Ok(())
        }
        r => r,
    }
}

/// The directories of `print` and `recover`, which `getParameterConfiguration`
/// checks before the tool runs.
fn parameter_dirs(args: &dirs::DataDirArgs) -> anyhow::Result<dirs::DataDirs> {
    args.resolve()
        .map_err(|e| crate::client::UsageError(format!("{e:#}")).into())
}

fn print(a: &PrintArgs) -> anyhow::Result<()> {
    let _lock = dirs::CliLock::take(crate::client::instance_dir().as_deref())?;
    let d = parameter_dirs(&a.dirs)?;
    let opts = PrintDataOptions {
        safe: a.safe,
        reclaimed: a.reclaimed,
        skip_bindings: a.skip_bindings,
        skip_journal: a.skip_journal,
        max_pages: u32::try_from(a.max_pages).ok(),
        print_pages: Some(saranga_paging::print::print_pages),
    };
    let (bindings, journal, paging) = (d.bindings, d.journal, d.paging);
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    saranga_protocol_amqp::describe::register();
    print_data(&mut out, &bindings, &journal, &paging, &opts)?;
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tools_errors_are_treated_but_not_what_is_thrown_before() {
        assert!(treat_error(Err(anyhow::anyhow!("broken"))).is_ok());
        let lock = treat_error(Err(crate::client::CliError("held".into()).into()));
        assert!(lock.is_err());
        let usage = treat_error(Err(crate::client::UsageError("missing".into()).into()));
        assert!(usage.is_err());
    }
}
