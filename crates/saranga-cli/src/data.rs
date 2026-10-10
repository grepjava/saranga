//! `saranga data`: tools on a broker's data directory (`artemis data`).

use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use saranga_storage::describe::{PrintDataOptions, print_data};

#[derive(Subcommand)]
pub enum DataCommand {
    /// Prints the bindings and message journals, as `artemis data print`
    /// does. Don't use it on the data of a running broker.
    Print(PrintArgs),
}

#[derive(Args)]
pub struct PrintArgs {
    /// The folder used for messages (the message journal).
    #[arg(long)]
    journal: PathBuf,
    /// The folder used for bindings.
    #[arg(long)]
    bindings: PathBuf,
    /// The folder used for paged messages.
    #[arg(long)]
    paging: Option<PathBuf>,
    /// The folder used for large messages (not read by `data print`).
    #[arg(long)]
    large_messages: Option<PathBuf>,
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

pub fn run(cmd: DataCommand) -> anyhow::Result<()> {
    match cmd {
        DataCommand::Print(a) => {
            let _ = a.large_messages;
            let opts = PrintDataOptions {
                safe: a.safe,
                reclaimed: a.reclaimed,
                skip_bindings: a.skip_bindings,
                skip_journal: a.skip_journal,
                max_pages: u32::try_from(a.max_pages).ok(),
                print_pages: Some(saranga_paging::print::print_pages),
            };
            let paging = a
                .paging
                .unwrap_or_else(|| a.journal.with_file_name("paging"));
            // DataAbstract.checkIfDirectoryExists.
            for dir in [&a.bindings, &a.journal, &paging] {
                if !dir.exists() {
                    anyhow::bail!(
                        "Could not find folder: {}, please pass --bindings, --journal and --paging as arguments",
                        dir.display()
                    );
                }
            }
            let stdout = std::io::stdout();
            let mut out = std::io::BufWriter::new(stdout.lock());
            print_data(&mut out, &a.bindings, &a.journal, &paging, &opts)?;
            out.flush()?;
            Ok(())
        }
    }
}
