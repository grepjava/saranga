//! wf_probe [--stax] <files...>: each file's well-formedness as the DOM
//! (or StAX) probe of Artemis prints it.

use saranga_config::xerces::{Failure, check_dom, check_stax};

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let stax = args.first().is_some_and(|a| a == "--stax");
    if stax {
        args.remove(0);
    }
    let dir = std::env::current_dir().unwrap();
    for f in args {
        let bytes = std::fs::read(&f).unwrap();
        let r = if stax {
            match check_stax(&bytes, dir.clone()) {
                Ok(_) => "OK".to_string(),
                Err(Failure::Parse(p)) => format!(
                    "{} PE [row,col]:[{},{}] Message: {}",
                    if p.at_init { "ERROR" } else { "INVALID" },
                    p.line,
                    p.column,
                    p.message
                ),
                Err(Failure::Io {
                    message,
                    line,
                    column,
                    ..
                }) => {
                    format!("INVALID PE [row,col]:[{line},{column}] Message: {message}")
                }
                Err(Failure::Xni {
                    message,
                    line,
                    column,
                }) => {
                    format!("INVALID PE [row,col]:[{line},{column}] Message: {message}")
                }
                Err(Failure::Runtime { class, message }) => format!("ERROR {class}: {message}"),
            }
        } else {
            let text = String::from_utf8_lossy(&bytes);
            match check_dom(&text, dir.clone()) {
                Ok(printed) => format!("OK{}", stderr(&printed.printed)),
                Err(Failure::Parse(p)) => {
                    format!(
                        "FATAL {}:{} {}{}",
                        p.line,
                        p.column,
                        p.message,
                        stderr(&p.printed)
                    )
                }
                Err(Failure::Io { class, message, .. }) => format!("ERROR {class}: {message}"),
                Err(Failure::Xni { message, .. }) => {
                    format!("ERROR org.xml.sax.SAXException: {message}")
                }
                Err(Failure::Runtime { class, message }) => format!("ERROR {class}: {message}"),
            }
        };
        println!("{f}\t{}", r.replace('\r', "\\r").replace('\n', "\\n"));
    }
}

/// What the default error handler printed, as the Java probe shows it.
fn stderr(printed: &[String]) -> String {
    if printed.is_empty() {
        return String::new();
    }
    format!(
        "\t{}",
        printed
            .iter()
            .map(|l| format!("{l}\r\n"))
            .collect::<String>()
    )
}
