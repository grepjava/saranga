//! `address create|delete|update|show` (`AddressGroup`).

use saranga_server::management::json::Json;

use super::ConnectionArgs;
use super::management::perform;

#[derive(clap::Subcommand, Debug)]
pub enum AddressCommand {
    /// Create an address.
    Create(AddressArgs),
    /// Delete an address.
    Delete {
        #[command(flatten)]
        args: AddressArgs,
        /// Delete the address even if it has queues. All messages in those
        /// queues will be deleted! Default: false.
        #[arg(long)]
        force: bool,
    },
    /// Update an address.
    Update(AddressArgs),
    /// Show the selected address.
    Show {
        #[command(flatten)]
        args: AddressArgs,
        /// Show the bindings for this address.
        #[arg(long)]
        bindings: bool,
    },
}

/// `AddressAbstract`'s options.
#[derive(clap::Args, Debug)]
pub struct AddressArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// The address's name.
    #[arg(long)]
    name: Option<String>,
    /// Whether the address supports anycast queues.
    #[arg(long)]
    anycast: bool,
    /// Whether the address won't support anycast queues.
    #[arg(long = "no-anycast")]
    no_anycast: bool,
    /// Whether the address supports multicast queues.
    #[arg(long)]
    multicast: bool,
    /// Whether the address won't support multicast queues.
    #[arg(long = "no-multicast")]
    no_multicast: bool,
}

impl AddressArgs {
    fn name(&mut self, require_input: bool) -> anyhow::Result<Option<String>> {
        if self.name.is_none() && require_input {
            self.name =
                self.conn
                    .common
                    .input("--name", "What is the name of the address?", None)?;
        }
        Ok(self.name.clone())
    }

    fn is_anycast(&self) -> anyhow::Result<bool> {
        Ok(if self.no_anycast {
            false
        } else {
            self.anycast
                || self.conn.common.input_boolean(
                    "--anycast",
                    "Will this address support anycast queues?",
                    false,
                )?
        })
    }

    fn is_multicast(&self) -> anyhow::Result<bool> {
        Ok(if self.no_multicast {
            false
        } else {
            self.multicast
                || self.conn.common.input_boolean(
                    "--multicast",
                    "Will this address support multicast queues?",
                    true,
                )?
        })
    }

    /// `getRoutingTypes`: `ANYCAST,MULTICAST`, either, or (none) the
    /// default or nothing.
    fn routing_types(&self, use_default: bool) -> anyhow::Result<Option<String>> {
        let anycast = self.is_anycast()?;
        let multicast = self.is_multicast()?;
        Ok(match (anycast, multicast) {
            (true, true) => Some("ANYCAST,MULTICAST".to_owned()),
            (true, false) => Some("ANYCAST".to_owned()),
            (false, true) => Some("MULTICAST".to_owned()),
            (false, false) => use_default.then(|| "MULTICAST".to_owned()),
        })
    }
}

fn opt(s: Option<String>) -> Json {
    s.map(Json::from).unwrap_or(Json::Null)
}

pub async fn run(cmd: AddressCommand) -> anyhow::Result<()> {
    match cmd {
        AddressCommand::Create(mut args) => {
            args.conn.resolve(false)?;
            let name = args.name(true)?;
            let params = vec![opt(name.clone()), opt(args.routing_types(true)?)];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "createAddress", params).await
            })
            .await?;
            if reply.succeeded {
                println!("{} created successfully.", reply.string());
            } else {
                eprintln!(
                    "Failed to create address {}. Reason: {}",
                    name.as_deref().unwrap_or("null"),
                    reply.string()
                );
            }
        }
        AddressCommand::Update(mut args) => {
            args.conn.resolve(false)?;
            let name = args.name(true)?;
            let params = vec![opt(name.clone()), opt(args.routing_types(false)?)];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "updateAddress", params).await
            })
            .await?;
            if reply.succeeded {
                println!("{} updated successfully.", reply.string());
            } else {
                eprintln!(
                    "Failed to update address {}. Reason: {}",
                    name.as_deref().unwrap_or("null"),
                    reply.string()
                );
            }
        }
        AddressCommand::Delete { mut args, force } => {
            args.conn.resolve(false)?;
            let name = args.name(true)?;
            let params = vec![opt(name.clone()), Json::from(force)];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "deleteAddress", params).await
            })
            .await?;
            let name = name.as_deref().unwrap_or("null");
            if reply.succeeded {
                println!("Address {name} deleted successfully.");
            } else {
                eprintln!(
                    "Failed to delete address {name}. Reason: {}",
                    reply.string()
                );
            }
        }
        AddressCommand::Show { mut args, bindings } => {
            args.conn.resolve(false)?;
            let name = args.name(false)?;
            let reply = perform(&args.conn, async |r| match &name {
                None => {
                    r.invoke("broker", "listAddresses", vec![Json::from("\n")])
                        .await
                }
                Some(n) if bindings => {
                    r.invoke(
                        "broker",
                        "listBindingsForAddress",
                        vec![Json::from(n.clone())],
                    )
                    .await
                }
                Some(n) => {
                    r.invoke("broker", "getAddressInfo", vec![Json::from(n.clone())])
                        .await
                }
            })
            .await?;
            if reply.succeeded {
                println!("{}", reply.string());
            } else {
                eprintln!(
                    "Failed to show address {}. Reason: {}",
                    name.as_deref().unwrap_or("null"),
                    reply.string()
                );
            }
        }
    }
    Ok(())
}
