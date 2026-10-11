//! `user add|list|reset|rm` (`UserGroup`): the broker's users, through
//! management.

use saranga_server::management::json::Json;

use super::ConnectionArgs;
use super::management::perform;

#[derive(clap::Subcommand, Debug)]
pub enum UserCommand {
    /// Add a user.
    Add {
        #[command(flatten)]
        args: UserArgs,
        /// The password to use for the chosen user command. Default: input.
        #[arg(long = "user-command-password")]
        command_password: Option<String>,
        /// Store the password in plaintext. Default: false.
        #[arg(long)]
        plaintext: bool,
    },
    /// List user(s).
    List(UserArgs),
    /// Reset user's password or roles.
    Reset {
        #[command(flatten)]
        args: UserArgs,
        /// The password to use for the chosen user command. Default: input.
        #[arg(long = "user-command-password")]
        command_password: Option<String>,
        /// Store the password in plaintext. Default: false.
        #[arg(long)]
        plaintext: bool,
    },
    /// Remove an existing user.
    Rm(UserArgs),
}

/// `UserAction`'s options.
#[derive(clap::Args, Debug)]
pub struct UserArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// The user's role(s). Separate multiple roles with comma.
    #[arg(long)]
    role: Option<String>,
    /// The username to use for the chosen user command. Default: input.
    #[arg(long = "user-command-user")]
    command_user: Option<String>,
}

impl UserArgs {
    fn input_user(&mut self) {
        if self.command_user.is_none() {
            self.command_user = self.conn.common.input(
                "--user-command-user",
                "Please provide the username to use for the chosen user command:",
                None,
            );
        }
    }

    fn input_role(&mut self) {
        if self.role.is_none() {
            self.role = self.conn.common.input(
                "--role",
                "What is the user's role(s)? Separate multiple roles with comma.",
                None,
            );
        }
    }

    fn input_password(&self, password: Option<String>) -> anyhow::Result<String> {
        match password {
            Some(p) => Ok(p),
            None => self.conn.common.input_password(
                "--user-command-password",
                "What is the password to use for the chosen user command?",
            ),
        }
    }

    fn user_name(&self) -> &str {
        self.command_user.as_deref().unwrap_or("null")
    }
}

fn opt(s: Option<&str>) -> Json {
    s.map(Json::from).unwrap_or(Json::Null)
}

pub async fn run(cmd: UserCommand) -> anyhow::Result<()> {
    match cmd {
        UserCommand::Add {
            mut args,
            command_password: password,
            plaintext,
        } => {
            args.conn.resolve(false)?;
            args.input_user();
            let password = args.input_password(password)?;
            args.input_role();
            let params = vec![
                opt(args.command_user.as_deref()),
                Json::from(password),
                opt(args.role.as_deref()),
                Json::from(plaintext),
            ];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "addUser", params).await
            })
            .await?;
            if reply.succeeded {
                println!("{} added successfully.", args.user_name());
            } else {
                eprintln!(
                    "Failed to add user {}. Reason: {}",
                    args.user_name(),
                    reply.string()
                );
            }
        }
        UserCommand::Reset {
            mut args,
            command_password: password,
            plaintext,
        } => {
            args.conn.resolve(false)?;
            args.input_user();
            let password = args.input_password(password)?;
            let params = vec![
                opt(args.command_user.as_deref()),
                Json::from(password),
                opt(args.role.as_deref()),
                Json::from(plaintext),
            ];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "resetUser", params).await
            })
            .await?;
            if reply.succeeded {
                println!("{} reset successfully.", args.user_name());
            } else {
                eprintln!(
                    "Failed to reset user {}. Reason: {}",
                    args.user_name(),
                    reply.string()
                );
            }
        }
        UserCommand::Rm(mut args) => {
            args.conn.resolve(false)?;
            args.input_user();
            let params = vec![opt(args.command_user.as_deref())];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "removeUser", params).await
            })
            .await?;
            if reply.succeeded {
                println!("{} removed successfully.", args.user_name());
            } else {
                eprintln!(
                    "Failed to remove user {}. Reason: {}",
                    args.user_name(),
                    reply.string()
                );
            }
        }
        UserCommand::List(mut args) => {
            args.conn.resolve(false)?;
            let params = vec![opt(args.command_user.as_deref())];
            let reply = perform(&args.conn, async |r| {
                r.invoke("broker", "listUser", params).await
            })
            .await?;
            if !reply.succeeded {
                eprintln!(
                    "Failed to list user {}. Reason: {}",
                    args.user_name(),
                    reply.string()
                );
                return Ok(());
            }
            let mut out = String::from("--- \"user\"(roles) ---\n");
            let mut count = 0;
            if let Ok(Json::Array(users)) = Json::parse(&reply.string()) {
                for user in &users {
                    let name = user.get("username").and_then(Json::as_str).unwrap_or("");
                    let roles: Vec<&str> = match user.get("roles") {
                        Some(Json::Array(r)) => r.iter().filter_map(Json::as_str).collect(),
                        _ => Vec::new(),
                    };
                    out.push_str(&format!("\"{name}\"({})\n", roles.join(",")));
                    count += 1;
                }
            }
            out.push_str(&format!("\n Total: {count}"));
            println!("{out}");
        }
    }
    Ok(())
}
