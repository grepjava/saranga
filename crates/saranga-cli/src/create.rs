//! `saranga create`: a broker instance as `artemis create` makes one
//! (`Create`, `InstallAbstract`): the same `etc/` files from the same
//! templates, `data/`, `log/`, `tmp/` and `lib/`, and in `bin/` an
//! `artemis` (and `saranga`) script that runs this `saranga` for the
//! instance, so the instance's scripts and runbooks work as they are.

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

use crate::client::{Common, UsageError};
use crate::perf::journal::{decimal_format, sync_test, to_nanos};

macro_rules! template {
    ($name:literal) => {
        ($name, include_str!(concat!("../templates/", $name)))
    };
}

/// The templates, by their path under the instance.
const TEMPLATES: &[(&str, &str)] = &[
    template!("etc/amqp-acceptor.txt"),
    template!("etc/artemis-roles.properties"),
    template!("etc/artemis-users.properties"),
    template!("etc/artemis-utility.profile"),
    template!("etc/artemis-utility.profile.cmd"),
    template!("etc/artemis.profile"),
    template!("etc/artemis.profile.cmd"),
    template!("etc/basic-security-manager.txt"),
    template!("etc/bootstrap-web-settings.txt"),
    template!("etc/bootstrap.xml"),
    template!("etc/broker.xml"),
    template!("etc/cluster-security-settings.txt"),
    template!("etc/cluster-settings.txt"),
    template!("etc/cluster-static-settings.txt"),
    template!("etc/commented-ping-settings.txt"),
    template!("etc/global-max-default.txt"),
    template!("etc/global-max-specified.txt"),
    template!("etc/hornetq-acceptor.txt"),
    template!("etc/jaas-security-manager.txt"),
    template!("etc/jolokia-access.xml"),
    template!("etc/journal-buffer-settings.txt"),
    template!("etc/log4j2-utility.properties"),
    template!("etc/log4j2.properties"),
    template!("etc/login-with-guest.config"),
    template!("etc/login-without-guest.config"),
    template!("etc/management.xml"),
    template!("etc/mqtt-acceptor.txt"),
    template!("etc/page-sync-settings.txt"),
    template!("etc/ping-settings.txt"),
    template!("etc/replicated-backup-settings.txt"),
    template!("etc/replicated-primary-settings.txt"),
    template!("etc/shared-store-settings.txt"),
    template!("etc/stomp-acceptor.txt"),
    template!("bin/artemis-service"),
    template!("bin/instance.sh"),
    template!("bin/instance.cmd"),
];

pub(crate) fn template_text(name: &str) -> anyhow::Result<&'static str> {
    TEMPLATES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, t)| *t)
        .with_context(|| format!("could not find resource {name}"))
}

/// The placeholders, replaced in the order they were added
/// (`applyFilters` over a `LinkedHashMap`).
#[derive(Default)]
pub(crate) struct Filters(Vec<(String, String)>);

impl Filters {
    pub(crate) fn put(&mut self, key: &str, value: impl Into<String>) {
        let value = value.into();
        match self.0.iter_mut().find(|(k, _)| k == key) {
            Some((_, v)) => *v = value,
            None => self.0.push((key.to_owned(), value)),
        }
    }

    pub(crate) fn apply(&self, content: &str) -> String {
        self.0
            .iter()
            .fold(content.to_owned(), |c, (k, v)| c.replace(k.as_str(), v))
    }

    /// `readTextFile`: a template with the placeholders so far.
    pub(crate) fn read(&self, name: &str) -> anyhow::Result<String> {
        Ok(self.apply(template_text(name)?))
    }
}

/// `InstallAbstract`'s options, shared by `create` and `upgrade`.
#[derive(clap::Args, Debug)]
pub struct InstallArgs {
    /// The instance directory to hold the broker's configuration and data.
    /// Path must be writable.
    pub(crate) directory: PathBuf,
    /// Directory where the configuration is located. Paths can be absolute
    /// or relative to the instance directory. Default: etc.
    #[arg(long, default_value = "etc")]
    pub(crate) etc: String,
    /// Directory where the data is stored. Paths can be absolute or
    /// relative to the instance directory. Default: data.
    #[arg(long, default_value = "data")]
    pub(crate) data: String,
    /// Directory written as the profiles' ARTEMIS_HOME. Default:
    /// ARTEMIS_HOME, else the directory above this saranga's.
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    /// The encoding that text files should use. Default: UTF-8 (the only
    /// one saranga writes).
    #[arg(long, default_value = "UTF-8")]
    pub(crate) encoding: String,
    /// Force Windows script creation. Default: based on your actual system.
    #[arg(long)]
    pub(crate) windows: bool,
    /// Force Linux or Cygwin script creation. Default: based on your actual
    /// system.
    #[arg(long, alias = "linux")]
    pub(crate) cygwin: bool,
    /// Extra Java options to be passed to the profile (written to it; not
    /// used by saranga).
    #[arg(long = "java-options", allow_hyphen_values = true)]
    pub(crate) java_options: Vec<String>,
    /// Extra Java options to be passed to the utility profile.
    #[arg(long = "java-utility-options", allow_hyphen_values = true)]
    pub(crate) java_utility_options: Vec<String>,
    /// Define the -Xmx memory parameter in the profile. Default: 2G.
    #[arg(long = "java-memory", default_value = "2G")]
    pub(crate) java_memory: String,
}

impl InstallArgs {
    /// `InstallAbstract.run`: whether the scripts are Windows ones.
    pub(crate) fn is_windows(&self) -> bool {
        if self.windows {
            true
        } else if self.cygwin {
            false
        } else {
            cfg!(windows) && std::env::var("OSTYPE").as_deref() != Ok("cygwin")
        }
    }

    /// `getJavaOptions`: each option followed by a space.
    pub(crate) fn java_options(&self) -> String {
        self.java_options.iter().map(|s| format!("{s} ")).collect()
    }

    /// `getJavaUtilityOptions`.
    pub(crate) fn java_utility_options(&self) -> String {
        self.java_utility_options
            .iter()
            .map(|s| format!("{s} "))
            .collect()
    }
}

#[derive(clap::Args, Debug)]
pub struct CreateArgs {
    #[command(flatten)]
    common: Common,
    #[command(flatten)]
    install: InstallArgs,
    /// Broker's host name. Default: 0.0.0.0.
    #[arg(long)]
    host: Option<String>,
    /// Embedded web server's host name. Default: localhost.
    #[arg(long = "http-host", default_value = "localhost")]
    http_host: String,
    /// Disable strict checking in jolokia-access.xml.
    #[arg(long = "relax-jolokia")]
    relax_jolokia: bool,
    /// A comma separated string to be passed on to the broker config as
    /// network-check-list.
    #[arg(long)]
    ping: Option<String>,
    /// The port number to use for the main 'artemis' acceptor. Default:
    /// 61616.
    #[arg(long = "default-port", default_value_t = 61616)]
    default_port: i32,
    /// Embedded web server's port. Default: 8161.
    #[arg(long = "http-port", default_value_t = 8161)]
    http_port: i32,
    /// Embedded web server's key store path.
    #[arg(long = "ssl-key")]
    ssl_key: Option<String>,
    /// The key store's password.
    #[arg(long = "ssl-key-password")]
    ssl_key_password: Option<String>,
    /// Require client certificate authentication when connecting to the
    /// embedded web server.
    #[arg(long = "use-client-auth")]
    use_client_auth: bool,
    /// The trust store path in case of client authentication.
    #[arg(long = "ssl-trust")]
    ssl_trust: Option<String>,
    /// The trust store's password.
    #[arg(long = "ssl-trust-password")]
    ssl_trust_password: Option<String>,
    /// The name of the broker. Default: same as host name.
    #[arg(long)]
    name: Option<String>,
    /// How much to off-set the ports of every acceptor.
    #[arg(
        long = "port-offset",
        default_value_t = 0,
        allow_negative_numbers = true
    )]
    port_offset: i32,
    /// Overwrite configuration at destination directory.
    #[arg(long)]
    force: bool,
    /// Paged folders should be removed upon leaving page state.
    #[arg(long = "purge-page-folders")]
    purge_page_folders: bool,
    /// Allow connections from users with no security credentials. Opposite
    /// of --require-login. Default: input.
    #[arg(long = "allow-anonymous", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    allow_anonymous: Option<bool>,
    /// Require security credentials from users for connection. Opposite of
    /// --allow-anonymous.
    #[arg(long = "require-login", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    require_login: Option<bool>,
    /// Page messages to disk when address becomes full. Opposite of
    /// --blocking. Default: true.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    paging: Option<bool>,
    /// Block producers when address becomes full. Opposite of --paging.
    /// Default: false.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    blocking: Option<bool>,
    /// Disable auto tuning of the journal-buffer-timeout in broker.xml.
    #[arg(long = "no-autotune")]
    no_autotune: bool,
    /// Disable auto creation for addresses & queues.
    #[arg(long = "no-autocreate", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    no_autocreate: Option<bool>,
    /// Allow automatic creation of addresses & queues. Default: true.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    autocreate: Option<bool>,
    /// Allow automatic deletion of addresses & queues. Default: false.
    #[arg(long)]
    autodelete: bool,
    /// The username. Default: input.
    #[arg(long)]
    user: Option<String>,
    /// The user's password. Default: input.
    #[arg(long)]
    password: Option<String>,
    /// The name for the role created. Default: amq.
    #[arg(long, default_value = "amq")]
    role: String,
    /// Whether to omit the web-server definition from bootstrap.xml.
    #[arg(long = "no-web")]
    no_web: bool,
    /// A comma separated list of queues with the option to specify a
    /// routing type, e.g. --queues myQueue1,myQueue2:multicast.
    /// Routing-type default: anycast.
    #[arg(long)]
    queues: Option<String>,
    /// A comma separated list of addresses with the option to specify a
    /// routing type, e.g. --addresses myAddress1,myAddress2:anycast.
    /// Routing-type default: multicast.
    #[arg(long)]
    addresses: Option<String>,
    /// Set the journal as asyncio.
    #[arg(long)]
    aio: bool,
    /// Set the journal as nio.
    #[arg(long)]
    nio: bool,
    /// Set the journal as mapped.
    #[arg(long)]
    mapped: bool,
    /// Disable message persistence to the journal
    #[arg(long = "disable-persistence")]
    disable_persistence: bool,
    /// Disable the AMQP specific acceptor.
    #[arg(long = "no-amqp-acceptor")]
    no_amqp_acceptor: bool,
    /// Disable the MQTT specific acceptor.
    #[arg(long = "no-mqtt-acceptor")]
    no_mqtt_acceptor: bool,
    /// Disable the STOMP specific acceptor.
    #[arg(long = "no-stomp-acceptor")]
    no_stomp_acceptor: bool,
    /// Disable the HornetQ specific acceptor.
    #[arg(long = "no-hornetq-acceptor")]
    no_hornetq_acceptor: bool,
    /// Disable usage of fdatasync on the journal.
    #[arg(long = "no-fsync")]
    no_fsync: bool,
    /// The block size of the journal's storage device. Default: 4096.
    #[arg(long = "journal-device-block-size", default_value_t = 4096)]
    journal_device_block_size: i32,
    /// Configure journal retention in days. If > 0 then enable
    /// journal-retention-directory from broker.xml allowing replay options.
    #[arg(long = "journal-retention", default_value_t = 0)]
    retention_days: i32,
    /// Maximum number of bytes to keep in the retention directory.
    #[arg(long = "journal-retention-max-bytes")]
    retention_max_bytes: Option<String>,
    /// Maximum amount of memory which message data may consume.
    #[arg(long = "global-max-size")]
    global_max_size: Option<String>,
    /// Maximum number of messages that will be accepted in memory before
    /// using address full policy mode. Default: undefined.
    #[arg(long = "global-max-messages", default_value_t = -1, allow_negative_numbers = true)]
    global_max_messages: i64,
    /// Support advisory messages for the OpenWire protocol.
    #[arg(long = "support-advisory")]
    support_advisory: bool,
    /// Do not register any advisory addresses/queues for the OpenWire
    /// protocol with the broker's management service.
    #[arg(long = "suppress-internal-management-objects")]
    suppress_internal_management_objects: bool,
    /// Which security manager to use - jaas or basic. Default: jaas.
    #[arg(long = "security-manager", default_value = "jaas")]
    security_manager: String,
    /// The journal-max-io value to use when also using the ASYNCIO
    /// journal-type. Default: 4096
    #[arg(long = "journal-max-io", default_value_t = 4096)]
    journal_max_io: i32,
    #[command(flatten)]
    cluster: Clustering,
    #[command(flatten)]
    unsupported: Unsupported,
}

/// `artemis create`'s clustering and shared-store options.
#[derive(clap::Args, Debug)]
struct Clustering {
    /// Enable clustering.
    #[arg(long)]
    clustered: bool,
    /// Number of hops on the cluster configuration.
    #[arg(long = "max-hops", default_value_t = 1)]
    max_hops: i32,
    /// Message load balancing policy for cluster. Default: ON_DEMAND. Valid
    /// values: ON_DEMAND, STRICT, OFF, OFF_WITH_REDISTRIBUTION.
    #[arg(long = "message-load-balancing", default_value = "ON_DEMAND",
          value_parser = ["ON_DEMAND", "STRICT", "OFF", "OFF_WITH_REDISTRIBUTION"])]
    message_load_balancing: String,
    /// Enable broker replication.
    #[arg(long)]
    replicated: bool,
    /// Enable broker shared store.
    #[arg(long = "shared-store")]
    shared_store: bool,
    /// Deprecated for removal. Use 'backup' instead.
    #[arg(long, hide = true)]
    slave: bool,
    /// Be a backup broker. Valid for shared store or replication.
    #[arg(long)]
    backup: bool,
    /// Whether broker shutdown will trigger failover for clients using the
    /// core protocol. Valid only for shared store. Default: false.
    #[arg(long = "failover-on-shutdown")]
    failover_on_shutdown: bool,
    /// The user to use for clustering. Default: input.
    #[arg(long = "cluster-user")]
    cluster_user: Option<String>,
    /// The password to use for clustering. Default: input.
    #[arg(long = "cluster-password")]
    cluster_password: Option<String>,
    /// Cluster node connectors list separated by comma, e.g.
    /// "tcp://server:61616,tcp://server2:61616,tcp://server3:61616".
    #[arg(long = "static-cluster", alias = "staticCluster")]
    static_cluster: Option<String>,
    /// Enable server discovery.
    #[arg(long = "discovery-enabled", hide = true)]
    discovery_enabled: bool,
}

/// `PrintWriter.println`'s line separator.
const LINE_SEPARATOR: &str = if cfg!(windows) { "\r\n" } else { "\n" };

/// `artemis create` options for what Saranga does not do: JDBC storage.
#[derive(clap::Args, Debug)]
struct Unsupported {
    #[arg(long, hide = true)]
    jdbc: bool,
    // The JDBC store's settings, unused without --jdbc.
    #[arg(long = "jdbc-bindings-table-name", hide = true)]
    jdbc_bindings_table_name: Option<String>,
    #[arg(long = "jdbc-message-table-name", hide = true)]
    jdbc_message_table_name: Option<String>,
    #[arg(long = "jdbc-large-message-table-name", hide = true)]
    jdbc_large_message_table_name: Option<String>,
    #[arg(long = "jdbc-page-store-table-name", hide = true)]
    jdbc_page_store_table_name: Option<String>,
    #[arg(long = "jdbc-node-manager-table-name", hide = true)]
    jdbc_node_manager_table_name: Option<String>,
    #[arg(long = "jdbc-connection-url", hide = true)]
    jdbc_connection_url: Option<String>,
    #[arg(long = "jdbc-driver-class-name", hide = true)]
    jdbc_driver_class_name: Option<String>,
    #[arg(long = "jdbc-network-timeout", hide = true)]
    jdbc_network_timeout: Option<String>,
    #[arg(long = "jdbc-lock-renew-period", hide = true)]
    jdbc_lock_renew_period: Option<String>,
    #[arg(long = "jdbc-lock-expiration", hide = true)]
    jdbc_lock_expiration: Option<String>,
}

impl Unsupported {
    fn check(&self) -> anyhow::Result<()> {
        if self.jdbc {
            bail!(
                "saranga create does not support --jdbc: Saranga does not store messages in a database"
            );
        }
        Ok(())
    }
}

/// `Create.run`.
pub fn run(mut args: CreateArgs) -> anyhow::Result<()> {
    args.unsupported.check()?;
    // `-Dartemis.discovery.enabled=true` first in both profiles' options.
    if args.cluster.discovery_enabled {
        let flag = format!(
            "-D{}=true",
            saranga_client::discovery::DISCOVERY_ENABLED_PROPERTY
        );
        args.install.java_options.insert(0, flag.clone());
        args.install.java_utility_options.insert(0, flag);
    }
    if !args.install.encoding.eq_ignore_ascii_case("UTF-8") {
        bail!("saranga create writes UTF-8 files only");
    }
    // `checkDirectory`.
    let directory = &args.install.directory;
    if !directory.exists() {
        std::fs::create_dir_all(directory).map_err(|_| {
            UsageError(format!(
                "Unable to create the path '{}'.",
                directory.display()
            ))
        })?;
    } else if std::fs::metadata(directory)?.permissions().readonly() {
        bail!(UsageError(format!(
            "The path '{}' is not writable.",
            directory.display()
        )));
    }
    let directory = canonical(directory)?;
    let windows = args.install.is_windows();
    let nix = !windows;

    // `setupJournalType`.
    if args.no_fsync
        && !args.mapped
        && args.common.input_boolean(
            "--mapped",
            "Since you disabled syncs, it is recommended to use the Mapped Memory Journal. Do you want to use the Memory Mapped Journal",
            true,
        )?
    {
        args.mapped = true;
        args.nio = false;
        args.aio = false;
    }
    let journal_types = [args.aio, args.nio, args.mapped]
        .iter()
        .filter(|b| **b)
        .count();
    if journal_types > 1 {
        bail!(UsageError(
            "You can only select one journal type (--nio | --aio | --mapped).".to_owned()
        ));
    }
    // Saranga's journal has no libaio; NIO is the default, as Artemis's
    // where libaio is missing.
    let journal_type = if args.aio {
        "ASYNCIO"
    } else if args.mapped {
        "MAPPED"
    } else {
        "NIO"
    };
    // `--require-login` (true) means no anonymous access, before any
    // question.
    let allow_anonymous_given = if args.require_login == Some(true) {
        Some(false)
    } else {
        args.allow_anonymous
    };

    println!(
        "Creating Saranga broker instance at: {}",
        directory.display()
    );

    let mut f = Filters::default();
    if args.journal_device_block_size % 512 != 0 {
        bail!(UsageError(
            "The device-block-size must be a multiple of 512".to_owned()
        ));
    }
    f.put(
        "${device-block-size}",
        args.journal_device_block_size.to_string(),
    );
    let cluster = &args.cluster;
    let backup = cluster.slave || cluster.backup;
    f.put(
        "${primary-backup}",
        if backup { "backup" } else { "primary" },
    );
    f.put(
        "${failover-on-shutdown}",
        cluster.failover_on_shutdown.to_string(),
    );
    f.put(
        "${persistence-enabled}",
        (!args.disable_persistence).to_string(),
    );
    f.put("${purge-page-folders}", args.purge_page_folders.to_string());
    match args.ping.as_deref().filter(|p| !p.is_empty()) {
        Some(ping) => {
            f.put("${ping}", ping);
            let text = f.read("etc/ping-settings.txt")?;
            f.put("${ping-config.settings}", text);
        }
        None => {
            let text = f.read("etc/commented-ping-settings.txt")?;
            f.put("${ping-config.settings}", text);
        }
    }
    let clustered = cluster.clustered
        || cluster.static_cluster.is_some()
        || cluster.replicated
        || cluster.shared_store;
    if cluster.replicated {
        let text = f.read(if backup {
            "etc/replicated-backup-settings.txt"
        } else {
            "etc/replicated-primary-settings.txt"
        })?;
        f.put("${replicated.settings}", text);
    } else {
        f.put("${replicated.settings}", "");
    }
    if cluster.shared_store {
        let text = f.read("etc/shared-store-settings.txt")?;
        f.put("${shared-store.settings}", text);
    } else {
        f.put("${shared-store.settings}", "");
    }
    f.put("${journal.settings}", journal_type);
    match &args.ssl_key {
        Some(key) => {
            f.put("${web.protocol}", "https");
            let password = args.ssl_key_password.clone().map(Ok).unwrap_or_else(|| {
                args.common
                    .input_password("--ssl-key-password", "What is the keystore password?")
            })?;
            let mut extra = format!(" keyStorePath=\"{key}\" keyStorePassword=\"{password}\"");
            if args.use_client_auth {
                let trust = match args.ssl_trust.clone() {
                    Some(t) => Some(t),
                    None => {
                        args.common
                            .input("--ssl-trust", "What is the trust store path?", None)?
                    }
                }
                .unwrap_or_default();
                let trust_password =
                    args.ssl_trust_password.clone().map(Ok).unwrap_or_else(|| {
                        args.common.input_password(
                            "--ssl-trust-password",
                            "What is the trust store password?",
                        )
                    })?;
                extra.push_str(&format!(
                    " clientAuth=\"true\" trustStorePath=\"{trust}\" trustStorePassword=\"{trust_password}\""
                ));
            }
            f.put("${extra.web.attributes}", extra);
        }
        None => {
            f.put("${web.protocol}", "http");
            f.put("${extra.web.attributes}", "");
        }
    }
    let offset = args.port_offset;
    f.put("${fsync}", (!args.no_fsync).to_string());
    f.put("${default.port}", (args.default_port + offset).to_string());
    f.put("${support-advisory}", args.support_advisory.to_string());
    f.put(
        "${suppress-internal-management-objects}",
        args.suppress_internal_management_objects.to_string(),
    );
    f.put("${amqp.port}", (5672 + offset).to_string());
    f.put("${stomp.port}", (61613 + offset).to_string());
    f.put("${hq.port}", (5445 + offset).to_string());
    f.put("${mqtt.port}", (1883 + offset).to_string());
    f.put("${http.host}", args.http_host.clone());
    f.put("${http.port}", (args.http_port + offset).to_string());
    f.put("${data.dir}", args.install.data.clone());
    f.put("${max-hops}", cluster.max_hops.to_string());
    f.put(
        "${message-load-balancing}",
        cluster.message_load_balancing.clone(),
    );
    let user = match args.user.clone() {
        Some(u) => Some(u),
        None => args
            .common
            .input("--user", "What is the default username?", Some("admin"))?,
    }
    .unwrap_or_else(|| "admin".to_owned());
    f.put("${user}", user.clone());
    let password = match args.password.clone() {
        Some(p) => p,
        None => args
            .common
            .input_password("--password", "What is the default password?")?,
    };
    f.put("${password}", hash_password(&password)?);
    f.put("${encoded.role}", args.role.replace(' ', "\\ "));
    f.put(
        "${global-max-messages}",
        args.global_max_messages.to_string(),
    );
    match args
        .global_max_size
        .as_deref()
        .filter(|s| !s.trim().is_empty())
    {
        None => {
            let text = f.read("etc/global-max-default.txt")?;
            f.put("${global-max-section}", text);
        }
        Some(size) => {
            f.put("${global-max-size}", size);
            let text = f.read("etc/global-max-specified.txt")?;
            f.put("${global-max-section}", text);
        }
    }
    f.put("${jdbc}", "");
    if clustered {
        if cluster.static_cluster.is_none() && !cluster.discovery_enabled {
            bail!(UsageError(
                "Clustered instances without --static-cluster use discovery groups, which are disabled by default. Supply static configuration, or see the Server Discovery section in the user manual for security requirements and enablement.".to_owned()
            ));
        };
        // `getHostForClustered`.
        let mut host = args.host.clone().unwrap_or_else(|| "0.0.0.0".to_owned());
        if host == "0.0.0.0" {
            host = args
                .common
                .input(
                    "--host",
                    &format!(
                        "Host {host} is not valid for clustering. Please provide a valid IP or hostname"
                    ),
                    Some("localhost"),
                )?
                .unwrap_or_else(|| "localhost".to_owned());
        }
        f.put("${host}", host.clone());
        f.put("${name}", args.name.clone().unwrap_or_else(|| host.clone()));
        let nodes: Vec<&str> = cluster
            .static_cluster
            .as_deref()
            .map(|s| s.split(',').collect())
            .unwrap_or_default();
        // `getConnectors`.
        let mut connectors = String::new();
        let mut line = |text: &str| {
            connectors.push_str(text);
            connectors.push_str(LINE_SEPARATOR);
        };
        line("      <connectors>");
        line(
            "            <!-- Connector used to be announced through cluster connections and notifications -->",
        );
        line(&f.apply(
            "            <connector name=\"artemis\">tcp://${host}:${default.port}</connector>",
        ));
        for (i, node) in nodes.iter().enumerate() {
            line(&format!(
                "            <connector name = \"node{i}\">{node}</connector>"
            ));
        }
        line("      </connectors>");
        f.put("${connector-config.settings}", connectors);
        let text = f.read("etc/cluster-security-settings.txt")?;
        f.put("${cluster-security.settings}", text);
        if cluster.static_cluster.is_some() {
            let list: String = (0..nodes.len())
                .map(|i| {
                    format!("               <connector-ref>node{i}</connector-ref>{LINE_SEPARATOR}")
                })
                .collect();
            f.put("${connectors-list}", list);
            let text = f.read("etc/cluster-static-settings.txt")?;
            f.put("${cluster.settings}", text);
        } else {
            let text = f.read("etc/cluster-settings.txt")?;
            f.put("${cluster.settings}", text);
        }
        let user = match cluster.cluster_user.clone() {
            Some(u) => u,
            None => args
                .common
                .input(
                    "--cluster-user",
                    "What is the cluster user?",
                    Some(saranga_config::BrokerConfig::DEFAULT_CLUSTER_USER),
                )?
                .unwrap_or_else(|| saranga_config::BrokerConfig::DEFAULT_CLUSTER_USER.to_owned()),
        };
        f.put("${cluster-user}", user);
        let mut password = match cluster.cluster_password.clone() {
            Some(p) => p,
            None => args
                .common
                .input_password("--cluster-password", "What is the cluster password?")?,
        };
        // `getClusterPassword`: stored masked.
        if !saranga_config::mask::is_enc_masked(&password) {
            // `getDefaultCodec`: its deprecation warning is logged where
            // `create` logs nothing.
            let masked = tracing::subscriber::with_default(
                tracing::subscriber::NoSubscriber::default(),
                || saranga_config::mask::Codec::from_desc(None).and_then(|c| c.encode(&password)),
            );
            match masked {
                Ok(masked) => {
                    password = saranga_config::mask::wrap(&masked);
                    println!("Using masked cluster-password: {password}");
                }
                Err(e) => {
                    eprintln!("Warning: Failed to mask password.");
                    eprintln!("Reason: {e}");
                }
            }
        }
        f.put("${cluster-password}", password);
    } else {
        let host = args.host.clone().unwrap_or_else(|| "0.0.0.0".to_owned());
        f.put("${name}", args.name.clone().unwrap_or_else(|| host.clone()));
        f.put("${host}", host);
        f.put("${connector-config.settings}", "");
        f.put("${cluster-security.settings}", "");
        f.put("${cluster.settings}", "");
        f.put("${cluster-user}", "");
        f.put("${cluster-password}", "");
    }
    f.put("${address-queue.settings}", addresses_and_queues(&args));
    let home = match &args.install.home {
        Some(h) => canonical(h)?,
        None => saranga_home()?,
    };
    f.put("${home}", path(&home));

    std::fs::create_dir_all(directory.join("bin"))?;
    let etc = create_directory(&args.install.etc, &directory)?;
    std::fs::create_dir_all(directory.join("tmp"))?;
    std::fs::create_dir_all(directory.join("lib"))?;
    let data = create_directory(&args.install.data, &directory)?;
    let log = create_directory("log", &directory)?;
    let oome = log.join("oom_dump.hprof");

    let java_options = args.install.java_options();
    add_script_filters(
        &mut f,
        &args.install,
        &ScriptDirs {
            home: &home,
            instance: &directory,
            etc: &etc,
            data: &data,
            oome: &oome,
        },
        &args.role,
    )?;

    let allow_anonymous = match allow_anonymous_given {
        Some(a) => a,
        None => args.common.input_boolean(
            "--allow-anonymous | --require-login",
            "Allow anonymous access?",
            true,
        )?,
    };
    let retention = if args.retention_days > 0 {
        match &args.retention_max_bytes {
            Some(max) => format!(
                "<journal-retention-directory period=\"{}\" unit=\"DAYS\" storage-limit=\"{max}\">{}/retention</journal-retention-directory>",
                args.retention_days, args.install.data
            ),
            None => format!(
                "<journal-retention-directory period=\"{}\" unit=\"DAYS\">{}/retention</journal-retention-directory>",
                args.retention_days, args.install.data
            ),
        }
    } else {
        // `Create`'s text block: a blank line first, the indentation
        // left after the block's own is stripped.
        concat!(
            "
",
            "      <!-- if you want to retain your journal uncomment this following configuration.
",
            "
",
            "      This will allow your system to keep 7 days of your data, up to 10G. Tweak it accordingly to your use case and capacity.
",
            "
",
            "      it is recommended to use a separate storage unit from the journal for performance considerations.
",
            "
",
            "      <journal-retention-directory period=\"7\" unit=\"DAYS\" storage-limit=\"10G\">data/retention</journal-retention-directory>
",
            "
",
            "      You can also enable retention by using the argument journal-retention on the `artemis create` command -->
",
            "
",
        )
        .to_owned()
    };
    f.put("${journal-retention}", retention);
    f.put("${java-opts}", java_options);
    f.put("${java-memory}", args.install.java_memory.clone());

    let writer = Writer {
        force: args.force,
        nix,
    };
    let login = if allow_anonymous {
        "etc/login-with-guest.config"
    } else {
        "etc/login-without-guest.config"
    };
    writer.write(login, &etc.join("login.config"), Some(&f), false)?;
    writer.write_etc("artemis-roles.properties", &etc, Some(&f), false)?;
    if windows {
        writer.write(
            "bin/instance.cmd",
            &directory.join("bin/artemis.cmd"),
            Some(&f),
            false,
        )?;
        writer.write(
            "bin/instance.cmd",
            &directory.join("bin/saranga.cmd"),
            Some(&f),
            false,
        )?;
        writer.write_etc("artemis.profile.cmd", &etc, Some(&f), false)?;
        writer.write_etc("artemis-utility.profile.cmd", &etc, Some(&f), false)?;
    }
    if nix {
        for script in ["artemis", "saranga"] {
            let target = directory.join("bin").join(script);
            writer.write("bin/instance.sh", &target, Some(&f), true)?;
            make_exec(&target)?;
        }
        let service = directory.join("bin/artemis-service");
        writer.write("bin/artemis-service", &service, Some(&f), true)?;
        make_exec(&service)?;
        writer.write_etc("artemis.profile", &etc, Some(&f), true)?;
        writer.write_etc("artemis-utility.profile", &etc, Some(&f), true)?;
    }
    writer.write_etc("log4j2.properties", &etc, None, false)?;
    writer.write_etc("log4j2-utility.properties", &etc, None, false)?;

    let text = if args.no_web {
        String::new()
    } else {
        f.read("etc/bootstrap-web-settings.txt")?
    };
    f.put("${bootstrap-web-settings}", text);
    for (flag, key, file) in [
        (
            args.no_amqp_acceptor,
            "${amqp-acceptor}",
            "etc/amqp-acceptor.txt",
        ),
        (
            args.no_mqtt_acceptor,
            "${mqtt-acceptor}",
            "etc/mqtt-acceptor.txt",
        ),
        (
            args.no_stomp_acceptor,
            "${stomp-acceptor}",
            "etc/stomp-acceptor.txt",
        ),
        (
            args.no_hornetq_acceptor,
            "${hornetq-acceptor}",
            "etc/hornetq-acceptor.txt",
        ),
    ] {
        let text = if flag { String::new() } else { f.read(file)? };
        f.put(key, text);
    }
    f.put(
        "${full-policy}",
        if args.paging != Some(true) && args.blocking == Some(true) {
            "BLOCK"
        } else {
            "PAGE"
        },
    );
    // `isAutoCreate`: --autocreate, else the opposite of --no-autocreate,
    // else true.
    let auto_create = args
        .autocreate
        .or(args.no_autocreate.map(|no| !no))
        .unwrap_or(true);
    f.put("${auto-create}", auto_create.to_string());
    f.put("${auto-delete}", args.autodelete.to_string());
    auto_tune(&mut f, &args, &data, journal_type)?;
    writer.write_etc("broker.xml", &etc, Some(&f), false)?;
    writer.write_etc("artemis-users.properties", &etc, Some(&f), false)?;
    let security = if args.security_manager.eq_ignore_ascii_case("basic") {
        f.read("etc/basic-security-manager.txt")?
    } else {
        f.read("etc/jaas-security-manager.txt")?
    };
    f.put("${security-manager-settings}", security);
    writer.write_etc("bootstrap.xml", &etc, Some(&f), false)?;
    writer.write_etc("management.xml", &etc, Some(&f), false)?;
    f.put(
        "${jolokia.options}",
        if args.relax_jolokia {
            "<!-- option relax-jolokia used, so strict-checking will be removed here -->"
        } else {
            "<!-- Check for the proper origin on the server side, too -->\n        <strict-checking/>"
        },
    );
    writer.write_etc("jolokia-access.xml", &etc, Some(&f), false)?;

    println!();
    println!("You can now start the broker by executing:  ");
    println!();
    let script = if windows {
        "bin/artemis.cmd"
    } else {
        "bin/artemis"
    };
    println!("   \"{}\" run", path(&directory.join(script)));
    println!();
    if nix {
        println!("Or you can run the broker in the background using:");
        println!();
        println!(
            "   \"{}\" start",
            path(&directory.join("bin").join("artemis-service"))
        );
        println!();
    }
    Ok(())
}

/// The directories the scripts and profiles name.
pub(crate) struct ScriptDirs<'a> {
    pub home: &'a Path,
    pub instance: &'a Path,
    pub etc: &'a Path,
    pub data: &'a Path,
    pub oome: &'a Path,
}

/// `Create.addScriptFilters`, and the `saranga` the scripts run.
pub(crate) fn add_script_filters(
    f: &mut Filters,
    install: &InstallArgs,
    dirs: &ScriptDirs,
    role: &str,
) -> anyhow::Result<()> {
    f.put("${artemis.home}", path(dirs.home));
    f.put("@artemis.instance@", path(dirs.instance));
    f.put(
        "${artemis.instance.name}",
        dirs.instance
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
    );
    f.put("${java.home}", "");
    f.put("${artemis.instance.etc.uri}", file_uri(dirs.etc));
    f.put("${artemis.instance.etc}", path(dirs.etc));
    f.put("${artemis.instance.oome.dump}", path(dirs.oome));
    f.put("${artemis.instance.data}", path(dirs.data));
    f.put("${java-memory}", install.java_memory.clone());
    f.put("${java-opts}", install.java_options());
    f.put("${java-utility-opts}", install.java_utility_options());
    f.put("${role}", role);
    f.put("${saranga}", path(&std::env::current_exe()?));
    Ok(())
}

/// `applyAddressesAndQueues`.
fn addresses_and_queues(args: &CreateArgs) -> String {
    let mut out = String::from("\n");
    let list = |s: &Option<String>| -> Vec<String> {
        s.as_deref()
            .map(|s| s.split(',').map(str::to_owned).collect())
            .unwrap_or_default()
    };
    let routing = |seg: &[&str], default: &str| -> String {
        let t = if seg.len() == 2 {
            seg[1].trim()
        } else {
            default
        };
        if !matches!(t.to_uppercase().as_str(), "ANYCAST" | "MULTICAST") {
            eprintln!("Invalid routing type: {t}");
        }
        t.to_owned()
    };
    for q in list(&args.queues) {
        let seg: Vec<&str> = q.split(':').collect();
        let name = seg[0].trim();
        let t = routing(&seg, "anycast");
        out.push_str(&format!(
            "         <address name=\"{name}\">\n            <{t}>\n               <queue name=\"{name}\" />\n            </{t}>\n         </address>\n"
        ));
    }
    for a in list(&args.addresses) {
        let seg: Vec<&str> = a.split(':').collect();
        let name = seg[0].trim();
        let t = routing(&seg, "multicast");
        out.push_str(&format!(
            "         <address name=\"{name}\">\n            <{t}/>\n         </address>\n"
        ));
    }
    out
}

/// `performAutoTune`: `journal-buffer-timeout` from a timed run of synced
/// writes in the data directory.
fn auto_tune(
    f: &mut Filters,
    args: &CreateArgs,
    data: &Path,
    journal_type: &str,
) -> anyhow::Result<()> {
    if args.no_autotune {
        f.put("${journal-buffer.settings}", "");
        f.put("${page-sync.settings}", "");
        return Ok(());
    }
    let mut sync = Filters::default();
    println!();
    println!("Auto tuning journal ...");
    if args.mapped && args.no_fsync {
        sync.put("${nanoseconds}", "0");
        sync.put("${writesPerMillisecond}", "0");
        sync.put("${maxaio}", "1");
        println!(
            "...Since you disabled sync and are using MAPPED journal, we are diabling buffer times"
        );
    } else {
        let writes: i32 = 250;
        match sync_test(
            data,
            "journal-test.tmp",
            4096,
            writes,
            5,
            !args.no_fsync,
            args.common.verbose,
        ) {
            Ok(time) => {
                let nanoseconds = to_nanos(time, i64::from(writes), args.common.verbose);
                let per_ms = decimal_format(f64::from(writes) / time as f64);
                sync.put("${nanoseconds}", nanoseconds.to_string());
                sync.put("${writesPerMillisecond}", per_ms.clone());
                sync.put(
                    "${maxaio}",
                    if journal_type == "ASYNCIO" {
                        args.journal_max_io.to_string()
                    } else {
                        "1".to_owned()
                    },
                );
                println!(
                    "done! Your system can make {per_ms} writes per millisecond, your journal-buffer-timeout will be {nanoseconds}"
                );
                let journal = sync.read("etc/journal-buffer-settings.txt")?;
                f.put("${journal-buffer.settings}", journal);
                if args.no_fsync {
                    sync.put("${nanoseconds}", "0");
                }
                // The NIO timing is the one measured: Saranga's journal
                // types all write as NIO does.
            }
            Err(e) => {
                eprintln!("{e:#}");
                eprintln!("Couldn't perform sync calculation, using default values");
                f.put("${journal-buffer.settings}", "");
                f.put("${page-sync.settings}", "");
                return Ok(());
            }
        }
    }
    if !f.0.iter().any(|(k, _)| k == "${journal-buffer.settings}") {
        let journal = sync.read("etc/journal-buffer-settings.txt")?;
        f.put("${journal-buffer.settings}", journal);
    }
    let page = sync.read("etc/page-sync-settings.txt")?;
    f.put("${page-sync.settings}", page);
    Ok(())
}

/// The hash `artemis create` writes for the user's password
/// (`HashUtil.tryHash`), or the password as given when it is masked.
fn hash_password(password: &str) -> anyhow::Result<String> {
    if password.starts_with("ENC(") && password.ends_with(')') {
        return Ok(password.to_owned());
    }
    let codec = saranga_config::mask::Codec::init(
        &[("algorithm".to_owned(), "one-way".to_owned())],
        |n| std::env::var(n).ok(),
    )?;
    Ok(format!("ENC({})", codec.encode(password)?))
}

struct Writer {
    force: bool,
    nix: bool,
}

impl Writer {
    /// `InstallAbstract.write`: the template, filtered, with the platform's
    /// line ends (LF for a Unix target).
    fn write(
        &self,
        source: &str,
        target: &Path,
        filters: Option<&Filters>,
        unix_target: bool,
    ) -> anyhow::Result<()> {
        if target.exists() && !self.force {
            bail!(crate::client::CliError(format!(
                "The file '{}' already exists.  Use --force to overwrite.",
                target.display()
            )));
        }
        let text = template_text(source)?;
        let content = match filters {
            Some(f) => f.apply(text),
            None => text.to_owned(),
        };
        let separator = if (unix_target && self.nix) || !cfg!(windows) {
            "\n"
        } else {
            "\r\n"
        };
        let content = content.replace("\r\n", "\n").replace('\n', separator);
        std::fs::write(target, content).with_context(|| format!("writing {}", target.display()))
    }

    fn write_etc(
        &self,
        name: &str,
        etc: &Path,
        filters: Option<&Filters>,
        unix_target: bool,
    ) -> anyhow::Result<()> {
        self.write(
            &format!("etc/{name}"),
            &etc.join(name),
            filters,
            unix_target,
        )
    }
}

fn create_directory(name: &str, root: &Path) -> anyhow::Result<PathBuf> {
    let dir = Path::new(name);
    let dir = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        root.join(dir)
    };
    std::fs::create_dir_all(&dir)?;
    canonical(&dir)
}

/// `File.getCanonicalPath`, without Windows' `\\?\` prefix.
pub(crate) fn canonical(p: &Path) -> anyhow::Result<PathBuf> {
    let c = std::fs::canonicalize(p).with_context(|| format!("{}", p.display()))?;
    let s = c.to_string_lossy();
    Ok(PathBuf::from(
        s.strip_prefix(r"\\?\").unwrap_or(&s).to_owned(),
    ))
}

pub(crate) fn path(p: &Path) -> String {
    p.display().to_string()
}

/// `File.toURI().toString()` of a directory: `file:/C:/dir/` or
/// `file:/dir/`.
pub(crate) fn file_uri(dir: &Path) -> String {
    let mut s = dir.display().to_string().replace('\\', "/");
    if !s.starts_with('/') {
        s.insert(0, '/');
    }
    if !s.ends_with('/') {
        s.push('/');
    }
    let encoded: String = s
        .chars()
        .map(|c| {
            if c == ' ' {
                "%20".to_owned()
            } else {
                c.to_string()
            }
        })
        .collect();
    format!("file:{encoded}")
}

/// The home, as Artemis's `artemis.home`: `ARTEMIS_HOME`, else the
/// directory above the one holding this `saranga`, as an Artemis home holds
/// `bin/artemis`.
pub(crate) fn saranga_home() -> anyhow::Result<PathBuf> {
    if let Some(home) = std::env::var_os("ARTEMIS_HOME").filter(|h| !h.is_empty()) {
        return Ok(PathBuf::from(home));
    }
    let exe = canonical(&std::env::current_exe()?)?;
    Ok(exe
        .parent()
        .and_then(Path::parent)
        .context("saranga has no directory above its own")?
        .to_path_buf())
}

#[cfg(unix)]
pub(crate) fn make_exec(p: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(p)?.permissions();
    perms.set_mode(perms.mode() | 0o755);
    std::fs::set_permissions(p, perms)?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn make_exec(_: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Filters, decimal_format, file_uri};

    #[test]
    fn decimal_format_like_java() {
        assert_eq!(decimal_format(12.5), "12.5");
        assert_eq!(decimal_format(3.0), "3");
        assert_eq!(decimal_format(0.85), ".85");
        assert_eq!(decimal_format(0.0), "0");
        assert_eq!(decimal_format(1.005), "1");
    }

    #[test]
    fn filters_apply_in_order() {
        let mut f = Filters::default();
        f.put("${a}", "x${b}");
        f.put("${b}", "y");
        assert_eq!(f.apply("${a}-${b}"), "xy-y");
    }

    #[test]
    fn uris() {
        assert_eq!(
            file_uri(std::path::Path::new("/opt/i/etc")),
            "file:/opt/i/etc/"
        );
    }
}
