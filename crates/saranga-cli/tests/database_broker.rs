//! A real broker on the database store: messages go in, the process is
//! killed, a new one gives them all back and nothing stays in the tables.
//! Run with `--ignored`, with `SARANGA_TEST_POSTGRES` naming a JDBC URL if
//! not the local default. They empty the tables, so they take turns.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls};

const BINARY: &str = env!("CARGO_BIN_EXE_saranga");

static TURN: Mutex<()> = Mutex::new(());

fn url() -> String {
    std::env::var("SARANGA_TEST_POSTGRES").unwrap_or_else(|_| {
        "jdbc:postgresql://127.0.0.1:5432/saranga?user=saranga&password=saranga".to_owned()
    })
}

/// The tables, as the test sees them.
struct Database {
    runtime: tokio::runtime::Runtime,
    client: Client,
}

impl Database {
    fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let url = url();
        let (client, connection) = runtime
            .block_on(tokio_postgres::connect(
                url.trim_start_matches("jdbc:"),
                NoTls,
            ))
            .expect("PostgreSQL is reachable");
        runtime.spawn(connection);
        Self { runtime, client }
    }

    /// Fails on a first run, before a broker has made the tables.
    fn empty(&self) {
        let _ = self.runtime.block_on(self.client.batch_execute(
            "TRUNCATE DB_MESSAGES, DB_REFERENCES, DB_ADDRESS, DB_QUEUE, DB_BROKER_DATA, DB_CONFIG_DATA, DB_PREP_TX",
        ));
    }

    fn count(&self, sql: &str) -> i64 {
        self.runtime
            .block_on(self.client.query_one(sql, &[]))
            .unwrap()
            .get(0)
    }

    fn messages(&self) -> i64 {
        self.count("SELECT count(*) FROM DB_MESSAGES")
    }

    fn references(&self) -> i64 {
        self.count("SELECT count(*) FROM DB_REFERENCES")
    }

    fn paged(&self) -> i64 {
        self.count("SELECT count(*) FROM DB_REFERENCES WHERE PAGED = 'Y'")
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// An instance on the store, and its running broker, which dies with the
/// test.
struct Instance {
    _dir: tempfile::TempDir,
    home: PathBuf,
    port: u16,
    broker: Option<Child>,
    starts: u32,
}

impl Instance {
    fn new(global_max_size: Option<u64>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("broker");
        let port = free_port();
        let out = Command::new(BINARY)
            .arg("create")
            .arg(&home)
            .args(["--user", "a", "--password", "b", "--allow-anonymous"])
            .args(["--no-web", "--no-amqp-acceptor", "--no-mqtt-acceptor"])
            .args(["--no-stomp-acceptor", "--no-hornetq-acceptor"])
            .args(["--default-port", &port.to_string()])
            .args(["--database-storage", "--jdbc-connection-url", &url()])
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
        let file = home.join("etc/broker.xml");
        let xml = std::fs::read_to_string(&file).unwrap();
        assert!(xml.contains("<database-storage>true"), "not on the store");
        let disk = "<max-disk-usage>90</max-disk-usage>";
        assert!(xml.contains(disk));
        let limit = global_max_size
            .map(|n| format!("<global-max-size>{n}</global-max-size>"))
            .unwrap_or_default();
        let xml = xml.replace(
            disk,
            &format!("<max-disk-usage>100</max-disk-usage>{limit}"),
        );
        std::fs::write(&file, xml).unwrap();
        std::fs::create_dir_all(home.join("log")).unwrap();
        Self {
            _dir: dir,
            home,
            port,
            broker: None,
            starts: 0,
        }
    }

    /// Shared-store HA: this `role` of a pair locking in `locks`.
    fn shared_store(&self, role: &str, locks: &Path) {
        let file = self.home.join("etc/broker.xml");
        let xml = std::fs::read_to_string(&file).unwrap();
        let locks = locks.display().to_string().replace('\\', "/");
        let policy = format!(
            "<ha-policy><shared-store><{role}/></shared-store></ha-policy>\
             <node-manager-lock-directory>{locks}</node-manager-lock-directory>\
             <max-disk-usage>100</max-disk-usage>"
        );
        let xml = xml.replace("<max-disk-usage>100</max-disk-usage>", &policy);
        std::fs::write(&file, xml).unwrap();
    }

    /// Starts the broker, which is not live yet if it is a backup.
    fn launch(&mut self) {
        self.starts += 1;
        let log = self.log();
        let file = std::fs::File::create(&log).unwrap();
        let child = Command::new(BINARY)
            .arg("run")
            .env("ARTEMIS_INSTANCE", &self.home)
            .env("ARTEMIS_INSTANCE_ETC", self.home.join("etc"))
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .unwrap();
        self.broker = Some(child);
    }

    fn log(&self) -> PathBuf {
        self.home.join(format!("log/run-{}.out", self.starts))
    }

    /// Waits for what the broker logs once it is live.
    fn live(&mut self) {
        let log = self.log();
        let child = self.broker.as_mut().expect("a broker");
        let deadline = Instant::now() + Duration::from_secs(60);
        // A primary's, or a backup's that has taken over.
        while !["AMQ221007", "AMQ221010"]
            .iter()
            .any(|m| read(&log).contains(m))
        {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("the broker ended with {status}:\n{}", read(&log));
            }
            assert!(Instant::now() < deadline, "not live:\n{}", read(&log));
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn start(&mut self) {
        self.launch();
        self.live();
    }

    /// As `kill -9`: nothing gets to finish.
    fn kill(&mut self) {
        let mut child = self.broker.take().expect("a broker");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    fn client(&self, command: &str, args: &[String]) -> String {
        let out = Command::new(BINARY)
            .arg(command)
            .args(["--url", &format!("tcp://localhost:{}", self.port)])
            .args(["--user", "a", "--password", "b"])
            .args(["--destination", "queue://KILLED"])
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(out.status.success(), "{command}: {text}");
        text
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        if let Some(mut child) = self.broker.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn read(path: &Path) -> String {
    String::from_utf8_lossy(&std::fs::read(path).unwrap_or_default()).into_owned()
}

/// Sends, kills the broker, and takes everything from the next one.
fn survives_a_kill(count: i64, size: u64, limit: Option<u64>, producer: &[&str], paged: bool) {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let database = Database::new();
    database.empty();
    let mut instance = Instance::new(limit);
    instance.start();

    let mut args = vec![
        "--message-count".to_owned(),
        count.to_string(),
        "--message-size".to_owned(),
        size.to_string(),
    ];
    args.extend(producer.iter().map(|&s| s.to_owned()));
    instance.client("producer", &args);
    assert_eq!(database.messages(), count, "what the producer was told");
    assert_eq!(database.references(), count);
    assert_eq!(database.paged() > 0, paged, "paged rows");

    instance.kill();
    instance.start();

    let out = instance.client(
        "consumer",
        &[
            "--message-count".to_owned(),
            count.to_string(),
            "--receive-timeout".to_owned(),
            "30000".to_owned(),
        ],
    );
    assert!(
        out.contains(&format!("Consumed: {count} messages")),
        "{out}"
    );

    // This consumer does not wait for its last acknowledgements.
    let deadline = Instant::now() + Duration::from_secs(20);
    while database.references() + database.messages() > 0 {
        assert!(
            Instant::now() < deadline,
            "left behind: {} messages, {} references",
            database.messages(),
            database.references()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
#[ignore = "needs PostgreSQL"]
fn plain_messages_survive_a_kill() {
    survives_a_kill(2000, 512, None, &[], false);
}

#[test]
#[ignore = "needs PostgreSQL"]
fn paged_messages_survive_a_kill() {
    survives_a_kill(5000, 512, Some(1_000_000), &[], true);
}

#[test]
#[ignore = "needs PostgreSQL"]
fn large_messages_survive_a_kill_through_paging() {
    survives_a_kill(100, 300_000, Some(100_000), &[], true);
}

/// A transaction of nothing but paged messages still has to commit.
#[test]
#[ignore = "needs PostgreSQL"]
fn transactions_of_paged_messages_survive_a_kill() {
    survives_a_kill(5000, 512, Some(1_000_000), &["--txt-size", "100"], true);
}

/// Messages still in the table are found by a purge, and none comes back: a
/// read that ran ahead of their deletes would hand them out again.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_purge_reaches_paged_messages() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let database = Database::new();
    database.empty();
    let mut instance = Instance::new(Some(1_000_000));
    instance.start();
    let send = |n: i64| {
        instance.client(
            "producer",
            &[
                "--message-count".to_owned(),
                n.to_string(),
                "--message-size".to_owned(),
                "512".to_owned(),
            ],
        );
    };
    send(5000);
    assert!(database.paged() > 0, "nothing paged");

    let out = Command::new(BINARY)
        .args(["queue", "purge", "--name", "KILLED"])
        .args(["--url", &format!("tcp://localhost:{}", instance.port)])
        .args(["--user", "a", "--password", "b"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    assert_eq!(database.messages(), 0, "{out:?}");
    assert_eq!(database.references(), 0);

    // A purged message that came back would take a new one's place, and
    // leave that one's rows behind.
    send(100);
    let out = instance.client(
        "consumer",
        &[
            "--message-count".to_owned(),
            "100".to_owned(),
            "--receive-timeout".to_owned(),
            "30000".to_owned(),
        ],
    );
    assert!(out.contains("Consumed: 100 messages"), "{out}");
    let deadline = Instant::now() + Duration::from_secs(20);
    while database.references() + database.messages() > 0 {
        assert!(Instant::now() < deadline, "rows left behind");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The data tools read the tables, paged messages and all, with no broker.
#[test]
#[ignore = "needs PostgreSQL"]
fn the_data_tools_read_the_tables() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let database = Database::new();
    database.empty();
    let mut instance = Instance::new(Some(200_000));
    instance.start();
    instance.client(
        "producer",
        &[
            "--message-count".to_owned(),
            "1000".to_owned(),
            "--message-size".to_owned(),
            "300".to_owned(),
        ],
    );
    assert!(database.paged() > 0, "nothing paged");
    instance.kill();

    let data = |args: &[&str]| {
        let out = Command::new(BINARY)
            .arg("data")
            .args(args)
            .args(["--f"])
            .env("ARTEMIS_INSTANCE", &instance.home)
            .env("ARTEMIS_INSTANCE_ETC", instance.home.join("etc"))
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let printed = data(&["print"]);
    assert!(printed.contains("Total messages: 1000"), "{printed}");
    assert!(printed.contains("Q U E U E S"));
    let exported = data(&["exp"]);
    assert_eq!(exported.matches("<message ").count(), 1000);
    assert!(exported.contains("name=\"KILLED\""));
}

/// Shared store on the database, as Artemis has it: the pair lock in a shared
/// directory and the backup opens the store when it takes over, which is when
/// the primary's session, and its lock on the store, are gone.
#[test]
#[ignore = "needs PostgreSQL"]
fn a_backup_takes_over_the_store() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let database = Database::new();
    database.empty();
    let locks = tempfile::tempdir().unwrap();
    let mut primary = Instance::new(Some(1_000_000));
    let mut backup = Instance::new(Some(1_000_000));
    primary.shared_store("primary", locks.path());
    backup.shared_store("backup", locks.path());
    primary.start();
    primary.client(
        "producer",
        &[
            "--message-count".to_owned(),
            "3000".to_owned(),
            "--message-size".to_owned(),
            "512".to_owned(),
        ],
    );
    assert!(database.paged() > 0, "nothing paged");

    backup.launch();
    std::thread::sleep(Duration::from_secs(3));
    assert!(
        !read(&backup.log()).contains("AMQ221010"),
        "two live brokers"
    );

    primary.kill();
    backup.live();
    let out = backup.client(
        "consumer",
        &[
            "--message-count".to_owned(),
            "3000".to_owned(),
            "--receive-timeout".to_owned(),
            "30000".to_owned(),
        ],
    );
    assert!(out.contains("Consumed: 3000 messages"), "{out}");
    let deadline = Instant::now() + Duration::from_secs(20);
    while database.references() + database.messages() > 0 {
        assert!(Instant::now() < deadline, "rows left behind");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Replication copies journals, and the store has none: the broker says so
/// rather than start.
#[test]
#[ignore = "needs PostgreSQL"]
fn replication_is_refused_with_the_store() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let mut instance = Instance::new(None);
    let file = instance.home.join("etc/broker.xml");
    let xml = std::fs::read_to_string(&file).unwrap().replace(
        "<max-disk-usage>100</max-disk-usage>",
        "<ha-policy><replication><primary/></replication></ha-policy>\
         <max-disk-usage>100</max-disk-usage>",
    );
    std::fs::write(&file, xml).unwrap();
    instance.launch();
    let mut child = instance.broker.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("it started:\n{}", read(&instance.log()));
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(!status.success());
    assert!(
        read(&instance.log()).contains("replication"),
        "{}",
        read(&instance.log())
    );
}
