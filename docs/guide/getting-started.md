# Getting started

This page takes you from a release or the source tree to a running broker: installing or building the `saranga` binary, creating a broker instance, starting it, sending and receiving a few messages with the CLI, and stopping it again. Saranga is inspired by Apache ActiveMQ Artemis 2.57.0 and works interchangeably with it, so an instance looks and behaves like one made by `artemis create`: the same `etc/` files, the same `bin/artemis` script, the same ports. If you know Artemis, most of this page will be familiar; the notes say where Saranga differs.

## Installing a release

Each release on GitHub has an archive for Linux (x86_64 and aarch64), Windows (x86_64) and macOS (x86_64 and aarch64), with `SHA256SUMS` to check them. An archive is a Saranga home, laid out as an Artemis home is:

```
saranga-1.0.0-x86_64-unknown-linux-gnu/
  bin/saranga
  web/                 where Artemis's console.war goes (see README.txt there)
  docs/guide/  docs/benchmarks.md
  CHANGELOG.md  COMPAT.md  LICENSE  NOTICE  README.md
```

```bash
tar xzf saranga-1.0.0-x86_64-unknown-linux-gnu.tar.gz -C /opt
/opt/saranga-1.0.0-x86_64-unknown-linux-gnu/bin/saranga version
```

The Linux binaries need glibc 2.28 or later (RHEL 8, Debian 10, Ubuntu 18.04 and later). On Windows, unzip the `.zip` and run `bin\saranga.exe`.

## Running in a container

The image is laid out as Artemis's: the home is `/opt/saranga`, and on its first start the container makes a broker instance in the `/var/lib/artemis-instance` volume. It uses the same variables as Artemis's image: `ARTEMIS_USER` and `ARTEMIS_PASSWORD` (both `artemis`), `ANONYMOUS_LOGIN` (`false`) and `EXTRA_ARGS` (`--http-host 0.0.0.0 --relax-jolokia`), passed to `saranga create`. Files in the instance's `etc-override` directory are copied over its `etc` once it has been made, and the container runs the instance's `bin/artemis` with its arguments, `run` by default.

```bash
docker run -d --name saranga -p 61616:61616 -p 8161:8161 ghcr.io/grepjava/saranga:1.0.0
```

It exposes Artemis's ports: 61616 (all protocols), 5445 (HornetQ and STOMP), 5672 (AMQP), 1883 (MQTT), 61613 (STOMP) and 8161 (HTTP). To build the image yourself:

```bash
docker build -f docker/Dockerfile -t saranga .
```

Saranga's own console is at http://localhost:8161/saranga/. To have Artemis's console as well, mount its `console.war` at `/opt/saranga/web/console.war`.

## Building

Saranga is built with Cargo. The repository's `rust-toolchain.toml` pins the Rust version, and `rustup` installs it on the first build.

```bash
cargo build --release -p saranga-cli
```

The binary is `target/release/saranga` (`target\release\saranga.exe` on Windows). It is self-contained: there is no JVM and no `lib/` of jars to install. You can run it from `target/release` or copy it anywhere on your `PATH`.

```bash
target/release/saranga version
```

```
Saranga 1.0.0 (Apache Artemis 2.57.0)
Saranga home: /home/me/saranga/target
Saranga instance: null
```

The *home* is the directory above the one holding the binary, unless `ARTEMIS_HOME` is set. Saranga looks there for Artemis's web console (see [Management](management.md)).

## Creating an instance

An instance is a directory holding one broker's configuration, data and logs. `saranga create` makes one, as `artemis create` does, from the same templates:

```bash
saranga create /var/lib/mybroker --user admin --password admin --require-login
```

Without `--user`, `--password` and `--allow-anonymous` or `--require-login`, it asks for them; `--silent` makes it take a best guess instead. It then tunes `journal-buffer-timeout` by timing a few writes, as Artemis's does (Saranga's own journal does not use it; see [Storage](storage.md)), and prints how to start the broker:

```
Creating Saranga broker instance at: /var/lib/mybroker

Auto tuning journal ...
done! Your system can make 2.21 writes per millisecond, your journal-buffer-timeout will be 452000

You can now start the broker by executing:

   "/var/lib/mybroker/bin/artemis" run
```

The instance has the layout Artemis's has:

| Directory | Holds |
|---|---|
| `bin/` | `artemis` and `saranga` (`artemis.cmd` and `saranga.cmd` on Windows), and on Unix `artemis-service` |
| `etc/` | `broker.xml`, `bootstrap.xml`, `login.config`, the users and roles files, `management.xml`, `jolokia-access.xml`, the logging configuration and the profiles |
| `data/` | the journal, bindings, paging and large-message directories |
| `log/` | `artemis.log` |
| `lib/`, `tmp/` | made for Artemis's layout; Saranga does not use them |

`bin/artemis` and `bin/saranga` are the same script. Each runs the `saranga` binary that created the instance, by its absolute path, for this instance. To run another binary, set `SARANGA` to its path, or run `saranga upgrade <instance>` with the new binary to rewrite the scripts (see [The CLI](cli.md#upgrade)).

The options most people use:

- `--port-offset <n>` adds `n` to every acceptor's port and to the web server's, to run several brokers on one machine.
- `--default-port`, `--http-port`, `--host` and `--http-host` set the main acceptor's port, the web server's port and the addresses they listen on.
- `--queues` and `--addresses` declare queues and addresses in `broker.xml`, for example `--queues orders,audit:multicast`.
- `--no-amqp-acceptor`, `--no-stomp-acceptor`, `--no-mqtt-acceptor` and `--no-hornetq-acceptor` leave those acceptors out; `--no-web` leaves out the web server.
- `--clustered`, `--replicated`, `--shared-store` and `--backup` write cluster and HA settings (see [Clustering](clustering.md) and [High availability](ha.md)).

`saranga create --help` lists every option. They are Artemis's; see [Artemis's CLI documentation](https://activemq.apache.org/components/artemis/documentation/latest/using-cli.html). The differences: the journal type is `NIO` unless you pass `--aio`, `--nio` or `--mapped`, `--jdbc` is refused, and there is no Windows service wrapper. See [COMPAT.md, CLI differences](../../COMPAT.md#cli-differences).

## Starting the broker

Start the broker in the foreground with the instance's script:

```bash
/var/lib/mybroker/bin/artemis run
```

The log goes to the console and to `log/artemis.log`, in Artemis's format and with Artemis's message codes. The broker is ready when it logs:

```
AMQ221007: Server is now active
AMQ221001: Saranga Message Broker version 1.0.0 [0.0.0.0, nodeID=4ba9e790-...]
```

A fresh instance listens on the same ports as an Artemis instance: 61616 for every protocol, 5672 for AMQP, 61613 for STOMP, 5445 for the HornetQ acceptor, 1883 for MQTT, and 8161 for the web server, Jolokia and the REST API. One warning is expected on a fresh instance: `The console's pages are not served: no console.war in ...`: it is about Artemis's console, which Saranga does not ship. Saranga's own console is served at `/saranga/`, and Jolokia and the REST API are served anyway. [Management](management.md#the-saranga-console) describes Saranga's console, and [the web console](management.md#the-web-console) how to serve Artemis's.

### `saranga run` directly

`bin/artemis run` sets `ARTEMIS_INSTANCE` and calls `saranga run`. You can also call `saranga run` yourself:

```bash
# The instance named by ARTEMIS_INSTANCE, as bin/artemis run does.
ARTEMIS_INSTANCE=/var/lib/mybroker saranga run

# An instance's etc directory: bootstrap.xml names broker.xml and the security domain.
saranga run --etc /var/lib/mybroker/etc

# A broker.xml on its own. Relative data directories are taken from the
# directory above the file's; login.config and bootstrap.xml next to it apply.
saranga run --config /var/lib/mybroker/etc/broker.xml

# Nothing: everything in memory, no security, every protocol on 0.0.0.0:61616.
saranga run
```

The options of `saranga run`:

| Option | Does |
|---|---|
| `--config <broker.xml>` | Runs that `broker.xml`. Cannot be combined with `--etc`. |
| `--etc <dir>` | Runs the instance whose `etc` directory this is, as `artemis run` does. |
| `--bind <host:port>` | Listens on this one address instead of the configured acceptors, with the first acceptor's settings (TLS, security domain). |
| `-D <name=value>` | A system property, as `java -D` sets one. `${name}` in the configuration files resolves to it, and `brokerconfig.`-prefixed names are broker properties. Repeatable. |
| `--properties <files>` | Broker properties applied over `broker.xml`, as `artemis run --properties`: comma-separated files, or directories ending in `/`. The default is `broker.properties` next to `broker.xml`, if there is one. |
| `--broker <broker.xml>` | Overrides the `broker.xml` that `bootstrap.xml` names. |
| `--allow-kill` | Lets `artemis kill` halt the broker (see below). |
| `[CONFIGURATION]` | Another bootstrap file, as `xml:path/to/bootstrap.xml`. |

For example, to run an instance's configuration on another port, on the loopback interface only, without editing its files:

```bash
saranga run --etc /var/lib/mybroker/etc --bind 127.0.0.1:61617
```

Through `bin/artemis`, the `-D` options in the profile's `JAVA_ARGS` (`etc/artemis.profile`) are passed to `saranga run` too, before those on the command line, which win. `-Xmx` sizes the default `global-max-size`, as in Artemis; the other Java options in the profile are ignored. [Configuration](configuration.md) covers system and broker properties in full.

## Sending and receiving

The CLI's `producer`, `consumer` and `browser` work as Artemis's do. Run from the instance's `bin`, they find the broker's URL from the acceptor called `artemis` in `broker.xml`; elsewhere, give `--url`.

```bash
cd /var/lib/mybroker/bin
./artemis producer --user admin --password admin --destination queue://orders --message-count 10 --message hello
./artemis queue stat --user admin --password admin --queueName orders
./artemis consumer --user admin --password admin --destination queue://orders --message-count 10
```

```
Connection brokerURL = tcp://localhost:61616
Producer ActiveMQQueue[orders], thread=0 Started to calculate elapsed time ...

Producer ActiveMQQueue[orders], thread=0 Produced: 10 messages
...
|NAME  |ADDRESS|CONSUMER|MESSAGE|MESSAGES|DELIVERING|MESSAGES|SCHEDULED|ROUTING|INTERNAL|
|      |       | COUNT  | COUNT | ADDED  |  COUNT   | ACKED  |  COUNT  | TYPE  |        |
|orders|orders |   0    |  10   |   10   |    0     |   0    |    0    |ANYCAST| false  |
...
Consumer ActiveMQQueue[orders], thread=0 Consumed: 10 messages
```

The address and queue `orders` are created on first use, as the default address settings allow. The CLI connects over Core (over TLS with `sslEnabled=true` in the URL, or over AMQP with `--protocol AMQP`); any Artemis or Saranga CLI, and any Artemis client, can talk to the broker. [Protocols](protocols.md) covers the other protocols and their clients.

If the producer stops with `AMQ212054: Destination address=orders is blocked`, look in the broker's log for `AMQ222210`: the disk is fuller than `max-disk-usage` (90% in the `broker.xml` that `create` writes), and the broker blocks producers until there is room, as Artemis does. Free some space, or raise `max-disk-usage` in `broker.xml`. See [Storage](storage.md#full-disks).

## Stopping the broker

Any of these stops the broker cleanly:

```bash
/var/lib/mybroker/bin/artemis stop
```

- `artemis stop` writes a file called `STOP_ME` next to `broker.xml`. The running broker checks for it every half second, removes it and stops. This works from another terminal or a script.
- Ctrl-C in the terminal where the broker runs.
- On Unix, `SIGTERM`, as `kill <pid>`, `artemis-service stop` and service managers send it.

On a clean stop the broker stops accepting connections, gives clients `graceful-shutdown-timeout` to leave when graceful shutdown is enabled, closes the rest and logs `AMQ221002: Saranga Message Broker version ... stopped, uptime ...`.

`artemis kill` writes `KILL_ME` instead. A broker started with `run --allow-kill` halts at once when it sees it, without a clean stop, which is useful to test failover. Without `--allow-kill`, `KILL_ME` is ignored.

## Linux notes

- `bin/artemis-service` is Artemis's script, unchanged: `artemis-service start` runs the broker in the background with `nohup`, writes its PID to `data/artemis.pid`, and `stop`, `restart`, `force-stop` and `status` use that file. With `ARTEMIS_USER` set, it starts the broker as that user through `sudo -n`. [Operations](operations.md#running-as-a-service) covers running it under a service manager such as systemd.
- The journal is written with buffered file I/O whatever `journal-type` says; Saranga does not use libaio. The files are the same as Artemis's `NIO` journal writes, and Artemis reads them with any journal type. See [Storage](storage.md) and [COMPAT.md, Persistence differences](../../COMPAT.md#persistence-differences).
- `saranga completion` prints a bash or zsh completion script: `source <(./artemis completion)` in the instance's `bin`.

## Windows notes

- `saranga create` writes `bin\artemis.cmd` and `bin\saranga.cmd`. Use them where the Unix examples use `bin/artemis`:

  ```bat
  C:\brokers\mybroker\bin\artemis.cmd run
  C:\brokers\mybroker\bin\artemis.cmd stop
  ```

  `--cygwin` (or `--linux`) on `create` writes the Unix scripts instead, for Cygwin; `--windows` forces the `.cmd` scripts.
- There is no `artemis-service.exe`. Run the broker in a console, or under a service manager of your choice, and stop it with `artemis.cmd stop` or Ctrl-C.
- `artemis.cmd` hands its arguments to `cmd` again, which ends the command at an `&` even inside quotes. Separate URL parameters with `;` instead, which Artemis and Saranga both accept: `--url "tcp://localhost:61616?reconnectAttempts=5;retryInterval=500"`.
- Saranga's `artemis.cmd` passes the profile's `JAVA_ARGS_APPEND` on, where Artemis's misspells the variable and never passes it.

## Next steps

- [The CLI](cli.md): every command, and how it compares with `artemis`.
- [Configuration](configuration.md): the instance's files and how Saranga reads them.
- [Migrating from Artemis](migration.md): running Saranga on an existing Artemis instance and its data.
