# The CLI

The `saranga` command is Saranga's counterpart of `artemis`. It has the same commands, with the same options and the same output, so scripts and runbooks written for the Artemis CLI work with it, and the Artemis CLI works against a Saranga broker. This page groups the commands by task, shows the options you will use most, and says where a command differs from its `artemis` namesake. The full list of differences is in [COMPAT.md, CLI differences](../../COMPAT.md#cli-differences).

## Running the CLI

There are three ways to run a command:

```bash
saranga <command> [options]                 # the binary, from anywhere
/var/lib/mybroker/bin/artemis <command>     # an instance's script
/var/lib/mybroker/bin/saranga <command>     # the same script under Saranga's name
```

On Windows, use `saranga.exe` and `bin\artemis.cmd` or `bin\saranga.cmd`.

An instance's script sets `ARTEMIS_INSTANCE` and passes on the profile's Java arguments, as Artemis's does. Commands that need an instance (`run`, `stop`, `kill`, the data tools) find it that way, and the messaging and administration commands read the default connection URL from its `broker.xml`. Run from the binary, give the instance or the URL explicitly.

`saranga` with no command starts the [shell](#shell).

## Getting help

```bash
saranga --help                  # Saranga's list of commands
saranga <command> --help        # Saranga's help for a command, every option it takes
saranga help <command>          # Artemis's help text for the command
```

`saranga help` prints Artemis's own usage message, laid out as Artemis's CLI lays it out, with the commands Artemis offers in the same place: from an instance's `bin` it lists `run`, `stop`, `kill` and `user`, and from the binary it lists `create` and `upgrade` instead. The commands Saranga does not have yet (`lock`, `properties` and `check cluster`) are left out. Every option `saranga help` lists is one Saranga takes; `--help` shows them in Saranga's words and adds Saranga's own, such as `run --config`.

The sections below show the useful options. `saranga <command> --help` lists the rest.

## Connection options

The commands that talk to a running broker (`producer`, `consumer`, `browser`, `queue`, `address`, `user`, `check`, `perf` and the shell's `connect`) share these options:

| Option | Does |
|---|---|
| `--url <url>` | The broker. Default: the URL of the acceptor called `artemis` in the instance's `broker.xml`, else `tcp://localhost:61616`. |
| `--acceptor <name>` | Takes the default URL from this acceptor instead. |
| `--user`, `--password` | The credentials. |
| `--silent` | Asks nothing: no prompt for missing values, no retry after a failed connection. Use it in scripts. |
| `--verbose` | Prints more. |

The CLI connects over Core on plain TCP. These are the differences from Artemis's CLI:

- `--protocol AMQP` is not supported, and a URL with `sslEnabled=true` is refused.
- A URL can list several servers, `(tcp://a:61616,tcp://b:61616)?reconnectAttempts=5`, tried in turn as Artemis's client tries them, and takes the usual `ServerLocator` parameters (`ha`, `reconnectAttempts`, `retryInterval`, `callTimeout` and the like).
- The default URL comes from an acceptor only; Artemis also looks for a connector of the same name.

When a connection fails, the CLI prints `Connection failed::<reason>` and, unless `--silent` is given, asks for the URL, user and password and tries once more, as Artemis's does.

## Instance lifecycle

### create

Creates a broker instance, as `artemis create` does, from the same templates. Its `bin/artemis` and `bin/saranga` run the `saranga` binary that created it.

```bash
saranga create /var/lib/mybroker --user admin --password admin --require-login
saranga create /tmp/b2 --silent --user admin --password admin --allow-anonymous --port-offset 100
```

| Option | Does |
|---|---|
| `--user`, `--password`, `--role` | The first user and its role (default `amq`). |
| `--allow-anonymous`, `--require-login` | Whether connections without credentials are let in. |
| `--port-offset <n>` | Adds `n` to every port, the web server's included. |
| `--default-port`, `--http-port`, `--host`, `--http-host` | The main acceptor's and the web server's ports and addresses. |
| `--queues`, `--addresses` | Queues and addresses to declare, as `name[:routing-type]`, comma-separated. |
| `--no-amqp-acceptor`, `--no-stomp-acceptor`, `--no-mqtt-acceptor`, `--no-hornetq-acceptor`, `--no-web` | Leave those parts out. |
| `--paging`, `--blocking` | What a full address does. |
| `--data`, `--etc` | Where the data and configuration go. |
| `--clustered`, `--static-cluster`, `--cluster-user`, `--cluster-password`, `--replicated`, `--shared-store`, `--backup`, `--failover-on-shutdown` | Cluster and HA settings. See [Clustering](clustering.md) and [High availability](ha.md). |
| `--force` | Overwrites an existing instance's configuration. |
| `--windows`, `--cygwin` (or `--linux`) | Which scripts to write. Default: those of the system you run on. |

Where it differs from `artemis create`: the journal type is `NIO` unless `--aio`, `--nio` or `--mapped` is given, and the auto-tuning of `journal-buffer-timeout` measures NIO writes; `--jdbc` is refused; Windows gets no `artemis-service.exe`. The Java options (`--java-options`, `--java-memory`) are written to the profiles, for an Artemis that later runs the instance, and are not used by Saranga. See [Getting started](getting-started.md#creating-an-instance) for the layout of an instance.

### upgrade

Rewrites an instance's scripts and profiles for this `saranga`, keeping its data and `broker.xml`, as `artemis upgrade` does: the same steps, the same backups (`old-config-bkp.<n>`), the same kept settings, and the same changes to `bootstrap.xml` and the logging files.

```bash
saranga upgrade /var/lib/mybroker
```

Because the rewritten `bin/artemis` runs this `saranga`, `saranga upgrade` moves an Artemis instance to Saranga, and `artemis upgrade` from an Artemis home moves it back. Use it too after moving the `saranga` binary or building a new one elsewhere. It also writes `bin/saranga` beside `bin/artemis`. Back up the instance first, as Artemis advises. [Migrating from Artemis](migration.md) walks through the move.

### run

Runs the broker in the foreground. `bin/artemis run` calls it for its instance.

```bash
/var/lib/mybroker/bin/artemis run
saranga run --etc /var/lib/mybroker/etc
saranga run --config /var/lib/mybroker/etc/broker.xml
saranga run --bind 127.0.0.1:61616        # no configuration: in memory, no security
```

| Option | Does |
|---|---|
| `--etc <dir>` | Runs the instance with this `etc` directory. Default: the instance `ARTEMIS_INSTANCE` names. |
| `--config <broker.xml>` | Runs this `broker.xml`, with `login.config` and `bootstrap.xml` next to it. |
| `--bind <host:port>` | Listens on this address instead of the configured acceptors. |
| `-D <name=value>` | A system property, for `${name}` in the configuration files; `brokerconfig.` names are broker properties. Repeatable. |
| `--properties <files>` | Broker properties applied over `broker.xml`, as `artemis run --properties`. |
| `--broker <broker.xml>` | Overrides the `broker.xml` that `bootstrap.xml` names. |
| `--allow-kill` | Lets `kill` halt the broker. |
| `[CONFIGURATION]` | Another bootstrap file, `xml:path/to/bootstrap.xml`. |

`artemis run` takes no `-D`, `--config`, `--etc` or `--bind`; the rest are Artemis's. `run` stops cleanly on `STOP_ME`, Ctrl-C and, on Unix, SIGTERM. A configuration URI with no scheme, a scheme other than `xml`, or a missing file is refused with Artemis's `Invalid configuration URI` messages. [Getting started](getting-started.md#saranga-run-directly) and [Configuration](configuration.md) say more.

### stop

Stops the instance's broker, as `artemis stop` does, by writing `STOP_ME` next to its `broker.xml`. The broker sees it within half a second and stops cleanly.

```bash
/var/lib/mybroker/bin/artemis stop
saranga stop --broker /var/lib/mybroker/etc/broker.xml
saranga stop xml:/var/lib/mybroker/etc/bootstrap.xml
```

It finds the instance through `ARTEMIS_INSTANCE` (or `ARTEMIS_INSTANCE_ETC`), a bootstrap URI, or `--broker`. Artemis offers `stop` only from an instance's CLI; Saranga's binary has it too.

### kill

Writes `KILL_ME` next to `broker.xml`. A broker started with `run --allow-kill` halts at once, without a clean stop; any other broker ignores the file. It is meant for failover tests, as in Artemis.

```bash
/var/lib/mybroker/bin/artemis run --allow-kill &
/var/lib/mybroker/bin/artemis kill
```

## Messaging

`producer`, `consumer` and `browser` work as Artemis's do, with the same output. A destination is `queue://name`, `topic://name` or a fully qualified queue name, `address::queue`; the default is `queue://TEST`. See [Artemis's CLI documentation](https://activemq.apache.org/components/artemis/documentation/latest/using-cli.html) for the details.

### producer

Sends messages.

```bash
saranga producer --url tcp://localhost:61616 --user admin --password admin \
  --destination queue://orders --message-count 100 --message "hello"
```

Useful options: `--message-count` (default 1000), `--message` for the text of a `TextMessage`, `--message-size` or `--text-size` for generated bodies, `--properties` for message properties as JSON (`[{"type":"string","key":"region","value":"eu"}]`), `--non-persistent`, `--msgttl`, `--group`, `--commit-interval` to send in transactions, `--threads` and `--sleep`. `--data` (messages from a file) and `--serializer` are not supported yet.

### consumer

Receives messages and acknowledges them.

```bash
saranga consumer --destination queue://orders --message-count 100 --receive-timeout 5000
saranga consumer --destination topic://news --durable --clientID c1 --subscriptionName s1
```

Useful options: `--message-count`, `--receive-timeout` (milliseconds; -1 waits for ever), `--break-on-null` to stop at the first empty receive, `--filter` for a selector, `--durable` with `--subscriptionName` and `--clientID` for a durable subscription, and `--commit-interval`. `--data` and `--serializer` are not supported yet.

### browser

Reads messages without consuming them. It takes `--destination`, `--message-count` and `--filter`. With `--verbose`, a message is printed as `ActiveMQMessage[<id>]:PERSISTENT`, without the Core message details Artemis adds.

### transfer

Moves messages from one queue or topic subscription to another destination, on the same broker or another, in transactions of `--commit-interval` messages (default 1000).

```bash
saranga transfer --source-url tcp://old:61616 --source-queue orders \
                 --target-url tcp://new:61616 --target-queue orders
```

Useful options: `--source-queue` or `--source-topic` (with `--durable-consumer`, `--shared-subscription` or `--shared-durable-subscription`), `--target-queue` or `--target-topic`, `--source-filter`, `--message-count`, `--copy` to leave the source messages in place, and `--source-user`, `--source-password`, `--target-user`, `--target-password`. Only Core is supported: `--source-protocol amqp` and `--target-protocol amqp` are not.

## Administration

These commands manage a running broker through its management API, as Artemis's do. They need a user with the `manage` permission when security is on. See [Management](management.md#the-cli).

### queue

`queue create`, `delete`, `update`, `purge` and `stat`.

```bash
saranga queue create --name jobs --address jobs --anycast --durable \
  --preserve-on-no-consumers --auto-create-address --silent
saranga queue stat --queueName jobs
saranga queue stat --field MESSAGE_COUNT --operation GREATER_THAN --value 0 --json
saranga queue purge --name jobs
saranga queue delete --name jobs
```

`queue create` and `update` ask for any of `--durable`, `--anycast` and the like that you leave out, unless `--silent` is given. `queue stat` filters with `--queueName`, or `--field`, `--operation` and `--value`, prints JSON with `--json`, covers the cluster with `--clustered`, and repeats with `--loop` and `--sleep`; with `--loop` it prints each iteration's time in UTC.

### address

`address create`, `delete`, `update` and `show`.

```bash
saranga address create --name news --multicast --no-anycast
saranga address show                      # every address
saranga address show --name news
```

### user

`user add`, `list`, `reset` and `rm`, for the users of the broker's properties login module, through management.

```bash
saranga user add --user admin --password admin \
  --user-command-user alice --user-command-password secret --role amq
saranga user list --user admin --password admin
saranga user reset --user-command-user alice --role amq,ops
saranga user rm --user-command-user alice
```

`--user` and `--password` are the credentials to connect with; `--user-command-user` and `--user-command-password` are the user being changed. Passwords are stored hashed unless `--plaintext` is given. Artemis offers `user` only from an instance's CLI; Saranga's binary has it too. See [Security](security.md).

### check

`check node` and `check queue` run health checks and exit non-zero when one fails, for monitoring and readiness probes.

```bash
saranga check node --up --diskUsage -1 --memoryUsage 90
saranga check node --backup --peers 2
saranga check queue --name orders --up --produce 1 --consume 1
```

`check node` checks that the broker is started (`--up`, the default), its disk usage (`--diskUsage`, -1 for `max-disk-usage`) and memory usage, that it has a primary (`--primary`, or `--live`) or a backup (`--backup`), and how many peers it sees (`--peers`). `check queue` checks that a queue exists and is not paused, and can send (`--produce`), browse (`--browse`) or consume (`--consume`) messages. `--timeout` (default 30000 ms) and `--fail-at-end` apply to both. `check cluster` is not there yet.

## Data tools

These work on the files of a stopped broker. Like Artemis's, they take the instance's CLI lock, so they refuse an instance whose broker is running. Run them from the instance's `bin`, or give the bootstrap URI. The output is Artemis's; on the same data, `data exp` writes the same XML byte for byte. See [Storage](storage.md#the-data-tools) and [Artemis's data tools](https://activemq.apache.org/components/artemis/documentation/latest/data-tools.html).

### data

| Command | Does |
|---|---|
| `data print` | Prints the bindings and message journals and the page files. `--safe` hides message bodies; `--skip-bindings`, `--skip-journal`, `--max-pages` cut it down; `--reclaimed` tries to print records from reclaimed files too. |
| `data exp` | Exports every message, address and queue as Artemis's XML. `--output` names the file, `--queue` limits it to some queues. |
| `data imp` | Imports such an XML file into a running broker, over Core: `--input` the file, `--url` the broker. |
| `data compact` | Compacts the journal. |
| `data encode`, `data decode` | Turn journal files into Artemis's text encoding and back, with `--directory`, `--prefix`, `--suffix` and `--file-size`. |
| `data recover` | Writes a new journal to `--target` with every message found, rolled-back and acknowledged ones included as far as possible. |

```bash
cd /var/lib/mybroker/bin
./artemis stop
./artemis data print --safe > journal.txt
./artemis data exp --output /backup/messages.xml
saranga data imp --input /backup/messages.xml --url tcp://other:61616 --user admin --password admin
```

`--f` on `print`, `exp` and `recover` skips the lock so a running broker's data can be read; changing data under a running broker can damage it. The folder options (`--journal`, `--bindings`, `--paging`, `--large-messages`) and `--broker` override what `broker.xml` says.

Where they differ from Artemis's: `--jdbc` is refused, since Saranga has no JDBC store; a failed tool prints `Error:<message>` as Artemis's does, without a Java stack trace. The files are the same either way, so Artemis's tools read Saranga's data and Saranga's read Artemis's.

### activation

`activation list` and `activation set` read and set the activation sequences of replication with a lock manager: the local one in the data directory, and the coordinated one in the lock manager (`--local`, `--remote`, `--node-id`, and `--to` for `set`). Use them, with the broker stopped, when recovering a replicated pair, as [Artemis's activation tools](https://activemq.apache.org/components/artemis/documentation/latest/activation-tools.html) describe. See [High availability](ha.md).

## Performance tools

### perf

`perf client`, `perf producer` and `perf consumer` load a broker and report throughput and latency, as Artemis's do, with the same options and reports, `--json` report files and `--hdr` HdrHistogram logs.

```bash
saranga perf client --duration 60 --warmup 10 --persistent --message-size 1024 queue://perf
saranga perf producer --rate 10000 --duration 60 --hdr producer.hdr queue://perf
saranga perf consumer --duration 60 --show-latency queue://perf
```

A send completes as soon as it is written, so `--max-pending` never holds a send back and `total blocked` stays 0. See [Artemis's perf tools](https://activemq.apache.org/components/artemis/documentation/latest/perf-tools.html).

### perf-journal

Measures how fast the journal's disk syncs and suggests a `journal-buffer-timeout`, as `artemis perf-journal` does.

```bash
cd /var/lib/mybroker/bin && ./artemis perf-journal
```

It measures every journal type with plain file writes and syncs, and `--max-aio` has no effect. It also works outside an instance, on `./data/journal`. Saranga's journal groups commits with its own flusher, so `journal-buffer-timeout` has no effect on Saranga itself; the value matters to an Artemis that runs the instance. See [COMPAT.md, Persistence differences](../../COMPAT.md#persistence-differences).

## Utilities

### version

```bash
saranga version
```

```
Saranga 1.0.0 (Apache Artemis 2.57.0)
Saranga home: /opt/saranga
Saranga instance: /var/lib/mybroker
```

### pwd

Prints the instance, home, `etc` and current directories, as `artemis pwd` does.

### mask

Masks a password for the configuration files, as `artemis mask` does. Write the result as `ENC(<result>)` where the configuration takes a masked password, such as an acceptor's key store password in `broker.xml`.

```bash
saranga mask secret                                   # the default codec
saranga mask --key mykey secret                       # the default codec with your own key
saranga mask --password-codec --config etc/broker.xml secret   # broker.xml's <password-codec>
saranga mask --hash secret                            # a one-way hash, for artemis-users.properties
```

The default codec's key can also come from `ARTEMIS_DEFAULT_SENSITIVE_STRING_CODEC_KEY`. See [Security](security.md) and [Artemis's masking passwords](https://activemq.apache.org/components/artemis/documentation/latest/masking-passwords.html).

### shell

Runs the CLI's commands typed at a prompt, as `artemis shell` does. `saranga` with no command starts it too.

```bash
saranga shell --url tcp://localhost:61616 --user admin --password admin
```

In the shell, `connect --url ... --user ... --password ...` keeps a connection's settings for the commands that follow and `disconnect` forgets them. `--history` names the history file, else `ARTEMIS_SHELL_HISTORY`, else the instance's `etc/history-file`. Saranga's shell reads plain lines: there is no line editing, completion or recall. Lines can be piped in, which makes it handy for scripts:

```bash
printf 'queue stat\naddress show\n' | saranga shell --url tcp://localhost:61616 --user admin --password admin
```

### completion

Prints the bash and zsh completion script `artemis completion` prints. For the commands Saranga has, it is the same script.

```bash
source <(./artemis completion)
saranga completion --start-script saranga > /etc/bash_completion.d/saranga
```

### help

Prints Artemis's help text for a command. See [Getting help](#getting-help).

## Not there yet

`artemis properties`, `artemis lock` (which goes with lock coordinators) and `artemis check cluster` have no Saranga counterpart yet, and `saranga help` leaves them out.
