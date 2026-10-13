#!/usr/bin/env bash
# The runbook suite: procedures from the Artemis 2.57.0 user manual, run
# as written through a broker instance's own bin/artemis, against Artemis
# and against Saranga. Their output, normalized, must match
# tests/interop/runbook/expected.txt (recorded with BROKER=artemis).
#
#   tests/interop/runbook/runbook.sh
#
# BROKER picks how the instance is made, and so what its bin/artemis runs:
#   saranga   (default) `saranga create`
#   upgraded  `artemis create`, then `saranga upgrade` (an Artemis instance
#             moved to Saranga)
#   artemis   `artemis create` (Artemis itself)
# UPDATE=1 rewrites expected.txt instead of comparing.
#
# The procedures, each under the manual chapter it comes from:
#   using-server       run an instance, find it active in its log, stop it
#   using-cli          queue create and stat, producer, consumer, browser;
#                      check node and queue, passing and failing;
#                      the shell, connected with connect and with options
#   security           user add, list and rm
#   masking-passwords  mask
#   logging            the audit log turned on while running
#   management         Jolokia reads, as the manual's curl commands do them;
#                      listConnections while a Core consumer, then an
#                      OpenWire one, is connected
#   versions           an acceptor's reload, with the manual's curl command
#   data-retention     journal retention and replay through Jolokia; data
#                      print and recover over the retention directory
#   config-reload      an address added to broker.xml while running
#   perf-tools         perf client; a target rate test with an HDR log
#   data-tools         data exp, compact, encode and decode, and imp into a
#                      second instance; restart recovery
#   graceful-shutdown  a broker stopped while a consumer is connected
#   network-isolation  create --ping; the broker stopped while the network
#                      is down and started again once it is back
#   upgrading          the home's upgrade tool run on a stopped instance;
#                      broker.xml and messages kept, changed files backed up
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/runbook"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
BROKER="${BROKER:-saranga}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
# Under target/: Artemis blocks producers once the disk is more than
# max-disk-usage (90%) full, and the system temp dir is often on such a disk.
mkdir -p "$ROOT/target/runbook"
WORK="$(mktemp -d -p "$ROOT/target/runbook")"
cleanup() {
  for i in "$WORK"/i1 "$WORK"/i2 "$WORK"/i3; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 2
  rm -rf "$WORK"
}
trap cleanup EXIT

# The reference distribution's CLI, outside any instance.
artemis_home() {
  java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis "$@" 2>&1 | grep -v '^WARNING: '
}

# A broker instance, made as BROKER says.
make_instance() {
  local dir="$1" offset="$2"
  shift 2
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --journal-retention 1 "$@")
  case "$BROKER" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) artemis_home create "$(native "$dir")" "${opts[@]}" > /dev/null ;;
    upgraded)
      artemis_home create "$(native "$dir")" "${opts[@]}" > /dev/null
      "$SARANGA" upgrade "$(native "$dir")" > /dev/null 2>&1 ;;
  esac
}

# The instance's bin/artemis, run from its bin/ as the manual runs it.
# Its standard input is the file IN names, or nothing. On Windows it is
# called, so that its status is the command's, as at a cmd prompt: Artemis's
# artemis.cmd ends with endlocal and GOTO :EOF, which a bare cmd /c reports
# as 0.
instance() {
  local dir="$1"; shift
  if windows; then
    (cd "$dir/bin" && timeout 300 cmd /c call "$(cygpath -w "$dir/bin/artemis.cmd")" "$@" < "${IN:-/dev/null}" 2>&1)
  else
    (cd "$dir/bin" && timeout 300 ./artemis "$@" < "${IN:-/dev/null}" 2>&1)
  fi | grep --line-buffered -v -E "NO_ARTEMIS_OOME_DUMP|operable program or batch file|^The system cannot find the file specified" \
    | sed -u -E -e '/^WARNING: /{' -e ':w' -e '$d' -e 'N' -e 's/^WARNING: [^\n]*\n(WARNING: )/\1/' -e 'tw' \
      -e '/^WARNING: [^\n]*\n$/d' -e 's/^WARNING: [^\n]*\n//' -e '}'
  return "${PIPESTATUS[0]}"
}

PIDS=()
start() {
  local dir="$1" port="$2"
  if windows; then
    (cd "$dir/bin" && cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run > "$dir/run.out" 2>&1 &)
  else
    (cd "$dir/bin" && ./artemis run > "$dir/run.out" 2>&1 &)
  fi
  for _ in $(seq 1 300); do
    (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null && return 0
    sleep 0.2
  done
  echo "the broker in $dir did not start"; cat "$dir/run.out"
  return 1
}
wait_down() {
  local port="$1"
  for _ in $(seq 1 300); do
    (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null || return 0
    sleep 0.2
  done
  return 1
}
# "artemis stop" takes no user: it only drops a file the broker watches for.
stop() {
  local dir="$1" port="$2"
  step "artemis stop"
  instance "$dir" stop
  echo "--- exit ${PIPESTATUS[0]}"
  if wait_down "$port"; then echo "stopped"; else echo "the broker in $dir did not stop"; fi
}
# Waits for a line in a file.
wait_for() {
  local file="$1" text="$2"
  for _ in $(seq 1 150); do
    grep -q -- "$text" "$file" 2>/dev/null && return 0
    sleep 0.2
  done
  return 1
}

step() { echo "=== $*"; }
I1="$WORK/i1" I2="$WORK/i2" I3="$WORK/i3"
PORT1=62316 PORT2=62416 PORT3=62516 WEB1=8861 WEB2=8871
AUTH=(--user admin --password admin)
run() {
  step "artemis $*"
  instance "$I1" "$@" "${AUTH[@]}"
  echo "--- exit ${PIPESTATUS[0]}"
}
# Jolokia, as the manual's curl commands call it.
MBEAN='org.apache.activemq.artemis:broker=%220.0.0.0%22'
jolokia() {
  step "curl $1"
  curl -s -H "Origin: http://localhost" -u admin:admin "${@:2}" "http://localhost:$WEB1/console/jolokia/$1"
  echo
}
jolokia_post() {
  step "curl POST $1"
  curl -s -H "Origin: http://localhost" -u admin:admin --header "Content-type: application/json" \
    --request POST --data "$1" "http://localhost:$WEB1/console/jolokia/"
  echo
}
# The shell of the instance i1, the lines of shell.in piped to it:
# Artemis's reads them at its prompts through JLine's dumb terminal. Its
# history goes to ARTEMIS_SHELL_HISTORY, which an instance's Linux profile
# sets to ~/.artemis_history; set here, the shell asks nothing on either
# platform. The banner and the prompt name the broker, and Saranga has no
# <TAB> completion to offer.
shell_lines() { printf '%s\n' "$@" > "$WORK/shell.in"; }
shell() {
  IN="$WORK/shell.in" ARTEMIS_SHELL_HISTORY="$(native "$WORK/shell-history")" instance "$I1" "$@" | sed -E -e 's/\x1b\[[0-9;]*m//g' -e 's/\r$//' \
    -e '/^ *[_/]/d' -e 's/^ (Apache Artemis|Saranga) [0-9.]+$/ BANNER/' \
    -e 's/(Apache Artemis|Saranga) [0-9.]+ > /CLI > /g' -e 's/ or press <TAB>//'
  echo  # the last prompt ends no line
}
# The lines of data print about messages sent to an address.
messages_to() { grep -c -E "userRecordType=(45|31);.*address=$1[],]" || true; }

suite() {
  make_instance "$I1" 700
  make_instance "$I2" 800

  # using-server: "bin/artemis run", then the log says it is active.
  step "using-server: artemis run"
  start "$I1" "$PORT1" && echo "listening"
  wait_for "$I1/log/artemis.log" "AMQ221007" && echo "log: AMQ221007: Server is now active"

  # logging: the audit loggers turned on in log4j2.properties, which is
  # reread every monitorInterval (5) seconds.
  step "logging: audit loggers set to INFO"
  sed -i -E 's/^(logger\.audit_(base|resource|message)\.level) = OFF/\1 = INFO/' "$I1/etc/log4j2.properties"
  sleep 8

  # using-cli.
  run queue create --name runbook --anycast --durable --auto-create-address --silent
  run producer --destination queue://runbook --message-count 100
  run queue stat --queueName runbook
  run consumer --destination queue://runbook --message-count 40
  run browser --destination queue://runbook --message-count 1
  run queue stat --queueName runbook

  # using-cli: check, which ends with status 1 when a check fails (versions,
  # 2.39.0).
  run check node --up --diskUsage 99 --memoryUsage 99
  run check queue --name runbook --up --browse 59 --produce 1 --consume 1
  run check queue --name missing

  # security: users of the properties login module.
  run user add --user-command-user guest --user-command-password guest --role admin
  run user list --user-command-user guest
  run user rm --user-command-user guest

  # masking-passwords.
  step "artemis mask"
  instance "$I1" mask secret

  # management: the manual's Jolokia reads.
  jolokia "read/$MBEAN/Active"
  jolokia_post '{"attribute": "Active", "mbean": "org.apache.activemq.artemis:broker=\"0.0.0.0\"", "type": "read"}'
  jolokia "exec/$MBEAN/listQueues/%7B%22field%22:%22name%22,%22operation%22:%22EQUALS%22,%22value%22:%22runbook%22%7D/1/10"

  # management: the connections, with a consumer connected.
  instance "$I1" consumer --destination queue://waiting --message-count 1 \
    --receive-timeout 30000 "${AUTH[@]}" > "$WORK/waiting.out" &
  local waiting=$!
  wait_for "$WORK/waiting.out" "messages are consumed" && sleep 1
  jolokia "exec/$MBEAN/listConnections/%22%22/-1/-1"
  jolokia_post '{"operation": "listConnections", "mbean": "org.apache.activemq.artemis:broker=\"0.0.0.0\"", "type": "exec", "arguments": [ "{\"field\":\"protocol\",\"operation\":\"EQUALS\",\"value\":\"CORE\"}", -1, -1 ] }'
  instance "$I1" producer --destination queue://waiting --message-count 1 "${AUTH[@]}" > /dev/null
  wait "$waiting"

  # management: the OpenWire connections, with the ActiveMQ 5 client that
  # Artemis ships connected.
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/OpenWireWait.java")" \
    "tcp://localhost:$PORT1" admin admin openwire > "$WORK/openwire.out" 2>&1 &
  waiting=$!
  wait_for "$WORK/openwire.out" "connected" && sleep 1
  jolokia "exec/$MBEAN/listConnections/%7B%22field%22:%22protocol%22,%22operation%22:%22EQUALS%22,%22value%22:%22OPENWIRE%22%7D/-1/-1"
  jolokia_post '{"operation": "listConnections", "mbean": "org.apache.activemq.artemis:broker=\"0.0.0.0\"", "type": "exec", "arguments": [ "{\"field\":\"protocol\",\"operation\":\"EQUALS\",\"value\":\"OPENWIRE\"}", -1, -1 ] }'
  instance "$I1" producer --destination queue://openwire --message-count 1 "${AUTH[@]}" > /dev/null
  wait "$waiting"
  grep -v '^WARNING' "$WORK/openwire.out"

  # versions (2.18.0): the keystores reloaded through the acceptor's reload
  # operation, with the manual's curl command. Jolokia's strict checking
  # refuses it without an Origin, so it is run again with the Origin the
  # management chapter gives; the acceptor takes clients again.
  local reload='{"type":"exec", "mbean":"org.apache.activemq.artemis:broker=\"0.0.0.0\",component=acceptors,name=\"artemis\"", "operation":"reload"}'
  step "versions: curl reload of the acceptor artemis"
  curl -s --user admin:admin --header "Content-Type: application/json" --request POST \
    --data "$reload" "http://localhost:$WEB1/console/jolokia/exec"
  echo
  step "versions: curl reload of the acceptor artemis, with an Origin"
  curl -s -H "Origin: http://localhost" --user admin:admin --header "Content-Type: application/json" \
    --request POST --data "$reload" "http://localhost:$WEB1/console/jolokia/exec"
  echo
  run queue stat --queueName runbook

  # logging: what the audit log says of the queue made, the Jolokia calls
  # and the connections listed.
  step "logging: log/audit.log"
  grep -E 'AMQ601707|AMQ601108|AMQ601022|AMQ601019: .*(ActiveMQServerControlImpl|AcceptorControlImpl)' "$I1/log/audit.log"

  # data-retention: the history keeps what was sent; replay sends it again.
  run producer --destination queue://history --message-count 600 --message-size 20000
  run queue create --name replayed --anycast --durable --auto-create-address --silent
  jolokia_post '{"type": "exec", "mbean": "org.apache.activemq.artemis:broker=\"0.0.0.0\"", "operation": "replay(java.lang.String,java.lang.String,java.lang.String)", "arguments": ["history", "replayed", null]}'
  run queue stat --queueName replayed
  jolokia_post '{"type": "exec", "mbean": "org.apache.activemq.artemis:broker=\"0.0.0.0\"", "operation": "replay(java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)", "arguments": ["20000101000000", "20000102000000", "history", "replayed", null]}'
  run queue stat --queueName replayed

  # config-reload: an address and queue added to broker.xml are deployed.
  step "config-reload: add the address reloaded to broker.xml"
  sed -i 's#</addresses>#   <address name="reloaded"><anycast><queue name="reloaded"/></anycast></address>\n      </addresses>#' "$I1/etc/broker.xml"
  wait_for "$I1/log/artemis.log" "Deploying ANYCAST queue reloaded" && echo "log: deployed queue reloaded"
  run queue stat --queueName reloaded

  # perf-tools.
  step "artemis perf client"
  instance "$I1" perf client --message-count 100 --max-pending 100 "${AUTH[@]}" queue://perf \
    | grep -E "^--- (total (sent|completed|received)|result)"
  # perf-tools: a target rate test, its latencies kept in an HDR histogram
  # log. Rates and latencies differ from run to run; what each prints is
  # compared.
  step "artemis perf client --rate 500 --hdr ../rate.hdr --warmup 1 --duration 3 --max-pending 100 --show-latency --url tcp://localhost:$PORT1?confirmationWindowSize=20000 --consumer-url tcp://localhost:$PORT1 queue://TEST_QUEUE"
  instance "$I1" perf client --rate 500 --hdr ../rate.hdr --warmup 1 --duration 3 --max-pending 100 \
    --show-latency --url "tcp://localhost:$PORT1?confirmationWindowSize=20000" \
    --consumer-url "tcp://localhost:$PORT1" "${AUTH[@]}" queue://TEST_QUEUE \
    | sed -n -E -e '/^Connection brokerURL/p' -e '/^--- SUMMARY/,$p' | sed -E 's/[0-9]+(\.[0-9]+)?/#/g; s/ +/ /g; s/ $//'
  step "the HDR histogram log"
  head -2 "$I1/rate.hdr" | tr -d '\r'
  grep -o '^Tag=[^,]*' "$I1/rate.hdr" | sort -u

  # using-cli: the shell, bin/artemis alone. connect keeps a connection
  # for the commands after it, and the end of the input disconnects, then
  # ends the shell.
  step "using-cli: artemis (the shell): connect, then queue stat"
  shell_lines "connect --user=admin --password=admin --url tcp://localhost:$PORT1" "queue stat --queueName runbook"
  shell
  step "using-cli: artemis shell --user admin --password admin --url tcp://localhost:$PORT1: queue stat, exit"
  shell_lines "queue stat --queueName runbook" exit
  shell shell --user admin --password admin --url "tcp://localhost:$PORT1"

  # using-server: stop, and the log says so.
  stop "$I1" "$PORT1"
  wait_for "$I1/log/artemis.log" "AMQ221002" && echo "log: AMQ221002: stopped"

  # data-retention: the CLI over the retention directory.
  step "artemis data print --journal ../data/retention"
  instance "$I1" data print --journal ../data/retention | messages_to history
  step "artemis data recover --journal ../data/retention --target ../data/recovered --large-messages ../data/large-messages"
  instance "$I1" data recover --journal ../data/retention --target ../data/recovered \
    --large-messages ../data/large-messages > /dev/null
  echo "--- exit ${PIPESTATUS[0]}"
  step "artemis data print --journal ../data/recovered"
  instance "$I1" data print --journal ../data/recovered | messages_to history

  # data-tools: export, compact, import elsewhere; restart recovery.
  step "artemis data exp"
  instance "$I1" data exp > "$WORK/export.xml"
  echo "messages: $(grep -c '<message ' "$WORK/export.xml")"
  step "artemis data compact"
  instance "$I1" data compact
  # encode and decode default to broker.xml's journal-directory as written,
  # data/journal, which from bin/ is not there: the directory is given.
  step "artemis data encode"
  instance "$I1" data encode | grep -c '^operation@'
  step "artemis data encode --directory ../data/journal"
  instance "$I1" data encode --directory ../data/journal > "$WORK/journal.txt"
  echo "--- exit ${PIPESTATUS[0]}"
  grep -o '^operation@[A-Za-z]*' "$WORK/journal.txt" | sort -u
  step "artemis data decode --input ../../journal.txt --directory ../data/decoded"
  instance "$I1" data decode --input ../../journal.txt --directory ../data/decoded
  echo "--- exit ${PIPESTATUS[0]}"
  step "artemis data encode --directory ../data/decoded"
  instance "$I1" data encode --directory ../data/decoded > "$WORK/decoded.txt"
  # The same records, in any order; decode writes them uncompacted.
  records() { grep '^operation@' "$1" | tr -d '\r' | sed -E 's/,compactCount@[0-9]+//' | sort; }
  if diff <(records "$WORK/journal.txt") <(records "$WORK/decoded.txt") > /dev/null; then
    echo "the decoded journal holds the encoded records"
  else
    echo "the decoded journal differs"
  fi

  # web-server: a binding updated with a webconfig. system property, added
  # to the JAVA_ARGS of the profile (which ends with no line end).
  step "web-server: -Dwebconfig.bindings.artemis.uri=http://localhost:$WEB2 in JAVA_ARGS"
  if windows; then
    printf '\r\nset JAVA_ARGS=%%JAVA_ARGS%% -Dwebconfig.bindings.artemis.uri=http://localhost:%s\r\n' "$WEB2" >> "$I1/etc/artemis.profile.cmd"
  else
    printf '\nJAVA_ARGS="$JAVA_ARGS -Dwebconfig.bindings.artemis.uri=http://localhost:%s"\n' "$WEB2" >> "$I1/etc/artemis.profile"
  fi
  step "using-server: artemis run, again"
  start "$I1" "$PORT1" && echo "listening"
  for _ in $(seq 1 100); do
    curl -s -o /dev/null "http://localhost:$WEB2/" && break
    sleep 0.2
  done
  step "curl read/$MBEAN/Active, on port $WEB2"
  curl -s -H "Origin: http://localhost" -u admin:admin "http://localhost:$WEB2/console/jolokia/read/$MBEAN/Active"
  echo
  step "curl read/$MBEAN/Active, on port $WEB1"
  curl -s -o /dev/null -H "Origin: http://localhost" -u admin:admin "http://localhost:$WEB1/console/jolokia/read/$MBEAN/Active" \
    || echo "nothing listening"
  run queue stat --queueName runbook
  run queue stat --queueName replayed
  run queue stat --queueName reloaded
  stop "$I1" "$PORT1"

  step "data-tools: artemis data imp into another instance"
  start "$I2" "$PORT2" && echo "listening"
  instance "$I2" data imp --input "$(native "$WORK/export.xml")" --host localhost --port "$PORT2" "${AUTH[@]}"
  echo "--- exit ${PIPESTATUS[0]}"
  for q in runbook history replayed; do
    step "artemis queue stat --queueName $q (imported)"
    instance "$I2" queue stat --queueName "$q" "${AUTH[@]}"
  done
  stop "$I2" "$PORT2"

  # graceful-shutdown: a stopping broker waits for its clients, for ever
  # or for graceful-shutdown-timeout.
  step "graceful-shutdown: graceful-shutdown-enabled, no timeout"
  sed -i 's#<persistence-enabled>true</persistence-enabled>#&\n      <graceful-shutdown-enabled>true</graceful-shutdown-enabled>#' "$I2/etc/broker.xml"
  graceful 15000 "the consumer disconnected"
  step "graceful-shutdown: graceful-shutdown-timeout 2000"
  sed -i 's#<graceful-shutdown-enabled>true</graceful-shutdown-enabled>#&\n      <graceful-shutdown-timeout>2000</graceful-shutdown-timeout>#' "$I2/etc/broker.xml"
  graceful 60000 "the timeout passed"

  network
  upgrading
}

# upgrading: "artemis upgrade" of the broker's home run on the stopped i3,
# which keeps broker.xml and the data and backs up the files it changes in
# old-config-bkp.<n> (.1 for an instance BROKER=upgraded already upgraded).
upgrading() {
  local before="$WORK/broker.xml.before" bkp
  # The last stop's AMQ221002 comes after its port closes.
  sleep 2
  cp "$I3/etc/broker.xml" "$before"
  step "upgrading: artemis upgrade, from the home's bin"
  case "$BROKER" in
    artemis) artemis_home upgrade "$(native "$I3")" > "$WORK/upgrade.out"; echo "--- exit ${PIPESTATUS[0]}" ;;
    *) "$SARANGA" upgrade "$(native "$I3")" > "$WORK/upgrade.out" 2>&1; echo "--- exit $?" ;;
  esac
  cmp -s "$before" "$I3/etc/broker.xml" && echo "broker.xml kept"
  bkp="$(ls -d "$I3"/old-config-bkp.* | sort -t. -k2 -n | tail -1)"
  local missing=0 n=0 f
  for f in "$bkp"/bin/* "$bkp"/etc/*; do
    [[ -e "$f" ]] || continue
    n=$((n + 1))
    [[ -e "$I3/${f#"$bkp"/}" ]] || { echo "backed up but gone: ${f#"$bkp"/}"; missing=1; }
  done
  (( n > 0 && missing == 0 )) && echo "the changed scripts and profiles backed up in old-config-bkp"
  step "upgrading: artemis run"
  start "$I3" "$PORT3" && echo "listening"
  step "artemis queue stat --queueName network"
  instance "$I3" queue stat --queueName network "${AUTH[@]}"
  step "artemis consumer --destination queue://network --message-count 1"
  instance "$I3" consumer --destination queue://network --message-count 1 "${AUTH[@]}"
  stop "$I3" "$PORT3"
}

# network-isolation: an instance made with --ping, whose broker stops while
# no address of network-check-list answers and starts again once one does.
# Its ping command is changed to one that answers while a file exists.
network() {
  local log="$I3/log/artemis.log" flag
  flag="$(native "$WORK/network-up")"
  step "network-isolation: artemis create --ping 192.0.2.1"
  make_instance "$I3" 900 --ping 192.0.2.1
  grep -o "<network-check-list>.*</network-check-list>" "$I3/etc/broker.xml"
  local ping="test -e $flag"
  windows && ping="cmd /C if exist $flag (exit 0) else (exit 1)"
  sed -i -e "s#<network-check-period>[0-9]*<#<network-check-period>1000<#" \
    -e "s#<network-check-ping-command>.*</network-check-ping-command>#<network-check-ping-command>$ping</network-check-ping-command>#" \
    "$I3/etc/broker.xml"
  touch "$flag"
  start "$I3" "$PORT3" && echo "listening"
  # Artemis checks the network once the broker is active and before its
  # web server starts; an outage between the two ends its run.
  wait_for "$log" "AMQ241001"
  step "the network goes down"
  rm "$flag"
  wait_for "$log" "AMQ221002" && grep -o "AMQ201001: .*" "$log"
  wait_down "$PORT3" && echo "stopped"
  step "the network comes back"
  touch "$flag"
  wait_for "$log" "AMQ201000" && grep -o "AMQ201000: .*" "$log"
  for _ in $(seq 1 150); do
    (echo > "/dev/tcp/127.0.0.1/$PORT3") 2>/dev/null && { echo "listening"; break; }
    sleep 0.2
  done
  step "artemis producer --destination queue://network --message-count 1"
  instance "$I3" producer --destination queue://network --message-count 1 "${AUTH[@]}"
  step "artemis queue stat --queueName network"
  instance "$I3" queue stat --queueName network "${AUTH[@]}"
  stop "$I3" "$PORT3"
}

# The broker of i2 stopped while a consumer waits receive-timeout ms for a
# message that does not come.
graceful() {
  local timeout="$1" until="$2" log="$I2/log/artemis.log" out="$WORK/consumer.out"
  # The last stop's AMQ221002 comes after its port closes.
  wait_for "$log" "AMQ221002" && sleep 1
  : > "$log"
  start "$I2" "$PORT2" && echo "listening"
  instance "$I2" consumer --destination queue://graceful --message-count 1 \
    --receive-timeout "$timeout" --break-on-null "${AUTH[@]}" > "$out" &
  local consumer=$!
  wait_for "$out" "messages are consumed" && sleep 1
  local stopped_at=$SECONDS
  step "artemis stop"
  instance "$I2" stop
  echo "--- exit ${PIPESTATUS[0]}"
  sleep 1
  if grep -q AMQ221002 "$log"; then echo "log: stopped at once"; else echo "log: still running"; fi
  wait "$consumer"
  step "artemis consumer (connected while the broker stopped)"
  cat "$out"
  wait_for "$log" "AMQ221002" && echo "log: AMQ221002: stopped once $until"
  if (( SECONDS - stopped_at < 30 )); then echo "the consumer ended with the broker"; fi
  wait_down "$PORT2" || echo "the broker in $I2 did not stop"
}

# Timings, IDs, paths and dates differ between runs and brokers.
normalize() {
  local uuid='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
  sed -E \
    -e 's/\r$//' \
    -e 's/\x1b\[[0-9;]*m//g' \
    -e "s#$(native "$WORK" | sed 's#[.]#\\.#g')#WORK#g" \
    -e "s#$(cygpath -w "$WORK" 2>/dev/null | sed 's#\\#\\\\#g; s#[.]#\\.#g')#WORK#g" \
    -e 's#WORK\\#WORK/#g' -e ':sep' -e 's#(WORK/[a-z0-9/]*)\\#\1/#' -e 'tsep' \
    -e 's/^Usage: (artemis|saranga) help/Usage: CLI help/' \
    -e 's/(Elapsed time in milli second :) [0-9]+ milli/\1 # milli/' \
    -e 's/(Elapsed time in second :) [0-9]+ s/\1 # s/' \
    -e 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3} /DATE /' \
    -e 's/"timestamp":[0-9]+/"timestamp":#/g' \
    -e 's/"id":"?[0-9]+"?/"id":#/g' \
    -e 's/\\"id\\":\\"[0-9]+\\"/\\"id\\":#/g' \
    -e 's/(\\"clientID\\":\\"ID:)[^\\]*/\1#/g' \
    -e 's/(JMS Message ID:|messageID=|ID:)[^ ,\\]*/\1#/g' \
    -e "s/$uuid/UUID/g" \
    -e 's/^result: .*/result: MASKED/' \
    -e 's/(Total time|time): [0-9.]+/\1: #/' \
    -e 's/(Time elapsed:) [0-9.]+ sec/\1 # sec/' \
    -e 's/^DATE \[AUDIT\]\(([^()]|\([^()]*\))*\) /[AUDIT] /' \
    -e 's/@127\.0\.0\.1:[0-9]+/@127.0.0.1:PORT/g; s/(ControlImpl|NettyAcceptor)@[0-9a-f]+/\1@HASH/g' \
    -e 's/(\\"connectionID\\":\\")[0-9a-f]+/\1#/g; s/(\\"remoteAddress\\":\\"127\.0\.0\.1:)[0-9]+/\1#/g' \
    -e 's/(\\"creationTime\\":\\")[^\\]*/\1DATE/g' \
    -e 's/"(lastValue|createdTimestamp|messagesAdded|persistentSize|durablePersistentSize|deliveringSize|scheduledSize)":"?[0-9]+"?/"\1":#/g'
}

OUT="$(mktemp)"
suite | normalize > "$OUT"
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/expected.txt"
  echo "== updated $HERE/expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/expected.txt" "$OUT"; then
  echo "ALL PASSED ($BROKER)"
  rm -f "$OUT"
else
  echo "FAILED ($BROKER; output in $OUT)"
  exit 1
fi
