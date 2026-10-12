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
#   using-cli          queue create and stat, producer, consumer, browser
#   security           user add, list and rm
#   masking-passwords  mask
#   management         Jolokia reads, as the manual's curl commands do them
#   data-retention     journal retention and replay through Jolokia; data
#                      print and recover over the retention directory
#   config-reload      an address added to broker.xml while running
#   perf-tools         perf client
#   data-tools         data exp, compact, and imp into a second instance;
#                      restart recovery
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
WORK="$(mktemp -d)"
cleanup() {
  for i in "$WORK"/i1 "$WORK"/i2; do
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
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --journal-retention 1)
  case "$BROKER" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) artemis_home create "$(native "$dir")" "${opts[@]}" > /dev/null ;;
    upgraded)
      artemis_home create "$(native "$dir")" "${opts[@]}" > /dev/null
      "$SARANGA" upgrade "$(native "$dir")" > /dev/null 2>&1 ;;
  esac
}

# The instance's bin/artemis, run from its bin/ as the manual runs it.
instance() {
  local dir="$1"; shift
  if windows; then
    (cd "$dir/bin" && timeout 300 cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$dir/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi | grep -v -E "^WARNING: |NO_ARTEMIS_OOME_DUMP|operable program or batch file|^The system cannot find the file specified"
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
I1="$WORK/i1" I2="$WORK/i2"
PORT1=62316 PORT2=62416 WEB1=8861
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
# The lines of data print about messages sent to an address.
messages_to() { grep -c -E "userRecordType=(45|31);.*address=$1[,\]]" || true; }

suite() {
  make_instance "$I1" 700
  make_instance "$I2" 800

  # using-server: "bin/artemis run", then the log says it is active.
  step "using-server: artemis run"
  start "$I1" "$PORT1" && echo "listening"
  wait_for "$I1/log/artemis.log" "AMQ221007" && echo "log: AMQ221007: Server is now active"

  # using-cli.
  run queue create --name runbook --anycast --durable --auto-create-address --silent
  run producer --destination queue://runbook --message-count 100
  run queue stat --queueName runbook
  run consumer --destination queue://runbook --message-count 40
  run browser --destination queue://runbook --message-count 1
  run queue stat --queueName runbook

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

  # using-server: stop, and the log says so.
  run stop
  wait_down "$PORT1" && echo "stopped"
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
  step "using-server: artemis run, again"
  start "$I1" "$PORT1" && echo "listening"
  run queue stat --queueName runbook
  run queue stat --queueName replayed
  run queue stat --queueName reloaded
  run stop
  wait_down "$PORT1" && echo "stopped"

  step "data-tools: artemis data imp into another instance"
  start "$I2" "$PORT2" && echo "listening"
  instance "$I2" data imp --input "$(native "$WORK/export.xml")" --port "$PORT2" "${AUTH[@]}"
  echo "--- exit ${PIPESTATUS[0]}"
  for q in runbook history replayed; do
    step "artemis queue stat --queueName $q (imported)"
    instance "$I2" queue stat --queueName "$q" "${AUTH[@]}"
  done
  instance "$I2" stop > /dev/null
  wait_down "$PORT2" && echo "stopped"
}

# Timings, IDs, paths and dates differ between runs and brokers.
normalize() {
  local uuid='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
  sed -E \
    -e 's/\r$//' \
    -e 's/\x1b\[[0-9;]*m//g' \
    -e "s#$(native "$WORK" | sed 's#[.]#\\.#g')#WORK#g" \
    -e "s#$(cygpath -w "$WORK" 2>/dev/null | sed 's#\\#\\\\#g; s#[.]#\\.#g')#WORK#g" \
    -e 's#WORK\\#WORK/#g; s#(WORK/[a-z0-9/]*)\\#\1/#g' \
    -e 's/^Usage: (artemis|saranga) help/Usage: CLI help/' \
    -e 's/(Elapsed time in milli second :) [0-9]+ milli/\1 # milli/' \
    -e 's/"timestamp":[0-9]+/"timestamp":#/g' \
    -e 's/"id":"?[0-9]+"?/"id":#/g' \
    -e 's/(JMS Message ID:|messageID=|ID:)[^ ,]*/\1#/g' \
    -e "s/$uuid/UUID/g" \
    -e 's/^result: .*/result: MASKED/' \
    -e 's/(Total time|time): [0-9.]+/\1: #/' \
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
