#!/usr/bin/env bash
# The failback suite: a shared-store primary and backup made by
# "artemis create --shared-store --static-cluster", each pointing at the
# other, run through their instances' own bin/artemis, with the Artemis JMS
# client failing over between them (Failover.java). The output, normalized,
# must match tests/interop/ha/failback.expected.txt (recorded with Artemis
# in both roles).
#
#   PRIMARY=saranga|artemis BACKUP=saranga|artemis tests/interop/ha/failback.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. The backup announces itself to the primary, whose topology pairs
#      them.
#   2. The primary is killed: the client fails over to the backup and
#      receives what it sent to the primary. So does a consumer of the
#      primary's own CLI, given its URL with reconnectAttempts: it receives
#      what was sent to the primary, then what is sent to the backup.
#   3. The primary, started again, announces itself to the active backup,
#      which gives way and waits as a backup again; the client fails back
#      and receives what it sent to the backup.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/ha"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
PRIMARY="${PRIMARY:-saranga}" BACKUP="${BACKUP:-saranga}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/ha"
WORK="$(mktemp -d -p "$ROOT/target/ha")"
P="$WORK/primary" B="$WORK/backup" SHARED="$WORK/shared" MARK="$WORK/marker"
PORTP=62616 PORTB=62716
CLIENT_PID= CLI_PID=
cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  [[ -n "$CLI_PID" ]] && kill "$CLI_PID" 2>/dev/null
  for i in "$P" "$B"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  if [[ -n "${KEEP:-}" ]]; then
    echo "kept $WORK" >&2
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

# An instance made by Artemis or by Saranga with create --shared-store, its
# data in the shared directory.
make_instance() {
  local kind="$1" dir="$2" offset="$3" other="$4"; shift 4
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --shared-store --host localhost
    --static-cluster "tcp://localhost:$other" --cluster-user cu --cluster-password cpw "$@")
  case "$kind" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac
  local shared
  shared="$(native "$SHARED")"
  sed -i -E \
    -e "s#<(paging|bindings|journal|large-messages)-directory>[^<]*<#<\1-directory>$shared/\1<#" \
    "$dir/etc/broker.xml"
}

# As the runbook suite runs bin/artemis.
instance() {
  local dir="$1"; shift
  if windows; then
    (cd "$dir/bin" && timeout 300 cmd /c call "$(cygpath -w "$dir/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$dir/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi | grep --line-buffered -v -E "NO_ARTEMIS_OOME_DUMP|operable program or batch file|^The system cannot find the file specified" \
    | perl -ne 'BEGIN { $| = 1 } s/\r\n\z/\n/; if (/^WARNING: /) { $w = 1; next } $b = $w && /^$/; $w = 0; print unless $b'
  return "${PIPESTATUS[0]}"
}

# "artemis run --allow-kill" in the background, a line added to the
# instance's "exited" file when the process ends.
launch() {
  local dir="$1"
  mkdir -p "$dir/log"
  if windows; then
    (cd "$dir/bin" && { cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run --allow-kill >> "$dir/run.out" 2>&1; echo >> "$dir/exited"; } &)
  else
    (cd "$dir/bin" && { ./artemis run --allow-kill >> "$dir/run.out" 2>&1; echo >> "$dir/exited"; } &)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_up() {
  for _ in $(seq 1 300); do up "$1" && return 0; sleep 0.2; done
  return 1
}
wait_down() {
  for _ in $(seq 1 300); do up "$1" || return 0; sleep 0.2; done
  return 1
}
# Waits for the n-th "artemis run" of an instance to end (default the
# first). A stop is waited for so rather than by its AMQ221002: Artemis's
# Run stops the broker from a daemon timer thread, so once the broker's last
# non-daemon threads end the JVM exits by itself, and its shutdown hooks
# (log4j's among them) can race the rest of the stop, losing that line.
wait_exit() {
  local dir="$1" n="${2:-1}"
  for _ in $(seq 1 600); do
    [[ "$(grep -c "" "$dir/exited" 2>/dev/null)" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}
# Waits for the n-th line matching text (default the first).
wait_for() {
  local file="$1" text="$2" n="${3:-1}"
  for _ in $(seq 1 600); do
    [[ "$(grep -c -- "$text" "$file" 2>/dev/null)" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}
# The lines of a broker's log that tell what it did since the last call,
# its IDs, versions and times left out.
declare -A SEEN
said() {
  local who="$1" dir="$2" from="${SEEN[$1]:-0}"
  local lines
  lines="$(grep -o -E 'AMQ22(1000|1002|1007|1008|1010|1031|1032|1033|1034|1035|1109|2308)[^,]*' "$dir/log/artemis.log")"
  SEEN[$who]="$(printf '%s\n' "$lines" | grep -c .)"
  printf '%s\n' "$lines" | tail -n +"$((from + 1))" | grep . \
    | sed -E -e 's/(Apache Artemis|Saranga) Backup Server version [0-9.]+ \[[^]]*\]/BACKUP [ID]/' \
      -e 's/(AMQ221002): .*/\1: stopped/' \
      -e 's/(AMQ221000: [A-Za-z]+) message broker is starting.*/\1 message broker is starting/' \
      -e 's/(because) .* (is null)\./\1 X \2./' \
    | sed "s/^/$who: /"
}

step() { echo "=== $*"; }
client() {
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Failover.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at "
}

suite() {
  make_instance "$PRIMARY" "$P" 1000 "$PORTB" --failover-on-shutdown
  make_instance "$BACKUP" "$B" 1100 "$PORTP" --backup

  # 1. The backup announces itself to the primary.
  step "artemis run (primary)"
  launch "$P"
  wait_up "$PORTP" && wait_for "$P/log/artemis.log" AMQ221007 && echo "primary: listening"
  step "artemis run (backup)"
  launch "$B"
  wait_for "$B/log/artemis.log" AMQ221031 && echo "backup: announced"
  said primary "$P"
  said backup "$B"
  step "Failover.java topology tcp://localhost:$PORTP"
  client topology "tcp://localhost:$PORTP"

  # 2. Failover: the primary killed.
  step "Failover.java failover (in the background)"
  local out="$WORK/client.out"
  client failover "tcp://localhost:$PORTP?ha=true&reconnectAttempts=-1&retryInterval=500" "$(native "$MARK")" 10 > "$out" &
  CLIENT_PID=$!
  wait_for "$out" "sent 10" && echo "client: sent 10"
  step "artemis producer (primary)"
  instance "$P" producer --user admin --password admin --url "tcp://localhost:$PORTP" \
    --destination queue://cli --message-count 10 | grep -o "Produced: .*"
  step "artemis consumer --url tcp://localhost:$PORTP?ha=true;reconnectAttempts=-1 (in the background)"
  local cli="$WORK/cli.out"
  instance "$P" consumer --user admin --password admin \
    --url "tcp://localhost:$PORTP?ha=true;reconnectAttempts=-1;retryInterval=500" \
    --destination queue://cli --message-count 20 --receive-timeout -1 > "$cli" &
  CLI_PID=$!
  # Its first ten received and acknowledged.
  sleep 5
  step "artemis kill (primary)"
  instance "$P" kill > /dev/null
  wait_down "$PORTP" && echo "primary: down"
  wait_up "$PORTB" && wait_for "$B/log/artemis.log" AMQ221010 && echo "backup: listening"
  said backup "$B"
  touch "$MARK.1"
  wait_for "$out" "received" && grep -E "^(failover event|received)" "$out" | sed 's/^/client: /'
  step "artemis producer (backup)"
  instance "$B" producer --user admin --password admin --url "tcp://localhost:$PORTB" \
    --destination queue://cli --message-count 10 | grep -o "Produced: .*"
  wait "$CLI_PID"
  CLI_PID=
  grep -o "Consumed: .*" "$cli" | sed "s/^/consumer: /"

  # 3. Failback: the primary started again.
  wait_for "$out" "sent 10" 2 && echo "client: sent 10"
  step "artemis run (primary)"
  launch "$P"
  wait_for "$B/log/artemis.log" AMQ221008 && echo "backup: failing back"
  wait_up "$PORTP" && wait_for "$P/log/artemis.log" AMQ221007 2 && echo "primary: listening"
  wait_for "$B/log/artemis.log" AMQ221031 2 && echo "backup: announced"
  said primary "$P"
  said backup "$B"
  touch "$MARK.2"
  wait "$CLIENT_PID"
  CLIENT_PID=
  sed -n '/^received/,$p' "$out" | tail -n +2 | grep -E "^(failover event|received)" | sed 's/^/client: /'
  step "Failover.java topology tcp://localhost:$PORTP"
  client topology "tcp://localhost:$PORTP"

  step "artemis stop (backup)"
  instance "$B" stop > /dev/null
  wait_exit "$B" && echo "backup: stopped"
  step "artemis stop (primary)"
  instance "$P" stop > /dev/null
  wait_down "$PORTP" && echo "primary: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/failback.expected.txt"
  echo "== updated $HERE/failback.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/failback.expected.txt" "$OUT"; then
  echo "ALL PASSED ($PRIMARY primary, $BACKUP backup)"
  rm -f "$OUT"
else
  echo "FAILED ($PRIMARY primary, $BACKUP backup; output in $OUT)"
  exit 1
fi
