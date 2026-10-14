#!/usr/bin/env bash
# The replication suite: a primary and a backup made by
# "artemis create --replicated --static-cluster", each pointing at the
# other and each with its own data, run through their instances' own
# bin/artemis, with the Artemis JMS client failing over between them
# (Failover.java). The output, normalized, must match
# tests/interop/ha/replication.expected.txt (recorded with Artemis in both
# roles).
#
#   PRIMARY=saranga|artemis BACKUP=saranga|artemis tests/interop/ha/replication.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. The primary has messages, two of them large, before the backup
#      starts: the backup copies its journals and large messages.
#   2. Then messages sent, and some consumed, reach the backup as they are
#      written.
#   3. The primary is killed: the backup becomes active with what was left
#      on the primary, the client fails over to it, and the CLI consumes
#      the rest there.
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
P="$WORK/primary" B="$WORK/backup" MARK="$WORK/marker"
PORTP=63616 PORTB=63716
CLIENT_PID=
cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  for i in "$P" "$B"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  rm -rf "$WORK"
}
trap cleanup EXIT

# An instance made by Artemis or by Saranga with create --replicated.
make_instance() {
  local kind="$1" dir="$2" offset="$3" other="$4"; shift 4
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --replicated --host localhost
    --static-cluster "tcp://localhost:$other" --cluster-user cu --cluster-password cpw "$@")
  case "$kind" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac
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

# "artemis run --allow-kill" in the background.
launch() {
  local dir="$1"
  mkdir -p "$dir/log"
  if windows; then
    (cd "$dir/bin" && cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run --allow-kill >> "$dir/run.out" 2>&1 &)
  else
    (cd "$dir/bin" && ./artemis run --allow-kill >> "$dir/run.out" 2>&1 &)
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
# its IDs, versions, names and times left out.
declare -A SEEN
said() {
  local who="$1" dir="$2" from="${SEEN[$1]:-0}"
  local lines
  lines="$(grep -o -E 'AMQ22(1000|1002|1007|1010|1024|1031|1034|1035|1037|1109|2162)[^,]*' "$dir/log/artemis.log")"
  SEEN[$who]="$(printf '%s\n' "$lines" | grep -c .)"
  printf '%s\n' "$lines" | tail -n +"$((from + 1))" | grep . \
    | sed -E -e 's/(Apache Artemis|Saranga) Backup Server version [0-9.]+ \[[^]]*\]/BACKUP [ID]/' \
      -e 's/(AMQ221002): .*/\1: stopped/' \
      -e 's/(AMQ221000: [A-Za-z]+) message broker is starting.*/\1 message broker is starting/' \
      -e 's/(AMQ221024): .*/\1: backup synchronized/' \
      -e 's/(AMQ221037): .*/\1: becoming active/' \
      -e 's/(AMQ222162): .*/\1: moving data away/' \
    | sed "s/^/$who: /"
}

step() { echo "=== $*"; }
client() {
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Failover.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at "
}
produce() {
  local dir="$1" port="$2"; shift 2
  instance "$dir" producer --user admin --password admin --url "tcp://localhost:$port" \
    --destination queue://cli "$@" | grep -o "Produced: .*"
}
consume() {
  local dir="$1" port="$2" count="$3"
  instance "$dir" consumer --user admin --password admin --url "tcp://localhost:$port" \
    --destination queue://cli --message-count "$count" --receive-timeout 5000 \
    | grep -o "Consumed: .*"
}

suite() {
  make_instance "$PRIMARY" "$P" 2000 "$PORTB"
  make_instance "$BACKUP" "$B" 2100 "$PORTP" --backup

  # 1. Data before the backup.
  step "artemis run (primary)"
  launch "$P"
  wait_up "$PORTP" && wait_for "$P/log/artemis.log" AMQ221007 && echo "primary: listening"
  said primary "$P"
  step "artemis producer (primary) 20, then 2 large"
  produce "$P" "$PORTP" --message-count 20
  produce "$P" "$PORTP" --message-count 2 --message-size 150000
  step "artemis run (backup)"
  launch "$B"
  wait_for "$B/log/artemis.log" AMQ221024 && echo "backup: synchronized"
  wait_for "$B/log/artemis.log" AMQ221031
  said backup "$B"

  # 2. Replicated as written.
  step "Failover.java failover-once (in the background)"
  local out="$WORK/client.out"
  client failover-once "tcp://localhost:$PORTP?ha=true&reconnectAttempts=-1&retryInterval=500" "$(native "$MARK")" 10 > "$out" &
  CLIENT_PID=$!
  wait_for "$out" "sent 10" && echo "client: sent 10"
  step "artemis producer (primary) 10, consumer 5"
  produce "$P" "$PORTP" --message-count 10
  consume "$P" "$PORTP" 5

  # 3. The primary killed.
  step "artemis kill (primary)"
  instance "$P" kill > /dev/null
  wait_down "$PORTP" && echo "primary: down"
  wait_up "$PORTB" && wait_for "$B/log/artemis.log" AMQ221007 && echo "backup: listening"
  said backup "$B"
  touch "$MARK.1"
  wait "$CLIENT_PID"
  CLIENT_PID=
  grep -E "^(failover event|received|sent|unexpected)" "$out" | sed 's/^/client: /'
  step "artemis consumer (backup) 27, then 1"
  consume "$B" "$PORTB" 27
  consume "$B" "$PORTB" 1

  step "artemis stop (backup)"
  instance "$B" stop > /dev/null
  wait_down "$PORTB" && echo "backup: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/replication.expected.txt"
  echo "== updated $HERE/replication.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/replication.expected.txt" "$OUT"; then
  echo "ALL PASSED ($PRIMARY primary, $BACKUP backup)"
  rm -f "$OUT"
else
  echo "FAILED ($PRIMARY primary, $BACKUP backup; output in $OUT)"
  exit 1
fi
