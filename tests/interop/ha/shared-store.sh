#!/usr/bin/env bash
# The shared-store suite: a primary and a backup on one journal directory,
# each run through its instance's own bin/artemis, as the manual's HA
# chapter configures them. Their output, normalized, must match
# tests/interop/ha/expected.txt (recorded with Artemis in both roles).
#
#   PRIMARY=saranga|artemis BACKUP=saranga|artemis tests/interop/ha/shared-store.sh
#
# Each defaults to saranga. UPDATE=1 rewrites expected.txt instead of
# comparing. The primary fails over on shutdown, the backup does not; there
# is no cluster connection, so the backup cannot fail back.
#
#   1. The backup waits; the primary is killed and the backup takes over
#      with the messages sent to the primary.
#   2. The primary, started again, waits for the lock the backup holds, and
#      becomes active once the backup stops (paused: no failover).
#   3. The primary stopped, it fails over: the backup takes over again.
#   4. Paused, the data keeps a backup waiting.
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
P="$WORK/primary" B="$WORK/backup" SHARED="$WORK/shared"
PORTP=62616 PORTB=62716
cleanup() {
  for i in "$P" "$B"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  rm -rf "$WORK"
}
trap cleanup EXIT

# An instance made by Artemis or by Saranga, its data in the shared
# directory and its ha-policy the one given.
make_instance() {
  local kind="$1" dir="$2" offset="$3" policy="$4"
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web)
  case "$kind" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac
  local shared
  shared="$(native "$SHARED")"
  sed -i -E \
    -e "s#<(paging|bindings|journal|large-messages)-directory>[^<]*<#<\1-directory>$shared/\1<#" \
    -e "s#</core>#   <ha-policy><shared-store>$policy</shared-store></ha-policy>\n   </core>#" \
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

# "artemis run --allow-kill" in the background, its log emptied first.
launch() {
  local dir="$1"
  : > "$dir/log/artemis.log" 2>/dev/null || mkdir -p "$dir/log"
  if windows; then
    (cd "$dir/bin" && cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run --allow-kill > "$dir/run.out" 2>&1 &)
  else
    (cd "$dir/bin" && ./artemis run --allow-kill > "$dir/run.out" 2>&1 &)
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
wait_for() {
  local file="$1" text="$2"
  for _ in $(seq 1 300); do
    grep -q -- "$text" "$file" 2>/dev/null && return 0
    sleep 0.2
  done
  return 1
}
# The lines of a broker's log that tell what it did, its IDs and versions
# left out.
said() {
  local who="$1" dir="$2"
  grep -o -E 'AMQ22(1032|1033|1034|1035|1109|1010|1007|2308)[^,]*' "$dir/log/artemis.log" \
    | sed -E -e 's/(Apache Artemis|Saranga) Backup Server version [0-9.]+ \[[^]]*\]/BACKUP [ID]/' \
      -e 's/(because) .* (is null)\./\1 X \2./' \
    | sed "s/^/$who: /"
}

step() { echo "=== $*"; }
AUTH=(--user admin --password admin)
produce() {
  step "artemis producer --url tcp://localhost:$2 --destination queue://ha --message-count $3"
  instance "$1" producer --url "tcp://localhost:$2" --destination queue://ha --message-count "$3" "${AUTH[@]}" \
    | grep -o -E 'Produced: [0-9]+ messages'
}
consume() {
  step "artemis consumer --url tcp://localhost:$2 --destination queue://ha --message-count $3"
  instance "$1" consumer --url "tcp://localhost:$2" --destination queue://ha --message-count "$3" \
    --receive-timeout 5000 --break-on-null "${AUTH[@]}" | grep -o -E 'Consumed: [0-9]+ messages'
}
stop() {
  step "artemis stop ($1)"
  instance "$2" stop > /dev/null
  wait_down "$3" && echo "$1: down"
}

suite() {
  make_instance "$PRIMARY" "$P" 1000 "<primary><failover-on-shutdown>true</failover-on-shutdown></primary>"
  make_instance "$BACKUP" "$B" 1100 "<backup><failover-on-shutdown>false</failover-on-shutdown></backup>"

  # 1. Failover after the primary dies.
  step "artemis run (primary)"
  launch "$P"
  wait_up "$PORTP" && wait_for "$P/log/artemis.log" AMQ221007 && echo "primary: listening"
  step "artemis run (backup)"
  launch "$B"
  wait_for "$B/log/artemis.log" AMQ221109 && sleep 1
  up "$PORTB" && echo "backup: listening" || echo "backup: not listening"
  produce "$P" "$PORTP" 10
  step "artemis kill (primary)"
  instance "$P" kill > /dev/null
  wait_down "$PORTP" && echo "primary: down"
  wait_up "$PORTB" && wait_for "$B/log/artemis.log" AMQ221010 && echo "backup: listening"
  said primary "$P"
  said backup "$B"
  consume "$B" "$PORTB" 20

  # 2. The primary waits for the active backup, which does not fail back.
  produce "$B" "$PORTB" 3
  step "artemis run (primary)"
  launch "$P"
  wait_for "$P/log/artemis.log" AMQ221034 && sleep 3
  up "$PORTP" && echo "primary: listening" || echo "primary: not listening"
  said primary "$P"
  stop backup "$B" "$PORTB"
  wait_up "$PORTP" && wait_for "$P/log/artemis.log" AMQ221007 && echo "primary: listening"
  said primary "$P"
  consume "$P" "$PORTP" 20

  # 3. Failover on shutdown.
  step "artemis run (backup)"
  launch "$B"
  wait_for "$B/log/artemis.log" AMQ221109 && sleep 1
  produce "$P" "$PORTP" 5
  stop primary "$P" "$PORTP"
  wait_up "$PORTB" && wait_for "$B/log/artemis.log" AMQ221010 && echo "backup: listening"
  said backup "$B"
  consume "$B" "$PORTB" 20

  # 4. Paused data: a backup started on it waits.
  stop backup "$B" "$PORTB"
  step "artemis run (backup)"
  launch "$B"
  wait_for "$B/log/artemis.log" AMQ221109 && sleep 6
  up "$PORTB" && echo "backup: listening" || echo "backup: not listening"
  said backup "$B"
  stop backup "$B" "$PORTB"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/expected.txt"
  echo "== updated $HERE/expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/expected.txt" "$OUT"; then
  echo "ALL PASSED ($PRIMARY primary, $BACKUP backup)"
  rm -f "$OUT"
else
  echo "FAILED ($PRIMARY primary, $BACKUP backup; output in $OUT)"
  exit 1
fi
