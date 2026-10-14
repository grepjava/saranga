#!/usr/bin/env bash
# The pluggable lock manager suite: a primary and a backup made by
# "artemis create --replicated --static-cluster", their ha-policy given a
# <manager> (Curator's ZooKeeper lock manager, Artemis's default) on one
# ZooKeeper server, run from the ZooKeeper jars Artemis ships, with the
# Artemis JMS client failing over between them (Failover.java). The
# output, normalized, must match tests/interop/ha/pluggable.expected.txt
# (recorded with Artemis in both roles).
#
#   PRIMARY=saranga|artemis BACKUP=saranga|artemis tests/interop/ha/pluggable.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. The primary activates on its new data (activation sequence 1) and
#      has messages, two of them large, before the backup copies them.
#   2. Messages sent, and some consumed, reach the backup as written.
#   3. The primary is killed: once its ZooKeeper session expires, the
#      backup takes the node's lock with the latest sequence and becomes
#      active (2); the client fails over, and the CLI consumes and produces
#      there.
#   4. The primary starts again: its sequence is stale, so it replicates
#      the active backup as a backup asking for failback; the backup
#      (allow-failback) hands back and replicates it again, and the primary
#      activates (3). The client fails back, and the CLI consumes the rest.
#   5. Both stopped, "artemis activation list" shows the local and
#      coordinated sequences of each.
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
sep() { windows && echo ';' || echo ':'; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/ha"
WORK="$(mktemp -d -p "$ROOT/target/ha")"
P="$WORK/primary" B="$WORK/backup" MARK="$WORK/marker" ZK="$WORK/zookeeper"
PORTP=63816 PORTB=63916 ZKPORT=62181
CLIENT_PID= ZK_PID=

# The jars ZooKeeper's server needs besides those Artemis ships: from the
# local Maven repository, else Maven Central.
zk_jar() {
  local group="$1" artifact="$2" version="$3"
  local path="${group//.//}/$artifact/$version/$artifact-$version.jar"
  local local_jar="$HOME/.m2/repository/$path" cached="$ROOT/target/ha/lib/$artifact-$version.jar"
  if [[ -f "$local_jar" ]]; then echo "$local_jar"; return; fi
  if [[ ! -f "$cached" ]]; then
    mkdir -p "$ROOT/target/ha/lib"
    curl -sfL -o "$cached" "https://repo1.maven.org/maven2/$path" || return 1
  fi
  echo "$cached"
}
zookeeper() {
  local metrics snappy
  metrics="$(zk_jar io.dropwizard.metrics metrics-core 3.2.5)" || return 1
  snappy="$(zk_jar org.xerial.snappy snappy-java 1.1.10.4)" || return 1
  mkdir -p "$ZK/data"
  # A tick of a second, so sessions may be as short as two.
  printf 'tickTime=1000\ndataDir=%s\nclientPort=%s\nadmin.enableServer=false\n' \
    "$(native "$ZK/data")" "$ZKPORT" > "$ZK/zoo.cfg"
  java -cp "$(native "$DIST/lib")/*$(sep)$(native "$metrics")$(sep)$(native "$snappy")" \
    org.apache.zookeeper.server.ZooKeeperServerMain "$(native "$ZK/zoo.cfg")" > "$ZK/out" 2>&1 &
  ZK_PID=$!
}
stop_zookeeper() {
  [[ -z "$ZK_PID" ]] && return
  if windows && [[ -r "/proc/$ZK_PID/winpid" ]]; then
    taskkill //F //PID "$(cat "/proc/$ZK_PID/winpid")" > /dev/null 2>&1
  fi
  kill "$ZK_PID" 2>/dev/null
  ZK_PID=
}
cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  for i in "$P" "$B"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  stop_zookeeper
  sleep 1
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
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
  lines="$(grep -o -E 'AMQ22(1000|1002|1007|1010|1024|1031|1034|1035|1037|1039|1070|1109|2162)[^,]*' "$dir/log/artemis.log")"
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
    --destination queue://cli --message-count "$count" --receive-timeout 5000 --break-on-null \
    | grep -o "Consumed: .*"
}
# The activation sequences, the node ID left out.
activation() {
  instance "$1" activation list | grep -E "activation sequence" \
    | sed -E 's/NodeID=[^:]*/NodeID=ID/'
}

suite() {
  make_instance "$PRIMARY" "$P" 2200 "$PORTB"
  local manager="<manager><properties><property key=\"connect-string\" value=\"127.0.0.1:$ZKPORT\"/><property key=\"session-ms\" value=\"4000\"/></properties></manager>"
  perl -0pi -e "s|<primary>\\s*<vote-on-replication-failure>true</vote-on-replication-failure>\\s*</primary>|<primary>$manager</primary>|" "$P/etc/broker.xml"
  make_instance "$BACKUP" "$B" 2300 "$PORTP" --backup
  perl -0pi -e "s|<backup/>|<backup>$manager<allow-failback>true</allow-failback></backup>|" "$B/etc/broker.xml"
  grep -q "<manager>" "$P/etc/broker.xml" && grep -q "<manager>" "$B/etc/broker.xml" \
    || echo "broker.xml not edited"

  step "zookeeper"
  zookeeper && wait_up "$ZKPORT" && echo "zookeeper: listening"

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
  step "Failover.java failover (in the background)"
  local out="$WORK/client.out"
  client failover "tcp://localhost:$PORTP?ha=true&reconnectAttempts=-1&retryInterval=500" "$(native "$MARK")" 10 > "$out" &
  CLIENT_PID=$!
  wait_for "$out" "sent 10" && echo "client: sent 10"
  step "artemis producer (primary) 10, consumer 5"
  produce "$P" "$PORTP" --message-count 10
  consume "$P" "$PORTP" 5

  # 3. The primary killed: the backup takes the lock once the primary's
  # session expires.
  step "artemis kill (primary)"
  instance "$P" kill > /dev/null
  wait_down "$PORTP" && echo "primary: down"
  wait_up "$PORTB" && wait_for "$B/log/artemis.log" AMQ221007 && echo "backup: listening"
  said backup "$B"
  touch "$MARK.1"
  wait_for "$out" "sent 10" 2 && echo "client: failed over"
  step "artemis consumer (backup) 20, producer (backup) 5"
  consume "$B" "$PORTB" 20
  produce "$B" "$PORTB" --message-count 5

  # 4. Failback.
  step "artemis run (primary, stale)"
  launch "$P"
  wait_for "$P/log/artemis.log" AMQ221007 2 && wait_up "$PORTP" && echo "primary: active again"
  wait_for "$B/log/artemis.log" AMQ221024 2 && wait_for "$B/log/artemis.log" AMQ221031 2 \
    && echo "backup: synchronized again"
  said primary "$P"
  said backup "$B"
  touch "$MARK.2"
  wait "$CLIENT_PID"
  CLIENT_PID=
  grep -E "^(failover event|received|sent|unexpected)" "$out" | sed 's/^/client: /'
  step "artemis consumer (primary) 12, then 1"
  consume "$P" "$PORTP" 12
  consume "$P" "$PORTP" 1

  step "artemis stop (backup), artemis stop (primary)"
  instance "$B" stop > /dev/null
  sleep 3
  instance "$P" stop > /dev/null
  wait_down "$PORTP" && echo "primary: down"
  sleep 2

  # 5. The sequences.
  step "artemis activation list (primary)"
  activation "$P"
  step "artemis activation list (backup)"
  activation "$B"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/pluggable.expected.txt"
  echo "== updated $HERE/pluggable.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/pluggable.expected.txt" "$OUT"; then
  echo "ALL PASSED ($PRIMARY primary, $BACKUP backup)"
  rm -f "$OUT"
else
  echo "FAILED ($PRIMARY primary, $BACKUP backup; output in $OUT)"
  exit 1
fi
