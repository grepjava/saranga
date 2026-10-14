#!/usr/bin/env bash
# The no-loss suite, Phase 9's exit check: a primary and a backup, in each
# of the three ways Artemis pairs them, with the Artemis JMS client
# producing and consuming in transactions throughout (Failover.java load)
# while the primary is killed and later started again. Every message whose
# transaction committed must be received, and none twice.
#
#   PRIMARY=saranga|artemis BACKUP=saranga|artemis tests/interop/ha/no-loss.sh
#
# Each defaults to saranga. MODES (default all three) picks among:
#
#   shared-store  instances made with "create --shared-store", on one set
#                 of data directories; the backup takes the lock when the
#                 primary is killed, and gives way when it starts again.
#   replication   instances made with "create --replicated"; the backup
#                 wins the quorum vote and becomes active on its copy, and
#                 the restarted primary (check-for-active-server) copies the
#                 backup's data and takes its node back (allow-failback).
#   pluggable     the same, with Curator's ZooKeeper lock manager on one
#                 ZooKeeper server instead of the vote.
#
# For each, once the client has committed 500 messages the primary is
# killed; after 500 more, committed on the backup, the primary is started
# again; once it is active and the backup has announced itself to it, 500
# more, then the producer stops and the consumer receives the rest.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/ha"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
PRIMARY="${PRIMARY:-saranga}" BACKUP="${BACKUP:-saranga}"
MODES="${MODES:-shared-store replication pluggable}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }
sep() { windows && echo ';' || echo ':'; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/ha"
WORK="$(mktemp -d -p "$ROOT/target/ha")"
PORTP=64016 PORTB=64116 ZKPORT=62281
P= B= ZK= CLIENT_PID= ZK_PID=

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
stop_all() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  CLIENT_PID=
  for i in "$P" "$B"; do
    [[ -n "$i" && -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  wait_down "$PORTP"
  wait_down "$PORTB"
  sleep 3
  stop_zookeeper
  sleep 1
}
cleanup() {
  stop_all
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

# An instance made by Artemis or by Saranga with create and the options
# given.
make_instance() {
  local kind="$1" dir="$2" offset="$3" other="$4"; shift 4
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --host localhost
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

step() { echo "=== $*"; }
client() {
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Failover.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at "
}

# The instances of a mode, made in $WORK/<mode>.
prepare() {
  local mode="$1" dir="$WORK/$1"
  P="$dir/primary" B="$dir/backup" ZK="$dir/zookeeper"
  mkdir -p "$dir"
  case "$mode" in
    shared-store)
      make_instance "$PRIMARY" "$P" 2400 "$PORTB" --shared-store
      make_instance "$BACKUP" "$B" 2500 "$PORTP" --shared-store --backup
      local shared
      shared="$(native "$dir/shared")"
      sed -i -E -e "s#<(paging|bindings|journal|large-messages)-directory>[^<]*<#<\1-directory>$shared/\1<#" \
        "$P/etc/broker.xml" "$B/etc/broker.xml"
      ;;
    replication)
      make_instance "$PRIMARY" "$P" 2400 "$PORTB" --replicated
      make_instance "$BACKUP" "$B" 2500 "$PORTP" --replicated --backup
      perl -0pi -e 's|(<vote-on-replication-failure>true</vote-on-replication-failure>)|$1<check-for-active-server>true</check-for-active-server>|' "$P/etc/broker.xml"
      perl -0pi -e 's|<backup/>|<backup><allow-failback>true</allow-failback></backup>|' "$B/etc/broker.xml"
      ;;
    pluggable)
      make_instance "$PRIMARY" "$P" 2400 "$PORTB" --replicated
      make_instance "$BACKUP" "$B" 2500 "$PORTP" --replicated --backup
      local manager="<manager><properties><property key=\"connect-string\" value=\"127.0.0.1:$ZKPORT\"/><property key=\"session-ms\" value=\"4000\"/></properties></manager>"
      perl -0pi -e "s|<primary>\\s*<vote-on-replication-failure>true</vote-on-replication-failure>\\s*</primary>|<primary>$manager</primary>|" "$P/etc/broker.xml"
      perl -0pi -e "s|<backup/>|<backup>$manager<allow-failback>true</allow-failback></backup>|" "$B/etc/broker.xml"
      zookeeper && wait_up "$ZKPORT" || echo "zookeeper: not listening"
      ;;
  esac
}

# The count the client printed last.
committed() {
  local n
  n="$(grep -o -E "^committed [0-9]+" "$1" | tail -1 | grep -o -E "[0-9]+")"
  echo "${n:-0}"
}
wait_committed() {
  local out="$1" n="$2"
  for _ in $(seq 1 900); do
    [[ "$(committed "$out")" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}

run_mode() {
  local mode="$1" out="$WORK/$1/client.out" mark="$WORK/$1/stop" n
  prepare "$mode"
  step "$mode: artemis run (primary), artemis run (backup)"
  launch "$P"
  wait_up "$PORTP" && wait_for "$P/log/artemis.log" AMQ221007 && echo "primary: active"
  launch "$B"
  if [[ "$mode" != shared-store ]]; then
    wait_for "$B/log/artemis.log" AMQ221024 && echo "backup: synchronized"
  fi
  wait_for "$B/log/artemis.log" AMQ221031 && echo "backup: announced"

  step "$mode: Failover.java load (in the background)"
  client load "tcp://localhost:$PORTP?ha=true&reconnectAttempts=-1&retryInterval=500" "$(native "$mark")" > "$out" &
  CLIENT_PID=$!
  wait_committed "$out" 500 && echo "client: committed 500"

  step "$mode: artemis kill (primary)"
  instance "$P" kill > /dev/null
  wait_down "$PORTP" && echo "primary: down"
  wait_up "$PORTB" && echo "backup: active"
  n="$(committed "$out")"
  wait_committed "$out" $((n + 500)) && echo "client: 500 more committed"

  step "$mode: artemis run (primary)"
  launch "$P"
  wait_for "$P/log/artemis.log" AMQ221007 2 && wait_up "$PORTP" && echo "primary: active again"
  wait_for "$B/log/artemis.log" AMQ221031 2 && echo "backup: announced again"
  n="$(committed "$out")"
  wait_committed "$out" $((n + 500)) && echo "client: 500 more committed"

  step "$mode: the producer stopped, the rest received"
  touch "$mark"
  wait "$CLIENT_PID"
  CLIENT_PID=
  grep -E "^(lost|received twice|done):" "$out"
  grep -q -E "^done: lost 0, received twice 0$" "$out" || { echo "client:"; cat "$out"; }
  grep -E "^committed [0-9]+, received" "$out" | sed "s/^/$mode: /" >&2

  step "$mode: artemis stop (backup), artemis stop (primary)"
  stop_all
}

expected() {
  for mode in $MODES; do
    echo "=== $mode: artemis run (primary), artemis run (backup)"
    echo "primary: active"
    [[ "$mode" != shared-store ]] && echo "backup: synchronized"
    echo "backup: announced"
    echo "=== $mode: Failover.java load (in the background)"
    echo "client: committed 500"
    echo "=== $mode: artemis kill (primary)"
    echo "primary: down"
    echo "backup: active"
    echo "client: 500 more committed"
    echo "=== $mode: artemis run (primary)"
    echo "primary: active again"
    echo "backup: announced again"
    echo "client: 500 more committed"
    echo "=== $mode: the producer stopped, the rest received"
    echo "done: lost 0, received twice 0"
    echo "=== $mode: artemis stop (backup), artemis stop (primary)"
  done
}

OUT="$(mktemp)"
for mode in $MODES; do
  run_mode "$mode"
done > "$OUT" 2> "$OUT.err"
grep -E "^[a-z-]+: committed" "$OUT.err"
if diff -u <(expected) "$OUT"; then
  echo "ALL PASSED ($PRIMARY primary, $BACKUP backup)"
  rm -f "$OUT" "$OUT.err"
else
  echo "FAILED ($PRIMARY primary, $BACKUP backup; output in $OUT)"
  exit 1
fi
