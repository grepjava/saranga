#!/usr/bin/env bash
# The basic security manager across brokers: <before> starts with the
# bootstrap user admin (tests/interop/basic/bootstrap-before.xml), which it
# stores in the bindings journal; <after> runs on the same data directory
# with the bootstrap user second (bootstrap-after.xml). Both users must log
# in on <after>, and a wrong password must be refused.
#
#   scripts/basic-security.sh <before> <after>     (saranga or artemis)
set -euo pipefail

BEFORE="${1:?usage: basic-security.sh <before> <after>}"
AFTER="${2:?usage: basic-security.sh <before> <after>}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="saranga/artemis-ref:$VERSION"
NAME="saranga-basic"
PORT="${PORT:-61677}"
DIST="$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
INSTANCE="$WORK/instance"
mkdir -p "$INSTANCE/etc"
cp "$ROOT/tests/golden/etc/broker.xml" "$INSTANCE/etc/broker.xml"
# run requires management.xml, as artemis run does; this is what create
# writes for the role amq.
cp "$ROOT/tests/interop/basic/management.xml" "$INSTANCE/etc/management.xml"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

start() {   # $1 broker, $2 phase
  cp "$ROOT/tests/interop/basic/bootstrap-$2.xml" "$INSTANCE/etc/bootstrap.xml"
  case "$1" in
    saranga)
      cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
      "$ROOT/target/release/saranga" run --etc "$(native "$INSTANCE/etc")" \
        --bind "127.0.0.1:$PORT" > "$WORK/saranga-$2.log" 2>&1 &
      BROKER_PID=$!
      for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0; sleep 0.2; done
      cat "$WORK/saranga-$2.log"; return 1 ;;
    artemis)
      docker rm -f "$NAME" >/dev/null 2>&1 || true
      docker create --name "$NAME" --user root -p "127.0.0.1:$PORT:61616" "$IMAGE" >/dev/null
      docker cp "$(native "$INSTANCE/etc")" "$NAME:/var/lib/artemis-instance/etc-override" >/dev/null
      [[ -d "$INSTANCE/data" ]] && docker cp "$(native "$INSTANCE/data")" "$NAME:/var/lib/artemis-instance/data" >/dev/null
      docker start "$NAME" >/dev/null
      for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && return 0; sleep 1; done
      docker logs "$NAME" >&2; return 1 ;;
  esac
}

stop() {   # $1 broker
  case "$1" in
    saranga) touch "$INSTANCE/etc/STOP_ME"; wait "$BROKER_PID" || true; BROKER_PID=""; rm -f "$INSTANCE/etc/STOP_ME" ;;
    artemis)
      docker stop -t 60 "$NAME" >/dev/null
      rm -rf "$INSTANCE/data"
      docker cp "$NAME:/var/lib/artemis-instance/data" "$(native "$INSTANCE/data")" >/dev/null
      docker rm -f "$NAME" >/dev/null ;;
  esac
}

check() {   # user password
  java -cp "$(native "$DIST/lib")/*" "$(native "$ROOT/tests/interop/jms/EtcCheck.java")" \
    "tcp://127.0.0.1:$PORT" "$1" "$2" "basic.check" wrong-password 2>&1 | grep -v '^WARNING'
}

echo "== $BEFORE -> $AFTER"
start "$BEFORE" before
check admin secret
stop "$BEFORE"
start "$AFTER" after
check admin secret
check second two
stop "$AFTER"
echo "ALL PASSED"
