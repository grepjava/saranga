#!/usr/bin/env bash
# The interceptors suite: an instance made by "artemis create" with
# <remoting-incoming-interceptors> and <remoting-outgoing-interceptors>
# naming the classes of Interceptors.java, and Suite.java checking what they
# do to Core, STOMP, MQTT, OpenWire and AMQP.
#
#   tests/interop/interceptors/interceptors.sh [saranga|artemis] [test-name...]
#
# Artemis loads the classes, compiled into its instance's lib/. Saranga has
# the same interceptors in Rust under the same names, in a build of the CLI
# with the interop-interceptors feature (in target/interceptors, so that
# the usual release build is left as it is).
#
# MAX_DISK_USAGE: for a machine whose disk is past create's 90%, where the
# broker would block every producer.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/interceptors"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
KIND="${1:-saranga}"; shift || true
OFFSET=2900
PORT=$((61616 + OFFSET))
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

mkdir -p "$ROOT/target/interceptors"
WORK="$(mktemp -d -p "$ROOT/target/interceptors")"
I="$WORK/i"
instance() {
  if windows; then
    (cd "$I/bin" && timeout 300 cmd /c call "$(cygpath -w "$I/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$I/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; }
cleanup() {
  [[ -d "$I" ]] && instance stop > /dev/null 2>&1
  for _ in $(seq 1 100); do up || break; sleep 0.1; done
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

opts=(--silent --no-autotune --user artemis --password artemis --allow-anonymous
  --port-offset "$OFFSET" --no-web --host localhost)
case "$KIND" in
  saranga)
    cargo build -q --release -p saranga-cli --features interop-interceptors \
      --target-dir "$(native "$ROOT/target/interceptors")" --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
    "$ROOT/target/interceptors/release/saranga" create "$(native "$I")" "${opts[@]}" > /dev/null 2>&1 \
      || { echo "create failed"; exit 1; }
    ;;
  artemis)
    java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$I")" "${opts[@]}" > /dev/null 2>&1 \
      || { echo "create failed"; exit 1; }
    mkdir -p "$WORK/classes"
    javac -d "$(native "$WORK/classes")" -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Interceptors.java")" || exit 1
    (cd "$WORK/classes" && jar cf "$(native "$I/lib/interceptors.jar")" .) || exit 1
    ;;
  *) echo "usage: interceptors.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

class() { echo "<class-name>org.saranga.interop.Interceptors\$$1</class-name>"; }
IN="<remoting-incoming-interceptors>$(class CoreIn)$(class StompIn)$(class MqttIn)$(class OpenWireIn)$(class AmqpIn)</remoting-incoming-interceptors>"
OUT="<remoting-outgoing-interceptors>$(class CoreOut)$(class StompOut)$(class MqttOut)$(class OpenWireOut)$(class AmqpOut)</remoting-outgoing-interceptors>"
sed -i "s|</core>|$IN$OUT</core>|" "$I/etc/broker.xml"
if [[ -n "${MAX_DISK_USAGE:-}" ]]; then
  sed -i -E "s#<max-disk-usage>[0-9]+</max-disk-usage>#<max-disk-usage>$MAX_DISK_USAGE</max-disk-usage>#" "$I/etc/broker.xml"
fi

mkdir -p "$I/log"
if windows; then
  (cd "$I/bin" && cmd /c "$(cygpath -w "$I/bin/artemis.cmd")" run > "$WORK/run.out" 2>&1 &)
else
  (cd "$I/bin" && ./artemis run > "$WORK/run.out" 2>&1 &)
fi
for _ in $(seq 1 600); do up && break; sleep 0.2; done
up || { echo "the broker did not start"; cat "$WORK/run.out"; exit 1; }

java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Suite.java")" "127.0.0.1:$PORT" "$@"
