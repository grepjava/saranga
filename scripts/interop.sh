#!/usr/bin/env bash
# Run the Java interop suites against Saranga, or against the reference
# Artemis broker to check the suites themselves.
#
#   scripts/interop.sh [saranga|artemis] [test-name...]
#
# Runs tests/interop/jms/Phase2.java and the golden scenarios that the current
# phase supports, with the Artemis client jars from the reference build.
set -euo pipefail

TARGET="${1:-saranga}"; shift || true
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
PORT=61621
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f saranga-interop >/dev/null 2>&1 || true
}
trap cleanup EXIT

case "$TARGET" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    "$ROOT/target/release/saranga" run --config "$(native "$ROOT/tests/golden/etc/broker.xml")" --bind "127.0.0.1:$PORT" &
    BROKER_PID=$!
    for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    ;;
  artemis)
    docker run -d --name saranga-interop -e ANONYMOUS_LOGIN=true \
      -p "127.0.0.1:$PORT:61616" "saranga/artemis-ref:$VERSION" >/dev/null
    for _ in $(seq 1 60); do docker logs saranga-interop 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    ;;
  *) echo "usage: interop.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

CP="$(native "$DIST/lib")/*"
URL="tcp://127.0.0.1:$PORT"
SUITE="${SUITE:-Phase2}"
java -cp "$CP" "$(native "$ROOT/tests/interop/jms/$SUITE.java")" "$URL" "$@"
if [[ $# -eq 0 ]]; then
  for scenario in CoreBasic CoreXa; do
    java -cp "$CP" "$(native "$ROOT/tests/golden-gen/scenarios/$scenario.java")" "$URL"
  done
fi
