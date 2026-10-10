#!/usr/bin/env bash
# Runs the TLS suite (tests/interop/jms/Tls.java) against Saranga or the
# reference Artemis broker, both with tests/interop/tls's etc files and the
# stores of tests/tls.
#
#   scripts/tls-interop.sh [saranga|artemis] [test-name...]
#
# TLS_PORT (default 61650) is the first of six ports: plain, p12, jks, pem,
# mtls and crl acceptors.
set -euo pipefail

TARGET="${1:-saranga}"; shift || true
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
BASE="${TLS_PORT:-61650}"
NAME="saranga-tls"
NAMES=(plain p12 jks pem mtls crl)
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

ETC="$WORK/etc"
mkdir -p "$ETC"
cp "$ROOT"/tests/interop/tls/* "$ETC/"
for f in server.p12 server.jks server.pem truststore.jks truststore.p12 revoked.crl; do
  cp "$ROOT/tests/tls/$f" "$ETC/"
done

case "$TARGET" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    ports=()
    for i in "${!NAMES[@]}"; do ports+=("tls.port.${NAMES[$i]}=$((BASE + i))"); done
    env "${ports[@]}" "$ROOT/target/release/saranga" run --config "$(native "$ETC/broker.xml")" \
      > "$WORK/saranga.log" 2>&1 &
    BROKER_PID=$!
    for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$((BASE + 5))") 2>/dev/null && break; sleep 0.1; done
    ;;
  artemis)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    maps=()
    for i in "${!NAMES[@]}"; do maps+=(-p "127.0.0.1:$((BASE + i)):$((61616 + i))"); done
    docker create --name "$NAME" "${maps[@]}" "saranga/artemis-ref:$VERSION" >/dev/null
    docker cp "$(native "$ETC")" "$NAME":/var/lib/artemis-instance/etc-override >/dev/null
    docker start "$NAME" >/dev/null
    for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    ;;
  *) echo "usage: tls-interop.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

CP="$(native "$DIST/lib")/*"
args=(localhost "$(native "$ROOT/tests/tls")")
for i in "${!NAMES[@]}"; do args+=($((BASE + i))); done
status=0
java -cp "$CP" "$(native "$ROOT/tests/interop/jms/Tls.java")" "${args[@]}" "$@" || status=$?
if [[ $status -ne 0 ]]; then
  case "$TARGET" in
    saranga) tail -30 "$WORK/saranga.log" ;;
    artemis) docker logs "$NAME" 2>&1 | tail -30 ;;
  esac
fi
exit $status
