#!/usr/bin/env bash
# Runs the LDAP suite (tests/interop/jms/Ldap.java) against Saranga or the
# reference Artemis broker, both logging in through an OpenLDAP container
# loaded with tests/interop/ldap/users.ldif.
#
#   scripts/ldap-interop.sh [saranga|artemis] [test-name...]
#
# LDAP_PORT (default 3890) is where the directory listens on this machine,
# PORT (default 61660) the broker. Also runs the LDAP login module's own
# tests (crates/saranga-security/tests/ldap.rs) against the directory.
set -euo pipefail

TARGET="${1:-saranga}"; shift || true
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
LDAP_PORT="${LDAP_PORT:-3890}"
PORT="${PORT:-61660}"
NET="saranga-ldap-net"
LDAP="saranga-ldap"
NAME="saranga-ldap-broker"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" "$LDAP" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

docker rm -f "$NAME" "$LDAP" >/dev/null 2>&1 || true
docker network create "$NET" >/dev/null 2>&1 || true
docker run -d --name "$LDAP" --network "$NET" -p "127.0.0.1:$LDAP_PORT:389" \
  -e LDAP_DOMAIN=example.com -e LDAP_ADMIN_PASSWORD=secret osixia/openldap:1.5.0 >/dev/null
for _ in $(seq 1 60); do
  docker exec "$LDAP" ldapsearch -x -H ldap://localhost -b dc=example,dc=com \
    -D cn=admin,dc=example,dc=com -w secret -s base >/dev/null 2>&1 && break
  sleep 1
done
docker cp "$(native "$ROOT/tests/interop/ldap/users.ldif")" "$LDAP:/tmp/users.ldif" >/dev/null
docker exec "$LDAP" ldapadd -x -H ldap://localhost -D cn=admin,dc=example,dc=com -w secret \
  -f /tmp/users.ldif >/dev/null

ETC="$WORK/etc"
mkdir -p "$ETC"
cp "$ROOT/tests/interop/ldap/broker.xml" "$ETC/"

case "$TARGET" in
  saranga)
    LDAP_URL="ldap://127.0.0.1:$LDAP_PORT" cargo test -q -p saranga-security --test ldap \
      --manifest-path "$(native "$ROOT/Cargo.toml")"
    sed "s|LDAP_URL|ldap://127.0.0.1:$LDAP_PORT|" "$ROOT/tests/interop/ldap/login.config" > "$ETC/login.config"
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    "$ROOT/target/release/saranga" run --config "$(native "$ETC/broker.xml")" --bind "127.0.0.1:$PORT" \
      > "$WORK/saranga.log" 2>&1 &
    BROKER_PID=$!
    for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    ;;
  artemis)
    sed "s|LDAP_URL|ldap://$LDAP:389|" "$ROOT/tests/interop/ldap/login.config" > "$ETC/login.config"
    docker create --name "$NAME" --network "$NET" -p "127.0.0.1:$PORT:61616" \
      "saranga/artemis-ref:$VERSION" >/dev/null
    docker cp "$(native "$ETC")" "$NAME":/var/lib/artemis-instance/etc-override >/dev/null
    docker start "$NAME" >/dev/null
    for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    ;;
  *) echo "usage: ldap-interop.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

CP="$(native "$DIST/lib")/*"
status=0
java -cp "$CP" "$(native "$ROOT/tests/interop/jms/Ldap.java")" "tcp://127.0.0.1:$PORT" "$@" || status=$?
if [[ $status -ne 0 ]]; then
  case "$TARGET" in
    saranga) tail -30 "$WORK/saranga.log" ;;
    artemis) docker logs "$NAME" 2>&1 | tail -30 ;;
  esac
fi
exit $status
