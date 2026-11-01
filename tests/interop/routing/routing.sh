#!/usr/bin/env bash
# The connection router suite: three clustered instances made by "artemis
# create --clustered" (UDP discovery on a group port of the suite's own),
# all Saranga or all Artemis. The router broker (r) has an acceptor per
# router that routes connections, and routers that only its management
# asks; the targets (t1, t2) are plain brokers. Suite.java connects with
# the Artemis JMS client (Core), Qpid JMS (AMQP), the ActiveMQ client
# (OpenWire) and raw MQTT 3.1.1 and 5 packets, and asks the routers through
# management messages and Jolokia.
#
#   tests/interop/routing/routing.sh [saranga|artemis] [test-name...]
#
#   redirect  to-t1: by user, FIRST_ELEMENT of a static pool of t1
#   local     local-only: by client ID, local-target-filter local-.*, no
#             pool, so any other client ID is refused
#   notready  not-ready: a pool of a connector nothing listens on
#   role      by-role: by role (key-filter .*-role), local-role local
#   sni       by-sni: TLS, by SNI host, local\.example local
#   ip        by-ip: by source IP, 127.0.0.1 local
#   missing   a router that is not configured
#
# After the main tests, t1 is stopped while the cached and volatile routers
# route a key to t2, then t1 and r are restarted: the persisted cache still
# routes it to t2, the other to t1.
#
# OFFSET: the port offset, 1300 unless given (r at 61616+OFFSET, t1 and t2
# 20 and 40 above). MAX_DISK_USAGE is applied to each broker when given.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/routing"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
KIND="${1:-saranga}"; shift || true
OFFSET="${OFFSET:-1300}"
P=$((61616 + OFFSET))
T1=$((P + 20))
T2=$((P + 40))
DEAD=$((P + 60))
HTTP=$((P + 70))
GROUP_PORT=$((9876 + OFFSET))
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

mkdir -p "$ROOT/target/routing"
WORK="$(mktemp -d -p "$ROOT/target/routing")"
instance() { # dir args...
  local dir="$1"; shift
  if windows; then
    (cd "$dir/bin" && timeout 300 cmd /c call "$(cygpath -w "$dir/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$dir/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
start() { # dir port
  mkdir -p "$1/log"
  if windows; then
    (cd "$1/bin" && cmd /c "$(cygpath -w "$1/bin/artemis.cmd")" run >> "$1/run.out" 2>&1 &)
  else
    (cd "$1/bin" && ./artemis run >> "$1/run.out" 2>&1 &)
  fi
  for _ in $(seq 1 600); do up "$2" && return 0; sleep 0.2; done
  echo "$1 did not start"; cat "$1/run.out"; exit 1
}
stop() { # dir port
  instance "$1" stop > /dev/null 2>&1
  for _ in $(seq 1 300); do up "$2" || return 0; sleep 0.2; done
}
cleanup() {
  for i in r:$P t1:$T1 t2:$T2; do
    [[ -d "$WORK/${i%%:*}" ]] && stop "$WORK/${i%%:*}" "${i#*:}"
  done
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

create() { # name offset [web]
  local opts=(--silent --no-autotune --user artemis --password artemis --require-login
    --port-offset "$2" --host localhost --name "$1" --clustered --discovery-enabled
    --cluster-user cluster --cluster-password cluster)
  if [[ -n "${3:-}" ]]; then
    # create adds the offset to it too.
    opts+=(--http-host localhost --http-port "$((HTTP - $2))")
  else
    opts+=(--no-web)
  fi
  case "$KIND" in
    saranga) "$TARGET/release/saranga" create "$(native "$WORK/$1")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$WORK/$1")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac || { echo "create $1 failed"; exit 1; }
  local xml="$WORK/$1/etc/broker.xml"
  sed -i -e "s#<group-port>9876</group-port>#<group-port>$GROUP_PORT</group-port>#" \
    -e "s#<broadcast-period>5000</broadcast-period>#<broadcast-period>1000</broadcast-period>#" "$xml"
  if [[ -n "${MAX_DISK_USAGE:-}" ]]; then
    sed -i -E "s#<max-disk-usage>[0-9]+</max-disk-usage>#<max-disk-usage>$MAX_DISK_USAGE</max-disk-usage>#" "$xml"
  fi
}

case "$KIND" in
  saranga) CARGO_TARGET_DIR="$(native "$TARGET")" cargo build -q --release -p saranga-cli \
    --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1 ;;
  artemis) ;;
  *) echo "usage: routing.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac
create t1 $((OFFSET + 20))
create t2 $((OFFSET + 40))
create r "$OFFSET" web
R="$WORK/r"
ETC="$R/etc"

# The SNI acceptor's certificate.
keytool -genkeypair -alias r -keyalg RSA -keysize 2048 -dname CN=localhost \
  -ext SAN=dns:localhost,dns:local.example,dns:other.example,ip:127.0.0.1 -validity 2 \
  -storetype PKCS12 -keystore "$(native "$ETC/r.p12")" -storepass changeit -keypass changeit \
  > /dev/null 2>&1 || exit 1

# Users with a role each, for the role router, on every broker (a
# redirected client logs in to its target).
for b in r t1 t2; do
  cat >> "$WORK/$b/etc/artemis-users.properties" <<USERS

localuser = local-secret
remoteuser = remote-secret
USERS
  sed -i -E 's/^amq ?= ?(.*)$/amq = \1,localuser,remoteuser/' "$WORK/$b/etc/artemis-roles.properties"
  cat >> "$WORK/$b/etc/artemis-roles.properties" <<ROLES

local-role = localuser
remote-role = remoteuser
ROLES
done

acceptor() { # name port router [params]
  echo "<acceptor name=\"$1\">tcp://localhost:$2?router=$3;protocols=CORE,AMQP,MQTT,OPENWIRE${4:-}</acceptor>"
}
ACCEPTORS="$(acceptor redirect $((P + 1)) to-t1)$(acceptor local $((P + 2)) local-only)"
ACCEPTORS="$ACCEPTORS$(acceptor notready $((P + 3)) not-ready)$(acceptor role $((P + 4)) by-role)"
ACCEPTORS="$ACCEPTORS$(acceptor sni $((P + 5)) by-sni ";sslEnabled=true;keyStorePath=$(native "$ETC/r.p12");keyStorePassword=changeit;keyStoreType=PKCS12")"
ACCEPTORS="$ACCEPTORS$(acceptor ip $((P + 6)) by-ip)$(acceptor missing $((P + 7)) nosuch)"
CONNECTORS="<connector name=\"t1\">tcp://localhost:$T1</connector><connector name=\"t2\">tcp://localhost:$T2</connector>"
CONNECTORS="$CONNECTORS<connector name=\"dead\">tcp://localhost:$DEAD</connector>"

pool() { # connectors... ; the targets' management logs in as artemis
  local refs=""
  for c in "$@"; do refs="$refs<connector-ref>$c</connector-ref>"; done
  echo "<pool><username>artemis</username><password>artemis</password><check-period>500</check-period><static-connectors>$refs</static-connectors></pool>"
}
policy() { echo "<policy name=\"$1\"/>"; }
router() { # name key body
  echo "<connection-router name=\"$1\"><key-type>$2</key-type>$3</connection-router>"
}
ROUTERS="$(router to-t1 USER_NAME "$(policy FIRST_ELEMENT)$(pool t1)")"
ROUTERS="$ROUTERS$(router local-only CLIENT_ID "<local-target-filter>local-.*</local-target-filter>")"
ROUTERS="$ROUTERS$(router not-ready CLIENT_ID "$(policy FIRST_ELEMENT)$(pool dead)")"
ROUTERS="$ROUTERS$(router by-role ROLE_NAME "<key-filter>.*-role</key-filter><local-target-filter>local-role</local-target-filter>$(policy FIRST_ELEMENT)$(pool t1)")"
ROUTERS="$ROUTERS$(router by-sni SNI_HOST "<local-target-filter>local\\\\.example</local-target-filter>")"
ROUTERS="$ROUTERS$(router by-ip SOURCE_IP "<local-target-filter>127\\\\.0\\\\.0\\\\.1</local-target-filter>")"
ROUTERS="$ROUTERS$(router hash CLIENT_ID "$(policy CONSISTENT_HASH)$(pool t1 t2)")"
ROUTERS="$ROUTERS$(router least CLIENT_ID "$(policy LEAST_CONNECTIONS)$(pool t1 t2)")"
ROUTERS="$ROUTERS$(router modulo CLIENT_ID "<local-target-filter>NULL|0</local-target-filter><policy name=\"CONSISTENT_HASH_MODULO\"><property key=\"MODULO\" value=\"2\"/></policy>")"
ROUTERS="$ROUTERS$(router quorum CLIENT_ID "$(policy FIRST_ELEMENT)<pool><username>artemis</username><password>artemis</password><check-period>500</check-period><quorum-size>2</quorum-size><quorum-timeout>300</quorum-timeout><static-connectors><connector-ref>t1</connector-ref><connector-ref>dead</connector-ref></static-connectors></pool>")"
ROUTERS="$ROUTERS$(router cached CLIENT_ID "<cache><persisted>true</persisted></cache>$(policy FIRST_ELEMENT)$(pool t1 t2)")"
ROUTERS="$ROUTERS$(router volatile CLIENT_ID "<cache><persisted>false</persisted></cache>$(policy FIRST_ELEMENT)$(pool t1 t2)")"
ROUTERS="$ROUTERS$(router local-enabled CLIENT_ID "$(policy FIRST_ELEMENT)<pool><username>artemis</username><password>artemis</password><check-period>500</check-period><local-target-enabled>true</local-target-enabled><static-connectors><connector-ref>t1</connector-ref></static-connectors></pool>")"
ROUTERS="$ROUTERS$(router cluster CLIENT_ID "$(policy CONSISTENT_HASH)<pool><username>artemis</username><password>artemis</password><check-period>500</check-period><cluster-connection>my-cluster</cluster-connection></pool>")"
ROUTERS="$ROUTERS$(router discovery CLIENT_ID "$(policy CONSISTENT_HASH)<pool><username>artemis</username><password>artemis</password><check-period>500</check-period><discovery-group-ref discovery-group-name=\"dg-group1\"/></pool>")"

XML="$ETC/broker.xml"
sed -i -e "s|</acceptors>|$ACCEPTORS</acceptors>|" -e "s|</connectors>|$CONNECTORS</connectors>|" \
  -e "s|</cluster-connections>|</cluster-connections><connection-routers>${ROUTERS//|/\\|}</connection-routers>|" \
  -e "s|<persistence-enabled>|<configuration-file-refresh-period>1000</configuration-file-refresh-period><persistence-enabled>|" "$XML"
grep -q "<connection-routers>" "$XML" || { echo "no routers in $XML"; exit 1; }

start "$WORK/t1" "$T1"
start "$WORK/t2" "$T2"
start "$R" "$P"

suite() { # phase [test-name...]
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Suite.java")" \
    "$1" "$P" "$T1" "$T2" "$HTTP" "$(native "$XML")" "${@:2}"
}

failed=0
suite main "$@" || failed=1
if [[ $# -eq 0 ]]; then
  # The persisted cache: a key routed to t2 while t1 is down stays there
  # after a restart of r, once t1 is back; the volatile cache's goes to t1.
  stop "$WORK/t1" "$T1"
  suite cache-prime || failed=1
  start "$WORK/t1" "$T1"
  stop "$R" "$P"
  start "$R" "$P"
  suite cache-check || failed=1
fi
if [[ $failed -ne 0 ]]; then
  echo "--- r's log"; tail -n 60 "$R/run.out"
fi
exit $failed
