#!/usr/bin/env bash
# The OIDC suite: an instance made by "artemis create --require-login" whose
# AMQP acceptors log clients in through OIDCLoginModule realms, each with
# other options. Suite.java runs the OpenID Connect provider (its metadata
# and keys over HTTP and over HTTPS), signs tokens with its keys, and logs in
# with Qpid JMS over PLAIN, the token as the password.
#
#   tests/interop/oidc/oidc.sh [saranga|artemis] [test-name...]
#
#   oidc        audience artemis, identity preferred_username, roles from
#               realm_access.roles with broker-user mapped to amq
#   oidc-plain  plain JWTs allowed, only sub required, roles from roles
#   oidc-tls    the provider over HTTPS, trusted through caCertificate
#
# MAX_DISK_USAGE: for a machine whose disk is past create's 90%, where the
# broker would block every producer.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/oidc"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
KIND="${1:-saranga}"; shift || true
OFFSET=3100
PORT=$((61616 + OFFSET))
OIDC_PORT=$((PORT + 1))
PLAIN_PORT=$((PORT + 2))
TLS_PORT=$((PORT + 3))
HTTP_PORT=$((PORT + 10))
HTTPS_PORT=$((PORT + 11))
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

mkdir -p "$ROOT/target/oidc"
WORK="$(mktemp -d -p "$ROOT/target/oidc")"
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

opts=(--silent --no-autotune --user artemis --password artemis --require-login
  --port-offset "$OFFSET" --no-web --host localhost)
case "$KIND" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
    "$ROOT/target/release/saranga" create "$(native "$I")" "${opts[@]}" > /dev/null 2>&1 || { echo "create failed"; exit 1; }
    ;;
  artemis)
    java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$I")" "${opts[@]}" > /dev/null 2>&1 \
      || { echo "create failed"; exit 1; }
    ;;
  *) echo "usage: oidc.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

ETC="$I/etc"
# The HTTPS provider's certificate, and the PEM the oidc-tls realm trusts.
keytool -genkeypair -alias oidc -keyalg RSA -keysize 2048 -dname CN=localhost \
  -ext SAN=dns:localhost,ip:127.0.0.1 -validity 2 -storetype PKCS12 \
  -keystore "$(native "$WORK/provider.p12")" -storepass changeit -keypass changeit > /dev/null 2>&1 || exit 1
keytool -exportcert -rfc -alias oidc -keystore "$(native "$WORK/provider.p12")" -storepass changeit \
  > "$ETC/oidc-ca.pem" 2> /dev/null || exit 1

MODULE=org.apache.activemq.artemis.spi.core.security.jaas.OIDCLoginModule
cat > "$ETC/login.config" <<CONF
activemq {
   org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule required
       reload=true
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};

oidc {
   $MODULE required
       provider="http://localhost:$HTTP_PORT"
       audience="artemis"
       identityPaths="preferred_username"
       rolesPaths="realm_access.roles"
       roleMapping="broker-user=amq";
};

oidc-plain {
   $MODULE required
       provider="http://localhost:$HTTP_PORT/"
       allowPlainJWT=true
       requiredClaims="sub"
       rolesPaths="roles";
};

oidc-tls {
   $MODULE required
       provider="https://localhost:$HTTPS_PORT"
       caCertificate="$(native "$ETC/oidc-ca.pem")"
       requiredClaims="sub"
       rolesPaths="roles";
};
CONF
# Artemis loops for ever on a provider URL ending in a slash (see COMPAT.md):
# its oidc-plain realm names the URL without one.
if [[ "$KIND" == artemis ]]; then
  sed -i "s|provider=\"http://localhost:$HTTP_PORT/\"|provider=\"http://localhost:$HTTP_PORT\"|" "$ETC/login.config"
fi
acceptor() {
  echo "<acceptor name=\"$1\">tcp://localhost:$2?protocols=AMQP;saslMechanisms=PLAIN;securityDomain=$1</acceptor>"
}
ACCEPTORS="$(acceptor oidc "$OIDC_PORT")$(acceptor oidc-plain "$PLAIN_PORT")$(acceptor oidc-tls "$TLS_PORT")"
sed -i "s|</acceptors>|$ACCEPTORS</acceptors>|" "$ETC/broker.xml"
if [[ -n "${MAX_DISK_USAGE:-}" ]]; then
  sed -i -E "s#<max-disk-usage>[0-9]+</max-disk-usage>#<max-disk-usage>$MAX_DISK_USAGE</max-disk-usage>#" "$ETC/broker.xml"
fi

mkdir -p "$I/log"
if windows; then
  (cd "$I/bin" && cmd /c "$(cygpath -w "$I/bin/artemis.cmd")" run > "$WORK/run.out" 2>&1 &)
else
  (cd "$I/bin" && ./artemis run > "$WORK/run.out" 2>&1 &)
fi
for _ in $(seq 1 600); do up && break; sleep 0.2; done
up || { echo "the broker did not start"; cat "$WORK/run.out"; exit 1; }

java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Suite.java")" \
  "127.0.0.1" "$OIDC_PORT" "$PLAIN_PORT" "$TLS_PORT" "$HTTP_PORT" "$HTTPS_PORT" \
  "$(native "$WORK/provider.p12")" "$@"
