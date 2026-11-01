#!/usr/bin/env bash
# The GSSAPI suite: an instance made by "artemis create --require-login"
# whose AMQP acceptors offer GSSAPI, the broker's keys coming from a login
# through the JDK's Krb5LoginModule in the acceptor's saslLoginConfigScope,
# and PLAIN against a realm whose Krb5LoginModule checks the password with
# the KDC. Kdc.java runs the KDC (Apache Kerby, from the Maven repository);
# Suite.java logs in with Qpid JMS.
#
#   tests/interop/gssapi/gssapi.sh [saranga|artemis] [test-name...]
#
#   gss       saslLoginConfigScope amqp-sasl-gssapi (amqp/localhost's keys),
#             users and roles from the krb5 realm (Artemis's
#             Krb5LoginModule, then roles by kerberos-roles.properties)
#   unbound   the same, the keytab's keys for any principal (principal=*)
#   other     amqp/otherhost's keys, which a ticket for amqp/localhost does
#             not match
#   nokey     amqp/localhost without storeKey, so no keys to accept with
#   password  PLAIN, the password checked by the JDK's Krb5LoginModule, the
#             KerberosPrincipal made the user by PrincipalConversionLoginModule
#
# Needs Linux (or macOS) with MIT Kerberos's libraries, for Saranga.
# OFFSET: the port offset, 2300 unless given. M2_REPO: the Maven repository
# with Apache Kerby 2.0.3 (~/.m2/repository unless given). CARGO_TARGET_DIR
# is honoured.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/gssapi"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
M2="${M2_REPO:-$HOME/.m2/repository}"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
KIND="${1:-saranga}"; shift || true
OFFSET="${OFFSET:-2300}"
PORT=$((61616 + OFFSET))
GSS_PORT=$((PORT + 1))
UNBOUND_PORT=$((PORT + 2))
OTHER_PORT=$((PORT + 3))
NOKEY_PORT=$((PORT + 4))
PASSWORD_PORT=$((PORT + 5))
KDC_PORT=$((PORT + 10))

mkdir -p "$ROOT/target/gssapi"
WORK="$(mktemp -d -p "$ROOT/target/gssapi")"
I="$WORK/i"
KDC="$WORK/kdc"
instance() { (cd "$I/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1); }
up() { (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; }
KDC_PID=
cleanup() {
  [[ -d "$I" ]] && instance stop > /dev/null 2>&1
  for _ in $(seq 1 100); do up || break; sleep 0.1; done
  [[ -n "$KDC_PID" ]] && kill "$KDC_PID" 2> /dev/null
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

KERBY="$(find "$M2/org/apache/kerby" -name '*-2.0.3.jar' | tr '\n' ':')"
[[ -n "$KERBY" ]] || { echo "Apache Kerby 2.0.3 is not in $M2"; exit 1; }
java -cp "$DIST/lib/*:$KERBY" "$HERE/Kdc.java" "$KDC" "$KDC_PORT" > "$WORK/kdc.out" 2>&1 &
KDC_PID=$!
for _ in $(seq 1 300); do [[ -f "$KDC/ready" ]] && break; sleep 0.2; done
[[ -f "$KDC/ready" ]] || { echo "the KDC did not start"; cat "$WORK/kdc.out"; exit 1; }

opts=(--silent --no-autotune --user artemis --password artemis --require-login
  --port-offset "$OFFSET" --no-web --host localhost)
case "$KIND" in
  saranga)
    CARGO_TARGET_DIR="$TARGET" cargo build -q --release -p saranga-cli --manifest-path "$ROOT/Cargo.toml" || exit 1
    "$TARGET/release/saranga" create "$I" "${opts[@]}" > /dev/null 2>&1 || { echo "create failed"; exit 1; }
    ;;
  artemis)
    java -cp "$DIST/lib/*" -Dartemis.home="$DIST" \
      org.apache.activemq.artemis.boot.Artemis create "$I" "${opts[@]}" > /dev/null 2>&1 \
      || { echo "create failed"; exit 1; }
    ;;
  *) echo "usage: gssapi.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

ETC="$I/etc"
JAAS=org.apache.activemq.artemis.spi.core.security.jaas
KRB5=com.sun.security.auth.module.Krb5LoginModule
service() { # scope principal [options]
  printf '%s {\n   %s required\n       isInitiator=false\n       useKeyTab=true\n       keyTab="%s"\n       principal="%s"%s;\n};\n\n' \
    "$1" "$KRB5" "$KDC/service.keytab" "$2" "${3:-}"
}
{
  cat <<CONF
activemq {
   $JAAS.PropertiesLoginModule required
       reload=true
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};

krb5 {
   $JAAS.Krb5LoginModule required;
   $JAAS.PropertiesLoginModule optional
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="kerberos-roles.properties";
};

krb5-password {
   $KRB5 required;
   $JAAS.PrincipalConversionLoginModule required
       principalClassList="javax.security.auth.kerberos.KerberosPrincipal";
   $JAAS.PropertiesLoginModule optional
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="kerberos-roles.properties";
};

CONF
  service amqp-sasl-gssapi amqp/localhost $'\n       storeKey=true'
  service sasl-unbound '*' $'\n       storeKey=true'
  service sasl-other amqp/otherhost $'\n       storeKey=true'
  service sasl-nokey amqp/localhost
} > "$ETC/login.config"
printf 'amq = client@EXAMPLE.COM,pwuser@EXAMPLE.COM\n' > "$ETC/kerberos-roles.properties"
gss() {
  echo "<acceptor name=\"$1\">tcp://localhost:$2?protocols=AMQP;saslMechanisms=GSSAPI;saslLoginConfigScope=$3;securityDomain=krb5</acceptor>"
}
ACCEPTORS="$(gss gss "$GSS_PORT" amqp-sasl-gssapi)$(gss unbound "$UNBOUND_PORT" sasl-unbound)"
ACCEPTORS="$ACCEPTORS$(gss other "$OTHER_PORT" sasl-other)$(gss nokey "$NOKEY_PORT" sasl-nokey)"
ACCEPTORS="$ACCEPTORS<acceptor name=\"password\">tcp://localhost:$PASSWORD_PORT?protocols=AMQP;saslMechanisms=PLAIN;securityDomain=krb5-password</acceptor>"
sed -i "s|</acceptors>|$ACCEPTORS</acceptors>|" "$ETC/broker.xml"
if [[ -n "${MAX_DISK_USAGE:-}" ]]; then
  sed -i -E "s#<max-disk-usage>[0-9]+</max-disk-usage>#<max-disk-usage>$MAX_DISK_USAGE</max-disk-usage>#" "$ETC/broker.xml"
fi

cat > "$WORK/client-login.config" <<CONF
client-keytab {
   $KRB5 required
       useKeyTab=true
       keyTab="$KDC/client.keytab"
       principal="client"
       doNotPrompt=true;
};

norole-keytab {
   $KRB5 required
       useKeyTab=true
       keyTab="$KDC/client.keytab"
       principal="norole"
       doNotPrompt=true;
};
CONF

mkdir -p "$I/log"
export JAVA_ARGS_APPEND="-Djava.security.krb5.conf=$KDC/krb5.conf"
(cd "$I/bin" && ./artemis run > "$WORK/run.out" 2>&1 &)
for _ in $(seq 1 600); do up && break; sleep 0.2; done
up || { echo "the broker did not start"; cat "$WORK/run.out"; exit 1; }

java -cp "$DIST/lib/*" -Djava.security.krb5.conf="$KDC/krb5.conf" \
  -Djava.security.auth.login.config="$WORK/client-login.config" "$HERE/Suite.java" \
  "$GSS_PORT" "$UNBOUND_PORT" "$OTHER_PORT" "$NOKEY_PORT" "$PASSWORD_PORT" "$@"
