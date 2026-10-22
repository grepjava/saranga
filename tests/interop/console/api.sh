#!/usr/bin/env bash
# The console API suite: a Saranga instance (saranga create) with the
# suite's management.xml and three users, checked by api.py; then the same
# instance with -Dsaranga.console.enabled=false, which serves no console.
#
#   tests/interop/console/api.sh
#
# SARANGA names the binary to test (target/release/saranga, built here, by
# default).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OFFSET=750
WEB_PORT=$((8161 + OFFSET))
CORE_PORT=$((61616 + OFFSET))
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

if [[ -z "${SARANGA:-}" ]]; then
  SARANGA="$ROOT/target/release/saranga"
  cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
fi
PYTHON="$(command -v python3 || command -v python)"
WORK="$(mktemp -d)"
I="$WORK/i"
PID=""
stop() {
  [[ -n "$PID" ]] || return 0
  ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" stop > /dev/null 2>&1
  for _ in $(seq 1 100); do kill -0 "$PID" 2>/dev/null || break; sleep 0.1; done
  kill "$PID" 2>/dev/null
  PID=""
}
cleanup() { stop; rm -rf "$WORK"; }
trap cleanup EXIT

start() {
  ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" run >> "$WORK/saranga.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$WEB_PORT") 2>/dev/null && break; sleep 0.1; done
}

"$SARANGA" create "$(native "$I")" --silent --no-autotune --user admin --password secret \
  --require-login --port-offset $OFFSET > /dev/null 2>&1 || { echo "create failed"; exit 1; }
cp "$ROOT/tests/interop/console/management.xml" "$I/etc/management.xml"
for p in "$I/etc/artemis.profile" "$I/etc/artemis.profile.cmd"; do
  [[ -f "$p" ]] && sed -i -E -e "s/^HAWTIO_ROLES=.*/HAWTIO_ROLES='amq,view'/" \
    -e "s/-Dhawtio.roles=amq /-Dhawtio.roles=amq,view /" "$p"
done
start
for u in "viewer view" "outsider other"; do
  set -- $u
  "$SARANGA" user add --user-command-user "$1" --user-command-password "$1" --role "$2" \
    --url "tcp://127.0.0.1:$CORE_PORT" --user admin --password secret --silent > /dev/null
done

# A Core consumer, whose session api.py closes (Saranga closes the
# sessions of Core alone; see COMPAT.md).
"$SARANGA" consumer --destination queue://console.core --url "tcp://127.0.0.1:$CORE_PORT" \
  --user admin --password secret --message-count 1 --receive-timeout 60000 > /dev/null 2>&1 &
CORE_CONSUMER=$!
"$PYTHON" -I "$(native "$ROOT/tests/interop/console/api.py")" \
  "http://127.0.0.1:$WEB_PORT" "127.0.0.1:$CORE_PORT"
RESULT=$?
kill "$CORE_CONSUMER" 2>/dev/null

# Turned off, the console's API is not there, and Jolokia still is.
stop
# bin/artemis exports the profile's JAVA_ARGS; run takes their -D options.
JAVA_ARGS_APPEND="-Dsaranga.console.enabled=false" start
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$WEB_PORT/saranga/api/v1/openapi.json")
if [[ "$code" == 404 ]]; then
  echo "ok   with saranga.console.enabled=false the API is not served"
else
  echo "FAIL with saranga.console.enabled=false the API answers $code"
  RESULT=1
fi
code=$(curl -s -o /dev/null -w '%{http_code}' -u admin:secret -H "Origin: http://localhost" \
  "http://127.0.0.1:$WEB_PORT/console/jolokia/version")
if [[ "$code" == 200 ]]; then
  echo "ok   Jolokia is still served"
else
  echo "FAIL Jolokia answers $code"
  RESULT=1
fi
[[ $RESULT == 0 ]] || { echo "--- saranga.log"; tail -40 "$WORK/saranga.log"; }
exit $RESULT
