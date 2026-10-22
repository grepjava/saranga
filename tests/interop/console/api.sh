#!/usr/bin/env bash
# The console API suite: a Saranga instance (saranga create) with the
# suite's management.xml and three users, checked by api.py; then the same
# instance with -Dsaranga.console.enabled=false, which serves no console.
# With the console's packages installed (npm ci in console/), Playwright
# then drives its pages in Chromium (console/e2e).
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

# The console's pages in a browser (console/e2e), when the console's
# packages are installed: SARANGA_E2E=0 skips them.
CONSOLE="$ROOT/console"
if [[ "${SARANGA_E2E:-1}" != 0 && -d "$CONSOLE/node_modules" ]]; then
  for q in orders.eu orders.us invoices; do
    "$SARANGA" producer --destination "queue://$q" --url "tcp://127.0.0.1:$CORE_PORT" \
      --user admin --password secret --message-count 20 --silent > /dev/null 2>&1
  done
  "$SARANGA" consumer --destination queue://invoices --url "tcp://127.0.0.1:$CORE_PORT" \
    --user admin --password secret --message-count 1 --receive-timeout 600000 --sleep 600000 \
    > /dev/null 2>&1 &
  E2E_CONSUMER=$!
  for _ in $(seq 1 50); do
    curl -s -u admin:secret "http://127.0.0.1:$WEB_PORT/saranga/api/v1/consumers" | grep -q invoices && break
    sleep 0.2
  done
  (cd "$CONSOLE" && SARANGA_URL="http://127.0.0.1:$WEB_PORT" npx playwright test) || RESULT=1
  kill "$E2E_CONSUMER" 2>/dev/null
else
  echo "skip the console's pages in a browser (no console/node_modules, or SARANGA_E2E=0)"
fi

# Turned off, the console and its API are not there, and Jolokia still is.
stop
# bin/artemis exports the profile's JAVA_ARGS; run takes their -D options.
JAVA_ARGS_APPEND="-Dsaranga.console.enabled=false" start
for path in /saranga/api/v1/openapi.json /saranga/; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$WEB_PORT$path")
  if [[ "$code" == 404 ]]; then
    echo "ok   with saranga.console.enabled=false $path is not served"
  else
    echo "FAIL with saranga.console.enabled=false $path answers $code"
    RESULT=1
  fi
done
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
