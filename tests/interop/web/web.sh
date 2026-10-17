#!/usr/bin/env bash
# The web suite: a Saranga instance (saranga create) with its web server,
# against an Artemis 2.57.0 container with its console.
#
#   tests/interop/web/web.sh [--no-artemis]
#
# See web.py for what is checked. Saranga serves the console of the
# Artemis distribution (ARTEMIS_DIST, as scripts/build-artemis.sh builds
# it), and console.py compares it with Artemis's. ARTEMIS_WEB_URL names an
# Artemis already running (user artemis/artemis) in place of the
# container.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
SARANGA="$ROOT/target/release/saranga"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
OFFSET=700
WEB_PORT=$((8161 + OFFSET))
CORE_PORT=$((61616 + OFFSET))
ARTEMIS_WEB=18171
ARTEMIS_CORE=61681
NAME=saranga-web-ref
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
WORK="$(mktemp -d)"
PID=""
cleanup() {
  [[ -n "$PID" ]] && kill "$PID" 2>/dev/null
  [[ -d "$WORK/i" ]] && ARTEMIS_INSTANCE="$(native "$WORK/i")" "$SARANGA" stop 2>/dev/null
  docker rm -f "$NAME" >/dev/null 2>&1
  sleep 1
  rm -rf "$WORK"
}
trap cleanup EXIT

# Saranga: an instance with the metrics app too, whose console takes the
# view role as well (HAWTIO_ROLES).
I="$WORK/i"
"$SARANGA" create "$(native "$I")" --silent --no-autotune --user admin --password secret \
  --require-login --port-offset $OFFSET > /dev/null 2>&1 || { echo "create failed"; exit 1; }
sed -i 's|<app name="console" url="console" war="console.war"/>|&\n           <app name="metrics" url="metrics" war="metrics.war"/>|' "$I/etc/bootstrap.xml"
for p in "$I/etc/artemis.profile" "$I/etc/artemis.profile.cmd"; do
  [[ -f "$p" ]] && sed -i -E -e "s/^HAWTIO_ROLES=.*/HAWTIO_ROLES='amq,view'/" \
    -e "s/-Dhawtio.roles=amq /-Dhawtio.roles=amq,view /" "$p"
done
# management.xml: enable the commented-out example that lets view read
# queues.
python -I "$(native "$ROOT/tests/interop/web/enable_queue_access.py")" "$(native "$I/etc/management.xml")" || exit 1
ARTEMIS_HOME="$(native "$DIST")" ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" run > "$WORK/saranga.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$WEB_PORT") 2>/dev/null && break; sleep 0.1; done
"$SARANGA" user add --user-command-user viewer --user-command-password viewer --role view \
  --url "tcp://127.0.0.1:$CORE_PORT" --user admin --password secret --silent > /dev/null

ARGS=("http://localhost:$WEB_PORT" "tcp://127.0.0.1:$CORE_PORT")
if [[ "${1:-}" != "--no-artemis" && -n "${ARTEMIS_WEB_URL:-}" ]]; then
  ARGS+=("$ARTEMIS_WEB_URL")
elif [[ "${1:-}" != "--no-artemis" ]]; then
  docker rm -f "$NAME" > /dev/null 2>&1
  docker run -d --name "$NAME" -e ANONYMOUS_LOGIN=false \
    -p "127.0.0.1:$ARTEMIS_WEB:8161" -p "127.0.0.1:$ARTEMIS_CORE:61616" \
    "saranga/artemis-ref:$VERSION" > /dev/null
  for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ241004 && break; sleep 1; done
  ARGS+=("http://127.0.0.1:$ARTEMIS_WEB")
fi

python -I "$(native "$ROOT/tests/interop/web/web.py")" "${ARGS[@]}"
STATUS=$?
if [[ ${#ARGS[@]} -gt 2 && -d "$DIST/web/console.war" ]]; then
  python -I "$(native "$ROOT/tests/interop/web/console.py")"     "http://127.0.0.1:$WEB_PORT,admin,secret" "${ARGS[2]},artemis,artemis" || STATUS=1
fi
[[ $STATUS == 0 ]] || { echo "--- saranga log"; tail -20 "$WORK/saranga.log"; }
exit $STATUS
