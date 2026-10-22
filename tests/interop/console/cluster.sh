#!/usr/bin/env bash
# The console across a cluster: three instances made by "artemis create
# --clustered", each with the other two as static cluster connectors, run
# through their own bin/artemis. Node 1 is Saranga and serves the console;
# node 2 is Saranga and node 3 Artemis 2.57.0 (Saranga where no Artemis
# distribution is built). cluster.py checks the console's API on node 1 for
# the other nodes: /nodes, /nodes/{node}/... and /nodes/all/...; then, with
# the console's packages installed, Playwright drives its cluster pages.
#
#   NODE3=saranga|artemis tests/interop/console/cluster.sh
#
# SARANGA names the binary to test (target/release/saranga, built here, by
# default).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

if [[ -z "${SARANGA:-}" ]]; then
  SARANGA="$ROOT/target/release/saranga"
  cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
fi
if [[ -z "${NODE3:-}" ]]; then
  if [[ -d "$DIST" ]]; then NODE3=artemis; else NODE3=saranga; echo "no Artemis $VERSION at $DIST: node 3 is Saranga"; fi
fi
PYTHON="$(command -v python3 || command -v python)"
mkdir -p "$ROOT/target/cluster"
WORK="$(mktemp -d -p "$ROOT/target/cluster")"
N1="$WORK/node1" N2="$WORK/node2" N3="$WORK/node3"
CORE1=63416 CORE2=63516 CORE3=63616 WEB1=9961
cleanup() {
  for i in "$N1" "$N2" "$N3"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

make_instance() {
  local kind="$1" dir="$2" name="$3" offset="$4" others="$5" web="$6"
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --host localhost --name "$name"
    --clustered --static-cluster "$others"
    --cluster-user cluster --cluster-password secret)
  [[ "$web" == web ]] || opts+=(--no-web)
  case "$kind" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac
}

instance() {
  local dir="$1"; shift
  if windows; then
    (cd "$dir/bin" && timeout 300 cmd /c call "$(cygpath -w "$dir/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$dir/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi
}

launch() {
  local dir="$1"
  mkdir -p "$dir/log"
  if windows; then
    (cd "$dir/bin" && cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run >> "$dir/run.out" 2>&1 &)
  else
    (cd "$dir/bin" && ./artemis run >> "$dir/run.out" 2>&1 &)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_up() {
  for _ in $(seq 1 300); do up "$1" && return 0; sleep 0.2; done
  return 1
}
wait_for() {
  local file="$1" text="$2" n="${3:-1}"
  for _ in $(seq 1 600); do
    [[ "$(grep -c -- "$text" "$file" 2>/dev/null)" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}

U1="tcp://localhost:$CORE1" U2="tcp://localhost:$CORE2" U3="tcp://localhost:$CORE3"
make_instance saranga "$N1" node1 1800 "$U2,$U3" web
make_instance saranga "$N2" node2 1900 "$U1,$U3" noweb
make_instance "$NODE3" "$N3" node3 2000 "$U1,$U2" noweb
for i in "$N1" "$N2" "$N3"; do
  [[ -f "$i/etc/broker.xml" ]] || { echo "create failed for $i"; exit 1; }
done
launch "$N1"; launch "$N2"; launch "$N3"
for p in $CORE1 $CORE2 $CORE3 $WEB1; do
  wait_up "$p" || { echo "port $p never opened"; exit 1; }
done
for i in "$N1" "$N2" "$N3"; do
  wait_for "$i/log/artemis.log" AMQ221027 2 || { echo "$i did not bridge to the others"; tail -20 "$i/log/artemis.log"; exit 1; }
done
echo "the cluster formed (node 3 is $NODE3)"
# A user only node 1 knows.
"$SARANGA" user add --user-command-user visitor --user-command-password visitor --role amq \
  --url "$U1" --user admin --password admin --silent > /dev/null

"$PYTHON" -I "$(native "$ROOT/tests/interop/console/cluster.py")" \
  "http://localhost:$WEB1" "$(native "$SARANGA")" "$U1" "$U2" "$U3"
RESULT=$?

# The console's cluster pages in a browser (console/e2e/cluster.e2e.ts), when
# the console's packages are installed: SARANGA_E2E=0 skips them.
CONSOLE="$ROOT/console"
if [[ "${SARANGA_E2E:-1}" != 0 && -d "$CONSOLE/node_modules" ]]; then
  (cd "$CONSOLE" && SARANGA_URL="http://localhost:$WEB1" SARANGA_CLUSTER=1     npx playwright test e2e/cluster.e2e.ts) || RESULT=1
else
  echo "skip the console's cluster pages in a browser (no console/node_modules, or SARANGA_E2E=0)"
fi
[[ $RESULT == 0 ]] || for i in "$N1" "$N2" "$N3"; do echo "--- $i"; tail -15 "$i/log/artemis.log"; done
exit $RESULT
