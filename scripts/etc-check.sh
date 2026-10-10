#!/usr/bin/env bash
# The Phase 6 exit check: unmodified Artemis etc/ directories, as
# `artemis create` writes them for typical deployments, start Saranga.
#
#   scripts/etc-check.sh [variant...]
#
# Each variant is made by the reference image's own `artemis create`, then
# run with `saranga run --etc <instance>/etc` exactly as created. A variant
# Saranga serves must take a login and a round trip on a queue (and refuse
# a wrong password when it requires logins); a variant Saranga cannot serve
# yet (an HA backup, the JDBC store) must refuse to start with a reason.
set -euo pipefail

VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
NAME="saranga-etc"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

# name | artemis create arguments | expectation | port | user | password | queue
VARIANTS=(
  "default|--require-login|serve|61616|admin|secret|etc.check"
  "anonymous|--allow-anonymous|serve|61616|||etc.check"
  "nio-blocking|--require-login --nio --no-fsync --blocking --no-amqp-acceptor --no-mqtt-acceptor --no-stomp-acceptor --no-hornetq-acceptor --queues q1,q2:multicast --addresses a1,a2:anycast --no-autocreate --global-max-size 100M --port-offset 10|serve|61626|admin|secret|q1"
  "mapped-paging|--require-login --mapped --paging --journal-retention 7 --purge-page-folders --no-autotune|serve|61616|admin|secret|etc.check"
  "no-persistence|--require-login --disable-persistence --support-advisory --suppress-internal-management-objects|serve|61616|admin|secret|etc.check"
  "custom-role|--require-login --role ops --user ops-user --password ops-pass --name broker1|serve|61616|ops-user|ops-pass|etc.check"
  "web-ssl|--require-login --http-host 0.0.0.0 --relax-jolokia --java-memory 1G|serve|61616|admin|secret|etc.check"
  "replicated-primary|--require-login --clustered --replicated --static-cluster tcp://backup:61616 --cluster-user cu --cluster-password cp|serve|61616|admin|secret|etc.check"
  "static-cluster|--require-login --clustered --static-cluster tcp://other:61616 --cluster-user cu --cluster-password cp --message-load-balancing ON_DEMAND --max-hops 2|serve|61616|admin|secret|etc.check"
  "shared-store-primary|--require-login --shared-store --failover-on-shutdown --static-cluster tcp://backup:61616 --cluster-user cu --cluster-password cp|serve|61616|admin|secret|etc.check"
  "shared-store-backup|--require-login --shared-store --backup --static-cluster tcp://primary:61616 --cluster-user cu --cluster-password cp|refuse"
  "replicated-backup|--require-login --replicated --backup --clustered --static-cluster tcp://primary:61616 --cluster-user cu --cluster-password cp|refuse"
  "jdbc|--require-login --jdbc|refuse"
)

WORK="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
CP="$(native "$DIST/lib")/*"

# Make every variant in one container.
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --rm --name "$NAME" --entrypoint sleep "saranga/artemis-ref:$VERSION" 3600 >/dev/null
for v in "${VARIANTS[@]}"; do
  IFS='|' read -r name args _ <<< "$v"
  only=("$@")
  if [[ ${#only[@]} -gt 0 && ! " ${only[*]} " == *" $name "* ]]; then continue; fi
  user_args="--user admin --password secret"
  [[ "$args" == *"--user "* ]] && user_args=""
  # shellcheck disable=SC2086 # the arguments are meant to split
  docker exec "$NAME" /opt/artemis/bin/artemis create --silent $user_args $args "/tmp/$name" >/dev/null 2>&1
  docker exec "$NAME" tar cf "/tmp/$name.tar" -C "/tmp/$name" etc
  mkdir -p "$WORK/$name"
  docker cp "$NAME:/tmp/$name.tar" "$(native "$WORK/$name.tar")" >/dev/null
  tar --force-local -xf "$WORK/$name.tar" -C "$WORK/$name"
done

failures=0
for v in "${VARIANTS[@]}"; do
  IFS='|' read -r name args expect port user password queue <<< "$v"
  [[ -d "$WORK/$name/etc" ]] || continue
  log="$WORK/$name.log"
  "$ROOT/target/release/saranga" run --etc "$(native "$WORK/$name/etc")" > "$log" 2>&1 &
  BROKER_PID=$!
  started=""
  for _ in $(seq 1 50); do
    if ! kill -0 "$BROKER_PID" 2>/dev/null; then break; fi
    if grep -aq "AMQ221007" "$log"; then started=yes; break; fi
    sleep 0.2
  done
  result=""
  case "$expect" in
    serve)
      if [[ -z "$started" ]]; then
        result="FAIL (did not start)"
      else
        wrong=()
        [[ -n "$user" ]] && wrong=(wrong-password)
        if out=$(java -cp "$CP" "$(native "$ROOT/tests/interop/jms/EtcCheck.java")" \
                   "tcp://127.0.0.1:$port" "$user" "$password" "$queue" "${wrong[@]}" 2>&1); then
          result="ok"
        else
          result="FAIL ($(echo "$out" | grep -v '^WARNING' | grep -v '^\s*at ' | tail -2 | tr '\n' ' '))"
        fi
      fi
      ;;
    refuse)
      if [[ -n "$started" ]]; then
        result="FAIL (started)"
      else
        # anyhow prints the context, then each cause indented.
        reason=$(sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a -A6 '^Caused by:' | grep -a '^    ' | tr -s ' ' | tr '\n' ' ' || true)
        result="ok (refused: ${reason:-no reason given})"
      fi
      ;;
  esac
  kill "$BROKER_PID" 2>/dev/null || true
  wait "$BROKER_PID" 2>/dev/null || true
  BROKER_PID=""
  warnings=$(sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -ac " WARN " || true)
  printf '%-22s %s, %s warnings\n' "$name" "$result" "$warnings"
  if [[ "$result" == FAIL* ]]; then
    failures=$((failures + 1))
    sed 's/\x1b\[[0-9;]*m//g' "$log" | tail -15
  elif [[ -n "${VERBOSE:-}" ]]; then
    sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a " WARN " || true
  fi
done
[[ $failures -eq 0 ]] && echo "== etc check passed" || { echo "== $failures failed"; exit 1; }
