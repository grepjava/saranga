#!/usr/bin/env bash
# The CLI suite: runs CLI commands against a broker and compares their
# output, normalized, with tests/interop/cli/expected.txt (made with the
# Artemis CLI against Artemis 2.57.0).
#
#   CLI=artemis|saranga SUITE_CMD=tests/interop/cli/cli.sh \
#     scripts/interop.sh saranga|artemis
#
# CLI=artemis (the default) runs the Artemis CLI of the reference
# distribution; CLI=saranga runs target/release/saranga. UPDATE=1 rewrites
# expected.txt instead of comparing.
set -uo pipefail

HOSTPORT="${1:?host:port}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/cli"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

# The user commands are an instance's; this one has nothing in it.
INSTANCE="$(mktemp -d)"
mkdir -p "$INSTANCE/etc"
trap 'rm -rf "$INSTANCE"' EXIT

cli() {
  case "${CLI:-artemis}" in
    artemis)
      timeout 120 java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
        -Dartemis.instance="$(native "$INSTANCE")" \
        org.apache.activemq.artemis.boot.Artemis "$@" ;;
    saranga) ARTEMIS_INSTANCE="$(native "$INSTANCE")" timeout 120 "$ROOT/target/release/saranga" "$@" ;;
  esac
}

URL="tcp://$HOSTPORT"
run() {
  echo "=== $*"
  cli "$@" --url "$URL" --silent </dev/null 2>&1
  echo "--- exit $?"
}
# The shell, its lines piped to the CLI alone (Artemis.run), as bin/artemis
# runs it: the lines are read at its prompts, connect keeps the URL for the
# commands after it, and nothing after exit runs.
shell() {
  echo "=== shell"
  printf 'NO_HISTORY\n' > "$INSTANCE/etc/history-file"
  printf '%s\n' "$@" | cli 2>&1
  echo "--- exit $?"
}
# Commands without --url.
run_bare() {
  echo "=== $*"
  cli "$@" </dev/null 2>&1
  echo "--- exit $?"
}

suite() {
  # queue
  run queue create --name cliq --address cliq --anycast --durable --auto-create-address
  run queue create --name cliq --address cliq --anycast --durable --auto-create-address
  run queue create --name clim --address clia --multicast --no-durable --auto-create-address --filter "color='red'" --max-consumers 3 --purge-on-no-consumers
  run queue update --name cliq --anycast --max-consumers 4
  run queue update --name nosuch --anycast
  # producer, browser, consumer
  run producer --destination queue://cliq --message-count 5
  run producer --destination cliq --message-count 2 --message hello --verbose
  run producer --destination queue://cliq --message-count 3 --text-size 20 --non-persistent --group g1 --properties '[{"type":"int","key":"k","value":"7"},{"type":"string","key":"s","value":"v"}]'
  run producer --destination queue://cliq --message-count 2 --message-size 10 --commit-interval 1
  run browser --destination queue://cliq
  run browser --destination queue://cliq --message-count 3 --filter "k = 7"
  run queue stat --queueName cliq
  run queue stat --queueName cliq --json
  run queue stat --field MESSAGE_COUNT --operation GREATER_THAN --value 5
  run queue stat --field name --operation EQUALS --value clim --single-line-header
  run queue stat --field NAME --operation NOPE --value x
  run queue stat --field NAME --queueName x
  run consumer --destination queue://cliq --message-count 2
  run consumer --destination queue://cliq --message-count 2 --filter "k = 7" --verbose
  run consumer --destination queue://cliq --message-count 20 --receive-timeout 500 --break-on-null
  # topics
  run consumer --destination topic://clitopic --message-count 1 --receive-timeout 200 --break-on-null
  run consumer --destination topic://clitopic --message-count 1 --receive-timeout 200 --durable --clientID cid --break-on-null
  run producer --destination topic://clitopic --message-count 2
  run consumer --destination topic://clitopic --message-count 5 --receive-timeout 300 --durable --clientID cid --break-on-null
  run queue stat --queueName cid
  # transfer
  run producer --destination queue://cliq --message-count 4
  run_bare transfer --source-url "$URL" --target-url "$URL" --source-queue cliq --target-queue cliq2 --message-count 3 --silent
  run_bare transfer --source-url "$URL" --target-url "$URL" --source-queue cliq --target-queue cliq2 --copy --silent
  run_bare transfer --source-url "$URL" --target-url "$URL" --source-queue cliq2 --target-topic clitopic --commit-interval 1 --receive-timeout 200 --silent
  run queue stat --field ADDRESS --operation CONTAINS --value cli
  run queue purge --name cliq
  run queue purge --name nosuch
  run queue delete --name cliq2
  run queue delete --name nosuch
  run queue delete --name clim --autoDeleteAddress
  # address
  run address create --name cliaddr --multicast
  run address create --name cliaddr --multicast
  run address create --name cliaddr2 --anycast --no-multicast
  run address show --name cliaddr
  run address show --name nosuch --bindings
  run address show --name nosuch
  run address update --name cliaddr --anycast --multicast
  run address delete --name cliaddr
  run address delete --name cliaddr2
  run address delete --name cliq
  run address delete --name cliq --force
  run address show
  # user
  run user list
  run user add --user-command-user u1 --user-command-password p1 --role r1,r2
  run user add --user-command-user u1 --user-command-password p1 --role r1
  run user add --user-command-user u2 --role r1
  run user list --user-command-user u1
  run user reset --user-command-user u1 --user-command-password p2 --role r3
  run user list
  run user rm --user-command-user u1
  run user rm --user-command-user u1
  # check
  run check node
  run check node --up --diskUsage 99 --memoryUsage 99
  run check node --diskUsage 0
  run check node --primary
  run check node --peers 1
  run check queue --name DLQ
  run check queue --name DLQ --produce 2 --browse 2 --consume 2
  run check queue --name DLQ --browse 1 --fail-at-end
  run check queue --name DLQ --consume -1 --browse -1
  run check queue --name nosuch
  run check queue --name DLQ --produce 0
  # Negative values, which fail or are taken as Artemis's commands take them.
  run producer --destination queue://clineg --message-count 1 --threads -5
  run producer --destination queue://clineg --message-count 1 --threads 0
  run consumer --destination queue://clineg --message-count 1 --receive-timeout 200 --threads -5
  run browser --destination queue://clineg --threads -5
  run check node --up --timeout -5
  run check queue --name DLQ --up --timeout -5
  run queue stat --queueName clineg --sleep -5
  # Mistakes on the command line: the message, then `help help`.
  run_bare producer --frob
  run_bare frob
  run_bare frob x --y
  run_bare queue stt
  run_bare queue
  run_bare producer x
  run_bare producer --message-count abc
  run_bare producer --message-count
  run_bare producer --message-count 1 --message-count 2
  run_bare producer --non-persistent=maybe
  run_bare producer --protocol frob
  run_bare queue create --name
  run_bare check queue --name x --frob
  run_bare data print --frob
  # And in the shell: Unknown command, or the message and the command's
  # usage, or picocli's suggestions.
  shell "producer --frob" "frob" "prodcer" "queue frob" "queue stt" "queue stat --u" \
    "producer --message-count abc" "queue" "producer x" "producer --help" "address creat" \
    "check nod" "exit"
  # shell
  shell "# the shell's connection" "" "connect --url $URL" \
    "queue create --name clishell --address clishell --anycast --auto-create-address --silent" \
    "producer --destination queue://clishell --message-count 2" \
    "queue stat --queueName clishell --silent" \
    "consumer --destination queue://clishell --message-count 2" \
    "queue delete --name clishell --autoDeleteAddress --silent" \
    "disconnect" "exit" "version"
}

# Timings, generated names and ports differ between runs and brokers. The
# shell's banner and prompt name the broker, and Saranga has no <TAB>
# completion to offer.
normalize() {
  local uuid='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
  sed -E \
    -e '/^WARNING: /d' \
    -e '/^Executing org\.apache\.activemq\.artemis\.cli\./d' \
    -e '/^Home::/d' \
    -e 's/\x1b\[[0-9;]*m//g' \
    -e 's/(^|> )Usage: (artemis|saranga) /\1Usage: CLI /' \
    -e 's/(Elapsed time in second :) [0-9]+ s/\1 # s/' \
    -e 's/(Elapsed time in milli second :) [0-9]+ milli/\1 # milli/' \
    -e 's/Time elapsed: [0-9.]+ sec/Time elapsed: # sec/' \
    -e "s#$URL#URL#g" \
    -e 's/"id":"[0-9]+"/"id":"#"/g' \
    -e 's/"id":[0-9]+/"id":#/g' \
    -e "s/$uuid/UUID/g" \
    -e 's/\r$//' \
    -e '/^ *[_/]/d' -e 's/^ (Apache Artemis|Saranga) [0-9.]+$/ BANNER/' \
    -e 's/(Apache Artemis|Saranga) [0-9.]+ > /CLI > /g' -e 's/ or press <TAB>//' \
    -e 's/^(Shell history disabled as recorded in) .*/\1 HISTORY/'
}

OUT="$(mktemp)"
suite | normalize > "$OUT"
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/expected.txt"
  echo "== updated $HERE/expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/expected.txt" "$OUT"; then
  echo "ALL PASSED"
  rm -f "$OUT"
else
  echo "FAILED (output in $OUT)"
  exit 1
fi
