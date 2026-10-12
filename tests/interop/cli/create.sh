#!/usr/bin/env bash
# saranga create against artemis create, and stop.
#
#   tests/interop/cli/create.sh
#
# 1. For each set of options, `saranga create` and `artemis create` (the
#    reference distribution's) make an instance; their etc/ files must be
#    the same but for paths and the password hash's salt.
# 2. A saranga-made instance runs through its own bin/artemis, takes a
#    queue made with its user, and stops on the Artemis CLI's `stop`.
# 3. An artemis-made instance runs on `saranga run` and stops on
#    `saranga stop`.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  rm -rf "$WORK"
}
trap cleanup EXIT

artemis() {
  local instance=()
  [[ -n "${ARTEMIS_INSTANCE:-}" ]] && instance=(-Dartemis.instance="$(native "$ARTEMIS_INSTANCE")")
  java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" "${instance[@]}" \
    org.apache.activemq.artemis.boot.Artemis "$@" 2>&1 | grep -v '^WARNING: '
}

FAILED=0
fail() { echo "FAIL $*"; FAILED=$((FAILED + 1)); }
pass() { echo "PASS $*"; }

# Paths, the password's salted hash and line ends differ.
normalize_tree() {
  local dir="$1" name="$2"
  (cd "$dir/etc" && for f in *; do
    sed -E -e 's/\r$//' \
      -e 's/ENC\([0-9]+:[0-9A-F]+:[0-9A-F]+\)/ENC(HASH)/' \
      -e "s#$(native "$dir" | sed 's#[.]#\\.#g')#INSTANCE#g" \
      -e "s#$(cygpath -w "$dir" 2>/dev/null | sed 's#\\#\\\\#g; s#[.]#\\.#g')#INSTANCE#g" \
      -e 's#^(set |export )?ARTEMIS_HOME=.*#ARTEMIS_HOME=HOME#' \
      "$f" > "$WORK/norm-$name-$f"
  done)
}

compare() {
  local label="$1"; shift
  local s="$WORK/s-$label" a="$WORK/a-$label"
  "$SARANGA" create "$(native "$s")" --silent --no-autotune "$@" > /dev/null 2>&1 || { fail "$label: saranga create"; return; }
  artemis create "$(native "$a")" --silent --no-autotune "$@" > /dev/null || { fail "$label: artemis create"; return; }
  rm -f "$WORK"/norm-*
  normalize_tree "$s" s
  normalize_tree "$a" a
  local bad=0
  for f in "$a"/etc/*; do
    f="$(basename "$f")"
    if ! diff -u "$WORK/norm-a-$f" "$WORK/norm-s-$f" > "$WORK/diff"; then
      bad=1; echo "--- $label: etc/$f differs"; head -30 "$WORK/diff"
    fi
  done
  [[ $(ls "$a/etc" | wc -l) == $(ls "$s/etc" | wc -l) ]] || { bad=1; echo "--- $label: etc/ has other files"; }
  [[ $bad == 0 ]] && pass "create $label" || fail "create $label"
}

compare defaults --user admin --password secret --require-login
compare options --user u --password p --allow-anonymous --role "my role" --port-offset 100 \
  --queues q1,q2:multicast --addresses a1,a2:anycast --ping 10.0.0.1 --global-max-size 100M \
  --global-max-messages 1000 --blocking --no-amqp-acceptor --no-hornetq-acceptor --nio \
  --journal-retention 3 --journal-retention-max-bytes 1G --security-manager basic --no-web \
  --autodelete --no-autocreate --support-advisory --purge-page-folders --name broker1 --host 127.0.0.1
compare nix --user u --password p --require-login --cygwin --java-options -Dx=1 --java-memory 1G
compare mapped --user u --password p --require-login --mapped --no-fsync --disable-persistence \
  --http-port 8200 --relax-jolokia --journal-device-block-size 1024 --data store

# Waits for a port, or for a process to end.
wait_port() { for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0; sleep 0.1; done; return 1; }
wait_gone() { for _ in $(seq 1 100); do kill -0 "$1" 2>/dev/null || return 0; sleep 0.1; done; return 1; }

# 2. A saranga instance, run through its bin/artemis, stopped by `artemis stop`.
I="$WORK/run-s"
"$SARANGA" create "$(native "$I")" --silent --no-autotune --user admin --password secret \
  --require-login --port-offset 300 > /dev/null 2>&1
if windows; then
  cmd /c "$(cygpath -w "$I/bin/artemis.cmd")" run > "$WORK/run-s.log" 2>&1 &
else
  "$I/bin/artemis" run > "$WORK/run-s.log" 2>&1 &
fi
PID=$!; PIDS+=("$PID")
if wait_port 61916; then
  pass "instance runs through bin/artemis"
  out="$("$SARANGA" queue create --name made --anycast --durable --auto-create-address \
    --url tcp://127.0.0.1:61916 --user admin --password secret --silent 2>&1)"
  [[ "$out" == *"created successfully."* ]] && pass "instance's user" || fail "instance's user: $out"
  out="$("$SARANGA" queue stat --url tcp://127.0.0.1:61916 --silent 2>&1)"
  [[ "$out" == *"Connection failed::"* ]] && pass "login required" || fail "login required: $out"
  ARTEMIS_INSTANCE="$I" artemis stop > /dev/null
  wait_gone "$PID" && pass "artemis stop stops it" || fail "artemis stop stops it"
else
  fail "instance runs through bin/artemis"; cat "$WORK/run-s.log"
fi

# 3. An Artemis instance on saranga run, stopped by `saranga stop`.
I="$WORK/run-a"
artemis create "$(native "$I")" --silent --no-autotune --user admin --password secret \
  --require-login --port-offset 400 > /dev/null
ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" run > "$WORK/run-a.log" 2>&1 &
PID=$!; PIDS+=("$PID")
if wait_port 62016; then
  pass "saranga runs an artemis instance"
  ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" stop
  wait_gone "$PID" && pass "saranga stop stops it" || fail "saranga stop stops it"
else
  fail "saranga runs an artemis instance"; cat "$WORK/run-a.log"
fi

# 4. upgrade: two like Artemis instances, one upgraded by `saranga upgrade`
#    and one by `artemis upgrade`, print the same steps and end with the
#    same etc/ (but for ARTEMIS_HOME). Saranga leaves Windows' service
#    wrapper alone, and writes bin/saranga beside bin/artemis.
upgrade_output() {
  sed -E -e 's/\r$//' -e "s#$(cygpath -w "$WORK" 2>/dev/null | sed 's#\\#\\\\#g; s#[.]#\\.#g')#WORK#g" \
    -e "s#$(native "$WORK" | sed 's#[.]#\\.#g')#WORK#g" -e 's#up-[sa]#up#g' \
    -e 's#artemis.home=.*#artemis.home=HOME#' | grep -v -E 'artemis-service\.(exe|xml)|saranga(\.cmd)?$'
}
upgrade_compare() {
  local label="$1"; shift
  local s="$WORK/up-s" a="$WORK/up-a"
  rm -rf "$s" "$a"
  artemis create "$(native "$s")" --silent --no-autotune --user u --password p --require-login \
    --java-memory 3G "$@" > /dev/null
  artemis create "$(native "$a")" --silent --no-autotune --user u --password p --require-login \
    --java-memory 3G "$@" > /dev/null
  "$SARANGA" upgrade "$(native "$s")" "$@" 2>&1 | upgrade_output > "$WORK/up-s.out"
  artemis upgrade "$(native "$a")" "$@" | upgrade_output > "$WORK/up-a.out"
  local bad=0
  diff -u "$WORK/up-a.out" "$WORK/up-s.out" || bad=1
  rm -f "$WORK"/norm-*
  normalize_tree "$s" s
  normalize_tree "$a" a
  for f in "$a"/etc/*; do
    f="$(basename "$f")"
    diff -u "$WORK/norm-a-$f" "$WORK/norm-s-$f" || bad=1
  done
  grep -q -- '-Xmx3G' "$s/etc/artemis.profile"* || { bad=1; echo "-Xmx not kept"; }
  [[ -f "$s/bin/saranga" || -f "$s/bin/saranga.cmd" ]] || { bad=1; echo "no bin/saranga"; }
  [[ $bad == 0 ]] && pass "upgrade $label" || fail "upgrade $label"
}
upgrade_compare defaults
upgrade_compare nix --cygwin

# The upgraded instance runs saranga; `artemis upgrade` takes it back.
artemis create "$(native "$WORK/up-r")" --silent --no-autotune --user u --password p --require-login > /dev/null
"$SARANGA" upgrade "$(native "$WORK/up-r")" > /dev/null 2>&1
instance_cmd() {
  if windows; then cmd /c "$(cygpath -w "$1/bin/artemis.cmd")" "${@:2}" < /dev/null 2>&1
  else "$1/bin/artemis" "${@:2}" 2>&1; fi
}
out="$(instance_cmd "$WORK/up-r" version)"
[[ "$out" == *"Saranga instance: $(native "$WORK/up-r")"* ]] && pass "upgraded instance runs saranga" \
  || fail "upgraded instance runs saranga: $out"
artemis upgrade "$(native "$WORK/up-r")" > /dev/null
out="$(instance_cmd "$WORK/up-r" version)"
[[ "$out" == *"Apache Artemis $VERSION"* ]] && pass "artemis upgrade takes it back" \
  || fail "artemis upgrade takes it back: $out"
"$SARANGA" upgrade "$(native "$WORK/up-r")" > /dev/null 2>&1
[[ -d "$WORK/up-r/old-config-bkp.2" ]] && pass "upgrade backups" || fail "upgrade backups"

if [[ $FAILED == 0 ]]; then echo "ALL PASSED"; else echo "$FAILED FAILED"; exit 1; fi
