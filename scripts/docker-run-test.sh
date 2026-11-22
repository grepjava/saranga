#!/usr/bin/env bash
# Tests the image's entry point (docker/docker-run.sh) without an image: a
# copy runs against a stand-in for `saranga create` that records its
# arguments, one a line, and makes the instance's etc/broker.xml and
# bin/artemis.
#   scripts/docker-run-test.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT
failures=0

cat > "$W/saranga" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$ARGS_FILE"
mkdir -p etc bin
touch etc/broker.xml
printf '#!/usr/bin/env bash\necho "ran $*"\n' > bin/artemis
chmod +x bin/artemis
EOF
chmod +x "$W/saranga"
sed "s|/opt/saranga/bin/saranga|$W/saranga|" "$ROOT/docker/docker-run.sh" > "$W/docker-run.sh"
grep -q "$W/saranga" "$W/docker-run.sh" || { echo "PRECOND the entry point no longer calls /opt/saranga/bin/saranga"; exit 1; }
chmod +x "$W/docker-run.sh"

# Runs the entry point in a new instance directory with the given
# environment; its output goes to out, the create arguments to args.
entry() {
  local dir="$W/case-$1"; shift
  mkdir -p "$dir"
  # Files a wildcard in an argument would match.
  touch "$dir/pXss" "$dir/aXb"
  if [ -n "${OVERRIDE:-}" ]; then mkdir -p "$dir/etc-override"; touch "$dir/etc-override/$OVERRIDE"; fi
  (cd "$dir" && env "$@" ARGS_FILE="$dir/args" "$W/docker-run.sh" run > "$dir/out")
  echo "$dir"
}

expect() { # what, expected, actual
  if [ "$2" == "$3" ]; then
    echo "ok   $1"
  else
    echo "FAIL $1"; echo "  expected: $(printf '%q ' "$2")"; echo "  actual:   $(printf '%q ' "$3")"
    failures=$((failures + 1))
  fi
}

d=$(entry credentials 'ARTEMIS_USER=ad min' 'ARTEMIS_PASSWORD=p?ss *' ANONYMOUS_LOGIN=false \
  'EXTRA_ARGS=--http-host 0.0.0.0 --name a?b --relax-jolokia')
expect "credentials are one argument each, not expanded; EXTRA_ARGS split, not expanded" \
  "$(printf '%s\n' create --user 'ad min' --password 'p?ss *' --silent --require-login \
      --http-host 0.0.0.0 --name 'a?b' --relax-jolokia .)" "$(cat "$d/args")"
expect "the instance's bin/artemis runs with the container's arguments" "ran run" "$(tail -1 "$d/out")"

d=$(entry empty ARTEMIS_USER=artemis ARTEMIS_PASSWORD=artemis ANONYMOUS_LOGIN=true EXTRA_ARGS=)
expect "no EXTRA_ARGS adds no argument" \
  "$(printf '%s\n' create --user artemis --password artemis --silent --allow-anonymous .)" "$(cat "$d/args")"

d=$(OVERRIDE='my broker.xml' entry override ARTEMIS_USER=artemis ARTEMIS_PASSWORD=artemis EXTRA_ARGS=)
expect "an override file with a space in its name is copied" "yes" \
  "$([ -f "$d/etc/my broker.xml" ] && echo yes || echo no)"

[ "$failures" -eq 0 ] && echo "PASS" || { echo "$failures FAILED"; exit 1; }
