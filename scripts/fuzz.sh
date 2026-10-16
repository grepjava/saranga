#!/usr/bin/env bash
# Runs the cargo-fuzz targets in fuzz/, each for SECONDS (default 60), from
# seeds cut from the golden corpus (fuzz-seeds) plus whatever the corpus
# already holds. Stops at the first target that fails: libFuzzer leaves the
# input in fuzz/artifacts/<target>/. Fix it, then keep the input in
# fuzz/regressions/<target>/, where `cargo test -p saranga-fuzz` runs it.
#
#     scripts/fuzz.sh [SECONDS] [TARGET...]
#
# Needs a nightly toolchain and cargo-fuzz (cargo install cargo-fuzz), on
# Linux or macOS. JOBS runs that many targets at once (default 1).
# FUZZ_ARGS adds libFuzzer options.
set -euo pipefail
cd "$(dirname "$0")/.."

seconds=${1:-60}
shift || true
if [ $# -gt 0 ]; then
	targets=("$@")
else
	mapfile -t targets < <(cd fuzz && cargo +nightly fuzz list)
fi

cargo run -q -p saranga-fuzz --bin fuzz-seeds fuzz/corpus
(cd fuzz && cargo +nightly fuzz build)

run() {
	local t=$1
	echo "== $t (${seconds}s)"
	# -rss_limit_mb: an input that makes a decoder allocate what its
	# lengths claim, rather than what it holds, fails here.
	(cd fuzz && cargo +nightly fuzz run "$t" "corpus/$t" -- \
		-max_total_time="$seconds" -rss_limit_mb=2048 -print_final_stats=1 \
		${FUZZ_ARGS:-}) >"fuzz/$t.log" 2>&1 || {
		echo "== $t FAILED"
		tail -n 60 "fuzz/$t.log"
		return 1
	}
	grep -E "^stat::(number_of_executed_units|new_units_added)" "fuzz/$t.log" | tr '\n' ' '
	echo
}

status=0
running=0
for t in "${targets[@]}"; do
	run "$t" &
	running=$((running + 1))
	if [ "$running" -ge "${JOBS:-1}" ]; then
		wait -n || status=1
		running=$((running - 1))
	fi
done
while [ "$running" -gt 0 ]; do
	wait -n || status=1
	running=$((running - 1))
done
exit $status
