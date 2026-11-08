#!/usr/bin/env bash
# XA across Artemis and Saranga on one relational store.
#
#   ARTEMIS_DIST=<artemis with database-storage> POSTGRES_JDBC_JAR=<driver jar> \
#     tests/interop/database/xa.sh
#
# Runs where both brokers run (Linux, or WSL). Needs PostgreSQL with a database
# the brokers may own, as SARANGA_TEST_POSTGRES names it (the store tests'
# variable, the same default), and psql. Its tables are dropped. SARANGA names
# a built binary to use instead of building one.
#
# 1. For each pair, Saranga and Artemis either way round and each with itself:
#    the first broker takes seed messages and three prepared XA transactions
#    (a send and an ack with a duplicate ID, the same to roll back, a large
#    message) and is killed; the second recovers them by Xid, commits two and
#    rolls one back; what a consumer gets is exactly what should be there, a
#    resent duplicate ID is dropped, nothing is left in the tables and the
#    second broker starts again cleanly.
# 2. A record delete Saranga keeps for a prepared transaction (a row of type
#    201, which Artemis does not know) is put in the tables by hand: Artemis
#    warns of it and finishes the transaction, and the next Saranga start
#    removes it.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/database"
DIST="${ARTEMIS_DIST:?ARTEMIS_DIST names an Artemis distribution with the relational store}"
JAR="${POSTGRES_JDBC_JAR:?POSTGRES_JDBC_JAR names the PostgreSQL JDBC driver}"
URL="${SARANGA_TEST_POSTGRES:-jdbc:postgresql://127.0.0.1:5432/saranga?user=saranga&password=saranga}"
PORT="${XA_PORT:-61616}"
SARANGA="${SARANGA:-}"

[ -x "$DIST/bin/artemis" ] || { echo "PRECOND no $DIST/bin/artemis"; exit 1; }
ls "$DIST"/lib/artemis-database-storage-*.jar > /dev/null 2>&1 || { echo "PRECOND $DIST has no relational store"; exit 1; }
[ -f "$JAR" ] || { echo "PRECOND no $JAR"; exit 1; }
command -v psql > /dev/null || { echo "PRECOND no psql"; exit 1; }
command -v java > /dev/null || { echo "PRECOND no java"; exit 1; }
PG="${URL#jdbc:}"
q() { psql "$PG" -qtA -c "$1"; }
q 'select 1' > /dev/null || { echo "PRECOND PostgreSQL unreachable at $PG"; exit 1; }
ss -ltn | grep -q ":$PORT " && { echo "PRECOND port $PORT in use"; exit 1; }
if [ -z "$SARANGA" ]; then
  cargo build -q --release -p saranga-cli --manifest-path "$ROOT/Cargo.toml" || exit 1
  SARANGA="$ROOT/target/release/saranga"
fi
[ -x "$SARANGA" ] || { echo "PRECOND no $SARANGA"; exit 1; }

WORK="$(mktemp -d)"
BROKER=
stop() { # kills the broker and all it started
  [ -n "$BROKER" ] && kill -9 -- "-$BROKER" 2> /dev/null
  BROKER=
  for _ in $(seq 1 30); do ss -ltn | grep -q ":$PORT " || return 0; sleep 1; done
  echo "FAIL port $PORT still in use"; exit 1
}
trap 'stop; rm -rf "$WORK"' EXIT

FAILED=0
fail() { echo "FAIL $*"; FAILED=$((FAILED + 1)); }

# One instance of each, on the database, with a duplicate-ID cache of two.
common=(--silent --allow-anonymous --user admin --password admin --no-web --no-hornetq-acceptor
  --no-amqp-acceptor --no-mqtt-acceptor --no-stomp-acceptor --default-port "$PORT"
  --database-storage --jdbc-connection-url "$URL")
"$SARANGA" create "$WORK/sar" "${common[@]}" > "$WORK/create-sar.log" 2>&1 || { cat "$WORK/create-sar.log"; exit 1; }
"$DIST/bin/artemis" create "${common[@]}" --jdbc-driver-class-name org.postgresql.Driver "$WORK/art" \
  > "$WORK/create-art.log" 2>&1 || { cat "$WORK/create-art.log"; exit 1; }
cp "$JAR" "$WORK/art/lib/"
for s in sar art; do
  x="$WORK/$s/etc/broker.xml"
  # Artemis's create does not escape the URL's `&`.
  sed -i 's/&\([a-z]*=\)/\&amp;\1/g; s/&amp;amp;/\&amp;/g' "$x"
  sed -i 's#<max-disk-usage>90</max-disk-usage>#<max-disk-usage>100</max-disk-usage><id-cache-size>2</id-cache-size>#' "$x"
  grep -q '<id-cache-size>2' "$x" || { echo "PRECOND id-cache-size in $s"; exit 1; }
  grep -q '<database-storage>true' "$x" || { echo "PRECOND $s not on the database"; exit 1; }
done

start() { # instance label: on the tables as they are
  local log="$WORK/$2.log"
  rm -rf "$WORK/$1/data"
  setsid "$WORK/$1/bin/artemis" run > "$log" 2>&1 &
  BROKER=$!
  for _ in $(seq 1 120); do
    grep -q AMQ221007 "$log" && return 0
    kill -0 "$BROKER" 2> /dev/null || break
    sleep 0.5
  done
  tail -5 "$log"; fail "$2: not live"; stop; return 1
}
client() { # mode label
  local log="$WORK/$2-$1.log"
  timeout 60 java -cp "$DIST/lib/*" "$HERE/XaCross.java" "$1" "tcp://localhost:$PORT" > "$log" 2>&1
  grep -E 'recovered|consumed' "$log"
  grep -q "XACROSS $1 ok" "$log" || { grep -E 'XACROSS|Exception|error:' "$log" | head -3; fail "$2: $1"; return 1; }
}
warnings() { grep -E ' (WARN|ERROR) ' "$WORK/$1.log" | cut -c1-200; }
rows() {
  echo "  rows: prepared=$(q 'select count(*) from db_prep_tx') references=$(q 'select count(*) from db_references')" \
    "messages=$(q 'select count(*) from db_messages') type201=$(q 'select count(*) from db_broker_data where record_type = 201')" \
    "in-tx=$(q 'select count(*) from db_broker_data where tx_id is not null')"
}
reset() { q 'drop schema public cascade; create schema public' > /dev/null; }

cross() { # first second
  local l="$1-$2"
  echo "== $1 prepares, $2 finishes"
  reset
  start "$1" "$l-1" || return
  client seed "$l" && client prepare "$l" || { stop; return; }
  [ "$(q 'select count(*) from db_prep_tx')" = 3 ] || fail "$l: not 3 prepared"
  stop
  start "$2" "$l-2" || return
  warnings "$l-2"
  client finish "$l" && client check "$l" || { stop; return; }
  sleep 3
  rows
  [ "$(q 'select (select count(*) from db_prep_tx) + (select count(*) from db_references) + (select count(*) from db_messages) + (select count(*) from db_broker_data where tx_id is not null)')" = 0 ] \
    || fail "$l: rows left"
  stop
  start "$2" "$l-3" || return
  [ -z "$(warnings "$l-3")" ] || { warnings "$l-3"; fail "$l: warnings on the next start"; }
  stop
}
for pair in "sar art" "art sar" "sar sar" "art art"; do cross $pair; done

echo "== a type 201 row: Saranga prepares, Artemis finishes, Saranga starts"
reset
if start sar r201-1; then
  client seed r201 && client prepare r201
  stop
  q "insert into db_broker_data (id, record_type, tx_id, data_record) select 9000000000000, 201, min(tx_id), decode('00000000000004d2', 'hex') from db_prep_tx" > /dev/null
  if start art r201-2; then
    warnings r201-2 | grep -q 'Unexpected record type' || fail "r201: Artemis did not notice the row"
    client finish r201 && client check r201
    stop
    [ "$(q 'select count(*) from db_broker_data where record_type = 201')" = 1 ] || fail "r201: Artemis removed the row, the test needs changing"
    if start sar r201-3; then
      [ "$(q 'select count(*) from db_broker_data where record_type = 201')" = 0 ] || fail "r201: Saranga left the row"
      stop
    fi
  fi
fi

[ "$FAILED" = 0 ] && echo "XA OK" || echo "XA FAILED: $FAILED"
exit "$FAILED"
