#!/usr/bin/env bash
# Runs tests/interop/hornetq/HornetQ.java with a HornetQ 2.4.7 JMS client
# against Saranga or the reference Artemis broker, both with
# tests/interop/hornetq/broker.xml.
#
#   scripts/hornetq-interop.sh [saranga|artemis]
#
# The HornetQ client jars are fetched once with Maven (in Docker) into
# target/client-hornetq.
set -euo pipefail

TARGET="${1:-saranga}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

LIB="$ROOT/target/client-hornetq"
if [[ ! -f "$LIB/lib/hornetq-jms-client-2.4.7.Final.jar" ]]; then
  mkdir -p "$LIB"
  cat > "$LIB/pom.xml" <<'POM'
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>saranga</groupId><artifactId>client-hornetq</artifactId><version>1</version>
  <dependencies>
    <dependency><groupId>org.hornetq</groupId><artifactId>hornetq-jms-client</artifactId><version>2.4.7.Final</version></dependency>
  </dependencies>
</project>
POM
  docker run --rm -v "$(native "$LIB"):/w" -v saranga-m2:/root/.m2 -w /w maven:3.9-eclipse-temurin-21 \
    mvn -B -q -ntp dependency:copy-dependencies -DoutputDirectory=lib
fi

# interop.sh starts the broker and runs the suite it is given; point it at
# the HornetQ classpath through a wrapper.
export CONFIG="$ROOT/tests/interop/hornetq/broker.xml" SUITE_CP="$(native "$LIB/lib")/*"
# Netty 4.0 predates Java modules: open what it reaches into.
export SUITE_JAVA_OPTS="-Dio.netty.noUnsafe=true --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED"
export SUITE_FILE="$ROOT/tests/interop/hornetq/HornetQ.java"
PORT="${PORT:-61677}" NAME="${NAME:-saranga-hornetq}" bash "$ROOT/scripts/interop.sh" "$TARGET"
