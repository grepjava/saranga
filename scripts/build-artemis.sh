#!/usr/bin/env bash
# Build the pinned Artemis reference broker from source and package it as a
# Docker image (saranga/artemis-ref:<version>). Artemis 2.45+ is tagged but not
# published, so we build it ourselves. Needs Docker; no local JDK or Maven.
set -euo pipefail

VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/reference/activemq-artemis"
IMAGE="saranga/artemis-ref:$VERSION"
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting container paths

if [[ ! -d "$SRC" ]]; then
  git clone -c core.autocrlf=false --depth 1 --branch "$VERSION" https://github.com/apache/activemq-artemis.git "$SRC"
fi

# Docker on Windows wants a native path for bind mounts.
HOST_SRC="$SRC"
command -v cygpath >/dev/null && HOST_SRC="$(cygpath -m "$SRC")"

DIST="$SRC/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION"
if [[ ! -d "$DIST" ]]; then
  # JDK 24 or later, as the image runs on JDK 25: the jars' Java 24 classes
  # (META-INF/versions/24, such as SecurityManagerShim on Subject.current())
  # are only built then, and without them the audit log fails on JDK 25.
  docker run --rm \
    -v "$HOST_SRC:/src" -v saranga-m2:/root/.m2 -w /src \
    maven:3.9-eclipse-temurin-25 \
    mvn -B -ntp install -pl artemis-distribution -am \
      -DskipTests -Dcheckstyle.skip -Drat.skip -Denforcer.skip -Dmaven.javadoc.skip
fi

cd "$SRC/artemis-docker"

./prepare-docker.sh --from-local-dist --local-dist-path "$DIST"
cd "$DIST"   # prepare-docker.sh drops docker/ into the dist dir
docker build -q -f docker/Dockerfile-ubuntu-25-jre -t "$IMAGE" .
echo "built $IMAGE"
