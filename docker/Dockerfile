# The Saranga image, laid out as Artemis's (artemis-docker): the home in
# /opt/saranga, a broker instance made on the first start in the
# /var/lib/artemis-instance volume from the same variables, its files in
# etc-override copied over the instance's etc, and the same ports.
#
#   docker build -f docker/Dockerfile -t saranga .
#   docker run -p 8161:8161 -p 61616:61616 saranga
#
# The binary serves Saranga's own console at /saranga/ when console/build
# (npm run build in console/) was there to embed; the release builds it
# first and passes SARANGA_CONSOLE_REQUIRED=1. Artemis's console is not
# shipped: mount its console.war at /opt/saranga/web/console.war to have it.

FROM rust:1.98.1-slim-trixie AS build
ARG SARANGA_CONSOLE_REQUIRED
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p saranga-cli

FROM debian:trixie-slim
LABEL org.opencontainers.image.title="Saranga" \
      org.opencontainers.image.description="A message broker inspired by Apache ActiveMQ Artemis 2.57.0" \
      org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /opt

ENV ARTEMIS_USER=artemis
ENV ARTEMIS_PASSWORD=artemis
ENV ANONYMOUS_LOGIN=false
ENV EXTRA_ARGS="--http-host 0.0.0.0 --relax-jolokia"

# The user and group Artemis's image runs as.
RUN groupadd -g 1001 -r artemis && useradd -r -u 1001 -g artemis artemis

COPY --from=build /src/target/release/saranga /opt/saranga/bin/saranga
COPY LICENSE NOTICE README.md CHANGELOG.md COMPAT.md /opt/saranga/
RUN mkdir /opt/saranga/web

EXPOSE \
# Port for HTTP
    8161 \
# Port for CORE,MQTT,AMQP,HORNETQ,STOMP,OPENWIRE
    61616 \
# Port for legacy HORNETQ,STOMP clients
    5445 \
# Port for AMQP
    5672 \
# Port for MQTT
    1883 \
# Port for STOMP
    61613

RUN mkdir /var/lib/artemis-instance && chown -R artemis:artemis /var/lib/artemis-instance

COPY docker/docker-run.sh /

USER artemis

VOLUME ["/var/lib/artemis-instance"]
WORKDIR /var/lib/artemis-instance

ENTRYPOINT ["/docker-run.sh"]
CMD ["run"]
