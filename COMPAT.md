# Compatibility target

| Item | Value |
|---|---|
| Reference implementation | Apache ActiveMQ Artemis |
| Pinned version | **2.57.0** (tag `2.57.0`, checked out in `reference/activemq-artemis`, git-ignored) |

Saranga aims for wire compatibility (Core, AMQP 1.0, MQTT 3.1.1/5, STOMP, OpenWire, WebSockets)
and on-disk compatibility (journal, bindings, paging, large messages) with the pinned version.
See [docs/design.md](docs/design.md) for the strategy.

## Why we build the reference from source
Artemis 2.45.0 to 2.57.0 are tagged in git, but no distribution, server jars or Docker images
were published for them. Maven Central has POMs only, and the newest published release is 2.44.0.
`scripts/build-artemis.sh` therefore builds 2.57.0 from the tag and packages it with Artemis's own
`artemis-docker` Dockerfile as `saranga/artemis-ref:2.57.0`. Clone the reference with
`core.autocrlf=false` on Windows, because CRLF line endings break the container entrypoint.
