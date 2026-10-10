# Compatibility target

| Item | Value |
|---|---|
| Reference implementation | Apache ActiveMQ Artemis |
| Pinned version | **2.57.0** (tag `2.57.0`, checked out in `reference/activemq-artemis`, git-ignored) |

Saranga aims for wire compatibility (Core, AMQP 1.0, MQTT 3.1.1/5, STOMP, OpenWire, WebSockets)
and on-disk compatibility (journal, bindings, paging, large messages) with the pinned version.
See [docs/design.md](docs/design.md) for the strategy.
