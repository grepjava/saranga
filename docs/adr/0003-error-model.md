# ADR 0003: Error model

Status: accepted

## Context
Artemis signals failures to clients through `ActiveMQException` types, and each has a numeric
`ActiveMQExceptionType` code carried in Core `ActiveMQExceptionMessage` packets. Clients
branch on those codes, for example failover on `DISCONNECTED` and duplicate handling on
`DUPLICATE_ID_REJECTED`.

## Decision
- Library crates use `thiserror` enums. Binaries use `anyhow` only at the top level.
- `saranga-core-protocol` defines `ErrorType`, which mirrors `ActiveMQExceptionType` with the
  same numeric codes. Every server error that can reach a client maps to one. The mapping is
  exhaustive and tested.
- Protocol adapters translate `ErrorType` into their own error vocabulary (AMQP conditions, MQTT
  reason codes, STOMP ERROR frames, OpenWire `ExceptionResponse`), following the Artemis
  protocol managers.
- Storage I/O errors are fatal to the journal (as Artemis's `IOCriticalErrorListener` is):
  the broker stops accepting work instead of continuing with an unknown on-disk state.
- `tracing` provides logging. Log message IDs (e.g. `AMQ222xxx`) are kept on equivalent events
  so existing log alerting keeps working.

## Consequences
- Client behaviour on error matches Artemis without client changes.
- New error paths require choosing an Artemis code. This is deliberate.
