# ADR 0001: Runtime model

Status: accepted

## Context
Artemis runs on Netty event loops and serialises per-queue work through `OrderedExecutor`s,
with fine-grained locking around queue internals. We need the same ordering guarantees
(per-queue FIFO processing, per-session ordering) without porting the lock structure.

## Decision
- tokio multi-threaded runtime.
- One task per connection. The task owns the codec, the session state, and the producer and consumer handles.
- One task per queue (actor) with an mpsc mailbox. The queue task alone owns the message
  reference list, consumer list, delivery cursor and scheduled/redelivery timers.
  Routing, acks, consumer add/remove and management calls are messages to it.
- Messages are `Arc<Message>` carrying a lazily cached encoded buffer, shared between
  queues, persistence and every consumer.
- Global and per-address memory accounting uses atomics, and is checked on routing to drive
  the PAGE/BLOCK/FAIL/DROP policies.

## Consequences
- Delivery does not need locks, and ordering per queue follows from how the mailbox works.
- Cross-queue operations (a transaction touching N queues) become N messages plus a
  completion barrier. Transactions are coordinated by the session task.
- Mailboxes must be bounded and apply backpressure to producers. This mirrors producer
  credits.
