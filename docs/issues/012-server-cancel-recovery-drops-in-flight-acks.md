# Recovery after `ServerCancel` drops the old channel and its in-flight deliveries

- **Priority:** P3
- **Layer:** high-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #11)
- **Confidence:** From reading the code

## Where

- `src/high_level/consumer.rs:133`

## Problem

After a `ServerCancel` the consumer's old channel is still open, but
installing the recovered consumer replaces `_channel`, which closes the old
one. Every delivery the application still holds from it is requeued by the
broker, and its `ack` fails. The behaviour predates the `Stream` change, but
the README's `for_each_concurrent` example makes it likelier.

## Failure scenario

`for_each_concurrent(10, ...)`; a quorum-leader failover sends
`Basic.Cancel` → recovery installs the new consumer → the old channel closes →
up to 10 in-flight handlers' `d.ack()` return `ChannelClosed("channel
dropped")` and those messages are redelivered, so they are processed twice.

## Possible fix

After `ServerCancel` (not `ChannelClosed`) the channel is fine: consume
again on it instead of opening a new one, or keep the old channel alive until
its outstanding deliveries are settled or dropped. Delivery is at-least-once
either way (ADR-0002), so this reduces duplicates rather than fixing a
guarantee.
