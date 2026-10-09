# The per-channel incoming frame queue is unbounded

- **Priority:** P2
- **Layer:** low-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #9)
- **Confidence:** From reading the code

## Where

- `src/client/connection.rs:523`

## Problem

The reader hands frames to each channel's dispatch task over an unbounded
mpsc. The consumer buffer after dispatch is bounded (4096), but when it is
full, dispatch blocks while the reader keeps queueing frames, so nothing slows
the socket down.

## Failure scenario

A `no_ack` (or large-prefetch) consumer that reads slowly → memory grows
without limit. Replies and `Channel.Close` for that channel also queue behind
the deliveries.

## Possible fix

Bound the queue, accepting that a full channel then stalls the reader for
the whole connection (the usual AMQP trade-off), or rely on prefetch and
document that `no_ack` without a prefetch is unbounded.
