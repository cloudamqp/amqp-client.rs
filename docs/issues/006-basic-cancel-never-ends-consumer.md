# `basic_cancel` never ends the `Consumer`

- **Priority:** P2
- **Layer:** low-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #4)
- **Confidence:** Confirmed: reproduced against a local broker

## Where

- `src/client/channel.rs:727`

## Problem

`basic_cancel` removes the consumer from the shared `consumer_senders` map
only. The dispatch loop's `cached_consumers` keeps a clone of the `Sender`, so
the `Consumer`'s receiver never closes. No `CancellationReason` is set
either.

## Failure scenario

Consume, receive one message (which caches the entry), call
`basic_cancel(tag)` → `Consumer::next()` hangs forever instead of returning
`None`. Confirmed: still pending after 2 s.

## Possible fix

Let dispatch handle `CancelOk` and remove the entry from both maps there,
or give the cache a way to be invalidated. Set a cancellation reason (a
client cancel may need its own variant).
