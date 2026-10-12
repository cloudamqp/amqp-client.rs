# Confirm delivery tags can be misattributed under concurrency

- **Priority:** P1
- **Layer:** low-level
- **Status:** fixed
- **Found:** code review, 2026-10-09 (finding #2)
- **Confidence:** From reading the code

## Where

- `src/client/channel.rs:872`

## Problem

`basic_publish_confirmed` takes the next delivery tag under the
`confirms_state` lock, releases it, and only then sends the frames. The broker
numbers publishes in the order it receives them, so two concurrent confirmed
publishes can be numbered in the opposite order to their client-side tags.

## Failure scenario

Task A takes tag 1, task B takes tag 2, B's frames are enqueued first → the
broker numbers B as 1 and A as 2 → a nack meant for B resolves A's
`PublishConfirm`: A is told `Acked` although the broker rejected it, or the
other way round. Publisher-confirm guarantees break.

## Possible fix

Assign the tag and enqueue the frames in one critical section, the same one
that fixes [002](002-concurrent-publishes-interleave-frames.md).

## Fix

Fixed in `vpqwoqtv` (low-level: a publish is one unit in the frame queue): the
tag is taken and the frames are queued under one `confirms_state` lock. Tested
by `confirmed_publishes_are_queued_in_tag_order`.
