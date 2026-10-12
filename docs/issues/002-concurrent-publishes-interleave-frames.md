# Concurrent publishes on one channel interleave their frames

- **Priority:** P1
- **Layer:** low-level
- **Status:** fixed
- **Found:** code review, 2026-10-09 (finding #1)
- **Confidence:** Confirmed: reproduced against a local broker

## Where

- `src/client/channel.rs:649` (`basic_publish`)
- `src/client/channel.rs:887-899` (`basic_publish_confirmed`)

## Problem

A publish is a Method, a Header and one or more Body frames, sent as
separate `send_frame` calls onto the connection's frame queue with nothing
holding them together. AMQP requires a channel's content frames to follow
their method frame without anything in between on that channel.

The high-level `Client` shares one channel (`session.channel()`) and one
confirm channel between all tasks, so any concurrent use hits this.

## Failure scenario

Multi-thread runtime, 8 tasks calling `Client::publish` → within a few
hundred messages the broker closes the channel with `505 UNEXPECTED_FRAME`.
`Client::publish_confirmed` from 8 tasks failed 8 out of 8. An RPC, ack or
`FlowOk` landing between one publish's frames has the same effect.

## Possible fix

Send a publish's frames as one unit: e.g. a single `OutgoingFrame` variant
carrying method + header + bodies, or a per-channel publish lock held across
the sends (it is not held across an `.await`, so a std mutex works). Fix
together with [003](003-confirm-tags-can-be-misattributed.md), which needs the
same critical section.

## Fix

Fixed in `vpqwoqtv` (low-level: a publish is one unit in the frame queue): a
publish's frames are queued as one item. Tested by
`concurrent_publishes_do_not_interleave_their_frames`.
