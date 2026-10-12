# Dropping a `Consumer` never sends `basic.cancel`

- **Priority:** P2
- **Layer:** low-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #10)
- **Confidence:** From reading the code

## Where

- `src/client/channel.rs:1530`

## Problem

Dropping a low-level `Consumer` sends nothing. Dispatch only notices on the
next delivery, when the send fails; it then removes the entry and drops every
later delivery for that tag. The broker keeps delivering.

## Failure scenario

Drop a `Consumer` on a channel that stays open → with `no_ack` every later
message for it is silently lost; with acks they sit unacked and use up
prefetch until the channel closes.

## Possible fix

Send `basic.cancel` (no-wait) when dispatch first finds the receiver gone,
or from `Consumer`'s `Drop`. The high-level consumer owns its channel and
closes it on drop, so it is less affected.
