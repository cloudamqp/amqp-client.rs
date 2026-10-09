# A confirm channel closed by one publish fails a concurrent one with its error

- **Priority:** P3
- **Layer:** high-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #13)
- **Confidence:** From reading the code

## Where

- `src/high_level/client.rs:213`

## Problem

When the broker closes the shared confirm channel because of one publish
(e.g. 404 for a missing exchange), a concurrent `publish_confirmed` that got
the same channel just before fails its `check_open` with that close. It is not
retried on a reopened confirm channel, and the docs say that error means this
publish caused the close.

## Failure scenario

Task A publishes to a missing exchange; the broker closes the confirm
channel with 404. Task B's send then fails → B returns `ChannelClosed { 404
NOT_FOUND - no exchange 'a-exchange' }` for a publish to a valid exchange.

## Possible fix

A send that fails because the channel was already closed was never sent,
so it can be retried on a fresh confirm channel, as `with_session` does for
`ConnectionClosed`. Needs the confirm-channel reopen path to distinguish
"closed before my send" from "closed because of my send".
