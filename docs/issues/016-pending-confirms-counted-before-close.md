# A publish can register between counting pending confirms and closing

- **Priority:** P2
- **Layer:** low-level
- **Status:** fixed
- **Found:** code review of `atomic-publish`, 2026-10-09
- **Confidence:** Reproduced in a unit test

## Where

- `src/client/channel.rs` (`dispatch_loop`, `ChannelClose`)

## Problem

On `ChannelClose`, dispatch counted the pending confirms under the
`confirms_state` lock, released it, and only then set `closed`. A confirmed
publish could pass `check_open` and register in that gap, so
`pending_confirms_at_close` (behind the "certain only if it alone was pending"
rule) was wrong. Related to [013](013-confirm-channel-close-misattributed.md).

## Failure scenario

Publish A to a missing exchange; the broker closes the channel and dispatch
counts 1. Publish B registers before `closed` is set. Both are dropped with
the count at 1, so B is reported as having caused A's 404.

## Fix

Fixed in `wlonusmm` (low-level: count pending confirms and close the channel
atomically): `closed` is set under the same lock, which publishers hold from
`check_open` until their confirm is registered. Tested by
`pending_confirms_at_close_counts_every_accepted_publish`.
