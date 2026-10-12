# A plain publish on a confirm channel shifts every later confirm tag

- **Priority:** P2
- **Layer:** low-level
- **Status:** partly fixed
- **Found:** code review, 2026-10-09 (finding #5)
- **Confidence:** From reading the code

## Where

- `src/client/channel.rs:626` (`basic_publish`)
- `src/client/channel.rs:820-826` (`confirm_select` docs)

## Problem

In confirm mode the broker counts every publish on the channel. The plain
`basic_publish` does not advance `confirms_state.next_seq`, although the
`confirm_select` docs say it "continues to work" on a confirm-mode channel.

## Failure scenario

`confirm_select`, then `basic_publish`, then `basic_publish_confirmed` × N →
the broker's ack for tag 1 (the plain publish) resolves the first confirmed
publish before it is actually confirmed, and the last `PublishConfirm` only
resolves when the channel closes, as an error.

## Possible fix

Have `basic_publish` take a sequence number (and discard its confirm) when
confirms are enabled, or reject plain publishes on a confirm channel and fix
the docs. Belongs in the same critical section as
[002](002-concurrent-publishes-interleave-frames.md).

## Fix

Partly fixed in `vpqwoqtv` (low-level: a publish is one unit in the frame
queue): a plain publish on a confirm channel now advances `next_seq`. Tested by
`a_plain_publish_on_a_confirm_channel_takes_a_tag`.

Still open: `confirm_select` sets `next_seq = 1` after `SelectOk`, but the
broker never resets its counter, and it counts publishes queued after
`Confirm.Select` even before `SelectOk` arrives. Calling `confirm_select`
twice after N publishes, or a plain publish from another task between sending
`Confirm.Select` and handling `SelectOk`, leaves every later tag off.
