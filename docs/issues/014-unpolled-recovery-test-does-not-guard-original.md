# The unpolled-recovery test does not guard against the original consumer code

- **Priority:** P3
- **Layer:** tests
- **Status:** fixed
- **Found:** code review, 2026-10-09 (finding #14)
- **Confidence:** Confirmed: the reviewer ran it against the pre-change code

## Where

- `tests/recovery.rs:461` (`an_unpolled_recovering_consumer_does_not_block_a_delete`)

## Problem

The test failed against the intermediate implementation that kept the
recovery future inside the consumer (the regression it was written for). It
also passes against the original `async fn next()` code, because a timed-out
`next()` dropped the inline recovery future and its lock. So it guards the
regression, but not the spawned-task behaviour as such: that recovery carries
on, and resumes rather than restarts, while the consumer is not polled.

## Failure scenario

Not a bug: a gap in coverage. Reverting to "recovery restarts on every
poll" would not make any test fail.

## Possible fix

Add a test that polls the consumer once as a `Stream` (starting recovery),
keeps it alive unpolled, and checks that recovery finished in the meantime:
e.g. a message published meanwhile is delivered on the first poll after,
without a new redeclare.

## Fix

Fixed in `xvrxkzwz` (high-level: the consumer is a Stream):
`a_consumer_recovers_while_not_polled` checks that the broker sees the
consumer again while it is not polled, and that the next poll resumes on it.
It fails if recovery is an inline future again.
