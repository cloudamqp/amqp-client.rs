# A `frame_max` of 8 or less makes a publish loop forever

- **Priority:** P2
- **Layer:** low-level
- **Status:** fixed
- **Found:** code review of `atomic-publish`, 2026-10-09
- **Confidence:** From reading the code

## Where

- `src/client/connection.rs` (`frame_max` negotiation)
- `src/client/channel.rs` (body chunking)

## Problem

The body chunk size is `frame_max - 8`, saturating. With `0 < frame_max <= 8`
it is 0, so chunking never advances. Negotiation took the minimum of the two
proposals without enforcing the spec's 4096 floor.

## Failure scenario

`Config.frame_max = 8`, then any publish with a non-empty body: empty body
frames are produced until the process runs out of memory.

## Fix

Fixed in `yvvsnpty` (low-level: raise frame_max to the spec minimum):
`negotiate_frame_max` raises any non-zero value to 4096. Unit-tested. Body
chunking (now in `Content`) also never uses a chunk size below 1.
