# A large publish is encoded whole before the writer flushes

- **Priority:** P2
- **Layer:** low-level
- **Status:** fixed
- **Found:** code review of `atomic-publish`, 2026-10-09
- **Confidence:** Reproduced in a unit test

## Where

- `src/client/connection.rs` (`writer_loop`)

## Problem

Once a publish was queued as one item, the writer checked its 64 KiB soft
flush cap only after the item's last frame. A large body was copied into
`write_buf` in full before any write, and `BytesMut::clear` keeps that
capacity for the connection's lifetime.

## Failure scenario

Publishing one 200 MB message: ~200 MB is encoded before the first write,
doubling peak memory, and `write_buf` stays ~200 MB for as long as the
connection lives.

## Fix

Fixed in `vpqwoqtv` (low-level: a publish is one unit in the frame queue),
which introduced it: `flush_if_full` runs after every frame; the publish's
permit is still released only after its last frame is written. Tested by
`writer_flushes_a_large_publish_in_batch_sized_writes`.
