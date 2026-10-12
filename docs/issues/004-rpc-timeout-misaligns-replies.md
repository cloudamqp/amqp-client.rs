# An RPC timeout misaligns every later reply on the channel

- **Priority:** P2
- **Layer:** low-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #3)
- **Confidence:** From reading the code

## Where

- `src/client/channel.rs:1158`

## Problem

On timeout, `rpc_call` takes the in-flight slot, dropping the expectation
and its `rpc_lock` guard, while the broker's reply is still outstanding. The
next RPC registers its own expectation, and the late reply resolves it.
Replies stay off by one from then on.

## Failure scenario

A slow or blocked broker makes `queue_declare("")` pass `rpc_timeout` → the
next caller's `queue_declare("")` gets the first call's `DeclareOk` (a
different server-named queue, which the high-level client then records), and
a following `exchange_declare` gets `UnexpectedMethod`. The shared session
channel stays misaligned for good.

## Possible fix

After a timeout the channel cannot be trusted: close it (or mark it
unusable so the high-level client opens a new one), rather than handing the
lock to the next caller. Alternatively keep the expectation registered as an
orphan so the late reply is consumed, but then a reply that never comes blocks
the channel forever.
