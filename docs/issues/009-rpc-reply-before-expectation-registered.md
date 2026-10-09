# An RPC reply can arrive before its expectation is registered

- **Priority:** P2
- **Layer:** low-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #8)
- **Confidence:** From reading the code

## Where

- `src/client/channel.rs:1127`

## Problem

`rpc_call` sends the request with `send_method` and only then fills the
in-flight slot. On a multi-thread runtime, dispatch can process the reply, or
connection-loss handling can run `resolve_inflight`, in that window, find the
slot empty, and drop the reply as an orphan.

## Failure scenario

The calling thread is pre-empted between `send_method` and filling the slot
→ the reply is dropped → the caller waits the full 30 s `rpc_timeout` and
gets `Timeout`. `with_session` does not retry `Timeout`.

## Possible fix

Register the expectation first, then send; on a failed send, take it back
out. The existing `check_open` after registration then still covers a channel
that closed in between.
