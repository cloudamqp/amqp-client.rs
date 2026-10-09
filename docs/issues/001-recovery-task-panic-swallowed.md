# A panic in a consumer's recovery task is swallowed

- **Priority:** P0
- **Layer:** high-level
- **Status:** fixed
- **Found:** code review, 2026-10-09 (finding #12)
- **Confidence:** From reading the code

## Where

- `src/high_level/consumer.rs:128`

## Problem

`poll_delivery` maps the recovery task's `JoinHandle` result with
`recovered.ok().flatten()`, so a `JoinError` (the task panicked) is treated
like a normal "could not recover". The consumer ends with `None` and nothing
reports the panic. Before recovery moved into a spawned task, a panic in it
propagated to the caller of `next()`.

Polling again after that end calls `start_recovery` again, which spawns a
fresh recovery even though `end()` already dropped the consumer's
`ConsumerRegistration`. That recovery no longer counts as recovery demand, so
the client's recovery task may stand down while it waits.

## Failure scenario

Something in `recover` panics (e.g. a poisoned topology mutex) → `next()`
returns `None`, the panic is lost, and a later poll starts an unregistered
recovery.

## Possible fix

Re-raise a panic with `std::panic::resume_unwind(e.into_panic())` when
`e.is_panic()`; keep `None` for cancellation. Also consider making the
consumer stay ended once it has ended (a fused stream), so a later poll does
not start recovery without a registration.

## Fix

Fixed in `xvrxkzwz` (high-level: the consumer is a Stream): `recovery_result`
re-raises the task's panic with `resume_unwind`; an aborted task still ends
the consumer with `None`. Unit-tested.

Not done: making the consumer fused. Once the panic is re-raised, a later poll
is only possible if the caller catches it, so a recovery without a
registration now needs that.
