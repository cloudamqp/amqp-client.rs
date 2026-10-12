# Deliveries right behind `ConsumeOk` can be dropped

- **Priority:** P2
- **Layer:** low-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #7)
- **Confidence:** Plausible: did not reproduce in 40 runs

## Where

- `src/client/channel.rs:699`

## Problem

`basic_consume` inserts the consumer into `consumer_senders` only after the
awaiting task wakes from `ConsumeOk`. The dispatch loop can process
`Basic.Deliver` frames that arrive right behind `ConsumeOk` before that
happens, and drops them as "message for unknown consumer".

## Failure scenario

A queue already holding messages; `ConsumeOk` and the first `Deliver`
frames arrive in one read → dispatch resolves `ConsumeOk` and handles the
deliveries before the subscriber task runs → they are dropped. With acks they
stay unacked, holding prefetch until the channel closes; with `no_ack` they are
lost.

## Possible fix

Register the consumer in dispatch when it handles `ConsumeOk` (the RPC
outcome can carry the receiver back), or pre-register under the requested tag
when one is given. A broker-assigned tag is only known from `ConsumeOk`, so
registering in dispatch is the general fix.
