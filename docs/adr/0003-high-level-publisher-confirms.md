---
status: accepted
---

# High-level publisher confirms

The high-level `Client` gets `publish_confirmed` and
`publish_confirmed_with_props`, which return once the broker has confirmed the
message. `publish` stays fire-and-forget. Each AMQP session has a channel of
its own in confirm mode, opened on first use and replaced once the broker
closes it. The confirm wait is bounded by `rpc_timeout`. Message properties
are the caller's: confirmed publishes are not made persistent.

A confirmed publish ends in one of four ways:

| Outcome | Meaning | Broker has the message? |
|---|---|---|
| `Ok(())` | Acked | Yes |
| `Err(PublishNacked)` | Nacked, e.g. full queue with `reject-publish` | No |
| `Err(ChannelClosed)` | The broker closed the confirm channel, e.g. 404 for a missing exchange | No |
| `Err(ConfirmUncertain)` | Sent, but the connection was lost or `rpc_timeout` passed before the confirm | Unknown |

With recovery on (ADR-0002), a confirmed publish that could not be sent
because the connection was already gone waits for the recovered session
(within `RecoveryConfig::timeout`) and is sent once more there. A send that
fails part-way never leaves a routable message: the broker routes only
complete messages. A publish that was sent is never sent again by the client.

## Considered options

- **Confirmed by default, as in amqp-client.go.** Rejected for now: every
  sequential `publish` would cost a round trip, a throughput surprise for
  existing callers. Explicit `publish_confirmed` keeps both available.
- **A confirm handle the caller awaits later (pipelining in one task).**
  Rejected for now as more API surface; concurrent callers already pipeline
  on the shared confirm channel.
- **Republish after recovery when the confirm is lost.** Rejected: it turns an
  unknown outcome into silent duplicates. `ConfirmUncertain` hands the choice
  to the caller.
- **Confirm mode on the client's shared channel.** Rejected: confirm mode
  cannot be turned off, and it would change every other publish on that
  channel.

## Consequences

- Callers must handle `ConfirmUncertain`, e.g. by republishing with
  idempotent consumers, or by recording the message for later.
- One bad confirmed publish (e.g. to a missing exchange) closes the shared
  confirm channel; other confirmed publishes in flight on it fail with the
  same `ChannelClosed`.
- Mandatory publishes and returns are not part of the high-level API.
