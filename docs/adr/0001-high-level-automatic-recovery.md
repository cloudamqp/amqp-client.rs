---
status: superseded
superseded_by: 0002-recorded-topology-and-recovery
---

# High-level automatic recovery

Automatic recovery is an opt-in behavior on the high-level `Client` that, after
AMQP session loss, establishes a fresh session and restores configured client
behavior on it. It is **operation-driven**: recovery is triggered when a
high-level operation observes session loss, never by a proactive health-check
monitor. Concurrent observers coordinate through a single shared session slot
carrying a monotonic **generation**; the first operation to observe a stale
generation claims recovery and spawns a single `Client`-owned recovery task that
owns the forever-retry loop (connect → setup → restore consumers → bump
generation, else wait `RecoveryConfig::timeout` and retry). The triggering
operation does not run that loop — it is demoted to an ordinary waiter. The task
is **demand-driven**: it runs only while there is demand (a live recoverable
consumer in the registry, or an operation currently parked waiting) and stands
down when demand reaches zero, leaving the next operation to re-trigger.

## Considered options

- **Low-level recovery (recover the `Connection`/`Channel`).** Rejected: AMQP
  handles, delivery tags, and consumer state are session-bound; a recovered
  socket cannot honor handles minted on the dead session. Recovery belongs where
  client behavior (codec, setup, recoverable consumers) is owned.
- **A background recovery monitor.** A task that proactively health-checks the
  connection and reconnects on its own. Rejected: it does work when nothing
  needs a session and turns liveness into a polling problem. Recovery should
  start from an observed failure, not a probe.
- **Inline recovery in the triggering operation.** The operation that hits
  `ConnectionClosed` runs the reconnect loop itself. Rejected: the loop retries
  forever, so a `publish` unlucky enough to be the first observer would block
  forever — contradicting the settled rule that a publish waits at most
  `RecoveryConfig::timeout` then fails. Spawning a dedicated task decouples the
  loop's lifetime from any one operation's latency budget.
- **Unconditional run-to-success while the `Client` lives.** Once triggered, the
  task reconnects until it succeeds regardless of who is waiting. Rejected: an
  idle, consumer-less client whose only `publish` already failed and returned
  would keep opening connections to a dead broker every `timeout`, forever, with
  no backoff — observable churn for a session nobody awaits. Demand-driven
  stand-down avoids this at the cost of tracking demand and paying one reconnect
  latency on the next operation.

## Consequences

- **Bounded waits for transient operations.** `publish` and new `consume()`
  calls wait up to `RecoveryConfig::timeout` for a usable session, then return
  `Error::ConnectionClosed { code: 0, .. }`. Publishes are not buffered.
- **Unbounded wait for recoverable consumers.** `TypedConsumer::next()` waits
  across recovery and resumes on the new session rather than ending; it counts
  as a recovery-triggering operation. `ServerCancel` and `ChannelClosed` remain
  terminal.
- **Session-bound handles and deliveries are not restored.** `Client::channel()`
  and any previously cloned `Arc<Channel>` are bound to the session that minted
  them. `ack`/`nack`/`reject` are never replayed; delivery is at-least-once.
- **Setup is idempotent topology setup.** It reruns after every recovered
  session and must succeed for a recovery attempt to complete; setup failure
  fails that attempt and retries after `timeout`.
- **Trigger boundaries.** Recovery fires only on AMQP session loss /
  `ConnectionClosed`, not on `ChannelClosed`, `ServerCancel`, setup errors,
  codec errors, or publish-confirm uncertainty.
- **Initial `build()` fails fast.** Recovery engages only after an initially
  successful session is later lost; the first connect has no built-in retry.
- **Idle publish-only clients go quiet.** With no consumers and no parked
  waiter, recovery stands down rather than reconnecting indefinitely.
- **`Client` drop is the hard stop.** It ends recovery ownership and drops the
  current session; recoverable consumers eventually return `None` and never keep
  the client alive.
- **High-level publisher confirms are out of scope** for this feature (see
  `PUBLISH_CONFIRM_ISSUE.md`); recovery does not invent confirm-retry semantics.
