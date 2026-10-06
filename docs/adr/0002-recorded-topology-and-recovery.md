---
status: accepted
supersedes: 0001-high-level-automatic-recovery
---

# Recorded topology and automatic recovery

Automatic recovery is opt-in: `Client::builder(uri)?.recovery(RecoveryConfig::default())`
(`RecoveryConfig::timeout` defaults to 250 ms). Without it, session loss is
terminal, as in the low-level API.

The high-level `Client` remembers the exchanges, queues and bindings declared
through it (**recorded topology**). After AMQP session loss it establishes a
fresh session, declares the recorded topology again and restores recoverable
consumers. A recoverable consumer also recovers on its own when its channel
closes (`ChannelClosed`, e.g. consumer timeout) or the broker cancels it
(`ServerCancel`, e.g. its queue was deleted): it declares its queue and that
queue's bindings again and consumes again. The `setup` closure is removed.

Recovery is **operation-driven**: it starts when a high-level operation observes
session loss, never from a proactive health-check monitor. Concurrent observers
coordinate through a single shared session slot carrying a monotonic **session
generation**; the first operation to observe a stale generation claims recovery
and spawns a single `Client`-owned **recovery task** that owns the forever-retry
loop (connect → declare recorded topology → restore consumers → bump
generation, else wait `RecoveryConfig::timeout` and retry). The triggering
operation does not run that loop; it becomes an ordinary waiter. The task is
**demand-driven**: it runs only while there is demand (a live recoverable
consumer, or an operation currently parked waiting) and stands down when demand
reaches zero, leaving the next operation to re-trigger.

## API shape

```rust
let client = Client::builder(uri)?
    .codec(JsonCodec)
    .recovery(RecoveryConfig::default())
    .build()
    .await?;

let ex = client.topic("orders", ExchangeDeclareOpts::default()).await?;
// also: client.direct / fanout / headers, and
// client.exchange(name, ExchangeKind::Topic, opts) which they call
let q = client.queue("", QueueDeclareOpts { exclusive: true, ..Default::default() }).await?;
q.bind(&ex, "order.*").await?;
q.bind("amq.topic", "audit.#").await?; // by name: exchange not recorded
let mut c = q.subscribe::<Order>(SubscribeOpts { prefetch: 50, ..Default::default() }).await?;
c.tag(); // the consumer tag
q.name(); // current name; changes after recovery for a server-named queue
q.unbind(&ex, "order.*").await?;
q.delete(QueueDeleteOpts::default()).await?;
```

- `Exchange` and `Queue` are cheap, cloneable handles into the recorded topology.
  They belong to the `Client`, not to an AMQP session.
- `bind` and `unbind` take the exchange as an `&Exchange` or by name
  (`impl AsRef<str>`; `Exchange` implements it). Binding by name records the
  binding but not the exchange: the exchange is assumed to exist (e.g. `amq.*`
  or one declared elsewhere), and a missing one fails recovery like any other
  declaration failure.
- Declaring, binding and consuming record; `unbind` and `delete` forget.
  Deleting a recorded queue also forgets its bindings and ends its consumers
  (`next()` returns `None`), so a deliberate delete is not undone by recovery.
- Each recoverable consumer has its own channel and its own prefetch
  (`basic_qos`), so one consumer's `ChannelClosed` does not affect the others
  and QoS needs no setup hook.
- `SubscribeOpts` is a high-level type, separate from the low-level
  `ConsumeOpts`, which maps 1:1 to `basic.consume`. It has `prefetch` (default
  20), `consumer_tag` (`Option`), `no_ack`, `exclusive` and `arguments`; no
  `no_local`. A caller-set consumer tag is reused on every recovery; without
  one the broker assigns a tag, which may differ after recovery.
- Exchange-to-exchange bindings are part of the recovery order but get an API
  only when needed.
- `client.publish` keeps taking exchange and routing-key names.

## Recovery rules

- **Order.** Exchanges, then exchange-to-exchange bindings, then queues, then
  queue bindings, then consumers. Within each step, first-declared first.
- **Server-named queues** get a new name when declared again on a new session.
  In consumer-level recovery the session is alive, so a server-named queue
  keeps its name while it still exists (checked with a passive declare) and
  is declared anew only if the broker deleted it. `Queue::name()` and every
  recorded binding of that queue follow the new name.
- **Consumer-level recovery** (`ChannelClosed`, `ServerCancel` while the session
  is alive) runs inside the consumer's `next()`: open a channel, declare the
  queue and its bindings, set prefetch, consume. On failure it waits
  `RecoveryConfig::timeout` and retries. If it observes session loss, it becomes
  a recovery waiter on the session recovery above.
- **Declaration failures during recovery** (e.g. `PRECONDITION_FAILED`) fail
  that attempt, are logged at `warn`, and are retried after
  `RecoveryConfig::timeout`.

## Considered options

- **User `setup` closure re-run after every session (ADR-0001).** Rejected: a
  closure cannot tell a live consumer that its server-named queue has a new
  name, and redeclaring after a consumer-level failure means re-running all of
  setup on a session that is still alive. It also diverges from the CloudAMQP
  sibling clients (Go, Ruby, Crystal, JS), which record topology.
- **Recorded topology plus an on-connect hook.** Rejected for now: the
  remaining jobs of `setup` (QoS) move to per-consumer options; anything else
  belongs on the low-level channel.
- **`ChannelClosed` / `ServerCancel` terminal (ADR-0001).** Rejected: consumer
  timeouts and auto-delete queues disappearing are routine, and the Go client
  recovers from both.
- **Low-level recovery (recover the `Connection`/`Channel`).** Rejected: AMQP
  handles, delivery tags and consumer state are session-bound; a recovered
  socket cannot honour handles minted on the dead session.
- **A background recovery monitor.** Rejected: it does work when nothing needs
  a session and turns liveness into a polling problem.
- **Inline recovery in the triggering operation.** Rejected: the loop retries
  forever, so the first `publish` to observe session loss would block forever,
  contradicting the bounded wait for transient operations.
- **Unconditional run-to-success while the `Client` lives.** Rejected: an idle,
  consumer-less client would keep reconnecting to a dead broker every
  `timeout`, forever.

## Consequences

- **Only topology declared through the `Client` is recovered.** Anything
  declared through `Client::channel()` is session-bound and not restored.
- **Exclusive and auto-delete queues come back empty**, and a server-named one
  comes back under a new name. Messages in them are lost.
- **A queue deleted by someone else is recreated** while a recoverable consumer
  on it lives. Only deleting through the `Queue` handle, or dropping the
  consumer, stops that.
- **Bounded waits for transient operations.** `publish` and new `consume()`
  calls wait up to `RecoveryConfig::timeout` for a usable session, then return
  `Error::ConnectionClosed { code: 0, .. }`. Publishes are not buffered.
- **Unbounded wait for recoverable consumers.** `next()` waits across recovery
  and resumes; dropping the consumer, deleting its queue through the handle, or
  dropping the `Client` ends it.
- **Session-bound handles and deliveries are not restored.** `Client::channel()`
  and any cloned `Arc<Channel>` are bound to the session that minted them.
  `ack`/`nack`/`reject` are never replayed; delivery is at-least-once, so
  handlers must be idempotent.
- **Trigger boundaries.** Session recovery fires only on AMQP session loss.
  Consumer-level recovery fires on `ChannelClosed` and `ServerCancel` for that
  consumer. Codec errors and publish-confirm uncertainty trigger neither.
- **Initial `build()` fails fast.** Recovery engages only after an initially
  successful session is lost; the first connect has no built-in retry.
- **Idle publish-only clients go quiet.** With no consumers and no parked
  waiter, recovery stands down.
- **`Client` drop is the hard stop.** It ends recovery and drops the session;
  recoverable consumers eventually return `None` and never keep the client
  alive.
- **High-level publisher confirms are out of scope** (see
  `PUBLISH_CONFIRM_ISSUE.md`).
