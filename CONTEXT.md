# amqp-client.rs

amqp-client.rs is an AMQP 0-9-1 client library for Rust. Its language distinguishes protocol-level AMQP sessions from higher-level client behavior built on top of those sessions.

## Language

**Automatic recovery**:
A high-level, opt-in client behavior that creates a fresh AMQP session after a connection failure and restores configured client behavior on that new session.
_Avoid_: Auto-reconnect, transparent reconnect

**Operation-driven recovery**:
Automatic recovery that starts when a high-level operation observes AMQP session loss, rather than from a proactive background monitor.
_Avoid_: Active recovery monitor, health-check reconnect

**Recovery timeout**:
The duration automatic recovery uses both as the fixed wait between failed recovery attempts and as the short wait for new high-level operations while recovery is in progress.
_Avoid_: Recovery interval, operation timeout, backoff

**Session generation**:
A monotonic counter identifying the current AMQP session; an operation that observes session loss on a stale generation knows recovery is needed or already done.
_Avoid_: Connection id, epoch, version

**Recovery task**:
The single task owned by the `Client` that runs the retry loop (connect, declare the recorded topology, bump the session generation) until it succeeds or recovery demand ends.
_Avoid_: Reconnect loop, background monitor

**Recovery waiter**:
A high-level operation parked until the recovery task produces a new session generation; transient operations wait at most the recovery timeout, recoverable consumers wait indefinitely.
_Avoid_: Blocked caller, queued operation

**Recovery demand**:
The condition that keeps the recovery task running: at least one live recoverable consumer or at least one recovery waiter. When demand reaches zero the task stands down.
_Avoid_: Keepalive, active client

**Recovery registry**:
The `Client`'s count of live recoverable consumers, part of recovery demand; a consumer leaves it when dropped. Each consumer consumes again on the new AMQP session itself, on its next poll.
_Avoid_: Subscription list, consumer cache

**AMQP session**:
A single live connection and its channels, delivery tags, consumers, publisher confirms, and other broker-scoped state.
_Avoid_: Socket, reconnectable connection

**AMQP session loss**:
The termination of an AMQP session because the transport or connection-level protocol state is no longer usable.
_Avoid_: Channel error, consumer cancellation, setup failure

**Recoverable consumer**:
A high-level consumer whose stream remains valid across automatic recovery, waits while recovery is in progress, and continues receiving messages after the client establishes a new AMQP session. It also recovers on its own after its channel closes or the broker cancels it, by declaring its queue and bindings again and consuming again.
_Avoid_: Persistent consumer, transparent consumer

**Recorded topology**:
The exchanges, queues and bindings declared through the high-level `Client`, which it remembers and declares again on every new AMQP session and before a recoverable consumer consumes again. Unbinding or deleting through the client removes them.
_Avoid_: Setup hook, idempotent topology setup, topology cache

**Consumer-level recovery**:
Recovery of a single recoverable consumer after `ChannelClosed` or `ServerCancel` while the AMQP session is still alive; it runs in the consumer itself and does not involve the recovery task.
_Avoid_: Channel recovery, resubscribe

**Session-bound handle**:
A low-level AMQP handle whose validity ends with the AMQP session that created it and is not automatically restored by automatic recovery.
_Avoid_: Recoverable channel, channel proxy

**Session-bound delivery**:
A delivered message and its delivery tag are valid only in the AMQP session that produced them; acknowledgements are not replayed by automatic recovery.
_Avoid_: Recoverable delivery, ack replay

**Broker-assigned consumer tag**:
A consumer identifier chosen by the broker when the caller does not provide one; it may differ between AMQP sessions and is less predictable when used with automatic recovery.
_Avoid_: Stable consumer identity

**At-least-once delivery**:
A delivery guarantee where a message may be delivered again if acknowledgement state is lost with an AMQP session.
_Avoid_: Exactly-once delivery, client-side deduplication
