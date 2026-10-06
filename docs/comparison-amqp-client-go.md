# amqp-client.rs vs amqp-client.go

Comparison with [cloudamqp/amqp-client.go](https://github.com/cloudamqp/amqp-client.go)
(cloned 2026-10-06). Both are AMQP 0-9-1 clients with the same two-layer
shape: a protocol-mapped low-level API and an opinionated high-level `Client`.

## Usage at a glance

Low-level usage is near-identical; high-level usage diverges.

| | amqp-client.rs | amqp-client.go |
|---|---|---|
| Connect (low) | `Connection::connect(uri)` / `connect_with_config(Config)` | `amqp.Dial(ctx, uri, *Config)` |
| Channel | `conn.open_channel()` | `conn.Channel(ctx)` |
| Declare | `ch.queue_declare(name, opts)` | `ch.QueueDeclare(ctx, name, opts)` |
| Consume (low) | `ch.basic_consume(q, tag, opts)` → `consumer.next()` | `ch.BasicConsume(ctx, q, opts)` → `consumer.Next(ctx)` |
| Ack | `msg.ack()` | `d.Ack()` |
| Confirms (low) | `confirm_select()` + `basic_publish_confirmed` | `BasicPublishConfirm` → `conf.Wait(ctx)`, `WaitForConfirms` |
| Returns | `ch.returns()` stream | `ch.OnReturn(callback)` |
| Fluent wrappers | `ch.queue(name)` / `ch.exchange(name)` (low level); high level `client.queue(..)`, `client.topic(..)` etc. planned (ADR-0002) | `c.Queue(...)`, `c.TopicExchange(...)` (high level, owned by `Client`) |
| High-level build | `Client::builder(uri)?.codec(..).setup(..).build()` | `amqp.NewClient(ctx, uri, &ClientOptions{...})` |
| High-level consume | `client.consume::<T>(q, tag, opts)` → pull `next()` | `q.Subscribe(ctx, handler, opts)` → push handler |

Cancellation/timeouts: Go passes a `context.Context` to every blocking call;
amqp-client.rs uses config-wide `connect_timeout` / `rpc_timeout` (callers can wrap in
`tokio::time::timeout`).

## Feature differences

### High-level Client

| Feature | amqp-client.rs | amqp-client.go |
|---|---|---|
| Automatic recovery | Not implemented (ADR-0002 accepted) | Yes, always on |
| Recovery trigger | Operation-driven (ADR-0002) | Background supervisor reconnects as soon as `conn.Done()` fires |
| Retry wait | Fixed `RecoveryConfig::timeout` (250ms), no backoff | Exponential: 1s doubling to 30s; first retry immediate |
| Give up | Never | `MaxRetries` (0 = never), then `OnFailed` |
| Topology restore | Recorded topology, as in Go (ADR-0002); `setup` closure removed | Declarative: records queues/exchanges/bindings declared via `Client` and redeclares them; server-named queues get a new name, `Queue.Name()` updates |
| Channel-close recovery | Consumers recover after `ChannelClosed` and `ServerCancel` (ADR-0002) | Subscriptions re-consume after channel close too (e.g. consumer timeout) |
| Waits during recovery | Bounded by `RecoveryConfig::timeout`, then `ConnectionClosed` | Bounded by caller's `ctx` |
| Lifecycle hooks | None | `OnConnect`, `OnDisconnect`, `OnFailed`, `OnReturn` |
| Publish | Fire-and-forget, transient by default | Persistent and confirmed by default; pipelined confirms; retries transparently if channel closed before send |
| Mandatory / returns | Only low level | `Message.Mandatory` + `OnReturn` |
| Consume model | Pull: `TypedConsumer::next()`, caller acks | Push: handler; acks on `nil`, requeues on error/panic; `Workers` for concurrency; default prefetch 20 |
| Decode failure | Auto-nack (no requeue), skipped | Handler calls `d.Decode`, decides itself |
| Codecs | One `Codec` per client, compile-time typed (serde); bring your own (only `ByteCodec` built in) | Registry by `content-type` + `content-encoding`; built-in JSON, text, gzip, deflate; dynamic (`any`) |
| RPC | None | `RPCServer`, `RPCCall`, `RPCClient` over direct reply-to |
| Basic get | Only low level | `Queue.Get` |
| Escape hatch | `client.channel()` (session-bound) | `c.Connection(ctx)` |

### Low-level / connection

| Feature | amqp-client.rs | amqp-client.go |
|---|---|---|
| Protocol coverage | Queue, exchange, basic, confirm, tx, qos, recover | Same, plus `Channel.Flow` (client-initiated) |
| OAuth token refresh | No | `Connection.UpdateSecret` |
| Connection name / client properties | Fixed | `ConnectionName`, `ClientProperties` |
| Blocked/unblocked | Logged only | `OnBlocked` / `OnUnblocked` callbacks |
| Custom dialer (proxy, UNIX socket) | No | `Config.Dial` |
| TLS | rustls + webpki roots, not configurable | Full `*tls.Config` |
| Logger | `tracing` | `*slog.Logger` |
| Publish backpressure | `publish_admission_bytes` semaphore | TCP backpressure via buffered writer |
| Consumer buffering | Bounded mpsc (4096) per consumer | Per-consumer buffer; "always set a prefetch" |
| Lifecycle observation | `Consumer::cancellation()` | `Done()` / `Err()` on connection, channel, consumer, subscription |
| Errors | `Error` enum (`ConnectionClosed`, `ChannelClosed`, …) | `*amqp.Error` with code/class/method + `ErrClosed` sentinel, `IsCode` |
| Dependencies | tokio, rustls, serde, … | None (stdlib only) |
| Benchmarks published | `examples/benchmark.rs`, no published numbers | README table vs amqp091-go |

## Where amqp-client.rs's accepted design diverges from amqp-client.go

These are deliberate in ADR-0002 but worth re-checking given that the CloudAMQP
sibling libraries (Go, Ruby, Crystal, JS) share one high-level API:

1. **Operation-driven vs supervisor-driven recovery.** Go reconnects immediately
   in the background; amqp-client.rs waits for an operation to notice.
2. **Fixed interval vs exponential backoff**, and no `MaxRetries` in amqp-client.rs.
3. **Publish confirms.** Go's high-level publish is confirmed + persistent by
   default; amqp-client.rs defers this (`PUBLISH_CONFIRM_ISSUE.md`).
4. **Wait budget.** Go uses the caller's context; amqp-client.rs a single global
   `RecoveryConfig::timeout`.
5. **No lifecycle hooks** (`OnConnect`/`OnDisconnect`/`OnFailed`) in amqp-client.rs.
