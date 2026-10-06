---
status: proposed
---

# Consumer returns decode failures

`TypedConsumer::next` hands every delivery to the caller, including those
that fail to decode:

```rust
pub async fn next(&mut self) -> Option<Result<TypedDelivery<T>, DecodeFailure>>;

#[must_use]
pub struct TypedDelivery<T> { pub body: T, /* metadata as today */ }

#[must_use]
pub struct DecodeFailure {
    pub error: Error,
    pub body: Vec<u8>,
    pub properties: BasicProperties,
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    // + ack / nack / reject, as on TypedDelivery
}
```

```rust
while let Some(r) = consumer.next().await {
    match r {
        Ok(d) => { handle(&d.body); d.ack()?; }
        Err(f) => { warn!(error = %f.error); f.reject(false)?; }
    }
}
```

- `None`: the consumer has ended.
- `Some(Err(f))`: one delivery failed to decode, either in the
  `ContentEncoder` (ADR-0005) or in the `Codec` (ADR-0004). `f.error` is
  `Error::ContentEncoding` or `Error::Codec`, so the caller knows which.
  `f.body` is the body as it came off the wire, before content decoding,
  since that may be what failed.
- The client settles nothing and logs nothing. The caller decides whether to
  ack, reject, requeue or dead-letter.

## Unsettled deliveries

`TypedDelivery` and `DecodeFailure` are `#[must_use]`. Dropping one does
nothing: with `no_ack: false` it stays unacked until the channel closes, when
the broker requeues it. Each unsettled delivery holds a `prefetch` slot, so
leaking them stalls the consumer.

## `no_ack`

`no_ack` stays a field on `SubscribeOpts`, and both modes return the same
`TypedConsumer` and `TypedDelivery`. The caller knows which mode it
subscribed with and does not settle deliveries from a `no_ack: true`
consumer.

If it does, `ack`, `nack` and `reject` return `Err(Error::NoAck)` without
sending anything. Sent, the broker would close the channel with
`PRECONDITION_FAILED` (unknown delivery tag), taking the consumer with it.
The delivery records the consumer's mode to make this check.

## Why

Today a failed delivery is nacked without requeue and skipped, with only a
`warn!` to show for it. In metrics-integrations that is how gzip bodies were
dropped unnoticed (ADR-0005). What to do with a bad message is the caller's
policy, not the client's, and a `Result` makes the failure part of the type
the caller has to match on.

## Considered options

- **Auto-nack and skip (today).** Rejected: hides failures.
- **`SubscribeOpts::on_decode_error` (`Reject` / `Requeue`-once).** Rejected:
  a fixed menu of policies in the client, still hidden from the loop.
  `Requeue`-once also leaned on `redelivered`, which the broker sets for other
  reasons too.
- **`Err` also reports why the consumer ended.** Rejected: mixes per-message
  and terminal failures in one type. `None` is the end, as for a `Stream`.
- **Nack on `Drop`.** Rejected: implicit behaviour; `#[must_use]` warns
  instead.
- **Separate consumer/delivery types for `no_ack`** (two `subscribe` methods,
  or `SubscribeOpts<Ack | NoAck>` typestate), so `ack` does not exist on a
  `no_ack` delivery. Rejected: more API surface and generics than the
  mistake warrants; the local `Error::NoAck` keeps the channel alive.
- **Silently ignore `ack` on a `no_ack` delivery.** Rejected: hides the
  caller's mistake.

## Consequences

- Breaking change: `next()` now returns `Option<Result<..>>`.
- New `DecodeFailure` type, and new `Error::ContentEncoding` and
  `Error::NoAck` variants.
- `next()` maps to `Stream<Item = Result<TypedDelivery<T>, DecodeFailure>>`
  if a `Stream` impl is added later.
