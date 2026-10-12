---
status: accepted
---

# Codec sees message metadata

`Codec` only turns payload bytes into values and back. Both directions see
the message's metadata:

```rust
pub trait Codec: Send + Sync + 'static {
    fn content_type(&self) -> &str;
    fn encode<T: Serialize + 'static>(
        &self,
        value: &T,
        props: &mut BasicProperties,
    ) -> Result<Vec<u8>, Error>;
    fn decode<T: DeserializeOwned + 'static>(
        &self,
        bytes: &[u8],
        meta: &MessageMeta<'_>,
    ) -> Result<T, Error>;
}

pub struct MessageMeta<'a> {
    pub properties: &'a BasicProperties,
    pub exchange: &'a str,
    pub routing_key: &'a str,
}
```

Today `decode` sees only the bytes, so a codec cannot know which type it
is decoding. In metrics-integrations, one queue bound to `amq.topic` receives
`rmq.queues.<host>` (a JSON array of queues) and `rmq.overview.<host>` (a
JSON object). The type is named by the routing key, or should be by the AMQP
`type` property, not by the body. A codec that sees `meta` can decode into an
enum `T` by that discriminator. An unknown discriminator is an `Error::Codec`,
like any other body the codec cannot decode.

`encode` gets the outgoing properties so a codec can set the same
discriminator, e.g. `type` from the enum variant. That keeps both ends of a
typed message in one place.

Content encoding is not the codec's concern: the client's `ContentEncoder`
runs between the wire and the codec (ADR-0005). The bytes `decode` receives
are already decoded, and `encode` returns uncompressed bytes. A codec must
not read or set `content_encoding`.

`TypedConsumer::next` builds `MessageMeta` from the raw delivery it already
holds. A `Codec::decode` error reaches the caller as a `DecodeFailure` with
`error: Error::Codec` (ADR-0006); the client does not settle it. A
`Codec::encode` error fails the publish before anything is sent.

## Considered options

- **Keep `decode(bytes)`; use `#[serde(untagged)]` enums.** Rejected: the
  first variant that fits wins, and partial structs that ignore unknown fields
  fit too easily. A wrong type is chosen without error.
- **Keep `decode(bytes)`; require a tag in the body (`#[serde(tag = ...)]`).**
  Rejected: the client does not own the producers, and the discriminator
  belongs in the envelope, where AMQP already has the `type` property.
- **Two-stage decode (`T = RawValue`, caller dispatches on routing key).**
  Works today, but the caller decodes by hand and the typed consumer gives
  nothing over the raw one.
- **`Queue::subscribe_with(|meta, bytes| -> Result<T, Error>)`.** A decoder
  per consumer instead of per client. Rejected for now: two places to decode
  is more surface. Can be added later on top of this.
- **Pass the whole raw `Message`.** Rejected: exposes the delivery tag and
  ack handle to the codec, which must not ack.
- **Leave `encode` unchanged; publishers set `type` via
  `publish_with_props`.** Rejected: the codec that reads the discriminator
  should also write it, or the two drift apart.

## Consequences

- Breaking change for every `Codec` implementation, including `ByteCodec`
  and user codecs; acceptable before 1.0. A codec that does not care ignores
  `meta` and `props`.
- On publish, the caller's `publish_with_props` properties are passed to
  `encode`, which may add to or overwrite them.
