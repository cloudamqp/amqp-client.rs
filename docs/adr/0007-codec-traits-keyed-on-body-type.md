---
status: accepted
---

# Codec traits keyed on body type

Supersedes the trait signature in ADR-0004; what a codec sees (the message's
metadata) is unchanged.

```rust
pub trait Codec: Send + Sync + 'static {
    fn content_type(&self) -> &str;
}

pub trait Encode<T: ?Sized>: Codec {
    fn encode(&self, value: &T, props: &mut BasicProperties) -> Result<Vec<u8>, Error>;
}

pub trait Decode<T>: Codec {
    fn decode(&self, bytes: &[u8], meta: &MessageMeta<'_>) -> Result<T, Error>;
}
```

`Client::publish*` take `body: &T` where `C: Encode<T>`, and
`Queue::subscribe::<T>` requires `C: Decode<T>`.

With generic `encode<T: Serialize + 'static>` methods, an implementation
cannot narrow which `T` it accepts. `ByteCodec` had to downcast at runtime and
reject everything but `Vec<u8>`, so `client.publish("", "q", &"hello")`
compiled and then failed, and `subscribe::<String>()` made every delivery a
`DecodeFailure` (#6). The `'static` bound needed for the downcast also
rejected borrows of locals.

Keyed on `T`, a codec states the types it supports, and any other type is a
compile error. `ByteCodec` implements `Encode` for `str`, `String`, `[u8]`,
`[u8; N]`, `Vec<u8>` and `Bytes`, and `Decode` for `Vec<u8>`, `Bytes` and
`String`; a body that is not UTF-8 is an `Error::Codec`, a `DecodeFailure`
like any other (ADR-0006). A serde codec implements both with a blanket impl:

```rust
impl<T: Serialize + ?Sized> Encode<T> for JsonCodec { ... }
impl<T: DeserializeOwned> Decode<T> for JsonCodec { ... }
```

`Encode` and `Decode` are separate traits because the types differ: a `&str`
can be published, but a delivery can only be decoded into an owned `String`.

## Considered options

- **More downcasts in `ByteCodec`.** Rejected: `[u8; N]` cannot be matched
  for every `N`, the `'static` bound stays, `Bytes` is not `Serialize` without
  a feature, and unsupported types still fail only at runtime.
- **One `Codec<T>` trait.** Rejected: it forces the same type for encode and
  decode, so `&str` could not be published where `String` is decoded.
- **`publish_bytes(impl IntoBody)` beside the codec.** Fixes publishing
  bytes only, not `subscribe::<String>()`, and is a second way to publish.

## Consequences

- Breaking change for every `Codec` implementation and for generic code over
  `C: Codec`, which now needs `C: Encode<T>` / `C: Decode<T>`; acceptable
  before 1.0.
- A codec cannot mix a blanket serde impl with impls for specific types like
  `[u8; N]`: they overlap.
- The `'static` bound on `T` is gone.
- `encode` still returns `Vec<u8>`, so publishing bytes copies them once.
