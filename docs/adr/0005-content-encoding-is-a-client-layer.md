---
status: proposed
---

# Content encoding is a client layer

The high-level `Client` handles `content_encoding` in its own layer, between
the wire and the `Codec`, as HTTP separates `Content-Encoding` from
`Content-Type`. A codec sees and produces uncompressed bytes only (ADR-0004).

The layer is a trait the user configures on the client:

```rust
pub trait ContentEncoder: Send + Sync + 'static {
    fn decode<'a>(
        &self,
        body: &'a [u8],
        meta: &MessageMeta<'_>,
    ) -> Result<Cow<'a, [u8]>, Error>;
    fn encode(
        &self,
        body: Vec<u8>,
        props: &mut BasicProperties,
    ) -> Result<Vec<u8>, Error>;
}

Client::builder()
    .codec(JsonCodec)
    .encoding(Compression::gzip())
```

`decode` reads `meta.properties.content_encoding` to decide what to do;
`encode` sets it. Users can implement their own (e.g. `zstd`). Encoder
errors, in either direction, are `Error::ContentEncoding`, so they are not
confused with the codec's `Error::Codec`.

On consume: wire → `ContentEncoder::decode` → `Codec::decode`.
On publish: `Codec::encode` → `ContentEncoder::encode` → wire.

## Built-in encoders

`Identity` is the default. It passes bodies through and does not touch
`content_encoding` on publish. A delivery whose `content_encoding` is
anything but absent or `identity` is a decode failure whose message names
the `compression` feature, so compressed bytes never reach the codec.

`Compression` (behind the `compression` cargo feature, pulling `flate2`, off
by default) decodes by the delivery's `content_encoding`:

| `content_encoding` | Body |
|---|---|
| absent or `identity` | Passed through |
| `gzip` | Gunzipped |
| `deflate` | Inflated as zlib (HTTP's meaning of `deflate`) |
| anything else | Decode failure |

It accepts all of these regardless of how it was built.
`Compression::gzip()` and `Compression::deflate()` choose only what it
publishes: every body is compressed and `content_encoding` set. If the
caller already set a different `content_encoding` in `publish_with_props`,
the publish fails with `Error::ContentEncoding` before sending, since the property
would lie about the body.

Decompressed bodies are capped (`Compression::max_decoded_size`, default
128 MiB); a body that inflates past the cap is a decode failure, so a small
message cannot exhaust memory.

Every consume-side failure above (unknown encoding, `Identity` given a
compressed body, corrupt data, the size cap) reaches the caller as a
`DecodeFailure` with `error: Error::ContentEncoding` and the body as it came
off the wire (ADR-0006). The codec is not called, and the client does not
settle the delivery.

## Why

In metrics-integrations, producers on one exchange send JSON as
`content_encoding: deflate` and `gzip`. With only `Codec::decode(bytes)`, the
libtest-metrics demo had to guess the encoding from magic bytes (`1f 8b` for
gzip). The broker already says how each body is encoded; the client should
read it, so every codec does not have to. (That gzip bodies were nacked and
dropped unnoticed before the demo sniffed them is ADR-0006's concern.)

## Considered options

- **Codec sniffs magic bytes.** What the demo does. Rejected: every codec
  repeats it, and it cannot tell zlib from uncompressed bytes that happen to
  start with `0x78`.
- **Codec reads `content_encoding` via ADR-0004's metadata.** Rejected: mixes
  two concerns in one trait, and each codec reimplements the same table.
- **A `Codec` wrapper (`Compressed<JsonCodec>`).** Rejected: same mixing of
  concerns, just in a separate type.
- **A closed enum (`ContentEncoding::{Identity, Gzip, Deflate}`).** Simpler,
  but users cannot add an encoding without a release.
- **Compression on by default.** Rejected: most callers don't need `flate2`.
- **Low-level layer too.** Rejected: `Consumer` hands out the raw wire body
  and stays a thin protocol wrapper.

## Consequences

- `ByteCodec` consumers receive decoded bytes. A caller who wants the
  compressed body uses the low-level `Consumer`. With `Identity`, a
  compressed delivery arrives as a `DecodeFailure` whose `body` is the
  compressed bytes. To publish pre-compressed bytes, keep `Identity` and set
  `content_encoding` by hand.
- Codecs stay pure: `JsonCodec` is serde_json and nothing else.
- `MessageMeta` is shared by `Codec` and `ContentEncoder`.
- Raw deflate without a zlib header is not supported; Ruby's
  `Zlib::Deflate.deflate` and `Zlib.gzip` both produce what the table expects.
