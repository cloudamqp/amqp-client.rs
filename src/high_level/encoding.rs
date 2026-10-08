use std::borrow::Cow;

use super::codec::MessageMeta;
use crate::error::Error;
use crate::protocol::properties::BasicProperties;

/// Handles `content_encoding` between the wire and the [`Codec`](super::Codec)
/// (ADR-0005), as HTTP separates `Content-Encoding` from `Content-Type`.
///
/// On consume: wire → [`decode`](Self::decode) → `Codec::decode`.
/// On publish: `Codec::encode` → [`encode`](Self::encode) → wire.
/// Errors in either direction are [`Error::ContentEncoding`].
pub trait ContentEncoder: Send + Sync + 'static {
    /// Decode `body` by `meta.properties.content_encoding`.
    fn decode<'a>(&self, body: &'a [u8], meta: &MessageMeta<'_>) -> Result<Cow<'a, [u8]>, Error>;
    /// Encode `body`, setting `content_encoding` in `props`.
    fn encode(&self, body: Vec<u8>, props: &mut BasicProperties) -> Result<Vec<u8>, Error>;
}

/// The default [`ContentEncoder`]: bodies pass through as they are.
///
/// A delivery with any `content_encoding` but none or `identity` fails to
/// decode, so compressed bytes never reach the codec. On publish it leaves
/// `content_encoding` alone.
pub struct Identity;

impl ContentEncoder for Identity {
    fn decode<'a>(&self, body: &'a [u8], meta: &MessageMeta<'_>) -> Result<Cow<'a, [u8]>, Error> {
        match content_encoding(meta)? {
            None | Some("identity") => Ok(Cow::Borrowed(body)),
            Some(other) => Err(Error::ContentEncoding(format!(
                "content_encoding {other:?} is not supported; \
                 enable the `compression` feature and use `Compression`"
            ))),
        }
    }

    fn encode(&self, body: Vec<u8>, _props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
        Ok(body)
    }
}

/// The delivery's `content_encoding`, if any.
pub(crate) fn content_encoding<'m>(meta: &MessageMeta<'m>) -> Result<Option<&'m str>, Error> {
    meta.properties
        .content_encoding
        .as_ref()
        .map(|e| {
            e.as_str()
                .map_err(|_| Error::ContentEncoding("content_encoding is not UTF-8".into()))
        })
        .transpose()
}

#[cfg(test)]
pub(crate) fn meta(properties: &BasicProperties) -> MessageMeta<'_> {
    MessageMeta {
        properties,
        exchange: "",
        routing_key: "k",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(encoding: &str) -> BasicProperties {
        BasicProperties::builder()
            .content_encoding(encoding)
            .build()
            .unwrap()
    }

    #[test]
    fn identity_passes_unencoded_bodies_through() {
        let none = BasicProperties::default();
        let body = Identity.decode(b"abc", &meta(&none)).unwrap();
        assert!(matches!(body, Cow::Borrowed(b"abc")));
        let identity = encoded("identity");
        let body = Identity.decode(b"abc", &meta(&identity)).unwrap();
        assert_eq!(&*body, b"abc");
    }

    #[test]
    fn identity_rejects_encoded_bodies_naming_the_feature() {
        let gzip = encoded("gzip");
        match Identity.decode(b"\x1f\x8b", &meta(&gzip)) {
            Err(Error::ContentEncoding(msg)) => assert!(msg.contains("compression"), "{msg}"),
            other => panic!("expected ContentEncoding error, got {other:?}"),
        }
    }

    #[test]
    fn identity_publishes_bodies_and_properties_as_they_are() {
        let mut props = BasicProperties::default();
        assert_eq!(
            Identity.encode(b"abc".to_vec(), &mut props).unwrap(),
            b"abc"
        );
        assert_eq!(props, BasicProperties::default());
    }
}
