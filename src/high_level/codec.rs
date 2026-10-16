use bytes::Bytes;

use crate::error::Error;
use crate::protocol::properties::BasicProperties;

/// What a codec may know about a message besides its body (ADR-0004).
#[derive(Clone, Copy, Debug)]
pub struct MessageMeta<'a> {
    pub properties: &'a BasicProperties,
    pub exchange: &'a str,
    pub routing_key: &'a str,
}

/// Turns typed values into wire bytes and back, for the types it implements
/// [`Encode`] and [`Decode`] for (ADR-0007). Publishing or subscribing with
/// any other type does not compile.
///
/// Both directions see the message's metadata, so a codec can pick the type
/// to decode by e.g. the routing key or the `type` property, and set that
/// property when encoding (ADR-0004). Bodies are uncompressed: a codec must
/// not read or set `content_encoding`.
pub trait Codec: Send + Sync + 'static {
    fn content_type(&self) -> &str;
}

/// A [`Codec`] that can encode a `T`.
pub trait Encode<T: ?Sized>: Codec {
    /// Encode `value`, adding to or overwriting the outgoing `props`.
    fn encode(&self, value: &T, props: &mut BasicProperties) -> Result<Vec<u8>, Error>;
}

/// A [`Codec`] that can decode a `T`.
pub trait Decode<T>: Codec {
    fn decode(&self, bytes: &[u8], meta: &MessageMeta<'_>) -> Result<T, Error>;
}

/// Pass-through codec for raw bytes and UTF-8 text.
///
/// This is the default codec used by [`Client`](super::client::Client) when
/// the caller doesn't provide one via
/// [`ClientBuilder::codec`](super::client::ClientBuilder::codec). It encodes
/// `str`, `String`, `[u8]`, `[u8; N]`, `Vec<u8>` and [`Bytes`], and decodes
/// into `Vec<u8>`, [`Bytes`] and `String`; a body that is not UTF-8 does not
/// decode into a `String`. Sets `content_type` to `application/octet-stream`.
///
/// Other types do not compile:
///
/// ```compile_fail
/// # use amqp_client_rs::{BasicProperties, ByteCodec, Encode};
/// ByteCodec.encode(&42u32, &mut BasicProperties::default());
/// ```
///
/// ```compile_fail
/// # use amqp_client_rs::{ByteCodec, Decode, MessageMeta};
/// # fn f(meta: &MessageMeta<'_>) {
/// let n: Result<u32, _> = ByteCodec.decode(b"42", meta);
/// # }
/// ```
pub struct ByteCodec;

impl Codec for ByteCodec {
    fn content_type(&self) -> &str {
        "application/octet-stream"
    }
}

impl Encode<[u8]> for ByteCodec {
    fn encode(&self, value: &[u8], _props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
        Ok(value.to_vec())
    }
}

/// Implements `Encode<$t>` for a type that derefs to `[u8]` via `$bytes`.
macro_rules! encode_as_bytes {
    ($($t:ty => $bytes:expr),* $(,)?) => {$(
        impl Encode<$t> for ByteCodec {
            fn encode(&self, value: &$t, props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
                Encode::<[u8]>::encode(self, $bytes(value), props)
            }
        }
    )*};
}

encode_as_bytes! {
    str => str::as_bytes,
    String => String::as_bytes,
    Vec<u8> => Vec::as_slice,
    Bytes => Bytes::as_ref,
}

impl<const N: usize> Encode<[u8; N]> for ByteCodec {
    fn encode(&self, value: &[u8; N], props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
        Encode::<[u8]>::encode(self, value, props)
    }
}

impl Decode<Vec<u8>> for ByteCodec {
    fn decode(&self, bytes: &[u8], _meta: &MessageMeta<'_>) -> Result<Vec<u8>, Error> {
        Ok(bytes.to_vec())
    }
}

impl Decode<Bytes> for ByteCodec {
    fn decode(&self, bytes: &[u8], _meta: &MessageMeta<'_>) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(bytes))
    }
}

impl Decode<String> for ByteCodec {
    fn decode(&self, bytes: &[u8], _meta: &MessageMeta<'_>) -> Result<String, Error> {
        String::from_utf8(bytes.to_vec()).map_err(|e| Error::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::ShortStr;
    use bytes::Bytes;

    fn meta<'a>(properties: &'a BasicProperties, routing_key: &'a str) -> MessageMeta<'a> {
        MessageMeta {
            properties,
            exchange: "amq.topic",
            routing_key,
        }
    }

    struct TestCodec;

    impl Codec for TestCodec {
        fn content_type(&self) -> &str {
            "application/json"
        }
    }

    impl<T: serde::Serialize + ?Sized> Encode<T> for TestCodec {
        fn encode(&self, value: &T, _props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
        }
    }

    impl<T: serde::de::DeserializeOwned> Decode<T> for TestCodec {
        fn decode(&self, bytes: &[u8], _meta: &MessageMeta<'_>) -> Result<T, Error> {
            serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
        }
    }

    /// Decodes `{"<routing key prefix>": body}`, so serde picks the enum
    /// variant from the routing key, and on encode sets `type` to the variant.
    struct RoutedCodec;

    impl Codec for RoutedCodec {
        fn content_type(&self) -> &str {
            "application/json"
        }
    }

    impl<T: serde::Serialize + ?Sized> Encode<T> for RoutedCodec {
        fn encode(&self, value: &T, props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
            let tagged = serde_json::to_value(value).map_err(|e| Error::Codec(e.to_string()))?;
            let (variant, body) = tagged
                .as_object()
                .and_then(|o| o.iter().next())
                .ok_or_else(|| Error::Codec("not an enum".into()))?;
            props.type_field = Some(ShortStr::try_from(variant.as_str()).map_err(Error::Protocol)?);
            serde_json::to_vec(body).map_err(|e| Error::Codec(e.to_string()))
        }
    }

    impl<T: serde::de::DeserializeOwned> Decode<T> for RoutedCodec {
        fn decode(&self, bytes: &[u8], meta: &MessageMeta<'_>) -> Result<T, Error> {
            let variant = meta.routing_key.split('.').nth(1).unwrap_or_default();
            let body: serde_json::Value =
                serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))?;
            serde_json::from_value(serde_json::json!({ variant: body }))
                .map_err(|e| Error::Codec(e.to_string()))
        }
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum Metric {
        Queues(Vec<String>),
        Overview { messages: u64 },
    }

    #[test]
    fn codec_content_type() {
        let codec = TestCodec;
        assert_eq!(codec.content_type(), "application/json");
    }

    #[test]
    fn codec_roundtrip() {
        let codec = TestCodec;
        let original = String::from("hello, world");
        let mut props = BasicProperties::default();
        let encoded = codec.encode(&original, &mut props).expect("encode failed");
        let decoded: String = codec
            .decode(&encoded, &meta(&props, "k"))
            .expect("decode failed");
        assert_eq!(decoded, original);
    }

    #[test]
    fn codec_decode_error() {
        let codec = TestCodec;
        let bad_bytes = b"not valid json{{{{";
        let props = BasicProperties::default();
        let result: Result<String, Error> = codec.decode(bad_bytes, &meta(&props, "k"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Codec(_)));
    }

    #[test]
    fn decode_can_pick_the_type_by_routing_key() {
        let props = BasicProperties::default();
        let queues: Metric = RoutedCodec
            .decode(br#"["a","b"]"#, &meta(&props, "rmq.queues.host"))
            .unwrap();
        assert_eq!(queues, Metric::Queues(vec!["a".into(), "b".into()]));
        let overview: Metric = RoutedCodec
            .decode(br#"{"messages":3}"#, &meta(&props, "rmq.overview.host"))
            .unwrap();
        assert_eq!(overview, Metric::Overview { messages: 3 });
    }

    #[test]
    fn decode_of_an_unknown_discriminator_is_a_codec_error() {
        let props = BasicProperties::default();
        let result: Result<Metric, _> = RoutedCodec.decode(b"1", &meta(&props, "rmq.nodes.host"));
        assert!(matches!(result, Err(Error::Codec(_))));
    }

    #[test]
    fn encode_can_set_properties() {
        let mut props = BasicProperties::default();
        let body = RoutedCodec
            .encode(&Metric::Overview { messages: 3 }, &mut props)
            .unwrap();
        assert_eq!(body, br#"{"messages":3}"#);
        assert_eq!(props.type_field.unwrap().as_str().unwrap(), "overview");
    }

    #[test]
    fn byte_codec_content_type() {
        assert_eq!(ByteCodec.content_type(), "application/octet-stream");
    }

    /// `value` encoded by `ByteCodec`, which must leave the properties alone.
    fn byte_encode<T: ?Sized>(value: &T) -> Vec<u8>
    where
        ByteCodec: Encode<T>,
    {
        let mut props = BasicProperties::default();
        let encoded = ByteCodec.encode(value, &mut props).unwrap();
        assert_eq!(
            props,
            BasicProperties::default(),
            "ByteCodec set properties"
        );
        encoded
    }

    fn byte_decode<T>(bytes: &[u8]) -> Result<T, Error>
    where
        ByteCodec: Decode<T>,
    {
        ByteCodec.decode(bytes, &meta(&BasicProperties::default(), "k"))
    }

    #[test]
    fn byte_codec_encodes_bytes_and_text() {
        assert_eq!(byte_encode("hello"), b"hello");
        assert_eq!(byte_encode(&String::from("hello")), b"hello");
        assert_eq!(byte_encode(&b"hello"[..]), b"hello");
        assert_eq!(byte_encode(b"hello"), b"hello");
        assert_eq!(byte_encode(&b"hello".to_vec()), b"hello");
        assert_eq!(byte_encode(&Bytes::from_static(b"hello")), b"hello");
    }

    #[test]
    fn byte_codec_encodes_a_borrow_of_a_local() {
        let owned = String::from("hello");
        assert_eq!(byte_encode(owned.as_str()), b"hello");
    }

    #[test]
    fn byte_codec_decodes_bytes_and_text() {
        assert_eq!(
            byte_decode::<Vec<u8>>(b"\xDE\xAD").unwrap(),
            vec![0xDE, 0xAD]
        );
        assert_eq!(
            byte_decode::<Bytes>(b"hello").unwrap(),
            Bytes::from_static(b"hello")
        );
        assert_eq!(byte_decode::<String>(b"hello").unwrap(), "hello");
    }

    #[test]
    fn byte_codec_rejects_invalid_utf8_as_string() {
        let result = byte_decode::<String>(b"\xFF\xFE");
        assert!(matches!(result, Err(Error::Codec(_))), "{result:?}");
    }
}
