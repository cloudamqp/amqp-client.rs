use std::any::{Any, TypeId};

use crate::error::Error;
use crate::protocol::properties::BasicProperties;

/// What a codec may know about a message besides its body (ADR-0004).
#[derive(Clone, Copy, Debug)]
pub struct MessageMeta<'a> {
    pub properties: &'a BasicProperties,
    pub exchange: &'a str,
    pub routing_key: &'a str,
}

/// Encodes typed values to wire bytes and back.
///
/// Both directions see the message's metadata, so a codec can pick the type
/// to decode by e.g. the routing key or the `type` property, and set that
/// property when encoding (ADR-0004). Bodies are uncompressed: a codec must
/// not read or set `content_encoding`.
///
/// The `+ 'static` bound on `T` lets implementations inspect the concrete
/// type at runtime (used by [`ByteCodec`]) and is satisfied by every owned
/// type and `'static` borrow that derives `Serialize`/`Deserialize`.
pub trait Codec: Send + Sync + 'static {
    fn content_type(&self) -> &str;
    /// Encode `value`, adding to or overwriting the outgoing `props`.
    fn encode<T: serde::Serialize + 'static>(
        &self,
        value: &T,
        props: &mut BasicProperties,
    ) -> Result<Vec<u8>, Error>;
    fn decode<T: serde::de::DeserializeOwned + 'static>(
        &self,
        bytes: &[u8],
        meta: &MessageMeta<'_>,
    ) -> Result<T, Error>;
}

/// Pass-through codec for raw bytes.
///
/// This is the default codec used by [`Client`](super::client::Client) when
/// the caller doesn't provide one via
/// [`ClientBuilder::codec`](super::client::ClientBuilder::codec). It accepts
/// only `Vec<u8>` — any other type returns
/// [`Error::Codec`]. Sets `content_type` to `application/octet-stream`.
pub struct ByteCodec;

impl Codec for ByteCodec {
    fn content_type(&self) -> &str {
        "application/octet-stream"
    }

    fn encode<T: serde::Serialize + 'static>(
        &self,
        value: &T,
        _props: &mut BasicProperties,
    ) -> Result<Vec<u8>, Error> {
        let any: &dyn Any = value;
        if let Some(v) = any.downcast_ref::<Vec<u8>>() {
            return Ok(v.clone());
        }
        Err(Error::Codec(format!(
            "ByteCodec only supports Vec<u8>, got {}",
            std::any::type_name::<T>()
        )))
    }

    fn decode<T: serde::de::DeserializeOwned + 'static>(
        &self,
        bytes: &[u8],
        _meta: &MessageMeta<'_>,
    ) -> Result<T, Error> {
        if TypeId::of::<T>() == TypeId::of::<Vec<u8>>() {
            let boxed: Box<dyn Any> = Box::new(bytes.to_vec());
            return boxed
                .downcast::<T>()
                .map(|b| *b)
                .map_err(|_| Error::Codec("internal: failed to downcast to Vec<u8>".into()));
        }
        Err(Error::Codec(format!(
            "ByteCodec only supports Vec<u8>, got {}",
            std::any::type_name::<T>()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::ShortStr;

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

        fn encode<T: serde::Serialize + 'static>(
            &self,
            value: &T,
            _props: &mut BasicProperties,
        ) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
        }

        fn decode<T: serde::de::DeserializeOwned + 'static>(
            &self,
            bytes: &[u8],
            _meta: &MessageMeta<'_>,
        ) -> Result<T, Error> {
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

        fn encode<T: serde::Serialize + 'static>(
            &self,
            value: &T,
            props: &mut BasicProperties,
        ) -> Result<Vec<u8>, Error> {
            let tagged = serde_json::to_value(value).map_err(|e| Error::Codec(e.to_string()))?;
            let (variant, body) = tagged
                .as_object()
                .and_then(|o| o.iter().next())
                .ok_or_else(|| Error::Codec("not an enum".into()))?;
            props.type_field = Some(ShortStr::try_from(variant.as_str()).map_err(Error::Protocol)?);
            serde_json::to_vec(body).map_err(|e| Error::Codec(e.to_string()))
        }

        fn decode<T: serde::de::DeserializeOwned + 'static>(
            &self,
            bytes: &[u8],
            meta: &MessageMeta<'_>,
        ) -> Result<T, Error> {
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

    #[test]
    fn byte_codec_roundtrips_vec_u8() {
        let original: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let mut props = BasicProperties::default();
        let encoded = ByteCodec.encode(&original, &mut props).unwrap();
        assert_eq!(encoded, original);
        assert_eq!(
            props,
            BasicProperties::default(),
            "ByteCodec set properties"
        );
        let decoded: Vec<u8> = ByteCodec.decode(&encoded, &meta(&props, "k")).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn byte_codec_rejects_non_bytes_on_encode() {
        let result = ByteCodec.encode(&String::from("oops"), &mut BasicProperties::default());
        let err = result.unwrap_err();
        match err {
            Error::Codec(msg) => assert!(msg.contains("Vec<u8>")),
            other => panic!("expected Codec error, got {other:?}"),
        }
    }

    #[test]
    fn byte_codec_rejects_non_bytes_on_decode() {
        let props = BasicProperties::default();
        let result: Result<String, _> = ByteCodec.decode(b"oops", &meta(&props, "k"));
        let err = result.unwrap_err();
        match err {
            Error::Codec(msg) => assert!(msg.contains("Vec<u8>")),
            other => panic!("expected Codec error, got {other:?}"),
        }
    }
}
