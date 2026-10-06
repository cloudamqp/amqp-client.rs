use std::any::{Any, TypeId};

use crate::error::Error;

/// Encodes typed values to wire bytes and back.
///
/// The `+ 'static` bound on `T` lets implementations inspect the concrete
/// type at runtime (used by [`ByteCodec`]) and is satisfied by every owned
/// type and `'static` borrow that derives `Serialize`/`Deserialize`.
pub trait Codec: Send + Sync + 'static {
    fn content_type(&self) -> &str;
    fn encode<T: serde::Serialize + 'static>(&self, value: &T) -> Result<Vec<u8>, Error>;
    fn decode<T: serde::de::DeserializeOwned + 'static>(&self, bytes: &[u8]) -> Result<T, Error>;
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

    fn encode<T: serde::Serialize + 'static>(&self, value: &T) -> Result<Vec<u8>, Error> {
        let any: &dyn Any = value;
        if let Some(v) = any.downcast_ref::<Vec<u8>>() {
            return Ok(v.clone());
        }
        Err(Error::Codec(format!(
            "ByteCodec only supports Vec<u8>, got {}",
            std::any::type_name::<T>()
        )))
    }

    fn decode<T: serde::de::DeserializeOwned + 'static>(&self, bytes: &[u8]) -> Result<T, Error> {
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

    struct TestCodec;

    impl Codec for TestCodec {
        fn content_type(&self) -> &str {
            "application/json"
        }

        fn encode<T: serde::Serialize + 'static>(&self, value: &T) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
        }

        fn decode<T: serde::de::DeserializeOwned + 'static>(
            &self,
            bytes: &[u8],
        ) -> Result<T, Error> {
            serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
        }
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
        let encoded = codec.encode(&original).expect("encode failed");
        let decoded: String = codec.decode(&encoded).expect("decode failed");
        assert_eq!(decoded, original);
    }

    #[test]
    fn codec_decode_error() {
        let codec = TestCodec;
        let bad_bytes = b"not valid json{{{{";
        let result: Result<String, Error> = codec.decode(bad_bytes);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Codec(_)));
    }

    #[test]
    fn byte_codec_content_type() {
        assert_eq!(ByteCodec.content_type(), "application/octet-stream");
    }

    #[test]
    fn byte_codec_roundtrips_vec_u8() {
        let original: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let encoded = ByteCodec.encode(&original).unwrap();
        assert_eq!(encoded, original);
        let decoded: Vec<u8> = ByteCodec.decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn byte_codec_rejects_non_bytes_on_encode() {
        let result = ByteCodec.encode(&String::from("oops"));
        let err = result.unwrap_err();
        match err {
            Error::Codec(msg) => assert!(msg.contains("Vec<u8>")),
            other => panic!("expected Codec error, got {other:?}"),
        }
    }

    #[test]
    fn byte_codec_rejects_non_bytes_on_decode() {
        let result: Result<String, _> = ByteCodec.decode(b"oops");
        let err = result.unwrap_err();
        match err {
            Error::Codec(msg) => assert!(msg.contains("Vec<u8>")),
            other => panic!("expected Codec error, got {other:?}"),
        }
    }
}
