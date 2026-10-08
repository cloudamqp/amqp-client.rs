use super::codec::MessageMeta;
use std::borrow::Cow;
use std::io::{Read, Write};

use flate2::read::{GzDecoder, ZlibDecoder};
use flate2::write::{GzEncoder, ZlibEncoder};

use super::encoding::{ContentEncoder, content_encoding};
use crate::error::Error;
use crate::protocol::properties::BasicProperties;
use crate::protocol::types::ShortStr;

/// gzip and deflate [`ContentEncoder`] (ADR-0005), behind the `compression`
/// feature.
///
/// It decodes by each delivery's `content_encoding`, however it was built:
///
/// | `content_encoding` | Body |
/// |---|---|
/// | absent or `identity` | Passed through |
/// | `gzip` | Gunzipped |
/// | `deflate` | Inflated as zlib (HTTP's meaning of `deflate`) |
/// | anything else | Decode failure |
///
/// [`gzip`](Self::gzip) and [`deflate`](Self::deflate) choose what it
/// publishes: every body is compressed and `content_encoding` set.
pub struct Compression {
    publish: Format,
    max_decoded_size: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Gzip,
    Deflate,
}

impl Format {
    fn name(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
        }
    }
}

impl Compression {
    /// Publishes gzip.
    pub fn gzip() -> Self {
        Self::publishing(Format::Gzip)
    }

    /// Publishes deflate (zlib).
    pub fn deflate() -> Self {
        Self::publishing(Format::Deflate)
    }

    fn publishing(publish: Format) -> Self {
        Self {
            publish,
            max_decoded_size: 128 * 1024 * 1024,
        }
    }

    /// The largest body a delivery may decompress to; a larger one is a
    /// decode failure, so a small message cannot exhaust memory. Default
    /// 128 MiB.
    pub fn max_decoded_size(mut self, bytes: usize) -> Self {
        self.max_decoded_size = bytes;
        self
    }

    /// Reads `decoder` to its end, failing past `max_decoded_size`.
    fn inflate(&self, decoder: impl Read, format: Format) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        let cap = self.max_decoded_size as u64;
        decoder
            .take(cap.saturating_add(1))
            .read_to_end(&mut out)
            .map_err(|e| Error::ContentEncoding(format!("corrupt {} body: {e}", format.name())))?;
        if out.len() as u64 > cap {
            return Err(Error::ContentEncoding(format!(
                "{} body decodes past max_decoded_size ({} bytes)",
                format.name(),
                self.max_decoded_size
            )));
        }
        Ok(out)
    }
}

impl ContentEncoder for Compression {
    fn decode<'a>(&self, body: &'a [u8], meta: &MessageMeta<'_>) -> Result<Cow<'a, [u8]>, Error> {
        match content_encoding(meta)? {
            None | Some("identity") => Ok(Cow::Borrowed(body)),
            Some("gzip") => Ok(Cow::Owned(
                self.inflate(GzDecoder::new(body), Format::Gzip)?,
            )),
            Some("deflate") => Ok(Cow::Owned(
                self.inflate(ZlibDecoder::new(body), Format::Deflate)?,
            )),
            Some(other) => Err(Error::ContentEncoding(format!(
                "content_encoding {other:?} is not supported"
            ))),
        }
    }

    fn encode(&self, body: Vec<u8>, props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
        let name = self.publish.name();
        if let Some(set) = &props.content_encoding
            && set.as_str().ok() != Some(name)
        {
            // The property would lie about the body.
            return Err(Error::ContentEncoding(format!(
                "content_encoding is already set to {set}, but the body is published as {name}"
            )));
        }
        let level = flate2::Compression::default();
        let compressed = match self.publish {
            Format::Gzip => {
                let mut e = GzEncoder::new(Vec::new(), level);
                e.write_all(&body).and_then(|()| e.finish())
            }
            Format::Deflate => {
                let mut e = ZlibEncoder::new(Vec::new(), level);
                e.write_all(&body).and_then(|()| e.finish())
            }
        }
        .map_err(|e| Error::ContentEncoding(format!("{name} failed: {e}")))?;
        props.content_encoding = Some(ShortStr::try_from(name).map_err(Error::Protocol)?);
        Ok(compressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::high_level::encoding::meta;

    fn encoded(encoding: &str) -> BasicProperties {
        BasicProperties::builder()
            .content_encoding(encoding)
            .build()
            .unwrap()
    }

    fn gzipped(body: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(body).unwrap();
        e.finish().unwrap()
    }

    fn zlibbed(body: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(body).unwrap();
        e.finish().unwrap()
    }

    fn decode(c: &Compression, body: &[u8], props: &BasicProperties) -> Result<Vec<u8>, Error> {
        c.decode(body, &meta(props)).map(Cow::into_owned)
    }

    #[test]
    fn decodes_every_supported_encoding_however_built() {
        for c in [Compression::gzip(), Compression::deflate()] {
            let none = BasicProperties::default();
            assert_eq!(decode(&c, b"abc", &none).unwrap(), b"abc");
            assert_eq!(decode(&c, b"abc", &encoded("identity")).unwrap(), b"abc");
            assert_eq!(
                decode(&c, &gzipped(b"abc"), &encoded("gzip")).unwrap(),
                b"abc"
            );
            assert_eq!(
                decode(&c, &zlibbed(b"abc"), &encoded("deflate")).unwrap(),
                b"abc"
            );
        }
    }

    #[test]
    fn passes_unencoded_bodies_through_without_copying() {
        let none = BasicProperties::default();
        let body = Compression::gzip().decode(b"abc", &meta(&none)).unwrap();
        assert!(matches!(body, Cow::Borrowed(_)));
    }

    #[test]
    fn an_unknown_encoding_fails() {
        let result = decode(&Compression::gzip(), b"abc", &encoded("br"));
        assert!(
            matches!(result, Err(Error::ContentEncoding(_))),
            "{result:?}"
        );
    }

    #[test]
    fn corrupt_data_fails() {
        let gzip = decode(&Compression::gzip(), b"not gzip", &encoded("gzip"));
        assert!(matches!(gzip, Err(Error::ContentEncoding(_))), "{gzip:?}");
        let zlib = decode(&Compression::gzip(), b"not zlib", &encoded("deflate"));
        assert!(matches!(zlib, Err(Error::ContentEncoding(_))), "{zlib:?}");
    }

    #[test]
    fn a_body_inflating_past_the_cap_fails() {
        let c = Compression::gzip().max_decoded_size(4);
        assert_eq!(
            decode(&c, &gzipped(b"abcd"), &encoded("gzip")).unwrap(),
            b"abcd"
        );
        let big = decode(&c, &gzipped(b"abcde"), &encoded("gzip"));
        assert!(matches!(big, Err(Error::ContentEncoding(_))), "{big:?}");
        let big = decode(&c, &zlibbed(b"abcde"), &encoded("deflate"));
        assert!(matches!(big, Err(Error::ContentEncoding(_))), "{big:?}");
    }

    #[test]
    fn the_cap_defaults_to_128_mib() {
        assert_eq!(Compression::gzip().max_decoded_size, 128 * 1024 * 1024);
    }

    #[test]
    fn publishes_compressed_bodies_and_sets_the_encoding() {
        let mut props = BasicProperties::default();
        let gzip = Compression::gzip()
            .encode(b"abc".to_vec(), &mut props)
            .unwrap();
        assert_eq!(props.content_encoding.unwrap().as_str().unwrap(), "gzip");
        assert_eq!(
            decode(&Compression::gzip(), &gzip, &encoded("gzip")).unwrap(),
            b"abc"
        );

        let mut props = BasicProperties::default();
        let zlib = Compression::deflate()
            .encode(b"abc".to_vec(), &mut props)
            .unwrap();
        assert_eq!(props.content_encoding.unwrap().as_str().unwrap(), "deflate");
        assert_eq!(
            decode(&Compression::gzip(), &zlib, &encoded("deflate")).unwrap(),
            b"abc"
        );
    }

    #[test]
    fn publishing_over_a_different_encoding_fails() {
        let mut props = encoded("deflate");
        let result = Compression::gzip().encode(b"abc".to_vec(), &mut props);
        assert!(
            matches!(result, Err(Error::ContentEncoding(_))),
            "{result:?}"
        );
        let mut props = encoded("gzip");
        assert!(
            Compression::gzip()
                .encode(b"abc".to_vec(), &mut props)
                .is_ok()
        );
    }
}
