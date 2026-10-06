use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

use crate::error::ProtocolError;

// ---------------------------------------------------------------------------
// Encode / Decode traits
// ---------------------------------------------------------------------------

/// Serialize an AMQP value into a byte buffer.
pub trait AmqpEncode {
    fn encode(&self, buf: &mut BytesMut);
}

/// Deserialize an AMQP value from a byte buffer.
pub trait AmqpDecode: Sized {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError>;
}

// ---------------------------------------------------------------------------
// Primitive impls
// ---------------------------------------------------------------------------

impl AmqpEncode for u8 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(*self);
    }
}

impl AmqpDecode for u8 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 1)?;
        Ok(buf.get_u8())
    }
}

impl AmqpEncode for u16 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(*self);
    }
}

impl AmqpDecode for u16 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 2)?;
        Ok(buf.get_u16())
    }
}

impl AmqpEncode for u32 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(*self);
    }
}

impl AmqpDecode for u32 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 4)?;
        Ok(buf.get_u32())
    }
}

impl AmqpEncode for u64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(*self);
    }
}

impl AmqpDecode for u64 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 8)?;
        Ok(buf.get_u64())
    }
}

impl AmqpEncode for i8 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i8(*self);
    }
}

impl AmqpDecode for i8 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 1)?;
        Ok(buf.get_i8())
    }
}

impl AmqpEncode for i16 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(*self);
    }
}

impl AmqpDecode for i16 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 2)?;
        Ok(buf.get_i16())
    }
}

impl AmqpEncode for i32 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(*self);
    }
}

impl AmqpDecode for i32 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 4)?;
        Ok(buf.get_i32())
    }
}

impl AmqpEncode for i64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i64(*self);
    }
}

impl AmqpDecode for i64 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 8)?;
        Ok(buf.get_i64())
    }
}

impl AmqpEncode for f32 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_f32(*self);
    }
}

impl AmqpDecode for f32 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 4)?;
        Ok(buf.get_f32())
    }
}

impl AmqpEncode for f64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_f64(*self);
    }
}

impl AmqpDecode for f64 {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 8)?;
        Ok(buf.get_f64())
    }
}

impl AmqpEncode for bool {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(u8::from(*self));
    }
}

impl AmqpDecode for bool {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 1)?;
        Ok(buf.get_u8() != 0)
    }
}

// ---------------------------------------------------------------------------
// ShortStr — AMQP short string (u8 length prefix, max 255 bytes)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ShortStr(Bytes);

impl ShortStr {
    pub fn new() -> Self {
        Self(Bytes::new())
    }

    /// Create from a static str. Panics if longer than 255 bytes.
    pub fn from_static(s: &'static str) -> Self {
        assert!(s.len() <= 255, "short string exceeds 255 bytes");
        Self(Bytes::from_static(s.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str(&self) -> Result<&str, ProtocolError> {
        std::str::from_utf8(&self.0).map_err(|_| ProtocolError::InvalidUtf8)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<&str> for ShortStr {
    type Error = ProtocolError;

    fn try_from(s: &str) -> Result<Self, ProtocolError> {
        if s.len() > 255 {
            return Err(ProtocolError::ShortStrTooLong(s.len()));
        }
        Ok(Self(Bytes::copy_from_slice(s.as_bytes())))
    }
}

impl TryFrom<String> for ShortStr {
    type Error = ProtocolError;

    fn try_from(s: String) -> Result<Self, ProtocolError> {
        if s.len() > 255 {
            return Err(ProtocolError::ShortStrTooLong(s.len()));
        }
        Ok(Self(Bytes::from(s)))
    }
}

impl From<ShortStr> for Bytes {
    fn from(s: ShortStr) -> Bytes {
        s.0
    }
}

impl fmt::Debug for ShortStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "ShortStr({s:?})"),
            Err(_) => write!(f, "ShortStr({:?})", self.0),
        }
    }
}

impl fmt::Display for ShortStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => f.write_str(s),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

impl AmqpEncode for ShortStr {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.0.len() as u8);
        buf.extend_from_slice(&self.0);
    }
}

impl AmqpDecode for ShortStr {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 1)?;
        let len = buf.get_u8() as usize;
        ensure_remaining(buf, len)?;
        Ok(Self(buf.split_to(len)))
    }
}

// ---------------------------------------------------------------------------
// LongStr — AMQP long string / binary (u32 length prefix)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct LongStr(Bytes);

impl LongStr {
    pub fn new() -> Self {
        Self(Bytes::new())
    }

    pub fn from_static(s: &'static str) -> Self {
        Self(Bytes::from_static(s.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str(&self) -> Result<&str, ProtocolError> {
        std::str::from_utf8(&self.0).map_err(|_| ProtocolError::InvalidUtf8)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&str> for LongStr {
    fn from(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for LongStr {
    fn from(s: String) -> Self {
        Self(Bytes::from(s))
    }
}

impl From<Vec<u8>> for LongStr {
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl From<Bytes> for LongStr {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl From<LongStr> for Bytes {
    fn from(s: LongStr) -> Bytes {
        s.0
    }
}

impl fmt::Debug for LongStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "LongStr({s:?})"),
            Err(_) => write!(f, "LongStr(<{} bytes>)", self.0.len()),
        }
    }
}

impl AmqpEncode for LongStr {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(self.0.len() as u32);
        buf.extend_from_slice(&self.0);
    }
}

impl AmqpDecode for LongStr {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 4)?;
        let len = buf.get_u32() as usize;
        ensure_remaining(buf, len)?;
        Ok(Self(buf.split_to(len)))
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

pub(crate) fn ensure_remaining(buf: &Bytes, needed: usize) -> Result<(), ProtocolError> {
    if buf.remaining() < needed {
        Err(ProtocolError::InsufficientData {
            needed,
            available: buf.remaining(),
        })
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_str_roundtrip() {
        let s = ShortStr::try_from("hello").unwrap();
        let mut buf = BytesMut::new();
        s.encode(&mut buf);
        assert_eq!(buf[0], 5); // length prefix
        assert_eq!(&buf[1..], b"hello");

        let mut bytes = buf.freeze();
        let decoded = ShortStr::decode(&mut bytes).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn short_str_empty() {
        let s = ShortStr::new();
        let mut buf = BytesMut::new();
        s.encode(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0);

        let mut bytes = buf.freeze();
        let decoded = ShortStr::decode(&mut bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn short_str_max_length() {
        let long = "a".repeat(255);
        let s = ShortStr::try_from(long.as_str()).unwrap();
        assert_eq!(s.len(), 255);
    }

    #[test]
    fn short_str_too_long() {
        let long = "a".repeat(256);
        assert!(ShortStr::try_from(long.as_str()).is_err());
    }

    #[test]
    fn long_str_roundtrip() {
        let s = LongStr::from("hello world");
        let mut buf = BytesMut::new();
        s.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = LongStr::decode(&mut bytes).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn primitive_u16_roundtrip() {
        let val: u16 = 12345;
        let mut buf = BytesMut::new();
        val.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = u16::decode(&mut bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn primitive_u32_roundtrip() {
        let val: u32 = 0xDEAD_BEEF;
        let mut buf = BytesMut::new();
        val.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = u32::decode(&mut bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn insufficient_data() {
        let mut bytes = Bytes::from_static(&[0x00]);
        assert!(u16::decode(&mut bytes).is_err());
    }
}
