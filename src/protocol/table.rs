use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::types::{AmqpDecode, AmqpEncode, LongStr, ShortStr, ensure_remaining};
use crate::error::ProtocolError;

/// AMQP field table: an ordered map of short-string keys to typed values.
pub type Table = BTreeMap<ShortStr, FieldValue>;

/// A typed value within an AMQP field table.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Boolean(bool),          // 't'
    ShortShortInt(i8),      // 'b'
    ShortShortUint(u8),     // 'B'
    ShortInt(i16),          // 's'
    ShortUint(u16),         // 'u'
    LongInt(i32),           // 'I'
    LongUint(u32),          // 'i'
    LongLongInt(i64),       // 'l'
    Float(f32),             // 'f'
    Double(f64),            // 'd'
    Decimal(u8, u32),       // 'D' (scale, value)
    LongStr(LongStr),       // 'S'
    ByteArray(Bytes),       // 'x'
    Timestamp(u64),         // 'T'
    Table(Table),           // 'F'
    Array(Vec<FieldValue>), // 'A'
    Void,                   // 'V'
}

impl AmqpEncode for Table {
    fn encode(&self, buf: &mut BytesMut) {
        let size_pos = buf.len();
        buf.put_u32(0); // placeholder
        let start = buf.len();
        for (key, value) in self {
            key.encode(buf);
            value.encode(buf);
        }
        let size = (buf.len() - start) as u32;
        buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
    }
}

impl AmqpDecode for Table {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 4)?;
        let size = buf.get_u32() as usize;
        ensure_remaining(buf, size)?;
        let mut table_buf = buf.split_to(size);
        let mut table = BTreeMap::new();
        while table_buf.has_remaining() {
            let key = ShortStr::decode(&mut table_buf)?;
            let value = FieldValue::decode(&mut table_buf)?;
            table.insert(key, value);
        }
        Ok(table)
    }
}

impl AmqpEncode for FieldValue {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Boolean(v) => {
                buf.put_u8(b't');
                buf.put_u8(u8::from(*v));
            }
            Self::ShortShortInt(v) => {
                buf.put_u8(b'b');
                buf.put_i8(*v);
            }
            Self::ShortShortUint(v) => {
                buf.put_u8(b'B');
                buf.put_u8(*v);
            }
            Self::ShortInt(v) => {
                buf.put_u8(b's');
                buf.put_i16(*v);
            }
            Self::ShortUint(v) => {
                buf.put_u8(b'u');
                buf.put_u16(*v);
            }
            Self::LongInt(v) => {
                buf.put_u8(b'I');
                buf.put_i32(*v);
            }
            Self::LongUint(v) => {
                buf.put_u8(b'i');
                buf.put_u32(*v);
            }
            Self::LongLongInt(v) => {
                buf.put_u8(b'l');
                buf.put_i64(*v);
            }
            Self::Float(v) => {
                buf.put_u8(b'f');
                buf.put_f32(*v);
            }
            Self::Double(v) => {
                buf.put_u8(b'd');
                buf.put_f64(*v);
            }
            Self::Decimal(scale, value) => {
                buf.put_u8(b'D');
                buf.put_u8(*scale);
                buf.put_u32(*value);
            }
            Self::LongStr(v) => {
                buf.put_u8(b'S');
                v.encode(buf);
            }
            Self::ByteArray(v) => {
                buf.put_u8(b'x');
                buf.put_u32(v.len() as u32);
                buf.extend_from_slice(v);
            }
            Self::Timestamp(v) => {
                buf.put_u8(b'T');
                buf.put_u64(*v);
            }
            Self::Table(v) => {
                buf.put_u8(b'F');
                v.encode(buf);
            }
            Self::Array(v) => {
                buf.put_u8(b'A');
                let size_pos = buf.len();
                buf.put_i32(0); // placeholder
                let start = buf.len();
                for item in v {
                    item.encode(buf);
                }
                let size = (buf.len() - start) as i32;
                buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
            }
            Self::Void => {
                buf.put_u8(b'V');
            }
        }
    }
}

impl AmqpDecode for FieldValue {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 1)?;
        let tag = buf.get_u8();
        match tag {
            b't' => {
                ensure_remaining(buf, 1)?;
                Ok(Self::Boolean(buf.get_u8() != 0))
            }
            b'b' => {
                ensure_remaining(buf, 1)?;
                Ok(Self::ShortShortInt(buf.get_i8()))
            }
            b'B' => {
                ensure_remaining(buf, 1)?;
                Ok(Self::ShortShortUint(buf.get_u8()))
            }
            b's' => {
                ensure_remaining(buf, 2)?;
                Ok(Self::ShortInt(buf.get_i16()))
            }
            b'u' => {
                ensure_remaining(buf, 2)?;
                Ok(Self::ShortUint(buf.get_u16()))
            }
            b'I' => {
                ensure_remaining(buf, 4)?;
                Ok(Self::LongInt(buf.get_i32()))
            }
            b'i' => {
                ensure_remaining(buf, 4)?;
                Ok(Self::LongUint(buf.get_u32()))
            }
            b'l' => {
                ensure_remaining(buf, 8)?;
                Ok(Self::LongLongInt(buf.get_i64()))
            }
            b'f' => {
                ensure_remaining(buf, 4)?;
                Ok(Self::Float(buf.get_f32()))
            }
            b'd' => {
                ensure_remaining(buf, 8)?;
                Ok(Self::Double(buf.get_f64()))
            }
            b'D' => {
                ensure_remaining(buf, 5)?;
                let scale = buf.get_u8();
                let value = buf.get_u32();
                Ok(Self::Decimal(scale, value))
            }
            b'S' => Ok(Self::LongStr(LongStr::decode(buf)?)),
            b'x' => {
                ensure_remaining(buf, 4)?;
                let len = buf.get_u32() as usize;
                ensure_remaining(buf, len)?;
                Ok(Self::ByteArray(buf.split_to(len)))
            }
            b'T' => {
                ensure_remaining(buf, 8)?;
                Ok(Self::Timestamp(buf.get_u64()))
            }
            b'F' => Ok(Self::Table(Table::decode(buf)?)),
            b'A' => {
                ensure_remaining(buf, 4)?;
                let size = buf.get_i32() as usize;
                ensure_remaining(buf, size)?;
                let mut arr_buf = buf.split_to(size);
                let mut arr = Vec::new();
                while arr_buf.has_remaining() {
                    arr.push(FieldValue::decode(&mut arr_buf)?);
                }
                Ok(Self::Array(arr))
            }
            b'V' => Ok(Self::Void),
            _ => Err(ProtocolError::InvalidFieldType(tag)),
        }
    }
}

// Convenience conversions
impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<i32> for FieldValue {
    fn from(v: i32) -> Self {
        Self::LongInt(v)
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        Self::LongLongInt(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        Self::LongStr(LongStr::from(v))
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        Self::LongStr(LongStr::from(v))
    }
}

impl From<Table> for FieldValue {
    fn from(v: Table) -> Self {
        Self::Table(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_roundtrip() {
        let table = Table::new();
        let mut buf = BytesMut::new();
        table.encode(&mut buf);
        assert_eq!(buf.len(), 4); // just the u32 size (0)

        let mut bytes = buf.freeze();
        let decoded = Table::decode(&mut bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn table_with_string() {
        let mut table = Table::new();
        table.insert(
            ShortStr::try_from("key").unwrap(),
            FieldValue::LongStr(LongStr::from("value")),
        );

        let mut buf = BytesMut::new();
        table.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Table::decode(&mut bytes).unwrap();
        assert_eq!(decoded, table);
    }

    #[test]
    fn table_with_various_types() {
        let mut table = Table::new();
        table.insert(
            ShortStr::try_from("bool").unwrap(),
            FieldValue::Boolean(true),
        );
        table.insert(ShortStr::try_from("int").unwrap(), FieldValue::LongInt(42));
        table.insert(
            ShortStr::try_from("str").unwrap(),
            FieldValue::LongStr(LongStr::from("hello")),
        );
        table.insert(ShortStr::try_from("void").unwrap(), FieldValue::Void);

        let mut buf = BytesMut::new();
        table.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Table::decode(&mut bytes).unwrap();
        assert_eq!(decoded, table);
    }

    #[test]
    fn nested_table() {
        let mut inner = Table::new();
        inner.insert(ShortStr::try_from("x").unwrap(), FieldValue::LongInt(1));

        let mut outer = Table::new();
        outer.insert(
            ShortStr::try_from("nested").unwrap(),
            FieldValue::Table(inner),
        );

        let mut buf = BytesMut::new();
        outer.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Table::decode(&mut bytes).unwrap();
        assert_eq!(decoded, outer);
    }

    #[test]
    fn array_field() {
        let arr = FieldValue::Array(vec![
            FieldValue::LongInt(1),
            FieldValue::LongInt(2),
            FieldValue::LongStr(LongStr::from("three")),
        ]);

        let mut buf = BytesMut::new();
        arr.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = FieldValue::decode(&mut bytes).unwrap();
        assert_eq!(decoded, arr);
    }

    #[test]
    fn decimal_field() {
        let val = FieldValue::Decimal(2, 12345);
        let mut buf = BytesMut::new();
        val.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = FieldValue::decode(&mut bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn byte_array_field() {
        let val = FieldValue::ByteArray(Bytes::from_static(b"\x01\x02\x03"));
        let mut buf = BytesMut::new();
        val.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = FieldValue::decode(&mut bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn invalid_field_type() {
        let mut bytes = Bytes::from_static(&[0xFF]);
        assert!(FieldValue::decode(&mut bytes).is_err());
    }
}
