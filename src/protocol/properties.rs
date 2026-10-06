use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::table::Table;
use super::types::{AmqpDecode, AmqpEncode, ShortStr, ensure_remaining};
use crate::error::{Error, ProtocolError};

bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct PropertyFlags: u16 {
        const CONTENT_TYPE     = 1 << 15;
        const CONTENT_ENCODING = 1 << 14;
        const HEADERS          = 1 << 13;
        const DELIVERY_MODE    = 1 << 12;
        const PRIORITY         = 1 << 11;
        const CORRELATION_ID   = 1 << 10;
        const REPLY_TO         = 1 << 9;
        const EXPIRATION       = 1 << 8;
        const MESSAGE_ID       = 1 << 7;
        const TIMESTAMP        = 1 << 6;
        const TYPE_FIELD       = 1 << 5;
        const USER_ID          = 1 << 4;
        const APP_ID           = 1 << 3;
    }
}

/// AMQP Basic message properties.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BasicProperties {
    pub content_type: Option<ShortStr>,
    pub content_encoding: Option<ShortStr>,
    pub headers: Option<Table>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<ShortStr>,
    pub reply_to: Option<ShortStr>,
    pub expiration: Option<ShortStr>,
    pub message_id: Option<ShortStr>,
    pub timestamp: Option<u64>,
    pub type_field: Option<ShortStr>,
    pub user_id: Option<ShortStr>,
    pub app_id: Option<ShortStr>,
}

impl BasicProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn builder() -> BasicPropertiesBuilder {
        BasicPropertiesBuilder::default()
    }

    fn flags(&self) -> PropertyFlags {
        let mut flags = PropertyFlags::empty();
        if self.content_type.is_some() {
            flags |= PropertyFlags::CONTENT_TYPE;
        }
        if self.content_encoding.is_some() {
            flags |= PropertyFlags::CONTENT_ENCODING;
        }
        if self.headers.is_some() {
            flags |= PropertyFlags::HEADERS;
        }
        if self.delivery_mode.is_some() {
            flags |= PropertyFlags::DELIVERY_MODE;
        }
        if self.priority.is_some() {
            flags |= PropertyFlags::PRIORITY;
        }
        if self.correlation_id.is_some() {
            flags |= PropertyFlags::CORRELATION_ID;
        }
        if self.reply_to.is_some() {
            flags |= PropertyFlags::REPLY_TO;
        }
        if self.expiration.is_some() {
            flags |= PropertyFlags::EXPIRATION;
        }
        if self.message_id.is_some() {
            flags |= PropertyFlags::MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            flags |= PropertyFlags::TIMESTAMP;
        }
        if self.type_field.is_some() {
            flags |= PropertyFlags::TYPE_FIELD;
        }
        if self.user_id.is_some() {
            flags |= PropertyFlags::USER_ID;
        }
        if self.app_id.is_some() {
            flags |= PropertyFlags::APP_ID;
        }
        flags
    }
}

impl AmqpEncode for BasicProperties {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.flags().bits());
        if let Some(ref v) = self.content_type {
            v.encode(buf);
        }
        if let Some(ref v) = self.content_encoding {
            v.encode(buf);
        }
        if let Some(ref v) = self.headers {
            v.encode(buf);
        }
        if let Some(v) = self.delivery_mode {
            buf.put_u8(v);
        }
        if let Some(v) = self.priority {
            buf.put_u8(v);
        }
        if let Some(ref v) = self.correlation_id {
            v.encode(buf);
        }
        if let Some(ref v) = self.reply_to {
            v.encode(buf);
        }
        if let Some(ref v) = self.expiration {
            v.encode(buf);
        }
        if let Some(ref v) = self.message_id {
            v.encode(buf);
        }
        if let Some(v) = self.timestamp {
            buf.put_u64(v);
        }
        if let Some(ref v) = self.type_field {
            v.encode(buf);
        }
        if let Some(ref v) = self.user_id {
            v.encode(buf);
        }
        if let Some(ref v) = self.app_id {
            v.encode(buf);
        }
    }
}

impl AmqpDecode for BasicProperties {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 2)?;
        let flags = PropertyFlags::from_bits_truncate(buf.get_u16());

        let content_type = if flags.contains(PropertyFlags::CONTENT_TYPE) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let content_encoding = if flags.contains(PropertyFlags::CONTENT_ENCODING) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let headers = if flags.contains(PropertyFlags::HEADERS) {
            Some(Table::decode(buf)?)
        } else {
            None
        };
        let delivery_mode = if flags.contains(PropertyFlags::DELIVERY_MODE) {
            Some(u8::decode(buf)?)
        } else {
            None
        };
        let priority = if flags.contains(PropertyFlags::PRIORITY) {
            Some(u8::decode(buf)?)
        } else {
            None
        };
        let correlation_id = if flags.contains(PropertyFlags::CORRELATION_ID) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let reply_to = if flags.contains(PropertyFlags::REPLY_TO) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let expiration = if flags.contains(PropertyFlags::EXPIRATION) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let message_id = if flags.contains(PropertyFlags::MESSAGE_ID) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let timestamp = if flags.contains(PropertyFlags::TIMESTAMP) {
            Some(u64::decode(buf)?)
        } else {
            None
        };
        let type_field = if flags.contains(PropertyFlags::TYPE_FIELD) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let user_id = if flags.contains(PropertyFlags::USER_ID) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };
        let app_id = if flags.contains(PropertyFlags::APP_ID) {
            Some(ShortStr::decode(buf)?)
        } else {
            None
        };

        Ok(Self {
            content_type,
            content_encoding,
            headers,
            delivery_mode,
            priority,
            correlation_id,
            reply_to,
            expiration,
            message_id,
            timestamp,
            type_field,
            user_id,
            app_id,
        })
    }
}

/// Builder for `BasicProperties`.
///
/// Setters that take a string convert to AMQP `ShortStr` (≤255 bytes); if a
/// value is too long the error is captured and surfaced from [`build`].
/// Subsequent setter calls become no-ops once an error is recorded, so the
/// fluent chain still composes.
///
/// [`build`]: Self::build
pub struct BasicPropertiesBuilder(Result<BasicProperties, Error>);

impl Default for BasicPropertiesBuilder {
    fn default() -> Self {
        Self(Ok(BasicProperties::default()))
    }
}

impl BasicPropertiesBuilder {
    fn set_short<F>(mut self, v: &str, assign: F) -> Self
    where
        F: FnOnce(&mut BasicProperties, ShortStr),
    {
        if let Ok(props) = self.0.as_mut() {
            match ShortStr::try_from(v) {
                Ok(s) => assign(props, s),
                Err(e) => self.0 = Err(Error::Protocol(e)),
            }
        }
        self
    }

    fn set<F>(mut self, assign: F) -> Self
    where
        F: FnOnce(&mut BasicProperties),
    {
        if let Ok(props) = self.0.as_mut() {
            assign(props);
        }
        self
    }

    pub fn content_type(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.content_type = Some(s))
    }

    pub fn content_encoding(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.content_encoding = Some(s))
    }

    pub fn headers(self, v: Table) -> Self {
        self.set(|p| p.headers = Some(v))
    }

    pub fn delivery_mode(self, v: u8) -> Self {
        self.set(|p| p.delivery_mode = Some(v))
    }

    pub fn persistent(self) -> Self {
        self.delivery_mode(2)
    }

    pub fn transient(self) -> Self {
        self.delivery_mode(1)
    }

    pub fn priority(self, v: u8) -> Self {
        self.set(|p| p.priority = Some(v))
    }

    pub fn correlation_id(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.correlation_id = Some(s))
    }

    pub fn reply_to(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.reply_to = Some(s))
    }

    pub fn expiration(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.expiration = Some(s))
    }

    pub fn message_id(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.message_id = Some(s))
    }

    pub fn timestamp(self, v: u64) -> Self {
        self.set(|p| p.timestamp = Some(v))
    }

    pub fn type_field(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.type_field = Some(s))
    }

    pub fn user_id(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.user_id = Some(s))
    }

    pub fn app_id(self, v: &str) -> Self {
        self.set_short(v, |p, s| p.app_id = Some(s))
    }

    /// Finalize the builder.
    ///
    /// Returns `Err(Error::Protocol(ShortStrTooLong))` if any string
    /// setter was given a value over 255 bytes.
    pub fn build(self) -> Result<BasicProperties, Error> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_properties_roundtrip() {
        let props = BasicProperties::new();
        let mut buf = BytesMut::new();
        props.encode(&mut buf);
        assert_eq!(buf.len(), 2); // just the flags u16

        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn properties_with_all_fields() {
        let props = BasicProperties::builder()
            .content_type("application/json")
            .content_encoding("utf-8")
            .delivery_mode(2)
            .priority(5)
            .correlation_id("corr-123")
            .reply_to("reply.queue")
            .expiration("60000")
            .message_id("msg-456")
            .timestamp(1234567890)
            .type_field("test.message")
            .user_id("guest")
            .app_id("test-app")
            .build()
            .unwrap();

        let mut buf = BytesMut::new();
        props.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn properties_with_headers() {
        let mut headers = Table::new();
        headers.insert(
            ShortStr::try_from("x-custom").unwrap(),
            super::super::table::FieldValue::LongStr(crate::protocol::types::LongStr::from(
                "value",
            )),
        );

        let props = BasicProperties::builder().headers(headers).build().unwrap();

        let mut buf = BytesMut::new();
        props.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn properties_partial_fields() {
        let props = BasicProperties::builder()
            .delivery_mode(2)
            .correlation_id("abc")
            .build()
            .unwrap();

        let mut buf = BytesMut::new();
        props.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(decoded.delivery_mode, Some(2));
        assert_eq!(
            decoded.correlation_id,
            Some(ShortStr::try_from("abc").unwrap())
        );
        assert_eq!(decoded.content_type, None);
        assert_eq!(decoded.timestamp, None);
    }
}
