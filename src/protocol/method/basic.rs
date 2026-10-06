use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::table::Table;
use crate::protocol::types::{AmqpDecode, AmqpEncode, ShortStr};

/// Basic.Qos (60, 10)
#[derive(Clone, Debug, PartialEq)]
pub struct Qos {
    pub prefetch_size: u32,
    pub prefetch_count: u16,
    pub global: bool,
}

impl AmqpEncode for Qos {
    fn encode(&self, buf: &mut BytesMut) {
        self.prefetch_size.encode(buf);
        self.prefetch_count.encode(buf);
        buf.put_u8(u8::from(self.global));
    }
}

impl AmqpDecode for Qos {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            prefetch_size: u32::decode(buf)?,
            prefetch_count: u16::decode(buf)?,
            global: u8::decode(buf)? != 0,
        })
    }
}

// Basic.QosOk (60, 11) — no fields

/// Basic.Consume (60, 20)
#[derive(Clone, Debug, PartialEq)]
pub struct Consume {
    pub queue: ShortStr,
    pub consumer_tag: ShortStr,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub no_wait: bool,
    pub arguments: Table,
}

impl AmqpEncode for Consume {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        self.consumer_tag.encode(buf);
        let flags = u8::from(self.no_local)
            | (u8::from(self.no_ack) << 1)
            | (u8::from(self.exclusive) << 2)
            | (u8::from(self.no_wait) << 3);
        buf.put_u8(flags);
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Consume {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        let queue = ShortStr::decode(buf)?;
        let consumer_tag = ShortStr::decode(buf)?;
        let flags = u8::decode(buf)?;
        let arguments = Table::decode(buf)?;
        Ok(Self {
            queue,
            consumer_tag,
            no_local: flags & 1 != 0,
            no_ack: flags & (1 << 1) != 0,
            exclusive: flags & (1 << 2) != 0,
            no_wait: flags & (1 << 3) != 0,
            arguments,
        })
    }
}

/// Basic.ConsumeOk (60, 21)
#[derive(Clone, Debug, PartialEq)]
pub struct ConsumeOk {
    pub consumer_tag: ShortStr,
}

impl AmqpEncode for ConsumeOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.consumer_tag.encode(buf);
    }
}

impl AmqpDecode for ConsumeOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_tag: ShortStr::decode(buf)?,
        })
    }
}

/// Basic.Cancel (60, 30)
#[derive(Clone, Debug, PartialEq)]
pub struct Cancel {
    pub consumer_tag: ShortStr,
    pub no_wait: bool,
}

impl AmqpEncode for Cancel {
    fn encode(&self, buf: &mut BytesMut) {
        self.consumer_tag.encode(buf);
        buf.put_u8(u8::from(self.no_wait));
    }
}

impl AmqpDecode for Cancel {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_tag: ShortStr::decode(buf)?,
            no_wait: u8::decode(buf)? != 0,
        })
    }
}

/// Basic.CancelOk (60, 31)
#[derive(Clone, Debug, PartialEq)]
pub struct CancelOk {
    pub consumer_tag: ShortStr,
}

impl AmqpEncode for CancelOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.consumer_tag.encode(buf);
    }
}

impl AmqpDecode for CancelOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_tag: ShortStr::decode(buf)?,
        })
    }
}

/// Basic.Publish (60, 40)
#[derive(Clone, Debug, PartialEq)]
pub struct Publish {
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub mandatory: bool,
    pub immediate: bool,
}

impl AmqpEncode for Publish {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.exchange.encode(buf);
        self.routing_key.encode(buf);
        let flags = u8::from(self.mandatory) | (u8::from(self.immediate) << 1);
        buf.put_u8(flags);
    }
}

impl AmqpDecode for Publish {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        let exchange = ShortStr::decode(buf)?;
        let routing_key = ShortStr::decode(buf)?;
        let flags = u8::decode(buf)?;
        Ok(Self {
            exchange,
            routing_key,
            mandatory: flags & 1 != 0,
            immediate: flags & (1 << 1) != 0,
        })
    }
}

/// Basic.Return (60, 50)
#[derive(Clone, Debug, PartialEq)]
pub struct Return {
    pub reply_code: u16,
    pub reply_text: ShortStr,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
}

impl AmqpEncode for Return {
    fn encode(&self, buf: &mut BytesMut) {
        self.reply_code.encode(buf);
        self.reply_text.encode(buf);
        self.exchange.encode(buf);
        self.routing_key.encode(buf);
    }
}

impl AmqpDecode for Return {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            reply_code: u16::decode(buf)?,
            reply_text: ShortStr::decode(buf)?,
            exchange: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
        })
    }
}

/// Basic.Deliver (60, 60)
#[derive(Clone, Debug, PartialEq)]
pub struct Deliver {
    pub consumer_tag: ShortStr,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
}

impl AmqpEncode for Deliver {
    fn encode(&self, buf: &mut BytesMut) {
        self.consumer_tag.encode(buf);
        self.delivery_tag.encode(buf);
        buf.put_u8(u8::from(self.redelivered));
        self.exchange.encode(buf);
        self.routing_key.encode(buf);
    }
}

impl AmqpDecode for Deliver {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_tag: ShortStr::decode(buf)?,
            delivery_tag: u64::decode(buf)?,
            redelivered: u8::decode(buf)? != 0,
            exchange: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
        })
    }
}

/// Basic.Get (60, 70)
#[derive(Clone, Debug, PartialEq)]
pub struct Get {
    pub queue: ShortStr,
    pub no_ack: bool,
}

impl AmqpEncode for Get {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        buf.put_u8(u8::from(self.no_ack));
    }
}

impl AmqpDecode for Get {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        Ok(Self {
            queue: ShortStr::decode(buf)?,
            no_ack: u8::decode(buf)? != 0,
        })
    }
}

/// Basic.GetOk (60, 71)
#[derive(Clone, Debug, PartialEq)]
pub struct GetOk {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub message_count: u32,
}

impl AmqpEncode for GetOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.delivery_tag.encode(buf);
        buf.put_u8(u8::from(self.redelivered));
        self.exchange.encode(buf);
        self.routing_key.encode(buf);
        self.message_count.encode(buf);
    }
}

impl AmqpDecode for GetOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            delivery_tag: u64::decode(buf)?,
            redelivered: u8::decode(buf)? != 0,
            exchange: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
            message_count: u32::decode(buf)?,
        })
    }
}

// Basic.GetEmpty (60, 72) — only reserved short string, handled in MethodFrame

/// Basic.Ack (60, 80)
#[derive(Clone, Debug, PartialEq)]
pub struct Ack {
    pub delivery_tag: u64,
    pub multiple: bool,
}

impl AmqpEncode for Ack {
    fn encode(&self, buf: &mut BytesMut) {
        self.delivery_tag.encode(buf);
        buf.put_u8(u8::from(self.multiple));
    }
}

impl AmqpDecode for Ack {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            delivery_tag: u64::decode(buf)?,
            multiple: u8::decode(buf)? != 0,
        })
    }
}

/// Basic.Reject (60, 90)
#[derive(Clone, Debug, PartialEq)]
pub struct Reject {
    pub delivery_tag: u64,
    pub requeue: bool,
}

impl AmqpEncode for Reject {
    fn encode(&self, buf: &mut BytesMut) {
        self.delivery_tag.encode(buf);
        buf.put_u8(u8::from(self.requeue));
    }
}

impl AmqpDecode for Reject {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            delivery_tag: u64::decode(buf)?,
            requeue: u8::decode(buf)? != 0,
        })
    }
}

/// Basic.RecoverAsync (60, 100)
#[derive(Clone, Debug, PartialEq)]
pub struct RecoverAsync {
    pub requeue: bool,
}

impl AmqpEncode for RecoverAsync {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(u8::from(self.requeue));
    }
}

impl AmqpDecode for RecoverAsync {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            requeue: u8::decode(buf)? != 0,
        })
    }
}

/// Basic.Recover (60, 110)
#[derive(Clone, Debug, PartialEq)]
pub struct Recover {
    pub requeue: bool,
}

impl AmqpEncode for Recover {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(u8::from(self.requeue));
    }
}

impl AmqpDecode for Recover {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            requeue: u8::decode(buf)? != 0,
        })
    }
}

// Basic.RecoverOk (60, 111) — no fields

/// Basic.Nack (60, 120) — RabbitMQ extension
#[derive(Clone, Debug, PartialEq)]
pub struct Nack {
    pub delivery_tag: u64,
    pub multiple: bool,
    pub requeue: bool,
}

impl AmqpEncode for Nack {
    fn encode(&self, buf: &mut BytesMut) {
        self.delivery_tag.encode(buf);
        let flags = u8::from(self.multiple) | (u8::from(self.requeue) << 1);
        buf.put_u8(flags);
    }
}

impl AmqpDecode for Nack {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        let delivery_tag = u64::decode(buf)?;
        let flags = u8::decode(buf)?;
        Ok(Self {
            delivery_tag,
            multiple: flags & 1 != 0,
            requeue: flags & (1 << 1) != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_roundtrip() {
        let pub_ = Publish {
            exchange: ShortStr::try_from("amq.topic").unwrap(),
            routing_key: ShortStr::try_from("test.key").unwrap(),
            mandatory: true,
            immediate: false,
        };
        let mut buf = BytesMut::new();
        pub_.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Publish::decode(&mut bytes).unwrap();
        assert_eq!(decoded, pub_);
    }

    #[test]
    fn deliver_roundtrip() {
        let deliver = Deliver {
            consumer_tag: ShortStr::try_from("ctag1").unwrap(),
            delivery_tag: 42,
            redelivered: false,
            exchange: ShortStr::try_from("amq.direct").unwrap(),
            routing_key: ShortStr::try_from("key").unwrap(),
        };
        let mut buf = BytesMut::new();
        deliver.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Deliver::decode(&mut bytes).unwrap();
        assert_eq!(decoded, deliver);
    }

    #[test]
    fn consume_roundtrip() {
        let consume = Consume {
            queue: ShortStr::try_from("q").unwrap(),
            consumer_tag: ShortStr::try_from("tag").unwrap(),
            no_local: false,
            no_ack: true,
            exclusive: false,
            no_wait: false,
            arguments: Table::new(),
        };
        let mut buf = BytesMut::new();
        consume.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Consume::decode(&mut bytes).unwrap();
        assert_eq!(decoded, consume);
    }

    #[test]
    fn nack_roundtrip() {
        let nack = Nack {
            delivery_tag: 100,
            multiple: true,
            requeue: true,
        };
        let mut buf = BytesMut::new();
        nack.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Nack::decode(&mut bytes).unwrap();
        assert_eq!(decoded, nack);
    }

    #[test]
    fn get_ok_roundtrip() {
        let get_ok = GetOk {
            delivery_tag: 7,
            redelivered: true,
            exchange: ShortStr::try_from("ex").unwrap(),
            routing_key: ShortStr::try_from("rk").unwrap(),
            message_count: 99,
        };
        let mut buf = BytesMut::new();
        get_ok.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = GetOk::decode(&mut bytes).unwrap();
        assert_eq!(decoded, get_ok);
    }

    #[test]
    fn qos_roundtrip() {
        let qos = Qos {
            prefetch_size: 0,
            prefetch_count: 10,
            global: false,
        };
        let mut buf = BytesMut::new();
        qos.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Qos::decode(&mut bytes).unwrap();
        assert_eq!(decoded, qos);
    }

    #[test]
    fn ack_roundtrip() {
        let ack = Ack {
            delivery_tag: 5,
            multiple: true,
        };
        let mut buf = BytesMut::new();
        ack.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Ack::decode(&mut bytes).unwrap();
        assert_eq!(decoded, ack);
    }
}
