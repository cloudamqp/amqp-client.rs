use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::table::Table;
use crate::protocol::types::{AmqpDecode, AmqpEncode, ShortStr};

/// Queue.Declare (50, 10)
#[derive(Clone, Debug, PartialEq)]
pub struct Declare {
    pub queue: ShortStr,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub no_wait: bool,
    pub arguments: Table,
}

impl AmqpEncode for Declare {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        let flags = u8::from(self.passive)
            | (u8::from(self.durable) << 1)
            | (u8::from(self.exclusive) << 2)
            | (u8::from(self.auto_delete) << 3)
            | (u8::from(self.no_wait) << 4);
        buf.put_u8(flags);
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Declare {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        let queue = ShortStr::decode(buf)?;
        let flags = u8::decode(buf)?;
        let arguments = Table::decode(buf)?;
        Ok(Self {
            queue,
            passive: flags & 1 != 0,
            durable: flags & (1 << 1) != 0,
            exclusive: flags & (1 << 2) != 0,
            auto_delete: flags & (1 << 3) != 0,
            no_wait: flags & (1 << 4) != 0,
            arguments,
        })
    }
}

/// Queue.DeclareOk (50, 11)
#[derive(Clone, Debug, PartialEq)]
pub struct DeclareOk {
    pub queue: ShortStr,
    pub message_count: u32,
    pub consumer_count: u32,
}

impl AmqpEncode for DeclareOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.queue.encode(buf);
        self.message_count.encode(buf);
        self.consumer_count.encode(buf);
    }
}

impl AmqpDecode for DeclareOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            queue: ShortStr::decode(buf)?,
            message_count: u32::decode(buf)?,
            consumer_count: u32::decode(buf)?,
        })
    }
}

/// Queue.Bind (50, 20)
#[derive(Clone, Debug, PartialEq)]
pub struct Bind {
    pub queue: ShortStr,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub no_wait: bool,
    pub arguments: Table,
}

impl AmqpEncode for Bind {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        self.exchange.encode(buf);
        self.routing_key.encode(buf);
        buf.put_u8(u8::from(self.no_wait));
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Bind {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        Ok(Self {
            queue: ShortStr::decode(buf)?,
            exchange: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
            no_wait: u8::decode(buf)? != 0,
            arguments: Table::decode(buf)?,
        })
    }
}

// Queue.BindOk (50, 21) — no fields

/// Queue.Purge (50, 30)
#[derive(Clone, Debug, PartialEq)]
pub struct Purge {
    pub queue: ShortStr,
    pub no_wait: bool,
}

impl AmqpEncode for Purge {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        buf.put_u8(u8::from(self.no_wait));
    }
}

impl AmqpDecode for Purge {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        Ok(Self {
            queue: ShortStr::decode(buf)?,
            no_wait: u8::decode(buf)? != 0,
        })
    }
}

/// Queue.PurgeOk (50, 31)
#[derive(Clone, Debug, PartialEq)]
pub struct PurgeOk {
    pub message_count: u32,
}

impl AmqpEncode for PurgeOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.message_count.encode(buf);
    }
}

impl AmqpDecode for PurgeOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            message_count: u32::decode(buf)?,
        })
    }
}

/// Queue.Delete (50, 40)
#[derive(Clone, Debug, PartialEq)]
pub struct Delete {
    pub queue: ShortStr,
    pub if_unused: bool,
    pub if_empty: bool,
    pub no_wait: bool,
}

impl AmqpEncode for Delete {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        let flags = u8::from(self.if_unused)
            | (u8::from(self.if_empty) << 1)
            | (u8::from(self.no_wait) << 2);
        buf.put_u8(flags);
    }
}

impl AmqpDecode for Delete {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        let queue = ShortStr::decode(buf)?;
        let flags = u8::decode(buf)?;
        Ok(Self {
            queue,
            if_unused: flags & 1 != 0,
            if_empty: flags & (1 << 1) != 0,
            no_wait: flags & (1 << 2) != 0,
        })
    }
}

/// Queue.DeleteOk (50, 41)
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteOk {
    pub message_count: u32,
}

impl AmqpEncode for DeleteOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.message_count.encode(buf);
    }
}

impl AmqpDecode for DeleteOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            message_count: u32::decode(buf)?,
        })
    }
}

/// Queue.Unbind (50, 50)
#[derive(Clone, Debug, PartialEq)]
pub struct Unbind {
    pub queue: ShortStr,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub arguments: Table,
}

impl AmqpEncode for Unbind {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.queue.encode(buf);
        self.exchange.encode(buf);
        self.routing_key.encode(buf);
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Unbind {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        Ok(Self {
            queue: ShortStr::decode(buf)?,
            exchange: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
            arguments: Table::decode(buf)?,
        })
    }
}

// Queue.UnbindOk (50, 51) — no fields

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declare_roundtrip() {
        let decl = Declare {
            queue: ShortStr::try_from("test.queue").unwrap(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            no_wait: false,
            arguments: Table::new(),
        };
        let mut buf = BytesMut::new();
        decl.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Declare::decode(&mut bytes).unwrap();
        assert_eq!(decoded, decl);
    }

    #[test]
    fn declare_ok_roundtrip() {
        let ok = DeclareOk {
            queue: ShortStr::try_from("test.queue").unwrap(),
            message_count: 42,
            consumer_count: 3,
        };
        let mut buf = BytesMut::new();
        ok.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = DeclareOk::decode(&mut bytes).unwrap();
        assert_eq!(decoded, ok);
    }

    #[test]
    fn bind_roundtrip() {
        let bind = Bind {
            queue: ShortStr::try_from("q").unwrap(),
            exchange: ShortStr::try_from("ex").unwrap(),
            routing_key: ShortStr::try_from("rk").unwrap(),
            no_wait: false,
            arguments: Table::new(),
        };
        let mut buf = BytesMut::new();
        bind.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Bind::decode(&mut bytes).unwrap();
        assert_eq!(decoded, bind);
    }

    #[test]
    fn delete_roundtrip() {
        let del = Delete {
            queue: ShortStr::try_from("q").unwrap(),
            if_unused: true,
            if_empty: true,
            no_wait: false,
        };
        let mut buf = BytesMut::new();
        del.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Delete::decode(&mut bytes).unwrap();
        assert_eq!(decoded, del);
    }

    #[test]
    fn unbind_roundtrip() {
        let unbind = Unbind {
            queue: ShortStr::try_from("q").unwrap(),
            exchange: ShortStr::try_from("ex").unwrap(),
            routing_key: ShortStr::try_from("rk").unwrap(),
            arguments: Table::new(),
        };
        let mut buf = BytesMut::new();
        unbind.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Unbind::decode(&mut bytes).unwrap();
        assert_eq!(decoded, unbind);
    }
}
