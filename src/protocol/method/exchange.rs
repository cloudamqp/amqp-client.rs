use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::table::Table;
use crate::protocol::types::{AmqpDecode, AmqpEncode, ShortStr};

/// Exchange.Declare (40, 10)
#[derive(Clone, Debug, PartialEq)]
pub struct Declare {
    pub exchange: ShortStr,
    pub kind: ShortStr,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub no_wait: bool,
    pub arguments: Table,
}

impl AmqpEncode for Declare {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.exchange.encode(buf);
        self.kind.encode(buf);
        let flags = u8::from(self.passive)
            | (u8::from(self.durable) << 1)
            | (u8::from(self.auto_delete) << 2)
            | (u8::from(self.internal) << 3)
            | (u8::from(self.no_wait) << 4);
        buf.put_u8(flags);
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Declare {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        let exchange = ShortStr::decode(buf)?;
        let kind = ShortStr::decode(buf)?;
        let flags = u8::decode(buf)?;
        let arguments = Table::decode(buf)?;
        Ok(Self {
            exchange,
            kind,
            passive: flags & 1 != 0,
            durable: flags & (1 << 1) != 0,
            auto_delete: flags & (1 << 2) != 0,
            internal: flags & (1 << 3) != 0,
            no_wait: flags & (1 << 4) != 0,
            arguments,
        })
    }
}

// Exchange.DeclareOk (40, 11) — no fields

/// Exchange.Delete (40, 20)
#[derive(Clone, Debug, PartialEq)]
pub struct Delete {
    pub exchange: ShortStr,
    pub if_unused: bool,
    pub no_wait: bool,
}

impl AmqpEncode for Delete {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.exchange.encode(buf);
        let flags = u8::from(self.if_unused) | (u8::from(self.no_wait) << 1);
        buf.put_u8(flags);
    }
}

impl AmqpDecode for Delete {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        let exchange = ShortStr::decode(buf)?;
        let flags = u8::decode(buf)?;
        Ok(Self {
            exchange,
            if_unused: flags & 1 != 0,
            no_wait: flags & (1 << 1) != 0,
        })
    }
}

// Exchange.DeleteOk (40, 21) — no fields

/// Exchange.Bind (40, 30) — RabbitMQ extension
#[derive(Clone, Debug, PartialEq)]
pub struct Bind {
    pub destination: ShortStr,
    pub source: ShortStr,
    pub routing_key: ShortStr,
    pub no_wait: bool,
    pub arguments: Table,
}

impl AmqpEncode for Bind {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.destination.encode(buf);
        self.source.encode(buf);
        self.routing_key.encode(buf);
        buf.put_u8(u8::from(self.no_wait));
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Bind {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        Ok(Self {
            destination: ShortStr::decode(buf)?,
            source: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
            no_wait: u8::decode(buf)? != 0,
            arguments: Table::decode(buf)?,
        })
    }
}

// Exchange.BindOk (40, 31) — no fields

/// Exchange.Unbind (40, 40) — RabbitMQ extension
#[derive(Clone, Debug, PartialEq)]
pub struct Unbind {
    pub destination: ShortStr,
    pub source: ShortStr,
    pub routing_key: ShortStr,
    pub no_wait: bool,
    pub arguments: Table,
}

impl AmqpEncode for Unbind {
    fn encode(&self, buf: &mut BytesMut) {
        0u16.encode(buf); // reserved-1
        self.destination.encode(buf);
        self.source.encode(buf);
        self.routing_key.encode(buf);
        buf.put_u8(u8::from(self.no_wait));
        self.arguments.encode(buf);
    }
}

impl AmqpDecode for Unbind {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        u16::decode(buf)?; // reserved-1
        Ok(Self {
            destination: ShortStr::decode(buf)?,
            source: ShortStr::decode(buf)?,
            routing_key: ShortStr::decode(buf)?,
            no_wait: u8::decode(buf)? != 0,
            arguments: Table::decode(buf)?,
        })
    }
}

// Exchange.UnbindOk (40, 51) — no fields

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declare_roundtrip() {
        let decl = Declare {
            exchange: ShortStr::try_from("test.exchange").unwrap(),
            kind: ShortStr::try_from("topic").unwrap(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
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
    fn declare_all_flags() {
        let decl = Declare {
            exchange: ShortStr::try_from("x").unwrap(),
            kind: ShortStr::try_from("direct").unwrap(),
            passive: true,
            durable: true,
            auto_delete: true,
            internal: true,
            no_wait: true,
            arguments: Table::new(),
        };
        let mut buf = BytesMut::new();
        decl.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Declare::decode(&mut bytes).unwrap();
        assert_eq!(decoded, decl);
    }

    #[test]
    fn delete_roundtrip() {
        let del = Delete {
            exchange: ShortStr::try_from("test.exchange").unwrap(),
            if_unused: true,
            no_wait: false,
        };
        let mut buf = BytesMut::new();
        del.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Delete::decode(&mut bytes).unwrap();
        assert_eq!(decoded, del);
    }

    #[test]
    fn bind_roundtrip() {
        let bind = Bind {
            destination: ShortStr::try_from("dest").unwrap(),
            source: ShortStr::try_from("src").unwrap(),
            routing_key: ShortStr::try_from("key.#").unwrap(),
            no_wait: false,
            arguments: Table::new(),
        };
        let mut buf = BytesMut::new();
        bind.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Bind::decode(&mut bytes).unwrap();
        assert_eq!(decoded, bind);
    }
}
