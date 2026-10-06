use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::table::Table;
use crate::protocol::types::{AmqpDecode, AmqpEncode, LongStr, ShortStr};

/// Connection.Start (10, 10) — server → client
#[derive(Clone, Debug, PartialEq)]
pub struct Start {
    pub version_major: u8,
    pub version_minor: u8,
    pub server_properties: Table,
    pub mechanisms: LongStr,
    pub locales: LongStr,
}

impl AmqpEncode for Start {
    fn encode(&self, buf: &mut BytesMut) {
        self.version_major.encode(buf);
        self.version_minor.encode(buf);
        self.server_properties.encode(buf);
        self.mechanisms.encode(buf);
        self.locales.encode(buf);
    }
}

impl AmqpDecode for Start {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            version_major: u8::decode(buf)?,
            version_minor: u8::decode(buf)?,
            server_properties: Table::decode(buf)?,
            mechanisms: LongStr::decode(buf)?,
            locales: LongStr::decode(buf)?,
        })
    }
}

/// Connection.StartOk (10, 11) — client → server
#[derive(Clone, Debug, PartialEq)]
pub struct StartOk {
    pub client_properties: Table,
    pub mechanism: ShortStr,
    pub response: LongStr,
    pub locale: ShortStr,
}

impl AmqpEncode for StartOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.client_properties.encode(buf);
        self.mechanism.encode(buf);
        self.response.encode(buf);
        self.locale.encode(buf);
    }
}

impl AmqpDecode for StartOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            client_properties: Table::decode(buf)?,
            mechanism: ShortStr::decode(buf)?,
            response: LongStr::decode(buf)?,
            locale: ShortStr::decode(buf)?,
        })
    }
}

/// Connection.Secure (10, 20) — server → client
#[derive(Clone, Debug, PartialEq)]
pub struct Secure {
    pub challenge: LongStr,
}

impl AmqpEncode for Secure {
    fn encode(&self, buf: &mut BytesMut) {
        self.challenge.encode(buf);
    }
}

impl AmqpDecode for Secure {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            challenge: LongStr::decode(buf)?,
        })
    }
}

/// Connection.SecureOk (10, 21) — client → server
#[derive(Clone, Debug, PartialEq)]
pub struct SecureOk {
    pub response: LongStr,
}

impl AmqpEncode for SecureOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.response.encode(buf);
    }
}

impl AmqpDecode for SecureOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            response: LongStr::decode(buf)?,
        })
    }
}

/// Connection.Tune (10, 30) — server → client
#[derive(Clone, Debug, PartialEq)]
pub struct Tune {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl AmqpEncode for Tune {
    fn encode(&self, buf: &mut BytesMut) {
        self.channel_max.encode(buf);
        self.frame_max.encode(buf);
        self.heartbeat.encode(buf);
    }
}

impl AmqpDecode for Tune {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            channel_max: u16::decode(buf)?,
            frame_max: u32::decode(buf)?,
            heartbeat: u16::decode(buf)?,
        })
    }
}

/// Connection.TuneOk (10, 31) — client → server
#[derive(Clone, Debug, PartialEq)]
pub struct TuneOk {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

impl AmqpEncode for TuneOk {
    fn encode(&self, buf: &mut BytesMut) {
        self.channel_max.encode(buf);
        self.frame_max.encode(buf);
        self.heartbeat.encode(buf);
    }
}

impl AmqpDecode for TuneOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            channel_max: u16::decode(buf)?,
            frame_max: u32::decode(buf)?,
            heartbeat: u16::decode(buf)?,
        })
    }
}

/// Connection.Open (10, 40) — client → server
#[derive(Clone, Debug, PartialEq)]
pub struct Open {
    pub virtual_host: ShortStr,
    // reserved fields (capabilities, insist) are deprecated
}

impl AmqpEncode for Open {
    fn encode(&self, buf: &mut BytesMut) {
        self.virtual_host.encode(buf);
        ShortStr::new().encode(buf); // reserved (capabilities)
        buf.put_u8(0); // reserved (insist)
    }
}

impl AmqpDecode for Open {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        let virtual_host = ShortStr::decode(buf)?;
        ShortStr::decode(buf)?; // reserved
        u8::decode(buf)?; // reserved
        Ok(Self { virtual_host })
    }
}

// Connection.OpenOk (10, 41) — has only a reserved short string, handled in MethodFrame

/// Connection.Close (10, 50) — bidirectional
#[derive(Clone, Debug, PartialEq)]
pub struct Close {
    pub reply_code: u16,
    pub reply_text: ShortStr,
    pub class_id: u16,
    pub method_id: u16,
}

impl AmqpEncode for Close {
    fn encode(&self, buf: &mut BytesMut) {
        self.reply_code.encode(buf);
        self.reply_text.encode(buf);
        self.class_id.encode(buf);
        self.method_id.encode(buf);
    }
}

impl AmqpDecode for Close {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            reply_code: u16::decode(buf)?,
            reply_text: ShortStr::decode(buf)?,
            class_id: u16::decode(buf)?,
            method_id: u16::decode(buf)?,
        })
    }
}

// Connection.CloseOk (10, 51) — no fields

/// Connection.Blocked (10, 60) — server → client (RabbitMQ extension)
#[derive(Clone, Debug, PartialEq)]
pub struct Blocked {
    pub reason: ShortStr,
}

impl AmqpEncode for Blocked {
    fn encode(&self, buf: &mut BytesMut) {
        self.reason.encode(buf);
    }
}

impl AmqpDecode for Blocked {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            reason: ShortStr::decode(buf)?,
        })
    }
}

// Connection.Unblocked (10, 61) — no fields

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tune_roundtrip() {
        let tune = Tune {
            channel_max: 2047,
            frame_max: 131072,
            heartbeat: 60,
        };
        let mut buf = BytesMut::new();
        tune.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Tune::decode(&mut bytes).unwrap();
        assert_eq!(decoded, tune);
    }

    #[test]
    fn close_roundtrip() {
        let close = Close {
            reply_code: 200,
            reply_text: ShortStr::try_from("Normal shutdown").unwrap(),
            class_id: 0,
            method_id: 0,
        };
        let mut buf = BytesMut::new();
        close.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Close::decode(&mut bytes).unwrap();
        assert_eq!(decoded, close);
    }

    #[test]
    fn open_roundtrip() {
        let open = Open {
            virtual_host: ShortStr::try_from("/").unwrap(),
        };
        let mut buf = BytesMut::new();
        open.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Open::decode(&mut bytes).unwrap();
        assert_eq!(decoded, open);
    }
}
