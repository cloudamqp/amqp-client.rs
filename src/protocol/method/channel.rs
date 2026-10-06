use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::types::{AmqpDecode, AmqpEncode, ShortStr};

// Channel.Open (20, 10) — only has a reserved short string, handled in MethodFrame
// Channel.OpenOk (20, 11) — only has a reserved long string, handled in MethodFrame

/// Channel.Flow (20, 20)
#[derive(Clone, Debug, PartialEq)]
pub struct Flow {
    pub active: bool,
}

impl AmqpEncode for Flow {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(u8::from(self.active));
    }
}

impl AmqpDecode for Flow {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            active: u8::decode(buf)? != 0,
        })
    }
}

/// Channel.FlowOk (20, 21)
#[derive(Clone, Debug, PartialEq)]
pub struct FlowOk {
    pub active: bool,
}

impl AmqpEncode for FlowOk {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(u8::from(self.active));
    }
}

impl AmqpDecode for FlowOk {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            active: u8::decode(buf)? != 0,
        })
    }
}

/// Channel.Close (20, 40)
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

// Channel.CloseOk (20, 41) — no fields

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_roundtrip() {
        let close = Close {
            reply_code: 404,
            reply_text: ShortStr::try_from("NOT_FOUND").unwrap(),
            class_id: 50,
            method_id: 10,
        };
        let mut buf = BytesMut::new();
        close.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Close::decode(&mut bytes).unwrap();
        assert_eq!(decoded, close);
    }

    #[test]
    fn flow_roundtrip() {
        let flow = Flow { active: true };
        let mut buf = BytesMut::new();
        flow.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Flow::decode(&mut bytes).unwrap();
        assert_eq!(decoded, flow);
    }
}
