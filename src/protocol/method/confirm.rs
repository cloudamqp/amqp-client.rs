use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::types::{AmqpDecode, AmqpEncode};

/// Confirm.Select (85, 10)
#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub no_wait: bool,
}

impl AmqpEncode for Select {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(u8::from(self.no_wait));
    }
}

impl AmqpDecode for Select {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        Ok(Self {
            no_wait: u8::decode(buf)? != 0,
        })
    }
}

// Confirm.SelectOk (85, 11) — no fields

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_roundtrip() {
        let sel = Select { no_wait: false };
        let mut buf = BytesMut::new();
        sel.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Select::decode(&mut bytes).unwrap();
        assert_eq!(decoded, sel);
    }
}
