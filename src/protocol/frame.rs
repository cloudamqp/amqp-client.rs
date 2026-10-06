use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::method::MethodFrame;
use super::properties::BasicProperties;
use super::types::{AmqpDecode, AmqpEncode, ensure_remaining};
use crate::error::ProtocolError;

/// AMQP frame type identifiers.
pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;

/// Frame end marker.
pub const FRAME_END: u8 = 0xCE;

/// AMQP protocol header sent at connection start.
pub const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

/// A decoded AMQP frame.
///
/// `ContentHeader` is boxed because it holds a full `BasicProperties` (~360
/// bytes), while the other variants are well under 200 bytes. Boxing keeps
/// the enum compact in mpsc queues without forcing every Method/Body frame
/// to pay the size.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Method(u16, MethodFrame),
    Header(u16, Box<ContentHeader>),
    Body(u16, Bytes),
    Heartbeat,
}

/// Content header (frame type 2).
#[derive(Clone, Debug, PartialEq)]
pub struct ContentHeader {
    pub class_id: u16,
    pub body_size: u64,
    pub properties: BasicProperties,
}

impl Frame {
    /// Encode this frame into a byte buffer (including the 7-byte envelope and 0xCE end marker).
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Method(channel_id, method) => {
                // Write envelope with size placeholder, encode payload in
                // place, then patch the size. Saves one BytesMut allocation
                // per outgoing method frame compared to encoding into a
                // scratch BytesMut and copying.
                buf.put_u8(FRAME_METHOD);
                buf.put_u16(*channel_id);
                let size_pos = buf.len();
                buf.put_u32(0);
                let payload_start = buf.len();
                method.encode(buf);
                let size = (buf.len() - payload_start) as u32;
                buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
                buf.put_u8(FRAME_END);
            }
            Self::Header(channel_id, header) => {
                buf.put_u8(FRAME_HEADER);
                buf.put_u16(*channel_id);
                let size_pos = buf.len();
                buf.put_u32(0);
                let payload_start = buf.len();
                header.class_id.encode(buf);
                0u16.encode(buf); // weight (always 0)
                header.body_size.encode(buf);
                header.properties.encode(buf);
                let size = (buf.len() - payload_start) as u32;
                buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
                buf.put_u8(FRAME_END);
            }
            Self::Body(channel_id, body) => {
                buf.put_u8(FRAME_BODY);
                buf.put_u16(*channel_id);
                buf.put_u32(body.len() as u32);
                buf.extend_from_slice(body);
                buf.put_u8(FRAME_END);
            }
            Self::Heartbeat => {
                buf.put_u8(FRAME_HEARTBEAT);
                buf.put_u16(0); // channel 0
                buf.put_u32(0); // no payload
                buf.put_u8(FRAME_END);
            }
        }
    }

    /// Decode a single frame from a byte buffer.
    /// The buffer must contain a complete frame (header + payload + end marker).
    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        ensure_remaining(buf, 7)?; // type + channel + size
        let frame_type = buf.get_u8();
        let channel_id = buf.get_u16();
        let size = buf.get_u32() as usize;

        ensure_remaining(buf, size + 1)?; // payload + frame_end
        let mut payload = buf.split_to(size);
        let frame_end = buf.get_u8();

        if frame_end != FRAME_END {
            return Err(ProtocolError::InvalidFrameEnd(frame_end));
        }

        match frame_type {
            FRAME_METHOD => {
                let method = MethodFrame::decode(&mut payload)?;
                Ok(Self::Method(channel_id, method))
            }
            FRAME_HEADER => {
                let class_id = u16::decode(&mut payload)?;
                let _weight = u16::decode(&mut payload)?;
                let body_size = u64::decode(&mut payload)?;
                let properties = BasicProperties::decode(&mut payload)?;
                Ok(Self::Header(
                    channel_id,
                    Box::new(ContentHeader {
                        class_id,
                        body_size,
                        properties,
                    }),
                ))
            }
            FRAME_BODY => Ok(Self::Body(channel_id, payload)),
            FRAME_HEARTBEAT => Ok(Self::Heartbeat),
            _ => Err(ProtocolError::InvalidFrameType(frame_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::method::connection;
    use crate::protocol::types::ShortStr;

    #[test]
    fn heartbeat_roundtrip() {
        let frame = Frame::Heartbeat;
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        assert_eq!(buf.len(), 8); // 1 + 2 + 4 + 0 + 1
        let mut bytes = buf.freeze();
        let decoded = Frame::decode(&mut bytes).unwrap();
        assert_eq!(decoded, Frame::Heartbeat);
    }

    #[test]
    fn method_frame_roundtrip() {
        let method = MethodFrame::ConnectionTune(connection::Tune {
            channel_max: 2047,
            frame_max: 131072,
            heartbeat: 60,
        });
        let frame = Frame::Method(0, method.clone());

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Frame::decode(&mut bytes).unwrap();
        assert_eq!(decoded, Frame::Method(0, method));
    }

    #[test]
    fn header_frame_roundtrip() {
        let props = BasicProperties::builder()
            .content_type("text/plain")
            .delivery_mode(2)
            .build()
            .unwrap();

        let header = ContentHeader {
            class_id: 60, // Basic
            body_size: 13,
            properties: props,
        };
        let frame = Frame::Header(1, Box::new(header.clone()));

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Frame::decode(&mut bytes).unwrap();
        assert_eq!(decoded, Frame::Header(1, Box::new(header)));
    }

    #[test]
    fn body_frame_roundtrip() {
        let body = Bytes::from_static(b"Hello, AMQP!");
        let frame = Frame::Body(1, body.clone());

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Frame::decode(&mut bytes).unwrap();
        assert_eq!(decoded, Frame::Body(1, body));
    }

    #[test]
    fn invalid_frame_end() {
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_HEARTBEAT);
        buf.put_u16(0);
        buf.put_u32(0);
        buf.put_u8(0xFF); // wrong end marker

        let mut bytes = buf.freeze();
        let result = Frame::decode(&mut bytes);
        assert!(matches!(result, Err(ProtocolError::InvalidFrameEnd(0xFF))));
    }

    #[test]
    fn method_frame_with_connection_close() {
        let close = connection::Close {
            reply_code: 200,
            reply_text: ShortStr::try_from("OK").unwrap(),
            class_id: 0,
            method_id: 0,
        };
        let frame = Frame::Method(0, MethodFrame::ConnectionClose(close));

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Frame::decode(&mut bytes).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let frame1 = Frame::Heartbeat;
        let frame2 = Frame::Body(1, Bytes::from_static(b"test"));

        let mut buf = BytesMut::new();
        frame1.encode(&mut buf);
        frame2.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded1 = Frame::decode(&mut bytes).unwrap();
        let decoded2 = Frame::decode(&mut bytes).unwrap();
        assert_eq!(decoded1, frame1);
        assert_eq!(decoded2, frame2);
    }

    #[test]
    fn decode_frames_from_accumulated_buffer() {
        // Simulate what the buffered reader will do: accumulate multiple frames
        // in one BytesMut, then split_to + freeze each frame for decoding.
        let frame1 = Frame::Heartbeat;
        let frame2 = Frame::Body(1, Bytes::from_static(b"hello"));
        let frame3 = Frame::Heartbeat;

        let mut buf = BytesMut::new();
        frame1.encode(&mut buf);
        frame2.encode(&mut buf);
        frame3.encode(&mut buf);

        // Parse frame1: peek header to get size, split, freeze, decode
        let size1 = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
        let frame_len1 = 7 + size1 + 1;
        let mut slice1 = buf.split_to(frame_len1).freeze();
        assert_eq!(Frame::decode(&mut slice1).unwrap(), frame1);

        // Parse frame2
        let size2 = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
        let frame_len2 = 7 + size2 + 1;
        let mut slice2 = buf.split_to(frame_len2).freeze();
        assert_eq!(Frame::decode(&mut slice2).unwrap(), frame2);

        // Parse frame3
        let size3 = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
        let frame_len3 = 7 + size3 + 1;
        let mut slice3 = buf.split_to(frame_len3).freeze();
        assert_eq!(Frame::decode(&mut slice3).unwrap(), frame3);

        assert!(buf.is_empty());
    }
}
