//! A publish's content, queued for the writer as one item.

use bytes::Bytes;

use crate::protocol::frame::{ContentHeader, Frame};
use crate::protocol::method::MethodFrame;
use crate::protocol::properties::BasicProperties;

/// A publish's method, header and body. Yields them as frames, the body
/// split to fit `frame_max`, without allocating a frame list.
pub(crate) struct Content {
    channel: u16,
    method: Option<MethodFrame>,
    header: Option<Box<ContentHeader>>,
    body: Bytes,
    max_body: usize,
}

impl Content {
    pub(crate) fn new(
        channel: u16,
        method: MethodFrame,
        props: BasicProperties,
        body: Bytes,
        frame_max: u32,
    ) -> Self {
        let header = Box::new(ContentHeader {
            class_id: 60,
            body_size: body.len() as u64,
            properties: props,
        });
        // A body frame has 8 bytes of overhead.
        let max_body = match frame_max {
            0 => usize::MAX,
            n => (n as usize).saturating_sub(8).max(1),
        };
        Self {
            channel,
            method: Some(method),
            header: Some(header),
            body,
            max_body,
        }
    }
}

impl Iterator for Content {
    type Item = Frame;

    fn next(&mut self) -> Option<Frame> {
        if let Some(method) = self.method.take() {
            return Some(Frame::Method(self.channel, method));
        }
        if let Some(header) = self.header.take() {
            return Some(Frame::Header(self.channel, header));
        }
        if self.body.is_empty() {
            return None;
        }
        let chunk = self.body.split_to(self.max_body.min(self.body.len()));
        Some(Frame::Body(self.channel, chunk))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::protocol::frame::Frame;
    use crate::protocol::method::MethodFrame;
    use crate::protocol::properties::BasicProperties;

    fn content(body: &'static [u8], frame_max: u32) -> Content {
        Content::new(
            3,
            MethodFrame::ChannelCloseOk,
            BasicProperties::default(),
            Bytes::from_static(body),
            frame_max,
        )
    }

    fn bodies(content: Content) -> Vec<Bytes> {
        content
            .filter_map(|frame| match frame {
                Frame::Body(3, body) => Some(body),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn yields_the_method_then_the_header() {
        let mut frames = content(b"hello", 4096);
        assert!(matches!(frames.next(), Some(Frame::Method(3, _))));
        let Some(Frame::Header(3, header)) = frames.next() else {
            panic!("the method must be followed by the header");
        };
        assert_eq!(header.class_id, 60);
        assert_eq!(header.body_size, 5);
    }

    #[test]
    fn splits_the_body_to_fit_frame_max() {
        let body: &'static [u8] = &[1; 10_000];
        let sizes: Vec<usize> = bodies(content(body, 4096)).iter().map(Bytes::len).collect();
        assert_eq!(sizes, [4088, 4088, 1824]);
    }

    #[test]
    fn sends_the_whole_body_in_one_frame_without_frame_max() {
        let body: &'static [u8] = &[1; 10_000];
        assert_eq!(bodies(content(body, 0)), [Bytes::from_static(body)]);
    }

    #[test]
    fn sends_no_body_frame_for_an_empty_body() {
        assert_eq!(content(b"", 4096).count(), 2);
    }
}
