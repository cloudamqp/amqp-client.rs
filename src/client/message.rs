use std::sync::Arc;

use bytes::Bytes;

use super::channel::{ClosedState, check_open};
use super::connection::{ConnectionShared, OutgoingFrame};
use crate::error::Error;
use crate::protocol::frame::Frame;
use crate::protocol::method::MethodFrame;
use crate::protocol::method::basic as basic_method;
use crate::protocol::properties::BasicProperties;
use crate::protocol::types::ShortStr;

/// The channel a message arrived on, used to ack/nack/reject it.
pub(crate) struct MessageHandle {
    channel_id: u16,
    shared: Arc<ConnectionShared>,
    closed: ClosedState,
}

impl MessageHandle {
    pub(crate) fn new(channel_id: u16, shared: Arc<ConnectionShared>, closed: ClosedState) -> Self {
        Self {
            channel_id,
            shared,
            closed,
        }
    }

    fn ack(&self, delivery_tag: u64) -> Result<(), Error> {
        self.send_method(MethodFrame::BasicAck(basic_method::Ack {
            delivery_tag,
            multiple: false,
        }))
    }

    fn nack(&self, delivery_tag: u64, requeue: bool) -> Result<(), Error> {
        self.send_method(MethodFrame::BasicNack(basic_method::Nack {
            delivery_tag,
            multiple: false,
            requeue,
        }))
    }

    fn reject(&self, delivery_tag: u64, requeue: bool) -> Result<(), Error> {
        self.send_method(MethodFrame::BasicReject(basic_method::Reject {
            delivery_tag,
            requeue,
        }))
    }

    fn send_method(&self, method: MethodFrame) -> Result<(), Error> {
        // Acking on a channel the broker closed is a connection error.
        check_open(self.channel_id, &self.closed)?;
        self.shared
            .send(OutgoingFrame::Frame(Frame::Method(self.channel_id, method)))
    }
}

/// A message delivered to a consumer via Basic.Deliver.
pub struct DeliverMessage {
    pub consumer_tag: ShortStr,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub properties: BasicProperties,
    pub body: Bytes,
    pub(crate) handle: MessageHandle,
}

impl DeliverMessage {
    /// Acknowledge the message.
    pub fn ack(&self) -> Result<(), Error> {
        self.handle.ack(self.delivery_tag)
    }

    /// Negative-acknowledge the message.
    pub fn nack(&self, requeue: bool) -> Result<(), Error> {
        self.handle.nack(self.delivery_tag, requeue)
    }

    /// Reject the message.
    pub fn reject(&self, requeue: bool) -> Result<(), Error> {
        self.handle.reject(self.delivery_tag, requeue)
    }
}

impl std::fmt::Debug for DeliverMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliverMessage")
            .field("consumer_tag", &self.consumer_tag)
            .field("delivery_tag", &self.delivery_tag)
            .field("redelivered", &self.redelivered)
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// A message retrieved via Basic.Get.
pub struct GetMessage {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub message_count: u32,
    pub properties: BasicProperties,
    pub body: Bytes,
    pub(crate) handle: MessageHandle,
}

impl GetMessage {
    /// Acknowledge the message.
    pub fn ack(&self) -> Result<(), Error> {
        self.handle.ack(self.delivery_tag)
    }

    /// Negative-acknowledge the message.
    pub fn nack(&self, requeue: bool) -> Result<(), Error> {
        self.handle.nack(self.delivery_tag, requeue)
    }

    /// Reject the message.
    pub fn reject(&self, requeue: bool) -> Result<(), Error> {
        self.handle.reject(self.delivery_tag, requeue)
    }
}

impl std::fmt::Debug for GetMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetMessage")
            .field("delivery_tag", &self.delivery_tag)
            .field("redelivered", &self.redelivered)
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("message_count", &self.message_count)
            .field("body_len", &self.body.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::connection::ChannelIdAllocator;
    use std::sync::Mutex;
    use tokio::sync::{Semaphore, mpsc};

    fn handle() -> (MessageHandle, mpsc::UnboundedReceiver<OutgoingFrame>) {
        let (frame_tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(ConnectionShared {
            frame_tx,
            publish_admission: Arc::new(Semaphore::new(1)),
            publish_admission_capacity: 1,
            id_alloc: Mutex::new(ChannelIdAllocator::new(1)),
            dead: tokio::sync::Notify::new(),
            lost: std::sync::atomic::AtomicBool::new(false),
        });
        (MessageHandle::new(7, shared, ClosedState::default()), rx)
    }

    #[test]
    fn ack_sends_basic_ack_on_channel() {
        let (h, mut rx) = handle();
        h.ack(42).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            OutgoingFrame::Frame(Frame::Method(
                7,
                MethodFrame::BasicAck(basic_method::Ack {
                    delivery_tag: 42,
                    multiple: false
                })
            ))
        ));
    }

    #[test]
    fn nack_sends_basic_nack_with_requeue() {
        let (h, mut rx) = handle();
        h.nack(42, true).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            OutgoingFrame::Frame(Frame::Method(
                7,
                MethodFrame::BasicNack(basic_method::Nack {
                    delivery_tag: 42,
                    multiple: false,
                    requeue: true
                })
            ))
        ));
    }

    #[test]
    fn reject_sends_basic_reject_with_requeue() {
        let (h, mut rx) = handle();
        h.reject(42, false).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            OutgoingFrame::Frame(Frame::Method(
                7,
                MethodFrame::BasicReject(basic_method::Reject {
                    delivery_tag: 42,
                    requeue: false
                })
            ))
        ));
    }

    #[test]
    fn ack_on_a_closed_channel_errors() {
        let (h, mut rx) = handle();
        let _ = h.closed.set((406, "PRECONDITION_FAILED".into()));
        assert!(matches!(
            h.ack(1),
            Err(Error::ChannelClosed { code: 406, .. })
        ));
        assert!(rx.try_recv().is_err(), "ack was sent on a closed channel");
    }

    #[test]
    fn ack_after_connection_closed_errors() {
        let (h, rx) = handle();
        drop(rx);
        assert!(matches!(
            h.ack(1),
            Err(Error::ConnectionClosed { code: 0, .. })
        ));
    }
}
