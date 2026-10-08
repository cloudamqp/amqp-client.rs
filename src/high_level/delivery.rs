use super::codec::Codec;
use crate::client::message::DeliverMessage;
use crate::error::Error;
use crate::protocol::properties::BasicProperties;

/// A decoded message delivered to a [`TypedConsumer`](super::TypedConsumer).
///
/// Holds the deserialized body alongside the original AMQP metadata.
/// Dropping it unsettled leaves it unacked until its channel closes, holding a
/// `prefetch` slot until then (ADR-0006).
#[must_use = "settle the delivery with ack, nack or reject"]
#[derive(Debug)]
pub struct TypedDelivery<T> {
    pub body: T,
    pub properties: BasicProperties,
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    settlement: Settlement,
}

impl<T> TypedDelivery<T> {
    /// Acknowledge the message.
    pub fn ack(&self) -> Result<(), Error> {
        self.settlement.ack()
    }

    /// Negative-acknowledge the message.
    pub fn nack(&self, requeue: bool) -> Result<(), Error> {
        self.settlement.nack(requeue)
    }

    /// Reject the message.
    pub fn reject(&self, requeue: bool) -> Result<(), Error> {
        self.settlement.reject(requeue)
    }
}

/// A delivery whose body could not be decoded (ADR-0006).
///
/// `body` is as it came off the wire. The client has not settled it: the
/// caller decides whether to ack, reject, requeue or dead-letter it.
#[must_use = "settle the delivery with ack, nack or reject"]
pub struct DecodeFailure {
    pub error: Error,
    pub body: Vec<u8>,
    pub properties: BasicProperties,
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    settlement: Settlement,
}

impl DecodeFailure {
    /// Acknowledge the message.
    pub fn ack(&self) -> Result<(), Error> {
        self.settlement.ack()
    }

    /// Negative-acknowledge the message.
    pub fn nack(&self, requeue: bool) -> Result<(), Error> {
        self.settlement.nack(requeue)
    }

    /// Reject the message.
    pub fn reject(&self, requeue: bool) -> Result<(), Error> {
        self.settlement.reject(requeue)
    }
}

impl std::fmt::Debug for DecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodeFailure")
            .field("error", &self.error)
            .field("body_len", &self.body.len())
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("delivery_tag", &self.delivery_tag)
            .field("redelivered", &self.redelivered)
            .finish()
    }
}

/// Settles a delivery, unless its consumer is `no_ack`: the broker does not
/// know its delivery tag then, and would close the channel.
#[derive(Debug)]
struct Settlement {
    msg: DeliverMessage,
    no_ack: bool,
}

impl Settlement {
    fn ack(&self) -> Result<(), Error> {
        self.check()?;
        self.msg.ack()
    }

    fn nack(&self, requeue: bool) -> Result<(), Error> {
        self.check()?;
        self.msg.nack(requeue)
    }

    fn reject(&self, requeue: bool) -> Result<(), Error> {
        self.check()?;
        self.msg.reject(requeue)
    }

    fn check(&self) -> Result<(), Error> {
        if self.no_ack {
            return Err(Error::NoAck);
        }
        Ok(())
    }
}

/// Decodes `msg` with `codec`. A body that does not decode is handed back,
/// unsettled, as a [`DecodeFailure`].
// Both variants hold the delivery; boxing only the `Err` would not shrink it.
#[allow(clippy::result_large_err)]
pub(crate) fn decode_delivery<T, C>(
    codec: &C,
    msg: DeliverMessage,
    no_ack: bool,
) -> Result<TypedDelivery<T>, DecodeFailure>
where
    T: serde::de::DeserializeOwned + 'static,
    C: Codec,
{
    let decoded = codec.decode::<T>(&msg.body);
    let properties = msg.properties.clone();
    let exchange = msg.exchange.to_string();
    let routing_key = msg.routing_key.to_string();
    let (delivery_tag, redelivered) = (msg.delivery_tag, msg.redelivered);
    match decoded {
        Ok(body) => Ok(TypedDelivery {
            body,
            properties,
            exchange,
            routing_key,
            delivery_tag,
            redelivered,
            settlement: Settlement { msg, no_ack },
        }),
        Err(error) => Err(DecodeFailure {
            error,
            body: msg.body.to_vec(),
            properties,
            exchange,
            routing_key,
            delivery_tag,
            redelivered,
            settlement: Settlement { msg, no_ack },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::connection::OutgoingFrame;
    use crate::client::message::test_delivery;
    use crate::high_level::ByteCodec;
    use crate::protocol::frame::Frame;
    use crate::protocol::method::MethodFrame;

    struct JsonCodec;

    impl Codec for JsonCodec {
        fn content_type(&self) -> &str {
            "application/json"
        }

        fn encode<T: serde::Serialize + 'static>(&self, value: &T) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
        }

        fn decode<T: serde::de::DeserializeOwned + 'static>(
            &self,
            bytes: &[u8],
        ) -> Result<T, Error> {
            serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
        }
    }

    #[test]
    fn a_decodable_body_is_a_typed_delivery() {
        let (msg, _rx) = test_delivery(b"\"hi\"", BasicProperties::default());
        let d = decode_delivery::<String, _>(&JsonCodec, msg, false).unwrap();
        assert_eq!(d.body, "hi");
        assert_eq!(d.delivery_tag, 42);
        assert_eq!(d.exchange, "amq.topic");
        assert_eq!(d.routing_key, "rmq.queues.host");
    }

    #[test]
    fn an_undecodable_body_is_a_decode_failure_left_unsettled() {
        let (msg, mut rx) = test_delivery(b"not json", BasicProperties::default());
        let f = decode_delivery::<String, _>(&JsonCodec, msg, false).unwrap_err();
        assert!(matches!(f.error, Error::Codec(_)));
        assert_eq!(f.body, b"not json");
        assert_eq!(f.delivery_tag, 42);
        assert_eq!(f.exchange, "amq.topic");
        assert_eq!(f.routing_key, "rmq.queues.host");
        assert!(rx.try_recv().is_err(), "the client settled the failure");
    }

    #[test]
    fn a_decode_failure_can_be_rejected() {
        let (msg, mut rx) = test_delivery(b"x", BasicProperties::default());
        let f = decode_delivery::<String, _>(&ByteCodec, msg, false).unwrap_err();
        f.reject(false).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            OutgoingFrame::Frame(Frame::Method(7, MethodFrame::BasicReject(_)))
        ));
    }

    #[test]
    fn a_typed_delivery_can_be_acked() {
        let (msg, mut rx) = test_delivery(b"x", BasicProperties::default());
        let d = decode_delivery::<Vec<u8>, _>(&ByteCodec, msg, false).unwrap();
        d.ack().unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            OutgoingFrame::Frame(Frame::Method(7, MethodFrame::BasicAck(_)))
        ));
    }

    #[test]
    fn settling_a_no_ack_delivery_errors_without_sending() {
        let (msg, mut rx) = test_delivery(b"x", BasicProperties::default());
        let d = decode_delivery::<Vec<u8>, _>(&ByteCodec, msg, true).unwrap();
        assert!(matches!(d.ack(), Err(Error::NoAck)));
        assert!(matches!(d.nack(true), Err(Error::NoAck)));
        assert!(matches!(d.reject(false), Err(Error::NoAck)));
        assert!(rx.try_recv().is_err(), "settled a no_ack delivery");
    }

    #[test]
    fn settling_a_no_ack_decode_failure_errors_without_sending() {
        let (msg, mut rx) = test_delivery(b"x", BasicProperties::default());
        let f = decode_delivery::<String, _>(&ByteCodec, msg, true).unwrap_err();
        assert!(matches!(f.ack(), Err(Error::NoAck)));
        assert!(matches!(f.reject(false), Err(Error::NoAck)));
        assert!(rx.try_recv().is_err(), "settled a no_ack delivery");
    }
}
