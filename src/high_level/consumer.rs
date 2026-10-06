use std::marker::PhantomData;
use std::sync::Arc;

use tracing::warn;

use super::codec::Codec;
use crate::client::channel::Channel;
use crate::client::consumer::Consumer;
use crate::client::message::DeliverMessage;
use crate::error::Error;
use crate::protocol::properties::BasicProperties;
use crate::protocol::table::Table;

/// Options for [`Queue::subscribe`](super::queue::Queue::subscribe).
#[derive(Clone, Debug)]
pub struct SubscribeOpts {
    /// Unacked messages the broker sends at most (`basic.qos`). Default 20.
    pub prefetch: u16,
    /// Consumer tag; reused on every recovery. `None` lets the broker
    /// assign one.
    pub consumer_tag: Option<String>,
    pub no_ack: bool,
    pub exclusive: bool,
    pub arguments: Table,
}

impl Default for SubscribeOpts {
    fn default() -> Self {
        Self {
            prefetch: 20,
            consumer_tag: None,
            no_ack: false,
            exclusive: false,
            arguments: Table::default(),
        }
    }
}

/// A typed consumer that decodes messages using a [`Codec`].
///
/// Created by [`Queue::subscribe`](super::queue::Queue::subscribe), on a
/// channel of its own. Messages that fail to decode are automatically nacked
/// (requeue=false) and skipped.
pub struct TypedConsumer<T, C: Codec> {
    inner: Consumer,
    /// Owned so the consumer's channel lives exactly as long as it does.
    _channel: Channel,
    codec: Arc<C>,
    _phantom: PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned + 'static, C: Codec> TypedConsumer<T, C> {
    pub(crate) fn new(inner: Consumer, channel: Channel, codec: Arc<C>) -> Self {
        Self {
            inner,
            _channel: channel,
            codec,
            _phantom: PhantomData,
        }
    }

    /// Returns the consumer tag.
    pub fn tag(&self) -> &str {
        self.inner.tag()
    }

    /// Receive the next typed message. Returns `None` if the consumer or channel
    /// has been closed.
    ///
    /// Messages that cannot be decoded are automatically nacked with `requeue=false`
    /// and skipped — the loop continues until a decodable message arrives or the
    /// consumer is closed.
    pub async fn next(&mut self) -> Option<TypedDelivery<T>> {
        loop {
            let msg = self.inner.next().await?;
            match self.codec.decode::<T>(&msg.body) {
                Ok(body) => {
                    return Some(TypedDelivery {
                        body,
                        properties: msg.properties.clone(),
                        exchange: msg.exchange.to_string(),
                        routing_key: msg.routing_key.to_string(),
                        delivery_tag: msg.delivery_tag,
                        redelivered: msg.redelivered,
                        inner_msg: msg,
                    });
                }
                Err(e) => {
                    warn!(
                        consumer_tag = self.inner.tag(),
                        delivery_tag = msg.delivery_tag,
                        error = %e,
                        "failed to decode message body, nacking"
                    );
                    let _ = msg.nack(false);
                }
            }
        }
    }
}

/// A decoded message delivered to a [`TypedConsumer`].
///
/// Holds the deserialized body alongside the original AMQP metadata.
/// Acknowledgement methods delegate to the underlying raw message.
pub struct TypedDelivery<T> {
    pub body: T,
    pub properties: BasicProperties,
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    inner_msg: DeliverMessage,
}

impl<T> TypedDelivery<T> {
    /// Acknowledge the message.
    pub fn ack(&self) -> Result<(), Error> {
        self.inner_msg.ack()
    }

    /// Negative-acknowledge the message.
    pub fn nack(&self, requeue: bool) -> Result<(), Error> {
        self.inner_msg.nack(requeue)
    }

    /// Reject the message.
    pub fn reject(&self, requeue: bool) -> Result<(), Error> {
        self.inner_msg.reject(requeue)
    }
}
