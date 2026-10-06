use std::marker::PhantomData;

use tracing::warn;

use super::codec::Codec;
use super::queue::Queue;
use super::session::{ConsumerRegistration, session_after};
use crate::client::channel::{Channel, QueueDeleteOpts};
use crate::client::consumer::{CancellationReason, Consumer};
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
    /// The session generation `inner` consumes on.
    generation: u64,
    /// With recovery on, counts this consumer as recovery demand.
    _registration: Option<ConsumerRegistration>,
    queue: Queue<C>,
    opts: SubscribeOpts,
    _phantom: PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned + 'static, C: Codec> TypedConsumer<T, C> {
    pub(crate) fn new(
        inner: Consumer,
        channel: Channel,
        generation: u64,
        registration: Option<ConsumerRegistration>,
        queue: Queue<C>,
        opts: SubscribeOpts,
    ) -> Self {
        Self {
            inner,
            _channel: channel,
            generation,
            _registration: registration,
            queue,
            opts,
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
            let Some(msg) = self.inner.next().await else {
                if self.recover().await {
                    continue;
                }
                // Ended for good: no longer recovery demand, even if the
                // caller keeps the consumer around.
                self._registration = None;
                return None;
            };
            match self.queue.codec().decode::<T>(&msg.body) {
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

impl<T, C: Codec> TypedConsumer<T, C> {
    /// Recovery, when the client has it on. After `ChannelClosed` /
    /// `ServerCancel` it consumes again on the current session; after
    /// `ConnectionClosed` it first waits, unbounded, for a newer session. A
    /// failed attempt is retried after `RecoveryConfig::timeout`. Gives up if
    /// the queue was deleted through its handle or the client is dropped.
    /// Returns whether it recovered.
    async fn recover(&mut self) -> bool {
        let Some(timeout) = self
            .queue
            .client()
            .ok()
            .and_then(|c| c.recovery().map(|r| r.retry_delay()))
        else {
            return false;
        };
        let mut lost_generation = match self.inner.cancellation() {
            Some(CancellationReason::ConnectionClosed { .. }) => Some(self.generation),
            Some(CancellationReason::ChannelClosed { .. } | CancellationReason::ServerCancel) => {
                None
            }
            None => return false,
        };
        while self.queue.is_recorded() {
            let session = match lost_generation {
                Some(generation) => {
                    match session_after(self.queue.weak_client(), generation, None).await {
                        Ok(session) => session,
                        Err(_) => return false,
                    }
                }
                None => match self.queue.client() {
                    Ok(client) => client.current(),
                    Err(_) => return false,
                },
            };
            match self.queue.redeclare_and_consume(&session, &self.opts).await {
                Ok((channel, consumer, name)) if !self.queue.is_recorded() => {
                    // Deleted through its handle while we declared it again:
                    // undo that and end.
                    drop(consumer);
                    let _ = channel
                        .queue_delete(&name, QueueDeleteOpts::default())
                        .await;
                    return false;
                }
                Ok((channel, consumer, _)) => {
                    self.inner = consumer;
                    self._channel = channel;
                    self.generation = session.generation;
                    return true;
                }
                Err(Error::ConnectionClosed { .. }) => lost_generation = Some(session.generation),
                Err(e) => {
                    warn!(error = %e, "consumer recovery failed, retrying");
                    tokio::time::sleep(timeout).await;
                }
            }
        }
        false
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
