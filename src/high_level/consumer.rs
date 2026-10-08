use std::marker::PhantomData;

use tracing::warn;

use super::codec::Codec;
use super::delivery::{DecodeFailure, TypedDelivery, decode_delivery};
use super::queue::Queue;
use super::session::{ConsumerRegistration, session_after};
use crate::client::channel::Channel;
use crate::client::consumer::{CancellationReason, Consumer};
use crate::error::Error;
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
/// channel of its own. Messages that fail to decode are returned as
/// [`DecodeFailure`]s.
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

    /// Receive the next delivery. Returns `None` once the consumer has
    /// ended: its channel or connection closed and it did not recover.
    ///
    /// A delivery whose body cannot be decoded is `Some(Err(_))`, unsettled,
    /// for the caller to ack, reject or requeue (ADR-0006).
    pub async fn next(&mut self) -> Option<Result<TypedDelivery<T>, DecodeFailure>> {
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
            return Some(decode_delivery(
                self.queue.codec(),
                self.queue.encoder(),
                msg,
                self.opts.no_ack,
            ));
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
            // Not held while waiting for a session above: a delete must not
            // wait on a recovery that waits on the broker.
            let guard = self.queue.lock().await;
            let recovered = self.queue.redeclare_and_consume(&session, &self.opts).await;
            drop(guard);
            match recovered {
                Ok((channel, consumer)) => {
                    self.inner = consumer;
                    self._channel = channel;
                    self.generation = session.generation;
                    return true;
                }
                Err(Error::QueueDeleted { .. }) => return false,
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
