use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_core::Stream;
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
/// channel of its own. Receive with [`next`](Self::next), or use it as a
/// [`Stream`]. Messages that fail to decode are returned as
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
    /// Set while consuming again after `inner` ended.
    recovery: Option<Recovery>,
    /// `fn() -> T`: the consumer holds no `T`, so it is `Unpin`, `Send` and
    /// `Sync` whatever `T` is.
    _phantom: PhantomData<fn() -> T>,
}

/// Consuming again; `None` if the consumer cannot. A task of its own, so it
/// makes progress, and releases the queue's lock, whether or not the consumer
/// is polled. Aborted when dropped with the consumer.
struct Recovery(tokio::task::JoinHandle<Option<Recovered>>);

impl Drop for Recovery {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What a finished recovery task gave: `None` if it gave up or was aborted.
/// A panic in it is raised again, in the caller.
fn recovery_result<R>(result: Result<Option<R>, tokio::task::JoinError>) -> Option<R> {
    match result {
        Ok(recovered) => recovered,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(_) => None,
    }
}

/// A consumer on a new channel, from recovery.
struct Recovered {
    channel: Channel,
    consumer: Consumer,
    generation: u64,
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
            recovery: None,
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
        std::future::poll_fn(|cx| self.poll_delivery(cx)).await
    }

    fn poll_delivery(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<TypedDelivery<T>, DecodeFailure>>> {
        loop {
            if let Some(recovery) = &mut self.recovery {
                let recovered = ready!(Pin::new(&mut recovery.0).poll(cx));
                self.recovery = None;
                let Some(r) = recovery_result(recovered) else {
                    self.end();
                    return Poll::Ready(None);
                };
                self.inner = r.consumer;
                self._channel = r.channel;
                self.generation = r.generation;
            }
            let Some(msg) = ready!(self.inner.poll_next(cx)) else {
                let Some(recovery) = self.start_recovery() else {
                    self.end();
                    return Poll::Ready(None);
                };
                self.recovery = Some(recovery);
                continue;
            };
            return Poll::Ready(Some(decode_delivery(
                self.queue.codec(),
                self.queue.encoder(),
                msg,
                self.opts.no_ack,
            )));
        }
    }
}

impl<T, C: Codec> TypedConsumer<T, C> {
    /// Ended for good: no longer recovery demand, even if the caller keeps
    /// the consumer around.
    fn end(&mut self) {
        self._registration = None;
    }

    /// Recovery, when the client has it on and `inner` was cancelled.
    fn start_recovery(&self) -> Option<Recovery> {
        let client = self.queue.client().ok()?;
        let timeout = client.recovery()?.retry_delay();
        let lost_generation = match self.inner.cancellation()? {
            CancellationReason::ConnectionClosed { .. } => Some(self.generation),
            CancellationReason::ChannelClosed { .. } | CancellationReason::ServerCancel => None,
        };
        Some(Recovery(tokio::spawn(recover(
            self.queue.clone(),
            self.opts.clone(),
            lost_generation,
            timeout,
        ))))
    }
}

impl<T: serde::de::DeserializeOwned + 'static, C: Codec> Stream for TypedConsumer<T, C> {
    type Item = Result<TypedDelivery<T>, DecodeFailure>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_delivery(cx)
    }
}

/// After `ChannelClosed` / `ServerCancel` (`lost_generation` is `None`) it
/// consumes again on the current session; after `ConnectionClosed` it first
/// waits, unbounded, for a session newer than `lost_generation`. A failed
/// attempt is retried after `timeout`. Gives up (`None`) if the queue was
/// deleted through its handle or the client is dropped.
async fn recover<C: Codec>(
    queue: Queue<C>,
    opts: SubscribeOpts,
    mut lost_generation: Option<u64>,
    timeout: Duration,
) -> Option<Recovered> {
    while queue.is_recorded() {
        let session = match lost_generation {
            Some(generation) => session_after(queue.weak_client(), generation, None)
                .await
                .ok()?,
            None => queue.client().ok()?.current(),
        };
        // Not held while waiting for a session above: a delete must not wait
        // on a recovery that waits on the broker.
        let guard = queue.lock().await;
        let recovered = queue.redeclare_and_consume(&session, &opts).await;
        drop(guard);
        match recovered {
            Ok((channel, consumer)) => {
                return Some(Recovered {
                    channel,
                    consumer,
                    generation: session.generation,
                });
            }
            Err(Error::QueueDeleted { .. }) => return None,
            Err(Error::ConnectionClosed { .. }) => lost_generation = Some(session.generation),
            Err(e) => {
                warn!(error = %e, "consumer recovery failed, retrying");
                tokio::time::sleep(timeout).await;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    #[tokio::test]
    async fn a_panic_in_recovery_is_raised_again() {
        let result = tokio::spawn(async {
            panic!("recovery panicked");
            #[allow(unreachable_code)]
            None::<()>
        })
        .await;
        let raised = catch_unwind(AssertUnwindSafe(|| recovery_result(result))).unwrap_err();
        assert_eq!(raised.downcast_ref::<&str>(), Some(&"recovery panicked"));
    }

    #[tokio::test]
    async fn an_aborted_recovery_ends_the_consumer() {
        let task = tokio::spawn(std::future::pending::<Option<()>>());
        task.abort();
        assert!(recovery_result(task.await).is_none());
    }
}
