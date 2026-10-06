use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;

use super::message::DeliverMessage;
use crate::protocol::types::ShortStr;

/// Why a [`Consumer`] stream ended.
///
/// Available via [`Consumer::cancellation`] after [`Consumer::next`] has
/// returned `None`. Lets callers distinguish between "the broker explicitly
/// cancelled this consumer" (e.g. queue deleted, quorum-queue failover),
/// "the channel closed" (RPC error, explicit close), and "the connection
/// died" (heartbeat timeout, peer reset).
#[derive(Clone, Debug)]
pub enum CancellationReason {
    /// The broker sent `Basic.Cancel` for this consumer. Often means the
    /// underlying queue was deleted or the consumer was forced off by HA
    /// failover; resubscribing will typically fail until the queue exists
    /// again, so callers should not blindly retry.
    ServerCancel,
    /// The channel hosting this consumer was closed (by the server with an
    /// error, or by the client). Resubscribing requires opening a new
    /// channel.
    ChannelClosed { code: u16, reason: String },
    /// The underlying connection closed (heartbeat timeout, peer disconnect,
    /// graceful close). Resubscribing requires reconnecting.
    ConnectionClosed { code: u16, reason: String },
}

/// An async consumer that receives messages from a queue.
///
/// Created by [`Channel::basic_consume`](super::channel::Channel::basic_consume).
/// Messages can be received one at a time with [`next`](Self::next). Once the
/// stream ends (returns `None`), [`cancellation`](Self::cancellation) reveals
/// why — useful for deciding whether reconnect-and-resubscribe is appropriate.
pub struct Consumer {
    pub(crate) tag: ShortStr,
    pub(crate) rx: mpsc::Receiver<DeliverMessage>,
    pub(crate) cancellation: Arc<OnceLock<CancellationReason>>,
}

impl Consumer {
    /// Returns the consumer tag.
    pub fn tag(&self) -> &str {
        self.tag.as_str().unwrap_or("<non-utf8>")
    }

    /// Receive the next message, or `None` if the channel/consumer was closed.
    pub async fn next(&mut self) -> Option<DeliverMessage> {
        self.rx.recv().await
    }

    /// Why the consumer stream ended.
    ///
    /// Returns `Some(reason)` only after [`next`](Self::next) has returned
    /// `None` (or, in races, slightly before). Returns `None` while the
    /// consumer is still active.
    pub fn cancellation(&self) -> Option<CancellationReason> {
        self.cancellation.get().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_returns_none_when_active() {
        let (_tx, rx) = mpsc::channel::<DeliverMessage>(1);
        let consumer = Consumer {
            tag: ShortStr::try_from("ctag").unwrap(),
            rx,
            cancellation: Arc::new(OnceLock::new()),
        };
        assert!(consumer.cancellation().is_none());
    }

    #[test]
    fn cancellation_surfaces_set_reason() {
        let (_tx, rx) = mpsc::channel::<DeliverMessage>(1);
        let cancellation = Arc::new(OnceLock::new());
        let consumer = Consumer {
            tag: ShortStr::try_from("ctag").unwrap(),
            rx,
            cancellation: cancellation.clone(),
        };
        // Simulate the dispatch task setting the reason on server cancel.
        cancellation.set(CancellationReason::ServerCancel).unwrap();
        assert!(matches!(
            consumer.cancellation(),
            Some(CancellationReason::ServerCancel)
        ));
    }

    #[test]
    fn cancellation_reason_is_clone() {
        let r = CancellationReason::ChannelClosed {
            code: 404,
            reason: "NOT_FOUND".into(),
        };
        let r2 = r.clone();
        match r2 {
            CancellationReason::ChannelClosed { code, reason } => {
                assert_eq!(code, 404);
                assert_eq!(reason, "NOT_FOUND");
            }
            _ => panic!("expected ChannelClosed"),
        }
    }
}
