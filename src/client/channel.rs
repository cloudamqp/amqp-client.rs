use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::{Bytes, BytesMut};

use super::consumer::CancellationReason;

/// Convertible into the `Bytes` body that `basic_publish` ships on the wire.
///
/// Implemented for every common shape so that callers can pass `&[u8]`,
/// `&[u8; N]` literals, `Vec<u8>`, `String`, `&str`, or `Bytes`. Owned types
/// (`Bytes`, `Vec<u8>`, `String`) are zero-copy through the writer; borrowed
/// types perform one `Bytes::copy_from_slice` at the call boundary.
///
/// We use a custom trait instead of `impl Into<Bytes>` because `Bytes` does
/// not implement `From<&[u8; N]>` — `&[u8; N]` does not deref-coerce to
/// `&[u8]` in generic-trait-bound contexts, which would force callers to
/// `.as_slice()` every byte-string literal.
pub trait IntoBody {
    fn into_body(self) -> Bytes;
}

impl IntoBody for Bytes {
    fn into_body(self) -> Bytes {
        self
    }
}

impl IntoBody for Vec<u8> {
    fn into_body(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoBody for String {
    fn into_body(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoBody for &[u8] {
    fn into_body(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl<const N: usize> IntoBody for &[u8; N] {
    fn into_body(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl IntoBody for &str {
    fn into_body(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use super::connection::{ConnectionShared, OutgoingFrame};
use super::consumer::Consumer;
use super::exchange::Exchange;
use super::message::{DeliverMessage, GetMessage, MessageHandle};
use super::queue::Queue;
use crate::error::Error;
use crate::protocol::frame::{ContentHeader, Frame};
use crate::protocol::method::MethodFrame;
use crate::protocol::method::basic as basic_method;
use crate::protocol::method::channel as ch_method;
use crate::protocol::method::confirm as confirm_method;
use crate::protocol::method::exchange as exchange_method;
use crate::protocol::method::queue as queue_method;
use crate::protocol::properties::BasicProperties;
use crate::protocol::table::Table;
use crate::protocol::types::ShortStr;

/// Per-consumer routing state stored in the channel's `consumer_senders` map
/// and cloned into the dispatch loop's local cache. Carries the message
/// `Sender` plus a one-shot slot for the cancellation reason that the
/// dispatch loop can write into to communicate `why` a consumer ended.
#[derive(Clone)]
pub(crate) struct ConsumerEntry {
    pub(crate) sender: mpsc::Sender<DeliverMessage>,
    pub(crate) cancellation: Arc<OnceLock<CancellationReason>>,
}

/// Maximum time a publish may block waiting for the broker to lift a
/// `Channel.Flow` suspend before the call returns `FlowSuspendedTooLong`.
const FLOW_SUSPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Capacity of the `channel.returns()` mpsc. Returns are diagnostic-class
/// events; users that don't drain at this rate get drop-newest semantics
/// (see `ReturnsState` for the count of dropped returns).
const RETURNS_BUFFER_CAPACITY: usize = 256;

/// A message returned by the broker because it was unroutable.
///
/// Sent on a `basic_publish` with `mandatory=true` (or `immediate=true` —
/// deprecated in modern AMQP) when no queue is bound to the message's
/// exchange/routing-key combination. Surfaces via [`Returns`].
#[derive(Clone, Debug)]
pub struct ReturnedMessage {
    pub reply_code: u16,
    pub reply_text: ShortStr,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub properties: BasicProperties,
    pub body: Bytes,
}

/// A stream of `Basic.Return` events for a channel — messages the broker
/// rejected as unroutable (mandatory publishes with no matching binding).
///
/// Created by [`Channel::returns`]. Single-subscriber per channel:
/// calling `Channel::returns` more than once returns
/// `Err(Error::ReturnsAlreadySubscribed)`.
///
/// **Backpressure:** the underlying buffer is bounded (256 entries). If the
/// consumer falls behind, dispatch will drop the *newest* arriving return
/// (with a warn-log) rather than block delivery of method frames or
/// deliveries on the same channel. Callers that need to detect drops can
/// observe [`Channel::dropped_returns`] (atomic counter).
pub struct Returns {
    rx: mpsc::Receiver<ReturnedMessage>,
}

impl Returns {
    /// Receive the next returned message, or `None` when the channel
    /// closes.
    pub async fn next(&mut self) -> Option<ReturnedMessage> {
        self.rx.recv().await
    }
}

/// Subscriber state shared with the dispatch loop. Wrapped in `Mutex` so
/// `Channel::returns` can install a fresh sender once and dispatch can
/// `try_send` for every returned message.
#[derive(Default)]
struct ReturnsState {
    sender: Option<mpsc::Sender<ReturnedMessage>>,
    /// Count of returns dropped because the consumer fell behind. Read via
    /// `Channel::dropped_returns()`.
    dropped: std::sync::atomic::AtomicU64,
}

/// Outcome of a confirmed publish.
///
/// Returned by awaiting a [`PublishConfirm`] after `basic_publish_confirmed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confirmation {
    /// The broker accepted (and routed, if mandatory was set) the message.
    Acked,
    /// The broker rejected the message. `requeue` matches the broker's
    /// flag, but for publish-confirm Nacks RabbitMQ always sends false.
    Nacked { requeue: bool },
}

/// Publisher-confirms state shared with the dispatch loop.
struct ConfirmsState {
    /// Whether confirm_select has been negotiated. Until set, calls to
    /// `basic_publish_confirmed` return an error (the broker would not
    /// emit Ack/Nack and the future would hang forever).
    enabled: bool,
    /// Next publish-sequence number. Broker assigns delivery_tag = N for
    /// the Nth publish on a confirm-mode channel, starting at 1.
    next_seq: u64,
    /// Pending oneshots keyed by delivery_tag.
    pending: std::collections::BTreeMap<u64, tokio::sync::oneshot::Sender<Confirmation>>,
}

impl Default for ConfirmsState {
    fn default() -> Self {
        Self {
            enabled: false,
            next_seq: 1,
            pending: std::collections::BTreeMap::new(),
        }
    }
}

/// Future-like handle returned by `Channel::basic_publish_confirmed`.
///
/// Resolves when the broker emits `Basic.Ack` or `Basic.Nack` for the
/// publish (or when the channel closes, in which case awaiting yields
/// `Err(ConnectionClosed)`).
pub struct PublishConfirm {
    delivery_tag: u64,
    rx: tokio::sync::oneshot::Receiver<Confirmation>,
}

impl PublishConfirm {
    /// The broker-assigned delivery tag for this publish. Useful for
    /// correlating with `Basic.Return` events when `mandatory=true`.
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }
}

impl std::future::Future for PublishConfirm {
    type Output = Result<Confirmation, Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let rx = std::pin::Pin::new(&mut self.rx);
        match std::future::Future::poll(rx, cx) {
            std::task::Poll::Ready(Ok(conf)) => std::task::Poll::Ready(Ok(conf)),
            std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(Err(Error::ConnectionClosed {
                    code: 0,
                    reason: "channel closed before confirm arrived".into(),
                }))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// What kind of reply the RPC caller is expecting. Tells the dispatch
/// loop how to interpret the next inbound frames addressed to the slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RpcKind {
    /// A single method-only reply (most RPCs: queue_declare-ok,
    /// exchange_declare-ok, etc.).
    PlainMethod,
    /// `Basic.Get` reply: either `GetEmpty`, or `GetOk` followed by
    /// `Header + Body` content frames that we bundle into a `GetMessage`
    /// before resolving.
    Get,
}

/// What dispatch produces and the RPC future receives.
#[allow(
    clippy::large_enum_variant,
    reason = "short-lived, one per RPC reply; boxing would add an allocation per basic_get"
)]
enum RpcOutcome {
    Method(MethodFrame),
    Get(GetMessage),
    GetEmpty,
    ChannelClosed {
        code: u16,
        reason: String,
    },
    /// The connection was lost before the reply arrived.
    ConnectionClosed,
}

/// In-flight RPC slot: a typed kind, a oneshot back to the awaiter, and a
/// guard that holds the per-channel `rpc_lock` until dispatch processes the
/// reply. Holding the guard until dispatch consumes the slot is the linchpin
/// of cancellation safety: if the user drops the future mid-await, the
/// guard stays in the slot, the next caller blocks on the lock, and the
/// stale broker reply lands harmlessly on the dropped oneshot. Without the
/// guard handoff, the next caller could register a new expectation just in
/// time to receive the previous caller's reply.
struct RpcExpectation {
    kind: RpcKind,
    tx: tokio::sync::oneshot::Sender<RpcOutcome>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// An AMQP channel multiplexed over a connection.
pub struct Channel {
    id: u16,
    shared: Arc<ConnectionShared>,
    frame_max: u32,
    /// Per-RPC timeout. Applied inside `rpc_call` around the oneshot await.
    rpc_timeout: std::time::Duration,
    /// Channel registry (shared with connection) for cleanup on close.
    channel_senders: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>>,
    /// Set once the channel is closed on the broker (by `close()` or by the
    /// broker): later operations fail with it, and drop need not send
    /// `Channel.Close`.
    closed: ClosedState,
    /// Per-channel RPC serialization lock. Each public RPC method takes
    /// this as an `OwnedMutexGuard` and hands ownership to the dispatch
    /// task via the `inflight_rpc` slot, so the next caller can't proceed
    /// until dispatch has fully consumed the previous response.
    rpc_lock: Arc<tokio::sync::Mutex<()>>,
    /// In-flight RPC slot. Set by RPC methods when registering an
    /// expectation, taken by dispatch on reply.
    inflight_rpc: Arc<Mutex<Option<RpcExpectation>>>,
    /// Active consumers (shared with dispatch task).
    consumer_senders: Arc<Mutex<HashMap<ShortStr, ConsumerEntry>>>,
    /// Watch handle for broker-initiated `Channel.Flow` state. The dispatch
    /// task writes (`true` = active, `false` = suspended); publish paths
    /// peek-then-await on the receiver clone before sending content frames.
    flow_rx: tokio::sync::watch::Receiver<bool>,
    /// Returns subscriber slot. The dispatch task tries to push every
    /// `Basic.Return` here; users opt in via [`Channel::returns`].
    returns_state: Arc<Mutex<ReturnsState>>,
    /// Publisher-confirms slot. After [`Channel::confirm_select`] is called,
    /// each `basic_publish_confirmed` registers a oneshot here keyed by
    /// delivery_tag; the dispatch task resolves them on Ack/Nack.
    confirms_state: Arc<Mutex<ConfirmsState>>,
    /// Handle to the dispatch task.
    _dispatch_task: JoinHandle<()>,
}

impl Channel {
    pub(crate) fn new(
        id: u16,
        shared: Arc<ConnectionShared>,
        frame_max: u32,
        rpc_timeout: std::time::Duration,
        channel_senders: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>>,
        incoming_rx: mpsc::UnboundedReceiver<Frame>,
    ) -> Self {
        let consumer_senders: Arc<Mutex<HashMap<ShortStr, ConsumerEntry>>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Channel.Flow latch: starts active. Dispatch task owns the Sender;
        // publish paths clone the Receiver to peek/await on flow state.
        let (flow_tx, flow_rx) = tokio::sync::watch::channel(true);
        let returns_state = Arc::new(Mutex::new(ReturnsState::default()));
        let confirms_state = Arc::new(Mutex::new(ConfirmsState::default()));
        let rpc_lock = Arc::new(tokio::sync::Mutex::new(()));
        let inflight_rpc: Arc<Mutex<Option<RpcExpectation>>> = Arc::new(Mutex::new(None));
        let closed = ClosedState::default();

        // Spawn the dispatch task
        let dispatch_shared = shared.clone();
        let dispatch_consumers = consumer_senders.clone();
        let dispatch_returns = returns_state.clone();
        let dispatch_confirms = confirms_state.clone();
        let dispatch_inflight = inflight_rpc.clone();
        let dispatch_task = tokio::spawn(dispatch_loop(
            id,
            dispatch_shared,
            dispatch_consumers,
            dispatch_inflight,
            flow_tx,
            dispatch_returns,
            dispatch_confirms,
            closed.clone(),
            incoming_rx,
        ));

        Self {
            id,
            shared,
            frame_max,
            rpc_timeout,
            channel_senders,
            closed,
            rpc_lock,
            inflight_rpc,
            consumer_senders,
            flow_rx,
            returns_state,
            confirms_state,
            _dispatch_task: dispatch_task,
        }
    }

    /// Subscribe to `Basic.Return` events on this channel — messages the
    /// broker rejected as unroutable. Returns `Err(ReturnsAlreadySubscribed)`
    /// if called more than once on the same channel.
    pub fn returns(&self) -> Result<Returns, Error> {
        let mut state = self.returns_state.lock().unwrap();
        if state.sender.is_some() {
            return Err(Error::ReturnsAlreadySubscribed);
        }
        let (tx, rx) = mpsc::channel(RETURNS_BUFFER_CAPACITY);
        state.sender = Some(tx);
        Ok(Returns { rx })
    }

    /// Number of returned messages dropped because the [`Returns`] subscriber
    /// fell behind (or was never installed). Useful for monitoring.
    pub fn dropped_returns(&self) -> u64 {
        self.returns_state
            .lock()
            .unwrap()
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns whether broker-initiated flow control is currently allowing
    /// content frames. Useful for monitoring; publish paths consult this
    /// internally before sending.
    pub fn is_active(&self) -> bool {
        *self.flow_rx.borrow()
    }

    /// Block until flow control is `active=true`, with a hard cap.
    /// Returns `Err(Error::FlowSuspendedTooLong)` if the timeout fires.
    async fn await_flow_active(&self) -> Result<(), Error> {
        if *self.flow_rx.borrow() {
            return Ok(());
        }
        let mut rx = self.flow_rx.clone();
        let wait = async {
            while !*rx.borrow_and_update() {
                if rx.changed().await.is_err() {
                    // Sender dropped — channel is dead. Surface as flow err
                    // rather than block forever; the next send_frame will
                    // give a more accurate ConnectionClosed.
                    return;
                }
            }
        };
        match tokio::time::timeout(FLOW_SUSPEND_TIMEOUT, wait).await {
            Ok(()) => Ok(()),
            Err(_) => Err(Error::FlowSuspendedTooLong),
        }
    }

    /// Returns the channel ID.
    pub fn id(&self) -> u16 {
        self.id
    }

    /// Create a `Queue` wrapper for fluent operations on a named queue.
    pub fn queue(&self, name: &str) -> Queue<'_> {
        Queue::new(self, name.to_string())
    }

    /// Create an `Exchange` wrapper for fluent operations on a named exchange.
    pub fn exchange(&self, name: &str) -> Exchange<'_> {
        Exchange::new(self, name.to_string())
    }

    // -----------------------------------------------------------------------
    // Queue methods
    // -----------------------------------------------------------------------

    /// Declare a queue. Returns the queue name, message count, and consumer count.
    pub async fn queue_declare(
        &self,
        name: &str,
        opts: QueueDeclareOpts,
    ) -> Result<QueueDeclareOk, Error> {
        let method = MethodFrame::QueueDeclare(queue_method::Declare {
            queue: ShortStr::try_from(name).map_err(Error::Protocol)?,
            passive: opts.passive,
            durable: opts.durable,
            exclusive: opts.exclusive,
            auto_delete: opts.auto_delete,
            no_wait: false,
            arguments: opts.arguments,
        });

        let response = self.expect_plain(method, "Queue.DeclareOk").await?;
        match response {
            MethodFrame::QueueDeclareOk(ok) => Ok(QueueDeclareOk {
                queue: ok.queue.to_string(),
                message_count: ok.message_count,
                consumer_count: ok.consumer_count,
            }),
            other => Err(unexpected("Queue.DeclareOk", &other)),
        }
    }

    /// Bind a queue to an exchange.
    pub async fn queue_bind(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        let method = MethodFrame::QueueBind(queue_method::Bind {
            queue: ShortStr::try_from(queue).map_err(Error::Protocol)?,
            exchange: ShortStr::try_from(exchange).map_err(Error::Protocol)?,
            routing_key: ShortStr::try_from(routing_key).map_err(Error::Protocol)?,
            no_wait: false,
            arguments,
        });

        let response = self.expect_plain(method, "Queue.BindOk").await?;
        match response {
            MethodFrame::QueueBindOk => Ok(()),
            other => Err(unexpected("Queue.BindOk", &other)),
        }
    }

    /// Delete a queue. Returns the number of messages purged.
    pub async fn queue_delete(&self, queue: &str, opts: QueueDeleteOpts) -> Result<u32, Error> {
        let method = MethodFrame::QueueDelete(queue_method::Delete {
            queue: ShortStr::try_from(queue).map_err(Error::Protocol)?,
            if_unused: opts.if_unused,
            if_empty: opts.if_empty,
            no_wait: false,
        });
        let response = self.expect_plain(method, "Queue.DeleteOk").await?;
        match response {
            MethodFrame::QueueDeleteOk(ok) => Ok(ok.message_count),
            other => Err(unexpected("Queue.DeleteOk", &other)),
        }
    }

    /// Purge all messages from a queue. Returns the number of messages purged.
    pub async fn queue_purge(&self, queue: &str) -> Result<u32, Error> {
        let method = MethodFrame::QueuePurge(queue_method::Purge {
            queue: ShortStr::try_from(queue).map_err(Error::Protocol)?,
            no_wait: false,
        });
        let response = self.expect_plain(method, "Queue.PurgeOk").await?;
        match response {
            MethodFrame::QueuePurgeOk(ok) => Ok(ok.message_count),
            other => Err(unexpected("Queue.PurgeOk", &other)),
        }
    }

    /// Unbind a queue from an exchange.
    pub async fn queue_unbind(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        let method = MethodFrame::QueueUnbind(queue_method::Unbind {
            queue: ShortStr::try_from(queue).map_err(Error::Protocol)?,
            exchange: ShortStr::try_from(exchange).map_err(Error::Protocol)?,
            routing_key: ShortStr::try_from(routing_key).map_err(Error::Protocol)?,
            arguments,
        });
        let response = self.expect_plain(method, "Queue.UnbindOk").await?;
        match response {
            MethodFrame::QueueUnbindOk => Ok(()),
            other => Err(unexpected("Queue.UnbindOk", &other)),
        }
    }

    // -----------------------------------------------------------------------
    // Exchange methods
    // -----------------------------------------------------------------------

    /// Declare an exchange.
    pub async fn exchange_declare(
        &self,
        exchange: &str,
        kind: &str,
        opts: ExchangeDeclareOpts,
    ) -> Result<(), Error> {
        let method = MethodFrame::ExchangeDeclare(exchange_method::Declare {
            exchange: ShortStr::try_from(exchange).map_err(Error::Protocol)?,
            kind: ShortStr::try_from(kind).map_err(Error::Protocol)?,
            passive: opts.passive,
            durable: opts.durable,
            auto_delete: opts.auto_delete,
            internal: opts.internal,
            no_wait: false,
            arguments: opts.arguments,
        });
        let response = self.expect_plain(method, "Exchange.DeclareOk").await?;
        match response {
            MethodFrame::ExchangeDeclareOk => Ok(()),
            other => Err(unexpected("Exchange.DeclareOk", &other)),
        }
    }

    /// Delete an exchange.
    pub async fn exchange_delete(&self, exchange: &str, if_unused: bool) -> Result<(), Error> {
        let method = MethodFrame::ExchangeDelete(exchange_method::Delete {
            exchange: ShortStr::try_from(exchange).map_err(Error::Protocol)?,
            if_unused,
            no_wait: false,
        });
        let response = self.expect_plain(method, "Exchange.DeleteOk").await?;
        match response {
            MethodFrame::ExchangeDeleteOk => Ok(()),
            other => Err(unexpected("Exchange.DeleteOk", &other)),
        }
    }

    /// Bind an exchange to another exchange (RabbitMQ extension).
    pub async fn exchange_bind(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        let method = MethodFrame::ExchangeBind(exchange_method::Bind {
            destination: ShortStr::try_from(destination).map_err(Error::Protocol)?,
            source: ShortStr::try_from(source).map_err(Error::Protocol)?,
            routing_key: ShortStr::try_from(routing_key).map_err(Error::Protocol)?,
            no_wait: false,
            arguments,
        });
        let response = self.expect_plain(method, "Exchange.BindOk").await?;
        match response {
            MethodFrame::ExchangeBindOk => Ok(()),
            other => Err(unexpected("Exchange.BindOk", &other)),
        }
    }

    /// Unbind an exchange from another exchange (RabbitMQ extension).
    pub async fn exchange_unbind(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        let method = MethodFrame::ExchangeUnbind(exchange_method::Unbind {
            destination: ShortStr::try_from(destination).map_err(Error::Protocol)?,
            source: ShortStr::try_from(source).map_err(Error::Protocol)?,
            routing_key: ShortStr::try_from(routing_key).map_err(Error::Protocol)?,
            no_wait: false,
            arguments,
        });
        let response = self.expect_plain(method, "Exchange.UnbindOk").await?;
        match response {
            MethodFrame::ExchangeUnbindOk => Ok(()),
            other => Err(unexpected("Exchange.UnbindOk", &other)),
        }
    }

    // -----------------------------------------------------------------------
    // Basic methods
    // -----------------------------------------------------------------------

    /// Publish a message.
    ///
    /// `body` accepts anything that implements [`IntoBody`]: `Bytes`, `Vec<u8>`,
    /// `String`, `&str`, `&[u8]`, or `&[u8; N]` byte-string literals. Owned
    /// types are shipped zero-copy through the writer; borrowed types perform
    /// one `Bytes::copy_from_slice` at the call boundary.
    pub async fn basic_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: impl IntoBody,
        props: BasicProperties,
    ) -> Result<(), Error> {
        let body: Bytes = body.into_body();
        // Honor broker-initiated flow control: if the broker has set
        // Channel.Flow{active=false}, block until it's lifted (or the
        // FLOW_SUSPEND_TIMEOUT cap fires).
        self.await_flow_active().await?;
        // Acquire publish-admission permits proportional to body length —
        // capped at the semaphore total so an over-large body doesn't
        // deadlock waiting for permits that can never come back.
        let permit = self.acquire_publish_permits(body.len()).await?;
        let method = MethodFrame::BasicPublish(basic_method::Publish {
            exchange: ShortStr::try_from(exchange).map_err(Error::Protocol)?,
            routing_key: ShortStr::try_from(routing_key).map_err(Error::Protocol)?,
            mandatory: false,
            immediate: false,
        });

        self.send_frame(Frame::Method(self.id, method))?;
        self.send_frame(Frame::Header(
            self.id,
            Box::new(ContentHeader {
                class_id: 60,
                body_size: body.len() as u64,
                properties: props,
            }),
        ))?;

        self.send_body_frames(body.clone(), permit)?;

        trace!(
            channel_id = self.id,
            exchange,
            routing_key,
            body_len = body.len(),
            "message published"
        );
        Ok(())
    }

    /// Start consuming messages from a queue. Returns a `Consumer` that yields messages.
    pub async fn basic_consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        opts: ConsumeOpts,
    ) -> Result<Consumer, Error> {
        let method = MethodFrame::BasicConsume(basic_method::Consume {
            queue: ShortStr::try_from(queue).map_err(Error::Protocol)?,
            consumer_tag: ShortStr::try_from(consumer_tag).map_err(Error::Protocol)?,
            no_local: opts.no_local,
            no_ack: opts.no_ack,
            exclusive: opts.exclusive,
            no_wait: false,
            arguments: opts.arguments,
        });

        let response = self.expect_plain(method, "Basic.ConsumeOk").await?;
        let tag = match response {
            MethodFrame::BasicConsumeOk(ok) => ok.consumer_tag,
            other => return Err(unexpected("Basic.ConsumeOk", &other)),
        };

        // Create the bounded message channel for this consumer plus a
        // shared OnceLock the dispatch task writes into when the consumer
        // ends (server cancel / channel close / connection close).
        let (msg_tx, msg_rx) = mpsc::channel(4096);
        let cancellation = Arc::new(OnceLock::new());
        self.consumer_senders.lock().unwrap().insert(
            tag.clone(),
            ConsumerEntry {
                sender: msg_tx,
                cancellation: cancellation.clone(),
            },
        );

        debug!(channel_id = self.id, consumer_tag = %tag, "consumer started");

        Ok(Consumer {
            tag,
            rx: msg_rx,
            cancellation,
        })
    }

    /// Cancel a consumer.
    pub async fn basic_cancel(&self, consumer_tag: &str) -> Result<(), Error> {
        let method = MethodFrame::BasicCancel(basic_method::Cancel {
            consumer_tag: ShortStr::try_from(consumer_tag).map_err(Error::Protocol)?,
            no_wait: false,
        });

        let response = self.expect_plain(method, "Basic.CancelOk").await?;
        match response {
            MethodFrame::BasicCancelOk(_) => {
                let key = ShortStr::try_from(consumer_tag).map_err(Error::Protocol)?;
                self.consumer_senders.lock().unwrap().remove(&key);
                debug!(channel_id = self.id, consumer_tag, "consumer cancelled");
                Ok(())
            }
            other => Err(unexpected("Basic.CancelOk", &other)),
        }
    }

    /// Synchronously get a single message from a queue.
    /// Returns `None` if the queue is empty.
    ///
    /// **Cancellation safety:** dropping the returned future is safe at any
    /// point. The dispatch task always drains the trailing `Header + Body`
    /// content frames before resolving the RPC slot, so a cancelled
    /// `basic_get` cannot leave stray frames to pollute the next call.
    pub async fn basic_get(&self, queue: &str, no_ack: bool) -> Result<Option<GetMessage>, Error> {
        let method = MethodFrame::BasicGet(basic_method::Get {
            queue: ShortStr::try_from(queue).map_err(Error::Protocol)?,
            no_ack,
        });

        match self.rpc_call(method, RpcKind::Get).await? {
            RpcOutcome::GetEmpty => Ok(None),
            RpcOutcome::Get(msg) => Ok(Some(msg)),
            RpcOutcome::ChannelClosed { code, reason } => Err(Error::ChannelClosed {
                channel_id: self.id,
                code,
                reason,
            }),
            RpcOutcome::ConnectionClosed => Err(connection_closed()),
            RpcOutcome::Method(other) => Err(unexpected("Basic.GetOk or Basic.GetEmpty", &other)),
        }
    }

    /// Acknowledge a message by delivery tag.
    pub fn basic_ack(&self, delivery_tag: u64, multiple: bool) -> Result<(), Error> {
        self.send_method(MethodFrame::BasicAck(basic_method::Ack {
            delivery_tag,
            multiple,
        }))
    }

    /// Negative-acknowledge a message.
    pub fn basic_nack(
        &self,
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    ) -> Result<(), Error> {
        self.send_method(MethodFrame::BasicNack(basic_method::Nack {
            delivery_tag,
            multiple,
            requeue,
        }))
    }

    /// Reject a message.
    pub fn basic_reject(&self, delivery_tag: u64, requeue: bool) -> Result<(), Error> {
        self.send_method(MethodFrame::BasicReject(basic_method::Reject {
            delivery_tag,
            requeue,
        }))
    }

    /// Recover unacknowledged messages.
    pub async fn basic_recover(&self, requeue: bool) -> Result<(), Error> {
        let method = MethodFrame::BasicRecover(basic_method::Recover { requeue });
        let response = self.expect_plain(method, "Basic.RecoverOk").await?;
        match response {
            MethodFrame::BasicRecoverOk => Ok(()),
            other => Err(unexpected("Basic.RecoverOk", &other)),
        }
    }

    /// Set QoS (prefetch) for this channel.
    pub async fn basic_qos(&self, prefetch_count: u16, global: bool) -> Result<(), Error> {
        let method = MethodFrame::BasicQos(basic_method::Qos {
            prefetch_size: 0,
            prefetch_count,
            global,
        });

        let response = self.expect_plain(method, "Basic.QosOk").await?;
        match response {
            MethodFrame::BasicQosOk => Ok(()),
            other => Err(unexpected("Basic.QosOk", &other)),
        }
    }

    // -----------------------------------------------------------------------
    // Confirm mode
    // -----------------------------------------------------------------------

    /// Enable publisher confirm mode on this channel.
    ///
    /// After this returns, [`basic_publish_confirmed`](Self::basic_publish_confirmed)
    /// can be used to publish messages and await per-message broker
    /// acknowledgements via [`PublishConfirm`]. The plain [`Channel::basic_publish`]
    /// continues to work but does not surface confirms.
    pub async fn confirm_select(&self) -> Result<(), Error> {
        let method = MethodFrame::ConfirmSelect(confirm_method::Select { no_wait: false });
        let response = self.expect_plain(method, "Confirm.SelectOk").await?;
        match response {
            MethodFrame::ConfirmSelectOk => {
                let mut state = self.confirms_state.lock().unwrap();
                state.enabled = true;
                state.next_seq = 1;
                Ok(())
            }
            other => Err(unexpected("Confirm.SelectOk", &other)),
        }
    }

    /// Publish a message and obtain a [`PublishConfirm`] handle that
    /// resolves to `Acked`/`Nacked` when the broker emits the confirmation.
    ///
    /// Requires that [`confirm_select`](Self::confirm_select) was called
    /// first; otherwise returns an error (the broker would not emit
    /// confirms and the future would hang forever).
    ///
    /// `mandatory=true` requests that the broker bounces unroutable
    /// messages back via `Basic.Return` — pair with [`returns`](Self::returns)
    /// to learn when that happens.
    pub async fn basic_publish_confirmed(
        &self,
        exchange: &str,
        routing_key: &str,
        body: impl IntoBody,
        props: BasicProperties,
        mandatory: bool,
    ) -> Result<PublishConfirm, Error> {
        let body: Bytes = body.into_body();
        self.await_flow_active().await?;
        let permit = self.acquire_publish_permits(body.len()).await?;

        let method = MethodFrame::BasicPublish(basic_method::Publish {
            exchange: ShortStr::try_from(exchange).map_err(Error::Protocol)?,
            routing_key: ShortStr::try_from(routing_key).map_err(Error::Protocol)?,
            mandatory,
            immediate: false,
        });

        // Allocate sequence number, register oneshot, send frames. Take and
        // release the confirms_state lock around the registration only — we
        // must not hold it across the awaits below.
        let (delivery_tag, rx) = {
            let mut state = self.confirms_state.lock().unwrap();
            if !state.enabled {
                return Err(Error::ConfirmsNotEnabled);
            }
            let delivery_tag = state.next_seq;
            state.next_seq += 1;
            let (tx, rx) = tokio::sync::oneshot::channel();
            state.pending.insert(delivery_tag, tx);
            (delivery_tag, rx)
        };

        // From here on, errors must un-register the oneshot or the BTreeMap
        // leaks. A failed send means the broker got an incomplete message at
        // most, which it never routes.
        let sent = self
            .send_frame(Frame::Method(self.id, method))
            .and_then(|()| {
                self.send_frame(Frame::Header(
                    self.id,
                    Box::new(ContentHeader {
                        class_id: 60,
                        body_size: body.len() as u64,
                        properties: props,
                    }),
                ))
            })
            .and_then(|()| self.send_body_frames(body, permit));
        if let Err(e) = sent {
            self.confirms_state
                .lock()
                .unwrap()
                .pending
                .remove(&delivery_tag);
            return Err(e);
        }

        Ok(PublishConfirm { delivery_tag, rx })
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    /// Enable transaction mode on this channel.
    pub async fn tx_select(&self) -> Result<(), Error> {
        let response = self
            .expect_plain(MethodFrame::TxSelect, "Tx.SelectOk")
            .await?;
        match response {
            MethodFrame::TxSelectOk => Ok(()),
            other => Err(unexpected("Tx.SelectOk", &other)),
        }
    }

    /// Commit the current transaction.
    pub async fn tx_commit(&self) -> Result<(), Error> {
        let response = self
            .expect_plain(MethodFrame::TxCommit, "Tx.CommitOk")
            .await?;
        match response {
            MethodFrame::TxCommitOk => Ok(()),
            other => Err(unexpected("Tx.CommitOk", &other)),
        }
    }

    /// Rollback the current transaction.
    pub async fn tx_rollback(&self) -> Result<(), Error> {
        let response = self
            .expect_plain(MethodFrame::TxRollback, "Tx.RollbackOk")
            .await?;
        match response {
            MethodFrame::TxRollbackOk => Ok(()),
            other => Err(unexpected("Tx.RollbackOk", &other)),
        }
    }

    // -----------------------------------------------------------------------
    // Channel control
    // -----------------------------------------------------------------------

    /// Close this channel.
    pub async fn close(self) -> Result<(), Error> {
        let method = MethodFrame::ChannelClose(ch_method::Close {
            reply_code: 200,
            reply_text: ShortStr::try_from("Normal shutdown").map_err(Error::Protocol)?,
            class_id: 0,
            method_id: 0,
        });

        let response = self.expect_plain(method, "Channel.CloseOk").await?;
        match response {
            MethodFrame::ChannelCloseOk => {
                // Drop releases the registry entry and the ID.
                let _ = self.closed.set((200, "closed by client".into()));
                debug!(channel_id = self.id, "channel closed");
                Ok(())
            }
            other => Err(unexpected("Channel.CloseOk", &other)),
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn send_frame(&self, frame: Frame) -> Result<(), Error> {
        self.check_open()?;
        self.shared.send(OutgoingFrame::Frame(frame))
    }

    fn send_method(&self, method: MethodFrame) -> Result<(), Error> {
        self.send_frame(Frame::Method(self.id, method))
    }

    /// `ChannelClosed` with the broker's reply, once the channel is closed.
    pub(crate) fn closed_error(&self) -> Option<Error> {
        self.check_open().err()
    }

    /// Whether the channel is closed on the broker.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.get().is_some()
    }

    fn check_open(&self) -> Result<(), Error> {
        check_open(self.id, &self.closed)
    }

    /// Acquire publish-admission permits proportional to `body_len` (capped
    /// at the semaphore capacity). Returns the permit, which the writer
    /// drops after the body's bytes hit the socket.
    ///
    /// `body_len == 0` returns `None` — empty bodies don't carry permits.
    async fn acquire_publish_permits(
        &self,
        body_len: usize,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, Error> {
        if body_len == 0 {
            return Ok(None);
        }
        // Cap at semaphore capacity so an over-large body acquires the
        // whole semaphore (becoming the only in-flight publish until it
        // finishes) instead of deadlocking waiting for permits that can
        // never be available.
        let want = body_len.min(self.shared.publish_admission_capacity);
        // Cap at semaphore's hard ceiling.
        let want_u32: u32 =
            u32::try_from(want.min(tokio::sync::Semaphore::MAX_PERMITS)).unwrap_or(u32::MAX);
        let permit = self
            .shared
            .publish_admission
            .clone()
            .acquire_many_owned(want_u32)
            .await
            .map_err(|_| Error::ConnectionClosed {
                code: 0,
                reason: "publish-admission semaphore closed".into(),
            })?;
        Ok(Some(permit))
    }

    /// Chunk and send a body. The optional permit is attached to the LAST
    /// body frame so it stays held until that frame's bytes hit the socket.
    fn send_body_frames(
        &self,
        body: Bytes,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), Error> {
        if body.is_empty() {
            // No body, nothing to send. Drop any permit (shouldn't be Some,
            // but be defensive).
            drop(permit);
            return Ok(());
        }
        let max_body_size = if self.frame_max > 0 {
            (self.frame_max as usize).saturating_sub(8)
        } else {
            body.len()
        };

        if body.len() <= max_body_size {
            // Single-frame fast path: ship the whole body in one frame and
            // attach the permit (if any) so the writer releases after this
            // frame's bytes are on the wire.
            self.send_body_with_optional_permit(Frame::Body(self.id, body), permit)?;
            return Ok(());
        }

        // Chunked path: send all-but-last as plain Body frames, then attach
        // the permit to the last chunk only. This way the permit isn't
        // released until the entire body has been written.
        let mut offset = 0usize;
        let mut held_permit = permit;
        while offset < body.len() {
            let end = (offset + max_body_size).min(body.len());
            let chunk = body.slice(offset..end);
            offset = end;
            if offset == body.len() {
                // Last chunk — carry the permit.
                self.send_body_with_optional_permit(
                    Frame::Body(self.id, chunk),
                    held_permit.take(),
                )?;
            } else {
                self.send_frame(Frame::Body(self.id, chunk))?;
            }
        }
        Ok(())
    }

    fn send_body_with_optional_permit(
        &self,
        frame: Frame,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), Error> {
        self.check_open()?;
        let item = match permit {
            Some(p) => OutgoingFrame::FrameWithPermit(frame, p),
            None => OutgoingFrame::Frame(frame),
        };
        self.shared.send(item)
    }

    /// Send an RPC request and await its reply, with cancellation safety.
    ///
    /// Acquires `rpc_lock` as an `OwnedMutexGuard`, sends the request frame,
    /// registers an expectation in `inflight_rpc` (handing the guard to the
    /// dispatch task), and awaits a typed `RpcOutcome` from a oneshot.
    ///
    /// Cancellation safety: if the future is dropped between sending the
    /// request and receiving the reply, the expectation stays in the slot
    /// until the dispatch task processes the broker's response. This holds
    /// the lock and prevents the next caller from registering — so when the
    /// broker's reply arrives, dispatch resolves the (orphaned) oneshot,
    /// drops the guard, and only then does the next caller proceed.
    ///
    /// For `RpcKind::Get`, the dispatch task drains the trailing `Header +
    /// Body` content and bundles it into `RpcOutcome::Get(GetMessage)`
    /// before resolving — so an orphaned `basic_get` never leaves stray
    /// content frames to pollute the next call.
    async fn rpc_call(&self, request: MethodFrame, kind: RpcKind) -> Result<RpcOutcome, Error> {
        // Acquire the per-channel RPC lock as an owned guard. This guard
        // gets transferred to the dispatch task via the slot below.
        let guard = self.rpc_lock.clone().lock_owned().await;

        // Send the request synchronously (frame_tx is unbounded). If the
        // writer side is gone, surface as ConnectionClosed without ever
        // registering an expectation.
        self.send_method(request)?;

        // Register the expectation. From this point until dispatch consumes
        // the slot, the guard lives inside the slot — the lock stays held
        // even if our future is cancelled.
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut slot = self.inflight_rpc.lock().unwrap();
            *slot = Some(RpcExpectation {
                kind,
                tx,
                _guard: guard,
            });
        }
        // The broker may have closed the channel after the send but before
        // the expectation was registered; dispatch then never resolves it.
        if let Err(e) = self.check_open() {
            self.inflight_rpc.lock().unwrap().take();
            return Err(e);
        }

        // Await the typed outcome with the configured timeout. On timeout we
        // explicitly take the slot so the lock is released — the broker may
        // still respond, in which case dispatch will see no expectation and
        // log an orphan.
        match tokio::time::timeout(self.rpc_timeout, rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(_)) => Err(Error::ConnectionClosed {
                code: 0,
                reason: "channel dispatch task closed before RPC reply".into(),
            }),
            Err(_) => {
                // Drop the expectation (and its guard) so the lock releases.
                self.inflight_rpc.lock().unwrap().take();
                Err(Error::Timeout)
            }
        }
    }

    /// Helper for the very common RPC pattern: send `request`, expect a
    /// `PlainMethod` reply, match against the typed `MethodFrame`. Returns
    /// either the matched frame or a typed error (channel-closed,
    /// unexpected-method).
    async fn expect_plain(
        &self,
        request: MethodFrame,
        expected_label: &'static str,
    ) -> Result<MethodFrame, Error> {
        match self.rpc_call(request, RpcKind::PlainMethod).await? {
            RpcOutcome::Method(m) => Ok(m),
            RpcOutcome::ConnectionClosed => Err(connection_closed()),
            RpcOutcome::ChannelClosed { code, reason } => Err(Error::ChannelClosed {
                channel_id: self.id,
                code,
                reason,
            }),
            RpcOutcome::Get(_) | RpcOutcome::GetEmpty => Err(Error::Protocol(
                crate::error::ProtocolError::UnexpectedMethod {
                    expected: expected_label,
                    got: "Get/GetEmpty (impossible for PlainMethod kind)".into(),
                },
            )),
        }
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        // Remove this channel from the connection's registry so the reader
        // task stops trying to route frames here, and return the ID to the
        // free-list so it can be reused.
        self.channel_senders.lock().unwrap().remove(&self.id);
        self._dispatch_task.abort();
        let was_closed = self.closed.get().is_some();
        // Late acks from this channel's deliveries must fail rather than land
        // on a channel that reuses the id.
        let _ = self.closed.set((0, "channel dropped".into()));
        if was_closed {
            self.shared.id_alloc.lock().unwrap().release(self.id);
        } else {
            // Still open on the broker: close it there before reusing the ID.
            self.shared.close_without_waiting(self.id);
        }
    }
}

fn connection_closed() -> Error {
    Error::ConnectionClosed {
        code: 0,
        reason: "connection closed".into(),
    }
}

/// The broker's reply code and text once a channel is closed.
pub(crate) type ClosedState = Arc<OnceLock<(u16, String)>>;

/// `Err(ChannelClosed)` once `closed` is set.
pub(crate) fn check_open(channel_id: u16, closed: &ClosedState) -> Result<(), Error> {
    match closed.get() {
        Some((code, reason)) => Err(Error::ChannelClosed {
            channel_id,
            code: *code,
            reason: reason.clone(),
        }),
        None => Ok(()),
    }
}

/// Dispatch loop: reads frames from the connection and routes them.
#[allow(
    clippy::too_many_arguments,
    reason = "spawned once per channel with its shared state handles"
)]
async fn dispatch_loop(
    channel_id: u16,
    shared: Arc<ConnectionShared>,
    consumer_senders: Arc<Mutex<HashMap<ShortStr, ConsumerEntry>>>,
    inflight_rpc: Arc<Mutex<Option<RpcExpectation>>>,
    flow_tx: tokio::sync::watch::Sender<bool>,
    returns_state: Arc<Mutex<ReturnsState>>,
    confirms_state: Arc<Mutex<ConfirmsState>>,
    closed: ClosedState,
    mut rx: mpsc::UnboundedReceiver<Frame>,
) {
    let mut cached_consumers: HashMap<ShortStr, ConsumerEntry> = HashMap::new();

    loop {
        let frame = match rx.recv().await {
            Some(f) => f,
            None => {
                debug!(channel_id, "channel frame receiver closed");
                // The connection died (heartbeat timeout, peer disconnect).
                // Tell every active consumer why their stream just ended,
                // resolve any pending publish confirms with an error so
                // their futures don't hang forever, and resolve any pending
                // RPC slot the same way.
                set_cancellation_for_all(
                    &consumer_senders,
                    CancellationReason::ConnectionClosed {
                        code: 0,
                        reason: "connection closed".into(),
                    },
                );
                drop_all_pending_confirms(&confirms_state);
                resolve_inflight(&inflight_rpc, RpcOutcome::ConnectionClosed);
                return;
            }
        };

        match frame {
            Frame::Method(_, MethodFrame::BasicDeliver(deliver)) => {
                handle_deliver(
                    channel_id,
                    &shared,
                    &consumer_senders,
                    &mut cached_consumers,
                    deliver,
                    &closed,
                    &mut rx,
                )
                .await;
            }
            Frame::Method(_, MethodFrame::BasicCancel(cancel)) => {
                // Server-initiated cancellation. Mark the consumer's reason
                // before we drop the sender so users can distinguish this
                // from a clean close.
                if let Some(entry) = consumer_senders
                    .lock()
                    .unwrap()
                    .remove(&cancel.consumer_tag)
                {
                    let _ = entry.cancellation.set(CancellationReason::ServerCancel);
                }
                cached_consumers.remove(&cancel.consumer_tag);
                warn!(channel_id, consumer_tag = %cancel.consumer_tag, "consumer cancelled by server");
            }
            Frame::Method(_, MethodFrame::BasicGetOk(get_ok)) => {
                // Always drain trailing Header + Body content first — even
                // if the awaiter dropped its future, otherwise the content
                // frames would pollute the next RPC. Then resolve the
                // pending RpcKind::Get expectation with the bundle.
                let content = receive_content(channel_id, &mut rx).await;
                match content {
                    Ok((header, body)) => {
                        let msg = GetMessage {
                            delivery_tag: get_ok.delivery_tag,
                            redelivered: get_ok.redelivered,
                            exchange: get_ok.exchange,
                            routing_key: get_ok.routing_key,
                            message_count: get_ok.message_count,
                            properties: header.properties,
                            body,
                            handle: MessageHandle::new(channel_id, shared.clone(), closed.clone()),
                        };
                        if !resolve_inflight_get(&inflight_rpc, RpcOutcome::Get(msg)) {
                            warn!(
                                channel_id,
                                "Basic.GetOk arrived with no matching RPC expectation; dropping"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(channel_id, error = %e, "failed to assemble GetOk content");
                    }
                }
            }
            Frame::Method(_, MethodFrame::BasicGetEmpty) => {
                if !resolve_inflight_get(&inflight_rpc, RpcOutcome::GetEmpty) {
                    warn!(
                        channel_id,
                        "Basic.GetEmpty arrived with no matching RPC expectation"
                    );
                }
            }
            Frame::Method(_, MethodFrame::ChannelClose(close)) => {
                // Server-initiated channel close. Spec compliance: we MUST
                // immediately reply with Channel.CloseOk so the broker can
                // free server-side resources. Then notify every consumer
                // and any pending RPC, drop pending confirms, and exit.
                warn!(
                    channel_id,
                    code = close.reply_code,
                    reason = %close.reply_text,
                    "server closing channel"
                );
                let _ = closed.set((close.reply_code, close.reply_text.to_string()));
                let close_ok = Frame::Method(channel_id, MethodFrame::ChannelCloseOk);
                let _ = shared.frame_tx.send(OutgoingFrame::Frame(close_ok));
                set_cancellation_for_all(
                    &consumer_senders,
                    CancellationReason::ChannelClosed {
                        code: close.reply_code,
                        reason: close.reply_text.to_string(),
                    },
                );
                drop_all_pending_confirms(&confirms_state);
                resolve_inflight(
                    &inflight_rpc,
                    RpcOutcome::ChannelClosed {
                        code: close.reply_code,
                        reason: close.reply_text.to_string(),
                    },
                );
                return;
            }
            // Publisher-confirms acks/nacks. Resolve every pending oneshot
            // up to (and including) `delivery_tag`. With multiple=false we
            // resolve only the exact tag.
            Frame::Method(_, MethodFrame::BasicAck(ack)) => {
                resolve_confirms(
                    &confirms_state,
                    ack.delivery_tag,
                    ack.multiple,
                    Confirmation::Acked,
                );
            }
            Frame::Method(_, MethodFrame::BasicNack(nack)) => {
                resolve_confirms(
                    &confirms_state,
                    nack.delivery_tag,
                    nack.multiple,
                    Confirmation::Nacked {
                        requeue: nack.requeue,
                    },
                );
            }
            Frame::Method(_, MethodFrame::BasicReturn(ret)) => {
                // BasicReturn is followed by Header + Body content frames.
                // Assemble them and try to deliver to a Returns subscriber if
                // one is installed; otherwise drop and bump the counter.
                let content_result = receive_content(channel_id, &mut rx).await;
                let (header, body) = match content_result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(channel_id, error = %e, "failed to drain returned-message content");
                        continue;
                    }
                };
                let returned = ReturnedMessage {
                    reply_code: ret.reply_code,
                    reply_text: ret.reply_text,
                    exchange: ret.exchange,
                    routing_key: ret.routing_key,
                    properties: header.properties,
                    body,
                };
                let state = returns_state.lock().unwrap();
                let dispatched = match state.sender.as_ref() {
                    Some(tx) => tx.try_send(returned).is_ok(),
                    None => false,
                };
                if !dispatched {
                    state
                        .dropped
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(
                        channel_id,
                        "Basic.Return dropped (no subscriber or buffer full); see Channel::dropped_returns"
                    );
                }
            }
            Frame::Method(_, MethodFrame::ChannelFlow(flow)) => {
                // Update the latch so any in-flight publish waker, plus all
                // future publishes, observe the new active state. Then ack
                // back so the broker knows we received the request.
                let _ = flow_tx.send(flow.active);
                if !flow.active {
                    warn!(channel_id, "Channel.Flow paused by broker");
                } else {
                    debug!(channel_id, "Channel.Flow resumed by broker");
                }
                let flow_ok = Frame::Method(
                    channel_id,
                    MethodFrame::ChannelFlowOk(ch_method::FlowOk {
                        active: flow.active,
                    }),
                );
                let _ = shared.frame_tx.send(OutgoingFrame::Frame(flow_ok));
            }
            Frame::Method(_, method) => {
                // Plain reply to an in-flight RPC. Resolve the slot. If the
                // user cancelled their future, the oneshot send fails and
                // we drop the reply harmlessly — but the guard in the slot
                // is also released, unblocking the next caller.
                if !resolve_inflight(&inflight_rpc, RpcOutcome::Method(method)) {
                    warn!(
                        channel_id,
                        "method reply with no matching RPC expectation; dropping"
                    );
                }
            }
            Frame::Header(_, _) | Frame::Body(_, _) => {
                trace!(channel_id, "skipping orphan content frame");
            }
            Frame::Heartbeat => {}
        }
    }
}

async fn handle_deliver(
    channel_id: u16,
    shared: &Arc<ConnectionShared>,
    consumer_senders: &Arc<Mutex<HashMap<ShortStr, ConsumerEntry>>>,
    cached_consumers: &mut HashMap<ShortStr, ConsumerEntry>,
    deliver: basic_method::Deliver,
    closed: &ClosedState,
    rx: &mut mpsc::UnboundedReceiver<Frame>,
) {
    // Receive Header + Body
    let (header, body) = match receive_content(channel_id, rx).await {
        Ok(result) => result,
        Err(e) => {
            warn!(channel_id, error = %e, "failed to receive content for deliver");
            return;
        }
    };

    // Clone consumer_tag up front: one copy for the message, one for lookup/logging.
    let tag = deliver.consumer_tag.clone();

    let msg = DeliverMessage {
        consumer_tag: deliver.consumer_tag,
        delivery_tag: deliver.delivery_tag,
        redelivered: deliver.redelivered,
        exchange: deliver.exchange,
        routing_key: deliver.routing_key,
        properties: header.properties,
        body,
        handle: MessageHandle::new(channel_id, shared.clone(), closed.clone()),
    };

    // Look up entry in local cache first, falling back to the shared map.
    let entry = cached_consumers.get(&tag).cloned().or_else(|| {
        let e = consumer_senders.lock().unwrap().get(&tag).cloned();
        if let Some(ref e) = e {
            cached_consumers.insert(tag.clone(), e.clone());
        }
        e
    });

    if let Some(entry) = entry {
        if entry.sender.send(msg).await.is_err() {
            warn!(channel_id, consumer_tag = %tag, "consumer dropped");
            cached_consumers.remove(&tag);
            consumer_senders.lock().unwrap().remove(&tag);
        }
    } else {
        warn!(channel_id, consumer_tag = %tag, "message for unknown consumer");
    }
}

/// Resolve the in-flight RPC slot with a typed outcome. Returns `true` if
/// an expectation was present (it may still have failed to deliver if the
/// awaiter dropped its receiver, but that's fine — the lock guard is
/// released either way and the next RPC can proceed).
fn resolve_inflight(slot: &Arc<Mutex<Option<RpcExpectation>>>, outcome: RpcOutcome) -> bool {
    let exp = slot.lock().unwrap().take();
    match exp {
        Some(expectation) => {
            // `tx.send` consumes the sender; drop on Err is fine.
            let _ = expectation.tx.send(outcome);
            // `_guard` drops here, releasing the rpc_lock.
            true
        }
        None => false,
    }
}

/// Like `resolve_inflight` but only takes the slot if the expectation is
/// `RpcKind::Get`. If a non-Get RPC is in flight when a `GetOk`/`GetEmpty`
/// arrives, that's a protocol violation by the broker — leave the
/// expectation untouched and let the orphan be logged.
fn resolve_inflight_get(slot: &Arc<Mutex<Option<RpcExpectation>>>, outcome: RpcOutcome) -> bool {
    let mut guard = slot.lock().unwrap();
    let take = matches!(
        guard.as_ref(),
        Some(exp) if exp.kind == RpcKind::Get,
    );
    if take {
        let expectation = guard.take().unwrap();
        let _ = expectation.tx.send(outcome);
        true
    } else {
        false
    }
}

/// Resolve pending publisher-confirm oneshots in response to a `Basic.Ack` or
/// `Basic.Nack` from the broker.
///
/// If `multiple=true`, every pending tag with `tag <= delivery_tag` is
/// resolved with `outcome`. If `multiple=false`, only the exact tag is
/// resolved (if present).
fn resolve_confirms(
    state: &Arc<Mutex<ConfirmsState>>,
    delivery_tag: u64,
    multiple: bool,
    outcome: Confirmation,
) {
    let mut state = state.lock().unwrap();
    if multiple {
        // Drain everything in `..=delivery_tag` from the BTreeMap.
        let mut tail = state.pending.split_off(&(delivery_tag + 1));
        std::mem::swap(&mut state.pending, &mut tail);
        for (_, tx) in tail {
            let _ = tx.send(outcome);
        }
    } else if let Some(tx) = state.pending.remove(&delivery_tag) {
        let _ = tx.send(outcome);
    }
}

/// Drop every pending confirm oneshot. Called when the channel or connection
/// terminates so awaiting `PublishConfirm` futures resolve to
/// `Err(ConnectionClosed)` instead of hanging forever.
fn drop_all_pending_confirms(state: &Arc<Mutex<ConfirmsState>>) {
    let drained: Vec<_> = state
        .lock()
        .unwrap()
        .pending
        .split_off(&0)
        .into_iter()
        .collect();
    // Senders drop here, RecvError fires on the future side.
    drop(drained);
}

/// Set the cancellation reason on every active consumer in `senders` and drop
/// their entries. Called when the channel or connection terminates so any
/// task awaiting `Consumer::next()` learns *why* its stream just ended.
fn set_cancellation_for_all(
    senders: &Arc<Mutex<HashMap<ShortStr, ConsumerEntry>>>,
    reason: CancellationReason,
) {
    let drained: Vec<ConsumerEntry> = senders.lock().unwrap().drain().map(|(_, v)| v).collect();
    for entry in drained {
        // OnceLock::set returns Err if already set; either way is fine.
        let _ = entry.cancellation.set(reason.clone());
        // Drop the sender by going out of scope.
    }
}

/// Receive a complete content (Header + Body frames) from a frame receiver.
async fn receive_content(
    channel_id: u16,
    rx: &mut mpsc::UnboundedReceiver<Frame>,
) -> Result<(ContentHeader, Bytes), Error> {
    // Wait for Header frame
    let header = loop {
        let frame = rx.recv().await.ok_or(Error::ConnectionClosed {
            code: 0,
            reason: "connection closed during content assembly".into(),
        })?;
        match frame {
            Frame::Header(_, h) => break *h,
            other => {
                trace!(channel_id, frame = ?other, "skipping during content header wait");
            }
        }
    };

    if header.body_size == 0 {
        return Ok((header, Bytes::new()));
    }

    // Collect Body frames.
    // Fast path: single body frame (common for small messages) — zero-copy.
    // Slow path: multiple body frames — allocate and concatenate.
    let mut body_opt: Option<Bytes> = None;
    let mut body_buf: Option<BytesMut> = None;
    let mut collected: u64 = 0;

    while collected < header.body_size {
        let frame = rx.recv().await.ok_or(Error::ConnectionClosed {
            code: 0,
            reason: "connection closed during body assembly".into(),
        })?;
        match frame {
            Frame::Body(_, chunk) => {
                collected += chunk.len() as u64;
                match body_opt.take() {
                    None if body_buf.is_none() => {
                        // First chunk — hold it directly (zero-copy)
                        body_opt = Some(chunk);
                    }
                    Some(first) => {
                        // Second chunk arrives — switch to concatenation mode
                        let mut buf = BytesMut::with_capacity(header.body_size as usize);
                        buf.extend_from_slice(&first);
                        buf.extend_from_slice(&chunk);
                        body_buf = Some(buf);
                    }
                    None => {
                        // Already in concatenation mode
                        body_buf.as_mut().unwrap().extend_from_slice(&chunk);
                    }
                }
            }
            other => {
                trace!(channel_id, frame = ?other, "skipping during body collection");
            }
        }
    }

    let body = match body_buf {
        Some(buf) => buf.freeze(),
        None => body_opt.unwrap_or_default(),
    };
    Ok((header, body))
}

/// Options for basic_consume.
#[derive(Clone, Debug, Default)]
pub struct ConsumeOpts {
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub arguments: Table,
}

/// Options for queue_declare.
#[derive(Clone, Debug, Default)]
pub struct QueueDeclareOpts {
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: Table,
}

/// Result of a successful queue_declare.
#[derive(Clone, Debug)]
pub struct QueueDeclareOk {
    pub queue: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

/// Options for queue_delete.
#[derive(Clone, Debug, Default)]
pub struct QueueDeleteOpts {
    pub if_unused: bool,
    pub if_empty: bool,
}

/// Options for exchange_declare.
#[derive(Clone, Debug, Default)]
pub struct ExchangeDeclareOpts {
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: Table,
}

fn unexpected(expected: &'static str, got: &MethodFrame) -> Error {
    Error::Protocol(crate::error::ProtocolError::UnexpectedMethod {
        expected,
        got: format!("{got:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unexpected_error_format() {
        let method = MethodFrame::ConnectionCloseOk;
        let err = unexpected("Queue.DeclareOk", &method);
        match err {
            Error::Protocol(crate::error::ProtocolError::UnexpectedMethod { expected, got }) => {
                assert_eq!(expected, "Queue.DeclareOk");
                assert!(got.contains("ConnectionCloseOk"));
            }
            other => panic!("expected UnexpectedMethod, got {other:?}"),
        }
    }

    #[test]
    fn queue_declare_opts_default() {
        let opts = QueueDeclareOpts::default();
        assert!(!opts.passive);
        assert!(!opts.durable);
        assert!(!opts.exclusive);
        assert!(!opts.auto_delete);
        assert!(opts.arguments.is_empty());
    }

    #[test]
    fn consume_opts_default() {
        let opts = ConsumeOpts::default();
        assert!(!opts.no_local);
        assert!(!opts.no_ack);
        assert!(!opts.exclusive);
        assert!(opts.arguments.is_empty());
    }

    #[test]
    fn queue_delete_opts_default() {
        let opts = QueueDeleteOpts::default();
        assert!(!opts.if_unused);
        assert!(!opts.if_empty);
    }

    #[test]
    fn exchange_declare_opts_default() {
        let opts = ExchangeDeclareOpts::default();
        assert!(!opts.passive);
        assert!(!opts.durable);
        assert!(!opts.auto_delete);
        assert!(!opts.internal);
        assert!(opts.arguments.is_empty());
    }

    #[test]
    fn into_body_zero_copy_for_bytes() {
        // Passing an existing `Bytes` must be true zero-copy: the reference
        // counter increments rather than re-allocating.
        let original = Bytes::from_static(b"hello");
        let cloned = original.clone();
        let body = original.into_body();
        // Same backing pointer (Bytes preserves the underlying allocation).
        assert_eq!(body.as_ptr(), cloned.as_ptr());
        assert_eq!(body.as_ref(), b"hello");
    }

    #[test]
    fn into_body_byte_array_literal() {
        let body = b"hello".into_body();
        assert_eq!(body.as_ref(), b"hello");
    }

    #[test]
    fn into_body_byte_slice() {
        let v = vec![1u8, 2, 3];
        let body = v.as_slice().into_body();
        assert_eq!(body.as_ref(), &[1u8, 2, 3]);
    }

    #[test]
    fn into_body_owned_vec_is_zero_copy() {
        // Vec<u8>::into() reuses the allocation directly.
        let v = vec![1u8, 2, 3, 4];
        let ptr_before = v.as_ptr();
        let body = v.into_body();
        assert_eq!(body.as_ptr(), ptr_before);
    }

    #[test]
    fn into_body_str() {
        let body = "abc".into_body();
        assert_eq!(body.as_ref(), b"abc");
    }

    /// Validates the watch channel semantics that `await_flow_active` relies on.
    /// If this test breaks, the publish gate's correctness is in question.
    #[tokio::test]
    async fn flow_latch_borrow_reflects_latest_send() {
        let (tx, rx) = tokio::sync::watch::channel(true);
        assert!(*rx.borrow());
        tx.send(false).unwrap();
        assert!(!*rx.borrow());
        tx.send(true).unwrap();
        assert!(*rx.borrow());
    }

    #[test]
    fn resolve_confirms_single() {
        let state = Arc::new(Mutex::new(ConfirmsState::default()));
        let (tx, rx) = tokio::sync::oneshot::channel();
        state.lock().unwrap().pending.insert(7, tx);
        resolve_confirms(&state, 7, false, Confirmation::Acked);
        let outcome = rx.blocking_recv().unwrap();
        assert_eq!(outcome, Confirmation::Acked);
        assert!(state.lock().unwrap().pending.is_empty());
    }

    #[test]
    fn resolve_confirms_multiple_drains_range() {
        let state = Arc::new(Mutex::new(ConfirmsState::default()));
        let mut rxs = Vec::new();
        for tag in 1..=5 {
            let (tx, rx) = tokio::sync::oneshot::channel();
            state.lock().unwrap().pending.insert(tag, tx);
            rxs.push((tag, rx));
        }
        // Multiple-Ack at tag 3 resolves tags 1, 2, 3 — 4 and 5 remain.
        resolve_confirms(&state, 3, true, Confirmation::Acked);
        for (tag, rx) in rxs {
            if tag <= 3 {
                assert_eq!(rx.blocking_recv().unwrap(), Confirmation::Acked);
            } else {
                // Sender still alive — recv would block; rely on pending check.
                assert!(state.lock().unwrap().pending.contains_key(&tag));
                drop(rx);
            }
        }
        assert_eq!(state.lock().unwrap().pending.len(), 2);
    }

    #[test]
    fn resolve_confirms_skips_unknown_tag() {
        let state = Arc::new(Mutex::new(ConfirmsState::default()));
        // No pending entries.
        resolve_confirms(&state, 42, false, Confirmation::Acked);
        // Nothing panics, nothing changes.
        assert!(state.lock().unwrap().pending.is_empty());
    }

    #[test]
    fn resolve_confirms_nack_carries_requeue() {
        let state = Arc::new(Mutex::new(ConfirmsState::default()));
        let (tx, rx) = tokio::sync::oneshot::channel();
        state.lock().unwrap().pending.insert(1, tx);
        resolve_confirms(&state, 1, false, Confirmation::Nacked { requeue: true });
        match rx.blocking_recv().unwrap() {
            Confirmation::Nacked { requeue } => assert!(requeue),
            other => panic!("expected Nacked, got {other:?}"),
        }
    }

    #[test]
    fn drop_all_pending_confirms_resolves_with_error() {
        let state = Arc::new(Mutex::new(ConfirmsState::default()));
        let (tx, rx) = tokio::sync::oneshot::channel::<Confirmation>();
        state.lock().unwrap().pending.insert(1, tx);
        drop_all_pending_confirms(&state);
        // Sender dropped → recv returns Err.
        assert!(rx.blocking_recv().is_err());
        assert!(state.lock().unwrap().pending.is_empty());
    }

    /// Cancellation safety invariant: even if the user drops the RPC future
    /// after registering an expectation, the rpc_lock stays held (via the
    /// guard inside the slot) until dispatch consumes the slot. Without
    /// this, the next caller could register a new expectation just in time
    /// to receive the previous caller's reply.
    #[tokio::test]
    async fn rpc_slot_holds_lock_until_dispatch_consumes() {
        let rpc_lock: Arc<tokio::sync::Mutex<()>> = Arc::new(tokio::sync::Mutex::new(()));
        let slot: Arc<Mutex<Option<RpcExpectation>>> = Arc::new(Mutex::new(None));

        // Caller A: acquire owned guard, register expectation, then "cancel"
        // by dropping the receiver below without consuming.
        let guard_a = rpc_lock.clone().lock_owned().await;
        let (tx_a, _rx_a) = tokio::sync::oneshot::channel::<RpcOutcome>();
        *slot.lock().unwrap() = Some(RpcExpectation {
            kind: RpcKind::PlainMethod,
            tx: tx_a,
            _guard: guard_a,
        });
        // _rx_a goes out of scope here — caller A "cancelled".

        // The lock must still be held by the guard inside the slot.
        let try_lock = rpc_lock.try_lock();
        assert!(
            try_lock.is_err(),
            "rpc_lock must stay held while expectation is in slot",
        );
        drop(try_lock);

        // Simulate dispatch consuming the slot (a stale broker reply arriving).
        let consumed = resolve_inflight(
            &slot,
            RpcOutcome::Method(MethodFrame::QueueDeclareOk(queue_method::DeclareOk {
                queue: ShortStr::try_from("q").unwrap(),
                message_count: 0,
                consumer_count: 0,
            })),
        );
        assert!(consumed, "dispatch should have found and consumed the slot");

        // Lock now free for the next caller.
        assert!(
            rpc_lock.try_lock().is_ok(),
            "rpc_lock should be released after dispatch consumes slot",
        );
    }

    #[tokio::test]
    async fn resolve_inflight_returns_false_on_empty_slot() {
        let slot: Arc<Mutex<Option<RpcExpectation>>> = Arc::new(Mutex::new(None));
        let consumed = resolve_inflight(&slot, RpcOutcome::Method(MethodFrame::QueueBindOk));
        assert!(!consumed, "no expectation to consume");
    }

    #[tokio::test]
    async fn resolve_inflight_get_skips_plain_method_expectation() {
        let rpc_lock: Arc<tokio::sync::Mutex<()>> = Arc::new(tokio::sync::Mutex::new(()));
        let slot: Arc<Mutex<Option<RpcExpectation>>> = Arc::new(Mutex::new(None));
        let guard = rpc_lock.clone().lock_owned().await;
        let (tx, _rx) = tokio::sync::oneshot::channel::<RpcOutcome>();
        *slot.lock().unwrap() = Some(RpcExpectation {
            kind: RpcKind::PlainMethod,
            tx,
            _guard: guard,
        });
        // resolve_inflight_get must not steal a PlainMethod expectation —
        // a misrouted GetEmpty would otherwise satisfy a queue_declare RPC.
        let consumed = resolve_inflight_get(&slot, RpcOutcome::GetEmpty);
        assert!(!consumed);
        // Slot still has the PlainMethod expectation.
        assert!(slot.lock().unwrap().is_some());
    }

    /// `changed()` must not return spuriously while the watched value stays
    /// false; it should only return when the sender publishes a new value
    /// (or drops). This is the loop condition `await_flow_active` relies on.
    #[tokio::test]
    async fn flow_latch_changed_unblocks_on_resume() {
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let waiter = tokio::spawn(async move {
            while !*rx.borrow_and_update() {
                rx.changed().await.unwrap();
            }
        });
        // Without setting active=true the waiter must stay blocked.
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        tx.send(true).unwrap();
        // Now the waiter wakes up.
        waiter.await.unwrap();
    }

    /// Validates that publish-admission permits release exactly when the
    /// `OwnedSemaphorePermit` is dropped. The writer relies on dropping the
    /// permit after `write_all` to provide end-to-end backpressure.
    #[tokio::test]
    async fn publish_admission_semaphore_release_unblocks_acquirer() {
        let sem = Arc::new(tokio::sync::Semaphore::new(1024));
        // Hold 1024 permits — semaphore is fully saturated.
        let permit = sem.clone().acquire_many_owned(1024).await.unwrap();

        // A second acquirer should NOT make progress.
        let sem_for_blocked = sem.clone();
        let blocked =
            tokio::spawn(async move { sem_for_blocked.acquire_many_owned(512).await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "second acquirer must wait");

        // Drop the permit (simulates writer flushing the body to the socket).
        drop(permit);

        // Now the second acquirer wakes.
        let _p2 = blocked.await.unwrap();
    }

    /// Documents the over-large-body cap behavior: when body_len exceeds
    /// the semaphore capacity, we acquire only the capacity (becoming the
    /// only in-flight publish until done) instead of deadlocking.
    #[tokio::test]
    async fn publish_admission_caps_oversize_body() {
        // Capacity 100; "body" wants 1000 permits.
        let sem = Arc::new(tokio::sync::Semaphore::new(100));
        let body_len: usize = 1000;
        let want = body_len.min(100);
        let permit = sem.clone().acquire_many_owned(want as u32).await.unwrap();
        // Permit count == capacity — semaphore is now fully saturated.
        assert_eq!(sem.available_permits(), 0);
        drop(permit);
        assert_eq!(sem.available_permits(), 100);
    }
}
