use std::sync::Arc;

use tracing::{debug, info};

use super::codec::{ByteCodec, Codec, Encode};
use super::declare::{ExchangeOpts, QueueOpts};
use super::encoding::{ContentEncoder, Identity};
use super::exchange::{Exchange, ExchangeKind};
use super::queue::Queue;
use super::recovery::RecoveryConfig;
use super::session::ClientInner;
use crate::client::channel::{Channel, Confirmation};
use crate::client::config::Config;
use crate::error::Error;
use crate::protocol::properties::BasicProperties;
use crate::protocol::types::ShortStr;

/// A high-level AMQP client with typed publish/consume and codec-based
/// serialization.
///
/// The codec is part of the client's type. By default it is [`ByteCodec`],
/// which only handles `Vec<u8>`. Call [`ClientBuilder::codec`] to install a
/// codec that handles your message types.
///
/// [`ClientBuilder::build`] connects once and fails fast. Without
/// [`ClientBuilder::recovery`], losing the connection is terminal: `publish`
/// returns an error and consumers' `next()` returns `None`. With it, the client
/// reconnects and declares the recorded exchanges, queues and bindings again,
/// operations wait at most `RecoveryConfig::timeout` for that, and consumers
/// carry on, also after their channel closes or their queue is cancelled.
/// Dropping the `Client` ends recovery and every consumer.
///
/// Use [`Client::builder`] to create a new client.
pub struct Client<C: Codec = ByteCodec> {
    codec: Arc<C>,
    encoder: Arc<dyn ContentEncoder>,
    /// The only strong reference: dropping the `Client` closes the
    /// connection, ending every consumer.
    inner: Arc<ClientInner>,
}

impl Client<ByteCodec> {
    /// Create a [`ClientBuilder`] for the given AMQP URI.
    ///
    /// The URI is parsed eagerly so that invalid URIs fail fast before any
    /// connection attempt. The returned builder defaults to [`ByteCodec`];
    /// chain [`ClientBuilder::codec`] to install a typed codec.
    pub fn builder(uri: &str) -> Result<ClientBuilder<ByteCodec>, Error> {
        let config = Config::from_uri(uri)?;
        Ok(ClientBuilder {
            config,
            codec: Arc::new(ByteCodec),
            encoder: Arc::new(Identity),
            recovery: None,
        })
    }
}

impl<C: Codec> Client<C> {
    /// Returns the current session's channel.
    ///
    /// Useful for low-level operations the high-level API doesn't surface
    /// (e.g. `queue_purge`, transactions). It is session-bound: nothing
    /// declared through it is recorded, and it is not restored by recovery.
    pub fn channel(&self) -> Arc<Channel> {
        self.inner.current().channel()
    }

    /// Declare an exchange and record it, so it is declared again after
    /// recovery. Durable unless `opts` say otherwise.
    pub async fn exchange(
        &self,
        name: &str,
        kind: ExchangeKind,
        opts: ExchangeOpts,
    ) -> Result<Exchange, Error> {
        let kind = kind.as_str();
        let opts = opts.resolve();
        self.inner
            .with_session(|s| {
                let opts = opts.clone();
                async move { s.channel().exchange_declare(name, kind, opts).await }
            })
            .await?;
        self.inner
            .topology
            .lock()
            .unwrap()
            .record_exchange(name, kind, opts);
        Ok(Exchange::new(name))
    }

    /// Declare a direct exchange. See [`Client::exchange`].
    pub async fn direct(&self, name: &str, opts: ExchangeOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Direct, opts).await
    }

    /// Declare a fanout exchange. See [`Client::exchange`].
    pub async fn fanout(&self, name: &str, opts: ExchangeOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Fanout, opts).await
    }

    /// Declare a topic exchange. See [`Client::exchange`].
    pub async fn topic(&self, name: &str, opts: ExchangeOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Topic, opts).await
    }

    /// Declare a headers exchange. See [`Client::exchange`].
    pub async fn headers(&self, name: &str, opts: ExchangeOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Headers, opts).await
    }

    /// Declare a queue and record it, so it is declared again after recovery.
    /// An empty `name` lets the broker name the queue. See [`QueueOpts`] for
    /// what is durable by default.
    pub async fn queue(&self, name: &str, opts: QueueOpts) -> Result<Queue<C>, Error> {
        let opts = opts.resolve(name);
        let ok = self
            .inner
            .with_session(|s| {
                let opts = opts.clone();
                async move { s.channel().queue_declare(name, opts).await }
            })
            .await?;
        let id = self
            .inner
            .topology
            .lock()
            .unwrap()
            .record_queue(name, &ok.queue, opts);
        Ok(Queue::new(
            id,
            &self.inner,
            self.codec.clone(),
            self.encoder.clone(),
        ))
    }

    /// Publish a serialized message to an exchange with the given routing key.
    ///
    /// The codec's `content_type` is set automatically on the message properties.
    /// `body` must be a type the codec implements [`Encode`] for; any other
    /// type does not compile:
    ///
    /// ```compile_fail
    /// # async fn f(client: amqp_client::Client) -> Result<(), amqp_client::Error> {
    /// client.publish("", "q", &42u32).await // ByteCodec: Encode<u32> is not satisfied
    /// # }
    /// ```
    pub async fn publish<T: ?Sized>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
    ) -> Result<(), Error>
    where
        C: Encode<T>,
    {
        let props = BasicProperties::builder()
            .content_type(self.codec.content_type())
            .build()?;
        self.publish_inner(exchange, routing_key, body, props).await
    }

    /// Publish a serialized message with custom properties.
    ///
    /// If the provided properties do not already have a `content_type` set, the
    /// codec's content type is used. The codec's `encode` may add to or
    /// overwrite them.
    pub async fn publish_with_props<T: ?Sized>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
        mut props: BasicProperties,
    ) -> Result<(), Error>
    where
        C: Encode<T>,
    {
        if props.content_type.is_none() {
            props.content_type =
                Some(ShortStr::try_from(self.codec.content_type()).map_err(Error::Protocol)?);
        }
        self.publish_inner(exchange, routing_key, body, props).await
    }

    /// Publish a serialized message and wait until the broker confirms it.
    ///
    /// Returns `Ok` once acked, [`Error::PublishNacked`] if the broker
    /// rejected it, and `ChannelClosed` if the broker closed the confirm
    /// channel because of this publish (e.g. 404 for a missing exchange): in
    /// those cases the broker does not have the message.
    /// [`Error::ConfirmUncertain`] means it was sent but no confirm arrived
    /// (connection lost, `rpc_timeout` passed, or the broker closed the
    /// confirm channel with other publishes in flight): the broker may or may
    /// not have it. With recovery on, a
    /// publish that could not be sent because the connection was already
    /// gone is sent once more on the recovered connection. Confirms are
    /// requested on a channel of their own.
    pub async fn publish_confirmed<T: ?Sized>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
    ) -> Result<(), Error>
    where
        C: Encode<T>,
    {
        self.publish_confirmed_with_props(exchange, routing_key, body, BasicProperties::default())
            .await
    }

    /// [`Client::publish_confirmed`] with custom properties. If they have no
    /// `content_type`, the codec's is used.
    pub async fn publish_confirmed_with_props<T: ?Sized>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
        mut props: BasicProperties,
    ) -> Result<(), Error>
    where
        C: Encode<T>,
    {
        if props.content_type.is_none() {
            props.content_type =
                Some(ShortStr::try_from(self.codec.content_type()).map_err(Error::Protocol)?);
        }
        let encoded = self.encode(body, &mut props)?;
        // Errors here mean the message was not (fully) sent, so with_session
        // may send it again on a recovered session.
        let (channel, confirm) = self
            .inner
            .with_session(|s| {
                let (encoded, props) = (encoded.clone(), props.clone());
                async move {
                    let channel = s.confirm_channel().await?;
                    let confirm = channel
                        .basic_publish_confirmed(exchange, routing_key, encoded, props, false)
                        .await?;
                    Ok((channel, confirm))
                }
            })
            .await?;
        let confirm = tokio::time::timeout(self.inner.rpc_timeout(), confirm)
            .await
            .ok();
        let closed_by_broker = channel
            .closed_error()
            .zip(channel.pending_confirms_at_close());
        confirm_outcome(confirm, closed_by_broker)
    }

    /// The wire body for `body`: the codec's, content-encoded (ADR-0005).
    fn encode<T: ?Sized>(&self, body: &T, props: &mut BasicProperties) -> Result<Vec<u8>, Error>
    where
        C: Encode<T>,
    {
        let encoded = self.codec.encode(body, props)?;
        self.encoder.encode(encoded, props)
    }

    async fn publish_inner<T: ?Sized>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
        mut props: BasicProperties,
    ) -> Result<(), Error>
    where
        C: Encode<T>,
    {
        let encoded = self.encode(body, &mut props)?;
        self.inner
            .with_session(|s| {
                let (encoded, props) = (encoded.clone(), props.clone());
                async move {
                    s.channel()
                        .basic_publish(exchange, routing_key, encoded, props)
                        .await
                }
            })
            .await
    }
}

/// The outcome of a confirmed publish, from its confirm (`None` if none came
/// within `rpc_timeout`) and, if the broker closed the confirm channel, that
/// close with the number of confirms then pending (ADR-0003).
fn confirm_outcome(
    confirm: Option<Result<Confirmation, Error>>,
    closed_by_broker: Option<(Error, usize)>,
) -> Result<(), Error> {
    match confirm {
        Some(Ok(Confirmation::Acked)) => Ok(()),
        Some(Ok(Confirmation::Nacked { .. })) => Err(Error::PublishNacked),
        Some(Err(e)) => match closed_by_broker {
            // The confirm channel carries only confirmed publishes: if this
            // one alone was pending, it caused the close and was discarded.
            Some((closed, 1)) => Err(closed),
            // Another publish caused it; this one may have been routed.
            Some((closed, _)) => Err(Error::ConfirmUncertain {
                reason: format!(
                    "the broker closed the confirm channel with other publishes in flight: \
                     {closed}"
                ),
            }),
            None => Err(Error::ConfirmUncertain {
                reason: format!("connection lost before the confirm arrived: {e}"),
            }),
        },
        None => Err(Error::ConfirmUncertain {
            reason: "no confirm within rpc_timeout".into(),
        }),
    }
}

/// Builder for constructing a [`Client`].
///
/// Created via [`Client::builder`]. Defaults to [`ByteCodec`]; calling
/// [`ClientBuilder::codec`] returns a `ClientBuilder<C>` whose `build`
/// produces a [`Client<C>`] with that codec installed.
pub struct ClientBuilder<C: Codec = ByteCodec> {
    config: Config,
    codec: Arc<C>,
    encoder: Arc<dyn ContentEncoder>,
    recovery: Option<RecoveryConfig>,
}

impl<C: Codec> ClientBuilder<C> {
    /// Replace the codec, changing the builder's type parameter to the new
    /// codec.
    pub fn codec<NewC: Codec>(self, codec: NewC) -> ClientBuilder<NewC> {
        ClientBuilder {
            config: self.config,
            codec: Arc::new(codec),
            encoder: self.encoder,
            recovery: self.recovery,
        }
    }

    /// Set the [`ContentEncoder`] that runs between the wire and the codec,
    /// e.g. `Compression::gzip()` with the `compression` feature (ADR-0005).
    /// Defaults to [`Identity`].
    pub fn encoding<E: ContentEncoder>(mut self, encoder: E) -> Self {
        self.encoder = Arc::new(encoder);
        self
    }

    /// Turn on automatic recovery (ADR-0002). Without it, losing a channel or
    /// the connection ends consumers.
    pub fn recovery(mut self, config: RecoveryConfig) -> Self {
        self.recovery = Some(config);
        self
    }

    /// Build and connect the client.
    ///
    /// Performs a single connection attempt. If it fails, the error is
    /// returned immediately — there is no built-in retry. Callers that want
    /// retry should wrap this call in their own loop.
    pub async fn build(self) -> Result<Client<C>, Error> {
        debug!("connecting to AMQP broker");
        let inner = ClientInner::connect(self.config, self.recovery).await?;
        info!("connected to AMQP broker");

        Ok(Client {
            codec: self.codec,
            encoder: self.encoder,
            inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn closed(code: u16) -> Error {
        Error::ChannelClosed {
            channel_id: 2,
            code,
            reason: "NOT_FOUND".into(),
        }
    }

    fn lost() -> Error {
        Error::ConnectionClosed {
            code: 0,
            reason: "channel closed before confirm arrived".into(),
        }
    }

    #[test]
    fn an_ack_is_ok() {
        assert!(confirm_outcome(Some(Ok(Confirmation::Acked)), None).is_ok());
    }

    #[test]
    fn a_nack_is_publish_nacked() {
        let nacked = confirm_outcome(Some(Ok(Confirmation::Nacked { requeue: false })), None);
        assert!(matches!(nacked, Err(Error::PublishNacked)));
    }

    #[test]
    fn a_close_with_only_this_confirm_pending_is_certain() {
        let outcome = confirm_outcome(Some(Err(lost())), Some((closed(404), 1)));
        assert!(matches!(
            outcome,
            Err(Error::ChannelClosed { code: 404, .. })
        ));
    }

    #[test]
    fn a_close_with_other_confirms_pending_is_uncertain() {
        let outcome = confirm_outcome(Some(Err(lost())), Some((closed(404), 3)));
        assert!(matches!(outcome, Err(Error::ConfirmUncertain { .. })));
    }

    #[test]
    fn a_lost_connection_is_uncertain() {
        let outcome = confirm_outcome(Some(Err(lost())), None);
        assert!(matches!(outcome, Err(Error::ConfirmUncertain { .. })));
    }

    #[test]
    fn no_confirm_in_time_is_uncertain() {
        let outcome = confirm_outcome(None, None);
        assert!(matches!(outcome, Err(Error::ConfirmUncertain { .. })));
    }
}
