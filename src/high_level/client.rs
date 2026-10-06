use std::sync::{Arc, Mutex};

use tracing::{debug, info};

use super::codec::{ByteCodec, Codec};
use super::exchange::{Exchange, ExchangeKind};
use super::queue::Queue;
use super::topology::Topology;
use crate::client::channel::{Channel, ExchangeDeclareOpts, QueueDeclareOpts};
use crate::client::config::Config;
use crate::client::connection::Connection;
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
/// The client connects once at [`ClientBuilder::build`] time and holds that
/// connection for its lifetime. If the underlying connection drops,
/// `publish` returns an error and consumers' `next()` returns `None`
/// terminally — the client does not recover on its own. Recovery strategy is
/// left to the caller (e.g. build a fresh `Client`).
///
/// Use [`Client::builder`] to create a new client.
pub struct Client<C: Codec = ByteCodec> {
    codec: Arc<C>,
    /// The only strong reference: dropping the `Client` closes the connection,
    /// ending every consumer.
    connection: Arc<Connection>,
    channel: Arc<Channel>,
    topology: Arc<Mutex<Topology>>,
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
        })
    }
}

impl<C: Codec> Client<C> {
    /// Returns a clone of the underlying `Arc<Channel>`.
    ///
    /// Useful for low-level operations the high-level API doesn't surface
    /// (e.g. `queue_delete`, `queue_purge`, transactions).
    pub fn channel(&self) -> Arc<Channel> {
        self.channel.clone()
    }

    /// Declare an exchange and record it, so it is declared again after
    /// recovery.
    pub async fn exchange(
        &self,
        name: &str,
        kind: ExchangeKind,
        opts: ExchangeDeclareOpts,
    ) -> Result<Exchange, Error> {
        self.channel
            .exchange_declare(name, kind.as_str(), opts.clone())
            .await?;
        self.topology
            .lock()
            .unwrap()
            .record_exchange(name, kind.as_str(), opts);
        Ok(Exchange::new(name))
    }

    /// Declare a direct exchange. See [`Client::exchange`].
    pub async fn direct(&self, name: &str, opts: ExchangeDeclareOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Direct, opts).await
    }

    /// Declare a fanout exchange. See [`Client::exchange`].
    pub async fn fanout(&self, name: &str, opts: ExchangeDeclareOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Fanout, opts).await
    }

    /// Declare a topic exchange. See [`Client::exchange`].
    pub async fn topic(&self, name: &str, opts: ExchangeDeclareOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Topic, opts).await
    }

    /// Declare a headers exchange. See [`Client::exchange`].
    pub async fn headers(&self, name: &str, opts: ExchangeDeclareOpts) -> Result<Exchange, Error> {
        self.exchange(name, ExchangeKind::Headers, opts).await
    }

    /// Declare a queue and record it, so it is declared again after recovery.
    /// An empty `name` lets the broker name the queue.
    pub async fn queue(&self, name: &str, opts: QueueDeclareOpts) -> Result<Queue<C>, Error> {
        let ok = self.channel.queue_declare(name, opts.clone()).await?;
        let id = self
            .topology
            .lock()
            .unwrap()
            .record_queue(name, &ok.queue, opts);
        Ok(Queue::new(
            id,
            self.topology.clone(),
            self.channel.clone(),
            Arc::downgrade(&self.connection),
            self.codec.clone(),
        ))
    }

    /// Publish a serialized message to an exchange with the given routing key.
    ///
    /// The codec's `content_type` is set automatically on the message properties.
    pub async fn publish<T: serde::Serialize + 'static>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
    ) -> Result<(), Error> {
        let props = BasicProperties::builder()
            .content_type(self.codec.content_type())
            .build()?;
        self.publish_inner(exchange, routing_key, body, props).await
    }

    /// Publish a serialized message with custom properties.
    ///
    /// If the provided properties do not already have a `content_type` set, the
    /// codec's content type is used.
    pub async fn publish_with_props<T: serde::Serialize + 'static>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
        mut props: BasicProperties,
    ) -> Result<(), Error> {
        if props.content_type.is_none() {
            props.content_type =
                Some(ShortStr::try_from(self.codec.content_type()).map_err(Error::Protocol)?);
        }
        self.publish_inner(exchange, routing_key, body, props).await
    }

    async fn publish_inner<T: serde::Serialize + 'static>(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &T,
        props: BasicProperties,
    ) -> Result<(), Error> {
        let encoded = self.codec.encode(body)?;
        self.channel
            .basic_publish(exchange, routing_key, encoded, props)
            .await
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
}

impl<C: Codec> ClientBuilder<C> {
    /// Replace the codec, changing the builder's type parameter to the new
    /// codec.
    pub fn codec<NewC: Codec>(self, codec: NewC) -> ClientBuilder<NewC> {
        ClientBuilder {
            config: self.config,
            codec: Arc::new(codec),
        }
    }

    /// Build and connect the client.
    ///
    /// Performs a single connection attempt. If it fails, the error is
    /// returned immediately — there is no built-in retry. Callers that want
    /// retry should wrap this call in their own loop.
    pub async fn build(self) -> Result<Client<C>, Error> {
        debug!("connecting to AMQP broker");
        let connection = Connection::connect_with_config(self.config).await?;
        let channel = Arc::new(connection.open_channel().await?);
        info!("connected to AMQP broker");

        Ok(Client {
            codec: self.codec,
            connection: Arc::new(connection),
            channel,
            topology: Arc::default(),
        })
    }
}
