use std::sync::{Arc, Mutex, Weak};

use super::codec::{ByteCodec, Codec};
use super::consumer::{SubscribeOpts, TypedConsumer};
use super::recovery::RecoveryConfig;
use super::topology::{QueueId, Topology};
use crate::client::channel::{Channel, ConsumeOpts, QueueDeleteOpts};
use crate::client::connection::Connection;
use crate::error::Error;
use crate::protocol::table::Table;

/// A queue declared through a [`Client`](crate::Client); declared again after
/// recovery.
pub struct Queue<C: Codec = ByteCodec> {
    id: QueueId,
    topology: Arc<Mutex<Topology>>,
    channel: Arc<Channel>,
    /// Weak, so queue handles and consumers never keep the connection alive
    /// after the `Client` is dropped.
    connection: Weak<Connection>,
    codec: Arc<C>,
    recovery: Option<RecoveryConfig>,
}

impl<C: Codec> Clone for Queue<C> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            topology: self.topology.clone(),
            channel: self.channel.clone(),
            connection: self.connection.clone(),
            codec: self.codec.clone(),
            recovery: self.recovery.clone(),
        }
    }
}

impl<C: Codec> Queue<C> {
    pub(crate) fn new(
        id: QueueId,
        topology: Arc<Mutex<Topology>>,
        channel: Arc<Channel>,
        connection: Weak<Connection>,
        codec: Arc<C>,
        recovery: Option<RecoveryConfig>,
    ) -> Self {
        Self {
            id,
            topology,
            channel,
            connection,
            codec,
            recovery,
        }
    }

    /// The queue's current name. For a server-named queue it changes when the
    /// queue is declared again after recovery.
    pub fn name(&self) -> String {
        self.topology
            .lock()
            .unwrap()
            .queue_name(self.id)
            .unwrap_or_default()
            .to_owned()
    }

    /// Bind the queue to an exchange, given as an [`Exchange`](crate::Exchange)
    /// or by name, and record the binding. Binding by name does not record the
    /// exchange itself.
    pub async fn bind(&self, exchange: impl AsRef<str>, routing_key: &str) -> Result<(), Error> {
        self.bind_with_args(exchange, routing_key, Table::default())
            .await
    }

    /// Like [`Queue::bind`], with binding arguments (e.g. for a headers
    /// exchange).
    pub async fn bind_with_args(
        &self,
        exchange: impl AsRef<str>,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        let exchange = exchange.as_ref();
        self.channel
            .queue_bind(&self.name(), exchange, routing_key, arguments.clone())
            .await?;
        self.topology
            .lock()
            .unwrap()
            .record_binding(self.id, exchange, routing_key, arguments);
        Ok(())
    }

    /// Start consuming on a channel of its own, with `opts.prefetch` set on
    /// that channel. Messages are decoded with the client's codec.
    pub async fn subscribe<T: serde::de::DeserializeOwned + 'static>(
        &self,
        opts: SubscribeOpts,
    ) -> Result<TypedConsumer<T, C>, Error> {
        let connection = self.connection.upgrade().ok_or(Error::ConnectionClosed {
            code: 0,
            reason: "client dropped".into(),
        })?;
        let channel = connection.open_channel().await?;
        channel.basic_qos(opts.prefetch, false).await?;
        let consume = ConsumeOpts {
            no_local: false,
            no_ack: opts.no_ack,
            exclusive: opts.exclusive,
            arguments: opts.arguments,
        };
        let tag = opts.consumer_tag.as_deref().unwrap_or("");
        let consumer = channel.basic_consume(&self.name(), tag, consume).await?;
        Ok(TypedConsumer::new(consumer, channel, self.codec.clone()))
    }

    /// Delete the queue and forget it and its bindings, so recovery does not
    /// declare it again. Its consumers end. Returns the number of messages
    /// deleted.
    pub async fn delete(&self, opts: QueueDeleteOpts) -> Result<u32, Error> {
        let count = self.channel.queue_delete(&self.name(), opts).await?;
        self.topology.lock().unwrap().forget_queue(self.id);
        Ok(count)
    }

    /// Remove a binding and forget it.
    pub async fn unbind(&self, exchange: impl AsRef<str>, routing_key: &str) -> Result<(), Error> {
        self.unbind_with_args(exchange, routing_key, Table::default())
            .await
    }

    /// Like [`Queue::unbind`], for a binding made with arguments.
    pub async fn unbind_with_args(
        &self,
        exchange: impl AsRef<str>,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        let exchange = exchange.as_ref();
        self.channel
            .queue_unbind(&self.name(), exchange, routing_key, arguments.clone())
            .await?;
        self.topology
            .lock()
            .unwrap()
            .forget_binding(self.id, exchange, routing_key, &arguments);
        Ok(())
    }
}

impl<C: Codec> std::fmt::Debug for Queue<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue").field("name", &self.name()).finish()
    }
}
