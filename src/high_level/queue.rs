use std::sync::{Arc, Mutex, Weak};

use super::codec::{ByteCodec, Codec};
use super::consumer::{SubscribeOpts, TypedConsumer};
use super::recovery::RecoveryConfig;
use super::topology::{QueueId, RecordedQueue, Topology};
use crate::client::channel::{Channel, ConsumeOpts, QueueDeclareOpts, QueueDeleteOpts};
use crate::client::connection::Connection;
use crate::client::consumer::Consumer;
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
        let channel = self.open_channel().await?;
        let consumer = self.consume_on(&channel, &self.name(), &opts).await?;
        Ok(TypedConsumer::new(consumer, channel, self.clone(), opts))
    }

    /// Consumer-level recovery: on a new channel, declare the queue and its
    /// bindings again, then consume.
    pub(crate) async fn redeclare_and_consume(
        &self,
        opts: &SubscribeOpts,
    ) -> Result<(Channel, Consumer), Error> {
        let (queue, bindings) = {
            let topology = self.topology.lock().unwrap();
            let Some(queue) = topology.queue(self.id).cloned() else {
                return Err(Error::ConnectionClosed {
                    code: 0,
                    reason: "queue deleted".into(),
                });
            };
            let bindings: Vec<_> = topology.bindings_for(self.id).cloned().collect();
            (queue, bindings)
        };
        let (channel, name) = self.ensure_declared(queue).await?;
        for b in bindings {
            channel
                .queue_bind(&name, &b.exchange, &b.routing_key, b.arguments)
                .await?;
        }
        let consumer = self.consume_on(&channel, &name, opts).await?;
        Ok((channel, consumer))
    }

    /// Declares the queue again on a new channel. A server-named queue keeps
    /// its name while it still exists; if the broker deleted it, it is
    /// declared anew and gets a new name.
    async fn ensure_declared(&self, queue: RecordedQueue) -> Result<(Channel, String), Error> {
        let mut channel = self.open_channel().await?;
        if queue.requested_name.is_empty() {
            let passive = QueueDeclareOpts {
                passive: true,
                ..Default::default()
            };
            match channel.queue_declare(&queue.name, passive).await {
                Ok(_) => return Ok((channel, queue.name)),
                // The failed passive declare closed the channel.
                Err(Error::ChannelClosed { code: 404, .. }) => {
                    channel = self.open_channel().await?;
                }
                Err(e) => return Err(e),
            }
        }
        let ok = channel
            .queue_declare(&queue.requested_name, queue.opts)
            .await?;
        self.topology
            .lock()
            .unwrap()
            .rename_queue(self.id, &ok.queue);
        Ok((channel, ok.queue))
    }

    async fn open_channel(&self) -> Result<Channel, Error> {
        let connection = self.connection.upgrade().ok_or(Error::ConnectionClosed {
            code: 0,
            reason: "client dropped".into(),
        })?;
        connection.open_channel().await
    }

    async fn consume_on(
        &self,
        channel: &Channel,
        name: &str,
        opts: &SubscribeOpts,
    ) -> Result<Consumer, Error> {
        channel.basic_qos(opts.prefetch, false).await?;
        let consume = ConsumeOpts {
            no_local: false,
            no_ack: opts.no_ack,
            exclusive: opts.exclusive,
            arguments: opts.arguments.clone(),
        };
        let tag = opts.consumer_tag.as_deref().unwrap_or("");
        channel.basic_consume(name, tag, consume).await
    }

    /// Whether the queue is still recorded, i.e. not deleted via a handle.
    pub(crate) fn is_recorded(&self) -> bool {
        self.topology.lock().unwrap().queue(self.id).is_some()
    }

    pub(crate) fn recovery(&self) -> Option<&RecoveryConfig> {
        self.recovery.as_ref()
    }

    pub(crate) fn codec(&self) -> &C {
        &self.codec
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
