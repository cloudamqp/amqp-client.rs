use std::sync::{Arc, Mutex, Weak};

use super::codec::{ByteCodec, Codec, Decode};
use super::consumer::{SubscribeOpts, TypedConsumer};
use super::encoding::ContentEncoder;
use super::session::{ClientInner, Session};
use super::topology::{QueueId, RecordedQueue, Topology};
use crate::client::channel::{Channel, ConsumeOpts, QueueDeclareOpts, QueueDeleteOpts};
use crate::client::consumer::Consumer;
use crate::error::Error;
use crate::protocol::table::Table;

/// A queue declared through a [`Client`](crate::Client); declared again after
/// recovery.
pub struct Queue<C: Codec = ByteCodec> {
    id: QueueId,
    topology: Arc<Mutex<Topology>>,
    /// Weak, so queue handles and consumers never keep the client alive.
    client: Weak<ClientInner>,
    codec: Arc<C>,
    encoder: Arc<dyn ContentEncoder>,
    /// The queue's operation lock, shared by every handle to it.
    lock: Arc<tokio::sync::Mutex<()>>,
    /// The last name seen, so `name()` still answers once it is deleted.
    last_name: Arc<Mutex<String>>,
}

impl<C: Codec> Clone for Queue<C> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            topology: self.topology.clone(),
            client: self.client.clone(),
            codec: self.codec.clone(),
            encoder: self.encoder.clone(),
            lock: self.lock.clone(),
            last_name: self.last_name.clone(),
        }
    }
}

impl<C: Codec> Queue<C> {
    pub(crate) fn new(
        id: QueueId,
        client: &Arc<ClientInner>,
        codec: Arc<C>,
        encoder: Arc<dyn ContentEncoder>,
    ) -> Self {
        let (lock, name) = {
            let topology = client.topology.lock().unwrap();
            let queue = topology.queue(id).expect("queue just recorded");
            (queue.lock.clone(), queue.name.clone())
        };
        Self {
            id,
            topology: client.topology.clone(),
            client: Arc::downgrade(client),
            codec,
            encoder,
            lock,
            last_name: Arc::new(Mutex::new(name)),
        }
    }

    /// The queue's current name. For a server-named queue it changes when the
    /// queue is declared again after recovery. After a delete it is the last
    /// name the queue had.
    pub fn name(&self) -> String {
        let mut last_name = self.last_name.lock().unwrap();
        if let Some(name) = self.topology.lock().unwrap().queue_name(self.id) {
            name.clone_into(&mut last_name);
        }
        last_name.clone()
    }

    /// The current name, or `QueueDeleted` once the queue was deleted through
    /// a handle. Never `""`: the broker reads an empty queue name as "the
    /// queue last declared on this channel".
    fn recorded_name(&self) -> Result<String, Error> {
        if self.is_recorded() {
            Ok(self.name())
        } else {
            Err(Error::QueueDeleted { name: self.name() })
        }
    }

    /// Takes the queue's operation lock.
    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lock.lock().await
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
        let _guard = self.lock().await;
        let name = self.recorded_name()?;
        self.client()?
            .with_session(|s| {
                let (name, arguments) = (name.clone(), arguments.clone());
                async move {
                    s.channel()
                        .queue_bind(&name, exchange, routing_key, arguments)
                        .await
                }
            })
            .await?;
        self.topology
            .lock()
            .unwrap()
            .record_binding(self.id, exchange, routing_key, arguments);
        Ok(())
    }

    /// Start consuming on a channel of its own, with `opts.prefetch` set on
    /// that channel. Messages are decoded with the client's codec.
    pub async fn subscribe<T>(&self, opts: SubscribeOpts) -> Result<TypedConsumer<T, C>, Error>
    where
        C: Decode<T>,
    {
        let client = self.client()?;
        let _guard = self.lock().await;
        let name = self.recorded_name()?;
        let (generation, channel, consumer) = client
            .with_session(|s| {
                let (name, opts) = (name.clone(), opts.clone());
                async move {
                    let channel = s.connection.open_channel().await?;
                    let consumer = consume_on(&channel, &name, &opts).await?;
                    Ok((s.generation, channel, consumer))
                }
            })
            .await?;
        let registration = client.recovery().map(|_| client.register_consumer());
        Ok(TypedConsumer::new(
            consumer,
            channel,
            generation,
            registration,
            self.clone(),
            opts,
        ))
    }

    /// Consumer-level recovery: on a new channel of `session`, declare the
    /// queue, the recorded exchanges its bindings use, and its bindings
    /// again, then consume. The caller holds the queue's lock.
    pub(crate) async fn redeclare_and_consume(
        &self,
        session: &Session,
        opts: &SubscribeOpts,
    ) -> Result<(Channel, Consumer), Error> {
        let (queue, bindings, exchanges) = {
            let topology = self.topology.lock().unwrap();
            let Some(queue) = topology.queue(self.id).cloned() else {
                // name() takes the topology lock itself.
                drop(topology);
                return Err(Error::QueueDeleted { name: self.name() });
            };
            let bindings: Vec<_> = topology.bindings_for(self.id).cloned().collect();
            // The recorded exchanges these bindings use; one may be gone too
            // (e.g. auto-deleted along with the queue's binding).
            let exchanges: Vec<_> = topology
                .exchanges()
                .iter()
                .filter(|e| bindings.iter().any(|b| b.exchange == e.name))
                .cloned()
                .collect();
            (queue, bindings, exchanges)
        };
        let (channel, name) = self.ensure_declared(session, queue).await?;
        for e in exchanges {
            channel.exchange_declare(&e.name, &e.kind, e.opts).await?;
        }
        for b in bindings {
            channel
                .queue_bind(&name, &b.exchange, &b.routing_key, b.arguments)
                .await?;
        }
        let consumer = consume_on(&channel, &name, opts).await?;
        Ok((channel, consumer))
    }

    /// Declares the queue again on a new channel. A server-named queue keeps
    /// its name while it still exists; if the broker deleted it, it is
    /// declared anew and gets a new name.
    async fn ensure_declared(
        &self,
        session: &Session,
        queue: RecordedQueue,
    ) -> Result<(Channel, String), Error> {
        let mut channel = session.connection.open_channel().await?;
        if queue.requested_name.is_empty() {
            let passive = QueueDeclareOpts {
                passive: true,
                ..Default::default()
            };
            match channel.queue_declare(&queue.name, passive).await {
                Ok(_) => return Ok((channel, queue.name)),
                // The failed passive declare closed the channel.
                Err(Error::ChannelClosed { code: 404, .. }) => {
                    channel = session.connection.open_channel().await?;
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

    pub(crate) fn client(&self) -> Result<Arc<ClientInner>, Error> {
        self.client.upgrade().ok_or(Error::ConnectionClosed {
            code: 0,
            reason: "client dropped".into(),
        })
    }

    pub(crate) fn weak_client(&self) -> Weak<ClientInner> {
        self.client.clone()
    }

    /// Whether the queue is still recorded, i.e. not deleted via a handle.
    pub(crate) fn is_recorded(&self) -> bool {
        self.topology.lock().unwrap().queue(self.id).is_some()
    }

    pub(crate) fn codec(&self) -> &C {
        &self.codec
    }

    pub(crate) fn encoder(&self) -> &dyn ContentEncoder {
        &*self.encoder
    }

    /// Delete the queue and forget it and its bindings, so recovery does not
    /// declare it again. Its consumers end. Returns the number of messages
    /// deleted.
    pub async fn delete(&self, opts: QueueDeleteOpts) -> Result<u32, Error> {
        let client = self.client()?;
        // Held throughout, so no other operation on this queue (including
        // consumer recovery) overlaps the delete.
        let _guard = self.lock().await;
        let name = self.recorded_name()?;
        // Forget it before sending: the broker cancels the queue's consumers
        // before it replies, and they must not declare it again.
        let taken = self.topology.lock().unwrap().take_queue(self.id);
        let result = client
            .with_session(|s| {
                let (name, opts) = (name.clone(), opts.clone());
                async move { s.channel().queue_delete(&name, opts).await }
            })
            .await;
        match result {
            // Already gone, e.g. deleted elsewhere, or by this delete before a
            // lost reply made it run again.
            Err(Error::ChannelClosed { code: 404, .. }) => Ok(0),
            Err(e) => {
                if let Some(taken) = taken {
                    self.topology.lock().unwrap().restore_queue(taken);
                }
                Err(e)
            }
            ok => ok,
        }
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
        let _guard = self.lock().await;
        let name = self.recorded_name()?;
        self.client()?
            .with_session(|s| {
                let (name, arguments) = (name.clone(), arguments.clone());
                async move {
                    s.channel()
                        .queue_unbind(&name, exchange, routing_key, arguments)
                        .await
                }
            })
            .await?;
        self.topology
            .lock()
            .unwrap()
            .forget_binding(self.id, exchange, routing_key, &arguments);
        Ok(())
    }
}

/// Sets prefetch on `channel` and starts consuming `name`.
async fn consume_on(
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

impl<C: Codec> std::fmt::Debug for Queue<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue").field("name", &self.name()).finish()
    }
}
