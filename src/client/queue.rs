use crate::error::Error;
use crate::protocol::properties::BasicProperties;
use crate::protocol::table::Table;

use super::channel::{Channel, ConsumeOpts, IntoBody, QueueDeleteOpts};
use super::consumer::Consumer;

/// A fluent wrapper around a declared queue, delegating operations to the channel.
pub struct Queue<'ch> {
    channel: &'ch Channel,
    name: String,
}

impl<'ch> Queue<'ch> {
    pub(crate) fn new(channel: &'ch Channel, name: String) -> Self {
        Self { channel, name }
    }

    /// Returns the queue name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Bind this queue to an exchange.
    pub async fn bind(
        &self,
        exchange: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        self.channel
            .queue_bind(&self.name, exchange, routing_key, arguments)
            .await
    }

    /// Unbind this queue from an exchange.
    pub async fn unbind(
        &self,
        exchange: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        self.channel
            .queue_unbind(&self.name, exchange, routing_key, arguments)
            .await
    }

    /// Purge all messages from this queue. Returns the number of messages purged.
    pub async fn purge(&self) -> Result<u32, Error> {
        self.channel.queue_purge(&self.name).await
    }

    /// Delete this queue. Returns the number of messages purged.
    pub async fn delete(&self, opts: QueueDeleteOpts) -> Result<u32, Error> {
        self.channel.queue_delete(&self.name, opts).await
    }

    /// Publish a message to the default exchange with this queue's name as routing key.
    ///
    /// `body` accepts anything that implements [`IntoBody`].
    pub async fn publish(&self, body: impl IntoBody, props: BasicProperties) -> Result<(), Error> {
        self.channel
            .basic_publish("", &self.name, body, props)
            .await
    }

    /// Start consuming messages from this queue. Returns a `Consumer` that yields messages.
    pub async fn consume(&self, consumer_tag: &str, opts: ConsumeOpts) -> Result<Consumer, Error> {
        self.channel
            .basic_consume(&self.name, consumer_tag, opts)
            .await
    }
}
