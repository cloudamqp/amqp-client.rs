use crate::error::Error;
use crate::protocol::properties::BasicProperties;
use crate::protocol::table::Table;

use super::channel::{Channel, IntoBody};

/// A fluent wrapper around a declared exchange, delegating operations to the channel.
pub struct Exchange<'ch> {
    channel: &'ch Channel,
    name: String,
}

impl<'ch> Exchange<'ch> {
    pub(crate) fn new(channel: &'ch Channel, name: String) -> Self {
        Self { channel, name }
    }

    /// Returns the exchange name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Publish a message to this exchange.
    ///
    /// `body` accepts anything that implements [`IntoBody`].
    pub async fn publish(
        &self,
        routing_key: &str,
        body: impl IntoBody,
        props: BasicProperties,
    ) -> Result<(), Error> {
        self.channel
            .basic_publish(&self.name, routing_key, body, props)
            .await
    }

    /// Bind another exchange to this exchange (this exchange is the source).
    pub async fn bind(
        &self,
        destination: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        self.channel
            .exchange_bind(destination, &self.name, routing_key, arguments)
            .await
    }

    /// Unbind another exchange from this exchange (this exchange is the source).
    pub async fn unbind(
        &self,
        destination: &str,
        routing_key: &str,
        arguments: Table,
    ) -> Result<(), Error> {
        self.channel
            .exchange_unbind(destination, &self.name, routing_key, arguments)
            .await
    }

    /// Delete this exchange.
    pub async fn delete(&self, if_unused: bool) -> Result<(), Error> {
        self.channel.exchange_delete(&self.name, if_unused).await
    }
}
