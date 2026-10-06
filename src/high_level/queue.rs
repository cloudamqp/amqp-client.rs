use std::sync::{Arc, Mutex};

use super::topology::{QueueId, Topology};
use crate::client::channel::Channel;
use crate::error::Error;
use crate::protocol::table::Table;

/// A queue declared through a [`Client`](crate::Client); declared again after
/// recovery.
#[derive(Clone)]
pub struct Queue {
    id: QueueId,
    topology: Arc<Mutex<Topology>>,
    channel: Arc<Channel>,
}

impl Queue {
    pub(crate) fn new(id: QueueId, topology: Arc<Mutex<Topology>>, channel: Arc<Channel>) -> Self {
        Self {
            id,
            topology,
            channel,
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

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue").field("name", &self.name()).finish()
    }
}
