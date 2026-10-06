use std::sync::{Arc, Mutex};

use super::topology::{QueueId, Topology};

/// A queue declared through a [`Client`](crate::Client); declared again after
/// recovery.
#[derive(Clone)]
pub struct Queue {
    id: QueueId,
    topology: Arc<Mutex<Topology>>,
}

impl Queue {
    pub(crate) fn new(id: QueueId, topology: Arc<Mutex<Topology>>) -> Self {
        Self { id, topology }
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
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue").field("name", &self.name()).finish()
    }
}
