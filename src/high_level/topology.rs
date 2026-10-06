//! The exchanges, queues and bindings declared through the high-level
//! `Client`, kept so they can be declared again after recovery (ADR-0002).

use crate::{ExchangeDeclareOpts, QueueDeclareOpts, Table};

/// Identifies a recorded queue; stable across renames of server-named queues.
pub(crate) type QueueId = u64;

#[derive(Clone)]
pub(crate) struct RecordedExchange {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) opts: ExchangeDeclareOpts,
}

#[derive(Clone)]
pub(crate) struct RecordedQueue {
    pub(crate) id: QueueId,
    /// The name passed to `queue_declare`; empty for a server-named queue.
    pub(crate) requested_name: String,
    /// The name the broker currently knows the queue by.
    pub(crate) name: String,
    pub(crate) opts: QueueDeclareOpts,
}

#[derive(Clone, PartialEq)]
pub(crate) struct RecordedBinding {
    pub(crate) queue: QueueId,
    pub(crate) exchange: String,
    pub(crate) routing_key: String,
    pub(crate) arguments: Table,
}

/// A queue and its bindings taken out of the topology by `take_queue`.
pub(crate) struct TakenQueue {
    queue: RecordedQueue,
    bindings: Vec<RecordedBinding>,
}

/// Recorded topology, listed in the order it is declared again on recovery.
#[derive(Default)]
pub(crate) struct Topology {
    next_id: QueueId,
    exchanges: Vec<RecordedExchange>,
    queues: Vec<RecordedQueue>,
    bindings: Vec<RecordedBinding>,
}

impl Topology {
    pub(crate) fn record_exchange(&mut self, name: &str, kind: &str, opts: ExchangeDeclareOpts) {
        let exchange = RecordedExchange {
            name: name.into(),
            kind: kind.into(),
            opts,
        };
        match self.exchanges.iter_mut().find(|e| e.name == name) {
            Some(existing) => *existing = exchange,
            None => self.exchanges.push(exchange),
        }
    }

    /// Records a queue the broker declared as `name`. A named queue that is
    /// already recorded keeps its id; a server-named one is always new.
    pub(crate) fn record_queue(
        &mut self,
        requested_name: &str,
        name: &str,
        opts: QueueDeclareOpts,
    ) -> QueueId {
        if !requested_name.is_empty()
            && let Some(q) = self
                .queues
                .iter_mut()
                .find(|q| q.requested_name == requested_name)
        {
            q.opts = opts;
            return q.id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.queues.push(RecordedQueue {
            id,
            requested_name: requested_name.into(),
            name: name.into(),
            opts,
        });
        id
    }

    pub(crate) fn rename_queue(&mut self, id: QueueId, name: &str) {
        if let Some(q) = self.queues.iter_mut().find(|q| q.id == id) {
            q.name = name.into();
        }
    }

    pub(crate) fn queue(&self, id: QueueId) -> Option<&RecordedQueue> {
        self.queues.iter().find(|q| q.id == id)
    }

    pub(crate) fn queue_name(&self, id: QueueId) -> Option<&str> {
        self.queue(id).map(|q| q.name.as_str())
    }

    /// Forgets a queue and its bindings, returning them so a failed delete
    /// can restore them.
    pub(crate) fn take_queue(&mut self, id: QueueId) -> Option<TakenQueue> {
        let index = self.queues.iter().position(|q| q.id == id)?;
        let queue = self.queues.remove(index);
        let (bindings, kept) = std::mem::take(&mut self.bindings)
            .into_iter()
            .partition(|b| b.queue == id);
        self.bindings = kept;
        Some(TakenQueue { queue, bindings })
    }

    pub(crate) fn restore_queue(&mut self, taken: TakenQueue) {
        self.queues.push(taken.queue);
        self.bindings.extend(taken.bindings);
    }

    pub(crate) fn record_binding(
        &mut self,
        queue: QueueId,
        exchange: &str,
        routing_key: &str,
        arguments: Table,
    ) {
        let binding = RecordedBinding {
            queue,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments,
        };
        if !self.bindings.contains(&binding) {
            self.bindings.push(binding);
        }
    }

    pub(crate) fn forget_binding(
        &mut self,
        queue: QueueId,
        exchange: &str,
        routing_key: &str,
        arguments: &Table,
    ) {
        self.bindings.retain(|b| {
            !(b.queue == queue
                && b.exchange == exchange
                && b.routing_key == routing_key
                && &b.arguments == arguments)
        });
    }

    pub(crate) fn exchanges(&self) -> &[RecordedExchange] {
        &self.exchanges
    }

    pub(crate) fn queues(&self) -> &[RecordedQueue] {
        &self.queues
    }

    pub(crate) fn bindings(&self) -> &[RecordedBinding] {
        &self.bindings
    }

    pub(crate) fn bindings_for(&self, queue: QueueId) -> impl Iterator<Item = &RecordedBinding> {
        self.bindings.iter().filter(move |b| b.queue == queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExchangeDeclareOpts, QueueDeclareOpts, Table};

    fn exchange_names(t: &Topology) -> Vec<&str> {
        t.exchanges().iter().map(|e| e.name.as_str()).collect()
    }

    fn queue_names(t: &Topology) -> Vec<&str> {
        t.queues().iter().map(|q| q.name.as_str()).collect()
    }

    fn binding_keys(t: &Topology, id: QueueId) -> Vec<(&str, &str)> {
        t.bindings_for(id)
            .map(|b| (b.exchange.as_str(), b.routing_key.as_str()))
            .collect()
    }

    #[test]
    fn exchanges_are_listed_in_declaration_order() {
        let mut t = Topology::default();
        t.record_exchange("b", "topic", ExchangeDeclareOpts::default());
        t.record_exchange("a", "direct", ExchangeDeclareOpts::default());
        assert_eq!(exchange_names(&t), ["b", "a"]);
    }

    #[test]
    fn redeclaring_an_exchange_replaces_it_in_place() {
        let mut t = Topology::default();
        t.record_exchange("a", "topic", ExchangeDeclareOpts::default());
        t.record_exchange("b", "topic", ExchangeDeclareOpts::default());
        let durable = ExchangeDeclareOpts {
            durable: true,
            ..Default::default()
        };
        t.record_exchange("a", "fanout", durable);
        assert_eq!(exchange_names(&t), ["a", "b"]);
        assert_eq!(t.exchanges()[0].kind, "fanout");
        assert!(t.exchanges()[0].opts.durable);
    }

    #[test]
    fn redeclaring_a_named_queue_returns_the_same_id() {
        let mut t = Topology::default();
        let a = t.record_queue("q", "q", QueueDeclareOpts::default());
        let b = t.record_queue("q", "q", QueueDeclareOpts::default());
        assert_eq!(a, b);
        assert_eq!(queue_names(&t), ["q"]);
    }

    #[test]
    fn server_named_queues_are_distinct() {
        let mut t = Topology::default();
        let a = t.record_queue("", "amq.gen-1", QueueDeclareOpts::default());
        let b = t.record_queue("", "amq.gen-2", QueueDeclareOpts::default());
        assert_ne!(a, b);
        assert_eq!(queue_names(&t), ["amq.gen-1", "amq.gen-2"]);
    }

    #[test]
    fn renaming_a_server_named_queue_keeps_its_bindings() {
        let mut t = Topology::default();
        let id = t.record_queue("", "amq.gen-1", QueueDeclareOpts::default());
        t.record_binding(id, "ex", "k", Table::default());
        t.rename_queue(id, "amq.gen-2");
        assert_eq!(t.queue_name(id), Some("amq.gen-2"));
        assert_eq!(t.queues()[0].requested_name, "");
        assert_eq!(binding_keys(&t, id), [("ex", "k")]);
    }

    #[test]
    fn identical_bindings_are_recorded_once() {
        let mut t = Topology::default();
        let id = t.record_queue("q", "q", QueueDeclareOpts::default());
        t.record_binding(id, "ex", "k", Table::default());
        t.record_binding(id, "ex", "k", Table::default());
        t.record_binding(id, "ex", "other", Table::default());
        assert_eq!(binding_keys(&t, id), [("ex", "k"), ("ex", "other")]);
    }

    #[test]
    fn unbinding_forgets_the_binding() {
        let mut t = Topology::default();
        let id = t.record_queue("q", "q", QueueDeclareOpts::default());
        t.record_binding(id, "ex", "k", Table::default());
        t.record_binding(id, "ex", "other", Table::default());
        t.forget_binding(id, "ex", "k", &Table::default());
        assert_eq!(binding_keys(&t, id), [("ex", "other")]);
    }

    #[test]
    fn bindings_for_returns_only_that_queues_bindings() {
        let mut t = Topology::default();
        let a = t.record_queue("a", "a", QueueDeclareOpts::default());
        let b = t.record_queue("b", "b", QueueDeclareOpts::default());
        t.record_binding(a, "ex", "ka", Table::default());
        t.record_binding(b, "ex", "kb", Table::default());
        assert_eq!(binding_keys(&t, a), [("ex", "ka")]);
        assert_eq!(binding_keys(&t, b), [("ex", "kb")]);
    }

    #[test]
    fn a_taken_queue_can_be_restored_with_its_bindings() {
        let mut t = Topology::default();
        let a = t.record_queue("a", "a", QueueDeclareOpts::default());
        let b = t.record_queue("b", "b", QueueDeclareOpts::default());
        t.record_binding(a, "ex", "ka", Table::default());
        t.record_binding(b, "ex", "kb", Table::default());

        let taken = t.take_queue(a).unwrap();
        assert_eq!(t.queue_name(a), None);
        assert!(binding_keys(&t, a).is_empty());

        t.restore_queue(taken);
        assert_eq!(t.queue_name(a), Some("a"));
        assert_eq!(binding_keys(&t, a), [("ex", "ka")]);
        assert_eq!(binding_keys(&t, b), [("ex", "kb")]);
    }

    #[test]
    fn deleting_a_queue_forgets_it_and_its_bindings() {
        let mut t = Topology::default();
        let a = t.record_queue("a", "a", QueueDeclareOpts::default());
        let b = t.record_queue("b", "b", QueueDeclareOpts::default());
        t.record_binding(a, "ex", "ka", Table::default());
        t.record_binding(b, "ex", "kb", Table::default());
        t.take_queue(a);
        assert_eq!(queue_names(&t), ["b"]);
        assert_eq!(t.queue_name(a), None);
        assert!(binding_keys(&t, a).is_empty());
        assert_eq!(t.bindings().len(), 1);
    }
}
