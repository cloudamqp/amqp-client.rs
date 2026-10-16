//! Options for declares through the high-level [`Client`](super::Client),
//! durable by default (ADR-0008).

use crate::client::channel::{ExchangeDeclareOpts, QueueDeclareOpts};
use crate::protocol::table::Table;

/// Options for [`Client::queue`](super::Client::queue).
///
/// A named queue is durable. A server-named queue (`""`) is exclusive and
/// transient, and so is any exclusive queue. Nothing is auto-delete: the
/// queue would go with its last consumer, before consumer recovery could
/// consume from it again. What the caller sets is used as is.
#[derive(Clone, Debug, Default)]
pub struct QueueOpts {
    durable: Option<bool>,
    exclusive: Option<bool>,
    auto_delete: Option<bool>,
    arguments: Table,
}

impl QueueOpts {
    /// Survive a broker restart.
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = Some(durable);
        self
    }

    /// Used by this connection only, and deleted when it closes.
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = Some(exclusive);
        self
    }

    /// Deleted when its last consumer is cancelled.
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = Some(auto_delete);
        self
    }

    /// Optional arguments, e.g. `x-queue-type`.
    pub fn arguments(mut self, arguments: Table) -> Self {
        self.arguments = arguments;
        self
    }

    /// The `queue.declare` options for a queue named `name`.
    pub(crate) fn resolve(self, name: &str) -> QueueDeclareOpts {
        let server_named = name.is_empty();
        let exclusive = self.exclusive.unwrap_or(server_named);
        QueueDeclareOpts {
            passive: false,
            durable: self.durable.unwrap_or(!server_named && !exclusive),
            exclusive,
            auto_delete: self.auto_delete.unwrap_or(false),
            arguments: self.arguments,
        }
    }
}

/// Options for [`Client::exchange`](super::Client::exchange) and its
/// shorthands. An exchange is durable unless set otherwise.
#[derive(Clone, Debug)]
pub struct ExchangeOpts {
    durable: bool,
    auto_delete: bool,
    internal: bool,
    arguments: Table,
}

impl Default for ExchangeOpts {
    fn default() -> Self {
        Self {
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: Table::new(),
        }
    }
}

impl ExchangeOpts {
    /// Survive a broker restart.
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Deleted when its last binding is removed.
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }

    /// Not published to directly, only through exchange-to-exchange bindings.
    pub fn internal(mut self, internal: bool) -> Self {
        self.internal = internal;
        self
    }

    /// Optional arguments, e.g. `alternate-exchange`.
    pub fn arguments(mut self, arguments: Table) -> Self {
        self.arguments = arguments;
        self
    }

    /// The `exchange.declare` options.
    pub(crate) fn resolve(self) -> ExchangeDeclareOpts {
        ExchangeDeclareOpts {
            passive: false,
            durable: self.durable,
            auto_delete: self.auto_delete,
            internal: self.internal,
            arguments: self.arguments,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::table::FieldValue;
    use crate::protocol::types::ShortStr;

    #[test]
    fn a_named_queue_is_durable() {
        let o = QueueOpts::default().resolve("q");
        assert!(o.durable && !o.exclusive && !o.auto_delete && !o.passive);
    }

    #[test]
    fn a_server_named_queue_is_exclusive_and_transient() {
        let o = QueueOpts::default().resolve("");
        assert!(!o.durable && o.exclusive && !o.auto_delete);
    }

    #[test]
    fn an_exclusive_queue_is_transient() {
        let o = QueueOpts::default().exclusive(true).resolve("q");
        assert!(!o.durable && o.exclusive && !o.auto_delete);
    }

    #[test]
    fn what_the_caller_sets_wins() {
        let o = QueueOpts::default().durable(false).resolve("q");
        assert!(!o.durable);
        let o = QueueOpts::default()
            .exclusive(false)
            .auto_delete(false)
            .durable(true)
            .resolve("");
        assert!(o.durable && !o.exclusive && !o.auto_delete);
        let o = QueueOpts::default()
            .exclusive(true)
            .durable(true)
            .resolve("q");
        assert!(o.durable && o.exclusive);
    }

    #[test]
    fn an_auto_delete_queue_stays_durable() {
        let o = QueueOpts::default().auto_delete(true).resolve("q");
        assert!(o.durable && o.auto_delete);
    }

    #[test]
    fn queue_arguments_pass_through() {
        let mut args = Table::new();
        args.insert(
            ShortStr::try_from("x-queue-type").unwrap(),
            FieldValue::from("quorum"),
        );
        let o = QueueOpts::default().arguments(args.clone()).resolve("q");
        assert_eq!(o.arguments, args);
    }

    #[test]
    fn an_exchange_is_durable() {
        let o = ExchangeOpts::default().resolve();
        assert!(o.durable && !o.auto_delete && !o.internal && !o.passive);
    }

    #[test]
    fn exchange_settings_pass_through() {
        let mut args = Table::new();
        args.insert(
            ShortStr::try_from("alternate-exchange").unwrap(),
            FieldValue::from("ae"),
        );
        let o = ExchangeOpts::default()
            .durable(false)
            .auto_delete(true)
            .internal(true)
            .arguments(args.clone())
            .resolve();
        assert!(!o.durable && o.auto_delete && o.internal);
        assert_eq!(o.arguments, args);
    }
}
