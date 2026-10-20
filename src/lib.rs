//! An AMQP 0-9-1 client library built on tokio.
//!
//! amqp-client.rs speaks the AMQP 0-9-1 wire protocol used by RabbitMQ, LavinMQ, and
//! other compatible brokers. It exposes two layers:
//!
//! - A **low-level** API ([`Connection`], [`Channel`], [`Consumer`]) that
//!   mirrors the protocol's primitives — open channels, declare queues and
//!   exchanges, publish, consume, ack/nack, transactions, publisher confirms.
//! - A **high-level** [`Client`] that adds typed serialization through a
//!   user-supplied [`Codec`], records the topology declared through it, and
//!   optionally recovers from connection loss.
//!
//! TLS (`amqps://`) is supported via rustls.
//!
//! ## Quick start
//!
//! ```no_run
//! use amqp_client_rs::{BasicProperties, Connection, QueueDeclareOpts};
//!
//! # async fn run() -> Result<(), amqp_client_rs::Error> {
//! let conn = Connection::connect("amqp://guest:guest@localhost").await?;
//! let ch = conn.open_channel().await?;
//! ch.queue_declare("hello", QueueDeclareOpts::default()).await?;
//! ch.basic_publish("", "hello", b"hi", BasicProperties::new()).await?;
//! ch.close().await?;
//! conn.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the `examples/` directory for more, including the high-level [`Client`].

pub(crate) mod client;
pub mod error;
pub(crate) mod high_level;
pub(crate) mod protocol;

pub use client::channel::{
    Channel, Confirmation, ConsumeOpts, ExchangeDeclareOpts, IntoBody, PublishConfirm,
    QueueDeclareOk, QueueDeclareOpts, QueueDeleteOpts, ReturnedMessage, Returns,
};
pub use client::config::Config;
pub use client::connection::Connection;
pub use client::consumer::{CancellationReason, Consumer};
pub use client::message::{DeliverMessage, GetMessage};
pub use error::{Error, ProtocolError};
#[cfg(feature = "compression")]
pub use high_level::Compression;
pub use high_level::{
    ByteCodec, Client, ClientBuilder, Codec, ContentEncoder, Decode, DecodeFailure, Encode,
    Exchange, ExchangeKind, ExchangeOpts, Identity, MessageMeta, Queue, QueueOpts, RecoveryConfig,
    SubscribeOpts, TypedConsumer, TypedDelivery,
};
pub use protocol::properties::BasicProperties;
pub use protocol::table::{FieldValue, Table};
pub use protocol::types::{LongStr, ShortStr};

/// Fluent wrappers that borrow a low-level [`Channel`].
pub mod low_level {
    pub use crate::client::exchange::Exchange;
    pub use crate::client::queue::Queue;
}
