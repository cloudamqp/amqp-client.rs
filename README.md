# amqp-client.rs

An AMQP 0-9-1 client library for Rust, built on [tokio](https://tokio.rs).

## Features

- Full AMQP 0-9-1 protocol support (queues, exchanges, consumers, publish, ack/nack, transactions, publisher confirms)
- TLS via [rustls](https://github.com/rustls/rustls) (`amqps://`)
- Async/await with tokio
- Clean two-layer architecture: runtime-agnostic protocol layer + tokio client layer
- High-level `Client`: typed messages via a `Codec`, `Exchange`/`Queue` handles, one channel per consumer
- Low-level fluent wrappers in `amqp_client_rs::low_level`

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
amqp-client-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Publish

```rust
use amqp_client_rs::{BasicProperties, Connection, QueueDeclareOpts};

#[tokio::main]
async fn main() -> Result<(), amqp_client_rs::Error> {
    let conn = Connection::connect("amqp://guest:guest@localhost").await?;
    let ch = conn.open_channel().await?;

    ch.queue_declare("hello", QueueDeclareOpts::default()).await?;

    ch.basic_publish(
        "",
        "hello",
        b"Hello from amqp-client.rs!",
        BasicProperties::builder()
            .content_type("text/plain")
            .persistent()
            .build()?,
    )
    .await?;

    ch.close().await?;
    conn.close().await?;
    Ok(())
}
```

### Consume

```rust
use amqp_client_rs::{Connection, ConsumeOpts, QueueDeclareOpts};

#[tokio::main]
async fn main() -> Result<(), amqp_client_rs::Error> {
    let conn = Connection::connect("amqp://guest:guest@localhost").await?;
    let ch = conn.open_channel().await?;

    ch.queue_declare("hello", QueueDeclareOpts::default()).await?;

    let mut consumer = ch
        .basic_consume("hello", "my-consumer", ConsumeOpts::default())
        .await?;

    while let Some(msg) = consumer.next().await {
        let body = std::str::from_utf8(&msg.body).unwrap_or("<non-utf8>");
        println!("Received: {body}");
        msg.ack()?;
    }

    Ok(())
}
```

### High-level client

```rust
use amqp_client_rs::{Client, ExchangeDeclareOpts, QueueDeclareOpts, SubscribeOpts};

async fn run() -> Result<(), amqp_client_rs::Error> {
    let client = Client::builder("amqp://guest:guest@localhost")?.build().await?;

    let orders = client.topic("orders", ExchangeDeclareOpts::default()).await?;
    let queue = client.queue("order-events", QueueDeclareOpts::default()).await?;
    queue.bind(&orders, "order.*").await?;   // or by name: queue.bind("amq.topic", ..)

    client.publish(orders.name(), "order.created", "order 42").await?;

    let mut consumer = queue
        .subscribe::<String>(SubscribeOpts { prefetch: 50, ..Default::default() })
        .await?;
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(d) => {
                println!("Received: {}", d.body);
                d.ack()?;
            }
            Err(f) => {
                println!("Undecodable: {}", f.error);
                f.reject(false)?;
            }
        }
    }
    Ok(())
}
```

The default `ByteCodec` publishes `&str`, `String`, byte slices, byte arrays,
`Vec<u8>` and `Bytes`, and decodes into `String`, `Vec<u8>` and `Bytes`. For
typed messages, set your own codec with `.codec(..)`: it implements
`Encode<T>` and `Decode<T>` for the types it supports, e.g. every serde type
(see examples/high_level.rs). Publishing or subscribing with a type the codec
does not support is a compile error
([ADR-0007](docs/adr/0007-codec-traits-keyed-on-body-type.md)).

A delivery that fails to decode arrives as a `DecodeFailure` with the raw
body. The client does not settle it: ack, reject or requeue it yourself
([ADR-0006](docs/adr/0006-consumer-returns-decode-failures.md)).

`TypedConsumer` is also a `futures::Stream` of the same items, so stream
combinators work on it:

```rust
use futures::StreamExt;

consumer
    .for_each_concurrent(10, |r| async move {
        match r {
            Ok(d) => { handle(&d.body).await; let _ = d.ack(); }
            Err(f) => { let _ = f.reject(false); }
        }
    })
    .await;
```

The codec sees each message's properties, exchange and routing key, so it can
pick the type to decode by e.g. the `type` property, and set it on publish
([ADR-0004](docs/adr/0004-codec-decode-sees-message-metadata.md)).

`content_encoding` is handled by the client, not the codec
([ADR-0005](docs/adr/0005-content-encoding-is-a-client-layer.md)). By default
a compressed delivery is a `DecodeFailure`. With the `compression` feature,
`.encoding(Compression::gzip())` on the builder decodes `gzip` and `deflate`
deliveries and publishes gzip.

The client records the exchanges, queues and bindings declared through it.
With `.recovery(RecoveryConfig::default())` on the builder
([ADR-0002](docs/adr/0002-recorded-topology-and-recovery.md)):

- After losing the connection, the client reconnects and declares the recorded
  topology again; a server-named queue gets a new name, and `queue.name()` and
  its bindings follow it. `publish` waits at most `RecoveryConfig::timeout`
  (250 ms) for a new connection, then fails. Consumers carry on.
- A consumer whose channel is closed (e.g. consumer timeout) or whose queue is
  deleted declares the queue and its bindings again and consumes again.
- Unacked messages are redelivered, so make handlers idempotent. `publish` is
  not confirmed: one sent just as the connection drops can be lost.

`publish_confirmed` returns once the broker has the message.
`Error::PublishNacked` and `Error::ChannelClosed` mean it does not;
`Error::ConfirmUncertain` means the connection was lost before the confirm
arrived, so the broker may or may not have it
([ADR-0003](docs/adr/0003-high-level-publisher-confirms.md)).

`cargo run --example recovery` publishes and consumes a tick every second;
restart the broker while it runs to watch it recover.

## Requirements

- Rust 2024 edition
- AMQP 0-9-1 compatible broker (RabbitMQ, LavinMQ, etc.)
- tokio runtime

