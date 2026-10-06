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

// `JsonCodec` is your own `Codec`, e.g. over serde_json (see examples/high_level.rs).
async fn run() -> Result<(), amqp_client_rs::Error> {
    let client = Client::builder("amqp://guest:guest@localhost")?
        .codec(JsonCodec)
        .build()
        .await?;

    let orders = client.topic("orders", ExchangeDeclareOpts::default()).await?;
    let queue = client.queue("order-events", QueueDeclareOpts::default()).await?;
    queue.bind(&orders, "order.*").await?;   // or by name: queue.bind("amq.topic", ..)

    client.publish(orders.name(), "order.created", &"order 42").await?;

    let mut consumer = queue
        .subscribe::<String>(SubscribeOpts { prefetch: 50, ..Default::default() })
        .await?;
    while let Some(delivery) = consumer.next().await {
        println!("Received: {}", delivery.body);
        delivery.ack()?;
    }
    Ok(())
}
```

The client records the exchanges, queues and bindings declared through it;
automatic recovery that declares them again is designed in
[ADR-0002](docs/adr/0002-recorded-topology-and-recovery.md) but not built yet.

## Requirements

- Rust 2024 edition
- AMQP 0-9-1 compatible broker (RabbitMQ, LavinMQ, etc.)
- tokio runtime

