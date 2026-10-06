# amqp-client.rs

An AMQP 0-9-1 client library for Rust, built on [tokio](https://tokio.rs).

## Features

- Full AMQP 0-9-1 protocol support (queues, exchanges, consumers, publish, ack/nack, transactions, publisher confirms)
- TLS via [rustls](https://github.com/rustls/rustls) (`amqps://`)
- Async/await with tokio
- Clean two-layer architecture: runtime-agnostic protocol layer + tokio client layer
- Fluent API with `Queue` and `Exchange` wrappers

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

## Requirements

- Rust 2024 edition
- AMQP 0-9-1 compatible broker (RabbitMQ, LavinMQ, etc.)
- tokio runtime

