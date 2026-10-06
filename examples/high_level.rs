use amqp_client_rs::{Client, ConsumeOpts, QueueDeclareOpts};
use serde::{Deserialize, Serialize};

// Bring your own codec (e.g. JSON via serde_json)
struct JsonCodec;

impl amqp_client_rs::Codec for JsonCodec {
    fn content_type(&self) -> &str {
        "application/json"
    }
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, amqp_client_rs::Error> {
        serde_json::to_vec(value).map_err(|e| amqp_client_rs::Error::Codec(e.to_string()))
    }
    fn decode<T: serde::de::DeserializeOwned>(
        &self,
        bytes: &[u8],
    ) -> Result<T, amqp_client_rs::Error> {
        serde_json::from_slice(bytes).map_err(|e| amqp_client_rs::Error::Codec(e.to_string()))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MyMessage {
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), amqp_client_rs::Error> {
    tracing_subscriber::fmt().init();

    // build() does a single connection attempt — no built-in retry.
    let client = Client::builder("amqp://guest:guest@localhost")?
        .codec(JsonCodec)
        .setup(|ch| {
            Box::pin(async move {
                ch.queue_declare("my-queue", QueueDeclareOpts::default())
                    .await?;
                Ok(())
            })
        })
        .build()
        .await?;

    client
        .publish(
            "",
            "my-queue",
            &MyMessage {
                text: "hello".into(),
            },
        )
        .await?;

    let mut consumer = client
        .consume::<MyMessage>("my-queue", "my-consumer", ConsumeOpts::default())
        .await?;

    // The Client does not recover from connection drops. When next() returns
    // None, the consumer is done — inspect the underlying Consumer's
    // cancellation reason if you need to react.
    while let Some(delivery) = consumer.next().await {
        println!("Received: {:?}", delivery.body);
        delivery.ack()?;
    }

    Ok(())
}
