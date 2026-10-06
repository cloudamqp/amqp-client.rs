use amqp_client_rs::{Client, QueueDeclareOpts, SubscribeOpts};
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
        .build()
        .await?;

    let queue = client
        .queue("my-queue", QueueDeclareOpts::default())
        .await?;

    client
        .publish(
            "",
            &queue.name(),
            &MyMessage {
                text: "hello".into(),
            },
        )
        .await?;

    let mut consumer = queue
        .subscribe::<MyMessage>(SubscribeOpts::default())
        .await?;

    // Without `.recovery(RecoveryConfig::default())` on the builder, the
    // consumer ends (next() returns None) when its channel or connection does;
    // with it, the consumer carries on after reconnecting.
    while let Some(delivery) = consumer.next().await {
        println!("Received: {:?}", delivery.body);
        delivery.ack()?;
    }

    Ok(())
}
