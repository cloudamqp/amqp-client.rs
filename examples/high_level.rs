use amqp_client_rs::{
    BasicProperties, Client, Codec, Decode, Encode, Error, MessageMeta, QueueDeclareOpts,
    SubscribeOpts,
};
use serde::{Deserialize, Serialize};

// Bring your own codec (e.g. JSON via serde_json)
struct JsonCodec;

impl Codec for JsonCodec {
    fn content_type(&self) -> &str {
        "application/json"
    }
}

// `props` and `meta` let a codec set and read e.g. the `type` property;
// this one only needs the body.
impl<T: Serialize + ?Sized> Encode<T> for JsonCodec {
    fn encode(&self, value: &T, _props: &mut BasicProperties) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
    }
}

impl<T: serde::de::DeserializeOwned> Decode<T> for JsonCodec {
    fn decode(&self, bytes: &[u8], _meta: &MessageMeta<'_>) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MyMessage {
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
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
        match delivery {
            Ok(d) => {
                println!("Received: {:?}", d.body);
                d.ack()?;
            }
            // Not decodable: the client leaves it to us to settle.
            Err(f) => {
                println!("Undecodable delivery: {}", f.error);
                f.reject(false)?;
            }
        }
    }

    Ok(())
}
