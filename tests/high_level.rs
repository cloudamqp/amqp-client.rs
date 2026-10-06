use std::time::Duration;

use amqp_client_rs::{
    BasicProperties, Client, Codec, Connection, ConsumeOpts, Error, ExchangeDeclareOpts,
    ExchangeKind, QueueDeclareOpts, QueueDeleteOpts,
};
use serde::{Deserialize, Serialize};

const AMQP_URI: &str = "amqp://guest:guest@127.0.0.1";

// ---------------------------------------------------------------------------
// Test codec
// ---------------------------------------------------------------------------

struct JsonCodec;

impl Codec for JsonCodec {
    fn content_type(&self) -> &str {
        "application/json"
    }

    fn encode<T: serde::Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Test message type
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Greeting {
    name: String,
    value: u32,
}

// ---------------------------------------------------------------------------
// Helper: unique queue name per test
// ---------------------------------------------------------------------------

fn queue_name(label: &str) -> String {
    format!("amqp-client-rs-hl-test-{}-{}", label, std::process::id())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Build a Client with JsonCodec, declare a queue in the setup closure,
/// publish a typed struct, consume it, verify the body, and ack.
#[tokio::test]
async fn client_publish_and_consume() {
    let queue = queue_name("pub-consume");
    let queue_clone = queue.clone();

    let client = Client::builder(AMQP_URI)
        .unwrap()
        .codec(JsonCodec)
        .setup(move |channel| {
            let q = queue_clone.clone();
            Box::pin(async move {
                channel
                    .queue_declare(&q, QueueDeclareOpts::default())
                    .await?;
                Ok(())
            })
        })
        .build()
        .await
        .unwrap();

    let msg = Greeting {
        name: "world".into(),
        value: 42,
    };

    client.publish("", &queue, &msg).await.unwrap();

    let mut consumer = client
        .consume::<Greeting>(&queue, "hl-pub-consume", ConsumeOpts::default())
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timed out waiting for message")
        .expect("consumer returned None");

    assert_eq!(delivery.body, msg);
    delivery.ack().unwrap();

    // Cleanup
    let ch = client.channel();
    ch.queue_delete(&queue, QueueDeleteOpts::default())
        .await
        .unwrap();
}

/// Publish with custom BasicProperties (correlation_id), verify both
/// content_type (from codec) and correlation_id are preserved on the received
/// message.
#[tokio::test]
async fn client_publish_with_props() {
    let queue = queue_name("pub-props");
    let queue_clone = queue.clone();

    let client = Client::builder(AMQP_URI)
        .unwrap()
        .codec(JsonCodec)
        .setup(move |channel| {
            let q = queue_clone.clone();
            Box::pin(async move {
                channel
                    .queue_declare(&q, QueueDeclareOpts::default())
                    .await?;
                Ok(())
            })
        })
        .build()
        .await
        .unwrap();

    let msg = Greeting {
        name: "props-test".into(),
        value: 99,
    };

    let props = BasicProperties::builder()
        .correlation_id("corr-abc-123")
        .build()
        .unwrap();

    client
        .publish_with_props("", &queue, &msg, props)
        .await
        .unwrap();

    let mut consumer = client
        .consume::<Greeting>(&queue, "hl-pub-props", ConsumeOpts::default())
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timed out waiting for message")
        .expect("consumer returned None");

    assert_eq!(delivery.body, msg);

    // content_type should have been set by the codec
    let ct = delivery.properties.content_type.as_ref().unwrap();
    assert_eq!(ct.as_str().unwrap(), "application/json");

    // correlation_id should be preserved from the custom props
    let corr = delivery.properties.correlation_id.as_ref().unwrap();
    assert_eq!(corr.as_str().unwrap(), "corr-abc-123");

    delivery.ack().unwrap();

    // Cleanup
    let ch = client.channel();
    ch.queue_delete(&queue, QueueDeleteOpts::default())
        .await
        .unwrap();
}

/// The setup closure declares a queue; this test verifies that publishing
/// works, which proves the setup ran and the queue exists.
#[tokio::test]
async fn client_setup_closure_runs() {
    let queue = queue_name("setup-runs");
    let queue_clone = queue.clone();

    let client = Client::builder(AMQP_URI)
        .unwrap()
        .codec(JsonCodec)
        .setup(move |channel| {
            let q = queue_clone.clone();
            Box::pin(async move {
                channel
                    .queue_declare(&q, QueueDeclareOpts::default())
                    .await?;
                Ok(())
            })
        })
        .build()
        .await
        .unwrap();

    // If the setup didn't run, publishing to a non-existent queue would not
    // necessarily error on the default exchange; but consuming would fail.
    // We publish then consume to confirm the queue was declared by setup.
    let msg = Greeting {
        name: "setup".into(),
        value: 1,
    };

    client.publish("", &queue, &msg).await.unwrap();

    let mut consumer = client
        .consume::<Greeting>(&queue, "hl-setup-check", ConsumeOpts::default())
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("timed out waiting for message")
        .expect("consumer returned None");

    assert_eq!(delivery.body.name, "setup");
    delivery.ack().unwrap();

    // Cleanup
    let ch = client.channel();
    ch.queue_delete(&queue, QueueDeleteOpts::default())
        .await
        .unwrap();
}

/// Connect to an unreachable host. The new high-level Client does not retry
/// in build() — first attempt fails fast.
#[tokio::test]
async fn client_build_fails_fast_on_unreachable_host() {
    let result = Client::builder("amqp://guest:guest@127.0.0.1:19999")
        .unwrap()
        .codec(JsonCodec)
        .build()
        .await;

    assert!(
        result.is_err(),
        "expected build() to fail for unreachable host"
    );
}

/// Each exchange shorthand declares an exchange of its kind: redeclaring with
/// the same kind is accepted, a different kind is refused.
#[tokio::test]
async fn client_declares_exchanges_of_each_kind() {
    let client = Client::builder(AMQP_URI).unwrap().build().await.unwrap();
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let opts = ExchangeDeclareOpts::default;
    let name = |label: &str| queue_name(&format!("ex-{label}"));

    let declared = [
        (client.direct(&name("direct"), opts()).await, "direct"),
        (client.fanout(&name("fanout"), opts()).await, "fanout"),
        (client.topic(&name("topic"), opts()).await, "topic"),
        (client.headers(&name("headers"), opts()).await, "headers"),
        (
            client
                .exchange(
                    &name("x-consistent-hash"),
                    ExchangeKind::Custom("x-consistent-hash".into()),
                    opts(),
                )
                .await,
            "x-consistent-hash",
        ),
    ]
    .map(|(exchange, kind)| (exchange.unwrap(), kind));

    for (exchange, kind) in &declared {
        assert_eq!(exchange.name(), name(kind));
        let ch = conn.open_channel().await.unwrap();
        ch.exchange_declare(exchange.name(), kind, opts())
            .await
            .unwrap();
        let other = if *kind == "fanout" {
            "direct"
        } else {
            "fanout"
        };
        let ch = conn.open_channel().await.unwrap();
        let refused = ch.exchange_declare(exchange.name(), other, opts()).await;
        assert!(refused.is_err());
    }

    let ch = conn.open_channel().await.unwrap();
    for (exchange, _) in &declared {
        ch.exchange_delete(exchange.name(), false).await.unwrap();
    }
}

/// A named queue keeps its name; it exists on the broker.
#[tokio::test]
async fn client_declares_a_named_queue() {
    let client = Client::builder(AMQP_URI).unwrap().build().await.unwrap();
    let name = queue_name("named");
    let q = client
        .queue(&name, QueueDeclareOpts::default())
        .await
        .unwrap();
    assert_eq!(q.name(), name);

    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    let ch = client.channel();
    ch.queue_declare(&name, passive).await.unwrap();
    ch.queue_delete(&name, QueueDeleteOpts::default())
        .await
        .unwrap();
}

/// A server-named queue takes the name the broker assigned.
#[tokio::test]
async fn client_declares_a_server_named_queue() {
    let client = Client::builder(AMQP_URI).unwrap().build().await.unwrap();
    let exclusive = QueueDeclareOpts {
        exclusive: true,
        ..Default::default()
    };
    let q = client.queue("", exclusive).await.unwrap();
    assert!(!q.name().is_empty());

    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    client
        .channel()
        .queue_declare(&q.name(), passive)
        .await
        .unwrap();
}
