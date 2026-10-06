use std::time::Duration;

use amqp_client_rs::{
    BasicProperties, Client, Codec, Connection, Error, ExchangeDeclareOpts, ExchangeKind,
    QueueDeclareOpts, QueueDeleteOpts, RecoveryConfig, SubscribeOpts, TypedConsumer, TypedDelivery,
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

async fn json_client() -> Client<JsonCodec> {
    Client::builder(AMQP_URI)
        .unwrap()
        .codec(JsonCodec)
        .build()
        .await
        .unwrap()
}

async fn next_within<T: serde::de::DeserializeOwned + 'static>(
    consumer: &mut TypedConsumer<T, JsonCodec>,
    timeout: Duration,
) -> Option<TypedDelivery<T>> {
    tokio::time::timeout(timeout, consumer.next())
        .await
        .ok()
        .flatten()
}

fn greeting(value: u32) -> Greeting {
    Greeting {
        name: "world".into(),
        value,
    }
}

/// Publish a typed struct to a declared queue, subscribe, verify the body,
/// and ack.
#[tokio::test]
async fn client_publish_and_subscribe() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();

    client.publish("", &q.name(), &greeting(42)).await.unwrap();

    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    let delivery = next_within(&mut consumer, Duration::from_secs(5))
        .await
        .expect("no message");
    assert_eq!(delivery.body, greeting(42));
    delivery.ack().unwrap();
}

/// Publish with custom BasicProperties (correlation_id), verify both
/// content_type (from codec) and correlation_id are preserved on the received
/// message.
#[tokio::test]
async fn client_publish_with_props() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();

    let props = BasicProperties::builder()
        .correlation_id("corr-abc-123")
        .build()
        .unwrap();
    client
        .publish_with_props("", &q.name(), &greeting(99), props)
        .await
        .unwrap();

    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    let delivery = next_within(&mut consumer, Duration::from_secs(5))
        .await
        .expect("no message");
    assert_eq!(delivery.body, greeting(99));

    let ct = delivery.properties.content_type.as_ref().unwrap();
    assert_eq!(ct.as_str().unwrap(), "application/json");
    let corr = delivery.properties.correlation_id.as_ref().unwrap();
    assert_eq!(corr.as_str().unwrap(), "corr-abc-123");
    delivery.ack().unwrap();
}

/// With prefetch 1, the second message arrives only after the first is acked.
#[tokio::test]
async fn subscribe_applies_prefetch() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    client.publish("", &q.name(), &greeting(1)).await.unwrap();
    client.publish("", &q.name(), &greeting(2)).await.unwrap();

    let opts = SubscribeOpts {
        prefetch: 1,
        ..Default::default()
    };
    let mut consumer = q.subscribe::<Greeting>(opts).await.unwrap();
    let first = next_within(&mut consumer, Duration::from_secs(5))
        .await
        .expect("no first message");
    assert!(
        next_within(&mut consumer, Duration::from_millis(200))
            .await
            .is_none()
    );

    first.ack().unwrap();
    let second = next_within(&mut consumer, Duration::from_secs(5))
        .await
        .expect("no second message");
    assert_eq!(second.body, greeting(2));
}

#[test]
fn subscribe_opts_default_prefetch_is_20() {
    assert_eq!(SubscribeOpts::default().prefetch, 20);
}

/// A caller-set consumer tag is used; without one the broker assigns one.
#[tokio::test]
async fn subscribe_uses_the_given_consumer_tag() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();

    let opts = SubscribeOpts {
        consumer_tag: Some("my-tag".into()),
        ..Default::default()
    };
    let tagged = q.subscribe::<Greeting>(opts).await.unwrap();
    assert_eq!(tagged.tag(), "my-tag");

    let untagged = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    assert!(!untagged.tag().is_empty());
    assert_ne!(untagged.tag(), "my-tag");
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

fn exclusive() -> QueueDeclareOpts {
    QueueDeclareOpts {
        exclusive: true,
        ..Default::default()
    }
}

async fn has_message(client: &Client, queue: &str) -> bool {
    client
        .channel()
        .basic_get(queue, true)
        .await
        .unwrap()
        .is_some()
}

/// A binding routes messages from the exchange; unbinding stops that.
#[tokio::test]
async fn queue_binds_and_unbinds() {
    let client = Client::builder(AMQP_URI).unwrap().build().await.unwrap();
    let ex = client
        .topic(&queue_name("bind-ex"), ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let q = client.queue("", exclusive()).await.unwrap();

    q.bind(&ex, "order.*").await.unwrap();
    client
        .publish(ex.name(), "order.new", &vec![1u8])
        .await
        .unwrap();
    assert!(has_message(&client, &q.name()).await);

    q.unbind(&ex, "order.*").await.unwrap();
    client
        .publish(ex.name(), "order.new", &vec![1u8])
        .await
        .unwrap();
    assert!(!has_message(&client, &q.name()).await);

    client
        .channel()
        .exchange_delete(ex.name(), false)
        .await
        .unwrap();
}

/// A queue can be bound to an exchange by name, e.g. a built-in one.
#[tokio::test]
async fn queue_binds_by_exchange_name() {
    let client = Client::builder(AMQP_URI).unwrap().build().await.unwrap();
    let q = client.queue("", exclusive()).await.unwrap();
    let key = queue_name("by-name");

    q.bind("amq.topic", &key).await.unwrap();
    client.publish("amq.topic", &key, &vec![1u8]).await.unwrap();
    assert!(has_message(&client, &q.name()).await);
}

/// Deleting a queue through its handle removes it on the broker and ends its
/// consumers.
#[tokio::test]
async fn queue_delete_ends_its_consumers() {
    let client = json_client().await;
    let name = queue_name("delete");
    let q = client
        .queue(&name, QueueDeclareOpts::default())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();

    q.delete(QueueDeleteOpts::default()).await.unwrap();

    let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
    assert!(matches!(ended, Ok(None)), "consumer did not end");
    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    assert!(ch.queue_declare(&name, passive).await.is_err());
}

async fn recovering_json_client() -> Client<JsonCodec> {
    Client::builder(AMQP_URI)
        .unwrap()
        .codec(JsonCodec)
        .recovery(RecoveryConfig::default())
        .build()
        .await
        .unwrap()
}

/// Acks the first delivery twice; the broker closes the consumer's channel
/// with PRECONDITION_FAILED (unknown delivery tag).
async fn close_consumer_channel(
    client: &Client<JsonCodec>,
    queue: &str,
    consumer: &mut TypedConsumer<Greeting, JsonCodec>,
) {
    client.publish("", queue, &greeting(1)).await.unwrap();
    let first = next_within(consumer, Duration::from_secs(5))
        .await
        .expect("no first message");
    first.ack().unwrap();
    first.ack().unwrap();
}

/// With recovery, a consumer whose channel the broker closed consumes again
/// on a new channel.
#[tokio::test]
async fn consumer_recovers_after_channel_closed() {
    let client = recovering_json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    close_consumer_channel(&client, &q.name(), &mut consumer).await;

    client.publish("", &q.name(), &greeting(2)).await.unwrap();
    let delivery = next_within(&mut consumer, Duration::from_secs(5))
        .await
        .expect("consumer did not recover");
    assert_eq!(delivery.body, greeting(2));
}

/// Without recovery, the consumer ends when its channel is closed.
#[tokio::test]
async fn consumer_ends_after_channel_closed_without_recovery() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    close_consumer_channel(&client, &q.name(), &mut consumer).await;

    let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
    assert!(matches!(ended, Ok(None)), "consumer did not end");
}
