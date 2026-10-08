use std::time::Duration;

use amqp_client_rs::{
    BasicProperties, Client, Codec, Connection, DecodeFailure, Error, ExchangeDeclareOpts,
    ExchangeKind, MessageMeta, QueueDeclareOpts, QueueDeleteOpts, RecoveryConfig, SubscribeOpts,
    TypedConsumer, TypedDelivery,
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

    fn encode<T: serde::Serialize>(
        &self,
        value: &T,
        _props: &mut BasicProperties,
    ) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decode<T: serde::de::DeserializeOwned>(
        &self,
        bytes: &[u8],
        _meta: &MessageMeta<'_>,
    ) -> Result<T, Error> {
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
        .map(|r| r.expect("delivery failed to decode"))
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

#[allow(clippy::result_large_err)]
async fn next_raw<T: serde::de::DeserializeOwned + 'static, C: Codec>(
    consumer: &mut TypedConsumer<T, C>,
) -> Result<TypedDelivery<T>, DecodeFailure> {
    tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("no message")
        .expect("consumer ended")
}

/// A body the codec cannot decode reaches the caller unsettled (ADR-0006): it
/// is redelivered when nacked with requeue, and gone once rejected.
#[tokio::test]
async fn an_undecodable_delivery_is_left_to_the_caller() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    client
        .channel()
        .basic_publish("", &q.name(), b"not json", BasicProperties::default())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();

    let f = next_raw(&mut consumer).await.unwrap_err();
    assert!(matches!(f.error, Error::Codec(_)), "{:?}", f.error);
    assert_eq!(f.body, b"not json");
    assert!(!f.redelivered);
    f.nack(true).unwrap();

    let f = next_raw(&mut consumer).await.unwrap_err();
    assert!(f.redelivered);
    f.reject(false).unwrap();

    client.publish("", &q.name(), &greeting(1)).await.unwrap();
    let d = next_raw(&mut consumer).await.unwrap();
    assert_eq!(d.body, greeting(1), "the rejected delivery came back");
    d.ack().unwrap();
}

/// Settling a delivery from a `no_ack` consumer fails locally and leaves the
/// consumer running.
#[tokio::test]
async fn settling_a_no_ack_delivery_keeps_the_consumer() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts {
            no_ack: true,
            ..Default::default()
        })
        .await
        .unwrap();

    client.publish("", &q.name(), &greeting(1)).await.unwrap();
    let d = next_raw(&mut consumer).await.unwrap();
    assert!(matches!(d.ack(), Err(Error::NoAck)));

    client.publish("", &q.name(), &greeting(2)).await.unwrap();
    let d = next_raw(&mut consumer).await.unwrap();
    assert_eq!(d.body, greeting(2));
}

/// JSON that sets `type` on encode and decodes only messages of that type.
struct TypedJsonCodec;

impl Codec for TypedJsonCodec {
    fn content_type(&self) -> &str {
        "application/json"
    }

    fn encode<T: serde::Serialize + 'static>(
        &self,
        value: &T,
        props: &mut BasicProperties,
    ) -> Result<Vec<u8>, Error> {
        props.type_field = Some("greeting".try_into().unwrap());
        JsonCodec.encode(value, props)
    }

    fn decode<T: serde::de::DeserializeOwned + 'static>(
        &self,
        bytes: &[u8],
        meta: &MessageMeta<'_>,
    ) -> Result<T, Error> {
        match meta.properties.type_field.as_ref().map(|t| t.as_str()) {
            Some(Ok("greeting")) => JsonCodec.decode(bytes, meta),
            other => Err(Error::Codec(format!("unknown type {other:?}"))),
        }
    }
}

/// What the codec sets on publish reaches the consumer's codec (ADR-0004).
#[tokio::test]
async fn the_codec_sets_and_reads_message_properties() {
    let client = Client::builder(AMQP_URI)
        .unwrap()
        .codec(TypedJsonCodec)
        .build()
        .await
        .unwrap();
    let q = client.queue("", exclusive()).await.unwrap();
    client.publish("", &q.name(), &greeting(7)).await.unwrap();
    client
        .channel()
        .basic_publish("", &q.name(), b"{}", BasicProperties::default())
        .await
        .unwrap();

    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    let d = next_raw(&mut consumer).await.unwrap();
    assert_eq!(d.body, greeting(7));
    d.ack().unwrap();
    let f = next_raw(&mut consumer).await.unwrap_err();
    assert!(matches!(f.error, Error::Codec(_)), "{:?}", f.error);
    f.reject(false).unwrap();
}

/// By default a compressed delivery is a decode failure carrying the
/// compressed body (ADR-0005).
#[tokio::test]
async fn identity_returns_compressed_deliveries_as_failures() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    let props = BasicProperties::builder()
        .content_encoding("gzip")
        .build()
        .unwrap();
    client
        .channel()
        .basic_publish("", &q.name(), b"\x1f\x8b...", props)
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();
    let f = next_raw(&mut consumer).await.unwrap_err();
    assert!(
        matches!(f.error, Error::ContentEncoding(_)),
        "{:?}",
        f.error
    );
    assert_eq!(f.body, b"\x1f\x8b...");
    f.reject(false).unwrap();
}

#[cfg(feature = "compression")]
async fn gzip_client() -> Client {
    Client::builder(AMQP_URI)
        .unwrap()
        .encoding(amqp_client_rs::Compression::gzip())
        .build()
        .await
        .unwrap()
}

/// With `Compression`, bodies go out gzipped and come back decoded.
#[cfg(feature = "compression")]
#[tokio::test]
async fn compression_publishes_gzip_and_decodes_it() {
    let client = gzip_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    client
        .publish("", &q.name(), &b"hello".to_vec())
        .await
        .unwrap();

    let raw = client
        .channel()
        .basic_get(&q.name(), true)
        .await
        .unwrap()
        .expect("no message");
    let encoding = raw.properties.content_encoding.as_ref().unwrap();
    assert_eq!(encoding.as_str().unwrap(), "gzip");
    assert_eq!(&raw.body[..2], b"\x1f\x8b", "body is not gzip");

    client
        .publish("", &q.name(), &b"hello".to_vec())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Vec<u8>>(SubscribeOpts::default())
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(d.body, b"hello");
    d.ack().unwrap();
}

/// A publish whose properties name another encoding fails before sending.
#[cfg(feature = "compression")]
#[tokio::test]
async fn compression_refuses_to_publish_under_another_encoding() {
    let client = gzip_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    let props = BasicProperties::builder()
        .content_encoding("deflate")
        .build()
        .unwrap();
    let result = client
        .publish_with_props("", &q.name(), &b"hello".to_vec(), props)
        .await;
    assert!(
        matches!(result, Err(Error::ContentEncoding(_))),
        "{result:?}"
    );
    assert!(!has_message(&client, &q.name()).await, "it was sent");
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

/// Publishes to `exchange` until the consumer receives a message; recovery
/// re-binds asynchronously, so early publishes may be dropped unrouted.
async fn publish_until_received(
    client: &Client<JsonCodec>,
    exchange: &str,
    routing_key: &str,
    consumer: &mut TypedConsumer<Greeting, JsonCodec>,
) -> Greeting {
    for _ in 0..50 {
        client
            .publish(exchange, routing_key, &greeting(7))
            .await
            .unwrap();
        if let Some(d) = next_within(consumer, Duration::from_millis(100)).await {
            d.ack().unwrap();
            return d.body;
        }
    }
    panic!("consumer did not recover");
}

async fn delete_elsewhere(queue: &str) {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    ch.queue_delete(queue, QueueDeleteOpts::default())
        .await
        .unwrap();
}

/// With recovery, a consumer whose queue was deleted by someone else declares
/// the queue and its bindings again and consumes again.
#[tokio::test]
async fn consumer_recovers_after_queue_deleted_elsewhere() {
    let client = recovering_json_client().await;
    let ex = client
        .topic(&queue_name("cancel-ex"), ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let name = queue_name("cancel");
    let q = client
        .queue(&name, QueueDeclareOpts::default())
        .await
        .unwrap();
    q.bind(&ex, "k").await.unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();

    delete_elsewhere(&name).await;

    let body = publish_until_received(&client, ex.name(), "k", &mut consumer).await;
    assert_eq!(body, greeting(7));
    assert_eq!(q.name(), name);
    q.delete(QueueDeleteOpts::default()).await.unwrap();
    client
        .channel()
        .exchange_delete(ex.name(), false)
        .await
        .unwrap();
}

/// A deleted server-named queue comes back under a new name, with its
/// bindings following it.
#[tokio::test]
async fn deleted_server_named_queue_comes_back_renamed() {
    let client = recovering_json_client().await;
    let ex = client
        .topic(&queue_name("rename-ex"), ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let auto_delete = QueueDeclareOpts {
        auto_delete: true,
        ..Default::default()
    };
    let q = client.queue("", auto_delete).await.unwrap();
    q.bind(&ex, "k").await.unwrap();
    let old_name = q.name();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();

    delete_elsewhere(&old_name).await;

    publish_until_received(&client, ex.name(), "k", &mut consumer).await;
    assert_ne!(q.name(), old_name);
    client
        .channel()
        .exchange_delete(ex.name(), false)
        .await
        .unwrap();
}

/// Deleting the queue through its handle ends its consumers, also with
/// recovery on.
#[tokio::test]
async fn queue_delete_ends_its_consumers_with_recovery() {
    let client = recovering_json_client().await;
    let q = client
        .queue(&queue_name("delete-recovery"), QueueDeclareOpts::default())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();

    q.delete(QueueDeleteOpts::default()).await.unwrap();

    let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
    assert!(matches!(ended, Ok(None)), "consumer did not end");
}

/// Publishing to a missing exchange makes the broker close the client's
/// channel; the client opens a new one for the next operation.
#[tokio::test]
async fn client_reopens_its_channel_after_the_broker_closes_it() {
    let client = Client::builder(AMQP_URI).unwrap().build().await.unwrap();
    client
        .publish(&queue_name("missing-ex"), "k", &vec![1u8])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let q = tokio::time::timeout(Duration::from_secs(2), client.queue("", exclusive()))
        .await
        .expect("declare hung")
        .unwrap();
    client.publish("", &q.name(), &vec![1u8]).await.unwrap();
    assert!(has_message(&client, &q.name()).await);
}

/// If the queue's exchange is gone too, consumer recovery declares the
/// recorded exchange again before binding.
#[tokio::test]
async fn consumer_recovery_declares_a_deleted_exchange_again() {
    let client = recovering_json_client().await;
    let ex = client
        .topic(&queue_name("gone-ex"), ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let name = queue_name("gone-ex-q");
    let q = client
        .queue(&name, QueueDeclareOpts::default())
        .await
        .unwrap();
    q.bind(&ex, "k").await.unwrap();
    let mut consumer = q
        .subscribe::<Greeting>(SubscribeOpts::default())
        .await
        .unwrap();

    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    ch.exchange_delete(ex.name(), false).await.unwrap();
    ch.queue_delete(&name, QueueDeleteOpts::default())
        .await
        .unwrap();

    let mut received = false;
    for _ in 0..50 {
        let _ = client.publish(ex.name(), "k", &greeting(7)).await;
        if let Some(d) = next_within(&mut consumer, Duration::from_millis(100)).await {
            d.ack().unwrap();
            received = true;
            break;
        }
    }
    assert!(received, "consumer did not recover");
    q.delete(QueueDeleteOpts::default()).await.unwrap();
    client
        .channel()
        .exchange_delete(ex.name(), false)
        .await
        .unwrap();
}

/// A confirmed publish returns once the broker has acked it.
#[tokio::test]
async fn publish_confirmed_returns_once_acked() {
    let client = json_client().await;
    let q = client.queue("", exclusive()).await.unwrap();
    client
        .publish_confirmed("", &q.name(), &greeting(1))
        .await
        .unwrap();
    let got = client.channel().basic_get(&q.name(), true).await.unwrap();
    assert!(got.is_some(), "acked message is not in the queue");
}

/// A confirmed publish to a missing exchange fails for certain with the
/// broker's 404; the next confirmed publish works on a new channel.
#[tokio::test]
async fn publish_confirmed_to_a_missing_exchange_fails() {
    let client = json_client().await;
    let missing = client
        .publish_confirmed(&queue_name("no-such-ex"), "k", &greeting(1))
        .await;
    assert!(
        matches!(missing, Err(Error::ChannelClosed { code: 404, .. })),
        "{missing:?}"
    );

    let q = client.queue("", exclusive()).await.unwrap();
    client
        .publish_confirmed("", &q.name(), &greeting(2))
        .await
        .unwrap();
}

/// A publish the broker rejects (full queue with reject-publish) fails with
/// `PublishNacked`.
#[tokio::test]
async fn publish_confirmed_reports_a_nack() {
    let client = json_client().await;
    let mut arguments = amqp_client_rs::Table::new();
    arguments.insert(
        amqp_client_rs::ShortStr::try_from("x-max-length").unwrap(),
        amqp_client_rs::FieldValue::LongLongInt(1),
    );
    arguments.insert(
        amqp_client_rs::ShortStr::try_from("x-overflow").unwrap(),
        amqp_client_rs::FieldValue::LongStr(amqp_client_rs::LongStr::from("reject-publish")),
    );
    let opts = QueueDeclareOpts {
        exclusive: true,
        arguments,
        ..Default::default()
    };
    let q = client.queue("", opts).await.unwrap();

    client
        .publish_confirmed("", &q.name(), &greeting(1))
        .await
        .unwrap();
    let second = client.publish_confirmed("", &q.name(), &greeting(2)).await;
    assert!(matches!(second, Err(Error::PublishNacked)), "{second:?}");
}

/// Deleting a queue while its consumer is waiting in `next()` must not let
/// consumer recovery declare it again.
#[tokio::test]
async fn queue_delete_while_the_consumer_is_polled() {
    for i in 0..5 {
        let client = recovering_json_client().await;
        let name = queue_name(&format!("delete-polled-{i}"));
        let q = client
            .queue(&name, QueueDeclareOpts::default())
            .await
            .unwrap();
        let mut consumer = q
            .subscribe::<Greeting>(SubscribeOpts::default())
            .await
            .unwrap();
        let next = tokio::spawn(async move { consumer.next().await.is_none() });
        tokio::time::sleep(Duration::from_millis(50)).await;

        q.delete(QueueDeleteOpts::default()).await.unwrap();

        let ended = tokio::time::timeout(Duration::from_secs(2), next).await;
        let conn = Connection::connect(AMQP_URI).await.unwrap();
        let passive = QueueDeclareOpts {
            passive: true,
            ..Default::default()
        };
        let exists = conn
            .open_channel()
            .await
            .unwrap()
            .queue_declare(&name, passive)
            .await
            .is_ok();
        if exists {
            let ch = conn.open_channel().await.unwrap();
            let _ = ch.queue_delete(&name, QueueDeleteOpts::default()).await;
        }
        assert!(!exists, "run {i}: deleted queue was declared again");
        assert!(
            matches!(ended, Ok(Ok(true))),
            "run {i}: consumer did not end"
        );
    }
}

async fn queue_exists(name: &str) -> bool {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    conn.open_channel()
        .await
        .unwrap()
        .queue_declare(name, passive)
        .await
        .is_ok()
}

async fn delete_named(name: &str) {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    let _ = ch.queue_delete(name, QueueDeleteOpts::default()).await;
}

/// Two overlapping deletes of one queue never touch another queue (an empty
/// queue name means "last declared on this channel" to the broker).
#[tokio::test]
async fn overlapping_deletes_leave_other_queues_alone() {
    let client = recovering_json_client().await;
    let (a, b) = (queue_name("dd-a"), queue_name("dd-b"));
    let qa = client.queue(&a, QueueDeclareOpts::default()).await.unwrap();
    client.queue(&b, QueueDeclareOpts::default()).await.unwrap();

    let qa2 = qa.clone();
    let (first, second) = tokio::join!(
        qa.delete(QueueDeleteOpts::default()),
        qa2.delete(QueueDeleteOpts::default())
    );

    let b_exists = queue_exists(&b).await;
    delete_named(&b).await;
    assert!(b_exists, "deleting {a} twice deleted {b}");
    assert!(first.is_ok(), "{first:?}");
    assert!(
        matches!(second, Err(Error::QueueDeleted { .. })),
        "{second:?}"
    );
}

/// A bind overlapping a delete never binds another queue.
#[tokio::test]
async fn a_bind_during_a_delete_leaves_other_queues_alone() {
    let client = recovering_json_client().await;
    let (a, b) = (queue_name("bd-a"), queue_name("bd-b"));
    let ex = client
        .fanout(&queue_name("bd-ex"), ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let qa = client.queue(&a, QueueDeclareOpts::default()).await.unwrap();
    client.queue(&b, QueueDeclareOpts::default()).await.unwrap();

    let qa2 = qa.clone();
    let (deleted, bound) = tokio::join!(qa.delete(QueueDeleteOpts::default()), qa2.bind(&ex, ""));
    deleted.unwrap();
    assert!(
        matches!(bound, Err(Error::QueueDeleted { .. })),
        "{bound:?}"
    );

    client.publish(ex.name(), "", &greeting(1)).await.unwrap();
    let got = client.channel().basic_get(&b, true).await.unwrap();
    delete_named(&b).await;
    client
        .channel()
        .exchange_delete(ex.name(), false)
        .await
        .unwrap();
    assert!(got.is_none(), "{b} was bound instead of {a}");
}

/// Deleting a queue someone else already deleted succeeds; the handle then
/// keeps its name but refuses further operations.
#[tokio::test]
async fn deleting_an_already_deleted_queue_succeeds() {
    let client = recovering_json_client().await;
    let name = queue_name("gone-already");
    let q = client
        .queue(&name, QueueDeclareOpts::default())
        .await
        .unwrap();
    delete_named(&name).await;

    assert_eq!(q.delete(QueueDeleteOpts::default()).await.unwrap(), 0);
    assert_eq!(q.name(), name);
    let bound = q.bind("amq.fanout", "").await;
    assert!(
        matches!(bound, Err(Error::QueueDeleted { .. })),
        "{bound:?}"
    );
    let subscribed = q.subscribe::<Greeting>(SubscribeOpts::default()).await;
    assert!(matches!(subscribed, Err(Error::QueueDeleted { .. })));
    assert!(!queue_exists(&name).await);
}
