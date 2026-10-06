//! Session recovery after connection loss (ADR-0002), through a cuttable
//! proxy in front of the broker.

mod support;

use std::time::{Duration, Instant};

use amqp_client_rs::{
    Client, Connection, Error, ExchangeDeclareOpts, QueueDeclareOpts, QueueDeleteOpts,
    RecoveryConfig, SubscribeOpts,
};
use support::proxy::Proxy;

const AMQP_URI: &str = "amqp://guest:guest@127.0.0.1";

fn name(label: &str) -> String {
    format!("amqp-client-rs-recovery-{}-{}", label, std::process::id())
}

async fn recovering_client(proxy: &Proxy) -> Client {
    Client::builder(&proxy.uri())
        .unwrap()
        .recovery(RecoveryConfig::default())
        .build()
        .await
        .unwrap()
}

/// Message count of `queue`, seen over a direct (unproxied) connection.
async fn depth(queue: &str) -> u32 {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    ch.queue_declare(queue, passive)
        .await
        .unwrap()
        .message_count
}

async fn delete_queue(queue: &str) {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    ch.queue_delete(queue, QueueDeleteOpts::default())
        .await
        .unwrap();
}

/// Publishes until a call fails; returns how long the failing call took.
async fn time_failing_publish(client: &Client, queue: &str) -> Duration {
    for _ in 0..20 {
        let start = Instant::now();
        match client.publish("", queue, &vec![1u8]).await {
            Err(Error::ConnectionClosed { .. }) => return start.elapsed(),
            Err(e) => panic!("unexpected error: {e}"),
            Ok(()) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("publish never failed");
}

/// After the connection is lost, publishing reconnects and the message
/// arrives.
#[tokio::test]
async fn publish_recovers_after_connection_loss() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    let queue = name("publish");
    client
        .queue(&queue, QueueDeclareOpts::default())
        .await
        .unwrap();

    proxy.cut();

    let mut arrived = false;
    for _ in 0..50 {
        let _ = client.publish("", &queue, &vec![1u8]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        if depth(&queue).await > 0 {
            arrived = true;
            break;
        }
    }
    assert!(arrived, "no publish arrived after connection loss");
    assert!(proxy.accepted() >= 2, "client did not reconnect");
    delete_queue(&queue).await;
}

/// Without recovery, publishing after connection loss fails.
#[tokio::test]
async fn publish_fails_after_connection_loss_without_recovery() {
    let proxy = Proxy::start().await;
    let client = Client::builder(&proxy.uri())
        .unwrap()
        .build()
        .await
        .unwrap();
    proxy.cut();
    time_failing_publish(&client, "amq.unrouted").await;
    assert_eq!(proxy.accepted(), 1);
}

/// While the broker is unreachable, a publish waits at most the recovery
/// timeout, then fails.
#[tokio::test]
async fn publish_fails_after_recovery_timeout_while_broker_down() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    proxy.set_down(true);
    proxy.cut();

    let took = time_failing_publish(&client, "amq.unrouted").await;
    assert!(
        took >= Duration::from_millis(200),
        "failed too fast: {took:?}"
    );
    assert!(took < Duration::from_secs(2), "waited too long: {took:?}");
}

/// After reconnecting, the recorded topology is declared again: an exclusive
/// server-named queue comes back under a new name, still bound.
#[tokio::test]
async fn topology_is_declared_again_after_connection_loss() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    let ex = client
        .topic(&name("topology-ex"), ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let exclusive = QueueDeclareOpts {
        exclusive: true,
        ..Default::default()
    };
    let q = client.queue("", exclusive).await.unwrap();
    q.bind(&ex, "k").await.unwrap();
    let old_name = q.name();

    proxy.cut();

    let mut routed = false;
    for _ in 0..50 {
        let _ = client.publish(ex.name(), "k", &vec![1u8]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        if q.name() != old_name
            && let Ok(Some(_)) = client.channel().basic_get(&q.name(), true).await
        {
            routed = true;
            break;
        }
    }
    assert!(routed, "topology was not declared again");
    client
        .channel()
        .exchange_delete(ex.name(), false)
        .await
        .unwrap();
}

/// A consumer keeps receiving after the connection is lost.
#[tokio::test]
async fn consumer_survives_connection_loss() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    let queue = name("consumer");
    let q = client
        .queue(&queue, QueueDeclareOpts::default())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Vec<u8>>(SubscribeOpts::default())
        .await
        .unwrap();

    proxy.cut();

    let mut received = false;
    for _ in 0..50 {
        let _ = client.publish("", &queue, &vec![7u8]).await;
        if let Ok(Some(d)) = tokio::time::timeout(Duration::from_millis(100), consumer.next()).await
        {
            assert_eq!(d.body, vec![7u8]);
            d.ack().unwrap();
            received = true;
            break;
        }
    }
    assert!(received, "consumer did not recover");
    drop(consumer);
    delete_queue(&queue).await;
}

/// With no consumers and nobody waiting, recovery stops reconnecting.
#[tokio::test]
async fn recovery_stands_down_without_demand() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    proxy.set_down(true);
    proxy.cut();
    time_failing_publish(&client, "amq.unrouted").await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    let attempts = proxy.accepted();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(proxy.accepted(), attempts, "recovery kept reconnecting");
}

/// A consumer waiting for recovery ends when the client is dropped.
#[tokio::test]
async fn dropping_the_client_ends_a_recovering_consumer() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    let queue = name("drop");
    let q = client
        .queue(&queue, QueueDeclareOpts::default())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Vec<u8>>(SubscribeOpts::default())
        .await
        .unwrap();
    proxy.set_down(true);
    proxy.cut();

    let next = tokio::spawn(async move { consumer.next().await.is_none() });
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(client);

    let ended = tokio::time::timeout(Duration::from_secs(5), next).await;
    assert!(matches!(ended, Ok(Ok(true))), "consumer did not end");
    delete_queue(&queue).await;
}

/// Dropping the client ends an idle consumer, with and without recovery.
#[tokio::test]
async fn dropping_the_client_ends_an_idle_consumer() {
    for recovery in [false, true] {
        let mut builder = Client::builder(AMQP_URI).unwrap();
        if recovery {
            builder = builder.recovery(RecoveryConfig::default());
        }
        let client = builder.build().await.unwrap();
        let exclusive = QueueDeclareOpts {
            exclusive: true,
            ..Default::default()
        };
        let q = client.queue("", exclusive).await.unwrap();
        let mut consumer = q
            .subscribe::<Vec<u8>>(SubscribeOpts::default())
            .await
            .unwrap();

        drop(client);

        let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
        assert!(
            matches!(ended, Ok(None)),
            "consumer did not end (recovery: {recovery})"
        );
    }
}

/// Dropping the client ends a waiting consumer promptly even while a
/// reconnect attempt is stalled (broker accepts TCP but never answers).
#[tokio::test]
async fn dropping_the_client_during_a_stalled_reconnect_ends_consumers() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    let queue = name("stalled");
    let q = client
        .queue(&queue, QueueDeclareOpts::default())
        .await
        .unwrap();
    let mut consumer = q
        .subscribe::<Vec<u8>>(SubscribeOpts::default())
        .await
        .unwrap();
    proxy.freeze();
    proxy.cut();

    let next = tokio::spawn(async move { consumer.next().await.is_none() });
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(client);

    let ended = tokio::time::timeout(Duration::from_secs(2), next).await;
    assert!(matches!(ended, Ok(Ok(true))), "consumer did not end");
    delete_queue(&queue).await;
}

/// If the connection is lost after a confirmed publish was sent but before
/// its ack, the outcome is uncertain.
#[tokio::test]
async fn publish_confirmed_is_uncertain_when_the_ack_is_lost() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    // Open the confirm channel while the proxy still forwards.
    client
        .publish_confirmed("", "amq.unrouted", &vec![0u8])
        .await
        .unwrap();
    proxy.freeze();

    let publish = tokio::spawn(async move {
        client
            .publish_confirmed("", "amq.unrouted", &vec![1u8])
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    proxy.cut();

    let result = tokio::time::timeout(Duration::from_secs(5), publish)
        .await
        .expect("publish_confirmed hung")
        .unwrap();
    assert!(
        matches!(result, Err(Error::ConfirmUncertain { .. })),
        "{result:?}"
    );
}

/// A confirmed publish that could not be sent because the connection was
/// already gone is sent once more on the recovered session.
#[tokio::test]
async fn publish_confirmed_is_retried_when_it_was_never_sent() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    let queue = name("confirm-retry");
    client
        .queue(&queue, QueueDeclareOpts::default())
        .await
        .unwrap();

    proxy.cut();
    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .publish_confirmed("", &queue, &vec![1u8])
        .await
        .unwrap();
    assert_eq!(depth(&queue).await, 1);
    delete_queue(&queue).await;
}

/// A declare in flight when the connection is lost waits for recovery and
/// succeeds on the new session.
#[tokio::test]
async fn an_operation_in_flight_at_connection_loss_is_retried() {
    let proxy = Proxy::start().await;
    let client = recovering_client(&proxy).await;
    proxy.freeze();
    let exclusive = QueueDeclareOpts {
        exclusive: true,
        ..Default::default()
    };
    let declare = tokio::spawn(async move { client.queue("", exclusive).await.map(|_| ()) });
    tokio::time::sleep(Duration::from_millis(200)).await;
    proxy.cut();
    proxy.unfreeze();

    let result = tokio::time::timeout(Duration::from_secs(5), declare)
        .await
        .expect("declare hung")
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
}
