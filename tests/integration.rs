mod support;

use std::time::Duration;

use amqp_client_rs::{
    BasicProperties, Channel, Connection, ConsumeOpts, ExchangeDeclareOpts, QueueDeclareOpts,
    QueueDeleteOpts, Table,
};

const AMQP_URI: &str = "amqp://guest:guest@127.0.0.1";

/// Helper: connect and open a channel, declare a unique queue, return all three.
async fn setup(test_name: &str) -> (Connection, Channel, String) {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    let queue_name = format!("amqp-client-rs-test-{}-{}", test_name, std::process::id());
    let ok = ch
        .queue_declare(&queue_name, QueueDeclareOpts::default())
        .await
        .unwrap();
    assert_eq!(ok.queue, queue_name);
    (conn, ch, queue_name)
}

/// Helper: delete queue, close channel and connection.
async fn teardown(ch: Channel, conn: Connection, queue_name: &str) {
    let _ = ch
        .queue_delete(queue_name, QueueDeleteOpts::default())
        .await;
    let _ = ch.close().await;
    let _ = conn.close().await;
}

// -----------------------------------------------------------------------
// Connection tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn connect_and_close() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn connect_bad_host() {
    let result = Connection::connect("amqp://guest:guest@127.0.0.1:19999").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn connect_with_config() {
    let config = amqp_client_rs::Config::from_uri(AMQP_URI).unwrap();
    let conn = Connection::connect_with_config(config).await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn open_multiple_channels() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch1 = conn.open_channel().await.unwrap();
    let ch2 = conn.open_channel().await.unwrap();
    let ch3 = conn.open_channel().await.unwrap();
    assert_eq!(ch1.id(), 1);
    assert_eq!(ch2.id(), 2);
    assert_eq!(ch3.id(), 3);
    ch1.close().await.unwrap();
    ch2.close().await.unwrap();
    ch3.close().await.unwrap();
    conn.close().await.unwrap();
}

// -----------------------------------------------------------------------
// Publish + Get tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn publish_and_basic_get() {
    let (conn, ch, queue) = setup("pub-get").await;

    ch.basic_publish("", &queue, b"hello world", BasicProperties::default())
        .await
        .unwrap();

    let msg = ch.basic_get(&queue, false).await.unwrap().unwrap();
    assert_eq!(msg.body.as_ref(), b"hello world");
    assert_eq!(msg.exchange.as_bytes(), b"");
    assert_eq!(msg.routing_key.as_bytes(), queue.as_bytes());
    msg.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

#[tokio::test]
async fn publish_large_body() {
    let (conn, ch, queue) = setup("pub-large").await;

    let body = vec![0xAB_u8; 256 * 1024]; // 256KB
    ch.basic_publish("", &queue, body.as_slice(), BasicProperties::default())
        .await
        .unwrap();

    let msg = ch.basic_get(&queue, false).await.unwrap().unwrap();
    assert_eq!(msg.body.len(), 256 * 1024);
    assert!(msg.body.iter().all(|&b| b == 0xAB));
    msg.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_publishes_on_one_channel() {
    let (conn, ch, queue) = setup("pub-concurrent").await;
    let ch = std::sync::Arc::new(ch);

    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let (ch, queue) = (ch.clone(), queue.clone());
            tokio::spawn(async move {
                for _ in 0..500 {
                    ch.basic_publish("", &queue, b"hello", BasicProperties::default())
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }

    // A passive declare is an RPC behind every publish on the channel: it
    // fails if the broker closed the channel over an interleaved frame.
    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    let ok = ch.queue_declare(&queue, passive).await.unwrap();
    assert_eq!(ok.message_count, 8 * 500);

    let ch = std::sync::Arc::into_inner(ch).unwrap();
    teardown(ch, conn, &queue).await;
}

#[tokio::test]
async fn publish_empty_body() {
    let (conn, ch, queue) = setup("pub-empty").await;

    ch.basic_publish("", &queue, b"", BasicProperties::default())
        .await
        .unwrap();

    let msg = ch.basic_get(&queue, false).await.unwrap().unwrap();
    assert!(msg.body.is_empty());
    msg.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

#[tokio::test]
async fn publish_with_headers() {
    let (conn, ch, queue) = setup("pub-headers").await;

    let mut headers = Table::new();
    headers.insert(
        amqp_client_rs::ShortStr::try_from("x-custom").unwrap(),
        amqp_client_rs::FieldValue::LongStr(amqp_client_rs::LongStr::from("value1")),
    );
    let props = BasicProperties::builder()
        .content_type("application/json")
        .headers(headers)
        .build()
        .unwrap();

    ch.basic_publish("", &queue, b"{}", props).await.unwrap();

    let msg = ch.basic_get(&queue, false).await.unwrap().unwrap();
    assert_eq!(msg.body.as_ref(), b"{}");
    assert!(msg.properties.content_type.is_some());
    assert!(msg.properties.headers.is_some());
    msg.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

// -----------------------------------------------------------------------
// Consumer tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn basic_consume_and_ack() {
    let (conn, ch, queue) = setup("consume-ack").await;

    ch.basic_publish("", &queue, b"consume me", BasicProperties::default())
        .await
        .unwrap();

    let mut consumer = ch
        .basic_consume(&queue, "test-consumer", ConsumeOpts::default())
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(msg.body.as_ref(), b"consume me");
    msg.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

#[tokio::test]
async fn basic_consume_nack_requeue() {
    let (conn, ch, queue) = setup("consume-nack").await;

    ch.basic_publish("", &queue, b"nack me", BasicProperties::default())
        .await
        .unwrap();

    let mut consumer = ch
        .basic_consume(&queue, "test-nack", ConsumeOpts::default())
        .await
        .unwrap();

    // Get the message and nack with requeue
    let msg = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.body.as_ref(), b"nack me");
    msg.nack(true).unwrap();

    // Message should be redelivered
    let msg2 = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg2.body.as_ref(), b"nack me");
    assert!(msg2.redelivered);
    msg2.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

// -----------------------------------------------------------------------
// Queue tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn queue_declare_and_delete() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    let qname = format!("amqp-client-rs-test-decl-del-{}", std::process::id());
    let ok = ch
        .queue_declare(&qname, QueueDeclareOpts::default())
        .await
        .unwrap();
    assert_eq!(ok.queue, qname);
    assert_eq!(ok.message_count, 0);

    let purged = ch
        .queue_delete(&qname, QueueDeleteOpts::default())
        .await
        .unwrap();
    assert_eq!(purged, 0);

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn queue_declare_server_named() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    // Empty name = server-generated name
    let ok = ch
        .queue_declare("", QueueDeclareOpts::default())
        .await
        .unwrap();
    assert!(!ok.queue.is_empty());

    let _ = ch.queue_delete(&ok.queue, QueueDeleteOpts::default()).await;
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn queue_bind_and_unbind() {
    let (conn, ch, queue) = setup("bind-unbind").await;

    ch.queue_bind(&queue, "amq.direct", "test-key", Table::new())
        .await
        .unwrap();
    ch.queue_unbind(&queue, "amq.direct", "test-key", Table::new())
        .await
        .unwrap();

    teardown(ch, conn, &queue).await;
}

#[tokio::test]
async fn queue_purge() {
    let (conn, ch, queue) = setup("purge").await;

    // Publish a few messages
    for _ in 0..5 {
        ch.basic_publish("", &queue, b"purge me", BasicProperties::default())
            .await
            .unwrap();
    }

    let purged = ch.queue_purge(&queue).await.unwrap();
    assert_eq!(purged, 5);

    teardown(ch, conn, &queue).await;
}

// -----------------------------------------------------------------------
// Exchange tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn exchange_declare_and_delete() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    let ename = format!("amqp-client-rs-test-exch-{}", std::process::id());
    ch.exchange_declare(&ename, "direct", ExchangeDeclareOpts::default())
        .await
        .unwrap();
    ch.exchange_delete(&ename, false).await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

// -----------------------------------------------------------------------
// QoS tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn basic_qos() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    ch.basic_qos(10, false).await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

// -----------------------------------------------------------------------
// Transaction tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn tx_commit() {
    let (conn, ch, queue) = setup("tx-commit").await;

    ch.tx_select().await.unwrap();
    ch.basic_publish("", &queue, b"tx msg", BasicProperties::default())
        .await
        .unwrap();
    ch.tx_commit().await.unwrap();

    let msg = ch.basic_get(&queue, false).await.unwrap().unwrap();
    assert_eq!(msg.body.as_ref(), b"tx msg");
    msg.ack().unwrap();

    teardown(ch, conn, &queue).await;
}

#[tokio::test]
async fn tx_rollback() {
    let (conn, ch, queue) = setup("tx-rollback").await;

    ch.tx_select().await.unwrap();
    ch.basic_publish("", &queue, b"rolled back", BasicProperties::default())
        .await
        .unwrap();
    ch.tx_rollback().await.unwrap();

    let msg = ch.basic_get(&queue, false).await.unwrap();
    assert!(msg.is_none());

    teardown(ch, conn, &queue).await;
}

// -----------------------------------------------------------------------
// Confirm tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn confirm_select() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

// -----------------------------------------------------------------------
// Fluent wrapper tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn queue_fluent_wrapper() {
    let (conn, ch, queue_name) = setup("fluent-q").await;

    let q = ch.queue(&queue_name);
    q.publish(b"fluent msg", BasicProperties::default())
        .await
        .unwrap();

    let msg = ch.basic_get(&queue_name, false).await.unwrap().unwrap();
    assert_eq!(msg.body.as_ref(), b"fluent msg");
    msg.ack().unwrap();

    teardown(ch, conn, &queue_name).await;
}

#[tokio::test]
async fn exchange_fluent_wrapper() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    let ename = format!("amqp-client-rs-test-fluent-exch-{}", std::process::id());
    ch.exchange_declare(&ename, "direct", ExchangeDeclareOpts::default())
        .await
        .unwrap();

    let ex = ch.exchange(&ename);
    assert_eq!(ex.name(), ename);

    // Clean up
    ch.exchange_delete(&ename, false).await.unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

// -----------------------------------------------------------------------
// Recovery tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn basic_recover() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();

    ch.basic_recover(true).await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// Dropping an open channel frees its id; a channel opened afterwards (which
/// may reuse that id) must work.
#[tokio::test]
async fn reopen_after_dropping_an_open_channel() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    drop(ch);
    let ch = tokio::time::timeout(Duration::from_secs(5), conn.open_channel())
        .await
        .expect("open_channel hung")
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        ch.queue_declare("", QueueDeclareOpts::default()),
    )
    .await
    .expect("queue_declare hung")
    .unwrap();
}

/// A channel the broker closed frees its id; a channel opened afterwards on
/// that id must work.
#[tokio::test]
async fn reopen_after_broker_closed_a_channel() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let name = format!("amqp-client-rs-test-reopen-{}", std::process::id());
    let ch = conn.open_channel().await.unwrap();
    ch.exchange_declare(&name, "direct", ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let ch = conn.open_channel().await.unwrap();
    assert!(
        ch.exchange_declare(&name, "fanout", ExchangeDeclareOpts::default())
            .await
            .is_err()
    );
    drop(ch);
    let ch = tokio::time::timeout(Duration::from_secs(5), conn.open_channel())
        .await
        .expect("open_channel hung")
        .unwrap();
    ch.exchange_delete(&name, false).await.unwrap();
}

/// When the connection drops, a consumer's stream ends with a
/// `ConnectionClosed` cancellation instead of hanging.
#[tokio::test]
async fn consumer_ends_when_the_connection_drops() {
    let proxy = support::proxy::Proxy::start().await;
    let conn = Connection::connect(&proxy.uri()).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    let ok = ch
        .queue_declare(
            "",
            QueueDeclareOpts {
                exclusive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut consumer = ch
        .basic_consume(&ok.queue, "", ConsumeOpts::default())
        .await
        .unwrap();

    proxy.cut();

    let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
    assert!(matches!(ended, Ok(None)), "consumer did not end");
    assert!(matches!(
        consumer.cancellation(),
        Some(amqp_client_rs::CancellationReason::ConnectionClosed { .. })
    ));
}

/// Dropping or closing the `Connection` ends its consumers too.
#[tokio::test]
async fn consumer_ends_when_the_connection_is_dropped_or_closed() {
    for close in [false, true] {
        let conn = Connection::connect(AMQP_URI).await.unwrap();
        let ch = conn.open_channel().await.unwrap();
        let ok = ch
            .queue_declare(
                "",
                QueueDeclareOpts {
                    exclusive: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut consumer = ch
            .basic_consume(&ok.queue, "", ConsumeOpts::default())
            .await
            .unwrap();

        if close {
            conn.close().await.unwrap();
        } else {
            drop(conn);
        }

        let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
        assert!(
            matches!(ended, Ok(None)),
            "consumer did not end (close: {close})"
        );
    }
}

/// A peer that goes silent without closing the socket is detected by the
/// heartbeat: the connection ends, and so do its consumers.
#[tokio::test]
async fn heartbeat_timeout_ends_the_connection() {
    let proxy = support::proxy::Proxy::start().await;
    let mut config = amqp_client_rs::Config::from_uri(&proxy.uri()).unwrap();
    config.heartbeat = 1;
    let conn = Connection::connect_with_config(config).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    let ok = ch
        .queue_declare(
            "",
            QueueDeclareOpts {
                exclusive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut consumer = ch
        .basic_consume(&ok.queue, "", ConsumeOpts::default())
        .await
        .unwrap();

    proxy.freeze();

    let ended = tokio::time::timeout(Duration::from_secs(6), consumer.next()).await;
    assert!(matches!(ended, Ok(None)), "consumer did not end");
    assert!(matches!(
        ch.queue_declare("", QueueDeclareOpts::default()).await,
        Err(amqp_client_rs::Error::ConnectionClosed { .. })
    ));
}

/// After the broker closes a channel, publishing and RPCs on it fail at once
/// with `ChannelClosed` instead of being dropped or waiting for rpc_timeout.
#[tokio::test]
async fn operations_on_a_broker_closed_channel_fail_fast() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let name = format!("amqp-client-rs-test-closed-{}", std::process::id());
    let ch = conn.open_channel().await.unwrap();
    ch.exchange_declare(&name, "direct", ExchangeDeclareOpts::default())
        .await
        .unwrap();
    let refused = ch
        .exchange_declare(&name, "fanout", ExchangeDeclareOpts::default())
        .await;
    assert!(matches!(
        refused,
        Err(amqp_client_rs::Error::ChannelClosed { code: 406, .. })
    ));

    let published = ch
        .basic_publish("", "nowhere", b"x".to_vec(), BasicProperties::default())
        .await;
    assert!(matches!(
        published,
        Err(amqp_client_rs::Error::ChannelClosed { .. })
    ));
    let declared = tokio::time::timeout(
        Duration::from_secs(1),
        ch.queue_declare("", QueueDeclareOpts::default()),
    )
    .await
    .expect("RPC on a closed channel waited");
    assert!(matches!(
        declared,
        Err(amqp_client_rs::Error::ChannelClosed { code: 406, .. })
    ));

    conn.open_channel()
        .await
        .unwrap()
        .exchange_delete(&name, false)
        .await
        .unwrap();
}

/// Once the connection is known to be gone, publishing fails at once instead
/// of queueing frames for a dead socket.
#[tokio::test]
async fn publish_fails_once_the_connection_is_gone() {
    let proxy = support::proxy::Proxy::start().await;
    let conn = Connection::connect(&proxy.uri()).await.unwrap();
    let ch = conn.open_channel().await.unwrap();
    let ok = ch
        .queue_declare(
            "",
            QueueDeclareOpts {
                exclusive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut consumer = ch
        .basic_consume(&ok.queue, "", ConsumeOpts::default())
        .await
        .unwrap();

    proxy.cut();
    // The consumer ending shows the reader has seen the connection close.
    let ended = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await;
    assert!(matches!(ended, Ok(None)), "consumer did not end");

    let published = ch
        .basic_publish("", &ok.queue, b"x".to_vec(), BasicProperties::default())
        .await;
    assert!(
        matches!(
            published,
            Err(amqp_client_rs::Error::ConnectionClosed { .. })
        ),
        "publish after connection loss: {published:?}"
    );
}

/// Acking a delivery after its channel was dropped fails, instead of landing
/// on a new channel that reused the id (which the broker would close with
/// "unknown delivery tag").
#[tokio::test]
async fn ack_after_the_channel_is_dropped_fails() {
    let conn = Connection::connect(AMQP_URI).await.unwrap();
    let ch0 = conn.open_channel().await.unwrap();
    let queue = ch0
        .queue_declare(
            "",
            QueueDeclareOpts {
                exclusive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .queue;
    ch0.basic_publish("", &queue, b"x".to_vec(), BasicProperties::default())
        .await
        .unwrap();
    let ch1 = conn.open_channel().await.unwrap();
    let mut consumer = ch1
        .basic_consume(&queue, "", ConsumeOpts::default())
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap();
    drop(consumer);
    let dropped_id = ch1.id();
    drop(ch1);
    tokio::time::sleep(Duration::from_millis(200)).await; // CloseOk frees the id
    let ch2 = conn.open_channel().await.unwrap();
    assert_eq!(ch2.id(), dropped_id, "test needs the id to be reused");

    assert!(matches!(
        msg.ack(),
        Err(amqp_client_rs::Error::ChannelClosed { .. })
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let passive = QueueDeclareOpts {
        passive: true,
        ..Default::default()
    };
    ch2.queue_declare(&queue, passive).await.unwrap();
}

/// An `open_channel` cancelled while waiting for OpenOk closes the channel on
/// the broker and frees its id, instead of leaking it.
#[tokio::test]
async fn cancelled_open_channel_frees_its_id() {
    let proxy = support::proxy::Proxy::start().await;
    let conn = Connection::connect(&proxy.uri()).await.unwrap();
    proxy.freeze();
    let cancelled = tokio::time::timeout(Duration::from_millis(200), conn.open_channel()).await;
    assert!(cancelled.is_err(), "open should still be waiting");
    proxy.unfreeze();
    tokio::time::sleep(Duration::from_millis(300)).await; // OpenOk, then CloseOk

    let ch = conn.open_channel().await.unwrap();
    assert_eq!(ch.id(), 1, "the cancelled open's id was not freed");
}
