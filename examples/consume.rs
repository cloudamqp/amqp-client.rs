use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use amqp_client::{Connection, ConsumeOpts, QueueDeclareOpts};

const QUEUE: &str = "hello";
const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);

#[tokio::main]
async fn main() {
    let conn = Connection::connect("amqp://guest:guest@127.0.0.1")
        .await
        .expect("failed to connect");
    let ch = conn.open_channel().await.expect("failed to open channel");

    ch.queue_declare(QUEUE, QueueDeclareOpts::default())
        .await
        .expect("failed to declare queue");

    let opts = ConsumeOpts {
        no_ack: true,
        ..Default::default()
    };
    let mut consumer = ch
        .basic_consume(QUEUE, "my-consumer", opts)
        .await
        .expect("failed to start consumer");

    let count = Arc::new(AtomicU64::new(0));

    // Reporter task
    let rep_count = count.clone();
    tokio::spawn(async move {
        let mut last_count: u64 = 0;
        let mut last_time = Instant::now();
        loop {
            tokio::time::sleep(REPORT_INTERVAL).await;
            let now = Instant::now();
            let total = rep_count.load(Ordering::Relaxed);
            let dt = (now - last_time).as_secs_f64();
            let rate = (total - last_count) as f64 / dt;
            println!("consumed: {total} total, {rate:.0} msg/s");
            last_count = total;
            last_time = now;
        }
    });

    println!("Consuming from queue '{QUEUE}'. Press Ctrl+C to stop.");
    while let Some(_msg) = consumer.next().await {
        count.fetch_add(1, Ordering::Relaxed);
    }

    ch.close().await.expect("failed to close channel");
    conn.close().await.expect("failed to close connection");
}
