use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use amqp_client::{BasicProperties, Connection, QueueDeclareOpts};

const QUEUE: &str = "hello";
const BODY: &[u8] = b"Hello from amqp-client.rs!";
const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);

#[tokio::main]
async fn main() {
    let url = "amqp://guest:guest@127.0.0.1";
    let Ok(conn) = Connection::connect(url).await else {
        eprintln!("Failed to connect to broker at {url}");
        return;
    };
    let ch = conn.open_channel().await.expect("failed to open channel");

    ch.queue_declare(QUEUE, QueueDeclareOpts::default())
        .await
        .expect("failed to declare queue");

    let count = Arc::new(AtomicU64::new(0));
    let props = BasicProperties::new();

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
            println!("published: {total} total, {rate:.0} msg/s");
            last_count = total;
            last_time = now;
        }
    });

    println!("Publishing to queue '{QUEUE}'. Press Ctrl+C to stop.");
    loop {
        ch.basic_publish("", QUEUE, BODY, props.clone())
            .await
            .expect("failed to publish");
        count.fetch_add(1, Ordering::Relaxed);
    }
}
