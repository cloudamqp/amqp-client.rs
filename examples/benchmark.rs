use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use amqp_client::{BasicProperties, Connection, ConsumeOpts, QueueDeclareOpts, QueueDeleteOpts};

fn humanize(n: f64) -> String {
    if n >= 1_000_000.0 {
        format!("{:.1}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.1}K", n / 1_000.0)
    } else {
        format!("{:.0}", n)
    }
}

const QUEUE: &str = "amqp-client-benchmark";
const URI: &str = "amqp://guest:guest@127.0.0.1";
const DURATION: Duration = Duration::from_secs(30);
const BODY: &[u8] = b"hello amqp-client.rs";
const REPORT_INTERVAL: Duration = Duration::from_secs(2);

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!(
        "Benchmark: {} byte messages for {}s",
        BODY.len(),
        DURATION.as_secs(),
    );
    println!();

    // --- Setup ---
    let setup_conn = Connection::connect(URI).await.expect("connect");
    let setup_ch = setup_conn.open_channel().await.expect("open channel");
    let _ = setup_ch
        .queue_delete(QUEUE, QueueDeleteOpts::default())
        .await;
    setup_ch
        .queue_declare(QUEUE, QueueDeclareOpts::default())
        .await
        .expect("declare queue");
    setup_ch.close().await.expect("close");
    setup_conn.close().await.expect("close");

    // Shared state
    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let producer_done = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    // --- Consumer ---
    let con_count = consumed.clone();
    let con_done = producer_done.clone();
    let consumer_handle = tokio::spawn(async move {
        let conn = Connection::connect(URI).await.expect("consumer: connect");
        let ch = conn.open_channel().await.expect("consumer: open channel");

        let opts = ConsumeOpts {
            no_ack: true,
            ..Default::default()
        };
        let mut consumer = ch
            .basic_consume(QUEUE, "bench-consumer", opts)
            .await
            .expect("consume");

        loop {
            if con_done.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_secs(2), consumer.next()).await {
                    Ok(Some(_)) => {
                        con_count.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => break,
                }
            } else {
                match consumer.next().await {
                    Some(_) => {
                        con_count.fetch_add(1, Ordering::Relaxed);
                    }
                    None => break,
                }
            }
        }

        ch.close().await.ok();
        conn.close().await.ok();
    });

    // Let consumer register before publishing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Producer ---
    let pub_count = published.clone();
    let producer_handle = tokio::spawn(async move {
        let conn = Connection::connect(URI).await.expect("producer: connect");
        let ch = conn.open_channel().await.expect("producer: open channel");

        let deadline = Instant::now() + DURATION;
        let props = BasicProperties::new();

        while Instant::now() < deadline {
            ch.basic_publish("", QUEUE, BODY, props.clone())
                .await
                .expect("publish");
            pub_count.fetch_add(1, Ordering::Relaxed);
        }

        ch.close().await.ok();
        conn.close().await.ok();
    });

    // --- Reporter ---
    let rep_pub = published.clone();
    let rep_con = consumed.clone();
    let rep_done = producer_done.clone();

    let reporter = tokio::spawn(async move {
        let mut last_pub: u64 = 0;
        let mut last_con: u64 = 0;
        let mut last_time = Instant::now();

        println!(
            "{:<10} {:>12} {:>12} {:>12} {:>12}",
            "elapsed", "pub total", "pub msg/s", "con total", "con msg/s"
        );
        println!("{}", "-".repeat(62));

        loop {
            tokio::time::sleep(REPORT_INTERVAL).await;

            let now = Instant::now();
            let dt = (now - last_time).as_secs_f64();
            let elapsed = (now - start).as_secs_f64();

            let pub_total = rep_pub.load(Ordering::Relaxed);
            let con_total = rep_con.load(Ordering::Relaxed);

            let pub_rate = (pub_total - last_pub) as f64 / dt;
            let con_rate = (con_total - last_con) as f64 / dt;

            println!(
                "{:>8.1}s {:>12} {:>10}/s {:>12} {:>10}/s",
                elapsed,
                humanize(pub_total as f64),
                humanize(pub_rate),
                humanize(con_total as f64),
                humanize(con_rate),
            );

            last_pub = pub_total;
            last_con = con_total;
            last_time = now;

            if rep_done.load(Ordering::Relaxed) && con_total >= pub_total {
                break;
            }
        }
    });

    // --- Wait ---
    producer_handle.await.expect("producer panicked");
    producer_done.store(true, Ordering::Relaxed);
    consumer_handle.await.expect("consumer panicked");
    reporter.abort();

    // --- Summary ---
    let elapsed = start.elapsed();
    let total_pub = published.load(Ordering::Relaxed);
    let total_con = consumed.load(Ordering::Relaxed);

    println!("{}", "-".repeat(62));
    println!();
    println!("Done in {:.1}s", elapsed.as_secs_f64());
    println!(
        "Published:  {} msgs ({}/s avg)",
        humanize(total_pub as f64),
        humanize(total_pub as f64 / DURATION.as_secs_f64()),
    );
    println!(
        "Consumed:   {} msgs ({}/s avg)",
        humanize(total_con as f64),
        humanize(total_con as f64 / elapsed.as_secs_f64()),
    );
    println!(
        "Throughput: {:.2} MB/s",
        (total_pub as f64 * BODY.len() as f64) / DURATION.as_secs_f64() / 1_000_000.0
    );

    // Cleanup
    let conn = Connection::connect(URI).await.expect("cleanup");
    let ch = conn.open_channel().await.expect("cleanup");
    let _ = ch.queue_delete(QUEUE, QueueDeleteOpts::default()).await;
    ch.close().await.ok();
    conn.close().await.ok();
}
