//! Automatic recovery. Run it, then restart the broker (or close the
//! connection in its management UI) and watch publishing and consuming carry
//! on. The server-named queue comes back under a new name.
//!
//!     cargo run --example recovery [amqp-uri]

use std::time::Duration;

use amqp_client_rs::{
    Client, ExchangeDeclareOpts, QueueDeclareOpts, RecoveryConfig, SubscribeOpts,
};

#[tokio::main]
async fn main() -> Result<(), amqp_client_rs::Error> {
    tracing_subscriber::fmt().init();
    let uri = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "amqp://guest:guest@localhost".into());

    // Opt in to recovery. build() still fails fast if the first connect fails.
    let client = Client::builder(&uri)?
        .recovery(RecoveryConfig::default())
        .build()
        .await?;

    // Declared through the client, so declared again after reconnecting.
    let auto_delete = ExchangeDeclareOpts {
        auto_delete: true,
        ..Default::default()
    };
    let exchange = client.topic("recovery-demo", auto_delete).await?;
    let exclusive = QueueDeclareOpts {
        exclusive: true,
        ..Default::default()
    };
    let queue = client.queue("", exclusive).await?;
    queue.bind(&exchange, "demo.#").await?;
    println!("consuming from {}", queue.name());

    let mut consumer = queue.subscribe::<Vec<u8>>(SubscribeOpts::default()).await?;
    let consuming = tokio::spawn(async move {
        // next() waits across recovery; it returns None once the client is
        // dropped.
        while let Some(delivery) = consumer.next().await {
            let body = String::from_utf8_lossy(&delivery.body);
            println!("received {body} from {}", queue.name());
            let _ = delivery.ack();
        }
    });

    // One future for the whole loop, so a Ctrl+C during a publish is not lost.
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    for n in 1u64.. {
        // publish waits at most RecoveryConfig::timeout (250 ms) for a new
        // connection, then fails; the next tick tries again.
        let body = format!("tick {n}").into_bytes();
        match client.publish(exchange.name(), "demo.tick", &body).await {
            Ok(()) => println!("published tick {n}"),
            Err(e) => println!("publish of tick {n} failed: {e}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = &mut ctrl_c => break,
        }
    }

    drop(client);
    let _ = consuming.await;
    Ok(())
}
